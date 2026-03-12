"""Autonomous VM benchmark scenarios."""

from __future__ import annotations

import base64
import json
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from importlib import resources
from ssl import SSLContext

from rally import exceptions as rally_exceptions
from rally.task import atomic
from rally.task import types
from rally.task import validation

from rally_openstack.common import consts
from rally_openstack.task import scenario
from rally_openstack.task.scenarios.nova import utils as nova_utils

from rally_ci_churn.results import build_metadata_output
from rally_ci_churn.results import build_stage_output
from rally_ci_churn.results import build_table_output
from rally_ci_churn.results import parse_console_result


POLL_INTERVAL_SECONDS = 2.0
RESULT_FETCH_TIMEOUT_SECONDS = 30


class _AutonomousVMBase(nova_utils.NovaScenario):
    @atomic.action_timer("vm.wait_for_shutdown")
    def _wait_for_shutdown(self, server, timeout_seconds: int):
        start = time.monotonic()
        while True:
            server = self._show_server(server)
            if server.status == "SHUTOFF":
                return server
            if server.status == "ERROR":
                raise rally_exceptions.ScriptError(
                    message=f"Server {server.id} entered ERROR state before shutdown"
                )
            if timeout_seconds > 0 and (time.monotonic() - start) >= timeout_seconds:
                raise rally_exceptions.TimeoutException(
                    timeout=timeout_seconds,
                    resource_type="server",
                    resource_name=server.name,
                    resource_id=server.id,
                    desired_status="SHUTOFF",
                    resource_status=server.status,
                )
            time.sleep(POLL_INTERVAL_SECONDS)

    def _build_user_data(self, payload: dict[str, object], swift_cacert_b64: str) -> str:
        runner_source = resources.files("rally_ci_churn.guest").joinpath("runner_main.py").read_text(
            encoding="utf-8"
        )
        runner_b64 = base64.b64encode(runner_source.encode("utf-8")).decode("ascii")
        payload_b64 = base64.b64encode(json.dumps(payload, sort_keys=True).encode("utf-8")).decode(
            "ascii"
        )
        lines = [
            "#cloud-config",
            "write_files:",
            "  - path: /opt/rally-ci/runner_main.py",
            '    permissions: "0755"',
            "    encoding: b64",
            f"    content: {runner_b64}",
            "  - path: /opt/rally-ci/config.json",
            '    permissions: "0644"',
            "    encoding: b64",
            f"    content: {payload_b64}",
        ]
        if swift_cacert_b64:
            lines.extend(
                [
                    "  - path: /etc/ssl/certs/rally-ci-swift-ca.pem",
                    '    permissions: "0644"',
                    "    encoding: b64",
                    f"    content: {swift_cacert_b64}",
                ]
            )
        lines.extend(
            [
                "runcmd:",
                (
                    '  - [ cloud-init-per, once, rally-ci-runner, /bin/bash, -lc, '
                    '"python3 /opt/rally-ci/runner_main.py /opt/rally-ci/config.json '
                    '> >(tee -a /var/log/rally-ci-runner.log /dev/console) 2>&1 || true; '
                    'sync; (systemctl poweroff --force --force || poweroff -f || shutdown -P now)" ]'
                ),
            ]
        )
        return "\n".join(lines) + "\n"

    def _build_ssl_context(self, swift_cacert_b64: str) -> SSLContext:
        if not swift_cacert_b64:
            return ssl.create_default_context()
        ca_data = base64.b64decode(swift_cacert_b64.encode("ascii")).decode("utf-8")
        return ssl.create_default_context(cadata=ca_data)

    def _request_json(
        self,
        method: str,
        url: str,
        headers: dict[str, str],
        context: SSLContext,
        data: bytes | None = None,
    ):
        request = urllib.request.Request(url=url, method=method, headers=headers, data=data)
        with urllib.request.urlopen(request, context=context) as response:
            body = response.read()
            return response, json.loads(body.decode("utf-8")) if body else {}

    def _normalize_auth_url(self, auth_url: str) -> str:
        auth_url = auth_url.rstrip("/")
        if auth_url.endswith("/v3"):
            return auth_url + "/auth/tokens"
        return auth_url + "/v3/auth/tokens"

    def _authenticate_swift(
        self,
        swift_auth_url: str,
        swift_username: str,
        swift_password: str,
        swift_project_name: str,
        swift_user_domain_name: str,
        swift_project_domain_name: str,
        swift_interface: str,
        swift_region_name: str,
        context: SSLContext,
    ) -> tuple[str, str]:
        auth = {
            "auth": {
                "identity": {
                    "methods": ["password"],
                    "password": {
                        "user": {
                            "name": swift_username,
                            "password": swift_password,
                            "domain": {"name": swift_user_domain_name},
                        }
                    },
                },
                "scope": {
                    "project": {
                        "name": swift_project_name,
                        "domain": {"name": swift_project_domain_name},
                    }
                },
            }
        }
        response, body = self._request_json(
            "POST",
            self._normalize_auth_url(swift_auth_url),
            {"Content-Type": "application/json"},
            context,
            json.dumps(auth).encode("utf-8"),
        )
        token = response.headers.get("X-Subject-Token")
        if not token:
            raise rally_exceptions.ScriptError(
                message="Keystone response did not include X-Subject-Token"
            )
        for service in body.get("token", {}).get("catalog", []):
            if service.get("type") != "object-store":
                continue
            for endpoint in service.get("endpoints", []):
                if endpoint.get("interface") != swift_interface:
                    continue
                if swift_region_name and endpoint.get("region") != swift_region_name:
                    continue
                return token, endpoint["url"].rstrip("/")
        raise rally_exceptions.ScriptError(
            message="Unable to find a Swift endpoint in Keystone catalog"
        )

    def _read_swift_object(
        self,
        endpoint: str,
        container: str,
        object_name: str,
        token: str,
        context: SSLContext,
    ) -> dict[str, object] | None:
        object_url = endpoint + "/" + "/".join(
            [
                urllib.parse.quote(container, safe=""),
                urllib.parse.quote(object_name, safe=""),
            ]
        )
        request = urllib.request.Request(
            url=object_url,
            method="GET",
            headers={"X-Auth-Token": token},
        )
        try:
            with urllib.request.urlopen(request, context=context) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return None
            raise

    @atomic.action_timer("swift.wait_for_result")
    def _wait_for_result_object(
        self,
        endpoint: str,
        container: str,
        object_name: str,
        token: str,
        context: SSLContext,
        timeout_seconds: int,
    ) -> dict[str, object] | None:
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            result = self._read_swift_object(endpoint, container, object_name, token, context)
            if result:
                return result
            time.sleep(POLL_INTERVAL_SECONDS)
        return None

    def _launch_runner_vm(
        self,
        image,
        flavor,
        scenario_name: str,
        artifact_container: str,
        artifact_ttl_seconds: int,
        swift_auth_url: str,
        swift_username: str,
        swift_password: str,
        swift_project_name: str,
        swift_user_domain_name: str,
        swift_project_domain_name: str,
        swift_interface: str,
        swift_region_name: str,
        swift_cacert_b64: str,
        workload_profile: str,
        workload_params: dict[str, object],
        iteration: int,
        wave: int,
        **kwargs,
    ) -> dict[str, object]:
        result_object_name = f"results/{uuid.uuid4().hex}.json"
        payload = {
            "scenario_name": scenario_name,
            "wave": wave,
            "iteration": iteration,
            "workload_profile": workload_profile,
            "workload_params": workload_params,
            "artifact_container": artifact_container,
            "artifact_ttl_seconds": artifact_ttl_seconds,
            "swift_auth_url": swift_auth_url,
            "swift_username": swift_username,
            "swift_password": swift_password,
            "swift_project_name": swift_project_name,
            "swift_user_domain_name": swift_user_domain_name,
            "swift_project_domain_name": swift_project_domain_name,
            "swift_interface": swift_interface,
            "swift_region_name": swift_region_name,
            "swift_cacert": "/etc/ssl/certs/rally-ci-swift-ca.pem" if swift_cacert_b64 else "",
            "result_object_name": result_object_name,
        }
        kwargs["userdata"] = self._build_user_data(payload, swift_cacert_b64)
        server = self._boot_server(image, flavor, auto_assign_nic=True, **kwargs)
        return {
            "server": server,
            "result_object_name": result_object_name,
            "launched_monotonic": time.monotonic(),
        }

    def _build_timeout_result(
        self,
        scenario_name: str,
        server,
        launched_monotonic: float,
        wave: int,
        iteration: int,
    ) -> dict[str, object]:
        return {
            "schema_version": 1,
            "scenario_family": "autonomous_vm",
            "scenario_name": scenario_name,
            "status": "timeout",
            "timeout": True,
            "wave": wave,
            "iteration": iteration,
            "hostname": server.name,
            "duration_seconds": round(time.monotonic() - launched_monotonic, 3),
            "artifact_refs": [],
            "metrics": {},
            "diagnostics": {"error": "Guest did not reach SHUTOFF before timeout"},
            "stages": [],
        }

    def _build_error_result(
        self,
        scenario_name: str,
        server,
        launched_monotonic: float,
        wave: int,
        iteration: int,
        error: str,
    ) -> dict[str, object]:
        return {
            "schema_version": 1,
            "scenario_family": "autonomous_vm",
            "scenario_name": scenario_name,
            "status": "error",
            "timeout": False,
            "wave": wave,
            "iteration": iteration,
            "hostname": server.name,
            "duration_seconds": round(time.monotonic() - launched_monotonic, 3),
            "artifact_refs": [],
            "metrics": {},
            "diagnostics": {"error": error},
            "stages": [],
        }

    def _fetch_vm_result(
        self,
        vm_state: dict[str, object],
        artifact_container: str,
        console_log_length: int,
        swift_context: SSLContext,
        swift_token: str,
        swift_endpoint: str,
    ) -> dict[str, object] | None:
        server = vm_state["server"]
        console_output = ""
        try:
            console_output = self._get_server_console_output(server, length=console_log_length)
        except Exception:  # noqa: BLE001
            console_output = ""
        result = parse_console_result(console_output)
        if result:
            return result
        return self._wait_for_result_object(
            swift_endpoint,
            artifact_container,
            str(vm_state["result_object_name"]),
            swift_token,
            swift_context,
            RESULT_FETCH_TIMEOUT_SECONDS,
        )

    def _delete_vm(self, vm_state: dict[str, object], force_delete: bool) -> None:
        self._delete_server(vm_state["server"], force=force_delete)


@types.convert(image={"type": "glance_image"}, flavor={"type": "nova_flavor"})
@validation.add("required_services", services=[consts.Service.NOVA])
@validation.add("image_valid_on_flavor", flavor_param="flavor", image_param="image")
@validation.add("required_platform", platform="openstack", users=True)
@scenario.configure(
    name="CIChurn.boot_autonomous_vm",
    platform="openstack",
    context={"cleanup@openstack": ["nova"], "network@openstack": {}},
)
class BootAutonomousVM(_AutonomousVMBase):
    """Boot one autonomous cloud-init runner, wait for SHUTOFF, then delete."""

    def run(
        self,
        image,
        flavor,
        workload_profile,
        artifact_container,
        swift_auth_url,
        swift_username,
        swift_password,
        swift_project_name,
        swift_user_domain_name,
        swift_project_domain_name,
        timeout_seconds=3600,
        timeout_mode="fail",
        artifact_ttl_seconds=0,
        swift_interface="public",
        swift_region_name="",
        swift_cacert_b64="",
        workload_params=None,
        console_log_length=400,
        force_delete=False,
        wave=0,
        **kwargs,
    ):
        workload_params = workload_params or {}
        iteration = int(self.context.get("iteration", 0) or 0)
        swift_context = self._build_ssl_context(swift_cacert_b64)
        swift_token, swift_endpoint = self._authenticate_swift(
            swift_auth_url,
            swift_username,
            swift_password,
            swift_project_name,
            swift_user_domain_name,
            swift_project_domain_name,
            swift_interface,
            swift_region_name,
            swift_context,
        )
        vm_state = self._launch_runner_vm(
            image,
            flavor,
            "CIChurn.boot_autonomous_vm",
            artifact_container,
            artifact_ttl_seconds,
            swift_auth_url,
            swift_username,
            swift_password,
            swift_project_name,
            swift_user_domain_name,
            swift_project_domain_name,
            swift_interface,
            swift_region_name,
            swift_cacert_b64,
            workload_profile,
            workload_params,
            iteration,
            wave,
            **kwargs,
        )
        result = None
        timed_out = False
        try:
            try:
                self._wait_for_shutdown(vm_state["server"], int(timeout_seconds))
            except rally_exceptions.TimeoutException:
                timed_out = True
            if timed_out:
                result = self._build_timeout_result(
                    "CIChurn.boot_autonomous_vm",
                    vm_state["server"],
                    vm_state["launched_monotonic"],
                    wave,
                    iteration,
                )
            else:
                result = self._fetch_vm_result(
                    vm_state,
                    artifact_container,
                    console_log_length,
                    swift_context,
                    swift_token,
                    swift_endpoint,
                )
            if not result:
                raise rally_exceptions.ScriptError(
                    message="Guest completed without emitting a structured result payload"
                )
            self.add_output(complete=build_metadata_output(result))
            self.add_output(complete=build_stage_output(result))
            if result.get("status") == "error":
                raise rally_exceptions.ScriptError(
                    message=str(result.get("diagnostics", {}).get("error", "Guest benchmark failed"))
                )
            if timed_out and timeout_mode == "fail":
                raise rally_exceptions.ScriptError(
                    message=f"Guest did not reach SHUTOFF within {timeout_seconds} seconds"
                )
        finally:
            self._delete_vm(vm_state, force_delete)


@types.convert(image={"type": "glance_image"}, flavor={"type": "nova_flavor"})
@validation.add("required_services", services=[consts.Service.NOVA])
@validation.add("image_valid_on_flavor", flavor_param="flavor", image_param="image")
@validation.add("required_platform", platform="openstack", users=True)
@scenario.configure(
    name="CIChurn.spiky_autonomous_vm",
    platform="openstack",
    context={"cleanup@openstack": ["nova"], "network@openstack": {}},
)
class SpikyAutonomousVM(_AutonomousVMBase):
    """Launch autonomous runners with a time-based burst schedule."""

    def _validate_burst_windows(self, burst_windows: list[dict[str, object]]) -> list[dict[str, object]]:
        normalized = []
        previous_end = 0
        for raw_window in sorted(
            burst_windows,
            key=lambda item: int(item.get("start_second", 0)),
        ):
            start_second = int(raw_window.get("start_second", 0))
            end_second = int(raw_window.get("end_second", 0))
            multiplier = float(raw_window.get("launch_rate_multiplier", 1.0))
            if start_second < 0 or end_second <= start_second:
                raise rally_exceptions.ScriptError(message=f"Invalid burst window: {raw_window}")
            if start_second < previous_end:
                raise rally_exceptions.ScriptError(message=f"Overlapping burst windows: {raw_window}")
            if multiplier < 0:
                raise rally_exceptions.ScriptError(
                    message=f"Burst multiplier must be >= 0: {raw_window}"
                )
            normalized.append(
                {
                    "start_second": start_second,
                    "end_second": end_second,
                    "launch_rate_multiplier": multiplier,
                }
            )
            previous_end = end_second
        return normalized

    def _multiplier_for_offset(
        self,
        offset_seconds: float,
        burst_windows: list[dict[str, object]],
    ) -> float:
        for window in burst_windows:
            if window["start_second"] <= offset_seconds < window["end_second"]:
                return float(window["launch_rate_multiplier"])
        return 1.0

    def _build_summary_output(self, summary: dict[str, object]) -> dict[str, object]:
        rows = [[key, str(summary[key])] for key in sorted(summary)]
        return build_table_output(
            "Spiky run summary",
            "Aggregate scheduler and VM lifecycle counters for the spiky run",
            ["key", "value"],
            rows,
        )

    def _build_timeline_output(self, rows: list[list[object]]) -> dict[str, object]:
        return build_table_output(
            "Spiky timeline",
            "Per-tick scheduler timeline",
            [
                "offset_seconds",
                "target_launches_per_minute",
                "active_vms",
                "launched",
                "dropped",
                "completed",
            ],
            rows,
        )

    def _build_vm_output(self, rows: list[list[object]]) -> dict[str, object]:
        return build_table_output(
            "VM results",
            "Per-VM result summary",
            ["server", "status", "duration_seconds", "artifact", "error"],
            rows,
        )

    def run(
        self,
        image,
        flavor,
        workload_profile,
        artifact_container,
        swift_auth_url,
        swift_username,
        swift_password,
        swift_project_name,
        swift_user_domain_name,
        swift_project_domain_name,
        duration_seconds,
        max_active_vms,
        baseline_launches_per_minute,
        burst_windows=None,
        launch_tick_seconds=1,
        timeout_seconds=3600,
        timeout_mode="fail",
        artifact_ttl_seconds=0,
        swift_interface="public",
        swift_region_name="",
        swift_cacert_b64="",
        workload_params=None,
        console_log_length=400,
        force_delete=False,
        wave=0,
        **kwargs,
    ):
        workload_params = workload_params or {}
        burst_windows = self._validate_burst_windows(burst_windows or [])
        duration_seconds = int(duration_seconds)
        max_active_vms = int(max_active_vms)
        launch_tick_seconds = max(1, int(launch_tick_seconds))
        baseline_launches_per_minute = float(baseline_launches_per_minute)
        if duration_seconds <= 0:
            raise rally_exceptions.ScriptError(message="duration_seconds must be > 0")
        if max_active_vms <= 0:
            raise rally_exceptions.ScriptError(message="max_active_vms must be > 0")
        if baseline_launches_per_minute < 0:
            raise rally_exceptions.ScriptError(
                message="baseline_launches_per_minute must be >= 0"
            )

        scenario_name = "CIChurn.spiky_autonomous_vm"
        iteration = int(self.context.get("iteration", 0) or 0)
        swift_context = self._build_ssl_context(swift_cacert_b64)
        swift_token, swift_endpoint = self._authenticate_swift(
            swift_auth_url,
            swift_username,
            swift_password,
            swift_project_name,
            swift_user_domain_name,
            swift_project_domain_name,
            swift_interface,
            swift_region_name,
            swift_context,
        )

        active_vms: dict[str, dict[str, object]] = {}
        timeline_rows: list[list[object]] = []
        vm_rows: list[list[object]] = []
        summary = {
            "duration_seconds": duration_seconds,
            "max_active_vms": max_active_vms,
            "baseline_launches_per_minute": baseline_launches_per_minute,
            "launched_vms": 0,
            "completed_vms": 0,
            "failed_vms": 0,
            "timed_out_vms": 0,
            "dropped_launches": 0,
            "peak_active_vms": 0,
        }
        errors: list[str] = []
        start = time.monotonic()
        next_tick = start
        arrival_deadline = start + duration_seconds
        tokens = 0.0
        completed_since_tick = 0

        try:
            while True:
                now = time.monotonic()
                for server_id, vm_state in list(active_vms.items()):
                    server = self._show_server(vm_state["server"])
                    vm_state["server"] = server
                    if server.status == "SHUTOFF":
                        result = self._fetch_vm_result(
                            vm_state,
                            artifact_container,
                            console_log_length,
                            swift_context,
                            swift_token,
                            swift_endpoint,
                        )
                        if not result:
                            result = self._build_error_result(
                                scenario_name,
                                server,
                                float(vm_state["launched_monotonic"]),
                                wave,
                                iteration,
                                "Guest completed without emitting a structured result payload",
                            )
                        status = str(result.get("status", "unknown"))
                        error = str(result.get("diagnostics", {}).get("error", ""))
                        artifact = ""
                        artifact_refs = result.get("artifact_refs", [])
                        if artifact_refs and isinstance(artifact_refs[0], dict):
                            artifact = str(artifact_refs[0].get("object_name", ""))
                        vm_rows.append(
                            [
                                server.name,
                                status,
                                result.get("duration_seconds", ""),
                                artifact,
                                error,
                            ]
                        )
                        summary["completed_vms"] += 1
                        if status == "error":
                            summary["failed_vms"] += 1
                            errors.append(error or f"{server.name} returned an error result")
                        completed_since_tick += 1
                        self._delete_vm(vm_state, force_delete)
                        del active_vms[server_id]
                    elif server.status == "ERROR":
                        summary["failed_vms"] += 1
                        errors.append(f"{server.name} entered ERROR state before shutdown")
                        vm_rows.append(
                            [
                                server.name,
                                "error",
                                round(time.monotonic() - float(vm_state["launched_monotonic"]), 3),
                                "",
                                "Server entered ERROR state before shutdown",
                            ]
                        )
                        self._delete_vm(vm_state, force_delete)
                        del active_vms[server_id]
                        completed_since_tick += 1
                    elif timeout_seconds > 0 and (
                        time.monotonic() - float(vm_state["launched_monotonic"])
                    ) >= int(timeout_seconds):
                        summary["timed_out_vms"] += 1
                        result = self._build_timeout_result(
                            scenario_name,
                            server,
                            float(vm_state["launched_monotonic"]),
                            wave,
                            iteration,
                        )
                        vm_rows.append(
                            [
                                server.name,
                                "timeout",
                                result["duration_seconds"],
                                "",
                                result["diagnostics"]["error"],
                            ]
                        )
                        if timeout_mode == "fail":
                            errors.append(f"{server.name} timed out after {timeout_seconds} seconds")
                        self._delete_vm(vm_state, force_delete)
                        del active_vms[server_id]
                        completed_since_tick += 1

                if now >= next_tick and next_tick < arrival_deadline:
                    offset_seconds = next_tick - start
                    multiplier = self._multiplier_for_offset(offset_seconds, burst_windows)
                    target_launches_per_minute = baseline_launches_per_minute * multiplier
                    tokens += target_launches_per_minute / 60.0 * launch_tick_seconds
                    launched_this_tick = 0
                    dropped_this_tick = 0
                    while tokens >= 1.0:
                        if len(active_vms) < max_active_vms:
                            vm_state = self._launch_runner_vm(
                                image,
                                flavor,
                                scenario_name,
                                artifact_container,
                                artifact_ttl_seconds,
                                swift_auth_url,
                                swift_username,
                                swift_password,
                                swift_project_name,
                                swift_user_domain_name,
                                swift_project_domain_name,
                                swift_interface,
                                swift_region_name,
                                swift_cacert_b64,
                                workload_profile,
                                workload_params,
                                iteration,
                                wave,
                                **kwargs,
                            )
                            active_vms[vm_state["server"].id] = vm_state
                            summary["launched_vms"] += 1
                            launched_this_tick += 1
                            summary["peak_active_vms"] = max(
                                int(summary["peak_active_vms"]),
                                len(active_vms),
                            )
                        else:
                            summary["dropped_launches"] += 1
                            dropped_this_tick += 1
                        tokens -= 1.0
                    timeline_rows.append(
                        [
                            int(offset_seconds),
                            round(target_launches_per_minute, 3),
                            len(active_vms),
                            launched_this_tick,
                            dropped_this_tick,
                            completed_since_tick,
                        ]
                    )
                    completed_since_tick = 0
                    next_tick += launch_tick_seconds

                if now >= arrival_deadline and not active_vms:
                    break

                if next_tick < arrival_deadline:
                    sleep_seconds = min(POLL_INTERVAL_SECONDS, max(next_tick - time.monotonic(), 0.1))
                else:
                    sleep_seconds = POLL_INTERVAL_SECONDS
                time.sleep(sleep_seconds)
        finally:
            for vm_state in list(active_vms.values()):
                self._delete_vm(vm_state, force_delete)

        effective_lpm = 0.0
        if duration_seconds > 0:
            effective_lpm = round(int(summary["launched_vms"]) * 60.0 / duration_seconds, 3)
        summary["effective_launches_per_minute"] = effective_lpm
        self.add_output(complete=self._build_summary_output(summary))
        self.add_output(complete=self._build_timeline_output(timeline_rows))
        self.add_output(complete=self._build_vm_output(vm_rows))
        if errors:
            raise rally_exceptions.ScriptError(message="; ".join(errors[:5]))

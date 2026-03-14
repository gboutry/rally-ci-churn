"""Network traffic benchmark scenarios."""

from __future__ import annotations

import json
import shutil
import stat
import tempfile
import time
import uuid
from importlib import resources
from pathlib import Path

from rally import exceptions as rally_exceptions
from rally.task import atomic
from rally.task import types
from rally.task import validation
from rally.utils import sshutils

from rally_openstack.common import consts
from rally_openstack.task import scenario
from rally_openstack.task.scenarios.vm import utils as vm_utils

from rally_ci_churn.results import build_artifacts_output
from rally_ci_churn.results import build_metrics_output
from rally_ci_churn.results import build_phase_output
from rally_ci_churn.results import build_summary_output
from rally_ci_churn.results import build_table_output
from rally_ci_churn.results import summarize_atomic_actions
from rally_ci_churn.results import summarize_numeric_series


SSH_PORT = 22
HTTP_PORT = 8080
ATTACH_RETRY_COUNT = 5
ATTACH_RETRY_DELAY_SECONDS = 5.0
VOLUME_POLL_INTERVAL_SECONDS = 2.0


def _as_int_list(values: list[object]) -> list[int]:
    return [int(value) for value in values]


def _as_str_list(values: list[object]) -> list[str]:
    return [str(value) for value in values]


class _NetTrafficBase(vm_utils.VMScenario):
    def _task_uuid(self) -> str:
        owner_id = self.context.get("owner_id")
        if owner_id:
            return str(owner_id)
        task = self.context.get("task")
        if isinstance(task, dict) and task.get("uuid"):
            return str(task["uuid"])
        return uuid.uuid4().hex

    def _iteration_number(self) -> int:
        return int(self.context.get("iteration", 0) or 0)

    def _tenant_cidr(self) -> str:
        tenant = self.context.get("tenant", {})
        subnets = tenant.get("subnets", [])
        if subnets:
            cidr = subnets[0].get("cidr")
            if cidr:
                return str(cidr)
        raise rally_exceptions.ScriptError(message="network@openstack did not provide a tenant subnet")

    def _fixed_ip(self, server) -> str:
        for addresses in server.addresses.values():
            for entry in addresses:
                if entry.get("OS-EXT-IPS:type") == "fixed" or "OS-EXT-IPS:type" not in entry:
                    return str(entry["addr"])
        raise rally_exceptions.ScriptError(message=f"Unable to determine a fixed IP for server {server.id}")

    def _create_keypair(self) -> dict[str, str]:
        keypair = self.clients("nova").keypairs.create(self.generate_random_name())
        return {
            "name": keypair.name,
            "private": keypair.private_key,
            "public": keypair.public_key,
        }

    def _delete_keypair(self, name: str) -> None:
        try:
            self.clients("nova").keypairs.delete(name)
        except Exception:
            return

    def _create_security_group(self, name: str, description: str) -> dict[str, object]:
        return self.neutron.create_security_group(name=name, description=description)

    def _delete_security_group(self, security_group_id: str) -> None:
        try:
            self.neutron.delete_security_group(security_group_id)
        except Exception:
            return

    def _add_security_group_rule(
        self,
        security_group_id: str,
        protocol: str | None = None,
        port_min: int | None = None,
        port_max: int | None = None,
        remote_ip_prefix: str | None = None,
        ethertype: str = "IPv4",
    ) -> None:
        kwargs: dict[str, object] = {
            "security_group_id": security_group_id,
            "ethertype": ethertype,
        }
        if protocol is not None:
            kwargs["protocol"] = protocol
        if port_min is not None:
            kwargs["port_range_min"] = port_min
        if port_max is not None:
            kwargs["port_range_max"] = port_max
        if remote_ip_prefix is not None:
            kwargs["remote_ip_prefix"] = remote_ip_prefix
        self.neutron.create_security_group_rule(**kwargs)

    def _create_controller_security_group(self) -> dict[str, object]:
        security_group = self._create_security_group(
            self.generate_random_name(),
            "Allow SSH access to the benchmark controller VM",
        )
        self._add_security_group_rule(security_group["id"], "tcp", SSH_PORT, SSH_PORT, "0.0.0.0/0")
        self._add_security_group_rule(security_group["id"], "icmp", remote_ip_prefix="0.0.0.0/0")
        return security_group

    def _create_benchmark_security_group(self, tenant_cidr: str) -> dict[str, object]:
        security_group = self._create_security_group(
            self.generate_random_name(),
            "Allow benchmark traffic from the tenant network",
        )
        self._add_security_group_rule(security_group["id"], "tcp", SSH_PORT, SSH_PORT, tenant_cidr)
        self._add_security_group_rule(security_group["id"], "tcp", 1, 65535, tenant_cidr)
        self._add_security_group_rule(security_group["id"], "udp", 1, 65535, tenant_cidr)
        self._add_security_group_rule(security_group["id"], "icmp", remote_ip_prefix=tenant_cidr)
        return security_group

    def _controller_user_data(self) -> str:
        return """#cloud-config
write_files:
  - path: /etc/rally-ci-net-role
    permissions: "0644"
    content: |
      role=controller
runcmd:
  - [ cloud-init-per, once, rally-netbench-controller-ready, /bin/sh, -c, "mkdir -p /var/lib/rally-netbench/run && chown -R ubuntu:ubuntu /var/lib/rally-netbench" ]
"""

    def _benchmark_user_data(self) -> str:
        return """#cloud-config
write_files:
  - path: /etc/rally-ci-net-role
    permissions: "0644"
    content: |
      role=benchmark
runcmd:
  - [ cloud-init-per, once, rally-netbench-host-ready, /bin/sh, -c, "mkdir -p /var/lib/rally-netbench && chown -R ubuntu:ubuntu /var/lib/rally-netbench" ]
"""

    @atomic.action_timer("controller.boot")
    def _boot_controller(
        self,
        image,
        flavor,
        external_network_name: str,
        key_name: str,
        security_group_name: str,
    ):
        return self._boot_server_with_fip(
            image,
            flavor,
            use_floating_ip=True,
            floating_network=external_network_name,
            key_name=key_name,
            security_groups=[security_group_name],
            userdata=self._controller_user_data(),
        )

    @atomic.action_timer("benchmark.boot")
    def _boot_benchmark_vm(
        self,
        image,
        flavor,
        key_name: str,
        security_group_name: str,
    ):
        return self._boot_server(
            image,
            flavor,
            auto_assign_nic=True,
            key_name=key_name,
            security_groups=[security_group_name],
            userdata=self._benchmark_user_data(),
        )

    def _wait_for_volume_status(self, volume_id: str, statuses: list[str], timeout_seconds: int = 600):
        deadline = time.monotonic() + timeout_seconds
        volume = None
        while time.monotonic() < deadline:
            volume = self.clients("cinder").volumes.get(volume_id)
            if volume.status in statuses:
                return volume
            time.sleep(VOLUME_POLL_INTERVAL_SECONDS)
        current = volume.status if volume is not None else "unknown"
        raise rally_exceptions.ScriptError(
            message=f"Volume {volume_id} did not reach {statuses} before timeout (current={current})"
        )

    @atomic.action_timer("server_volume.create")
    def _create_volume(self, size: int, volume_type: str | None):
        kwargs: dict[str, object] = {"size": size, "name": self.generate_random_name()}
        if volume_type:
            kwargs["volume_type"] = volume_type
        volume = self.clients("cinder").volumes.create(**kwargs)
        return self._wait_for_volume_status(volume.id, ["available"])

    @atomic.action_timer("server_volume.attach")
    def _attach_volume(self, server, volume_id: str, device_name: str):
        last_error = None
        for attempt in range(1, ATTACH_RETRY_COUNT + 1):
            try:
                self.clients("nova").volumes.create_server_volume(server.id, volume_id, device_name)
                return self._wait_for_volume_status(volume_id, ["in-use"])
            except Exception as exc:
                last_error = exc
                try:
                    volume = self.clients("cinder").volumes.get(volume_id)
                    if volume.status == "in-use":
                        return volume
                except Exception:
                    pass
                if attempt == ATTACH_RETRY_COUNT:
                    break
                time.sleep(ATTACH_RETRY_DELAY_SECONDS)
        raise last_error

    def _detach_volume(self, server_id: str, volume_id: str) -> None:
        try:
            self.clients("nova").volumes.delete_server_volume(server_id, volume_id)
        except Exception:
            return
        try:
            self._wait_for_volume_status(volume_id, ["available"], timeout_seconds=300)
        except Exception:
            return

    def _delete_volume(self, volume_id: str) -> None:
        try:
            self.clients("cinder").volumes.delete(volume_id)
        except Exception:
            return

    @atomic.action_timer("controller.connect_ssh")
    def _ssh(
        self,
        ip_address: str,
        username: str,
        private_key: str,
        timeout_seconds: int,
    ) -> sshutils.SSH:
        ssh = sshutils.SSH(username, ip_address, port=SSH_PORT, pkey=private_key)
        self._wait_for_ssh(ssh, timeout=timeout_seconds, interval=2)
        return ssh

    @atomic.action_timer("artifacts.download")
    def _download_tree(self, ssh: sshutils.SSH, remote_dir: str, local_dir: Path) -> None:
        client = ssh._get_client()  # noqa: SLF001
        local_dir.mkdir(parents=True, exist_ok=True)
        with client.open_sftp() as sftp:
            self._download_tree_sftp(sftp, remote_dir, local_dir)

    def _download_tree_sftp(self, sftp, remote_dir: str, local_dir: Path) -> None:
        for entry in sftp.listdir_attr(remote_dir):
            remote_path = f"{remote_dir.rstrip('/')}/{entry.filename}"
            local_path = local_dir / entry.filename
            if stat.S_ISDIR(entry.st_mode):
                local_path.mkdir(parents=True, exist_ok=True)
                self._download_tree_sftp(sftp, remote_path, local_path)
            else:
                sftp.get(remote_path, str(local_path))

    def _artifacts_dir(self, root_dir: str, scenario_slug: str) -> Path:
        scenario_root = Path(root_dir).expanduser().resolve() / self._task_uuid() / scenario_slug
        iteration_dir = scenario_root / f"iteration-{self._iteration_number():04d}"
        if iteration_dir.exists():
            shutil.rmtree(iteration_dir)
        iteration_dir.mkdir(parents=True, exist_ok=True)
        return iteration_dir

    @atomic.action_timer("controller.upload_inputs")
    def _upload_controller_inputs(
        self,
        ssh: sshutils.SSH,
        ssh_user: str,
        controller_remote_dir: str,
        inventory: dict[str, object],
        matrix: dict[str, object],
        private_key: str,
    ) -> None:
        ssh.execute(
            [
                "sudo",
                "install",
                "-d",
                "-o",
                ssh_user,
                "-g",
                ssh_user,
                "-m",
                "0755",
                controller_remote_dir,
            ],
            timeout=300,
        )
        controller_runner = resources.files("rally_ci_churn.net").joinpath("controller_runner.py")
        with tempfile.TemporaryDirectory(prefix="rally-netbench-controller-") as temp_dir:
            temp_path = Path(temp_dir)
            inventory_path = temp_path / "inventory.json"
            matrix_path = temp_path / "matrix.json"
            controller_path = temp_path / "controller_runner.py"
            private_key_path = temp_path / "id_rsa"
            inventory_path.write_text(json.dumps(inventory, indent=2, sort_keys=True), encoding="utf-8")
            matrix_path.write_text(json.dumps(matrix, indent=2, sort_keys=True), encoding="utf-8")
            controller_path.write_text(controller_runner.read_text(encoding="utf-8"), encoding="utf-8")
            private_key_path.write_text(private_key, encoding="utf-8")
            ssh.put_file(str(inventory_path), f"{controller_remote_dir}/inventory.json", mode=0o644)
            ssh.put_file(str(matrix_path), f"{controller_remote_dir}/matrix.json", mode=0o644)
            ssh.put_file(str(controller_path), f"{controller_remote_dir}/controller_runner.py", mode=0o755)
            ssh.put_file(str(private_key_path), f"{controller_remote_dir}/id_rsa", mode=0o600)

    @atomic.action_timer("traffic.run_case")
    def _run_controller_runner(
        self,
        ssh: sshutils.SSH,
        controller_remote_dir: str,
        command_timeout_seconds: int,
    ) -> tuple[int, str, str]:
        return ssh.execute(
            [
                "python3",
                f"{controller_remote_dir}/controller_runner.py",
                "--inventory",
                f"{controller_remote_dir}/inventory.json",
                "--matrix",
                f"{controller_remote_dir}/matrix.json",
                "--output-dir",
                controller_remote_dir,
            ],
            timeout=command_timeout_seconds,
        )

    def _artifact_rows(self, artifacts_dir: Path) -> list[list[object]]:
        return [
            ["artifact_root", str(artifacts_dir)],
            ["summary_markdown", str(artifacts_dir / "summary.md")],
            ["summary_json", str(artifacts_dir / "summary.json")],
            ["manifest_json", str(artifacts_dir / "manifest.json")],
        ]

    def _timings_payload(self) -> dict[str, dict[str, object]]:
        _, summary = summarize_atomic_actions(self.atomic_actions())
        return summary


@types.convert(
    controller_image={"type": "glance_image"},
    controller_flavor={"type": "nova_flavor"},
    server_image={"type": "glance_image"},
    server_flavor={"type": "nova_flavor"},
    client_image={"type": "glance_image"},
    client_flavor={"type": "nova_flavor"},
)
@validation.add("required_services", services=[consts.Service.NOVA, consts.Service.NEUTRON])
@validation.add("image_valid_on_flavor", flavor_param="controller_flavor", image_param="controller_image")
@validation.add("image_valid_on_flavor", flavor_param="server_flavor", image_param="server_image")
@validation.add("image_valid_on_flavor", flavor_param="client_flavor", image_param="client_image")
@validation.add("required_platform", platform="openstack", users=True)
@scenario.configure(
    name="CIChurn.net_many_to_one",
    platform="openstack",
    context={"cleanup@openstack": ["nova", "neutron", "cinder"], "network@openstack": {}},
)
class NetManyToOneScenario(_NetTrafficBase):
    """Run one benchmark server against many clients."""

    def run(
        self,
        controller_image,
        controller_flavor,
        server_image,
        server_flavor,
        client_image,
        client_flavor,
        external_network_name,
        ssh_user="ubuntu",
        ssh_connect_timeout_seconds=300,
        command_timeout_seconds=0,
        client_count=8,
        mode="iperf3",
        protocols=None,
        duration_seconds=20,
        ramp_time_seconds=5,
        base_port=5201,
        flow_direction="server_to_client",
        parallel_streams=None,
        udp_target_mbps=None,
        server_volume_size_gib=2,
        server_volume_type=None,
        http_file_count=4,
        http_file_size_mib=128,
        artifacts_root_dir="artifacts",
    ):
        protocols = _as_str_list(protocols or ["tcp", "udp"])
        parallel_streams = _as_int_list(parallel_streams or [4])
        udp_target_mbps = _as_int_list(udp_target_mbps or [500])
        client_count = int(client_count)
        ssh_connect_timeout_seconds = int(ssh_connect_timeout_seconds)
        command_timeout_seconds = int(command_timeout_seconds)
        duration_seconds = int(duration_seconds)
        ramp_time_seconds = int(ramp_time_seconds)
        base_port = int(base_port)
        tenant_cidr = self._tenant_cidr()

        keypair = self._create_keypair()
        controller_sg = self._create_controller_security_group()
        benchmark_sg = self._create_benchmark_security_group(tenant_cidr)
        controller = None
        controller_fip = None
        server = None
        clients = []
        ssh = None
        volume_id = None
        artifacts_dir = self._artifacts_dir(artifacts_root_dir, "net-many-to-one")
        summary_payload = None
        controller_remote_dir = f"/var/lib/rally-netbench/run/{uuid.uuid4().hex}"

        try:
            controller, controller_fip = self._boot_controller(
                controller_image,
                controller_flavor,
                external_network_name,
                keypair["name"],
                controller_sg["name"],
            )
            server = self._boot_benchmark_vm(server_image, server_flavor, keypair["name"], benchmark_sg["name"])
            for _ in range(client_count):
                client = self._boot_benchmark_vm(client_image, client_flavor, keypair["name"], benchmark_sg["name"])
                clients.append({"name": client.name, "fixed_ip": self._fixed_ip(client), "server": client})

            if mode == "http_volume":
                volume = self._create_volume(int(server_volume_size_gib), server_volume_type)
                volume_id = volume.id
                self._attach_volume(server, volume.id, "/dev/vdb")

            ssh = self._ssh(controller_fip["ip"], ssh_user, keypair["private"], ssh_connect_timeout_seconds)
            matrix_cases = []
            if mode == "iperf3":
                for protocol in protocols:
                    if protocol == "tcp":
                        for streams in parallel_streams:
                            case_id = f"iperf3-tcp-clients-{client_count}-streams-{streams}"
                            matrix_cases.append(
                                {
                                    "case_id": case_id,
                                    "mode": "iperf3",
                                    "protocol": protocol,
                                    "parallel_streams": int(streams),
                                }
                            )
                    elif protocol == "udp":
                        for target in udp_target_mbps:
                            case_id = f"iperf3-udp-clients-{client_count}-target-{int(target)}m"
                            matrix_cases.append(
                                {
                                    "case_id": case_id,
                                    "mode": "iperf3",
                                    "protocol": protocol,
                                    "udp_target_mbps": int(target),
                                }
                            )
            else:
                matrix_cases.append({"case_id": "http-volume", "mode": "http_volume", "protocol": "http"})

            inventory = {
                "ssh_user": ssh_user,
                "server": {"name": server.name, "fixed_ip": self._fixed_ip(server)},
                "clients": [{"name": client["name"], "fixed_ip": client["fixed_ip"]} for client in clients],
            }
            matrix = {
                "scenario_slug": "net-many-to-one",
                "traffic": {
                    "mode": mode,
                    "protocols": protocols,
                    "duration_seconds": duration_seconds,
                    "ramp_time_seconds": ramp_time_seconds,
                    "base_port": base_port,
                    "http_port": HTTP_PORT,
                },
                "many_to_one": {
                    "client_count": client_count,
                    "flow_direction": flow_direction,
                },
                "http_volume": {
                    "file_count": int(http_file_count),
                    "file_size_mib": int(http_file_size_mib),
                },
                "cases": matrix_cases,
            }
            self._upload_controller_inputs(ssh, ssh_user, controller_remote_dir, inventory, matrix, keypair["private"])
            exit_status, stdout, stderr = self._run_controller_runner(ssh, controller_remote_dir, command_timeout_seconds)
            if exit_status != 0:
                raise rally_exceptions.ScriptError(
                    message=f"Controller netbench runner failed with exit status {exit_status}: {stderr or stdout}"
                )
            self._download_tree(ssh, controller_remote_dir, artifacts_dir)
            summary_payload = json.loads((artifacts_dir / "summary.json").read_text(encoding="utf-8"))
        finally:
            if ssh is not None:
                try:
                    ssh.close()
                except Exception:
                    pass
            if volume_id is not None and server is not None:
                self._detach_volume(server.id, volume_id)
            for client in clients:
                try:
                    self._delete_server(client["server"], force=True)
                except Exception:
                    pass
            if server is not None:
                try:
                    self._delete_server(server, force=True)
                except Exception:
                    pass
            if controller is not None and controller_fip is not None:
                try:
                    self._delete_server_with_fip(controller, controller_fip, force_delete=True)
                except Exception:
                    pass
            if volume_id is not None:
                self._delete_volume(volume_id)
            self._delete_security_group(benchmark_sg["id"])
            self._delete_security_group(controller_sg["id"])
            self._delete_keypair(keypair["name"])

        if summary_payload is None:
            raise rally_exceptions.ScriptError(message="network run did not produce a local summary.json artifact")
        self._emit_many_to_one_outputs(artifacts_dir, summary_payload)
        return self._result_payload("net_many_to_one", "CIChurn.net_many_to_one", artifacts_dir, summary_payload)

    def _emit_many_to_one_outputs(self, artifacts_dir: Path, summary_payload: dict[str, object]) -> None:
        rows = summary_payload.get("rows", [])
        summary = {
            "controller_nodes": 1,
            "server_nodes": 1,
            "client_nodes": len(summary_payload.get("inventory", {}).get("clients", [])),
            "matrix_cases": len(rows),
            "mode": summary_payload.get("matrix", {}).get("traffic", {}).get("mode", ""),
            "artifact_root": str(artifacts_dir),
        }
        throughput_values = [float(row.get("throughput_mbps", 0.0)) for row in rows if isinstance(row, dict)]
        retransmits = sum(float(row.get("retransmits", 0.0)) for row in rows if isinstance(row, dict))
        request_counts = [int(row.get("requests", 0)) for row in rows if isinstance(row, dict) and row.get("requests") is not None]
        p95_downloads = [float(row.get("p95_duration_seconds", 0.0)) for row in rows if isinstance(row, dict) and row.get("p95_duration_seconds") is not None]
        metrics = [
            ["best_throughput_mbps", str(round(max(throughput_values, default=0.0), 3))],
            ["median_throughput_mbps", str(round(float(summarize_numeric_series(throughput_values).get("p50", 0.0)), 3))],
            ["total_retransmits", str(round(retransmits, 3))],
            ["total_requests", str(sum(request_counts))],
            ["worst_p95_download_seconds", str(round(max(p95_downloads, default=0.0), 3))],
        ]
        table_rows = [
            [
                row.get("case_id", ""),
                row.get("mode", ""),
                row.get("protocol", ""),
                row.get("client_count", ""),
                row.get("throughput_mbps", ""),
                row.get("avg_client_mbps", ""),
                row.get("max_client_mbps", ""),
                row.get("retransmits", ""),
                row.get("jitter_ms", ""),
                row.get("lost_percent", ""),
                row.get("success_rate", ""),
                row.get("requests", ""),
                row.get("avg_duration_seconds", ""),
                row.get("p95_duration_seconds", ""),
                row.get("p99_duration_seconds", ""),
            ]
            for row in rows
            if isinstance(row, dict)
        ]
        self.add_output(complete=build_summary_output(summary))
        self.add_output(complete=build_metrics_output(metrics))
        self.add_output(
            complete=build_table_output(
                "Many-to-one summary",
                "Aggregated network benchmark results for the one-server-many-clients scenario",
                [
                    "case_id",
                    "mode",
                    "protocol",
                    "client_count",
                    "throughput_mbps",
                    "avg_client_mbps",
                    "max_client_mbps",
                    "retransmits",
                    "jitter_ms",
                    "lost_percent",
                    "success_rate",
                    "requests",
                    "avg_duration_seconds",
                    "p95_duration_seconds",
                    "p99_duration_seconds",
                ],
                table_rows,
            )
        )
        self.add_output(complete=build_phase_output(self.atomic_actions()))
        self.add_output(complete=build_artifacts_output(self._artifact_rows(artifacts_dir)))

    def _result_payload(
        self,
        family: str,
        name: str,
        artifacts_dir: Path,
        summary_payload: dict[str, object],
    ) -> dict[str, object]:
        return {
            "schema_version": 1,
            "scenario_family": family,
            "scenario_name": name,
            "status": "success",
            "summary": {
                "artifact_root": str(artifacts_dir),
                "row_count": len(summary_payload.get("rows", [])),
            },
            "metrics": {
                "rows": summary_payload.get("rows", []),
            },
            "timings": self._timings_payload(),
            "artifacts": {key: value for key, value in self._artifact_rows(artifacts_dir)},
            "diagnostics": {},
        }


@types.convert(
    controller_image={"type": "glance_image"},
    controller_flavor={"type": "nova_flavor"},
    participant_image={"type": "glance_image"},
    participant_flavor={"type": "nova_flavor"},
)
@validation.add("required_services", services=[consts.Service.NOVA, consts.Service.NEUTRON])
@validation.add("image_valid_on_flavor", flavor_param="controller_flavor", image_param="controller_image")
@validation.add("image_valid_on_flavor", flavor_param="participant_flavor", image_param="participant_image")
@validation.add("required_platform", platform="openstack", users=True)
@scenario.configure(
    name="CIChurn.net_ring",
    platform="openstack",
    context={"cleanup@openstack": ["nova", "neutron"], "network@openstack": {}},
)
class NetRingScenario(_NetTrafficBase):
    """Run east-west network traffic in a bounded ring topology."""

    def run(
        self,
        controller_image,
        controller_flavor,
        participant_image,
        participant_flavor,
        external_network_name,
        ssh_user="ubuntu",
        ssh_connect_timeout_seconds=300,
        command_timeout_seconds=0,
        participant_count=8,
        protocols=None,
        duration_seconds=20,
        ramp_time_seconds=5,
        base_port=5201,
        neighbors_per_vm=1,
        bidirectional=True,
        parallel_streams=None,
        udp_target_mbps=None,
        artifacts_root_dir="artifacts",
    ):
        protocols = _as_str_list(protocols or ["tcp", "udp"])
        parallel_streams = _as_int_list(parallel_streams or [4])
        udp_target_mbps = _as_int_list(udp_target_mbps or [300])
        participant_count = int(participant_count)
        ssh_connect_timeout_seconds = int(ssh_connect_timeout_seconds)
        command_timeout_seconds = int(command_timeout_seconds)
        duration_seconds = int(duration_seconds)
        ramp_time_seconds = int(ramp_time_seconds)
        base_port = int(base_port)
        neighbors_per_vm = int(neighbors_per_vm)
        tenant_cidr = self._tenant_cidr()

        keypair = self._create_keypair()
        controller_sg = self._create_controller_security_group()
        benchmark_sg = self._create_benchmark_security_group(tenant_cidr)
        controller = None
        controller_fip = None
        participants = []
        ssh = None
        artifacts_dir = self._artifacts_dir(artifacts_root_dir, "net-ring")
        summary_payload = None
        controller_remote_dir = f"/var/lib/rally-netbench/run/{uuid.uuid4().hex}"

        try:
            controller, controller_fip = self._boot_controller(
                controller_image,
                controller_flavor,
                external_network_name,
                keypair["name"],
                controller_sg["name"],
            )
            for _ in range(participant_count):
                participant = self._boot_benchmark_vm(
                    participant_image,
                    participant_flavor,
                    keypair["name"],
                    benchmark_sg["name"],
                )
                participants.append({"name": participant.name, "fixed_ip": self._fixed_ip(participant), "server": participant})

            ssh = self._ssh(controller_fip["ip"], ssh_user, keypair["private"], ssh_connect_timeout_seconds)
            matrix_cases = []
            for protocol in protocols:
                if protocol == "tcp":
                    for streams in parallel_streams:
                        case_id = f"ring-tcp-participants-{participant_count}-neighbors-{neighbors_per_vm}-streams-{streams}"
                        matrix_cases.append(
                            {
                                "case_id": case_id,
                                "protocol": protocol,
                                "neighbors_per_vm": neighbors_per_vm,
                                "bidirectional": bool(bidirectional),
                                "parallel_streams": int(streams),
                            }
                        )
                elif protocol == "udp":
                    for target in udp_target_mbps:
                        case_id = f"ring-udp-participants-{participant_count}-neighbors-{neighbors_per_vm}-target-{int(target)}m"
                        matrix_cases.append(
                            {
                                "case_id": case_id,
                                "protocol": protocol,
                                "neighbors_per_vm": neighbors_per_vm,
                                "bidirectional": bool(bidirectional),
                                "udp_target_mbps": int(target),
                            }
                        )
            inventory = {
                "ssh_user": ssh_user,
                "participants": [{"name": participant["name"], "fixed_ip": participant["fixed_ip"]} for participant in participants],
            }
            matrix = {
                "scenario_slug": "net-ring",
                "traffic": {
                    "protocols": protocols,
                    "duration_seconds": duration_seconds,
                    "ramp_time_seconds": ramp_time_seconds,
                    "base_port": base_port,
                },
                "ring": {
                    "participant_count": participant_count,
                    "neighbors_per_vm": neighbors_per_vm,
                    "bidirectional": bool(bidirectional),
                },
                "cases": matrix_cases,
            }
            self._upload_controller_inputs(ssh, ssh_user, controller_remote_dir, inventory, matrix, keypair["private"])
            exit_status, stdout, stderr = self._run_controller_runner(ssh, controller_remote_dir, command_timeout_seconds)
            if exit_status != 0:
                raise rally_exceptions.ScriptError(
                    message=f"Controller netbench runner failed with exit status {exit_status}: {stderr or stdout}"
                )
            self._download_tree(ssh, controller_remote_dir, artifacts_dir)
            summary_payload = json.loads((artifacts_dir / "summary.json").read_text(encoding="utf-8"))
        finally:
            if ssh is not None:
                try:
                    ssh.close()
                except Exception:
                    pass
            for participant in participants:
                try:
                    self._delete_server(participant["server"], force=True)
                except Exception:
                    pass
            if controller is not None and controller_fip is not None:
                try:
                    self._delete_server_with_fip(controller, controller_fip, force_delete=True)
                except Exception:
                    pass
            self._delete_security_group(benchmark_sg["id"])
            self._delete_security_group(controller_sg["id"])
            self._delete_keypair(keypair["name"])

        if summary_payload is None:
            raise rally_exceptions.ScriptError(message="network run did not produce a local summary.json artifact")
        self._emit_ring_outputs(artifacts_dir, summary_payload)
        return self._result_payload("net_ring", "CIChurn.net_ring", artifacts_dir, summary_payload)

    def _emit_ring_outputs(self, artifacts_dir: Path, summary_payload: dict[str, object]) -> None:
        rows = summary_payload.get("rows", [])
        summary = {
            "controller_nodes": 1,
            "participant_nodes": len(summary_payload.get("inventory", {}).get("participants", [])),
            "matrix_cases": len(rows),
            "artifact_root": str(artifacts_dir),
        }
        throughput_values = [float(row.get("throughput_mbps", 0.0)) for row in rows if isinstance(row, dict)]
        imbalance_values = [float(row.get("imbalance_ratio", 1.0)) for row in rows if isinstance(row, dict)]
        metrics = [
            ["best_throughput_mbps", str(round(max(throughput_values, default=0.0), 3))],
            ["median_throughput_mbps", str(round(float(summarize_numeric_series(throughput_values).get("p50", 0.0)), 3))],
            ["worst_imbalance_ratio", str(round(max(imbalance_values, default=1.0), 3))],
        ]
        table_rows = [
            [
                row.get("case_id", ""),
                row.get("protocol", ""),
                row.get("participant_count", ""),
                row.get("flow_count", ""),
                row.get("throughput_mbps", ""),
                row.get("avg_flow_mbps", ""),
                row.get("max_flow_mbps", ""),
                row.get("retransmits", ""),
                row.get("jitter_ms", ""),
                row.get("lost_percent", ""),
                row.get("imbalance_ratio", ""),
            ]
            for row in rows
            if isinstance(row, dict)
        ]
        self.add_output(complete=build_summary_output(summary))
        self.add_output(complete=build_metrics_output(metrics))
        self.add_output(
            complete=build_table_output(
                "Ring summary",
                "Aggregated east-west network benchmark results for the bounded ring scenario",
                [
                    "case_id",
                    "protocol",
                    "participant_count",
                    "flow_count",
                    "throughput_mbps",
                    "avg_flow_mbps",
                    "max_flow_mbps",
                    "retransmits",
                    "jitter_ms",
                    "lost_percent",
                    "imbalance_ratio",
                ],
                table_rows,
            )
        )
        self.add_output(complete=build_phase_output(self.atomic_actions()))
        self.add_output(complete=build_artifacts_output(self._artifact_rows(artifacts_dir)))

    def _result_payload(
        self,
        family: str,
        name: str,
        artifacts_dir: Path,
        summary_payload: dict[str, object],
    ) -> dict[str, object]:
        return {
            "schema_version": 1,
            "scenario_family": family,
            "scenario_name": name,
            "status": "success",
            "summary": {
                "artifact_root": str(artifacts_dir),
                "row_count": len(summary_payload.get("rows", [])),
            },
            "metrics": {
                "rows": summary_payload.get("rows", []),
            },
            "timings": self._timings_payload(),
            "artifacts": {key: value for key, value in self._artifact_rows(artifacts_dir)},
            "diagnostics": {},
        }

"""Mixed cloud pressure benchmark scenario."""

from __future__ import annotations

import threading
import csv
import json
import shutil
import tempfile
import time
import uuid
from importlib import resources
from pathlib import Path

from rally import exceptions as rally_exceptions
from rally.task import atomic
from rally.task import types
from rally.task import validation

from rally_openstack.common import consts
from rally_openstack.task import scenario

from rally_ci_churn.plugins.autonomous_vm import _AutonomousVMBase
from rally_ci_churn.plugins.controller_runtime import ControllerRuntimeBase
from rally_ci_churn.plugins.controller_runtime import SSH_PORT
from rally_ci_churn.plugins.controller_runtime import build_root_volume_boot
from rally_ci_churn.results import build_artifacts_output
from rally_ci_churn.results import build_metrics_output
from rally_ci_churn.results import build_phase_output
from rally_ci_churn.results import build_summary_output
from rally_ci_churn.results import build_table_output
from rally_ci_churn.results import summarize_numeric_series


DEVICE_DISCOVERY_TIMEOUT_SECONDS = 600
DEVICE_POLL_INTERVAL_SECONDS = 2.0
WORKER_READY_TIMEOUT_SECONDS = 600


def _as_int_list(values: list[object]) -> list[int]:
    return [int(value) for value in values]


def _as_str_list(values: list[object]) -> list[str]:
    return [str(value) for value in values]


class _MixedPressureBase(ControllerRuntimeBase, _AutonomousVMBase):
    def _controller_user_data(self) -> str:
        return """#cloud-config
write_files:
  - path: /etc/rally-ci-mixed-role
    permissions: "0644"
    content: |
      role=controller
runcmd:
  - [ cloud-init-per, once, rally-mixed-controller-ready, /bin/sh, -c, "mkdir -p /var/lib/rally-mixed/run /var/lib/rally-netbench/run /var/lib/rally-fio/run && chown -R ubuntu:ubuntu /var/lib/rally-mixed /var/lib/rally-netbench /var/lib/rally-fio" ]
"""

    def _net_benchmark_user_data(self) -> str:
        return """#cloud-config
write_files:
  - path: /etc/rally-ci-net-role
    permissions: "0644"
    content: |
      role=benchmark
runcmd:
  - [ cloud-init-per, once, rally-netbench-host-ready, /bin/sh, -c, "mkdir -p /var/lib/rally-netbench && chown -R ubuntu:ubuntu /var/lib/rally-netbench" ]
"""

    def _build_fio_worker_user_data(self, expected_volumes: int, fio_port: int) -> str:
        script = f"""#!/bin/bash
set -euo pipefail
expected_volumes="{expected_volumes}"
fio_port="{fio_port}"
mkdir -p /var/lib/rally-fio/devices
start_ts=$(date +%s)
while true; do
  root_source=$(findmnt -n -o SOURCE / || true)
  root_pkname=$(lsblk -no PKNAME "$root_source" 2>/dev/null || true)
  root_disk=""
  if [ -n "$root_pkname" ]; then
    root_disk="/dev/$root_pkname"
  fi
  mapfile -t disks < <(lsblk -dnpo NAME,TYPE | awk '$2=="disk" {{print $1}}')
  data_disks=()
  for disk in "${{disks[@]}}"; do
    if [ -n "$root_disk" ] && [ "$disk" = "$root_disk" ]; then
      continue
    fi
    data_disks+=("$disk")
  done
  if [ "${{#data_disks[@]}}" -ge "$expected_volumes" ]; then
    break
  fi
  if [ $(( $(date +%s) - start_ts )) -ge {DEVICE_DISCOVERY_TIMEOUT_SECONDS} ]; then
    echo "Timed out waiting for attached data volumes" >&2
    exit 1
  fi
  sleep {DEVICE_POLL_INTERVAL_SECONDS}
done
rm -f /var/lib/rally-fio/devices/vol*
for index in $(seq 1 "$expected_volumes"); do
  disk="${{data_disks[$((index - 1))]}}"
  ln -sfn "$disk" "/var/lib/rally-fio/devices/vol$(printf '%02d' "$index")"
done
pkill -f 'fio --server' || true
exec fio --server=",${{fio_port}}" --daemonize=/var/run/rally-fio-server.pid
"""
        return f"""#cloud-config
write_files:
  - path: /usr/local/bin/rally-fio-worker.sh
    permissions: "0755"
    content: |
{chr(10).join(f'      {line}' for line in script.splitlines())}
  - path: /etc/rally-ci-fio-role
    permissions: "0644"
    content: |
      role=worker
runcmd:
  - [ cloud-init-per, once, rally-fio-worker-start, /bin/bash, -lc, "/usr/local/bin/rally-fio-worker.sh" ]
"""

    @atomic.action_timer("controller.boot")
    def _boot_controller(
        self,
        image,
        flavor,
        external_network_name: str,
        key_name: str,
        security_group_name: str,
        boot_from_volume: bool = False,
        root_volume_size_gib: int = 20,
        root_volume_type: str | None = None,
    ):
        boot_image, boot_kwargs = build_root_volume_boot(
            image,
            enabled=boot_from_volume,
            volume_size_gib=root_volume_size_gib,
            volume_type=root_volume_type,
        )
        return self._boot_server_with_fip(
            boot_image,
            flavor,
            use_floating_ip=True,
            floating_network=external_network_name,
            key_name=key_name,
            security_groups=[security_group_name],
            userdata=self._controller_user_data(),
            **boot_kwargs,
        )

    @atomic.action_timer("benchmark.boot")
    def _boot_benchmark_vm(
        self,
        image,
        flavor,
        key_name: str,
        security_group_name: str,
        boot_from_volume: bool = False,
        root_volume_size_gib: int = 20,
        root_volume_type: str | None = None,
    ):
        return self._boot_benchmark_vm_raw(
            image,
            flavor,
            key_name,
            security_group_name,
            boot_from_volume=boot_from_volume,
            root_volume_size_gib=root_volume_size_gib,
            root_volume_type=root_volume_type,
        )

    def _boot_benchmark_vm_raw(
        self,
        image,
        flavor,
        key_name: str,
        security_group_name: str,
        boot_from_volume: bool = False,
        root_volume_size_gib: int = 20,
        root_volume_type: str | None = None,
    ):
        boot_image, boot_kwargs = build_root_volume_boot(
            image,
            enabled=boot_from_volume,
            volume_size_gib=root_volume_size_gib,
            volume_type=root_volume_type,
        )
        return self._boot_server(
            boot_image,
            flavor,
            auto_assign_nic=True,
            key_name=key_name,
            security_groups=[security_group_name],
            userdata=self._net_benchmark_user_data(),
            **boot_kwargs,
        )

    @atomic.action_timer("fio.worker.boot")
    def _boot_fio_worker(
        self,
        image,
        flavor,
        key_name: str,
        security_group_name: str,
        expected_volumes: int,
        fio_port: int,
        boot_from_volume: bool = False,
        root_volume_size_gib: int = 20,
        root_volume_type: str | None = None,
    ):
        return self._boot_fio_worker_raw(
            image,
            flavor,
            key_name,
            security_group_name,
            expected_volumes,
            fio_port,
            boot_from_volume=boot_from_volume,
            root_volume_size_gib=root_volume_size_gib,
            root_volume_type=root_volume_type,
        )

    def _boot_fio_worker_raw(
        self,
        image,
        flavor,
        key_name: str,
        security_group_name: str,
        expected_volumes: int,
        fio_port: int,
        boot_from_volume: bool = False,
        root_volume_size_gib: int = 20,
        root_volume_type: str | None = None,
    ):
        boot_image, boot_kwargs = build_root_volume_boot(
            image,
            enabled=boot_from_volume,
            volume_size_gib=root_volume_size_gib,
            volume_type=root_volume_type,
        )
        return self._boot_server(
            boot_image,
            flavor,
            auto_assign_nic=True,
            key_name=key_name,
            security_groups=[security_group_name],
            userdata=self._build_fio_worker_user_data(expected_volumes, fio_port),
            **boot_kwargs,
        )

    def _create_fio_worker_security_group(self, tenant_cidr: str, fio_port: int) -> dict[str, object]:
        security_group = self._create_security_group(
            self.generate_random_name(),
            "Allow fio server access from the tenant network",
        )
        self._add_security_group_rule(security_group["id"], "tcp", fio_port, fio_port, tenant_cidr)
        self._add_security_group_rule(security_group["id"], "tcp", SSH_PORT, SSH_PORT, tenant_cidr)
        self._add_security_group_rule(security_group["id"], "icmp", remote_ip_prefix=tenant_cidr)
        return security_group

    @atomic.action_timer("worker.wait_ready")
    def _wait_for_worker_fio_ready(self, ssh, fixed_ip: str, fio_port: int) -> None:
        deadline = time.monotonic() + WORKER_READY_TIMEOUT_SECONDS
        python_snippet = (
            "import socket, sys; "
            "sock = socket.create_connection((sys.argv[1], int(sys.argv[2])), timeout=2); "
            "sock.close()"
        )
        while time.monotonic() < deadline:
            exit_status, _ = ssh.run(
                ["python3", "-c", python_snippet, fixed_ip, str(fio_port)],
                timeout=10,
                raise_on_error=False,
            )
            if exit_status == 0:
                return
            time.sleep(DEVICE_POLL_INTERVAL_SECONDS)
        raise rally_exceptions.ScriptError(
            message=f"Worker fio server {fixed_ip}:{fio_port} did not become ready before timeout"
        )

    def _validate_burst_windows(self, burst_windows: list[dict[str, object]]) -> list[dict[str, object]]:
        normalized = []
        previous_end = 0
        for raw_window in sorted(burst_windows, key=lambda item: int(item.get("start_second", 0))):
            start_second = int(raw_window.get("start_second", 0))
            end_second = int(raw_window.get("end_second", 0))
            multiplier = float(raw_window.get("launch_rate_multiplier", 1.0))
            if start_second < 0 or end_second <= start_second:
                raise rally_exceptions.ScriptError(message=f"Invalid burst window: {raw_window}")
            if start_second < previous_end:
                raise rally_exceptions.ScriptError(message=f"Overlapping burst windows: {raw_window}")
            if multiplier < 0:
                raise rally_exceptions.ScriptError(message=f"Burst multiplier must be >= 0: {raw_window}")
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

    @atomic.action_timer("controller.upload_inputs")
    def _upload_mixed_controller_inputs(
        self,
        ssh,
        ssh_user: str,
        controller_remote_dir: str,
        config: dict[str, object],
        fio_inventory: dict[str, object],
        fio_matrix: dict[str, object],
        many_inventory: dict[str, object],
        many_matrix: dict[str, object],
        ring_inventory: dict[str, object],
        ring_matrix: dict[str, object],
        private_key: str,
    ) -> None:
        ssh.execute(
            ["sudo", "install", "-d", "-o", ssh_user, "-g", ssh_user, "-m", "0755", controller_remote_dir],
            timeout=300,
        )
        subdirs = [
            controller_remote_dir,
            f"{controller_remote_dir}/fio",
            f"{controller_remote_dir}/net-many-to-one",
            f"{controller_remote_dir}/net-ring",
            f"{controller_remote_dir}/artifacts",
            f"{controller_remote_dir}/artifacts/fio",
            f"{controller_remote_dir}/artifacts/net-many-to-one",
            f"{controller_remote_dir}/artifacts/net-ring",
        ]
        ssh.execute(["mkdir", "-p", *subdirs], timeout=300)
        fio_runner = resources.files("rally_ci_churn.fio").joinpath("controller_runner.py")
        net_runner = resources.files("rally_ci_churn.net").joinpath("controller_runner.py")
        mixed_runner = resources.files("rally_ci_churn.mixed").joinpath("controller_runner.py")
        with tempfile.TemporaryDirectory(prefix="rally-mixed-controller-") as temp_dir:
            temp_path = Path(temp_dir)
            artifacts_root = temp_path / "artifacts"
            (artifacts_root / "net-many-to-one").mkdir(parents=True, exist_ok=True)
            (artifacts_root / "net-ring").mkdir(parents=True, exist_ok=True)
            files = {
                "config.json": json.dumps(config, indent=2, sort_keys=True),
                "fio/inventory.json": json.dumps(fio_inventory, indent=2, sort_keys=True),
                "fio/matrix.json": json.dumps(fio_matrix, indent=2, sort_keys=True),
                "net-many-to-one/inventory.json": json.dumps(many_inventory, indent=2, sort_keys=True),
                "net-many-to-one/matrix.json": json.dumps(many_matrix, indent=2, sort_keys=True),
                "net-ring/inventory.json": json.dumps(ring_inventory, indent=2, sort_keys=True),
                "net-ring/matrix.json": json.dumps(ring_matrix, indent=2, sort_keys=True),
                "fio_controller_runner.py": fio_runner.read_text(encoding="utf-8"),
                "net_controller_runner.py": net_runner.read_text(encoding="utf-8"),
                "mixed_controller_runner.py": mixed_runner.read_text(encoding="utf-8"),
                "id_rsa": private_key,
                "artifacts/net-many-to-one/id_rsa": private_key,
                "artifacts/net-ring/id_rsa": private_key,
            }
            for relative_path, content in files.items():
                local_path = temp_path / relative_path
                local_path.parent.mkdir(parents=True, exist_ok=True)
                local_path.write_text(content, encoding="utf-8")
                mode = 0o600 if relative_path.endswith("id_rsa") else 0o755 if relative_path.endswith(".py") else 0o644
                ssh.put_file(str(local_path), f"{controller_remote_dir}/{relative_path}", mode=mode)

    def _start_remote_component_runner(
        self,
        controller_ip: str,
        ssh_user: str,
        private_key: str,
        ssh_connect_timeout_seconds: int,
        command: list[str],
        command_timeout_seconds: int,
    ):
        state: dict[str, object] = {
            "done": False,
            "exit_status": None,
            "stdout": "",
            "stderr": "",
            "error": None,
        }

        def _runner() -> None:
            remote_ssh = None
            try:
                remote_ssh = self._ssh(
                    controller_ip,
                    ssh_user,
                    private_key,
                    ssh_connect_timeout_seconds,
                )
                exit_status, stdout_output, stderr_output = remote_ssh.execute(
                    command,
                    timeout=command_timeout_seconds or 86400,
                )
                state["exit_status"] = exit_status
                state["stdout"] = stdout_output
                state["stderr"] = stderr_output
            except Exception as exc:  # noqa: BLE001
                state["error"] = exc
            finally:
                if remote_ssh is not None:
                    try:
                        remote_ssh.close()
                    except Exception:
                        pass
                state["done"] = True

        thread = threading.Thread(target=_runner, name=f"mixed-component-{'-'.join(command[:2])}", daemon=True)
        thread.start()
        return thread, state

    @atomic.action_timer("benchmark.wait_ready")
    def _wait_for_controller_ssh_targets(
        self,
        ssh,
        remote_key_path: str,
        ssh_user: str,
        hosts: list[str],
        timeout_seconds: int = 600,
    ) -> None:
        deadline = time.monotonic() + timeout_seconds
        pending = set(hosts)
        while pending and time.monotonic() < deadline:
            for host in list(pending):
                exit_status, _ = ssh.run(
                    [
                        "ssh",
                        "-i",
                        remote_key_path,
                        "-o",
                        "StrictHostKeyChecking=no",
                        "-o",
                        "UserKnownHostsFile=/dev/null",
                        "-o",
                        "BatchMode=yes",
                        "-o",
                        "ConnectTimeout=5",
                        f"{ssh_user}@{host}",
                        "true",
                    ],
                    timeout=15,
                    raise_on_error=False,
                )
                if exit_status == 0:
                    pending.remove(host)
            if pending:
                time.sleep(2.0)
        if pending:
            raise rally_exceptions.ScriptError(
                message=f"Timed out waiting for controller SSH reachability to {sorted(pending)}"
            )

    def _write_component_csv(self, path: Path, rows: list[dict[str, object]], fieldnames: list[str]) -> None:
        with path.open("w", encoding="utf-8", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow({key: row.get(key, "") for key in fieldnames})

    def _write_churn_artifacts(self, churn_dir: Path, rows: list[dict[str, object]], summary: dict[str, object]) -> None:
        churn_dir.mkdir(parents=True, exist_ok=True)
        payload = {"summary": summary, "rows": rows}
        (churn_dir / "summary.json").write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        self._write_component_csv(
            churn_dir / "summary.csv",
            rows,
            ["server", "status", "duration_seconds", "artifact", "error", "wave", "iteration"],
        )
        lines = [
            "## VM churn summary",
            "",
            "| Server | Status | Duration (s) | Artifact | Error |",
            "|--------|--------|--------------|----------|-------|",
        ]
        for row in rows:
            lines.append(
                f"| {row['server']} | {row['status']} | {row['duration_seconds']} | {row['artifact']} | {row['error']} |"
            )
        (churn_dir / "summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")

    def _build_top_level_summary(
        self,
        artifacts_dir: Path,
        churn_summary: dict[str, object],
        fio_summary: dict[str, object],
        many_summary: dict[str, object],
        ring_summary: dict[str, object],
    ) -> tuple[dict[str, object], list[list[object]], list[list[object]]]:
        fio_rows = [row for row in fio_summary.get("rows", []) if isinstance(row, dict)]
        many_rows = [row for row in many_summary.get("rows", []) if isinstance(row, dict)]
        ring_rows = [row for row in ring_summary.get("rows", []) if isinstance(row, dict)]
        fio_best = max((float(row.get("throughput_bytes_per_sec", 0.0)) for row in fio_rows), default=0.0)
        fio_median = float(summarize_numeric_series([float(row.get("throughput_bytes_per_sec", 0.0)) for row in fio_rows]).get("p50", 0.0))
        fio_worst_p99 = max((float(row.get("p99_latency_ms", 0.0)) for row in fio_rows), default=0.0)
        many_best = max((float(row.get("throughput_mbps", 0.0)) for row in many_rows), default=0.0)
        many_retransmits = sum(float(row.get("retransmits", 0.0)) for row in many_rows)
        ring_best = max((float(row.get("throughput_mbps", 0.0)) for row in ring_rows), default=0.0)
        ring_imbalance = max((float(row.get("imbalance_ratio", 0.0)) for row in ring_rows), default=0.0)

        summary = {
            "controller_fips": 1,
            "artifact_root": str(artifacts_dir),
            "fio_cases": len(fio_rows),
            "many_to_one_cases": len(many_rows),
            "ring_cases": len(ring_rows),
            "churn_launched_vms": churn_summary.get("launched_vms", 0),
            "churn_completed_vms": churn_summary.get("completed_vms", 0),
            "churn_failed_vms": churn_summary.get("failed_vms", 0),
            "churn_timed_out_vms": churn_summary.get("timed_out_vms", 0),
            "churn_peak_active_vms": churn_summary.get("peak_active_vms", 0),
        }
        metrics = [
            ["fio_best_throughput_bytes_per_sec", str(round(fio_best, 3))],
            ["fio_median_throughput_bytes_per_sec", str(round(fio_median, 3))],
            ["fio_worst_p99_latency_ms", str(round(fio_worst_p99, 3))],
            ["many_to_one_best_throughput_mbps", str(round(many_best, 3))],
            ["many_to_one_total_retransmits", str(round(many_retransmits, 3))],
            ["ring_best_throughput_mbps", str(round(ring_best, 3))],
            ["ring_worst_imbalance_ratio", str(round(ring_imbalance, 3))],
        ]
        component_rows = [
            [
                "vm-churn",
                "success" if int(churn_summary.get("failed_vms", 0)) == 0 and int(churn_summary.get("timed_out_vms", 0)) == 0 else "degraded",
                churn_summary.get("launched_vms", 0),
                churn_summary.get("completed_vms", 0),
                churn_summary.get("failed_vms", 0),
                churn_summary.get("timed_out_vms", 0),
                churn_summary.get("peak_active_vms", 0),
                "",
            ],
            [
                "fio",
                "success",
                len(fio_rows),
                "",
                "",
                "",
                "",
                f"best_bw={round(fio_best, 3)}B/s p99={round(fio_worst_p99, 3)}ms",
            ],
            [
                "net-many-to-one",
                "success",
                len(many_rows),
                "",
                "",
                "",
                "",
                f"best_bw={round(many_best, 3)}Mbps retransmits={round(many_retransmits, 3)}",
            ],
            [
                "net-ring",
                "success",
                len(ring_rows),
                "",
                "",
                "",
                "",
                f"best_bw={round(ring_best, 3)}Mbps imbalance={round(ring_imbalance, 3)}",
            ],
        ]
        return summary, metrics, component_rows

    def _write_top_level_artifacts(
        self,
        artifacts_dir: Path,
        summary: dict[str, object],
        metric_rows: list[list[object]],
        component_rows: list[list[object]],
    ) -> None:
        payload = {
            "summary": summary,
            "metrics": {key: value for key, value in metric_rows},
            "components": [
                {
                    "component": row[0],
                    "status": row[1],
                    "count": row[2],
                    "completed": row[3],
                    "failed": row[4],
                    "timed_out": row[5],
                    "peak_active": row[6],
                    "details": row[7],
                }
                for row in component_rows
            ],
        }
        (artifacts_dir / "summary.json").write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        with (artifacts_dir / "summary.csv").open("w", encoding="utf-8", newline="") as stream:
            writer = csv.writer(stream)
            writer.writerow(["component", "status", "count", "completed", "failed", "timed_out", "peak_active", "details"])
            writer.writerows(component_rows)
        lines = [
            "## Mixed pressure summary",
            "",
            "| Component | Status | Count | Completed | Failed | Timed out | Peak active | Details |",
            "|-----------|--------|-------|-----------|--------|-----------|-------------|---------|",
        ]
        for row in component_rows:
            lines.append(
                f"| {row[0]} | {row[1]} | {row[2]} | {row[3]} | {row[4]} | {row[5]} | {row[6]} | {row[7]} |"
            )
        (artifacts_dir / "summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
        manifest = {
            "summary_json": str(artifacts_dir / "summary.json"),
            "summary_csv": str(artifacts_dir / "summary.csv"),
            "summary_md": str(artifacts_dir / "summary.md"),
            "components": {
                "fio": str(artifacts_dir / "fio"),
                "net_many_to_one": str(artifacts_dir / "net-many-to-one"),
                "net_ring": str(artifacts_dir / "net-ring"),
                "vm_churn": str(artifacts_dir / "vm-churn"),
            },
        }
        (artifacts_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")


@types.convert(
    controller_image={"type": "glance_image"},
    net_image={"type": "glance_image"},
    fio_worker_image={"type": "glance_image"},
    churn_image={"type": "glance_image"},
    controller_flavor={"type": "nova_flavor"},
    fixed_group_flavor={"type": "nova_flavor"},
    churn_flavor={"type": "nova_flavor"},
)
@validation.add(
    "required_services",
    services=[consts.Service.NOVA, consts.Service.NEUTRON, consts.Service.CINDER],
)
@validation.add("image_valid_on_flavor", flavor_param="controller_flavor", image_param="controller_image")
@validation.add("image_valid_on_flavor", flavor_param="fixed_group_flavor", image_param="net_image")
@validation.add("image_valid_on_flavor", flavor_param="fixed_group_flavor", image_param="fio_worker_image")
@validation.add("image_valid_on_flavor", flavor_param="churn_flavor", image_param="churn_image")
@validation.add("required_platform", platform="openstack", users=True)
@scenario.configure(
    name="CIChurn.mixed_pressure",
    platform="openstack",
    context={"cleanup@openstack": ["nova", "neutron", "cinder"], "network@openstack": {}},
)
class MixedPressureScenario(_MixedPressureBase):
    """Run fio, network traffic, and spiky stress-ng churn concurrently."""

    def run(
        self,
        controller_image,
        net_image,
        fio_worker_image,
        churn_image,
        controller_flavor,
        fixed_group_flavor,
        churn_flavor,
        external_network_name,
        ssh_user="ubuntu",
        ssh_connect_timeout_seconds=300,
        command_timeout_seconds=0,
        boot_from_volume=False,
        root_volume_size_gib=20,
        root_volume_type=None,
        boot_concurrency=1,
        duration_seconds=90,
        subbenchmark_failure_mode="fail",
        artifact_container="rally-ci-churn",
        artifact_ttl_seconds=0,
        swift_auth_url="",
        swift_username="",
        swift_password="",
        swift_project_name="",
        swift_user_domain_name="",
        swift_project_domain_name="",
        swift_interface="public",
        swift_region_name="",
        swift_cacert_b64="",
        max_active_vms=2,
        baseline_launches_per_minute=2,
        burst_windows=None,
        launch_tick_seconds=1,
        churn_timeout_seconds=3600,
        churn_timeout_mode="fail",
        churn_workload_params=None,
        fio_volume_size_gib=2,
        fio_volume_type=None,
        fio_client_counts=None,
        fio_volumes_per_client=None,
        fio_profile_names=None,
        fio_rw_modes=None,
        fio_block_sizes=None,
        fio_numjobs=None,
        fio_iodepths=None,
        fio_runtime_seconds=60,
        fio_ramp_time_seconds=5,
        fio_port=8765,
        fio_ioengine="io_uring",
        many_client_count=2,
        many_mode="iperf3",
        many_protocols=None,
        many_duration_seconds=60,
        many_ramp_time_seconds=5,
        many_base_port=5201,
        many_flow_direction="server_to_client",
        many_parallel_streams=None,
        many_udp_target_mbps=None,
        many_server_volume_size_gib=2,
        many_server_volume_type=None,
        many_http_file_count=4,
        many_http_file_size_mib=128,
        ring_participant_count=3,
        ring_protocols=None,
        ring_duration_seconds=60,
        ring_ramp_time_seconds=5,
        ring_base_port=6201,
        ring_neighbors_per_vm=1,
        ring_bidirectional=True,
        ring_parallel_streams=None,
        ring_udp_target_mbps=None,
        console_log_length=400,
        artifacts_root_dir="artifacts",
        force_delete=False,
        wave=0,
        **kwargs,
    ):
        churn_workload_params = dict(churn_workload_params or {})
        fio_client_counts = _as_int_list(fio_client_counts or [1])
        fio_volumes_per_client = _as_int_list(fio_volumes_per_client or [1])
        fio_profile_names = _as_str_list(fio_profile_names or ["mixed-workload"])
        fio_rw_modes = _as_str_list(fio_rw_modes or ["write"])
        fio_block_sizes = _as_str_list(fio_block_sizes or ["1M"])
        fio_numjobs = _as_int_list(fio_numjobs or [1])
        fio_iodepths = _as_int_list(fio_iodepths or [1])
        many_protocols = _as_str_list(many_protocols or ["tcp"])
        many_parallel_streams = _as_int_list(many_parallel_streams or [2])
        many_udp_target_mbps = _as_int_list(many_udp_target_mbps or [500])
        ring_protocols = _as_str_list(ring_protocols or ["tcp"])
        ring_parallel_streams = _as_int_list(ring_parallel_streams or [2])
        ring_udp_target_mbps = _as_int_list(ring_udp_target_mbps or [300])
        burst_windows = self._validate_burst_windows(burst_windows or [])
        duration_seconds = int(duration_seconds)
        max_active_vms = int(max_active_vms)
        launch_tick_seconds = max(1, int(launch_tick_seconds))
        baseline_launches_per_minute = float(baseline_launches_per_minute)
        ssh_connect_timeout_seconds = int(ssh_connect_timeout_seconds)
        command_timeout_seconds = int(command_timeout_seconds)
        root_volume_size_gib = int(root_volume_size_gib)
        fio_port = int(fio_port)
        boot_concurrency = int(boot_concurrency)
        tenant_cidr = self._tenant_cidr()
        if duration_seconds <= 0:
            raise rally_exceptions.ScriptError(message="duration_seconds must be > 0")

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

        keypair = self._create_keypair()
        controller_sg = self._create_controller_security_group()
        benchmark_sg = self._create_benchmark_security_group(tenant_cidr)
        fio_worker_sg = self._create_fio_worker_security_group(tenant_cidr, fio_port)
        controller = None
        controller_fip = None
        many_server = None
        many_clients: list[dict[str, object]] = []
        ring_participants: list[dict[str, object]] = []
        fio_workers: list[dict[str, object]] = []
        fio_volumes: list[str] = []
        fio_attachments: list[dict[str, str]] = []
        ssh = None
        fixed_component_states: dict[str, dict[str, object]] = {}
        artifacts_dir = self._artifacts_dir(artifacts_root_dir, "mixed-pressure")
        controller_remote_dir = f"/var/lib/rally-mixed/run/{uuid.uuid4().hex}"
        active_churn_vms: dict[str, dict[str, object]] = {}
        churn_rows: list[dict[str, object]] = []
        churn_summary = {
            "duration_seconds": duration_seconds,
            "launched_vms": 0,
            "completed_vms": 0,
            "failed_vms": 0,
            "timed_out_vms": 0,
            "dropped_launches": 0,
            "peak_active_vms": 0,
        }
        errors: list[str] = []

        try:
            controller, controller_fip = self._boot_controller(
                controller_image,
                controller_flavor,
                external_network_name,
                keypair["name"],
                controller_sg["name"],
                boot_from_volume=bool(boot_from_volume),
                root_volume_size_gib=root_volume_size_gib,
                root_volume_type=root_volume_type,
            )
            many_server = self._boot_benchmark_vm(
                net_image,
                fixed_group_flavor,
                keypair["name"],
                benchmark_sg["name"],
                boot_from_volume=bool(boot_from_volume),
                root_volume_size_gib=root_volume_size_gib,
                root_volume_type=root_volume_type,
            )
            def _boot_many_client_record(_index: int) -> dict[str, object]:
                client = self._boot_benchmark_vm_raw(
                    net_image,
                    fixed_group_flavor,
                    keypair["name"],
                    benchmark_sg["name"],
                    boot_from_volume=bool(boot_from_volume),
                    root_volume_size_gib=root_volume_size_gib,
                    root_volume_type=root_volume_type,
                )
                return {"server": client, "fixed_ip": self._fixed_ip(client), "name": client.name}

            self._boot_vm_group(
                count=int(many_client_count),
                concurrency=boot_concurrency,
                atomic_action_name="many.client.boot_group",
                boot_fn=_boot_many_client_record,
                destination=many_clients,
            )

            def _boot_ring_participant_record(_index: int) -> dict[str, object]:
                participant = self._boot_benchmark_vm_raw(
                    net_image,
                    fixed_group_flavor,
                    keypair["name"],
                    benchmark_sg["name"],
                    boot_from_volume=bool(boot_from_volume),
                    root_volume_size_gib=root_volume_size_gib,
                    root_volume_type=root_volume_type,
                )
                return {
                    "server": participant,
                    "fixed_ip": self._fixed_ip(participant),
                    "name": participant.name,
                }

            self._boot_vm_group(
                count=int(ring_participant_count),
                concurrency=boot_concurrency,
                atomic_action_name="ring.participant.boot_group",
                boot_fn=_boot_ring_participant_record,
                destination=ring_participants,
            )

            max_fio_clients = max(fio_client_counts)
            max_fio_volumes = max(fio_volumes_per_client)
            def _boot_fio_worker_record(_index: int) -> dict[str, object]:
                worker = self._boot_fio_worker_raw(
                    fio_worker_image,
                    fixed_group_flavor,
                    keypair["name"],
                    fio_worker_sg["name"],
                    max_fio_volumes,
                    fio_port,
                    boot_from_volume=bool(boot_from_volume),
                    root_volume_size_gib=root_volume_size_gib,
                    root_volume_type=root_volume_type,
                )
                return {"server": worker, "fixed_ip": self._fixed_ip(worker), "name": worker.name}

            self._boot_vm_group(
                count=max_fio_clients,
                concurrency=boot_concurrency,
                atomic_action_name="fio.worker.boot_group",
                boot_fn=_boot_fio_worker_record,
                destination=fio_workers,
            )
            device_letters = "bcdefghijklmnopqrstuvwxyz"
            for worker in fio_workers:
                for volume_index in range(max_fio_volumes):
                    volume = self._create_volume(int(fio_volume_size_gib), fio_volume_type)
                    fio_volumes.append(volume.id)
                    device_name = f"/dev/vd{device_letters[volume_index]}"
                    self._attach_volume(worker["server"], volume.id, device_name)
                    fio_attachments.append({"server_id": worker["server"].id, "volume_id": volume.id})

            ssh = self._ssh(controller_fip["ip"], ssh_user, keypair["private"], ssh_connect_timeout_seconds)
            for worker in fio_workers:
                self._wait_for_worker_fio_ready(ssh, str(worker["fixed_ip"]), fio_port)

            fio_inventory = {
                "fio_port": fio_port,
                "workers": [
                    {
                        "name": worker["name"],
                        "fixed_ip": worker["fixed_ip"],
                        "devices": [f"/var/lib/rally-fio/devices/vol{index + 1:02d}" for index in range(max_fio_volumes)],
                    }
                    for worker in fio_workers
                ],
            }
            fio_cases = []
            builtin_profiles = {
                "mixed-workload": {
                    "rw_mode": "randrw",
                    "block_size": "64k",
                    "job_name": "mixed-workload",
                    "profile_options": {"rwmixread": "50", "log_avg_msec": "1000"},
                },
                "db-workload": {
                    "rw_mode": "randrw",
                    "block_size": "4k",
                    "job_name": "db-workload",
                    "profile_options": {"rwmixread": "70", "random_distribution": "zipf:0.99", "log_avg_msec": "1000"},
                },
            }
            profile_defs = []
            for profile_name in fio_profile_names:
                profile = builtin_profiles.get(profile_name)
                if profile is None:
                    raise rally_exceptions.ScriptError(message=f"Unknown fio profile {profile_name}")
                profile_defs.append({"profile_name": profile_name, **profile})
            if not profile_defs:
                profile_defs = [
                    {"profile_name": None, "rw_mode": rw_mode, "block_size": block_size, "job_name": "workload", "profile_options": {}}
                    for rw_mode in fio_rw_modes
                    for block_size in fio_block_sizes
                ]
            for client_count in fio_client_counts:
                for volumes_per_client in fio_volumes_per_client:
                    for profile in profile_defs:
                        for jobs in fio_numjobs:
                            for depth in fio_iodepths:
                                case = {
                                    "client_count": client_count,
                                    "volumes_per_client": volumes_per_client,
                                    "profile_name": profile["profile_name"],
                                    "rw_mode": profile["rw_mode"],
                                    "block_size": profile["block_size"],
                                    "job_name": profile["job_name"],
                                    "profile_options": profile["profile_options"],
                                    "numjobs": jobs,
                                    "iodepth": depth,
                                }
                                profile_prefix = f"profile-{case['profile_name']}_" if case["profile_name"] else ""
                                case["case_id"] = (
                                    f"{profile_prefix}clients-{client_count}_vols-{volumes_per_client}_"
                                    f"rw-{case['rw_mode']}_bs-{str(case['block_size']).replace('/', '-')}_"
                                    f"numjobs-{jobs}_iodepth-{depth}"
                                )
                                fio_cases.append(case)
            fio_matrix = {
                "runtime_seconds": int(fio_runtime_seconds),
                "ramp_time_seconds": int(fio_ramp_time_seconds),
                "ioengine": fio_ioengine,
                "cases": fio_cases,
            }
            many_inventory = {
                "ssh_user": ssh_user,
                "server": {"name": many_server.name, "fixed_ip": self._fixed_ip(many_server)},
                "clients": [{"name": client["name"], "fixed_ip": client["fixed_ip"]} for client in many_clients],
            }
            many_cases = []
            if many_mode == "iperf3":
                for protocol in many_protocols:
                    if protocol == "tcp":
                        for streams in many_parallel_streams:
                            many_cases.append(
                                {
                                    "case_id": f"iperf3-tcp-clients-{many_client_count}-streams-{streams}",
                                    "mode": "iperf3",
                                    "protocol": protocol,
                                    "parallel_streams": int(streams),
                                }
                            )
                    else:
                        for target in many_udp_target_mbps:
                            many_cases.append(
                                {
                                    "case_id": f"iperf3-udp-clients-{many_client_count}-target-{target}m",
                                    "mode": "iperf3",
                                    "protocol": protocol,
                                    "udp_target_mbps": int(target),
                                }
                            )
            else:
                many_cases.append({"case_id": "http-volume", "mode": "http_volume", "protocol": "http"})
            many_matrix = {
                "scenario_slug": "net-many-to-one",
                "traffic": {
                    "mode": many_mode,
                    "protocols": many_protocols,
                    "duration_seconds": int(many_duration_seconds),
                    "ramp_time_seconds": int(many_ramp_time_seconds),
                    "base_port": int(many_base_port),
                    "http_port": 8080,
                },
                "many_to_one": {
                    "client_count": int(many_client_count),
                    "flow_direction": many_flow_direction,
                },
                "http_volume": {
                    "file_count": int(many_http_file_count),
                    "file_size_mib": int(many_http_file_size_mib),
                },
                "cases": many_cases,
            }
            ring_inventory = {
                "ssh_user": ssh_user,
                "participants": [{"name": participant["name"], "fixed_ip": participant["fixed_ip"]} for participant in ring_participants],
            }
            ring_cases = []
            for protocol in ring_protocols:
                if protocol == "tcp":
                    for streams in ring_parallel_streams:
                        ring_cases.append(
                            {
                                "case_id": f"ring-tcp-participants-{ring_participant_count}-neighbors-{ring_neighbors_per_vm}-streams-{streams}",
                                "protocol": protocol,
                                "neighbors_per_vm": int(ring_neighbors_per_vm),
                                "bidirectional": bool(ring_bidirectional),
                                "parallel_streams": int(streams),
                            }
                        )
                else:
                    for target in ring_udp_target_mbps:
                        ring_cases.append(
                            {
                                "case_id": f"ring-udp-participants-{ring_participant_count}-neighbors-{ring_neighbors_per_vm}-target-{target}m",
                                "protocol": protocol,
                                "neighbors_per_vm": int(ring_neighbors_per_vm),
                                "bidirectional": bool(ring_bidirectional),
                                "udp_target_mbps": int(target),
                            }
                        )
            ring_matrix = {
                "scenario_slug": "net-ring",
                "traffic": {
                    "protocols": ring_protocols,
                    "duration_seconds": int(ring_duration_seconds),
                    "ramp_time_seconds": int(ring_ramp_time_seconds),
                    "base_port": int(ring_base_port),
                },
                "ring": {
                    "participant_count": int(ring_participant_count),
                    "neighbors_per_vm": int(ring_neighbors_per_vm),
                    "bidirectional": bool(ring_bidirectional),
                },
                "cases": ring_cases,
            }
            self._upload_mixed_controller_inputs(
                ssh,
                ssh_user,
                controller_remote_dir,
                {
                    "schema_version": 1,
                    "note": "mixed-pressure inputs uploaded by Rally host",
                },
                fio_inventory,
                fio_matrix,
                many_inventory,
                many_matrix,
                ring_inventory,
                ring_matrix,
                keypair["private"],
            )
            self._wait_for_controller_ssh_targets(
                ssh,
                f"{controller_remote_dir}/id_rsa",
                ssh_user,
                [self._fixed_ip(many_server), *[client["fixed_ip"] for client in many_clients], *[participant["fixed_ip"] for participant in ring_participants], *[worker["fixed_ip"] for worker in fio_workers]],
                timeout_seconds=ssh_connect_timeout_seconds,
            )
            component_commands = {
                "fio": [
                    "python3",
                    f"{controller_remote_dir}/fio_controller_runner.py",
                    "--inventory",
                    f"{controller_remote_dir}/fio/inventory.json",
                    "--matrix",
                    f"{controller_remote_dir}/fio/matrix.json",
                    "--output-dir",
                    f"{controller_remote_dir}/artifacts/fio",
                ],
                "net-many-to-one": [
                    "python3",
                    f"{controller_remote_dir}/net_controller_runner.py",
                    "--inventory",
                    f"{controller_remote_dir}/net-many-to-one/inventory.json",
                    "--matrix",
                    f"{controller_remote_dir}/net-many-to-one/matrix.json",
                    "--output-dir",
                    f"{controller_remote_dir}/artifacts/net-many-to-one",
                ],
                "net-ring": [
                    "python3",
                    f"{controller_remote_dir}/net_controller_runner.py",
                    "--inventory",
                    f"{controller_remote_dir}/net-ring/inventory.json",
                    "--matrix",
                    f"{controller_remote_dir}/net-ring/matrix.json",
                    "--output-dir",
                    f"{controller_remote_dir}/artifacts/net-ring",
                ],
            }
            per_component_timeout = max(
                int(fio_runtime_seconds),
                int(many_duration_seconds),
                int(ring_duration_seconds),
            ) + 180
            for component_name, command in component_commands.items():
                thread, state = self._start_remote_component_runner(
                    controller_fip["ip"],
                    ssh_user,
                    keypair["private"],
                    ssh_connect_timeout_seconds,
                    command,
                    per_component_timeout,
                )
                fixed_component_states[component_name] = {
                    "thread": thread,
                    "state": state,
                    "reported": False,
                }

            scenario_name = "CIChurn.mixed_pressure"
            iteration = int(self.context.get("iteration", 0) or 0)
            start = time.monotonic()
            next_tick = start
            arrival_deadline = start + duration_seconds
            tokens = 0.0
            launch_index = 0
            completed_since_tick = 0

            while True:
                now = time.monotonic()
                all_components_done = True
                for component_name, component_state in fixed_component_states.items():
                    state = component_state["state"]
                    if not bool(state.get("done")):
                        all_components_done = False
                        continue
                    if component_state["reported"]:
                        continue
                    component_state["reported"] = True
                    fixed_error = state.get("error")
                    if fixed_error is not None:
                        raise rally_exceptions.ScriptError(
                            message=f"Fixed mixed component {component_name} failed: {fixed_error}"
                        )
                    fixed_exit_status = int(state.get("exit_status") or 0)
                    if fixed_exit_status != 0 and subbenchmark_failure_mode == "fail":
                        stderr_output = str(state.get("stderr") or "")
                        stdout_output = str(state.get("stdout") or "")
                        raise rally_exceptions.ScriptError(
                            message=f"Fixed mixed component {component_name} failed: {stderr_output or stdout_output or 'exit status ' + str(fixed_exit_status)}"
                        )

                for server_id, vm_state in list(active_churn_vms.items()):
                    server = self._show_server(vm_state["server"])
                    vm_state["server"] = server
                    result = self._read_swift_object(
                        swift_endpoint,
                        artifact_container,
                        str(vm_state["result_object_name"]),
                        swift_token,
                        swift_context,
                    )
                    if result:
                        status = str(result.get("status", "unknown"))
                        error = str(result.get("diagnostics", {}).get("error", ""))
                        churn_rows.append(
                            {
                                "server": server.name,
                                "status": status,
                                "duration_seconds": result.get("duration_seconds", ""),
                                "artifact": str((result.get("artifact_refs", [{}])[0] or {}).get("object_name", "")) if result.get("artifact_refs") else "",
                                "error": error,
                                "wave": wave,
                                "iteration": iteration,
                            }
                        )
                        churn_summary["completed_vms"] += 1
                        if status == "error":
                            churn_summary["failed_vms"] += 1
                            errors.append(error or f"{server.name} returned an error result")
                        self._delete_vm(vm_state, force_delete)
                        del active_churn_vms[server_id]
                        completed_since_tick += 1
                    elif server.status == "SHUTOFF":
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
                        churn_rows.append(
                            {
                                "server": server.name,
                                "status": status,
                                "duration_seconds": result.get("duration_seconds", ""),
                                "artifact": str((result.get("artifact_refs", [{}])[0] or {}).get("object_name", "")) if result.get("artifact_refs") else "",
                                "error": error,
                                "wave": wave,
                                "iteration": iteration,
                            }
                        )
                        churn_summary["completed_vms"] += 1
                        if status == "error":
                            churn_summary["failed_vms"] += 1
                            errors.append(error or f"{server.name} returned an error result")
                        self._delete_vm(vm_state, force_delete)
                        del active_churn_vms[server_id]
                        completed_since_tick += 1
                    elif server.status == "ERROR":
                        churn_summary["failed_vms"] += 1
                        errors.append(f"{server.name} entered ERROR state before shutdown")
                        churn_rows.append(
                            {
                                "server": server.name,
                                "status": "error",
                                "duration_seconds": round(time.monotonic() - float(vm_state["launched_monotonic"]), 3),
                                "artifact": "",
                                "error": "Server entered ERROR state before shutdown",
                                "wave": wave,
                                "iteration": iteration,
                            }
                        )
                        self._delete_vm(vm_state, force_delete)
                        del active_churn_vms[server_id]
                        completed_since_tick += 1
                    elif churn_timeout_seconds > 0 and (time.monotonic() - float(vm_state["launched_monotonic"])) >= int(churn_timeout_seconds):
                        churn_summary["timed_out_vms"] += 1
                        result = self._build_timeout_result(
                            scenario_name,
                            server,
                            float(vm_state["launched_monotonic"]),
                            wave,
                            iteration,
                        )
                        churn_rows.append(
                            {
                                "server": server.name,
                                "status": "timeout",
                                "duration_seconds": result["duration_seconds"],
                                "artifact": "",
                                "error": result["diagnostics"]["error"],
                                "wave": wave,
                                "iteration": iteration,
                            }
                        )
                        if churn_timeout_mode == "fail":
                            errors.append(f"{server.name} timed out after {churn_timeout_seconds} seconds")
                        self._delete_vm(vm_state, force_delete)
                        del active_churn_vms[server_id]
                        completed_since_tick += 1

                if now >= next_tick and next_tick < arrival_deadline:
                    offset_seconds = next_tick - start
                    multiplier = self._multiplier_for_offset(offset_seconds, burst_windows)
                    target_rate = baseline_launches_per_minute * multiplier
                    tokens += (target_rate / 60.0) * launch_tick_seconds
                    launched_this_tick = 0
                    dropped_this_tick = 0
                    while tokens >= 1.0:
                        if len(active_churn_vms) >= max_active_vms:
                            churn_summary["dropped_launches"] += 1
                            dropped_this_tick += 1
                            tokens -= 1.0
                            continue
                        launch_index += 1
                        vm_state = self._launch_runner_vm(
                            churn_image,
                            churn_flavor,
                            scenario_name,
                            artifact_container,
                            int(artifact_ttl_seconds),
                            swift_auth_url,
                            swift_username,
                            swift_password,
                            swift_project_name,
                            swift_user_domain_name,
                            swift_project_domain_name,
                            swift_interface,
                            swift_region_name,
                            swift_cacert_b64,
                            "stress_ng",
                            churn_workload_params,
                            iteration=launch_index,
                            wave=wave,
                        )
                        active_churn_vms[vm_state["server"].id] = vm_state
                        churn_summary["launched_vms"] += 1
                        launched_this_tick += 1
                        tokens -= 1.0
                    churn_summary["peak_active_vms"] = max(churn_summary["peak_active_vms"], len(active_churn_vms))
                    next_tick += launch_tick_seconds
                    completed_since_tick = 0

                if now >= arrival_deadline and not active_churn_vms and all_components_done:
                    break
                time.sleep(1.0)

            self._download_tree(ssh, f"{controller_remote_dir}/artifacts", artifacts_dir)
            churn_dir = artifacts_dir / "vm-churn"
            self._write_churn_artifacts(churn_dir, churn_rows, churn_summary)
            fio_summary = json.loads((artifacts_dir / "fio" / "summary.json").read_text(encoding="utf-8"))
            many_summary = json.loads((artifacts_dir / "net-many-to-one" / "summary.json").read_text(encoding="utf-8"))
            ring_summary = json.loads((artifacts_dir / "net-ring" / "summary.json").read_text(encoding="utf-8"))
            summary, metric_rows, component_rows = self._build_top_level_summary(
                artifacts_dir,
                churn_summary,
                fio_summary,
                many_summary,
                ring_summary,
            )
            self._write_top_level_artifacts(artifacts_dir, summary, metric_rows, component_rows)
        finally:
            if ssh is not None:
                try:
                    if not artifacts_dir.exists() or not any(artifacts_dir.iterdir()):
                        self._download_tree(ssh, f"{controller_remote_dir}/artifacts", artifacts_dir)
                except Exception:
                    pass
            if ssh is not None:
                try:
                    ssh.close()
                except Exception:
                    pass
            for vm_state in list(active_churn_vms.values()):
                try:
                    self._delete_vm(vm_state, force_delete)
                except Exception:
                    pass
            for attachment in reversed(fio_attachments):
                self._detach_volume(attachment["server_id"], attachment["volume_id"])
            for worker in fio_workers:
                try:
                    self._delete_server(worker["server"], force=True)
                except Exception:
                    pass
            for participant in ring_participants:
                try:
                    self._delete_server(participant["server"], force=True)
                except Exception:
                    pass
            for client in many_clients:
                try:
                    self._delete_server(client["server"], force=True)
                except Exception:
                    pass
            if many_server is not None:
                try:
                    self._delete_server(many_server, force=True)
                except Exception:
                    pass
            if controller is not None and controller_fip is not None:
                try:
                    self._delete_server_with_fip(controller, controller_fip, force_delete=True)
                except Exception:
                    pass
            for volume_id in reversed(fio_volumes):
                self._delete_volume(volume_id)
            self._delete_security_group(fio_worker_sg["id"])
            self._delete_security_group(benchmark_sg["id"])
            self._delete_security_group(controller_sg["id"])
            self._delete_keypair(keypair["name"])

        if errors and subbenchmark_failure_mode == "fail":
            raise rally_exceptions.ScriptError(message="; ".join(errors))

        summary_payload = json.loads((artifacts_dir / "summary.json").read_text(encoding="utf-8"))
        self.add_output(complete=build_summary_output(summary_payload["summary"]))
        self.add_output(complete=build_metrics_output([[key, value] for key, value in summary_payload["metrics"].items()]))
        self.add_output(
            complete=build_table_output(
                "Mixed components",
                "Per-component status and high-level counters for the mixed pressure run",
                ["component", "status", "count", "completed", "failed", "timed_out", "peak_active", "details"],
                [
                    [
                        row["component"],
                        row["status"],
                        row["count"],
                        row["completed"],
                        row["failed"],
                        row["timed_out"],
                        row["peak_active"],
                        row["details"],
                    ]
                    for row in summary_payload["components"]
                ],
            )
        )
        self.add_output(complete=build_phase_output(self.atomic_actions()))
        self.add_output(
            complete=build_artifacts_output(
                [
                    ["artifact_root", str(artifacts_dir)],
                    ["summary_json", str(artifacts_dir / "summary.json")],
                    ["fio_summary_json", str(artifacts_dir / "fio" / "summary.json")],
                    ["net_many_to_one_summary_json", str(artifacts_dir / "net-many-to-one" / "summary.json")],
                    ["net_ring_summary_json", str(artifacts_dir / "net-ring" / "summary.json")],
                    ["vm_churn_summary_json", str(artifacts_dir / "vm-churn" / "summary.json")],
                ]
            )
        )
        return {
            "schema_version": 1,
            "scenario_family": "mixed_pressure",
            "scenario_name": "CIChurn.mixed_pressure",
            "status": "success",
            "summary": summary_payload["summary"],
            "metrics": {
                "aggregates": summary_payload["metrics"],
                "components": summary_payload["components"],
            },
            "timings": self._timings_payload(),
            "artifacts": {
                "artifact_root": str(artifacts_dir),
                "summary_json": str(artifacts_dir / "summary.json"),
            },
            "diagnostics": {},
        }

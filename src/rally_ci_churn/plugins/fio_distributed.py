"""Distributed fio benchmark scenario."""

from __future__ import annotations

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

from rally_ci_churn.plugins.controller_runtime import ControllerRuntimeBase
from rally_ci_churn.plugins.controller_runtime import SSH_PORT
from rally_ci_churn.plugins.controller_runtime import build_root_volume_boot
from rally_ci_churn.results import build_artifacts_output
from rally_ci_churn.results import build_metrics_output
from rally_ci_churn.results import build_phase_output
from rally_ci_churn.results import build_summary_output
from rally_ci_churn.results import build_table_output
from rally_ci_churn.results import summarize_atomic_actions
from rally_ci_churn.results import summarize_numeric_series


DEFAULT_FIO_PORT = 8765
DEVICE_POLL_INTERVAL_SECONDS = 2.0
WORKER_READY_TIMEOUT_SECONDS = 600


def _as_int_list(values: list[object]) -> list[int]:
    return [int(value) for value in values]


def _as_str_list(values: list[object]) -> list[str]:
    return [str(value) for value in values]


BUILTIN_FIO_PROFILES = {
    "mixed-workload": {
        "rw_mode": "randrw",
        "block_size": "64k",
        "job_name": "mixed-workload",
        "profile_options": {
            "rwmixread": "50",
            "log_avg_msec": "1000",
        },
    },
    "db-workload": {
        "rw_mode": "randrw",
        "block_size": "4k",
        "job_name": "db-workload",
        "profile_options": {
            "rwmixread": "70",
            "random_distribution": "zipf:0.99",
            "log_avg_msec": "1000",
        },
    },
}


def _case_id(case: dict[str, object]) -> str:
    profile_name = case.get("profile_name")
    prefix = f"profile-{profile_name}_" if profile_name else ""
    return (
        f"{prefix}clients-{case['client_count']}_"
        f"vols-{case['volumes_per_client']}_"
        f"rw-{case['rw_mode']}_"
        f"bs-{str(case['block_size']).replace('/', '-')}_"
        f"numjobs-{case['numjobs']}_"
        f"iodepth-{case['iodepth']}"
    )


class _FioDistributedBase(ControllerRuntimeBase):
    def _create_worker_security_group(self, tenant_cidr: str, fio_port: int) -> dict[str, object]:
        security_group = self._create_security_group(
            self.generate_random_name(),
            "Allow fio server access from the tenant network",
        )
        self._add_security_group_rule(
            security_group["id"],
            "tcp",
            fio_port,
            fio_port,
            tenant_cidr,
        )
        self._add_security_group_rule(security_group["id"], "icmp", remote_ip_prefix=tenant_cidr)
        return security_group

    def _build_controller_user_data(self) -> str:
        return """#cloud-config
write_files:
  - path: /etc/rally-ci-fio-role
    permissions: "0644"
    content: |
      role=controller
runcmd:
  - [ cloud-init-per, once, rally-fio-controller-ready, /bin/sh, -c, "mkdir -p /var/lib/rally-fio/run /var/lib/rally-fio/devices && chown -R ubuntu:ubuntu /var/lib/rally-fio" ]
"""

    def _build_worker_user_data(self, expected_volumes: int, fio_port: int) -> str:
        script = f"""#!/bin/bash
set -euo pipefail
expected_volumes="{expected_volumes}"
fio_port="{fio_port}"
mkdir -p /var/lib/rally-fio/devices
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
  sleep {DEVICE_POLL_INTERVAL_SECONDS}
done
rm -f /var/lib/rally-fio/devices/vol*
for index in $(seq 1 "$expected_volumes"); do
  disk="${{data_disks[$((index - 1))]}}"
  ln -sfn "$disk" "/var/lib/rally-fio/devices/vol$(printf '%02d' "$index")"
done
python3 - <<'PY'
import json
from pathlib import Path
devices = [str(path) for path in sorted(Path("/var/lib/rally-fio/devices").glob("vol*"))]
Path("/var/lib/rally-fio/worker-ready.json").write_text(
    json.dumps({{"devices": devices}}, indent=2, sort_keys=True),
    encoding="utf-8",
)
PY
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
        controller_image,
        controller_flavor,
        external_network_name: str,
        key_name: str,
        security_group_name: str,
        boot_from_volume: bool = False,
        root_volume_size_gib: int = 20,
        root_volume_type: str | None = None,
    ):
        boot_image, boot_kwargs = build_root_volume_boot(
            controller_image,
            enabled=boot_from_volume,
            volume_size_gib=root_volume_size_gib,
            volume_type=root_volume_type,
        )
        return self._boot_server_with_fip(
            boot_image,
            controller_flavor,
            use_floating_ip=True,
            floating_network=external_network_name,
            key_name=key_name,
            security_groups=[security_group_name],
            userdata=self._build_controller_user_data(),
            **boot_kwargs,
        )

    @atomic.action_timer("worker.boot")
    def _boot_worker(
        self,
        worker_image,
        worker_flavor,
        key_name: str,
        security_group_name: str,
        expected_volumes: int,
        fio_port: int,
        boot_from_volume: bool = False,
        root_volume_size_gib: int = 20,
        root_volume_type: str | None = None,
    ):
        return self._boot_worker_raw(
            worker_image,
            worker_flavor,
            key_name,
            security_group_name,
            expected_volumes,
            fio_port,
            boot_from_volume=boot_from_volume,
            root_volume_size_gib=root_volume_size_gib,
            root_volume_type=root_volume_type,
        )

    def _boot_worker_raw(
        self,
        worker_image,
        worker_flavor,
        key_name: str,
        security_group_name: str,
        expected_volumes: int,
        fio_port: int,
        boot_from_volume: bool = False,
        root_volume_size_gib: int = 20,
        root_volume_type: str | None = None,
    ):
        boot_image, boot_kwargs = build_root_volume_boot(
            worker_image,
            enabled=boot_from_volume,
            volume_size_gib=root_volume_size_gib,
            volume_type=root_volume_type,
        )
        return self._boot_server(
            boot_image,
            worker_flavor,
            auto_assign_nic=True,
            key_name=key_name,
            security_groups=[security_group_name],
            userdata=self._build_worker_user_data(expected_volumes, fio_port),
            **boot_kwargs,
        )

    @atomic.action_timer("worker.wait_ready")
    def _wait_for_worker_fio_ready(
        self,
        ssh: sshutils.SSH,
        fixed_ip: str,
        fio_port: int,
        timeout_seconds: int = WORKER_READY_TIMEOUT_SECONDS,
    ) -> None:
        deadline = time.monotonic() + timeout_seconds
        python_snippet = (
            "import socket, sys; "
            "sock = socket.create_connection((sys.argv[1], int(sys.argv[2])), timeout=2); "
            "sock.close()"
        )
        while time.monotonic() < deadline:
            exit_status, _ = ssh.run(
                [
                    "python3",
                    "-c",
                    python_snippet,
                    fixed_ip,
                    str(fio_port),
                ],
                timeout=10,
                raise_on_error=False,
            )
            if exit_status == 0:
                return
            time.sleep(DEVICE_POLL_INTERVAL_SECONDS)
        raise rally_exceptions.ScriptError(
            message=f"Worker fio server {fixed_ip}:{fio_port} did not become ready before timeout"
        )

    def _matrix_cases(
        self,
        client_counts: list[int],
        volumes_per_client: list[int],
        profile_names: list[str],
        rw_modes: list[str],
        block_sizes: list[str],
        numjobs: list[int],
        iodepths: list[int],
    ) -> list[dict[str, object]]:
        cases: list[dict[str, object]] = []
        profile_defs: list[dict[str, object]] = []
        for profile_name in profile_names:
            profile = BUILTIN_FIO_PROFILES.get(profile_name)
            if profile is None:
                raise rally_exceptions.InvalidArgumentsException(
                    argument_name="profile_names",
                    value=profile_name,
                    valid_values=sorted(BUILTIN_FIO_PROFILES),
                )
            profile_defs.append({"profile_name": profile_name, **profile})
        if not profile_defs:
            profile_defs = [
                {
                    "profile_name": None,
                    "rw_mode": rw_mode,
                    "block_size": block_size,
                    "job_name": "workload",
                    "profile_options": {},
                }
                for rw_mode in rw_modes
                for block_size in block_sizes
            ]

        for client_count in client_counts:
            for volumes in volumes_per_client:
                for profile in profile_defs:
                    for jobs in numjobs:
                        for depth in iodepths:
                            case = {
                                "client_count": client_count,
                                "volumes_per_client": volumes,
                                "profile_name": profile["profile_name"],
                                "rw_mode": profile["rw_mode"],
                                "block_size": profile["block_size"],
                                "job_name": profile["job_name"],
                                "profile_options": profile["profile_options"],
                                "numjobs": jobs,
                                "iodepth": depth,
                            }
                            case["case_id"] = _case_id(case)
                            cases.append(case)
        return cases

    def _inventory_payload(
        self,
        workers: list[dict[str, object]],
        fio_port: int,
        max_volumes_per_client: int,
    ) -> dict[str, object]:
        return {
            "fio_port": fio_port,
            "workers": [
                {
                    "name": worker["server"].name,
                    "fixed_ip": worker["fixed_ip"],
                    "devices": [
                        f"/var/lib/rally-fio/devices/vol{index + 1:02d}"
                        for index in range(max_volumes_per_client)
                    ],
                }
                for worker in workers
            ],
        }

    @atomic.action_timer("controller.upload_inputs")
    def _upload_controller_inputs(
        self,
        ssh: sshutils.SSH,
        ssh_user: str,
        controller_remote_dir: str,
        inventory: dict[str, object],
        matrix: dict[str, object],
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
        controller_runner = resources.files("rally_ci_churn.fio").joinpath("controller_runner.py")
        with tempfile.TemporaryDirectory(prefix="rally-fio-controller-") as temp_dir:
            temp_path = Path(temp_dir)
            inventory_path = temp_path / "inventory.json"
            matrix_path = temp_path / "matrix.json"
            controller_path = temp_path / "controller_runner.py"
            inventory_path.write_text(json.dumps(inventory, indent=2, sort_keys=True), encoding="utf-8")
            matrix_path.write_text(json.dumps(matrix, indent=2, sort_keys=True), encoding="utf-8")
            controller_path.write_text(controller_runner.read_text(encoding="utf-8"), encoding="utf-8")
            ssh.put_file(str(inventory_path), f"{controller_remote_dir}/inventory.json", mode=0o644)
            ssh.put_file(str(matrix_path), f"{controller_remote_dir}/matrix.json", mode=0o644)
            ssh.put_file(str(controller_path), f"{controller_remote_dir}/controller_runner.py", mode=0o755)

    @atomic.action_timer("fio.run_matrix")
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

    def _summary_rows(self, summary_payload: dict[str, object]) -> list[list[object]]:
        rows = []
        for row in summary_payload.get("rows", []):
            if not isinstance(row, dict):
                continue
            rows.append(
                [
                    row.get("client_nodes", ""),
                    row.get("volumes_per_client", ""),
                    row.get("total_volumes", ""),
                    row.get("profile_name", ""),
                    row.get("rw_mode", ""),
                    row.get("block_size", ""),
                    row.get("numjobs", ""),
                    row.get("iodepth", ""),
                    row.get("throughput_human", ""),
                    row.get("iops_human", ""),
                    row.get("avg_latency_ms", ""),
                    row.get("p99_latency_ms", ""),
                ]
            )
        return rows

    def _summary_dict(self, summary_payload: dict[str, object], artifacts_dir: Path) -> dict[str, object]:
        inventory = summary_payload.get("inventory", {})
        matrix = summary_payload.get("matrix", {})
        workers = inventory.get("workers", []) if isinstance(inventory, dict) else []
        rows = summary_payload.get("rows", []) if isinstance(summary_payload, dict) else []
        max_volumes = max((len(worker.get("devices", [])) for worker in workers if isinstance(worker, dict)), default=0)
        return {
            "controller_nodes": 1,
            "worker_nodes": len(workers),
            "total_provisioned_volumes": len(workers) * max_volumes,
            "matrix_cases": len(rows),
            "ioengine": str(matrix.get("ioengine", "")) if isinstance(matrix, dict) else "",
            "artifact_root": str(artifacts_dir),
        }

    def _metric_rows(self, summary_payload: dict[str, object]) -> list[list[object]]:
        rows = summary_payload.get("rows", []) if isinstance(summary_payload, dict) else []
        if not isinstance(rows, list) or not rows:
            return []
        throughput_values = [
            float(row.get("throughput_bytes_per_sec", 0.0))
            for row in rows
            if isinstance(row, dict)
        ]
        iops_values = [
            float(row.get("iops", 0.0))
            for row in rows
            if isinstance(row, dict)
        ]
        p99_values = [
            float(row.get("p99_latency_ms", 0.0))
            for row in rows
            if isinstance(row, dict)
        ]
        avg_latency_values = [
            float(row.get("avg_latency_ms", 0.0))
            for row in rows
            if isinstance(row, dict)
        ]
        profiles = sorted(
            {
                str(row.get("profile_name", ""))
                for row in rows
                if isinstance(row, dict) and row.get("profile_name")
            }
        )
        throughput_stats = summarize_numeric_series(throughput_values)
        latency_stats = summarize_numeric_series(avg_latency_values)
        metrics = [
            ["best_throughput_bytes_per_sec", str(round(max(throughput_values), 3))],
            ["median_throughput_bytes_per_sec", str(round(float(throughput_stats.get("p50", 0.0)), 3))],
            ["best_iops", str(round(max(iops_values), 3))],
            ["worst_p99_latency_ms", str(round(max(p99_values), 3))],
            ["median_avg_latency_ms", str(round(float(latency_stats.get("p50", 0.0)), 3))],
            ["profiles", ",".join(profiles)],
        ]
        return metrics

    def _artifact_rows(self, artifacts_dir: Path) -> list[list[object]]:
        return [
            ["artifact_root", str(artifacts_dir)],
            ["summary_markdown", str(artifacts_dir / "summary.md")],
            ["summary_json", str(artifacts_dir / "summary.json")],
            ["manifest_json", str(artifacts_dir / "manifest.json")],
        ]

    def _result_payload(self, artifacts_dir: Path, summary_payload: dict[str, object]) -> dict[str, object]:
        summary = self._summary_dict(summary_payload, artifacts_dir)
        metrics = {
            "rows": summary_payload.get("rows", []),
            "aggregates": {
                key: value
                for key, value in (
                    (row[0], row[1]) for row in self._metric_rows(summary_payload)
                )
            },
        }
        return {
            "schema_version": 1,
            "scenario_family": "fio_distributed",
            "scenario_name": "CIChurn.fio_distributed",
            "status": "success",
            "artifact_root": str(artifacts_dir),
            "matrix_cases": len(summary_payload.get("rows", [])),
            "summary_rows": summary_payload.get("rows", []),
            "summary": summary,
            "metrics": metrics,
            "timings": self._timings_payload(),
            "artifacts": {key: value for key, value in self._artifact_rows(artifacts_dir)},
            "diagnostics": {},
        }


@types.convert(
    controller_image={"type": "glance_image"},
    controller_flavor={"type": "nova_flavor"},
    worker_image={"type": "glance_image"},
    worker_flavor={"type": "nova_flavor"},
)
@validation.add(
    "required_services",
    services=[consts.Service.NOVA, consts.Service.CINDER, consts.Service.NEUTRON],
)
@validation.add("image_valid_on_flavor", flavor_param="controller_flavor", image_param="controller_image")
@validation.add("image_valid_on_flavor", flavor_param="worker_flavor", image_param="worker_image")
@validation.add("required_platform", platform="openstack", users=True)
@scenario.configure(
    name="CIChurn.fio_distributed",
    platform="openstack",
    context={"cleanup@openstack": ["nova", "neutron", "cinder"], "network@openstack": {}},
)
class FioDistributedScenario(_FioDistributedBase):
    """Run a distributed fio matrix with one controller and many workers."""

    def run(
        self,
        controller_image,
        controller_flavor,
        worker_image,
        worker_flavor,
        external_network_name,
        ssh_user="ubuntu",
        ssh_connect_timeout_seconds=300,
        command_timeout_seconds=0,
        boot_from_volume=False,
        root_volume_size_gib=20,
        root_volume_type=None,
        boot_concurrency=1,
        volume_concurrency=1,
        volume_size_gib=10,
        volume_type=None,
        client_counts=None,
        volumes_per_client=None,
        profile_names=None,
        rw_modes=None,
        block_sizes=None,
        numjobs=None,
        iodepths=None,
        runtime_seconds=30,
        ramp_time_seconds=5,
        fio_port=DEFAULT_FIO_PORT,
        ioengine="io_uring",
        artifacts_root_dir="artifacts",
    ):
        client_counts = _as_int_list(client_counts or [1, 2])
        volumes_per_client = _as_int_list(volumes_per_client or [1])
        profile_names = _as_str_list(profile_names or [])
        rw_modes = _as_str_list(rw_modes or ["write", "read"])
        block_sizes = _as_str_list(block_sizes or ["1M"])
        numjobs = _as_int_list(numjobs or [1, 2])
        iodepths = _as_int_list(iodepths or [1, 32])
        volume_size_gib = int(volume_size_gib)
        runtime_seconds = int(runtime_seconds)
        ramp_time_seconds = int(ramp_time_seconds)
        fio_port = int(fio_port)
        ssh_connect_timeout_seconds = int(ssh_connect_timeout_seconds)
        command_timeout_seconds = int(command_timeout_seconds)
        root_volume_size_gib = int(root_volume_size_gib)
        boot_concurrency = int(boot_concurrency)
        volume_concurrency = int(volume_concurrency)
        max_clients = max(client_counts)
        max_volumes_per_client = max(volumes_per_client)
        tenant_cidr = self._tenant_cidr()

        keypair = self._create_keypair()
        controller_sg = self._create_controller_security_group()
        worker_sg = self._create_worker_security_group(tenant_cidr, fio_port)
        controller = None
        controller_fip = None
        workers: list[dict[str, object]] = []
        attachments: list[dict[str, object]] = []
        volumes: list[str] = []
        ssh = None
        artifacts_dir = self._artifacts_dir(artifacts_root_dir, "fio-distributed")
        summary_payload: dict[str, object] | None = None
        controller_remote_dir = f"/var/lib/rally-fio/run/{uuid.uuid4().hex}"

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
            def _boot_worker_record(_index: int) -> dict[str, object]:
                worker = self._boot_worker_raw(
                    worker_image,
                    worker_flavor,
                    keypair["name"],
                    worker_sg["name"],
                    max_volumes_per_client,
                    fio_port,
                    boot_from_volume=bool(boot_from_volume),
                    root_volume_size_gib=root_volume_size_gib,
                    root_volume_type=root_volume_type,
                )
                return {"server": worker, "fixed_ip": self._fixed_ip(worker)}

            self._boot_vm_group(
                count=max_clients,
                concurrency=boot_concurrency,
                atomic_action_name="worker.boot_group",
                boot_fn=_boot_worker_record,
                destination=workers,
            )

            device_letters = "bcdefghijklmnopqrstuvwxyz"
            volume_requests = [
                {
                    "server": worker["server"],
                    "size": volume_size_gib,
                    "volume_type": volume_type,
                    "device_name": f"/dev/vd{device_letters[volume_index]}",
                }
                for worker in workers
                for volume_index in range(max_volumes_per_client)
            ]
            self._provision_volume_group(
                requests=volume_requests,
                concurrency=volume_concurrency,
                volume_ids=volumes,
                attachments=attachments,
            )

            ssh = self._ssh(
                controller_fip["ip"],
                ssh_user,
                keypair["private"],
                ssh_connect_timeout_seconds,
            )
            for worker in workers:
                self._wait_for_worker_fio_ready(
                    ssh,
                    str(worker["fixed_ip"]),
                    fio_port,
                )
            matrix = {
                "runtime_seconds": runtime_seconds,
                "ramp_time_seconds": ramp_time_seconds,
                "ioengine": ioengine,
                "cases": self._matrix_cases(
                    client_counts,
                    volumes_per_client,
                    profile_names,
                    rw_modes,
                    block_sizes,
                    numjobs,
                    iodepths,
                ),
            }
            inventory = self._inventory_payload(workers, fio_port, max_volumes_per_client)
            self._upload_controller_inputs(
                ssh,
                ssh_user,
                controller_remote_dir,
                inventory,
                matrix,
            )
            exit_status, stdout, stderr = self._run_controller_runner(
                ssh,
                controller_remote_dir,
                command_timeout_seconds,
            )
            if exit_status != 0:
                raise rally_exceptions.ScriptError(
                    message=f"Controller fio runner failed with exit status {exit_status}: {stderr or stdout}"
                )
            self._download_tree(ssh, controller_remote_dir, artifacts_dir)
            summary_payload = json.loads((artifacts_dir / "summary.json").read_text(encoding="utf-8"))
        finally:
            if ssh is not None:
                try:
                    ssh.close()
                except Exception:
                    pass
            for attachment in reversed(attachments):
                self._detach_volume(attachment["server_id"], attachment["volume_id"])
            for worker in workers:
                try:
                    self._delete_server(worker["server"], force=True)
                except Exception:
                    pass
            if controller is not None and controller_fip is not None:
                try:
                    self._delete_server_with_fip(controller, controller_fip, force_delete=True)
                except Exception:
                    pass
            for volume_id in reversed(volumes):
                self._delete_volume(volume_id)
            self._delete_security_group(worker_sg["id"])
            self._delete_security_group(controller_sg["id"])
            self._delete_keypair(keypair["name"])

        if summary_payload is None:
            raise rally_exceptions.ScriptError(message="fio run did not produce a local summary.json artifact")

        summary = self._summary_dict(summary_payload, artifacts_dir)
        metric_rows = self._metric_rows(summary_payload)
        summary_rows = self._summary_rows(summary_payload)
        self.add_output(
            complete=build_summary_output(summary)
        )
        if metric_rows:
            self.add_output(complete=build_metrics_output(metric_rows))
        self.add_output(
            complete=build_table_output(
                "FIO summary",
                "Aggregated fio benchmark results collected from the controller artifact bundle",
                [
                    "client_nodes",
                    "volumes_per_client",
                    "total_volumes",
                    "profile_name",
                    "rw",
                    "block_size",
                    "numjobs",
                    "iodepth",
                    "throughput",
                    "iops",
                    "avg_latency_ms",
                    "p99_latency_ms",
                ],
                summary_rows,
            )
        )
        self.add_output(
            complete=build_phase_output(self.atomic_actions())
        )
        self.add_output(complete=build_artifacts_output(self._artifact_rows(artifacts_dir)))
        return self._result_payload(artifacts_dir, summary_payload)

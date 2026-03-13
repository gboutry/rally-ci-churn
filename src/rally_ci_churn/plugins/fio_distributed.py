"""Distributed fio benchmark scenario."""

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
from rally.task import types
from rally.task import validation
from rally.utils import sshutils

from rally_openstack.common import consts
from rally_openstack.task import scenario
from rally_openstack.task.scenarios.vm import utils as vm_utils

from rally_ci_churn.results import build_table_output


DEFAULT_FIO_PORT = 8765
SSH_PORT = 22
DEVICE_DISCOVERY_TIMEOUT_SECONDS = 600
DEVICE_POLL_INTERVAL_SECONDS = 2.0
VOLUME_POLL_INTERVAL_SECONDS = 2.0
SERVER_POLL_INTERVAL_SECONDS = 2.0
WORKER_READY_TIMEOUT_SECONDS = 600
ATTACH_RETRY_COUNT = 5
ATTACH_RETRY_DELAY_SECONDS = 5.0


def _as_int_list(values: list[object]) -> list[int]:
    return [int(value) for value in values]


def _as_str_list(values: list[object]) -> list[str]:
    return [str(value) for value in values]


def _case_id(case: dict[str, object]) -> str:
    return (
        f"clients-{case['client_count']}_"
        f"vols-{case['volumes_per_client']}_"
        f"rw-{case['rw_mode']}_"
        f"bs-{str(case['block_size']).replace('/', '-')}_"
        f"numjobs-{case['numjobs']}_"
        f"iodepth-{case['iodepth']}"
    )


class _FioDistributedBase(vm_utils.VMScenario):
    def _task_uuid(self) -> str:
        owner_id = self.context.get("owner_id")
        if owner_id:
            return str(owner_id)
        task = self.context.get("task")
        if isinstance(task, dict) and task.get("uuid"):
            return str(task["uuid"])
        return uuid.uuid4().hex

    def _network_name(self, server) -> str:
        return next(iter(server.networks))

    def _fixed_ip(self, server) -> str:
        for addresses in server.addresses.values():
            for entry in addresses:
                if entry.get("OS-EXT-IPS:type") == "fixed" or "OS-EXT-IPS:type" not in entry:
                    return str(entry["addr"])
        raise rally_exceptions.ScriptError(message=f"Unable to determine a fixed IP for server {server.id}")

    def _tenant_cidr(self) -> str:
        tenant = self.context.get("tenant", {})
        subnets = tenant.get("subnets", [])
        if subnets:
            cidr = subnets[0].get("cidr")
            if cidr:
                return str(cidr)
        raise rally_exceptions.ScriptError(message="network@openstack did not provide a tenant subnet")

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
        protocol: str,
        port_min: int | None = None,
        port_max: int | None = None,
        remote_ip_prefix: str | None = None,
        ethertype: str = "IPv4",
    ) -> None:
        kwargs: dict[str, object] = {
            "security_group_id": security_group_id,
            "protocol": protocol,
            "ethertype": ethertype,
        }
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
            "Allow SSH access to the fio controller VM",
        )
        self._add_security_group_rule(security_group["id"], "tcp", SSH_PORT, SSH_PORT, "0.0.0.0/0")
        self._add_security_group_rule(security_group["id"], "icmp", remote_ip_prefix="0.0.0.0/0")
        return security_group

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

    def _create_volume(self, size: int, volume_type: str | None):
        kwargs: dict[str, object] = {"size": size, "name": self.generate_random_name()}
        if volume_type:
            kwargs["volume_type"] = volume_type
        volume = self.clients("cinder").volumes.create(**kwargs)
        return self._wait_for_volume_status(volume.id, ["available"])

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
                    if volume.status == "attaching":
                        self._wait_for_volume_status(volume_id, ["in-use"], timeout_seconds=120)
                        return self.clients("cinder").volumes.get(volume_id)
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

    def _download_tree(self, ssh: sshutils.SSH, remote_dir: str, local_dir: Path) -> None:
        client = ssh._get_client()  # noqa: SLF001 - Rally's SSH helper does not expose SFTP download.
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
        rw_modes: list[str],
        block_sizes: list[str],
        numjobs: list[int],
        iodepths: list[int],
    ) -> list[dict[str, object]]:
        cases: list[dict[str, object]] = []
        for client_count in client_counts:
            for volumes in volumes_per_client:
                for rw_mode in rw_modes:
                    for block_size in block_sizes:
                        for jobs in numjobs:
                            for depth in iodepths:
                                case = {
                                    "client_count": client_count,
                                    "volumes_per_client": volumes,
                                    "rw_mode": rw_mode,
                                    "block_size": block_size,
                                    "numjobs": jobs,
                                    "iodepth": depth,
                                }
                                case["case_id"] = _case_id(case)
                                cases.append(case)
        return cases

    def _artifacts_dir(self, root_dir: str) -> Path:
        task_dir = Path(root_dir).expanduser().resolve() / self._task_uuid() / "fio-distributed"
        if task_dir.exists():
            shutil.rmtree(task_dir)
        task_dir.mkdir(parents=True, exist_ok=True)
        return task_dir

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

    def _result_payload(self, artifacts_dir: Path, summary_payload: dict[str, object]) -> dict[str, object]:
        return {
            "schema_version": 1,
            "scenario_family": "fio_distributed",
            "scenario_name": "CIChurn.fio_distributed",
            "status": "success",
            "artifact_root": str(artifacts_dir),
            "matrix_cases": len(summary_payload.get("rows", [])),
            "summary_rows": summary_payload.get("rows", []),
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
        volume_size_gib=10,
        volume_type=None,
        client_counts=None,
        volumes_per_client=None,
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
        artifacts_dir = self._artifacts_dir(artifacts_root_dir)
        summary_payload: dict[str, object] | None = None
        controller_remote_dir = f"/var/lib/rally-fio/run/{uuid.uuid4().hex}"

        try:
            controller, controller_fip = self._boot_server_with_fip(
                controller_image,
                controller_flavor,
                use_floating_ip=True,
                floating_network=external_network_name,
                key_name=keypair["name"],
                security_groups=[controller_sg["name"]],
                userdata=self._build_controller_user_data(),
            )
            for _ in range(max_clients):
                worker = self._boot_server(
                    worker_image,
                    worker_flavor,
                    auto_assign_nic=True,
                    key_name=keypair["name"],
                    security_groups=[worker_sg["name"]],
                    userdata=self._build_worker_user_data(max_volumes_per_client, fio_port),
                )
                workers.append({"server": worker, "fixed_ip": self._fixed_ip(worker)})

            device_letters = "bcdefghijklmnopqrstuvwxyz"
            for worker in workers:
                for volume_index in range(max_volumes_per_client):
                    volume = self._create_volume(volume_size_gib, volume_type)
                    volumes.append(volume.id)
                    device_name = f"/dev/vd{device_letters[volume_index]}"
                    self._attach_volume(worker["server"], volume.id, device_name)
                    attachments.append({"server_id": worker["server"].id, "volume_id": volume.id})

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
                    rw_modes,
                    block_sizes,
                    numjobs,
                    iodepths,
                ),
            }
            inventory = self._inventory_payload(workers, fio_port, max_volumes_per_client)
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
                timeout=ssh_connect_timeout_seconds,
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

            exit_status, stdout, stderr = ssh.execute(
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

        summary_rows = self._summary_rows(summary_payload)
        self.add_output(
            complete=build_table_output(
                "FIO summary",
                "Aggregated fio benchmark results collected from the controller artifact bundle",
                [
                    "client_nodes",
                    "volumes_per_client",
                    "total_volumes",
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
            complete=build_table_output(
                "Local artifacts",
                "Artifacts copied back from the fio controller to the Rally host",
                ["path", "value"],
                [["artifact_root", str(artifacts_dir)]],
            )
        )
        return self._result_payload(artifacts_dir, summary_payload)

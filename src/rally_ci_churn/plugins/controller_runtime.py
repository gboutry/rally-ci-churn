"""Shared controller/runtime helpers for controller-driven benchmark scenarios."""

from __future__ import annotations

import socket
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
import shutil
import stat
import time
import uuid
from pathlib import Path
from typing import TypeVar
from typing import cast

from rally import exceptions as rally_exceptions
from rally.task import atomic
from rally.utils import sshutils

from rally_openstack.task.scenarios.vm import utils as vm_utils

from rally_ci_churn.results import summarize_atomic_actions


SSH_PORT = 22
ATTACH_RETRY_COUNT = 5
ATTACH_RETRY_DELAY_SECONDS = 5.0
VOLUME_POLL_INTERVAL_SECONDS = 2.0
HTTP_POOL_BASE_SIZE = 10
HTTP_POOL_HEADROOM = 4

T = TypeVar("T")


def build_root_volume_boot(
    image,
    enabled: bool = False,
    volume_size_gib: int = 20,
    volume_type: str | None = None,
) -> tuple[object, dict[str, object]]:
    if not enabled:
        return image, {}
    image_id = getattr(image, "id", image)
    block_device = {
        "uuid": str(image_id),
        "source_type": "image",
        "destination_type": "volume",
        "boot_index": 0,
        "delete_on_termination": True,
        "volume_size": int(volume_size_gib),
    }
    if volume_type:
        block_device["volume_type"] = volume_type
    return "", {"block_device_mapping_v2": [block_device]}


class ParallelBootMixin:
    def _ensure_http_pool_capacity(self, concurrency: int) -> None:
        if concurrency <= 1:
            return

        target_size = max(HTTP_POOL_BASE_SIZE, int(concurrency) + HTTP_POOL_HEADROOM)
        clients = getattr(self, "_clients", None)
        if clients is None:
            return

        try:
            keystone_session = clients.keystone.get_session()[0]
        except Exception:
            return

        self._resize_session_adapters(keystone_session, target_size)

    def _resize_session_adapters(self, keystone_session, target_size: int) -> None:
        from requests import adapters as requests_adapters

        for scheme, adapter in list(keystone_session.adapters.items()):
            if not isinstance(adapter, requests_adapters.HTTPAdapter):
                continue

            current_pool_connections = int(
                getattr(adapter, "_pool_connections", HTTP_POOL_BASE_SIZE)
            )
            current_pool_maxsize = int(
                getattr(adapter, "_pool_maxsize", HTTP_POOL_BASE_SIZE)
            )
            if (
                current_pool_connections >= target_size
                and current_pool_maxsize >= target_size
            ):
                continue

            replacement = adapter.__class__(
                pool_connections=max(current_pool_connections, target_size),
                pool_maxsize=max(current_pool_maxsize, target_size),
                max_retries=adapter.max_retries,
                pool_block=getattr(adapter, "_pool_block", False),
            )
            keystone_session.mount(scheme, replacement)

    def _resolve_boot_concurrency(
        self,
        count: int,
        concurrency: int | None,
        argument_name: str = "boot_concurrency",
    ) -> int:
        if count < 0:
            raise rally_exceptions.InvalidArgumentsException(
                argument_name="count",
                value=count,
                valid_values="integer >= 0",
            )
        if count == 0:
            return 0
        if concurrency is None:
            return 1
        resolved = int(concurrency)
        if resolved < 1:
            raise rally_exceptions.InvalidArgumentsException(
                argument_name=argument_name,
                value=resolved,
                valid_values="integer >= 1",
            )
        return min(count, resolved)

    def _boot_vm_group(
        self,
        *,
        count: int,
        concurrency: int | None,
        atomic_action_name: str,
        boot_fn: Callable[[int], T],
        destination: list[T],
        argument_name: str = "boot_concurrency",
    ) -> None:
        resolved = self._resolve_boot_concurrency(
            count,
            concurrency,
            argument_name=argument_name,
        )
        if resolved == 0:
            return

        self._ensure_http_pool_capacity(resolved)

        ordered_results: dict[int, T] = {}
        first_error: Exception | None = None

        with atomic.ActionTimer(cast(atomic.ActionTimerMixin, self), atomic_action_name):
            if resolved == 1:
                for index in range(count):
                    ordered_results[index] = boot_fn(index)
            else:
                with ThreadPoolExecutor(
                    max_workers=resolved,
                    thread_name_prefix="rally-vm-boot",
                ) as executor:
                    future_to_index = {
                        executor.submit(boot_fn, index): index for index in range(count)
                    }
                    for future in as_completed(future_to_index):
                        index = future_to_index[future]
                        try:
                            ordered_results[index] = future.result()
                        except Exception as exc:
                            if first_error is None:
                                first_error = exc

        destination.extend(ordered_results[index] for index in sorted(ordered_results))
        if first_error is not None:
            raise first_error

    def _provision_volume_group(
        self,
        *,
        requests: list[dict[str, object]],
        concurrency: int | None,
        volume_ids: list[str],
        attachments: list[dict[str, object]],
        argument_name: str = "volume_concurrency",
    ) -> None:
        resolved = self._resolve_boot_concurrency(
            len(requests),
            concurrency,
            argument_name=argument_name,
        )
        if resolved == 0:
            return

        self._ensure_http_pool_capacity(resolved)

        created_by_index: dict[int, object] = {}
        create_error: Exception | None = None
        create_volume_raw = cast(Callable[[int, str | None], object], getattr(self, "_create_volume_raw"))
        attach_volume_raw = cast(Callable[[object, str, str], object], getattr(self, "_attach_volume_raw"))

        def _create(index: int) -> object:
            request = requests[index]
            return create_volume_raw(
                int(cast(int | str, request["size"])),
                cast(str | None, request.get("volume_type")),
            )

        with atomic.ActionTimer(cast(atomic.ActionTimerMixin, self), "volume.create_group"):
            if resolved == 1:
                for index in range(len(requests)):
                    created_by_index[index] = _create(index)
            else:
                with ThreadPoolExecutor(
                    max_workers=resolved,
                    thread_name_prefix="rally-volume-create",
                ) as executor:
                    future_to_index = {
                        executor.submit(_create, index): index for index in range(len(requests))
                    }
                    for future in as_completed(future_to_index):
                        index = future_to_index[future]
                        try:
                            created_by_index[index] = future.result()
                        except Exception as exc:
                            if create_error is None:
                                create_error = exc

        created_volumes = [created_by_index[index] for index in sorted(created_by_index)]
        volume_ids.extend(str(getattr(volume, "id")) for volume in created_volumes)
        if create_error is not None:
            raise create_error

        attached_by_index: dict[int, dict[str, object]] = {}
        attach_error: Exception | None = None

        def _attach(index: int) -> dict[str, object]:
            request = requests[index]
            volume = created_by_index[index]
            server = request["server"]
            volume_id = str(getattr(volume, "id"))
            server_id = str(getattr(server, "id"))
            attach_volume_raw(
                server,
                volume_id,
                str(request["device_name"]),
            )
            return {
                "server_id": server_id,
                "volume_id": volume_id,
            }

        with atomic.ActionTimer(cast(atomic.ActionTimerMixin, self), "volume.attach_group"):
            if resolved == 1:
                for index in sorted(created_by_index):
                    attached_by_index[index] = _attach(index)
            else:
                with ThreadPoolExecutor(
                    max_workers=resolved,
                    thread_name_prefix="rally-volume-attach",
                ) as executor:
                    future_to_index = {
                        executor.submit(_attach, index): index for index in sorted(created_by_index)
                    }
                    for future in as_completed(future_to_index):
                        index = future_to_index[future]
                        try:
                            attached_by_index[index] = future.result()
                        except Exception as exc:
                            if attach_error is None:
                                attach_error = exc

        attachments.extend(attached_by_index[index] for index in sorted(attached_by_index))
        if attach_error is not None:
            raise attach_error


class ControllerRuntimeBase(ParallelBootMixin, vm_utils.VMScenario):
    """Common OpenStack orchestration helpers for controller-driven scenarios."""

    def _task_uuid(self) -> str:
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

    def _wait_for_volume_status(self, volume_id: str, statuses: list[str], timeout_seconds: int = 1800):
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

    @atomic.action_timer("volume.create")
    def _create_volume(self, size: int, volume_type: str | None):
        return self._create_volume_raw(size, volume_type)

    def _create_volume_raw(self, size: int, volume_type: str | None):
        kwargs: dict[str, object] = {"size": size, "name": self.generate_random_name()}
        if volume_type:
            kwargs["volume_type"] = volume_type
        volume = self.clients("cinder").volumes.create(**kwargs)
        return self._wait_for_volume_status(volume.id, ["available"])

    @atomic.action_timer("volume.attach")
    def _attach_volume(self, server, volume_id: str, device_name: str):
        return self._attach_volume_raw(server, volume_id, device_name)

    def _attach_volume_raw(self, server, volume_id: str, device_name: str):
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

    def _wait_for_ssh_banner(self, ip_address: str, timeout_seconds: int) -> None:
        """Wait until sshd is sending its protocol banner before handing off to paramiko.

        Paramiko logs spurious ERROR messages when it connects before sshd has
        printed its banner (the server accepts the TCP connection but the SSH
        version string is not yet available).  By polling until we can read the
        banner ourselves we avoid that noise without touching paramiko internals.
        """
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            try:
                with socket.create_connection((ip_address, SSH_PORT), timeout=3) as sock:
                    data = sock.recv(64)
                    if data.startswith(b"SSH-"):
                        return
            except OSError:
                pass
            time.sleep(2)

    @atomic.action_timer("controller.connect_ssh")
    def _ssh(
        self,
        ip_address: str,
        username: str,
        private_key: str,
        timeout_seconds: int,
    ) -> sshutils.SSH:
        self._wait_for_ssh_banner(ip_address, timeout_seconds)
        ssh = sshutils.SSH(username, ip_address, port=SSH_PORT, pkey=private_key)
        self._wait_for_ssh(ssh, timeout=timeout_seconds, interval=2)
        return ssh

    @atomic.action_timer("workers.wait_for_tcp_ports")
    def _wait_for_tcp_ports_on_controller(
        self,
        ssh: sshutils.SSH,
        targets: list[tuple[str, int]],
        timeout_seconds: int,
    ) -> None:
        """Wait until all (ip, port) targets are accepting TCP connections.

        Runs a single Python probe script on the controller VM via a single SSH
        channel.  The script uses Python threads to probe all targets in parallel,
        avoiding both sequential O(N) wall time and the concurrent-channel
        ChannelException that arises from calling ssh.run() from multiple threads.
        """
        if not targets:
            return
        probe_script = (
            "import socket, time, threading\n"
            f"targets = {targets!r}\n"
            f"deadline = time.monotonic() + {int(timeout_seconds)}\n"
            "failed = []\n"
            "lock = threading.Lock()\n"
            "def probe(ip, port):\n"
            "    while time.monotonic() < deadline:\n"
            "        try:\n"
            "            s = socket.create_connection((ip, port), timeout=2)\n"
            "            s.close()\n"
            "            return\n"
            "        except Exception:\n"
            "            time.sleep(2)\n"
            "    with lock: failed.append(f'{ip}:{port}')\n"
            "threads = [threading.Thread(target=probe, args=(ip, port)) for ip, port in targets]\n"
            "for t in threads: t.start()\n"
            "for t in threads: t.join()\n"
            "if failed:\n"
            "    raise SystemExit(f'timed out: {failed}')\n"
        )
        exit_status, output = ssh.run(
            ["python3", "-"],
            stdin=probe_script,
            timeout=timeout_seconds + 30,
            raise_on_error=False,
        )
        if exit_status != 0:
            detail = str(output).strip() if output else ""
            raise rally_exceptions.ScriptError(
                message=f"TCP port readiness probe timed out after {timeout_seconds}s: {detail}"
            )

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

    def _timings_payload(self) -> dict[str, dict[str, object]]:
        _, summary = summarize_atomic_actions(self.atomic_actions())
        return summary

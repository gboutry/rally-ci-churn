"""Controller-side network benchmark runner."""

from __future__ import annotations

import argparse
import csv
import json
import shlex
import subprocess
import time
from pathlib import Path

_LOG_PATH: Path | None = None


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run network benchmark cases from the controller VM.")
    parser.add_argument("--inventory", required=True)
    parser.add_argument("--matrix", required=True)
    parser.add_argument("--output-dir", required=True)
    return parser.parse_args()


def _log(message: str) -> None:
    global _LOG_PATH
    if _LOG_PATH is None:
        return
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    with _LOG_PATH.open("a", encoding="utf-8") as stream:
        stream.write(f"{timestamp} {message}\n")


def _ssh_base(identity_file: str) -> list[str]:
    return [
        "ssh",
        "-i",
        identity_file,
        "-o",
        "StrictHostKeyChecking=no",
        "-o",
        "UserKnownHostsFile=/dev/null",
        "-o",
        "BatchMode=yes",
        "-o",
        "ConnectTimeout=10",
    ]


def _ssh_run(
    identity_file: str,
    user: str,
    host: str,
    command: str,
    timeout_seconds: int = 60,
) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            [*_ssh_base(identity_file), f"{user}@{host}", command],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(f"Timed out running SSH command on {host}: {command}") from exc


def _wait_for_ssh(identity_file: str, user: str, host: str, timeout_seconds: int = 600) -> None:
    _log(f"wait_for_ssh start host={host}")
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        result = _ssh_run(identity_file, user, host, "true", timeout_seconds=15)
        if result.returncode == 0:
            _log(f"wait_for_ssh ready host={host}")
            return
        time.sleep(2.0)
    raise RuntimeError(f"Timed out waiting for SSH on {host}")


def _remote_json(identity_file: str, user: str, host: str, command: str) -> dict[str, object]:
    result = _ssh_run(identity_file, user, host, command, timeout_seconds=60)
    if result.returncode != 0:
        raise RuntimeError(f"Remote command failed on {host}: {result.stderr.strip() or result.stdout.strip()}")
    return json.loads(result.stdout)


def _prepare_host(identity_file: str, user: str, host: str) -> None:
    _log(f"prepare_host start host={host}")
    command = (
        "sudo install -d -o {user} -g {user} -m 0755 /var/lib/rally-netbench /var/lib/rally-netbench/raw "
        "&& sudo chown -R {user}:{user} /var/lib/rally-netbench"
    ).format(user=shlex.quote(user))
    result = _ssh_run(identity_file, user, host, command, timeout_seconds=60)
    if result.returncode != 0:
        raise RuntimeError(f"Failed to prepare host {host}: {result.stderr.strip() or result.stdout.strip()}")
    _log(f"prepare_host done host={host}")


def _host_non_root_disks(identity_file: str, user: str, host: str) -> list[str]:
    script = r"""
set -euo pipefail
root_source="$(findmnt -n -o SOURCE / || true)"
root_disk=""
if [ -n "$root_source" ]; then
  root_pkname="$(lsblk -no PKNAME "$root_source" 2>/dev/null || true)"
  if [ -n "$root_pkname" ]; then
    root_disk="/dev/$root_pkname"
  fi
fi
ROOT_DISK="$root_disk" python3 - <<'PY'
import json
import os
import subprocess

root = os.environ.get("ROOT_DISK", "")
output = subprocess.check_output(["lsblk", "-J", "-dnpo", "NAME,TYPE"], text=True)
payload = json.loads(output)
disks = []
for blockdevice in payload.get("blockdevices", []):
    if blockdevice.get("type") != "disk":
        continue
    name = blockdevice.get("name")
    if root and name == root:
        continue
    disks.append(name)
print(json.dumps({"disks": disks}))
PY
"""
    result = _ssh_run(identity_file, user, host, script, timeout_seconds=30)
    if result.returncode != 0:
        raise RuntimeError(f"Failed to list data disks on {host}: {result.stderr.strip() or result.stdout.strip()}")
    payload = json.loads(result.stdout)
    disks = payload.get("disks", [])
    if not isinstance(disks, list):
        return []
    return [str(disk) for disk in disks]


def _wait_for_disks(identity_file: str, user: str, host: str, count: int, timeout_seconds: int = 600) -> list[str]:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        disks = _host_non_root_disks(identity_file, user, host)
        if len(disks) >= count:
            return disks[:count]
        time.sleep(2.0)
    raise RuntimeError(f"Timed out waiting for {count} non-root disks on {host}")


def _start_background(identity_file: str, user: str, host: str, command: str) -> None:
    _log(f"start_background host={host} command={command}")
    wrapped = f"nohup bash -lc {shlex.quote(command)} >/dev/null 2>&1 &"
    result = _ssh_run(identity_file, user, host, wrapped, timeout_seconds=30)
    if result.returncode != 0:
        raise RuntimeError(f"Failed to start background command on {host}: {result.stderr.strip() or result.stdout.strip()}")


def _kill_matching(identity_file: str, user: str, host: str, pattern: str) -> None:
    _ssh_run(identity_file, user, host, f"pkill -f {shlex.quote(pattern)} || true")


def _wait_for_tcp(identity_file: str, user: str, host: str, port: int, timeout_seconds: int = 120) -> None:
    _log(f"wait_for_tcp start host={host} port={port}")
    deadline = time.monotonic() + timeout_seconds
    command = (
        "python3 -c \"import socket; "
        f"sock=socket.create_connection(('127.0.0.1',{port}),2); sock.close()\""
    )
    while time.monotonic() < deadline:
        result = _ssh_run(identity_file, user, host, command, timeout_seconds=10)
        if result.returncode == 0:
            _log(f"wait_for_tcp ready host={host} port={port}")
            return
        time.sleep(1.0)
    raise RuntimeError(f"Timed out waiting for TCP port {port} on {host}")


def _iperf_server_command(port: int, protocol: str) -> str:
    return f"iperf3 -s -p {port}"


def _iperf_client_command(
    server_ip: str,
    port: int,
    duration_seconds: int,
    ramp_time_seconds: int,
    protocol: str,
    reverse: bool,
    parallel_streams: int,
    udp_target_mbps: int | None,
) -> str:
    timeout_seconds = int(duration_seconds) + int(ramp_time_seconds) + 60
    parts = [
        "timeout",
        "--signal=TERM",
        "--kill-after=10",
        f"{timeout_seconds}s",
        "iperf3",
        "-J",
        "-c",
        shlex.quote(server_ip),
        "-p",
        str(port),
        "-t",
        str(duration_seconds),
        "-O",
        str(ramp_time_seconds),
    ]
    if reverse:
        parts.append("-R")
    if protocol == "udp":
        parts.extend(["-u", "-b", f"{int(udp_target_mbps or 0)}M"])
    else:
        parts.extend(["-P", str(parallel_streams)])
    return " ".join(parts)


def _parse_iperf_json(text: str, protocol: str) -> dict[str, float]:
    payload = json.loads(text)
    end = payload.get("end", {})
    if not isinstance(end, dict):
        raise RuntimeError("iperf3 JSON payload does not contain an end section")
    if protocol == "udp":
        summary = end.get("sum") or end.get("sum_received") or {}
        if not isinstance(summary, dict):
            summary = {}
        return {
            "throughput_bits_per_sec": float(summary.get("bits_per_second", 0.0)),
            "retransmits": 0.0,
            "jitter_ms": float(summary.get("jitter_ms", 0.0)),
            "lost_percent": float(summary.get("lost_percent", 0.0)),
        }
    received = end.get("sum_received") or {}
    sent = end.get("sum_sent") or {}
    if not isinstance(received, dict):
        received = {}
    if not isinstance(sent, dict):
        sent = {}
    return {
        "throughput_bits_per_sec": float(received.get("bits_per_second", sent.get("bits_per_second", 0.0))),
        "retransmits": float(sent.get("retransmits", 0.0)),
        "jitter_ms": 0.0,
        "lost_percent": 0.0,
    }


def _run_iperf_many_to_one(
    inventory: dict[str, object],
    matrix: dict[str, object],
    output_dir: Path,
) -> list[dict[str, object]]:
    _log("many_to_one start")
    traffic = matrix["traffic"]
    duration_seconds = int(traffic["duration_seconds"])
    ramp_time_seconds = int(traffic["ramp_time_seconds"])
    flow_direction = str(matrix["many_to_one"]["flow_direction"])
    identity_file = str(output_dir / "id_rsa")
    ssh_user = str(inventory["ssh_user"])
    server = inventory["server"]
    clients = inventory["clients"]
    if not isinstance(server, dict) or not isinstance(clients, list):
        raise RuntimeError("Invalid many-to-one inventory payload")
    server_ip = str(server["fixed_ip"])
    server_host = str(server["fixed_ip"])
    base_port = int(traffic.get("base_port", 5201))
    for host in [server, *clients]:
        _wait_for_ssh(identity_file, ssh_user, str(host["fixed_ip"]))
        _prepare_host(identity_file, ssh_user, str(host["fixed_ip"]))
    rows: list[dict[str, object]] = []
    raw_dir = output_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    for case in matrix["cases"]:
        if case.get("mode") != "iperf3":
            continue
        protocol = str(case["protocol"])
        reverse = flow_direction == "server_to_client"
        ports = [base_port + index for index in range(len(clients))]
        for port in ports:
            _kill_matching(identity_file, ssh_user, server_host, f"iperf3 -s -p {port}")
            _start_background(identity_file, ssh_user, server_host, _iperf_server_command(port, protocol))
            _wait_for_tcp(identity_file, ssh_user, server_host, port)
        case_id = str(case["case_id"])
        _log(f"many_to_one case start case_id={case_id}")
        metrics: list[dict[str, float]] = []
        for index, client in enumerate(clients):
            command = _iperf_client_command(
                server_ip,
                ports[index],
                duration_seconds,
                ramp_time_seconds,
                protocol,
                reverse,
                int(case.get("parallel_streams", 1)),
                int(case.get("udp_target_mbps", 0)) if protocol == "udp" else None,
            )
            result = _ssh_run(
                identity_file,
                ssh_user,
                str(client["fixed_ip"]),
                command,
                timeout_seconds=duration_seconds + ramp_time_seconds + 90,
            )
            _log(f"many_to_one client done case_id={case_id} client={client['name']} rc={result.returncode}")
            stdout_path = raw_dir / f"{case_id}_{client['name']}.stdout"
            stdout_path.write_text(result.stdout, encoding="utf-8")
            if result.returncode != 0:
                raise RuntimeError(
                    f"iperf3 client {client['name']} failed for {case_id}: {result.stderr.strip() or result.stdout.strip()}"
                )
            json_path = raw_dir / f"{case_id}_{client['name']}.json"
            json_path.write_text(json.dumps(json.loads(result.stdout), indent=2, sort_keys=True), encoding="utf-8")
            metrics.append(_parse_iperf_json(result.stdout, protocol))
        total_bps = sum(entry["throughput_bits_per_sec"] for entry in metrics)
        row = {
            "case_id": case_id,
            "mode": "iperf3",
            "protocol": protocol,
            "client_count": len(clients),
            "parallel_streams": int(case.get("parallel_streams", 1)) if protocol == "tcp" else "",
            "udp_target_mbps": int(case.get("udp_target_mbps", 0)) if protocol == "udp" else "",
            "throughput_bits_per_sec": total_bps,
            "throughput_mbps": total_bps / 1_000_000.0,
            "avg_client_mbps": (total_bps / 1_000_000.0) / len(metrics) if metrics else 0.0,
            "max_client_mbps": max((entry["throughput_bits_per_sec"] / 1_000_000.0 for entry in metrics), default=0.0),
            "retransmits": sum(entry["retransmits"] for entry in metrics),
            "jitter_ms": max((entry["jitter_ms"] for entry in metrics), default=0.0),
            "lost_percent": max((entry["lost_percent"] for entry in metrics), default=0.0),
            "success_rate": 1.0,
        }
        rows.append(row)
        for port in ports:
            _kill_matching(identity_file, ssh_user, server_host, f"iperf3 -s -p {port}")
        _log(f"many_to_one case done case_id={case_id}")
    _log("many_to_one done")
    return rows


def _prepare_http_volume(
    identity_file: str,
    user: str,
    server_host: str,
    file_count: int,
    file_size_mib: int,
    listen_port: int,
) -> None:
    disks = _wait_for_disks(identity_file, user, server_host, 1)
    disk = disks[0]
    command = f"""
set -euo pipefail
sudo mkfs.ext4 -F {shlex.quote(disk)}
sudo install -d -m 0755 /srv/netbench
if mountpoint -q /srv/netbench; then
  sudo umount /srv/netbench
fi
sudo mount {shlex.quote(disk)} /srv/netbench
sudo chown {shlex.quote(user)}:{shlex.quote(user)} /srv/netbench
rm -f /srv/netbench/file-*.bin
for index in $(seq 1 {file_count}); do
  fallocate -l {file_size_mib}M /srv/netbench/file-$(printf "%02d" "$index").bin
done
pkill -f 'python3 -m http.server {listen_port}' || true
nohup python3 -m http.server {listen_port} --directory /srv/netbench >/var/lib/rally-netbench/http.log 2>&1 &
"""
    result = _ssh_run(identity_file, user, server_host, command)
    if result.returncode != 0:
        raise RuntimeError(f"Failed to prepare HTTP server on {server_host}: {result.stderr.strip() or result.stdout.strip()}")
    _wait_for_tcp(identity_file, user, server_host, listen_port)


def _run_http_many_to_one(
    inventory: dict[str, object],
    matrix: dict[str, object],
    output_dir: Path,
) -> list[dict[str, object]]:
    _log("http_many_to_one start")
    identity_file = str(output_dir / "id_rsa")
    ssh_user = str(inventory["ssh_user"])
    server = inventory["server"]
    clients = inventory["clients"]
    if not isinstance(server, dict) or not isinstance(clients, list):
        raise RuntimeError("Invalid many-to-one inventory payload")
    server_host = str(server["fixed_ip"])
    server_ip = str(server["fixed_ip"])
    for host in [server, *clients]:
        _wait_for_ssh(identity_file, ssh_user, str(host["fixed_ip"]))
        _prepare_host(identity_file, ssh_user, str(host["fixed_ip"]))
    http_cfg = matrix["http_volume"]
    file_count = int(http_cfg["file_count"])
    file_size_mib = int(http_cfg["file_size_mib"])
    listen_port = int(matrix["traffic"].get("http_port", 8080))
    _prepare_http_volume(identity_file, ssh_user, server_host, file_count, file_size_mib, listen_port)
    end_at = time.time() + int(matrix["traffic"]["duration_seconds"])
    raw_dir = output_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    metrics = []
    files = [f"file-{index:02d}.bin" for index in range(1, file_count + 1)]
    for client in clients:
        script = f"""
import json
import subprocess
import time

end_at = {int(end_at)}
files = {files!r}
url_prefix = "http://{server_ip}:{listen_port}"
durations = []
bytes_total = 0
requests = 0
failures = 0
index = 0
while time.time() < end_at:
    target = files[index % len(files)]
    started = time.time()
    proc = subprocess.run(
        [
            "curl",
            "-sS",
            "-o",
            "/dev/null",
            "-w",
            "%{{size_download}} %{{time_total}}",
            f"{{url_prefix}}/{{target}}",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if proc.returncode != 0:
        failures += 1
    else:
        size_text, time_text = proc.stdout.strip().split()
        bytes_total += int(float(size_text))
        durations.append(float(time_text))
        requests += 1
    index += 1
payload = {{
    "requests": requests,
    "failures": failures,
    "bytes_total": bytes_total,
    "durations": durations,
}}
print(json.dumps(payload))
"""
        result = _ssh_run(
            identity_file,
            ssh_user,
            str(client["fixed_ip"]),
            f"python3 - <<'PY'\n{script}\nPY",
            timeout_seconds=int(matrix["traffic"]["duration_seconds"]) + 60,
        )
        stdout_path = raw_dir / f"http_{client['name']}.stdout"
        stdout_path.write_text(result.stdout, encoding="utf-8")
        if result.returncode != 0:
            raise RuntimeError(f"HTTP client {client['name']} failed: {result.stderr.strip() or result.stdout.strip()}")
        payload = json.loads(result.stdout)
        metrics.append(payload)
        (raw_dir / f"http_{client['name']}.json").write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    total_bytes = sum(int(entry["bytes_total"]) for entry in metrics)
    total_requests = sum(int(entry["requests"]) for entry in metrics)
    total_failures = sum(int(entry["failures"]) for entry in metrics)
    all_durations = [float(value) for entry in metrics for value in entry.get("durations", [])]
    avg_duration = sum(all_durations) / len(all_durations) if all_durations else 0.0
    ordered = sorted(all_durations)
    p95 = ordered[int(round((len(ordered) - 1) * 0.95))] if ordered else 0.0
    p99 = ordered[int(round((len(ordered) - 1) * 0.99))] if ordered else 0.0
    duration_seconds = int(matrix["traffic"]["duration_seconds"])
    row = {
        "case_id": "http-volume",
        "mode": "http_volume",
        "protocol": "http",
        "client_count": len(clients),
        "parallel_streams": "",
        "udp_target_mbps": "",
        "throughput_bits_per_sec": (total_bytes * 8.0) / duration_seconds if duration_seconds else 0.0,
        "throughput_mbps": ((total_bytes * 8.0) / duration_seconds) / 1_000_000.0 if duration_seconds else 0.0,
        "avg_client_mbps": (((total_bytes * 8.0) / duration_seconds) / 1_000_000.0) / len(clients) if duration_seconds and clients else 0.0,
        "max_client_mbps": 0.0,
        "retransmits": 0.0,
        "jitter_ms": 0.0,
        "lost_percent": 0.0,
        "success_rate": total_requests / (total_requests + total_failures) if (total_requests + total_failures) else 0.0,
        "requests": total_requests,
        "bytes_total": total_bytes,
        "avg_duration_seconds": avg_duration,
        "p95_duration_seconds": p95,
        "p99_duration_seconds": p99,
    }
    return [row]


def _build_ring_flows(hosts: list[dict[str, object]], neighbors_per_vm: int, bidirectional: bool) -> list[dict[str, object]]:
    flows = []
    count = len(hosts)
    for index, source in enumerate(hosts):
        for offset in range(1, neighbors_per_vm + 1):
            dest = hosts[(index + offset) % count]
            flows.append({"source": source, "dest": dest})
            if bidirectional:
                flows.append({"source": dest, "dest": source})
    deduped = []
    seen: set[tuple[str, str]] = set()
    for flow in flows:
        key = (str(flow["source"]["name"]), str(flow["dest"]["name"]))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(flow)
    return deduped


def _run_ring(
    inventory: dict[str, object],
    matrix: dict[str, object],
    output_dir: Path,
) -> list[dict[str, object]]:
    _log("ring start")
    identity_file = str(output_dir / "id_rsa")
    ssh_user = str(inventory["ssh_user"])
    participants = inventory["participants"]
    if not isinstance(participants, list):
        raise RuntimeError("Invalid ring inventory payload")
    for host in participants:
        _wait_for_ssh(identity_file, ssh_user, str(host["fixed_ip"]))
        _prepare_host(identity_file, ssh_user, str(host["fixed_ip"]))
    traffic = matrix["traffic"]
    duration_seconds = int(traffic["duration_seconds"])
    ramp_time_seconds = int(traffic["ramp_time_seconds"])
    base_port = int(traffic.get("base_port", 5201))
    raw_dir = output_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    rows = []
    for case in matrix["cases"]:
        protocol = str(case["protocol"])
        flows = _build_ring_flows(participants, int(case["neighbors_per_vm"]), bool(case["bidirectional"]))
        for index, flow in enumerate(flows):
            port = base_port + index
            flow["port"] = port
            dest_ip = str(flow["dest"]["fixed_ip"])
            _kill_matching(identity_file, ssh_user, dest_ip, f"iperf3 -s -p {port}")
            _start_background(identity_file, ssh_user, dest_ip, _iperf_server_command(port, protocol))
            _wait_for_tcp(identity_file, ssh_user, dest_ip, port)
        metrics = []
        case_id = str(case["case_id"])
        _log(f"ring case start case_id={case_id}")
        for flow in flows:
            source_ip = str(flow["source"]["fixed_ip"])
            command = _iperf_client_command(
                str(flow["dest"]["fixed_ip"]),
                int(flow["port"]),
                duration_seconds,
                ramp_time_seconds,
                protocol,
                False,
                int(case.get("parallel_streams", 1)),
                int(case.get("udp_target_mbps", 0)) if protocol == "udp" else None,
            )
            result = _ssh_run(
                identity_file,
                ssh_user,
                source_ip,
                command,
                timeout_seconds=duration_seconds + ramp_time_seconds + 90,
            )
            _log(
                f"ring flow done case_id={case_id} source={flow['source']['name']} dest={flow['dest']['name']} rc={result.returncode}"
            )
            raw_name = f"{case_id}_{flow['source']['name']}_to_{flow['dest']['name']}"
            (raw_dir / f"{raw_name}.stdout").write_text(result.stdout, encoding="utf-8")
            if result.returncode != 0:
                raise RuntimeError(
                    f"Ring flow {flow['source']['name']}->{flow['dest']['name']} failed: {result.stderr.strip() or result.stdout.strip()}"
                )
            payload = json.loads(result.stdout)
            (raw_dir / f"{raw_name}.json").write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
            metric = _parse_iperf_json(result.stdout, protocol)
            metric["source"] = flow["source"]["name"]
            metric["dest"] = flow["dest"]["name"]
            metrics.append(metric)
        total_bps = sum(entry["throughput_bits_per_sec"] for entry in metrics)
        node_totals: dict[str, float] = {}
        for metric in metrics:
            node_totals[str(metric["source"])] = node_totals.get(str(metric["source"]), 0.0) + float(metric["throughput_bits_per_sec"])
        node_values = sorted(node_totals.values())
        row = {
            "case_id": case_id,
            "mode": "ring",
            "protocol": protocol,
            "participant_count": len(participants),
            "neighbor_flows_per_vm": int(case["neighbors_per_vm"]),
            "bidirectional": bool(case["bidirectional"]),
            "parallel_streams": int(case.get("parallel_streams", 1)) if protocol == "tcp" else "",
            "udp_target_mbps": int(case.get("udp_target_mbps", 0)) if protocol == "udp" else "",
            "flow_count": len(metrics),
            "throughput_bits_per_sec": total_bps,
            "throughput_mbps": total_bps / 1_000_000.0,
            "avg_flow_mbps": (total_bps / 1_000_000.0) / len(metrics) if metrics else 0.0,
            "max_flow_mbps": max((entry["throughput_bits_per_sec"] / 1_000_000.0 for entry in metrics), default=0.0),
            "retransmits": sum(entry["retransmits"] for entry in metrics),
            "jitter_ms": max((entry["jitter_ms"] for entry in metrics), default=0.0),
            "lost_percent": max((entry["lost_percent"] for entry in metrics), default=0.0),
            "imbalance_ratio": (node_values[-1] / node_values[0]) if len(node_values) > 1 and node_values[0] else 1.0,
        }
        rows.append(row)
        for flow in flows:
            _kill_matching(identity_file, ssh_user, str(flow["dest"]["fixed_ip"]), f"iperf3 -s -p {int(flow['port'])}")
        _log(f"ring case done case_id={case_id}")
    _log("ring done")
    return rows


def _format_markdown_table(
    headers: list[str],
    rows: list[list[object]],
) -> list[str]:
    """Build a column-aligned Markdown table."""
    str_rows = [[str(cell) for cell in row] for row in rows]
    widths = [len(h) for h in headers]
    for row in str_rows:
        for i, cell in enumerate(row):
            if i < len(widths):
                widths[i] = max(widths[i], len(cell))

    def _fmt(cells: list[str]) -> str:
        padded = [cell.ljust(widths[i]) for i, cell in enumerate(cells)]
        return "| " + " | ".join(padded) + " |"

    lines = [_fmt(headers)]
    lines.append("|" + "|".join("-" * (w + 2) for w in widths) + "|")
    for row in str_rows:
        lines.append(_fmt(row))
    return lines


def _write_markdown(output_dir: Path, scenario_slug: str, rows: list[dict[str, object]]) -> None:
    if scenario_slug == "net-many-to-one":
        headers = [
            "Case", "Mode", "Protocol", "Clients", "Throughput (Mb/s)",
            "Avg Client (Mb/s)", "Max Client (Mb/s)", "Retransmits",
            "Jitter (ms)", "Lost %", "Success Rate",
        ]
        table_rows = [
            [
                row["case_id"], row["mode"], row["protocol"],
                row["client_count"], f"{row['throughput_mbps']:.2f}",
                f"{row['avg_client_mbps']:.2f}", f"{row['max_client_mbps']:.2f}",
                f"{row['retransmits']:.0f}", f"{row['jitter_ms']:.2f}",
                f"{row['lost_percent']:.2f}", f"{row['success_rate']:.3f}",
            ]
            for row in rows
        ]
    else:
        headers = [
            "Case", "Protocol", "Participants", "Flows", "Throughput (Mb/s)",
            "Avg Flow (Mb/s)", "Max Flow (Mb/s)", "Retransmits",
            "Jitter (ms)", "Lost %", "Imbalance Ratio",
        ]
        table_rows = [
            [
                row["case_id"], row["protocol"], row["participant_count"],
                row["flow_count"], f"{row['throughput_mbps']:.2f}",
                f"{row['avg_flow_mbps']:.2f}", f"{row['max_flow_mbps']:.2f}",
                f"{row['retransmits']:.0f}", f"{row['jitter_ms']:.2f}",
                f"{row['lost_percent']:.2f}", f"{row['imbalance_ratio']:.3f}",
            ]
            for row in rows
        ]
    lines = ["## Summary Table", ""]
    lines.extend(_format_markdown_table(headers, table_rows))
    (output_dir / "summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_csv(output_dir: Path, rows: list[dict[str, object]]) -> None:
    if not rows:
        return
    with (output_dir / "summary.csv").open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    args = _parse_args()
    inventory = json.loads(Path(args.inventory).read_text(encoding="utf-8"))
    matrix = json.loads(Path(args.matrix).read_text(encoding="utf-8"))
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    global _LOG_PATH
    _LOG_PATH = output_dir / "controller_runner.progress.log"
    _log("main start")

    scenario_slug = str(matrix["scenario_slug"])
    rows: list[dict[str, object]]
    if scenario_slug == "net-many-to-one":
        if matrix["traffic"]["mode"] == "http_volume":
            rows = _run_http_many_to_one(inventory, matrix, output_dir)
        else:
            rows = _run_iperf_many_to_one(inventory, matrix, output_dir)
    elif scenario_slug == "net-ring":
        rows = _run_ring(inventory, matrix, output_dir)
    else:
        raise RuntimeError(f"Unsupported network benchmark scenario slug: {scenario_slug}")

    summary_payload = {
        "scenario_slug": scenario_slug,
        "inventory": inventory,
        "matrix": matrix,
        "rows": rows,
    }
    (output_dir / "summary.json").write_text(json.dumps(summary_payload, indent=2, sort_keys=True), encoding="utf-8")
    (output_dir / "manifest.json").write_text(
        json.dumps(
            {
                "scenario_slug": scenario_slug,
                "artifact_root": str(output_dir),
                "files": sorted(str(path.relative_to(output_dir)) for path in output_dir.rglob("*") if path.is_file()),
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    _write_csv(output_dir, rows)
    _write_markdown(output_dir, scenario_slug, rows)
    _log("main done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

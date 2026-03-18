"""Controller-side fio matrix runner."""

from __future__ import annotations

import argparse
import json
import subprocess
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

FIO_CLIENT_GROUP_SIZE = 20

BUILTIN_PROFILES = {
    "mixed-workload": {
        "rw_mode": "randrw",
        "block_size": "64k",
        "job_name": "mixed-workload",
        "global_options": {
            "rwmixread": "50",
            "log_avg_msec": "1000",
        },
    },
    "db-workload": {
        "rw_mode": "randrw",
        "block_size": "4k",
        "job_name": "db-workload",
        "global_options": {
            "rwmixread": "70",
            "random_distribution": "zipf:0.99",
            "log_avg_msec": "1000",
        },
    },
    "throughput-seqwrite": {
        "rw_mode": "write",
        "block_size": "1M",
        "job_name": "throughput-seqwrite",
        "global_options": {
            "log_avg_msec": "1000",
        },
    },
    "latency-randwrite": {
        "rw_mode": "randwrite",
        "block_size": "4k",
        "job_name": "latency-randwrite",
        "global_options": {
            "log_avg_msec": "1000",
        },
    },
}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a fio matrix from the controller VM.")
    parser.add_argument("--inventory", required=True)
    parser.add_argument("--matrix", required=True)
    parser.add_argument("--output-dir", required=True)
    return parser.parse_args()


def _write_jobfile(
    path: Path,
    case: dict[str, object],
    runtime_seconds: int,
    ramp_time_seconds: int,
    ioengine: str,
    volumes_per_client: int,
    prefill: bool,
) -> None:
    rw_mode = str(case["rw_mode"])
    block_size = str(case["block_size"])
    numjobs = int(case["numjobs"])
    iodepth = int(case["iodepth"])
    profile_name = str(case.get("profile_name") or "custom")
    profile_options = case.get("profile_options", {})
    if not isinstance(profile_options, dict):
        profile_options = {}
    log_prefix = f"{profile_name}-iodepth-{iodepth}-numjobs-{numjobs}"
    global_lines = [
        "[global]",
        f"ioengine={ioengine}",
        "direct=1",
        "thread=1",
        "group_reporting=1",
        "time_based=1" if not prefill else "time_based=0",
        f"runtime={runtime_seconds}" if not prefill else "",
        f"ramp_time={ramp_time_seconds}" if not prefill else "",
        f"rw={rw_mode if not prefill else 'write'}",
        f"bs={block_size if not prefill else '1M'}",
        f"numjobs={numjobs if not prefill else 1}",
        f"iodepth={iodepth if not prefill else 1}",
        "norandommap=1",
        "randrepeat=0",
        "size=100%",
        "invalidate=1",
        f"write_bw_log={log_prefix}",
        f"write_iops_log={log_prefix}",
        f"write_lat_log={log_prefix}",
        "",
    ]
    for key, value in profile_options.items():
        global_lines.insert(-1, f"{key}={value}")
    job_lines = [line for line in global_lines if line]
    job_name = str(case.get("job_name") or "workload")
    for index in range(volumes_per_client):
        job_lines.extend(
            [
                f"[{job_name}-vol{index + 1:02d}]",
                f"filename=/var/lib/rally-fio/devices/vol{index + 1:02d}",
                "",
            ]
        )
    path.write_text("\n".join(job_lines), encoding="utf-8")


def _read_direction_stats(payload: dict[str, object], rw_mode: str) -> dict[str, object]:
    jobs = payload.get("jobs", [])
    client_stats = payload.get("client_stats", [])
    entries = jobs if isinstance(jobs, list) and jobs else client_stats if isinstance(client_stats, list) else []
    if rw_mode in {"rw", "randrw", "readwrite", "randreadwrite"}:
        directions = ["read", "write"]
    else:
        directions = ["read" if "read" in rw_mode else "write"]
    bandwidth = 0.0
    iops = 0.0
    latencies_ms: list[float] = []
    p99_ms = 0.0
    for job in entries:
        if not isinstance(job, dict):
            continue
        if job.get("jobname") == "All clients":
            continue
        for direction in directions:
            stats = job.get(direction, {})
            if not isinstance(stats, dict):
                continue
            bandwidth += float(stats.get("bw_bytes", 0.0))
            iops += float(stats.get("iops", 0.0))
            clat_ns = stats.get("clat_ns", {})
            if isinstance(clat_ns, dict):
                if "mean" in clat_ns:
                    latencies_ms.append(float(clat_ns.get("mean", 0.0)) / 1_000_000.0)
                percentiles = clat_ns.get("percentile", {})
                if isinstance(percentiles, dict):
                    p99_raw = percentiles.get("99.000000") or percentiles.get("99.00")
                    if p99_raw:
                        p99_ms = max(p99_ms, float(p99_raw) / 1_000_000.0)
    average_latency = sum(latencies_ms) / len(latencies_ms) if latencies_ms else 0.0
    return {
        "throughput_bytes_per_sec": bandwidth,
        "iops": iops,
        "avg_latency_ms": average_latency,
        "p99_latency_ms": p99_ms,
    }


def _human_bw(bytes_per_sec: float) -> str:
    value = float(bytes_per_sec)
    units = ["B/s", "KiB/s", "MiB/s", "GiB/s", "TiB/s"]
    unit = units[0]
    for candidate in units[1:]:
        if value < 1024.0:
            break
        value /= 1024.0
        unit = candidate
    precision = 1 if unit.startswith("Gi") or unit.startswith("Ti") else 0
    return f"{value:.{precision}f} {unit}"


def _human_iops(iops: float) -> str:
    value = float(iops)
    if value >= 1000.0:
        return f"{value / 1000.0:.1f}k"
    return f"{value:.0f}"


def _extract_json_text(text: str) -> str | None:
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return None
    return text[start : end + 1]


def _split_hostfile(
    hostfile: Path, group_size: int, output_dir: Path, case_id: str,
) -> list[Path]:
    lines = [line for line in hostfile.read_text(encoding="utf-8").splitlines() if line.strip()]
    if len(lines) <= group_size:
        return [hostfile]
    groups: list[Path] = []
    for i in range(0, len(lines), group_size):
        chunk = lines[i : i + group_size]
        group_path = output_dir / f"{case_id}.clients.{len(groups):02d}"
        group_path.write_text("\n".join(chunk) + "\n", encoding="utf-8")
        groups.append(group_path)
    return groups


def _run_fio_single_group(
    group_hostfile: Path, jobfile: Path,
) -> tuple[int, str]:
    result = subprocess.run(
        [
            "fio",
            "--output-format=json+",
            "--eta=never",
            f"--client={group_hostfile}",
            str(jobfile),
        ],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    return (result.returncode, result.stdout)


def _run_fio_grouped(
    group_hostfiles: list[Path], jobfile: Path,
) -> list[tuple[int, str]]:
    if len(group_hostfiles) == 1:
        return [_run_fio_single_group(group_hostfiles[0], jobfile)]
    with ThreadPoolExecutor(max_workers=len(group_hostfiles)) as pool:
        futures = [
            pool.submit(_run_fio_single_group, hf, jobfile)
            for hf in group_hostfiles
        ]
        return [f.result() for f in futures]


def _merge_fio_payloads(payloads: list[dict[str, object]]) -> dict[str, object]:
    if len(payloads) == 1:
        return payloads[0]
    merged = dict(payloads[0])
    all_client_stats: list[object] = []
    all_jobs: list[object] = []
    all_disk_util: list[object] = []
    for p in payloads:
        for entry in p.get("client_stats", []):
            if isinstance(entry, dict) and entry.get("jobname") == "All clients":
                continue
            all_client_stats.append(entry)
        for entry in p.get("jobs", []):
            if isinstance(entry, dict) and entry.get("jobname") == "All clients":
                continue
            all_jobs.append(entry)
        all_disk_util.extend(p.get("disk_util", []))
    merged["client_stats"] = all_client_stats
    merged["jobs"] = all_jobs
    if all_disk_util:
        merged["disk_util"] = all_disk_util
    return merged


def _collect_grouped_stdout(
    group_results: list[tuple[int, str]], group_hostfiles: list[Path],
) -> str:
    parts: list[str] = []
    for idx, ((rc, stdout), hf) in enumerate(zip(group_results, group_hostfiles)):
        n_clients = len([l for l in hf.read_text(encoding="utf-8").splitlines() if l.strip()])
        parts.append(f"=== Group {idx} ({hf.name}, {n_clients} clients, exit={rc}) ===")
        parts.append(stdout)
    return "\n".join(parts)


def _load_case_payload(json_path: Path, stdout_text: str, case_id: str) -> dict[str, object]:
    candidates: list[str] = []
    if json_path.exists():
        file_text = json_path.read_text(encoding="utf-8", errors="replace").strip()
        if file_text:
            candidates.append(file_text)
    stdout_text = stdout_text.strip()
    if stdout_text:
        json_from_stdout = _extract_json_text(stdout_text)
        if json_from_stdout:
            candidates.append(json_from_stdout)
    for candidate in candidates:
        try:
            payload = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        json_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        return payload
    snippet = stdout_text[-1200:] if stdout_text else "<empty>"
    raise RuntimeError(f"fio did not produce valid JSON for {case_id}. Output tail:\n{snippet}")


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


def _write_summary_markdown(output_dir: Path, rows: list[dict[str, object]]) -> None:
    headers = [
        "Client Nodes", "Volumes/Client", "Total Volumes", "Profile", "RW",
        "Block Size", "NumJobs", "IoDepth", "Throughput (BW)", "IOPS",
        "Avg Latency (ms)", "99th Percentile Latency (ms)",
    ]
    table_rows = [
        [
            row["client_nodes"], row["volumes_per_client"], row["total_volumes"],
            row["profile_name"], row["rw_mode"], row["block_size"],
            row["numjobs"], row["iodepth"], row["throughput_human"],
            row["iops_human"], f"{row['avg_latency_ms']:.2f}",
            f"{row['p99_latency_ms']:.2f}",
        ]
        for row in rows
    ]
    lines = ["## Summary Table", ""]
    lines.extend(_format_markdown_table(headers, table_rows))
    lines.append("")
    for row in rows:
        case_id = str(row["case_id"])
        stdout_path = output_dir / "raw" / f"{case_id}.stdout"
        lines.extend(
            [
                f"## {case_id}",
                "",
                f"- Clients: {row['client_nodes']}",
                f"- Volumes/client: {row['volumes_per_client']}",
                f"- Profile: {row['profile_name']}",
                f"- RW: {row['rw_mode']}",
                f"- Block size: {row['block_size']}",
                f"- NumJobs: {row['numjobs']}",
                f"- IoDepth: {row['iodepth']}",
                "",
                "```text",
                stdout_path.read_text(encoding='utf-8', errors='replace').strip(),
                "```",
                "",
            ]
        )
    (output_dir / "summary.md").write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    args = _parse_args()
    inventory = json.loads(Path(args.inventory).read_text(encoding="utf-8"))
    matrix = json.loads(Path(args.matrix).read_text(encoding="utf-8"))
    output_dir = Path(args.output_dir)
    raw_dir = output_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    workers = inventory["workers"]
    fio_port = int(inventory["fio_port"])
    results: list[dict[str, object]] = []
    prefilled_slices: set[tuple[int, int]] = set()

    for case in matrix["cases"]:
        client_nodes = int(case["client_count"])
        volumes_per_client = int(case["volumes_per_client"])
        selected_workers = workers[:client_nodes]
        case_id = str(case["case_id"])
        case_job = output_dir / f"{case_id}.fio"
        hostfile = output_dir / f"{case_id}.clients"
        hostfile.write_text(
            "\n".join(f"{worker['fixed_ip']},{fio_port}" for worker in selected_workers) + "\n",
            encoding="utf-8",
        )
        group_hostfiles = _split_hostfile(hostfile, FIO_CLIENT_GROUP_SIZE, output_dir, case_id)

        slice_key = (client_nodes, volumes_per_client)
        if case["rw_mode"] in {"read", "randread", "randwrite"} and slice_key not in prefilled_slices:
            prefill_job = output_dir / f"prefill-{client_nodes}-{volumes_per_client}.fio"
            _write_jobfile(
                prefill_job,
                {
                    "rw_mode": "write",
                    "block_size": "1M",
                    "numjobs": 1,
                    "iodepth": 1,
                    "profile_name": "prefill",
                    "profile_options": {},
                    "job_name": "prefill-job",
                },
                0,
                0,
                str(matrix["ioengine"]),
                volumes_per_client,
                True,
            )
            prefill_results = _run_fio_grouped(group_hostfiles, prefill_job)
            for rc, stdout in prefill_results:
                if rc != 0:
                    raise RuntimeError(f"fio prefill failed for {case_id}: {stdout}")
            prefilled_slices.add(slice_key)

        _write_jobfile(
            case_job,
            case,
            int(matrix["runtime_seconds"]),
            int(matrix["ramp_time_seconds"]),
            str(matrix["ioengine"]),
            volumes_per_client,
            False,
        )
        stdout_path = raw_dir / f"{case_id}.stdout"
        group_results = _run_fio_grouped(group_hostfiles, case_job)
        combined_stdout = _collect_grouped_stdout(group_results, group_hostfiles)
        stdout_path.write_text(combined_stdout, encoding="utf-8")
        for rc, stdout in group_results:
            if rc != 0:
                raise RuntimeError(f"fio failed for {case_id}: {combined_stdout}")
        json_path = raw_dir / f"{case_id}.json"
        if len(group_results) == 1:
            payload = _load_case_payload(json_path, group_results[0][1], case_id)
        else:
            payloads: list[dict[str, object]] = []
            for group_idx, (_rc, stdout) in enumerate(group_results):
                group_json = raw_dir / f"{case_id}.group{group_idx:02d}.json"
                group_label = f"{case_id} group {group_idx}"
                payloads.append(_load_case_payload(group_json, stdout, group_label))
            payload = _merge_fio_payloads(payloads)
            json_path.write_text(
                json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8",
            )
        stats = _read_direction_stats(payload, str(case["rw_mode"]))
        results.append(
            {
                "case_id": case_id,
                "client_nodes": client_nodes,
                "volumes_per_client": volumes_per_client,
                "total_volumes": client_nodes * volumes_per_client,
                "profile_name": case.get("profile_name") or "custom",
                "rw_mode": case["rw_mode"],
                "block_size": case["block_size"],
                "numjobs": int(case["numjobs"]),
                "iodepth": int(case["iodepth"]),
                "throughput_bytes_per_sec": stats["throughput_bytes_per_sec"],
                "throughput_human": _human_bw(float(stats["throughput_bytes_per_sec"])),
                "iops": round(float(stats["iops"]), 2),
                "iops_human": _human_iops(float(stats["iops"])),
                "avg_latency_ms": round(float(stats["avg_latency_ms"]), 2),
                "p99_latency_ms": round(float(stats["p99_latency_ms"]), 2),
            }
        )

    summary = {
        "schema_version": 1,
        "inventory": inventory,
        "matrix": matrix,
        "rows": results,
    }
    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    (output_dir / "summary.csv").write_text(
        "\n".join(
            [
                "case_id,client_nodes,volumes_per_client,total_volumes,profile_name,rw_mode,block_size,numjobs,iodepth,throughput_bytes_per_sec,iops,avg_latency_ms,p99_latency_ms"
            ]
            + [
                ",".join(
                    [
                        str(row["case_id"]),
                        str(row["client_nodes"]),
                        str(row["volumes_per_client"]),
                        str(row["total_volumes"]),
                        str(row["profile_name"]),
                        str(row["rw_mode"]),
                        str(row["block_size"]),
                        str(row["numjobs"]),
                        str(row["iodepth"]),
                        str(row["throughput_bytes_per_sec"]),
                        str(row["iops"]),
                        str(row["avg_latency_ms"]),
                        str(row["p99_latency_ms"]),
                    ]
                )
                for row in results
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (output_dir / "manifest.json").write_text(
        json.dumps(
            {
                "summary": ["summary.md", "summary.csv", "summary.json"],
                "inventory": "inventory.json",
                "raw_dir": "raw",
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    (output_dir / "inventory.json").write_text(json.dumps(inventory, indent=2, sort_keys=True), encoding="utf-8")
    _write_summary_markdown(output_dir, results)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

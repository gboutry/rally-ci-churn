"""Controller-side fio matrix runner."""

from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a fio matrix from the controller VM.")
    parser.add_argument("--inventory", required=True)
    parser.add_argument("--matrix", required=True)
    parser.add_argument("--output-dir", required=True)
    return parser.parse_args()


def _write_jobfile(
    path: Path,
    rw_mode: str,
    block_size: str,
    numjobs: int,
    iodepth: int,
    runtime_seconds: int,
    ramp_time_seconds: int,
    ioengine: str,
    volumes_per_client: int,
    prefill: bool,
) -> None:
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
        "",
    ]
    job_lines = [line for line in global_lines if line]
    for index in range(volumes_per_client):
        job_lines.extend(
            [
                f"[vol{index + 1:02d}]",
                f"filename=/var/lib/rally-fio/devices/vol{index + 1:02d}",
                "",
            ]
        )
    path.write_text("\n".join(job_lines), encoding="utf-8")


def _read_direction_stats(payload: dict[str, object], rw_mode: str) -> dict[str, object]:
    jobs = payload.get("jobs", [])
    client_stats = payload.get("client_stats", [])
    entries = jobs if isinstance(jobs, list) and jobs else client_stats if isinstance(client_stats, list) else []
    direction = "read" if "read" in rw_mode else "write"
    bandwidth = 0.0
    iops = 0.0
    latencies_ms: list[float] = []
    p99_ms = 0.0
    for job in entries:
        if not isinstance(job, dict):
            continue
        if job.get("jobname") == "All clients":
            continue
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


def _write_summary_markdown(output_dir: Path, rows: list[dict[str, object]]) -> None:
    lines = [
        "## Summary Table",
        "",
        "| Client Nodes | Volumes/Client | Total Volumes | RW | Block Size | NumJobs | IoDepth | Throughput (BW) | IOPS | Avg Latency (ms) | 99th Percentile Latency (ms) |",
        "|--------------|----------------|---------------|----|------------|---------|---------|-----------------|------|------------------|-----------------------------|",
    ]
    for row in rows:
        lines.append(
            "| {client_nodes} | {volumes_per_client} | {total_volumes} | {rw_mode} | {block_size} | {numjobs} | {iodepth} | {throughput_human} | {iops_human} | {avg_latency_ms:.2f} | {p99_latency_ms:.2f} |".format(
                **row
            )
        )
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
        remote_args = [
            "fio",
            "--output-format=json+",
            *[f"--client={worker['fixed_ip']},{fio_port}" for worker in selected_workers],
        ]

        slice_key = (client_nodes, volumes_per_client)
        if case["rw_mode"] in {"read", "randread", "randwrite"} and slice_key not in prefilled_slices:
            prefill_job = output_dir / f"prefill-{client_nodes}-{volumes_per_client}.fio"
            _write_jobfile(
                prefill_job,
                "write",
                "1M",
                1,
                1,
                0,
                0,
                str(matrix["ioengine"]),
                volumes_per_client,
                True,
            )
            subprocess.run(
                [*remote_args, str(prefill_job)],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
            )
            prefilled_slices.add(slice_key)

        _write_jobfile(
            case_job,
            str(case["rw_mode"]),
            str(case["block_size"]),
            int(case["numjobs"]),
            int(case["iodepth"]),
            int(matrix["runtime_seconds"]),
            int(matrix["ramp_time_seconds"]),
            str(matrix["ioengine"]),
            volumes_per_client,
            False,
        )
        stdout_path = raw_dir / f"{case_id}.stdout"
        completed = subprocess.run(
            [*remote_args, str(case_job)],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        stdout_path.write_text(completed.stdout, encoding="utf-8")
        if completed.returncode != 0:
            raise RuntimeError(f"fio failed for {case_id}: {completed.stdout}")
        json_path = raw_dir / f"{case_id}.json"
        payload = _load_case_payload(json_path, completed.stdout, case_id)
        stats = _read_direction_stats(payload, str(case["rw_mode"]))
        results.append(
            {
                "case_id": case_id,
                "client_nodes": client_nodes,
                "volumes_per_client": volumes_per_client,
                "total_volumes": client_nodes * volumes_per_client,
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
                "case_id,client_nodes,volumes_per_client,total_volumes,rw_mode,block_size,numjobs,iodepth,throughput_bytes_per_sec,iops,avg_latency_ms,p99_latency_ms"
            ]
            + [
                ",".join(
                    [
                        str(row["case_id"]),
                        str(row["client_nodes"]),
                        str(row["volumes_per_client"]),
                        str(row["total_volumes"]),
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

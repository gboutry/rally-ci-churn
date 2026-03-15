"""Run percentage-based capacity sweeps across Rally benchmark scenarios."""

from __future__ import annotations

import argparse
import copy
import json
import math
import os
import re
import stat
import subprocess
import tempfile
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml

from rally_ci_churn.bootstrap import sunbeam


DEFAULT_LEVELS = [10, 25, 40, 60, 80]
DEFAULT_SCENARIOS = ["spiky", "fio-distributed", "net-many-to-one", "net-ring", "mixed-pressure"]
DEFAULT_CLUSTER = {
    "total_vcpus": 416 * 3,
    "total_ram_gib": 2048 * 3,
    "geneve_bandwidth_gbps": 200.0,
    "ceph_bandwidth_gbps": 200.0,
}
DEFAULT_CALIBRATION = {
    "fio_runtime_seconds": 20,
    "net_runtime_seconds": 15,
    "net_parallel_streams": 2,
    "assumed_rates": {
        "fio_worker_gbps": 2.0,
        "many_to_one_client_gbps": 10.0,
        "ring_participant_gbps": 20.0,
    },
}
DEFAULT_LIMITS = {
    "max_vm_count": 0,
    "max_volume_count": 0,
    "max_floating_ips": 1,
}
RESULT_UUID_RE = re.compile(r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b")
PRACTICAL_CLIENT_THRESHOLD = 64
PRACTICAL_RING_THRESHOLD = 64
PRACTICAL_FIO_WORKER_THRESHOLD = 32


@dataclass(frozen=True)
class ScenarioSpec:
    sweep_name: str
    preset: str
    scenario_name: str
    task_path: str
    flavor_field: str | None
    local_artifacts: bool


SCENARIOS: dict[str, ScenarioSpec] = {
    "spiky": ScenarioSpec(
        sweep_name="spiky",
        preset="spiky",
        scenario_name="CIChurn.spiky_autonomous_vm",
        task_path="tasks/spiky_autonomous_vm.yaml.j2",
        flavor_field="flavor_name",
        local_artifacts=False,
    ),
    "fio-distributed": ScenarioSpec(
        sweep_name="fio-distributed",
        preset="fio-distributed",
        scenario_name="CIChurn.fio_distributed",
        task_path="tasks/fio_distributed.yaml.j2",
        flavor_field="worker_flavor_name",
        local_artifacts=True,
    ),
    "net-many-to-one": ScenarioSpec(
        sweep_name="net-many-to-one",
        preset="net-many-to-one",
        scenario_name="CIChurn.net_many_to_one",
        task_path="tasks/net_many_to_one.yaml.j2",
        flavor_field="client_flavor_name",
        local_artifacts=True,
    ),
    "net-ring": ScenarioSpec(
        sweep_name="net-ring",
        preset="net-ring",
        scenario_name="CIChurn.net_ring",
        task_path="tasks/net_ring.yaml.j2",
        flavor_field="participant_flavor_name",
        local_artifacts=True,
    ),
    "mixed-pressure": ScenarioSpec(
        sweep_name="mixed-pressure",
        preset="mixed-pressure",
        scenario_name="CIChurn.mixed_pressure",
        task_path="tasks/mixed_pressure.yaml.j2",
        flavor_field="fixed_group_flavor_name",
        local_artifacts=True,
    ),
}

CALIBRATION_RATE_KEYS = {
    "fio-distributed": ("fio_worker_gbps",),
    "net-many-to-one": ("many_to_one_client_gbps",),
    "net-ring": ("ring_participant_gbps",),
}


def _load_yaml(path: Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        raise RuntimeError(f"Sweep config must be a mapping: {path}")
    return data


def _merge_dicts(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = copy.deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _merge_dicts(merged[key], value)
        else:
            merged[key] = value
    return merged


def _parse_levels(raw: str | None) -> list[int]:
    if not raw:
        return DEFAULT_LEVELS
    values = [int(item.strip()) for item in raw.split(",") if item.strip()]
    if not values:
        raise RuntimeError("At least one level is required.")
    return values


def _parse_scenarios(raw: str | None) -> list[str]:
    if not raw:
        return DEFAULT_SCENARIOS
    values = [item.strip() for item in raw.split(",") if item.strip()]
    unsupported = sorted(set(values) - set(SCENARIOS))
    if unsupported:
        raise RuntimeError(f"Unsupported sweep scenarios: {unsupported!r}")
    return values


def _round_down(value: float) -> int:
    return max(1, int(math.floor(value)))


def _now_stamp() -> str:
    return datetime.now(tz=UTC).strftime("%Y%m%d-%H%M%S")


def _run_rally(*args: str) -> str:
    command = os.environ.get("RALLY_CI_CHURN_RALLY_BIN", "rally")
    result = subprocess.run(
        [command, *args],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    return result.stdout


def _extract_task_id(output: str) -> str:
    matches = RESULT_UUID_RE.findall(output)
    if not matches:
        raise RuntimeError(f"Unable to determine Rally task id from output:\n{output}")
    return matches[-1]


def _extract_json_blob(text: str) -> Any:
    stripped = text.strip()
    if stripped:
        try:
            return json.loads(stripped)
        except json.JSONDecodeError:
            pass
    for marker in ("{", "["):
        index = text.find(marker)
        if index >= 0:
            try:
                return json.loads(text[index:])
            except json.JSONDecodeError:
                continue
    raise RuntimeError("Unable to parse JSON from Rally output.")


def _report_task_payload(task_id: str, scenario_name: str) -> dict[str, Any] | None:
    output = _run_rally("task", "report", task_id, "--json")
    report = _extract_json_blob(output)
    matches: list[dict[str, Any]] = []

    def _walk(node: Any) -> None:
        if isinstance(node, dict):
            if node.get("scenario_name") == scenario_name and "summary" in node:
                matches.append(node)
            for value in node.values():
                _walk(value)
        elif isinstance(node, list):
            for item in node:
                _walk(item)

    _walk(report)
    return matches[-1] if matches else None


def _run_openstack(clouds_yaml: Path, cloud_name: str, *args: str) -> str:
    return sunbeam._run_openstack(clouds_yaml, cloud_name, *args)  # noqa: SLF001


def _flavor_details(clouds_yaml: Path, flavor_name: str) -> dict[str, Any]:
    return json.loads(_run_openstack(clouds_yaml, "sunbeam-admin", "flavor", "show", "-f", "json", flavor_name))


def _pick_stress_ng_image(clouds_yaml: Path) -> str:
    return sunbeam._pick_custom_image(clouds_yaml, "ubuntu-stress-ng")  # noqa: SLF001


def _pick_stress_ng_flavor(clouds_yaml: Path) -> str:
    return sunbeam._pick_preferred_flavor(clouds_yaml, ("m1.stress-ng", "m1.netbench", "m1.benchmark", "m1.small"))  # noqa: SLF001


def _normalize_clouds_for_sweep(clouds_yaml: Path) -> tuple[dict[str, Any], Path, tempfile.TemporaryDirectory[str]]:
    config = sunbeam._normalize_clouds(clouds_yaml)  # noqa: SLF001
    temp_dir = tempfile.TemporaryDirectory(prefix="rally-ci-sweep-clouds-")
    normalized_clouds_yaml = Path(temp_dir.name) / "clouds.yaml"
    normalized_clouds_yaml.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
    return config, normalized_clouds_yaml, temp_dir


def _build_base_args(
    clouds_yaml: Path,
    config: dict[str, Any],
    scenario: str,
    *,
    overrides: dict[str, str] | None = None,
) -> tuple[dict[str, Any], str]:
    spec = SCENARIOS[scenario]
    image_override = (overrides or {}).get("image_name")
    flavor_override = (overrides or {}).get("flavor_name")
    args, task_path = sunbeam.build_preset(
        spec.preset,
        clouds_yaml,
        config,
        image_override=image_override,
        flavor_override=flavor_override,
    )
    if scenario == "spiky":
        args["description"] = "Spiky stress-ng sweep scenario"
        if image_override:
            args["cloud"]["image_name"] = image_override
        else:
            args["cloud"]["image_name"] = _pick_stress_ng_image(clouds_yaml)
        if flavor_override:
            args["cloud"]["flavor_name"] = flavor_override
        else:
            args["cloud"]["flavor_name"] = _pick_stress_ng_flavor(clouds_yaml)
        args["workload"] = {
            "profile": "stress_ng",
            "params": {
                "duration_seconds": 300,
                "cpu_workers": 1,
                "vm_workers": 1,
                "vm_bytes": "512M",
            },
        }
    return args, task_path


def _apply_artifact_root(args_data: dict[str, Any], sweep_artifacts_root: Path) -> None:
    if "artifacts" in args_data and isinstance(args_data["artifacts"], dict):
        args_data["artifacts"]["root_dir"] = str(sweep_artifacts_root)


def _capacity_from_flavor(cluster: dict[str, Any], flavor: dict[str, Any]) -> dict[str, int]:
    vcpus = int(flavor["vcpus"])
    ram_gib = max(1.0, int(flavor["ram"]) / 1024.0)
    cpu_capacity = int(cluster["total_vcpus"] // vcpus)
    ram_capacity = int(cluster["total_ram_gib"] // ram_gib)
    return {
        "cpu_capacity": max(1, cpu_capacity),
        "ram_capacity": max(1, ram_capacity),
        "vm_capacity": max(1, min(cpu_capacity, ram_capacity)),
    }


def _limit(value: int, max_value: int) -> int:
    if max_value > 0:
        return max(1, min(value, max_value))
    return max(1, value)


def _plan_spiky(
    args_data: dict[str, Any],
    cluster: dict[str, Any],
    clouds_yaml: Path,
    level: int,
    limits: dict[str, Any],
) -> dict[str, Any]:
    flavor = _flavor_details(clouds_yaml, str(args_data["cloud"]["flavor_name"]))
    capacity = _capacity_from_flavor(cluster, flavor)
    max_active = _limit(_round_down(capacity["vm_capacity"] * (level / 100.0)), int(limits["max_vm_count"]))
    fill_minutes = 10
    baseline_lpm = max(1, math.ceil(max_active / fill_minutes))
    duration_seconds = max(900, fill_minutes * 60 + 300)
    args_data["schedule"]["max_active_vms"] = max_active
    args_data["schedule"]["baseline_launches_per_minute"] = baseline_lpm
    args_data["schedule"]["duration_seconds"] = duration_seconds
    args_data["schedule"]["burst_windows"] = [
        {
            "start_second": duration_seconds // 3,
            "end_second": duration_seconds // 3 + 120,
            "launch_rate_multiplier": 2.0,
        }
    ]
    args_data["workload"]["params"]["duration_seconds"] = max(300, duration_seconds - 120)
    return {
        "capacity": capacity,
        "planned_max_active_vms": max_active,
        "baseline_launches_per_minute": baseline_lpm,
        "duration_seconds": duration_seconds,
    }


def _plan_fio(
    args_data: dict[str, Any],
    cluster: dict[str, Any],
    clouds_yaml: Path,
    level: int,
    limits: dict[str, Any],
    calibration: dict[str, Any],
) -> dict[str, Any]:
    flavor = _flavor_details(clouds_yaml, str(args_data["cloud"]["worker_flavor_name"]))
    capacity = _capacity_from_flavor(cluster, flavor)
    calibrated_worker_gbps = max(0.001, float(calibration["fio_worker_gbps"]))
    target_gbps = float(cluster["ceph_bandwidth_gbps"]) * (level / 100.0)
    workers = math.ceil(target_gbps / calibrated_worker_gbps)
    workers = _limit(workers, int(limits["max_vm_count"]))
    workers = min(workers, capacity["vm_capacity"])
    volumes_per_client = 1
    if workers > PRACTICAL_FIO_WORKER_THRESHOLD:
        workers = math.ceil(workers / 2)
        volumes_per_client = 2
    if int(limits["max_volume_count"]) > 0:
        max_workers_by_volumes = max(1, int(limits["max_volume_count"]) // volumes_per_client)
        workers = min(workers, max_workers_by_volumes)
    args_data["fio"]["client_counts"] = [max(1, workers)]
    args_data["fio"]["volumes_per_client"] = [volumes_per_client]
    args_data["fio"]["numjobs"] = [1]
    args_data["fio"]["iodepths"] = [1]
    args_data["fio"]["runtime_seconds"] = 120
    args_data["fio"]["ramp_time_seconds"] = 10
    return {
        "capacity": capacity,
        "target_ceph_gbps": round(target_gbps, 3),
        "calibrated_worker_gbps": round(calibrated_worker_gbps, 3),
        "planned_workers": max(1, workers),
        "planned_volumes_per_client": volumes_per_client,
    }


def _plan_many_to_one(
    args_data: dict[str, Any],
    cluster: dict[str, Any],
    clouds_yaml: Path,
    level: int,
    limits: dict[str, Any],
    calibration: dict[str, Any],
) -> dict[str, Any]:
    flavor = _flavor_details(clouds_yaml, str(args_data["cloud"]["client_flavor_name"]))
    capacity = _capacity_from_flavor(cluster, flavor)
    calibrated_client_gbps = max(0.001, float(calibration["many_to_one_client_gbps"]))
    target_gbps = float(cluster["geneve_bandwidth_gbps"]) * (level / 100.0)
    clients = math.ceil(target_gbps / calibrated_client_gbps)
    clients = _limit(clients, int(limits["max_vm_count"]))
    clients = min(clients, capacity["vm_capacity"])
    parallel_streams = 2
    if clients > PRACTICAL_CLIENT_THRESHOLD:
        parallel_streams = max(2, math.ceil(clients / PRACTICAL_CLIENT_THRESHOLD))
        clients = PRACTICAL_CLIENT_THRESHOLD
    args_data["many_to_one"]["client_count"] = max(1, clients)
    args_data["traffic"]["protocols"] = ["tcp"]
    args_data["traffic"]["parallel_streams"] = [parallel_streams]
    args_data["traffic"]["duration_seconds"] = 120
    args_data["traffic"]["ramp_time_seconds"] = 10
    return {
        "capacity": capacity,
        "target_geneve_gbps": round(target_gbps, 3),
        "calibrated_client_gbps": round(calibrated_client_gbps, 3),
        "planned_clients": max(1, clients),
        "planned_parallel_streams": parallel_streams,
    }


def _plan_ring(
    args_data: dict[str, Any],
    cluster: dict[str, Any],
    clouds_yaml: Path,
    level: int,
    limits: dict[str, Any],
    calibration: dict[str, Any],
) -> dict[str, Any]:
    flavor = _flavor_details(clouds_yaml, str(args_data["cloud"]["participant_flavor_name"]))
    capacity = _capacity_from_flavor(cluster, flavor)
    calibrated_participant_gbps = max(0.001, float(calibration["ring_participant_gbps"]))
    target_gbps = float(cluster["geneve_bandwidth_gbps"]) * (level / 100.0)
    participants = math.ceil(target_gbps / calibrated_participant_gbps)
    participants = _limit(participants, int(limits["max_vm_count"]))
    participants = min(participants, capacity["vm_capacity"])
    parallel_streams = 2
    if participants > PRACTICAL_RING_THRESHOLD:
        parallel_streams = max(2, math.ceil(participants / PRACTICAL_RING_THRESHOLD))
        participants = PRACTICAL_RING_THRESHOLD
    args_data["ring"]["participant_count"] = max(2, participants)
    args_data["traffic"]["protocols"] = ["tcp"]
    args_data["traffic"]["parallel_streams"] = [parallel_streams]
    args_data["traffic"]["duration_seconds"] = 120
    args_data["traffic"]["ramp_time_seconds"] = 10
    return {
        "capacity": capacity,
        "target_geneve_gbps": round(target_gbps, 3),
        "calibrated_participant_gbps": round(calibrated_participant_gbps, 3),
        "planned_participants": max(2, participants),
        "planned_parallel_streams": parallel_streams,
    }


def _plan_mixed(
    args_data: dict[str, Any],
    derived_level_plans: dict[str, dict[str, Any]],
    limits: dict[str, Any],
) -> dict[str, Any]:
    spiky = derived_level_plans["spiky"]
    fio = derived_level_plans["fio-distributed"]
    many = derived_level_plans["net-many-to-one"]
    ring = derived_level_plans["net-ring"]
    churn_max = _limit(max(1, round(int(spiky["planned_max_active_vms"]) * 0.35)), int(limits["max_vm_count"]))
    fio_workers = _limit(max(1, round(int(fio["planned_workers"]) * 0.25)), int(limits["max_vm_count"]))
    many_clients = _limit(max(1, round(int(many["planned_clients"]) * 0.2)), int(limits["max_vm_count"]))
    ring_participants = _limit(max(2, round(int(ring["planned_participants"]) * 0.2)), int(limits["max_vm_count"]))
    volumes_per_client = int(fio["planned_volumes_per_client"])
    if int(limits["max_volume_count"]) > 0:
        max_workers_by_volumes = max(1, int(limits["max_volume_count"]) // max(1, volumes_per_client))
        fio_workers = min(fio_workers, max_workers_by_volumes)
    args_data["mixed"]["duration_seconds"] = 300
    args_data["churn"]["max_active_vms"] = churn_max
    args_data["churn"]["baseline_launches_per_minute"] = max(1, math.ceil(churn_max / 8))
    args_data["churn"]["burst_windows"] = [{"start_second": 90, "end_second": 180, "launch_rate_multiplier": 2.0}]
    args_data["churn"]["workload_params"]["duration_seconds"] = 180
    args_data["churn"]["workload_params"]["cpu_workers"] = 1
    args_data["churn"]["workload_params"]["vm_workers"] = 1
    args_data["churn"]["workload_params"]["vm_bytes"] = "512M"
    args_data["fio"]["client_counts"] = [fio_workers]
    args_data["fio"]["volumes_per_client"] = [volumes_per_client]
    args_data["fio"]["numjobs"] = [1]
    args_data["fio"]["iodepths"] = [1]
    args_data["fio"]["runtime_seconds"] = 180
    args_data["fio"]["ramp_time_seconds"] = 10
    args_data["many_to_one"]["client_count"] = many_clients
    args_data["many_to_one"]["protocols"] = ["tcp"]
    args_data["many_to_one"]["parallel_streams"] = [max(1, int(many["planned_parallel_streams"]))]
    args_data["many_to_one"]["duration_seconds"] = 180
    args_data["many_to_one"]["ramp_time_seconds"] = 10
    args_data["ring"]["participant_count"] = ring_participants
    args_data["ring"]["protocols"] = ["tcp"]
    args_data["ring"]["parallel_streams"] = [max(1, int(ring["planned_parallel_streams"]))]
    args_data["ring"]["duration_seconds"] = 180
    args_data["ring"]["ramp_time_seconds"] = 10
    return {
        "planned_churn_max_active_vms": churn_max,
        "planned_fio_workers": fio_workers,
        "planned_many_to_one_clients": many_clients,
        "planned_ring_participants": ring_participants,
        "planned_volumes_per_client": volumes_per_client,
    }


def _write_yaml(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")
    path.chmod(stat.S_IRUSR | stat.S_IWUSR)


def _write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")


def _write_markdown(path: Path, manifest: dict[str, Any]) -> None:
    lines = ["# Capacity Sweep Summary", ""]
    lines.append(f"- started_at: `{manifest['started_at']}`")
    lines.append(f"- output_root: `{manifest['output_root']}`")
    lines.append("")
    for scenario_name in manifest["scenario_order"]:
        lines.append(f"## {scenario_name}")
        lines.append("")
        lines.append("| Level | Status | Task | Planned | Measured | Artifacts |")
        lines.append("|---|---|---|---|---|---|")
        for run in manifest["runs"].get(scenario_name, []):
            planned = json.dumps(run.get("sizing", {}), sort_keys=True)
            measured = json.dumps(run.get("measured", {}), sort_keys=True)
            lines.append(
                "| "
                f"{run.get('level_label', '')} | "
                f"{run.get('status', '')} | "
                f"{run.get('task_id', '')} | "
                f"`{planned}` | "
                f"`{measured}` | "
                f"`{run.get('artifact_root', '')}` |"
            )
        lines.append("")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _extract_measured_metrics(scenario: str, payload: dict[str, Any] | None) -> dict[str, Any]:
    if not payload:
        return {}
    if scenario == "spiky":
        summary = payload.get("summary", {})
        metrics = payload.get("metrics", {})
        return {
            "launched_vms": summary.get("launched_vms", summary.get("count", "")),
            "completed_vms": summary.get("completed_vms", ""),
            "failed_vms": summary.get("failed_vms", ""),
            "timed_out_vms": summary.get("timed_out_vms", ""),
            "peak_active_vms": summary.get("peak_active_vms", ""),
            "success_rate": summary.get("success_rate", ""),
            "duration_p50_seconds": metrics.get("duration_stats_seconds", {}).get("p50", ""),
        }
    if scenario == "fio-distributed":
        aggregates = payload.get("metrics", {}).get("aggregates", {})
        return {
            "best_throughput_bytes_per_sec": aggregates.get("best_throughput_bytes_per_sec", ""),
            "best_iops": aggregates.get("best_iops", ""),
            "worst_p99_latency_ms": aggregates.get("worst_p99_latency_ms", ""),
        }
    if scenario in {"net-many-to-one", "net-ring"}:
        rows = payload.get("metrics", {}).get("rows", [])
        throughput_values = [float(row.get("throughput_mbps", 0.0)) for row in rows if isinstance(row, dict)]
        retransmits = [float(row.get("retransmits", 0.0)) for row in rows if isinstance(row, dict)]
        return {
            "best_throughput_mbps": round(max(throughput_values, default=0.0), 3),
            "total_retransmits": round(sum(retransmits), 3),
            "row_count": len(rows),
        }
    if scenario == "mixed-pressure":
        return payload.get("metrics", {}).get("aggregates", {})
    return {}


def _artifact_root_from_payload(payload: dict[str, Any] | None) -> str:
    if not payload:
        return ""
    artifacts = payload.get("artifacts", {})
    if isinstance(artifacts, dict) and artifacts.get("artifact_root"):
        return str(artifacts["artifact_root"])
    summary = payload.get("summary", {})
    if isinstance(summary, dict) and summary.get("artifact_root"):
        return str(summary["artifact_root"])
    if payload.get("artifact_root"):
        return str(payload["artifact_root"])
    return ""


def _write_manifest(manifest_path: Path, summary_path: Path, manifest: dict[str, Any]) -> None:
    _write_json(manifest_path, manifest)
    _write_markdown(summary_path, manifest)


def _missing_calibration_keys(calibration: dict[str, Any], keys: tuple[str, ...]) -> list[str]:
    missing = []
    for key in keys:
        value = float(calibration.get(key, 0.0) or 0.0)
        if value <= 0.0:
            missing.append(key)
    return missing


def _run_task(task_path: str, args_path: Path, scenario_name: str) -> tuple[str, dict[str, Any] | None]:
    _run_rally("task", "validate", task_path, "--task-args-file", str(args_path))
    output = _run_rally("task", "start", task_path, "--task-args-file", str(args_path))
    task_id = _extract_task_id(output)
    payload = _report_task_payload(task_id, scenario_name)
    return task_id, payload


def _calibration_args_fio(base_args: dict[str, Any], runtime_seconds: int) -> dict[str, Any]:
    args_data = copy.deepcopy(base_args)
    args_data["fio"]["client_counts"] = [1]
    args_data["fio"]["volumes_per_client"] = [1]
    args_data["fio"]["profile_names"] = [args_data["fio"].get("profile_names", ["mixed-workload"])[0]]
    args_data["fio"]["numjobs"] = [1]
    args_data["fio"]["iodepths"] = [1]
    args_data["fio"]["runtime_seconds"] = runtime_seconds
    args_data["fio"]["ramp_time_seconds"] = max(2, runtime_seconds // 5)
    return args_data


def _calibration_args_many_to_one(base_args: dict[str, Any], runtime_seconds: int, parallel_streams: int) -> dict[str, Any]:
    args_data = copy.deepcopy(base_args)
    args_data["many_to_one"]["client_count"] = 2
    args_data["traffic"]["mode"] = "iperf3"
    args_data["traffic"]["protocols"] = ["tcp"]
    args_data["traffic"]["parallel_streams"] = [parallel_streams]
    args_data["traffic"]["duration_seconds"] = runtime_seconds
    args_data["traffic"]["ramp_time_seconds"] = max(2, runtime_seconds // 5)
    return args_data


def _calibration_args_ring(base_args: dict[str, Any], runtime_seconds: int, parallel_streams: int) -> dict[str, Any]:
    args_data = copy.deepcopy(base_args)
    args_data["ring"]["participant_count"] = 4
    args_data["ring"]["neighbors_per_vm"] = 1
    args_data["traffic"]["protocols"] = ["tcp"]
    args_data["traffic"]["parallel_streams"] = [parallel_streams]
    args_data["traffic"]["duration_seconds"] = runtime_seconds
    args_data["traffic"]["ramp_time_seconds"] = max(2, runtime_seconds // 5)
    return args_data


def _compute_calibration_rates(
    scenario: str,
    payload: dict[str, Any] | None,
) -> dict[str, float]:
    measured = _extract_measured_metrics(scenario, payload)
    if scenario == "fio-distributed":
        bytes_per_sec = float(measured.get("best_throughput_bytes_per_sec", 0.0) or 0.0)
        return {"fio_worker_gbps": (bytes_per_sec * 8.0) / 1_000_000_000.0}
    if scenario == "net-many-to-one":
        throughput_mbps = float(measured.get("best_throughput_mbps", 0.0) or 0.0)
        return {"many_to_one_client_gbps": throughput_mbps / 1000.0 / 2.0}
    if scenario == "net-ring":
        throughput_mbps = float(measured.get("best_throughput_mbps", 0.0) or 0.0)
        return {"ring_participant_gbps": throughput_mbps / 1000.0 / 4.0}
    return {}


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a percentage-based Rally benchmark capacity sweep.")
    parser.add_argument("--clouds-yaml", required=True)
    parser.add_argument("--config")
    parser.add_argument("--levels", help="Comma-separated percentage levels. Default: 10,25,40,60,80")
    parser.add_argument("--scenarios", help="Comma-separated scenario names to sweep.")
    parser.add_argument("--output-dir", help="Sweep output root. Default: sweeps/<timestamp>")
    parser.add_argument("--generate-only", action="store_true")
    parser.add_argument("--cluster-vcpus", type=int)
    parser.add_argument("--cluster-ram-gib", type=int)
    parser.add_argument("--cluster-geneve-gbps", type=float)
    parser.add_argument("--cluster-ceph-gbps", type=float)
    parser.add_argument("--max-vm-count", type=int, default=None)
    parser.add_argument("--max-volume-count", type=int, default=None)
    parser.add_argument("--deployment-name", help="Recorded into the manifest for operator traceability.")
    parser.add_argument("--image", help="Override all scenario image names with this image.")
    parser.add_argument("--flavor", help="Override all scenario flavor names with this flavor.")
    return parser


def _resolve_runtime_config(args: argparse.Namespace) -> dict[str, Any]:
    config = {
        "cluster": copy.deepcopy(DEFAULT_CLUSTER),
        "levels": list(DEFAULT_LEVELS),
        "scenarios": list(DEFAULT_SCENARIOS),
        "calibration": copy.deepcopy(DEFAULT_CALIBRATION),
        "limits": copy.deepcopy(DEFAULT_LIMITS),
        "execution": {
            "mode": "generate_and_run",
            "failure_policy": "continue",
            "deployment_name": args.deployment_name or "",
        },
        "overrides": {},
    }
    config = _merge_dicts(config, _load_yaml(Path(args.config).resolve()) if args.config else {})
    config["levels"] = _parse_levels(args.levels) if args.levels else list(config["levels"])
    config["scenarios"] = _parse_scenarios(args.scenarios) if args.scenarios else list(config["scenarios"])
    if args.generate_only:
        config["execution"]["mode"] = "generate_only"
    if args.cluster_vcpus is not None:
        config["cluster"]["total_vcpus"] = args.cluster_vcpus
    if args.cluster_ram_gib is not None:
        config["cluster"]["total_ram_gib"] = args.cluster_ram_gib
    if args.cluster_geneve_gbps is not None:
        config["cluster"]["geneve_bandwidth_gbps"] = args.cluster_geneve_gbps
    if args.cluster_ceph_gbps is not None:
        config["cluster"]["ceph_bandwidth_gbps"] = args.cluster_ceph_gbps
    if args.max_vm_count is not None:
        config["limits"]["max_vm_count"] = args.max_vm_count
    if args.max_volume_count is not None:
        config["limits"]["max_volume_count"] = args.max_volume_count
    if args.image:
        config.setdefault("overrides", {})["image_name"] = args.image
    if args.flavor:
        config.setdefault("overrides", {})["flavor_name"] = args.flavor
    return config


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    clouds_yaml = Path(args.clouds_yaml).resolve()
    runtime = _resolve_runtime_config(args)
    output_root = Path(args.output_dir).expanduser().resolve() if args.output_dir else (Path("sweeps").resolve() / _now_stamp())
    output_root.mkdir(parents=True, exist_ok=True)
    manifest = {
        "started_at": datetime.now(tz=UTC).isoformat(),
        "output_root": str(output_root),
        "cluster": runtime["cluster"],
        "levels": runtime["levels"],
        "scenario_order": runtime["scenarios"],
        "execution": runtime["execution"],
        "overrides": runtime.get("overrides", {}),
        "calibration": {},
        "runs": {},
    }
    manifest_path = output_root / "manifest.json"
    summary_path = output_root / "summary.md"
    config, normalized_clouds_yaml, temp_dir = _normalize_clouds_for_sweep(clouds_yaml)
    calibration_rates: dict[str, float] = {
        key: float(value)
        for key, value in runtime["calibration"].get("assumed_rates", {}).items()
    }
    overrides = runtime.get("overrides") or {}
    try:
        for scenario_name in runtime["scenarios"]:
            manifest["runs"].setdefault(scenario_name, [])
        for scenario_name in ("fio-distributed", "net-many-to-one", "net-ring"):
            if scenario_name not in runtime["scenarios"] and "mixed-pressure" not in runtime["scenarios"]:
                continue
            base_args, task_path = _build_base_args(normalized_clouds_yaml, config, scenario_name, overrides=overrides)
            calibration_dir = output_root / "runs" / scenario_name / "calibration"
            args_path = calibration_dir / "args.yaml"
            if scenario_name == "fio-distributed":
                rendered_args = _calibration_args_fio(base_args, int(runtime["calibration"]["fio_runtime_seconds"]))
            elif scenario_name == "net-many-to-one":
                rendered_args = _calibration_args_many_to_one(
                    base_args,
                    int(runtime["calibration"]["net_runtime_seconds"]),
                    int(runtime["calibration"]["net_parallel_streams"]),
                )
            else:
                rendered_args = _calibration_args_ring(
                    base_args,
                    int(runtime["calibration"]["net_runtime_seconds"]),
                    int(runtime["calibration"]["net_parallel_streams"]),
                )
            _apply_artifact_root(rendered_args, output_root / "artifacts")
            _write_yaml(args_path, rendered_args)
            calibration_entry = {
                "level_label": "calibration",
                "task_path": task_path,
                "args_path": str(args_path),
                "status": "planned",
            }
            if runtime["execution"]["mode"] == "generate_only":
                calibration_entry["rates"] = {
                    key: calibration_rates[key]
                    for key in CALIBRATION_RATE_KEYS[scenario_name]
                    if key in calibration_rates
                }
                manifest["calibration"][scenario_name] = calibration_entry
                _write_manifest(manifest_path, summary_path, manifest)
                continue
            try:
                task_id, payload = _run_task(task_path, args_path, SCENARIOS[scenario_name].scenario_name)
                rates = _compute_calibration_rates(scenario_name, payload)
                calibration_rates.update(rates)
                calibration_entry.update(
                    {
                        "status": "success",
                        "task_id": task_id,
                        "artifact_root": _artifact_root_from_payload(payload),
                        "measured": _extract_measured_metrics(scenario_name, payload),
                        "rates": rates,
                    }
                )
            except Exception as exc:
                calibration_entry.update({"status": "failed", "error": str(exc)})
                manifest["calibration"][scenario_name] = calibration_entry
                _write_manifest(manifest_path, summary_path, manifest)
                continue
            manifest["calibration"][scenario_name] = calibration_entry
            _write_manifest(manifest_path, summary_path, manifest)

        derived_level_plans: dict[int, dict[str, dict[str, Any]]] = {}
        for scenario_name in runtime["scenarios"]:
            for level in runtime["levels"]:
                spec = SCENARIOS[scenario_name]
                rendered_args, task_path = _build_base_args(normalized_clouds_yaml, config, scenario_name, overrides=overrides)
                _apply_artifact_root(rendered_args, output_root / "artifacts")
                level_dir = output_root / "runs" / scenario_name / f"level-{level:02d}"
                args_path = level_dir / "args.yaml"
                if scenario_name == "spiky":
                    sizing = _plan_spiky(rendered_args, runtime["cluster"], normalized_clouds_yaml, level, runtime["limits"])
                elif scenario_name == "fio-distributed":
                    missing = _missing_calibration_keys(calibration_rates, ("fio_worker_gbps",))
                    if missing:
                        run_entry = {
                            "level": level,
                            "level_label": f"{level}%",
                            "task_path": task_path,
                            "args_path": str(args_path),
                            "status": "skipped",
                            "error": f"Missing calibration rates: {', '.join(missing)}",
                        }
                        _write_yaml(args_path, rendered_args)
                        manifest["runs"][scenario_name].append(run_entry)
                        _write_manifest(manifest_path, summary_path, manifest)
                        continue
                    sizing = _plan_fio(
                        rendered_args,
                        runtime["cluster"],
                        normalized_clouds_yaml,
                        level,
                        runtime["limits"],
                        calibration_rates,
                    )
                elif scenario_name == "net-many-to-one":
                    missing = _missing_calibration_keys(calibration_rates, ("many_to_one_client_gbps",))
                    if missing:
                        run_entry = {
                            "level": level,
                            "level_label": f"{level}%",
                            "task_path": task_path,
                            "args_path": str(args_path),
                            "status": "skipped",
                            "error": f"Missing calibration rates: {', '.join(missing)}",
                        }
                        _write_yaml(args_path, rendered_args)
                        manifest["runs"][scenario_name].append(run_entry)
                        _write_manifest(manifest_path, summary_path, manifest)
                        continue
                    sizing = _plan_many_to_one(
                        rendered_args,
                        runtime["cluster"],
                        normalized_clouds_yaml,
                        level,
                        runtime["limits"],
                        calibration_rates,
                    )
                elif scenario_name == "net-ring":
                    missing = _missing_calibration_keys(calibration_rates, ("ring_participant_gbps",))
                    if missing:
                        run_entry = {
                            "level": level,
                            "level_label": f"{level}%",
                            "task_path": task_path,
                            "args_path": str(args_path),
                            "status": "skipped",
                            "error": f"Missing calibration rates: {', '.join(missing)}",
                        }
                        _write_yaml(args_path, rendered_args)
                        manifest["runs"][scenario_name].append(run_entry)
                        _write_manifest(manifest_path, summary_path, manifest)
                        continue
                    sizing = _plan_ring(
                        rendered_args,
                        runtime["cluster"],
                        normalized_clouds_yaml,
                        level,
                        runtime["limits"],
                        calibration_rates,
                    )
                else:
                    if level not in derived_level_plans or not {"spiky", "fio-distributed", "net-many-to-one", "net-ring"}.issubset(derived_level_plans[level]):
                        run_entry = {
                            "level": level,
                            "level_label": f"{level}%",
                            "task_path": task_path,
                            "args_path": str(args_path),
                            "status": "skipped",
                            "error": "mixed-pressure requires successful sizing of spiky, fio-distributed, net-many-to-one, and net-ring",
                        }
                        _write_yaml(args_path, rendered_args)
                        manifest["runs"][scenario_name].append(run_entry)
                        _write_manifest(manifest_path, summary_path, manifest)
                        continue
                    sizing = _plan_mixed(rendered_args, derived_level_plans[level], runtime["limits"])
                _write_yaml(args_path, rendered_args)
                run_entry = {
                    "level": level,
                    "level_label": f"{level}%",
                    "task_path": task_path,
                    "args_path": str(args_path),
                    "status": "planned",
                    "sizing": sizing,
                }
                if scenario_name != "mixed-pressure":
                    derived_level_plans.setdefault(level, {})[scenario_name] = sizing
                if runtime["execution"]["mode"] == "generate_only":
                    manifest["runs"][scenario_name].append(run_entry)
                    _write_manifest(manifest_path, summary_path, manifest)
                    continue
                try:
                    task_id, payload = _run_task(task_path, args_path, spec.scenario_name)
                    run_entry.update(
                        {
                            "status": "success",
                            "task_id": task_id,
                            "artifact_root": _artifact_root_from_payload(payload),
                            "measured": _extract_measured_metrics(scenario_name, payload),
                        }
                    )
                except Exception as exc:
                    run_entry.update({"status": "failed", "error": str(exc)})
                manifest["runs"][scenario_name].append(run_entry)
                _write_manifest(manifest_path, summary_path, manifest)
    finally:
        temp_dir.cleanup()
    print("Capacity sweep ready.")
    print(f"Manifest: {manifest_path}")
    print(f"Summary:  {summary_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

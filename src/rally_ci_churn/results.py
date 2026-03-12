"""Common result handling for benchmark scenarios."""

from __future__ import annotations

import json


RESULT_PREFIX = "RALLY_CI_RESULT="


def build_table_output(
    title: str,
    description: str,
    cols: list[str],
    rows: list[list[object]],
) -> dict[str, object]:
    return {
        "title": title,
        "description": description,
        "chart_plugin": "Table",
        "data": {"cols": cols, "rows": rows},
    }


def parse_console_result(console_output: str) -> dict[str, object] | None:
    """Return the last structured result emitted by a guest workload."""
    for line in reversed(console_output.splitlines()):
        if not line.startswith(RESULT_PREFIX):
            continue
        payload = line[len(RESULT_PREFIX):].strip()
        if not payload:
            continue
        return json.loads(payload)
    return None


def build_stage_output(result: dict[str, object]) -> dict[str, object]:
    rows = []
    for stage in result.get("stages", []):
        if not isinstance(stage, dict):
            continue
        detail = ", ".join(
            f"{key}={value}"
            for key, value in sorted(stage.items())
            if key not in ("stage", "seconds")
        )
        rows.append([stage.get("stage", "unknown"), stage.get("seconds", 0), detail])
    return build_table_output(
        "Stage timings",
        "Per-stage benchmark timings emitted by the guest runner",
        ["stage", "seconds", "details"],
        rows,
    )


def build_metadata_output(result: dict[str, object]) -> dict[str, object]:
    rows = []
    for key in (
        "scenario_family",
        "scenario_name",
        "status",
        "timeout",
        "wave",
        "iteration",
        "hostname",
        "duration_seconds",
    ):
        rows.append([key, str(result.get(key, ""))])
    diagnostics = result.get("diagnostics", {})
    if isinstance(diagnostics, dict):
        for key in sorted(diagnostics):
            rows.append([f"diagnostics.{key}", str(diagnostics[key])])
    return build_table_output(
        "Benchmark metadata",
        "Structured benchmark metadata for this iteration",
        ["key", "value"],
        rows,
    )

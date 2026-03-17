"""Common result handling for benchmark scenarios."""

from __future__ import annotations

import json
from statistics import mean
from statistics import median


RESULT_PREFIX = "RALLY_CI_RESULT="


def format_markdown_table(
    headers: list[str],
    rows: list[list[object]],
) -> list[str]:
    """Build a column-aligned Markdown table.

    Returns a list of lines (no trailing newline).  Each column is padded
    to the widest value in that column so the table renders neatly in
    fixed-width contexts.
    """
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


def build_key_value_output(
    title: str,
    description: str,
    rows: list[list[object]],
) -> dict[str, object]:
    return build_table_output(title, description, ["key", "value"], rows)


def build_summary_output(summary: dict[str, object]) -> dict[str, object]:
    rows = [[key, str(summary[key])] for key in sorted(summary)]
    return build_key_value_output(
        "Summary",
        "Top-level scenario outcome and aggregate counters",
        rows,
    )


def build_metrics_output(rows: list[list[object]]) -> dict[str, object]:
    return build_key_value_output(
        "Key metrics",
        "Condensed scenario metrics suitable for quick comparison across runs",
        rows,
    )


def build_artifacts_output(rows: list[list[object]]) -> dict[str, object]:
    return build_key_value_output(
        "Artifacts",
        "Artifact pointers for raw data, manifests, and benchmark summaries",
        rows,
    )


def build_failure_reason_output(rows: list[list[object]]) -> dict[str, object]:
    return build_table_output(
        "Failure reasons",
        "Aggregated failure reasons captured during the scenario run",
        ["reason", "count"],
        rows,
    )


def summarize_numeric_series(values: list[float]) -> dict[str, float]:
    if not values:
        return {}
    ordered = sorted(float(value) for value in values)

    def percentile(ratio: float) -> float:
        if len(ordered) == 1:
            return ordered[0]
        index = int(round((len(ordered) - 1) * ratio))
        return ordered[index]

    return {
        "count": float(len(ordered)),
        "min": ordered[0],
        "avg": mean(ordered),
        "p50": median(ordered),
        "p95": percentile(0.95),
        "max": ordered[-1],
    }


def _flatten_atomic_actions(
    actions: list[dict[str, object]],
    rows: list[dict[str, object]],
) -> None:
    for action in actions:
        started_at = action.get("started_at")
        finished_at = action.get("finished_at")
        duration = None
        if isinstance(started_at, (int, float)) and isinstance(finished_at, (int, float)):
            duration = float(finished_at) - float(started_at)
        rows.append(
            {
                "name": str(action.get("name", "unknown")),
                "duration_seconds": duration,
                "failed": bool(action.get("failed", False)),
            }
        )
        children = action.get("children", [])
        if isinstance(children, list):
            _flatten_atomic_actions(children, rows)


def summarize_atomic_actions(actions: list[dict[str, object]]) -> tuple[list[list[object]], dict[str, dict[str, object]]]:
    flattened: list[dict[str, object]] = []
    _flatten_atomic_actions(actions, flattened)
    buckets: dict[str, list[dict[str, object]]] = {}
    for action in flattened:
        buckets.setdefault(str(action["name"]), []).append(action)

    rows: list[list[object]] = []
    summary: dict[str, dict[str, object]] = {}
    for name in sorted(buckets):
        durations = [
            float(entry["duration_seconds"])
            for entry in buckets[name]
            if isinstance(entry.get("duration_seconds"), (int, float))
        ]
        count = len(buckets[name])
        total_seconds = sum(durations)
        avg_seconds = total_seconds / len(durations) if durations else 0.0
        max_seconds = max(durations) if durations else 0.0
        failed = sum(1 for entry in buckets[name] if entry.get("failed"))
        rows.append([name, count, round(total_seconds, 3), round(avg_seconds, 3), round(max_seconds, 3), failed])
        summary[name] = {
            "count": count,
            "total_seconds": round(total_seconds, 3),
            "avg_seconds": round(avg_seconds, 3),
            "max_seconds": round(max_seconds, 3),
            "failed": failed,
        }
    return rows, summary


def build_phase_output(actions: list[dict[str, object]]) -> dict[str, object]:
    rows, _ = summarize_atomic_actions(actions)
    return build_table_output(
        "Phase timings",
        "Aggregated Rally atomic timings for major scenario phases",
        ["phase", "count", "total_seconds", "avg_seconds", "max_seconds", "failed"],
        rows,
    )


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

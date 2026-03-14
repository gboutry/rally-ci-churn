"""Controller-side orchestrator for mixed benchmark components."""

from __future__ import annotations

import argparse
import json
import subprocess
import time
from pathlib import Path


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run fixed mixed benchmark components on the controller VM.")
    parser.add_argument("--config", required=True)
    parser.add_argument("--output-dir", required=True)
    return parser.parse_args()


def _run_component(component: dict[str, object], output_dir: Path) -> dict[str, object]:
    name = str(component["name"])
    command = [str(part) for part in component["command"]]
    component_dir = output_dir / name
    component_dir.mkdir(parents=True, exist_ok=True)
    stdout_path = component_dir / "controller_runner.stdout"
    start = time.monotonic()
    completed = subprocess.run(
        command,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    stdout_path.write_text(completed.stdout, encoding="utf-8")
    return {
        "name": name,
        "command": command,
        "returncode": completed.returncode,
        "duration_seconds": round(time.monotonic() - start, 3),
        "stdout_path": str(stdout_path),
    }


def main() -> int:
    args = _parse_args()
    config = json.loads(Path(args.config).read_text(encoding="utf-8"))
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    component_timeout_seconds = int(config.get("component_timeout_seconds", 180))

    components = config.get("components", [])
    if not isinstance(components, list) or not components:
        raise RuntimeError("Mixed controller config does not include any components")

    rows: list[dict[str, object]] = []
    failed = False
    for component in components:
        if not isinstance(component, dict):
            raise RuntimeError(f"Invalid component entry: {component!r}")
        name = str(component["name"])
        component_dir = output_dir / name
        component_dir.mkdir(parents=True, exist_ok=True)
        stdout_path = component_dir / "controller_runner.stdout"
        start = time.monotonic()
        timed_out = False
        try:
            completed = subprocess.run(
                [str(part) for part in component["command"]],
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                timeout=component_timeout_seconds,
            )
            returncode = completed.returncode
            stdout_text = completed.stdout
        except subprocess.TimeoutExpired as exc:
            timed_out = True
            returncode = 124
            stdout_text = exc.stdout or ""
        stdout_path.write_text(stdout_text, encoding="utf-8")
        rows.append(
            {
                "name": name,
                "command": [str(part) for part in component["command"]],
                "returncode": returncode,
                "duration_seconds": round(time.monotonic() - start, 3),
                "stdout_path": str(stdout_path),
                "timed_out": timed_out,
            }
        )
        if returncode != 0 or timed_out:
            failed = True

    summary = {
        "schema_version": 1,
        "status": "failed" if failed else "success",
        "components": rows,
    }
    (output_dir / "fixed-components.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())

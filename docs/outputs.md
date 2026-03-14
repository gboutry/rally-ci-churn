# Outputs and Artifacts

## Rally outputs

Custom scenarios now emit a consistent high-level layout in Rally where
applicable:

- `Summary`
- `Key metrics`
- `Phase timings`
- `Artifacts`
- scenario-specific detail tables such as fio case rows or VM timeline tables

## Autonomous VM scenarios

Typical outputs:

- top-level result summary
- guest-reported key metrics such as upload size and throughput
- guest stage timings
- artifact references

These scenarios may also upload benchmark artifacts and structured results to
Swift.

## Distributed fio scenario

This scenario writes a local artifact bundle on the Rally host instead of using
Swift.

Artifact layout:

- `artifacts/<task-uuid>/fio-distributed/iteration-0000/`
- `artifacts/<task-uuid>/fio-distributed/iteration-0001/`
- and so on

Expected files:

- `summary.md`
- `summary.csv`
- `summary.json`
- `manifest.json`
- `inventory.json`
- `raw/*.json`
- `raw/*.stdout`

## Network traffic scenarios

These scenarios also write local artifact bundles on the Rally host.

Artifact layout:

- `artifacts/<task-uuid>/net-many-to-one/iteration-0000/`
- `artifacts/<task-uuid>/net-ring/iteration-0000/`

Expected files:

- `summary.md`
- `summary.csv`
- `summary.json`
- `manifest.json`
- `inventory.json`
- `matrix.json`
- `raw/*.json`
- `raw/*.stdout`

## How to inspect results

- use `rally task report <task-id> --out output.html` for human review
- use `rally task report <task-id> --json --out output.json` for machine-readable export
- inspect local artifact bundles when you need the raw benchmark data

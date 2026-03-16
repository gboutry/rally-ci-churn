# Rally CI Churn

Rally CI Churn is a Rally-based benchmark project for OpenStack clouds, with a
Sunbeam-first bootstrap path and a small set of custom scenarios for:

- autonomous ephemeral CI runner churn
- bursty and quota-edge VM launch behavior
- distributed fio benchmarking with controller and worker VMs
- controller-driven overlay network benchmarks for one-to-many and east-west traffic
- overlapping mixed pressure runs combining churn, block, and network load

The repo is intentionally focused on benchmark orchestration and result
collection. It is not a general OpenStack operations toolkit.

## Start here

If you want the fastest end-to-end validation on Sunbeam:

```bash
./scripts/setup_uv.sh /path/to/clouds.yaml smoke
source .venv/bin/activate
source adminrc
rally db create
rally deployment create --fromenv --name sunbeam
rally task validate tasks/autonomous_vm_waves.yaml.j2 \
  --task-args-file args/smoke.yaml
rally task start tasks/autonomous_vm_waves.yaml.j2 \
  --task-args-file args/smoke.yaml
```

`smoke` is intentionally tiny and should be treated as a connectivity check,
not as the first real benchmark baseline. After it passes, generate and run
`steady` for the first meaningful low-resource autonomous VM run:

```bash
./scripts/setup_uv.sh /path/to/clouds.yaml steady
rally task validate tasks/autonomous_vm_waves.yaml.j2 \
  --task-args-file args/steady.yaml
rally task start tasks/autonomous_vm_waves.yaml.j2 \
  --task-args-file args/steady.yaml
```

Generated preset args under `args/` are annotated with the preset role,
required images and services, and the first knobs to tune for that scenario.

## Scenario overview

Operator-first starting points:

- `smoke`
  - smallest possible connectivity and bootstrap check
- `steady`
  - recommended first real autonomous VM baseline on low-resource clouds
- `spiky`, `quota-edge`, `tenant-churn`
  - specialized autonomous VM variants after `steady` is healthy
- `fio-distributed`, `net-many-to-one`, `net-ring`
  - standalone block and network sizing scenarios
- `mixed-pressure`
  - advanced composite scenario after standalone sizing is known

- `CIChurn.boot_autonomous_vm`
  - no-FIP, no-SSH autonomous runner lifecycle
- `CIChurn.spiky_autonomous_vm`
  - bursty autonomous runner scheduling with capped live VMs
- `CIChurn.quota_edge_autonomous_vm`
  - launch-until-refusal probing for quota and scheduler pressure
- `CIChurn.fio_distributed`
  - one fio controller VM plus many fio worker VMs with attached Cinder volumes
- `CIChurn.net_many_to_one`
  - one controller VM, one benchmark server VM, and many clients for overlay traffic tests
- `CIChurn.net_ring`
  - one controller VM and many participants communicating in a bounded east-west ring
- `CIChurn.mixed_pressure`
  - one controller VM driving overlapping fio, network, and spiky stress-ng pressure

Task templates live under [tasks/README.md](./tasks/README.md).

## Documentation map

- Project scope and intended use:
  [docs/scope.md](./docs/scope.md)
- Sunbeam bootstrap and first run:
  [docs/sunbeam-quickstart.md](./docs/sunbeam-quickstart.md)
- Scenario catalog and task mapping:
  [docs/scenarios.md](./docs/scenarios.md)
- Benchmark tuning and sizing:
  [docs/tuning.md](./docs/tuning.md)
- Capacity sweep runner:
  [docs/sweeps.md](./docs/sweeps.md)
- Image strategy and image recipes:
  [docs/images.md](./docs/images.md)
- Rally outputs, artifacts, and result inspection:
  [docs/outputs.md](./docs/outputs.md)
- Full docs index:
  [docs/README.md](./docs/README.md)

## Repo map

- `src/rally_ci_churn/plugins/`
  - Rally scenario implementations
- `src/rally_ci_churn/guest/`
  - guest-side workload runner for autonomous VM scenarios
- `src/rally_ci_churn/bootstrap/`
  - Sunbeam-oriented bootstrap and args generation
- `src/rally_ci_churn/results.py`
  - shared Rally output shaping and result helpers
- `tasks/`
  - Rally task templates
- `args/`
  - checked-in example args and generated local args
- `images/`
  - Imagecraft recipes and image-specific docs

## For agents

Canonical entrypoints:

- bootstrap:
  [scripts/setup_uv.sh](./scripts/setup_uv.sh)
- sweep runner:
  [scripts/run_capacity_sweep.sh](./scripts/run_capacity_sweep.sh)
- scenario code:
  [src/rally_ci_churn/plugins](./src/rally_ci_churn/plugins)
- task templates:
  [tasks/README.md](./tasks/README.md)
- image recipes:
  [images/README.md](./images/README.md)
- outputs and artifacts:
  [docs/outputs.md](./docs/outputs.md)

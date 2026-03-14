# Rally CI Churn

Rally CI Churn is a Rally-based benchmark project for OpenStack clouds, with a
Sunbeam-first bootstrap path and a small set of custom scenarios for:

- autonomous ephemeral CI runner churn
- bursty and quota-edge VM launch behavior
- distributed fio benchmarking with controller and worker VMs
- controller-driven overlay network benchmarks for one-to-many and east-west traffic

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

## Scenario overview

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

Task templates live under [tasks/README.md](/home/guillaume.boutry@canonical.com/Documents/canonical/projects/openstack/rally-ci-churn/tasks/README.md).

## Documentation map

- Project scope and intended use:
  [docs/scope.md](/home/guillaume.boutry@canonical.com/Documents/canonical/projects/openstack/rally-ci-churn/docs/scope.md)
- Sunbeam bootstrap and first run:
  [docs/sunbeam-quickstart.md](/home/guillaume.boutry@canonical.com/Documents/canonical/projects/openstack/rally-ci-churn/docs/sunbeam-quickstart.md)
- Scenario catalog and task mapping:
  [docs/scenarios.md](/home/guillaume.boutry@canonical.com/Documents/canonical/projects/openstack/rally-ci-churn/docs/scenarios.md)
- Benchmark tuning and sizing:
  [docs/tuning.md](/home/guillaume.boutry@canonical.com/Documents/canonical/projects/openstack/rally-ci-churn/docs/tuning.md)
- Image strategy and image recipes:
  [docs/images.md](/home/guillaume.boutry@canonical.com/Documents/canonical/projects/openstack/rally-ci-churn/docs/images.md)
- Rally outputs, artifacts, and result inspection:
  [docs/outputs.md](/home/guillaume.boutry@canonical.com/Documents/canonical/projects/openstack/rally-ci-churn/docs/outputs.md)
- Full docs index:
  [docs/README.md](/home/guillaume.boutry@canonical.com/Documents/canonical/projects/openstack/rally-ci-churn/docs/README.md)

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
  [scripts/setup_uv.sh](/home/guillaume.boutry@canonical.com/Documents/canonical/projects/openstack/rally-ci-churn/scripts/setup_uv.sh)
- scenario code:
  [src/rally_ci_churn/plugins](/home/guillaume.boutry@canonical.com/Documents/canonical/projects/openstack/rally-ci-churn/src/rally_ci_churn/plugins)
- task templates:
  [tasks/README.md](/home/guillaume.boutry@canonical.com/Documents/canonical/projects/openstack/rally-ci-churn/tasks/README.md)
- image recipes:
  [images/README.md](/home/guillaume.boutry@canonical.com/Documents/canonical/projects/openstack/rally-ci-churn/images/README.md)
- outputs and artifacts:
  [docs/outputs.md](/home/guillaume.boutry@canonical.com/Documents/canonical/projects/openstack/rally-ci-churn/docs/outputs.md)

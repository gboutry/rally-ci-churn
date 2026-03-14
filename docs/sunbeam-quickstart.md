# Sunbeam Quickstart

## What this is

This is the fastest supported path for a new user to bootstrap the repo on a
Sunbeam cloud and run an initial smoke benchmark.

## Prerequisites

- `clouds.yaml` with `sunbeam` and `sunbeam-admin`
- `sunbeam-admin` can list images, flavors, and networks
- the cloud exposes the services required by the scenario you want to run
- you are running from a machine that can reach the cloud APIs and any required
  floating-IP paths

## Bootstrap

```bash
./scripts/setup_uv.sh /path/to/clouds.yaml smoke
source .venv/bin/activate
source adminrc
```

The bootstrap:

- creates `.venv`
- installs Rally, `rally-openstack`, and this local package
- writes `args/<preset>.yaml`
- writes `adminrc`

## Create or select a Rally deployment

```bash
rally db create
rally deployment create --fromenv --name sunbeam
```

## First run

```bash
rally task validate tasks/autonomous_vm_waves.yaml.j2 \
  --task-args-file args/smoke.yaml

rally task start tasks/autonomous_vm_waves.yaml.j2 \
  --task-args-file args/smoke.yaml
```

## Other presets

```bash
./scripts/setup_uv.sh /path/to/clouds.yaml steady
./scripts/setup_uv.sh /path/to/clouds.yaml spiky
./scripts/setup_uv.sh /path/to/clouds.yaml failure-storm
./scripts/setup_uv.sh /path/to/clouds.yaml quota-edge
./scripts/setup_uv.sh /path/to/clouds.yaml tenant-churn
./scripts/setup_uv.sh /path/to/clouds.yaml fio-distributed
./scripts/setup_uv.sh /path/to/clouds.yaml mixed-pressure
./scripts/setup_uv.sh /path/to/clouds.yaml net-many-to-one
./scripts/setup_uv.sh /path/to/clouds.yaml net-many-to-one-http
./scripts/setup_uv.sh /path/to/clouds.yaml net-ring
./scripts/setup_uv.sh /path/to/clouds.yaml stress-ng
```

Generated args land under `args/`. See
[args/README.md](/home/guillaume.boutry@canonical.com/Documents/canonical/projects/openstack/rally-ci-churn/args/README.md)
for the mapping.

## Common pitfalls

- using a copied `clouds.yaml` with broken relative CA paths
- trying to run `stress-ng` or fio without building and uploading the required
  image first
- trying to run `mixed-pressure` without building and uploading
  `ubuntu-mixed-benchmark`
- missing Swift on autonomous VM scenarios
- missing Cinder or floating IP support on the distributed fio scenario

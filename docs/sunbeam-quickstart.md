# Sunbeam Quickstart

## What this is

This is the fastest supported path for a new user to bootstrap the repo on a
Sunbeam cloud, run a connectivity smoke check, and then move to the first real
baseline.

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

Generated args files are annotated with:

- what the preset is for
- which services and images it expects
- the first knobs to tune for that scenario

## Create or select a Rally deployment

```bash
rally db create
rally deployment create --fromenv --name sunbeam
```

## Connectivity smoke

```bash
rally task validate tasks/autonomous_vm_waves.yaml.j2 \
  --task-args-file args/smoke.yaml

rally task start tasks/autonomous_vm_waves.yaml.j2 \
  --task-args-file args/smoke.yaml
```

Treat `smoke` as a bootstrap and low-resource CI check. It validates one VM
lifecycle, tenant networking, and artifact plumbing, but it is not meant to be
the first serious churn baseline.

## First real baseline

Once `smoke` passes, switch to `steady`:

```bash
./scripts/setup_uv.sh /path/to/clouds.yaml steady

rally task validate tasks/autonomous_vm_waves.yaml.j2 \
  --task-args-file args/steady.yaml

rally task start tasks/autonomous_vm_waves.yaml.j2 \
  --task-args-file args/steady.yaml
```

`steady` is the recommended first real autonomous VM baseline on low-resource
clouds. Tune it before moving to `spiky`, `quota-edge`, or `tenant-churn`.

## Other presets

Pick exactly one preset, generate its annotated args file, run it, then tune
the sections called out near the top of the generated YAML.

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
[args/README.md](./args/README.md)
for the mapping.

## Capacity sweeps

Once the environment is bootstrapped, you can run the percentage-based sweep:

```bash
./scripts/run_capacity_sweep.sh \
  --clouds-yaml /home/ubuntu/.config/openstack/clouds.yaml
```

This reuses the existing presets, calibrates fio and network scenarios, then
runs the selected load levels sequentially. See
[sweeps.md](./docs/sweeps.md)
for the sizing model and output layout.

## Common pitfalls

- using a copied `clouds.yaml` with broken relative CA paths
- trying to run `stress-ng` or fio without building and uploading the required
  image first
- trying to run `mixed-pressure` without building and uploading the dedicated
  `ubuntu-fio`, `ubuntu-netbench`, and `ubuntu-stress-ng` images, or without
  editing the generated `cloud.*_image_name` fields to point at one combined
  image
- missing Swift on autonomous VM scenarios
- missing Cinder or floating IP support on the distributed fio scenario

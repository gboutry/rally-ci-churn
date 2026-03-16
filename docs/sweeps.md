# Capacity Sweeps

## What this is

The capacity sweep runner generates and optionally executes a load ladder across
the main load-bearing scenarios:

- `spiky`
- `fio-distributed`
- `net-many-to-one`
- `net-ring`
- `mixed-pressure`

The default levels are:

- `10%`
- `25%`
- `40%`
- `60%`
- `80%`

## Default cluster model

Unless overridden, the sweep uses:

- `1248` vCPU
- `6144 GiB` RAM
- `200 Gbps` Geneve bandwidth
- `200 Gbps` Ceph bandwidth

These defaults match a `3` node cluster with:

- `416` vCPU per node
- `2 TiB` RAM per node
- separate `200 Gbps` fabrics for Geneve and Ceph

## Run it

After bootstrapping the repo and Rally deployment:

```bash
./scripts/run_capacity_sweep.sh \
  --clouds-yaml /home/ubuntu/.config/openstack/clouds.yaml
```

Useful overrides:

```bash
./scripts/run_capacity_sweep.sh \
  --clouds-yaml /home/ubuntu/.config/openstack/clouds.yaml \
  --levels 10,25 \
  --scenarios spiky,fio-distributed,net-many-to-one \
  --cluster-vcpus 1248 \
  --cluster-ram-gib 6144 \
  --cluster-geneve-gbps 200 \
  --cluster-ceph-gbps 200
```

## Custom image and flavor

By default each scenario expects a dedicated image (`ubuntu-stress-ng`,
`ubuntu-fio`, `ubuntu-netbench`).  If you have a single all-in-one image that
bundles every benchmark tool, pass `--image` and optionally `--flavor` to
override the per-scenario defaults:

```bash
./scripts/run_capacity_sweep.sh \
  --clouds-yaml /home/ubuntu/.config/openstack/clouds.yaml \
  --image ubuntu-mixed-benchmark \
  --flavor m1.benchmark
```

The same overrides can be set in a config file:

```yaml
overrides:
  image_name: "ubuntu-mixed-benchmark"
  flavor_name: "m1.benchmark"
```

CLI flags take precedence over the config file.  Overrides apply to **every**
scenario image and flavor field (controller, worker, client, participant, etc.).

Generate-only planning:

```bash
./scripts/run_capacity_sweep.sh \
  --clouds-yaml /home/ubuntu/.config/openstack/clouds.yaml \
  --generate-only
```

Reference config:

- [args/capacity_sweep.example.yaml](./args/capacity_sweep.example.yaml)

## Before you run a sweep

- validate the environment with `smoke`, then use `steady` as the first real
  autonomous VM baseline
- build and upload the scenario images the sweep expects, or use one combined
  image override with `--image`
- expect `mixed-pressure` to depend on successful sizing of `spiky`,
  `fio-distributed`, `net-many-to-one`, and `net-ring`

## How sizing works

When running with `--generate-only`, the sweep uses the configured
`calibration.assumed_rates` because no live calibration task is executed.

### `spiky`

- sized from flavor vCPU and RAM footprint
- uses the smaller of CPU-based and RAM-based VM capacity
- scales `max_active_vms` first
- keeps per-VM `stress-ng` load stable by default

### `fio-distributed`

- runs a calibration case first
- measures per-worker fio throughput
- sizes worker count from the Ceph bandwidth target
- scales worker count first, then `volumes_per_client`

### `net-many-to-one`

- runs a calibration case first
- measures aggregate throughput and converts it to per-client rate
- scales `client_count` first
- increases `parallel_streams` only after a practical client-count threshold

### `net-ring`

- runs a calibration case first
- measures aggregate ring throughput and converts it to per-participant rate
- scales `participant_count` first
- keeps `neighbors_per_vm=1` by default

### `mixed-pressure`

- does not calibrate independently
- derives its shape from the level-matched standalone plans
- uses conservative fractions of those standalone plans: `35%` of spiky,
  `25%` of fio workers, and `20%` of many-to-one and ring participants
- keeps exactly one controller floating IP

## Outputs

Each sweep writes:

- `sweeps/<timestamp>/manifest.json`
- `sweeps/<timestamp>/summary.md`
- `sweeps/<timestamp>/runs/<scenario>/level-XX/args.yaml`

The manifest records:

- planned sizing
- Rally task ids
- status
- measured key metrics
- artifact roots
- calibration results

## Defaults and constraints

- scenarios run sequentially
- failures are recorded and the sweep continues
- the default goal is breadth-first scaling
- network and fio scenarios use live calibration before level sizing
- the script assumes `.venv` and a Rally deployment already exist

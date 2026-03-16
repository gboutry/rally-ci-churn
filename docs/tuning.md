# Benchmark Tuning

## What to tune first

### Autonomous VM scenarios

Key knobs:

- `waves`
- `vm_count`
- `task_concurrency`
- `timeout_seconds`
- `timeout_mode`
- workload profile and workload params

Guidance:

- raise `vm_count` when you want more VMs per wave
- raise `task_concurrency` when you want Rally to overlap more of those wave iterations
- use more waves when you want repeated control-plane churn with stable shape
- keep `timeout_seconds` finite unless you explicitly want hangs to persist

### Spiky scenario

Key knobs:

- `duration_seconds`
- `max_active_vms`
- `baseline_launches_per_minute`
- `burst_windows`

Guidance:

- increase `max_active_vms` only after validating network and quota headroom
- use burst windows to model CI rushes instead of just raising the baseline
- compare `effective_launches_per_minute` against configured rate to see drop behavior

### Quota-edge scenario

Key knobs:

- `launches_per_tick`
- `launch_tick_seconds`
- `max_consecutive_launch_failures`

Guidance:

- use small launch batches first to learn the failure mode
- increase aggressiveness only after confirming cleanup is reliable

### Distributed fio scenario

Key knobs:

- `boot_concurrency`
- `volume_concurrency`
- `client_counts`
- `volumes_per_client`
- `volume_size_gib`
- `profile_names`
- `numjobs`
- `iodepths`
- `runtime_seconds`

Guidance:

- raise `boot_concurrency` to parallelize worker VM creation inside one Rally iteration
- raise `volume_concurrency` to parallelize Cinder volume create/attach inside one Rally iteration
- scale `client_counts` first to increase distributed pressure
- scale `volumes_per_client` when you want more block-device fan-out per worker
- keep `volume_size_gib` large enough for the fio access pattern
- raise `numjobs` and `iodepths` only after confirming the baseline topology works

### Mixed pressure scenario

Key knobs:

- `boot_concurrency`
- `volume_concurrency`
- `duration_seconds`
- churn:
  `max_active_vms`, `baseline_launches_per_minute`, `burst_windows`
- fio:
  `client_counts`, `volumes_per_client`, `profile_names`
- many-to-one:
  `client_count`, `parallel_streams`, `protocols`
- ring:
  `participant_count`, `neighbors_per_vm`, `parallel_streams`

Guidance:

- keep the BM0 smoke preset small; the whole point is overlap, not per-axis peak
- raise `boot_concurrency` when fixed fio or network groups are slow to provision
- raise `volume_concurrency` when fio worker volume provisioning is the setup bottleneck
- grow fixed groups before making the churn schedule aggressive
- use `vm_workers` and `vm_bytes` together when you want churn VMs to apply CPU
  and memory pressure at the same time
- keep the one-FIP controller invariant unless you are debugging

### Capacity sweep runner

Key knobs:

- `levels`
- cluster `total_vcpus`
- cluster `total_ram_gib`
- cluster `geneve_bandwidth_gbps`
- cluster `ceph_bandwidth_gbps`
- optional `max_vm_count`
- optional `max_volume_count`

Guidance:

- use real cluster totals, not single-node smoke numbers
- leave bandwidth scenarios on calibration-first sizing unless you have a good
  reason to override it
- use `max_vm_count` or `max_volume_count` only as a safety cap, not as the
  primary sizing input
- run `--generate-only` first if you want to inspect the generated ladder before
  launching it

### Network traffic scenarios

Key knobs:

- `boot_concurrency`
- `client_count`
- `participant_count`
- `neighbors_per_vm`
- `protocols`
- `parallel_streams`
- `udp_target_mbps`
- `duration_seconds`

Guidance:

- raise `boot_concurrency` to parallelize client or participant VM creation inside one Rally iteration
- scale `client_count` first for the one-server-many-clients shape
- scale `participant_count` first for the east-west ring shape
- keep `neighbors_per_vm=1` until you know the cloud can sustain the baseline
- increase TCP `parallel_streams` before increasing VM counts if you are flow-limited
- use UDP targets carefully; packet loss is often the first useful signal

## Root boot volume toggle

Every VM-creating scenario supports:

```yaml
boot_volume:
  enabled: true
  size_gib: 20
  volume_type: null
```

This switches the root disk from ephemeral storage to a Cinder-backed boot
volume. It applies to all VM roles in the selected scenario. It does not replace
extra data volumes already used by `fio-distributed`, `mixed-pressure`, or the
HTTP server variant of `net-many-to-one`.

## Flavor and image tuning

Flavor and image guidance is intentionally split out:

- image strategy:
  [images.md](./docs/images.md)
- per-image build/upload/flavor notes:
  [images/README.md](./images/README.md)

## Result interpretation

- use Rally `Phase timings` to understand orchestration overhead
- use scenario `Summary` and `Key metrics` to compare runs
- use local artifact bundles when you need raw fio or guest-level details

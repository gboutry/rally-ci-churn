# Benchmark Tuning

## What to tune first

### Autonomous VM scenarios

Key knobs:

- `waves`
- `concurrency`
- `timeout_seconds`
- `timeout_mode`
- workload profile and workload params

Guidance:

- raise `concurrency` before adding many waves if you want to see live pressure
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

- `client_counts`
- `volumes_per_client`
- `volume_size_gib`
- `profile_names`
- `numjobs`
- `iodepths`
- `runtime_seconds`

Guidance:

- scale `client_counts` first to increase distributed pressure
- scale `volumes_per_client` when you want more block-device fan-out per worker
- keep `volume_size_gib` large enough for the fio access pattern
- raise `numjobs` and `iodepths` only after confirming the baseline topology works

### Mixed pressure scenario

Key knobs:

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
- grow fixed groups before making the churn schedule aggressive
- use `vm_workers` and `vm_bytes` together when you want churn VMs to apply CPU
  and memory pressure at the same time
- keep the one-FIP controller invariant unless you are debugging

### Network traffic scenarios

Key knobs:

- `client_count`
- `participant_count`
- `neighbors_per_vm`
- `protocols`
- `parallel_streams`
- `udp_target_mbps`
- `duration_seconds`

Guidance:

- scale `client_count` first for the one-server-many-clients shape
- scale `participant_count` first for the east-west ring shape
- keep `neighbors_per_vm=1` until you know the cloud can sustain the baseline
- increase TCP `parallel_streams` before increasing VM counts if you are flow-limited
- use UDP targets carefully; packet loss is often the first useful signal

## Flavor and image tuning

Flavor and image guidance is intentionally split out:

- image strategy:
  [images.md](/home/guillaume.boutry@canonical.com/Documents/canonical/projects/openstack/rally-ci-churn/docs/images.md)
- per-image build/upload/flavor notes:
  [images/README.md](/home/guillaume.boutry@canonical.com/Documents/canonical/projects/openstack/rally-ci-churn/images/README.md)

## Result interpretation

- use Rally `Phase timings` to understand orchestration overhead
- use scenario `Summary` and `Key metrics` to compare runs
- use local artifact bundles when you need raw fio or guest-level details

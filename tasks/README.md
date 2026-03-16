# Task Templates

Task templates map directly to the custom Rally scenarios.

## Template index

- `autonomous_vm_waves.yaml.j2`
  - stable autonomous VM churn
  - `scenario.vm_count` controls VMs per wave
  - `scenario.task_concurrency` controls Rally runner overlap
  - typically used with `smoke`, `steady`, or `stress-ng`
- `spiky_autonomous_vm.yaml.j2`
  - bursty autonomous VM churn
  - typically used with `spiky` or `failure-storm`
- `quota_edge_autonomous_vm.yaml.j2`
  - launch-until-refusal probing
  - typically used with `quota-edge`
- `tenant_churn_autonomous_vm.yaml.j2`
  - repeated short-lived tenants and VM batches
  - `tenant_churn.vms_per_cycle` controls cycle size
  - `tenant_churn.task_concurrency` controls Rally runner overlap
  - typically used with `tenant-churn`
- `fio_distributed.yaml.j2`
  - controller/worker fio benchmarking
  - `controller.boot_concurrency` controls in-scenario worker boot fan-out
  - `controller.volume_concurrency` controls in-scenario fio volume create/attach fan-out
  - typically used with `fio-distributed`
- `mixed_pressure.yaml.j2`
  - one controller running overlapping fio, network, and spiky stress-ng pressure
  - `controller.boot_concurrency` controls in-scenario fixed-group boot fan-out
  - `controller.volume_concurrency` controls in-scenario fio volume create/attach fan-out
  - typically used with `mixed-pressure`
- `net_many_to_one.yaml.j2`
  - one server, many clients, and one controller
  - `controller.boot_concurrency` controls in-scenario client boot fan-out
  - typically used with `net-many-to-one` or `net-many-to-one-http`
- `net_ring.yaml.j2`
  - bounded east-west ring communication
  - `controller.boot_concurrency` controls in-scenario participant boot fan-out
  - typically used with `net-ring`

## Normal workflow

Use generated preset args instead of editing templates directly:

```bash
./scripts/setup_uv.sh /path/to/clouds.yaml <preset>
rally task validate tasks/<template>.yaml.j2 --task-args-file args/<preset>.yaml
rally task start tasks/<template>.yaml.j2 --task-args-file args/<preset>.yaml
```

The normal operator order is:

- `smoke` for bootstrap connectivity only
- `steady` for the first real low-resource autonomous VM baseline
- standalone fio or network presets for single-axis sizing
- `mixed-pressure` only after those standalone baselines are known

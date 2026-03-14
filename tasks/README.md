# Task Templates

Task templates map directly to the custom Rally scenarios.

## Template index

- `autonomous_vm_waves.yaml.j2`
  - stable autonomous VM churn
  - typically used with `smoke`, `steady`, or `stress-ng`
- `spiky_autonomous_vm.yaml.j2`
  - bursty autonomous VM churn
  - typically used with `spiky` or `failure-storm`
- `quota_edge_autonomous_vm.yaml.j2`
  - launch-until-refusal probing
  - typically used with `quota-edge`
- `tenant_churn_autonomous_vm.yaml.j2`
  - repeated short-lived tenants and VM batches
  - typically used with `tenant-churn`
- `fio_distributed.yaml.j2`
  - controller/worker fio benchmarking
  - typically used with `fio-distributed`
- `mixed_pressure.yaml.j2`
  - one controller running overlapping fio, network, and spiky stress-ng pressure
  - typically used with `mixed-pressure`
- `net_many_to_one.yaml.j2`
  - one server, many clients, and one controller
  - typically used with `net-many-to-one` or `net-many-to-one-http`
- `net_ring.yaml.j2`
  - bounded east-west ring communication
  - typically used with `net-ring`

## Normal workflow

Use generated preset args instead of editing templates directly:

```bash
./scripts/setup_uv.sh /path/to/clouds.yaml <preset>
rally task validate tasks/<template>.yaml.j2 --task-args-file args/<preset>.yaml
rally task start tasks/<template>.yaml.j2 --task-args-file args/<preset>.yaml
```

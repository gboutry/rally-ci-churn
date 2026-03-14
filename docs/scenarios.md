# Scenarios

## Scenario catalog

### `CIChurn.boot_autonomous_vm`

- topology:
  one guest VM per Rally iteration
- intent:
  baseline autonomous ephemeral CI runner lifecycle
- template:
  `tasks/autonomous_vm_waves.yaml.j2`
- common presets:
  `smoke`, `steady`, `stress-ng`

### `CIChurn.spiky_autonomous_vm`

- topology:
  time-based launches with capped active VMs
- intent:
  simulate bursty CI arrival patterns
- template:
  `tasks/spiky_autonomous_vm.yaml.j2`
- common presets:
  `spiky`, `failure-storm`

### `CIChurn.quota_edge_autonomous_vm`

- topology:
  repeated launch batches until the control plane starts refusing work
- intent:
  surface quota, scheduler, or control-plane saturation behavior
- template:
  `tasks/quota_edge_autonomous_vm.yaml.j2`
- common presets:
  `quota-edge`

### `CIChurn.fio_distributed`

- topology:
  one controller VM plus many worker VMs with attached Cinder volumes
- intent:
  distributed block benchmarking driven by fio client/server mode
- template:
  `tasks/fio_distributed.yaml.j2`
- common presets:
  `fio-distributed`

### `CIChurn.net_many_to_one`

- topology:
  one controller VM, one benchmark server VM, many client VMs
- intent:
  maximize one-to-many overlay traffic with either pure `iperf3` or a
  volume-backed HTTP download variant
- template:
  `tasks/net_many_to_one.yaml.j2`
- common presets:
  `net-many-to-one`, `net-many-to-one-http`

### `CIChurn.net_ring`

- topology:
  one controller VM plus many benchmark participants in a bounded ring
- intent:
  maximize east-west overlay traffic without the full-mesh `N^2` explosion
- template:
  `tasks/net_ring.yaml.j2`
- common presets:
  `net-ring`

### `tenant_churn_autonomous_vm`

- topology:
  repeated short-lived projects/networks/users around VM batches
- intent:
  exercise tenant/network lifecycle churn
- template:
  `tasks/tenant_churn_autonomous_vm.yaml.j2`
- common presets:
  `tenant-churn`

## Task template index

See [tasks/README.md](/home/guillaume.boutry@canonical.com/Documents/canonical/projects/openstack/rally-ci-churn/tasks/README.md)
for the direct template-to-scenario mapping.

## Choosing the right scenario

- choose `boot_autonomous_vm` for single-runner lifecycle validation
- choose `spiky_autonomous_vm` for burst modeling
- choose `quota_edge_autonomous_vm` for refusal and saturation behavior
- choose `tenant_churn_autonomous_vm` for project/network churn
- choose `fio_distributed` for controller/worker volume benchmarking
- choose `net_many_to_one` for one-server-many-clients network pressure
- choose `net_ring` for bounded east-west overlay traffic

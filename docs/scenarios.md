# Scenarios

## Terms

- preset:
  the selector passed to `scripts/setup_uv.sh` that generates one annotated
  args file under `args/`
- scenario:
  the behavior you want to exercise, such as autonomous churn, distributed fio,
  or ring traffic
- task template:
  the Rally Jinja template under `tasks/` that consumes the generated args file

For operators, the normal flow is:

1. pick a preset
2. generate its args file
3. run the preset once
4. tune the few knobs called out near the top of the generated YAML

## Low-resource start order

- start with `smoke` if you need the smallest possible bootstrap and CI check
- move to `steady` for the first real autonomous VM baseline
- move to `spiky`, `quota-edge`, or `tenant-churn` only after `steady` is
  healthy
- size `fio-distributed`, `net-many-to-one`, and `net-ring` as standalone
  scenarios before running `mixed-pressure`

## Scenario catalog

All VM-creating scenarios support a top-level `boot_volume` toggle to boot
instances from Cinder-backed root disks instead of ephemeral local disks.

### `CIChurn.boot_autonomous_vm`

- topology:
  one guest VM per Rally iteration, with waves controlled by task runner settings
- intent:
  baseline autonomous ephemeral CI runner lifecycle
- template:
  `tasks/autonomous_vm_waves.yaml.j2`
- common presets:
  `smoke`, `steady`, `stress-ng`
- required images/services:
  base Ubuntu image; Nova, Neutron, and Swift
- operator note:
  `smoke` is connectivity-only, while `steady` is the recommended first real
  baseline

### `CIChurn.spiky_autonomous_vm`

- topology:
  time-based launches with capped active VMs
- intent:
  simulate bursty CI arrival patterns
- template:
  `tasks/spiky_autonomous_vm.yaml.j2`
- common presets:
  `spiky`, `failure-storm`
- required images/services:
  base Ubuntu image; Nova, Neutron, and Swift
- operator note:
  use after `steady`, not as the first run on a fresh cloud

### `CIChurn.quota_edge_autonomous_vm`

- topology:
  repeated launch batches until the control plane starts refusing work
- intent:
  surface quota, scheduler, or control-plane saturation behavior
- template:
  `tasks/quota_edge_autonomous_vm.yaml.j2`
- common presets:
  `quota-edge`
- required images/services:
  base Ubuntu image; Nova, Neutron, and Swift
- operator note:
  use after cleanup behavior is already trusted on the target cloud

### `CIChurn.fio_distributed`

- topology:
  one controller VM plus many worker VMs with attached Cinder volumes
- intent:
  distributed block benchmarking driven by fio client/server mode
- template:
  `tasks/fio_distributed.yaml.j2`
- common presets:
  `fio-distributed`
- required images/services:
  `ubuntu-fio`; Nova, Neutron, Cinder, and floating IP support
- operator note:
  use as the standalone block baseline before folding fio into `mixed-pressure`

### `CIChurn.mixed_pressure`

- topology:
  one controller VM, fixed fio/network benchmark groups, and spiky autonomous
  `stress-ng` churn on the same tenant network
- intent:
  exercise a cloud under overlapping compute, block, and east-west network
  pressure instead of isolated single-axis benchmarks
- template:
  `tasks/mixed_pressure.yaml.j2`
- common presets:
  `mixed-pressure`
- required images/services:
  `ubuntu-fio`, `ubuntu-netbench`, `ubuntu-stress-ng`; Nova, Neutron, Swift,
  Cinder, and floating IP support
- operator note:
  advanced composite scenario; size `spiky`, `fio-distributed`,
  `net-many-to-one`, and `net-ring` first

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
- required images/services:
  `ubuntu-netbench`; Nova, Neutron, and floating IP support
- operator note:
  `net-many-to-one-http` also relies on Cinder because the server serves a
  volume-backed payload

### `CIChurn.net_ring`

- topology:
  one controller VM plus many benchmark participants in a bounded ring
- intent:
  maximize east-west overlay traffic without the full-mesh `N^2` explosion
- template:
  `tasks/net_ring.yaml.j2`
- common presets:
  `net-ring`
- required images/services:
  `ubuntu-netbench`; Nova, Neutron, and floating IP support
- operator note:
  use as the standalone east-west baseline before `mixed-pressure`

### `tenant_churn_autonomous_vm`

- topology:
  repeated short-lived projects/networks/users around VM batches, with
  per-cycle VM count separate from Rally runner concurrency
- intent:
  exercise tenant/network lifecycle churn
- template:
  `tasks/tenant_churn_autonomous_vm.yaml.j2`
- common presets:
  `tenant-churn`
- required images/services:
  base Ubuntu image; Nova, Neutron, and Swift
- operator note:
  specialized tenant lifecycle test rather than a general first-run scenario

## Task template index

See [tasks/README.md](./tasks/README.md)
for the direct template-to-scenario mapping.

## Choosing the right scenario

- choose `smoke` when you only need bootstrap and one-VM connectivity validation
- choose `steady` for the first meaningful autonomous VM baseline
- choose `spiky_autonomous_vm` for burst modeling after `steady`
- choose `quota_edge_autonomous_vm` for refusal and saturation behavior after
  baseline cleanup is trusted
- choose `tenant_churn_autonomous_vm` for project and network lifecycle churn
- choose `fio_distributed` for standalone controller/worker block benchmarking
- choose `net_many_to_one` for one-server-many-clients overlay traffic sizing
- choose `net_ring` for bounded east-west overlay traffic sizing
- choose `mixed_pressure` only after standalone spiky, fio, and network sizing
  is known

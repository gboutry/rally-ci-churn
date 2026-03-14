# Images

## Strategy

Use stock cloud images when the benchmark does not require packages or tooling
that would introduce first-boot package-install noise.

Use pre-baked images when:

- the workload depends on packages like `stress-ng` or `fio`
- boot-time package installation would distort the benchmark
- the image should be reproducible and reusable across runs

## Current images

- `ubuntu-stress-ng`
  - used for the `stress-ng` preset and related autonomous VM runs
- `ubuntu-fio`
  - used for distributed fio controller and worker VMs
- `ubuntu-netbench`
  - used for controller/server/client overlay traffic benchmarks

See the local image docs:

- [images/ubuntu-stress-ng/README.md](/home/guillaume.boutry@canonical.com/Documents/canonical/projects/openstack/rally-ci-churn/images/ubuntu-stress-ng/README.md)
- [images/ubuntu-fio/README.md](/home/guillaume.boutry@canonical.com/Documents/canonical/projects/openstack/rally-ci-churn/images/ubuntu-fio/README.md)
- [images/ubuntu-netbench/README.md](/home/guillaume.boutry@canonical.com/Documents/canonical/projects/openstack/rally-ci-churn/images/ubuntu-netbench/README.md)

## Build model

The supported path is:

1. build with `./scripts/build_imagecraft_vm.sh`
2. upload `disk.img` to Glance
3. set required image properties
4. point the generated preset or task args at that image

The image-specific READMEs carry the concrete commands and flavor notes.

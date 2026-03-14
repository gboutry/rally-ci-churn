# ubuntu-mixed-benchmark

## What it is

`ubuntu-mixed-benchmark` is the pre-baked image used by the mixed cloud pressure
scenario:

- `CIChurn.mixed_pressure`

It contains the runtime packages needed for all four load classes in the mixed
scenario:

- `stress-ng`
- `fio`
- `iperf3`
- `curl`
- `openssh-server`

## Build

```bash
./scripts/build_imagecraft_vm.sh images/ubuntu-mixed-benchmark
```

## Upload to Glance

```bash
openstack image create ubuntu-mixed-benchmark \
  --file images/ubuntu-mixed-benchmark/disk.img \
  --disk-format raw \
  --container-format bare \
  --public

openstack image set ubuntu-mixed-benchmark \
  --property hw_firmware_type=uefi
```

## Recommended flavors

Recommended starting flavors:

- `m1.netbench`
  - `2 vCPU`
  - `2048 MB RAM`
  - `5 GB disk`
- `m1.small`
  - optional fallback only if you have ample local disk and need a smaller vCPU shape

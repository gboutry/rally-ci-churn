# ubuntu-netbench

## What it is

`ubuntu-netbench` is the pre-baked image used by the network traffic scenarios:

- `CIChurn.net_many_to_one`
- `CIChurn.net_ring`

It is used for the controller VM and all benchmark VMs.

## Build

```bash
./scripts/build_imagecraft_vm.sh images/ubuntu-netbench
```

## Upload to Glance

```bash
openstack image create ubuntu-netbench \
  --file images/ubuntu-netbench/disk.img \
  --disk-format raw \
  --container-format bare \
  --public

openstack image set ubuntu-netbench \
  --property hw_firmware_type=uefi
```

## Recommended flavor

Recommended starting flavor:

- `m1.netbench`
  - `2 vCPU`
  - `2048 MB RAM`
  - `5 GB disk`

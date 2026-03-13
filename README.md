# Rally CI Churn

This repository is now a small Rally benchmark project rather than a single
task file. It provides:

- a local Python package with Rally plugins under `src/rally_ci_churn/`
- a Sunbeam-oriented bootstrap path via `scripts/setup_uv.sh`
- autonomous VM benchmark tasks under `tasks/`
- guest-side workload logic packaged with the plugin, not pushed over SSH

The primary VM benchmark path is autonomous:

1. Rally boots a VM with `cloud-init`.
2. The guest runs the workload itself and uploads artifacts and a structured result to Swift.
3. Rally treats the uploaded result as the completion signal, falls back to `SHUTOFF` if needed, and deletes the VM.
4. Guest poweroff remains best-effort guest cleanup, not a hard dependency for success.

No floating IPs or SSH are required for the main benchmark path.

## Current scenarios

- `CIChurn.boot_autonomous_vm`
  - no-FIP, no-SSH autonomous runner lifecycle
  - configurable timeout
  - `timeout_seconds` controls per-VM wait time
  - `timeout_mode: fail|soft`
- `CIChurn.spiky_autonomous_vm`
  - no-FIP, no-SSH autonomous runner lifecycle
  - launches VMs over a time-based burst schedule
  - caps live VMs with `max_active_vms`
  - drops launches that would exceed the cap
- `CIChurn.quota_edge_autonomous_vm`
  - no-FIP, no-SSH autonomous runner lifecycle
  - launches until quota or scheduler failures accumulate
  - records launch failure reasons instead of stopping on the first one
- `CIChurn.fio_distributed`
  - one controller VM with a floating IP and SSH access
  - many worker VMs running `fio --server`
  - raw Cinder volumes attached to each worker
  - artifacts copied back locally to the Rally host

The primary task templates are:

- `tasks/autonomous_vm_waves.yaml.j2`
- `tasks/spiky_autonomous_vm.yaml.j2`
- `tasks/quota_edge_autonomous_vm.yaml.j2`
- `tasks/tenant_churn_autonomous_vm.yaml.j2`
- `tasks/fio_distributed.yaml.j2`

Sunbeam should normally use the generated preset args under `args/*.yaml`
rather than editing example files by hand.

## Fast start

This assumes:

- your `clouds.yaml` contains `sunbeam` and `sunbeam-admin`
- `sunbeam-admin` can list images, flavors, and networks
- Swift is enabled
- you want the autonomous VM scenario

Bootstrap:

```bash
./scripts/setup_uv.sh /path/to/clouds.yaml smoke
source .venv/bin/activate
source adminrc
```

Create or select a Rally deployment:

```bash
rally db create
rally deployment create --fromenv --name sunbeam
```

Validate and run:

```bash
rally task validate tasks/autonomous_vm_waves.yaml.j2 \
  --task-args-file args/smoke.yaml

rally task start tasks/autonomous_vm_waves.yaml.j2 \
  --task-args-file args/smoke.yaml
```

Other ready-to-run Sunbeam presets:

```bash
./scripts/setup_uv.sh /path/to/clouds.yaml steady
./scripts/setup_uv.sh /path/to/clouds.yaml spiky
./scripts/setup_uv.sh /path/to/clouds.yaml failure-storm
./scripts/setup_uv.sh /path/to/clouds.yaml quota-edge
./scripts/setup_uv.sh /path/to/clouds.yaml tenant-churn
./scripts/setup_uv.sh /path/to/clouds.yaml fio-distributed
```

The generated args files are:

- `args/smoke.yaml`
- `args/steady.yaml`
- `args/spiky.yaml`
- `args/failure-storm.yaml`
- `args/quota-edge.yaml`
- `args/tenant-churn.yaml`
- `args/fio-distributed.yaml`

Preset intent:

- `smoke`
  - one VM, one result upload, fastest end-to-end validation
- `steady`
  - fixed waves of synthetic CI work
- `spiky`
  - bursty arrival schedule using the spiky controller
- `failure-storm`
  - bursty arrivals with injected guest `fail_fast` and `hang` behaviors
- `quota-edge`
  - launches until the cloud starts rejecting requests repeatedly
- `tenant-churn`
  - repeats short-lived users/projects/networks around small VM batches
- `fio-distributed`
  - boots one fio controller with a floating IP
  - boots fio worker VMs on a large tenant network
  - attaches raw Cinder volumes to workers
  - SSHes into the controller to run an explicit fio matrix
  - pulls results back to the Rally host under `artifacts/<task-id>/fio-distributed/`

The `stress-ng` preset is image-backed:

```bash
./scripts/setup_uv.sh /path/to/clouds.yaml stress-ng
rally task validate tasks/autonomous_vm_waves.yaml.j2 \
  --task-args-file args/stress-ng.yaml
rally task start tasks/autonomous_vm_waves.yaml.j2 \
  --task-args-file args/stress-ng.yaml
```

Recommended `stress-ng` flavor on Sunbeam:

```bash
openstack flavor create m1.stress-ng \
  --vcpus 2 \
  --ram 2048 \
  --disk 5
```

That flavor is sufficient for the current `stress_ng` workload profile and
avoids wasting local disk compared with larger default flavors.

## Bootstrap behavior

`scripts/setup_uv.sh` is intentionally thin. It:

- creates `.venv`
- installs Rally, `rally-openstack`, and this local package
- delegates cloud discovery and args generation to
  `python -m rally_ci_churn.bootstrap.sunbeam`

The Sunbeam bootstrap currently discovers:

- image and flavor
- external network name and ID
- DNS nameservers from the Sunbeam project subnet
- Swift auth details from `sunbeam`
- admin RC details from `sunbeam-admin`

It writes:

- `args/<preset>.yaml`
- `adminrc`

## Project layout

- `src/rally_ci_churn/plugins/`: Rally scenario plugins
- `src/rally_ci_churn/guest/`: packaged autonomous guest workload runners
- `src/rally_ci_churn/results.py`: shared result parsing/output shaping
- `src/rally_ci_churn/bootstrap/`: Sunbeam-oriented discovery and args generation
- `images/`: experimental Imagecraft recipes for pre-baked benchmark images
- `tasks/`: Rally task templates
- `scripts/setup_uv.sh`: thin local bootstrap wrapper

## Experimental Imagecraft images

For workload images that should not install packages at first boot, this repo
now includes an Imagecraft path under `images/`.

The first recipe is:

- `images/ubuntu-stress-ng/imagecraft.yaml`

This path is intentionally parallel to the Rally runtime. It does not change
task bootstrap, Glance upload, or task execution. It is only for building local
benchmark images ahead of time.

The `ubuntu-stress-ng` recipe targets Ubuntu 24.04 on amd64 and preinstalls:

- boot assets for a classic UEFI image
- `cloud-init`
- `python3`
- `ca-certificates`
- `stress-ng`

It also ships a marker file at:

- `/etc/rally-ci-churn/image-profile`

Quick validation with the locally installed Imagecraft tool:

```bash
cd images/ubuntu-stress-ng
imagecraft stage --use-lxd
```

The bootable recipe uses `mmdebstrap`, so the full build path needs elevated
privileges on the host:

To build a local artifact, use Imagecraft's normal pack flow:

```bash
cd images/ubuntu-stress-ng
sudo imagecraft pack --destructive-mode
```

To keep that destructive build isolated from your host, use the helper:

```bash
./scripts/build_imagecraft_vm.sh images/ubuntu-stress-ng
```

That helper launches a temporary Ubuntu 24.04 LXD VM, installs Imagecraft
inside it, runs `imagecraft pack --destructive-mode` there, pulls `disk.img`
back into the recipe directory, and removes the VM by default.

The recipe follows a minimal bootable classic-image layout:

- GPT disk with `efi` and `rootfs` partitions
- `mmdebstrap` rootfs bootstrap
- `ubuntu-server-minimal`, `grub`, and `linux-image-generic`
- OpenStack-oriented cloud-init datasource preference
- serial console enabled through a grub drop-in plus cloud-init/journald forwarding

The resulting `disk.img` is local-only for now; Glance upload and cloud boot
validation remain separate manual steps.

To upload the built image to Glance for the `stress-ng` preset:

```bash
openstack image create ubuntu-stress-ng \
  --file images/ubuntu-stress-ng/disk.img \
  --disk-format raw \
  --container-format bare \
  --public
```

Set the required image properties after upload:

```bash
openstack image set ubuntu-stress-ng \
  --property hw_firmware_type=uefi
```

The `hw_firmware_type=uefi` property is required for the current
Imagecraft-built image layout on this cloud.

Useful verification:

```bash
openstack image show ubuntu-stress-ng -f yaml
```

The `stress-ng` preset expects:

- Glance image name: `ubuntu-stress-ng`

## Distributed fio scenario

The fio scenario is a separate orchestration model from the autonomous runner
benchmarks:

1. Rally creates a large tenant network using `network@openstack`.
2. Rally boots one controller VM with a floating IP.
3. Rally boots worker VMs without floating IPs.
4. Rally creates and attaches raw Cinder volumes to the workers.
5. Workers start `fio --server`.
6. Rally SSHes to the controller and runs the fio matrix from there.
7. The controller writes the full artifact bundle locally.
8. Rally copies the artifact bundle back to the host instead of uploading it to Swift.

The generated `fio-distributed` preset is intentionally small and smoke-oriented:

- `client_counts: [1, 2]`
- `volumes_per_client: [1]`
- `rw_modes: ["write", "read"]`
- `block_sizes: ["1M"]`
- `numjobs: [1, 2]`
- `iodepths: [1, 32]`

Run it with:

```bash
./scripts/setup_uv.sh /path/to/clouds.yaml fio-distributed
source .venv/bin/activate
source adminrc
rally task validate tasks/fio_distributed.yaml.j2 \
  --task-args-file args/fio-distributed.yaml
rally task start tasks/fio_distributed.yaml.j2 \
  --task-args-file args/fio-distributed.yaml
```

The task writes its local artifacts under:

- `artifacts/<task-id>/fio-distributed/`

Expected files include:

- `summary.md`
- `summary.csv`
- `summary.json`
- `manifest.json`
- `inventory.json`
- `raw/*.json`
- `raw/*.stdout`

### Building the `ubuntu-fio` image

The distributed fio scenario expects a pre-baked image named `ubuntu-fio`.

Build it with Imagecraft:

```bash
./scripts/build_imagecraft_vm.sh images/ubuntu-fio
```

Upload it to Glance:

```bash
openstack image create ubuntu-fio \
  --file images/ubuntu-fio/disk.img \
  --disk-format raw \
  --container-format bare \
  --public
```

Set the required image property:

```bash
openstack image set ubuntu-fio \
  --property hw_firmware_type=uefi
```
- flavor name: `m1.stress-ng`

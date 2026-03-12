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

The primary task templates are:

- `tasks/autonomous_vm_waves.yaml.j2`
- `tasks/spiky_autonomous_vm.yaml.j2`

The primary example args files are:

- `args/autonomous_vm.example.yaml`
- `args/spiky_autonomous_vm.example.yaml`

## Fast start

This assumes:

- your `clouds.yaml` contains `sunbeam` and `sunbeam-admin`
- `sunbeam-admin` can list images, flavors, and networks
- Swift is enabled
- you want the autonomous VM scenario

Bootstrap:

```bash
./scripts/setup_uv.sh /path/to/clouds.yaml
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
  --task-args-file args/sunbeam.local.yaml

rally task start tasks/autonomous_vm_waves.yaml.j2 \
  --task-args-file args/sunbeam.local.yaml
```

For the spiky variant:

```bash
./scripts/setup_uv.sh /path/to/clouds.yaml spiky_autonomous_vm

rally task validate tasks/spiky_autonomous_vm.yaml.j2 \
  --task-args-file args/sunbeam.local.yaml

rally task start tasks/spiky_autonomous_vm.yaml.j2 \
  --task-args-file args/sunbeam.local.yaml
```

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

- `args/sunbeam.local.yaml`
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

#!/bin/sh
set -eu

usage() {
    cat <<'EOF'
Usage:
  build_imagecraft_vm.sh IMAGE_DIR

Build an Imagecraft recipe inside a temporary LXD VM and pull the resulting
disk.img back into IMAGE_DIR.

Environment overrides:
  IMAGECRAFT_CHANNEL   Snap channel for imagecraft (default: edge)
  LXD_IMAGE            LXD image to launch (default: ubuntu:24.04)
  KEEP_VM              Keep the temporary VM after the build (default: 0)
  VM_NAME              Override the generated VM name
EOF
}

IMAGE_DIR="${1:-}"
[ -n "$IMAGE_DIR" ] || {
    usage >&2
    exit 2
}

case "$IMAGE_DIR" in
    /*) ;;
    *) IMAGE_DIR="$(cd "$IMAGE_DIR" && pwd)" ;;
esac

[ -f "$IMAGE_DIR/imagecraft.yaml" ] || {
    echo "Missing imagecraft.yaml in $IMAGE_DIR" >&2
    exit 2
}

command -v lxc >/dev/null 2>&1 || {
    echo "lxc is required" >&2
    exit 2
}

IMAGECRAFT_CHANNEL="${IMAGECRAFT_CHANNEL:-edge}"
LXD_IMAGE="${LXD_IMAGE:-ubuntu:24.04}"
KEEP_VM="${KEEP_VM:-0}"
VM_NAME="${VM_NAME:-imagecraft-build-$(date +%s)}"
REMOTE_DIR="/root/image"

cleanup() {
    if [ "$KEEP_VM" != "1" ]; then
        lxc delete -f "$VM_NAME" >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT INT TERM

echo "Launching temporary builder VM: $VM_NAME" >&2
lxc launch "$LXD_IMAGE" "$VM_NAME" --vm >/dev/null

echo "Waiting for cloud-init in $VM_NAME" >&2
ready=0
for _ in $(seq 1 90); do
    if lxc exec "$VM_NAME" -- cloud-init status --wait >/dev/null 2>&1; then
        ready=1
        break
    fi
    sleep 2
done
[ "$ready" = "1" ] || {
    echo "Timed out waiting for cloud-init in $VM_NAME" >&2
    exit 1
}

echo "Installing imagecraft inside $VM_NAME" >&2
lxc exec "$VM_NAME" -- sh -lc "
    MIRROR_IP=\$(getent ahostsv4 archive.ubuntu.com | awk 'NR==1 {print \$1}')
    cat > /etc/resolv.conf <<'EOF'
nameserver 1.1.1.1
nameserver 8.8.8.8
EOF
    printf '%s %s\n' \"\$MIRROR_IP\" archive.ubuntu.com >> /etc/hosts
    apt-get update >/dev/null
    apt-get install -y software-properties-common >/dev/null
    add-apt-repository -y universe >/dev/null
    apt-get update >/dev/null
    snap install imagecraft --classic --${IMAGECRAFT_CHANNEL} >/dev/null
"

echo "Pushing recipe into $VM_NAME" >&2
lxc exec "$VM_NAME" -- mkdir -p "$REMOTE_DIR"
tar \
    --exclude='./disk.img' \
    --exclude='./imagecraft_volumes' \
    --exclude='./parts' \
    --exclude='./stage' \
    --exclude='./prime' \
    --exclude='./overlay' \
    --exclude='./cloud-init' \
    -C "$IMAGE_DIR" \
    -cf - . | lxc exec "$VM_NAME" -- tar --no-same-owner -C "$REMOTE_DIR" -xf -

echo "Building image inside $VM_NAME" >&2
lxc exec "$VM_NAME" -- sh -lc "cd '$REMOTE_DIR' && imagecraft clean >/dev/null 2>&1 || true && imagecraft pack --destructive-mode -v"

echo "Pulling image back to $IMAGE_DIR" >&2
if lxc file pull "$VM_NAME$REMOTE_DIR/disk.img" "$IMAGE_DIR/disk.img" >/dev/null 2>&1; then
    :
elif lxc file pull "$VM_NAME$REMOTE_DIR/pc.img" "$IMAGE_DIR/disk.img" >/dev/null 2>&1; then
    :
else
    echo "Could not find disk.img or pc.img in $REMOTE_DIR" >&2
    exit 1
fi

echo "Build complete: $IMAGE_DIR/disk.img" >&2

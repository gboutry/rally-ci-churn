#!/usr/bin/env bash
set -euo pipefail
#
# diagnose_fio_workers.sh
#
# Boot a controller + N fio workers on the same network, attach volumes,
# and diagnose whether fio --server starts correctly.  Leaves VMs running
# for manual inspection unless --cleanup is passed.
#
# Requirements: openstack CLI, ssh, jq
#
# Usage:
#   source adminrc
#   ./scripts/diagnose_fio_workers.sh \
#       --image ubuntu-fio \
#       --flavor m1.benchmark-xl \
#       --network external-network \
#       --workers 3 \
#       --key-name mykey \
#       --key-file ~/.ssh/id_rsa
#

usage() {
    cat <<EOF
Usage: $0 [OPTIONS]

Options:
  --image NAME           Glance image name (default: ubuntu-fio)
  --flavor NAME          Nova flavor name (default: m1.small)
  --network NAME         External network for floating IP (default: external-network)
  --workers N            Number of worker VMs (default: 3)
  --key-name NAME        Existing Nova keypair name (created if missing)
  --key-file PATH        SSH private key path (default: ~/.ssh/id_rsa)
  --fio-port PORT        fio server port (default: 8765)
  --volume-size N        Volume size in GiB (default: 2)
  --timeout N            Seconds to wait for fio server (default: 600)
  --cleanup              Delete all resources on exit
  --help                 Show this help
EOF
}

IMAGE="ubuntu-fio"
FLAVOR="m1.small"
EXT_NETWORK="external-network"
WORKER_COUNT=3
KEY_NAME=""
KEY_FILE="$HOME/.ssh/id_rsa"
FIO_PORT=8765
VOLUME_SIZE=2
TIMEOUT=600
CLEANUP=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --image)       IMAGE="$2"; shift 2 ;;
        --flavor)      FLAVOR="$2"; shift 2 ;;
        --network)     EXT_NETWORK="$2"; shift 2 ;;
        --workers)     WORKER_COUNT="$2"; shift 2 ;;
        --key-name)    KEY_NAME="$2"; shift 2 ;;
        --key-file)    KEY_FILE="$2"; shift 2 ;;
        --fio-port)    FIO_PORT="$2"; shift 2 ;;
        --volume-size) VOLUME_SIZE="$2"; shift 2 ;;
        --timeout)     TIMEOUT="$2"; shift 2 ;;
        --cleanup)     CLEANUP=true; shift ;;
        --help)        usage; exit 0 ;;
        *)             echo "Unknown option: $1" >&2; usage >&2; exit 1 ;;
    esac
done

TAG="fio-diag-$$"
CREATED_SERVERS=()
CREATED_VOLUMES=()
CREATED_FIPS=()
CONTROLLER_FIP=""
SAVED_CORES=""
SAVED_RAM=""
SAVED_INSTANCES=""
SAVED_GIGABYTES=""
SAVED_VOLUMES=""
PROJECT_ID=""

cleanup_resources() {
    # Always restore quotas, even if we keep VMs
    if [ -n "$PROJECT_ID" ] && [ -n "$SAVED_CORES" ]; then
        echo ""
        echo "=== Restoring quotas ==="
        openstack quota set "$PROJECT_ID" \
            --cores "$SAVED_CORES" \
            --ram "$SAVED_RAM" \
            --instances "$SAVED_INSTANCES" \
            --gigabytes "$SAVED_GIGABYTES" \
            --volumes "$SAVED_VOLUMES" 2>/dev/null || true
        echo "  Restored: cores=$SAVED_CORES ram=$SAVED_RAM instances=$SAVED_INSTANCES gigabytes=$SAVED_GIGABYTES volumes=$SAVED_VOLUMES"
    fi

    if [ "$CLEANUP" != "true" ]; then
        echo ""
        echo "=== Resources left running for inspection ==="
        echo "  Tag: $TAG"
        echo "  Controller FIP: ${CONTROLLER_FIP:-none}"
        echo "  Servers: ${CREATED_SERVERS[*]:-none}"
        echo "  Volumes: ${CREATED_VOLUMES[*]:-none}"
        echo ""
        echo "  SSH: ssh -i $KEY_FILE ubuntu@$CONTROLLER_FIP"
        echo ""
        echo "  To clean up later:"
        echo "    openstack server delete ${CREATED_SERVERS[*]}"
        [ ${#CREATED_VOLUMES[@]} -gt 0 ] && echo "    openstack volume delete ${CREATED_VOLUMES[*]}"
        [ ${#CREATED_FIPS[@]} -gt 0 ] && echo "    openstack floating ip delete ${CREATED_FIPS[*]}"
        return
    fi
    echo ""
    echo "=== Cleaning up ==="
    for sid in "${CREATED_SERVERS[@]}"; do
        echo "  Deleting server $sid"
        openstack server delete "$sid" --wait 2>/dev/null || true
    done
    for vid in "${CREATED_VOLUMES[@]}"; do
        echo "  Deleting volume $vid"
        openstack volume delete "$vid" 2>/dev/null || true
    done
    for fip in "${CREATED_FIPS[@]}"; do
        echo "  Deleting floating IP $fip"
        openstack floating ip delete "$fip" 2>/dev/null || true
    done
    echo "  Done."
}
trap cleanup_resources EXIT

# --- Save current quotas and set unlimited ---
echo "=== Setup ==="

PROJECT_ID=$(openstack token issue -f value -c project_id)
echo "  Project: $PROJECT_ID"
SAVED_CORES=$(openstack quota show "$PROJECT_ID" -f value -c cores)
SAVED_RAM=$(openstack quota show "$PROJECT_ID" -f value -c ram)
SAVED_INSTANCES=$(openstack quota show "$PROJECT_ID" -f value -c instances)
SAVED_GIGABYTES=$(openstack quota show "$PROJECT_ID" -f value -c gigabytes)
SAVED_VOLUMES=$(openstack quota show "$PROJECT_ID" -f value -c volumes)
echo "  Saved quotas: cores=$SAVED_CORES ram=$SAVED_RAM instances=$SAVED_INSTANCES gigabytes=$SAVED_GIGABYTES volumes=$SAVED_VOLUMES"
openstack quota set "$PROJECT_ID" \
    --cores -1 --ram -1 --instances -1 --gigabytes -1 --volumes -1
echo "  Quotas set to unlimited (will restore on exit)"

if [ -z "$KEY_NAME" ]; then
    KEY_NAME="$TAG-key"
    echo "  Creating keypair $KEY_NAME..."
    TMP_KEY=$(mktemp)
    openstack keypair create "$KEY_NAME" > "$TMP_KEY"
    KEY_FILE="$TMP_KEY"
    chmod 600 "$KEY_FILE"
    echo "  Private key: $KEY_FILE"
fi

# Create a tenant network
NETWORK_NAME="$TAG-net"
SUBNET_NAME="$TAG-subnet"
ROUTER_NAME="$TAG-router"
echo "  Creating network $NETWORK_NAME..."
NET_ID=$(openstack network create "$NETWORK_NAME" -f value -c id)
openstack subnet create "$SUBNET_NAME" --network "$NET_ID" \
    --subnet-range "10.99.0.0/24" --dns-nameserver 8.8.8.8 -f value -c id >/dev/null
ROUTER_ID=$(openstack router create "$ROUTER_NAME" -f value -c id)
openstack router set "$ROUTER_ID" --external-gateway "$EXT_NETWORK"
openstack router add subnet "$ROUTER_ID" "$SUBNET_NAME"

# --- Worker cloud-init (same as fio_distributed.py) ---
WORKER_USERDATA=$(cat <<'CLOUDINIT'
#cloud-config
write_files:
  - path: /usr/local/bin/rally-fio-worker.sh
    permissions: "0755"
    content: |
      #!/bin/bash
      set -euo pipefail
      expected_volumes="1"
      fio_port="8765"
      mkdir -p /var/lib/rally-fio/devices
      while true; do
        root_source=$(findmnt -n -o SOURCE / || true)
        root_pkname=$(lsblk -no PKNAME "$root_source" 2>/dev/null || true)
        root_disk=""
        if [ -n "$root_pkname" ]; then
          root_disk="/dev/$root_pkname"
        fi
        mapfile -t disks < <(lsblk -dnpo NAME,TYPE | awk '$2=="disk" {print $1}')
        data_disks=()
        for disk in "${disks[@]}"; do
          if [ -n "$root_disk" ] && [ "$disk" = "$root_disk" ]; then
            continue
          fi
          data_disks+=("$disk")
        done
        if [ "${#data_disks[@]}" -ge "$expected_volumes" ]; then
          break
        fi
        sleep 2
      done
      rm -f /var/lib/rally-fio/devices/vol*
      for index in $(seq 1 "$expected_volumes"); do
        disk="${data_disks[$((index - 1))]}"
        ln -sfn "$disk" "/var/lib/rally-fio/devices/vol$(printf '%02d' "$index")"
      done
      pkill -f 'fio --server' || true
      exec fio --server=,"${fio_port}" --daemonize=/var/run/rally-fio-server.pid
runcmd:
  - [ cloud-init-per, once, rally-fio-worker-start, /bin/bash, -lc, "/usr/local/bin/rally-fio-worker.sh" ]
CLOUDINIT
)

USERDATA_FILE=$(mktemp)
echo "$WORKER_USERDATA" > "$USERDATA_FILE"

# --- Boot controller ---
echo ""
echo "=== Booting controller ==="
CTRL_ID=$(openstack server create "$TAG-controller" \
    --image "$IMAGE" --flavor "$FLAVOR" \
    --network "$NET_ID" --key-name "$KEY_NAME" \
    --wait -f value -c id)
CREATED_SERVERS+=("$CTRL_ID")
echo "  Controller: $CTRL_ID"

FIP_ID=$(openstack floating ip create "$EXT_NETWORK" -f value -c id)
CREATED_FIPS+=("$FIP_ID")
CONTROLLER_FIP=$(openstack floating ip show "$FIP_ID" -f value -c floating_ip_address)
openstack server add floating ip "$CTRL_ID" "$CONTROLLER_FIP"
echo "  Floating IP: $CONTROLLER_FIP"

# --- Boot workers ---
echo ""
echo "=== Booting $WORKER_COUNT workers ==="
WORKER_IDS=()
WORKER_IPS=()
for i in $(seq 1 "$WORKER_COUNT"); do
    WID=$(openstack server create "$TAG-worker-$i" \
        --image "$IMAGE" --flavor "$FLAVOR" \
        --network "$NET_ID" --key-name "$KEY_NAME" \
        --user-data "$USERDATA_FILE" \
        --wait -f value -c id)
    CREATED_SERVERS+=("$WID")
    WORKER_IDS+=("$WID")
    WIP=$(openstack server show "$WID" -f json -c addresses | \
        python3 -c "import json,sys,re; d=json.load(sys.stdin); m=re.search(r'(\d+\.\d+\.\d+\.\d+)', str(d)); print(m.group(1) if m else 'unknown')")
    WORKER_IPS+=("$WIP")
    echo "  Worker $i: $WID ($WIP)"
done

# --- Create and attach volumes ---
echo ""
echo "=== Creating and attaching volumes ==="
for i in $(seq 1 "$WORKER_COUNT"); do
    idx=$((i - 1))
    VID=$(openstack volume create "$TAG-vol-$i" --size "$VOLUME_SIZE" -f value -c id)
    CREATED_VOLUMES+=("$VID")
    echo "  Volume $VID -> worker ${WORKER_IDS[$idx]}"
    openstack volume set "$VID" --state available 2>/dev/null || true
    # Wait for volume to be available
    for _ in $(seq 1 30); do
        VSTATUS=$(openstack volume show "$VID" -f value -c status)
        [ "$VSTATUS" = "available" ] && break
        sleep 2
    done
    openstack server add volume "${WORKER_IDS[$idx]}" "$VID"
done

# --- Wait for SSH to controller ---
echo ""
echo "=== Waiting for SSH to controller ==="
SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=5"
for attempt in $(seq 1 60); do
    if ssh $SSH_OPTS -i "$KEY_FILE" "ubuntu@$CONTROLLER_FIP" true 2>/dev/null; then
        echo "  SSH ready after $((attempt * 3))s"
        break
    fi
    sleep 3
done

# --- Diagnose from controller ---
echo ""
echo "=== Diagnosing workers from controller ==="
echo ""

DIAG_SCRIPT=$(cat <<PYEOF
import socket, subprocess, time, sys

workers = sys.argv[1:]
fio_port = $FIO_PORT
timeout = $TIMEOUT
deadline = time.monotonic() + timeout

print(f"Probing {len(workers)} workers on port {fio_port}, timeout {timeout}s")
print()

# Phase 1: Check TCP connectivity to SSH (port 22)
print("--- Phase 1: SSH port check (instant) ---")
for ip in workers:
    try:
        s = socket.create_connection((ip, 22), timeout=3)
        s.close()
        print(f"  {ip}:22  OK")
    except Exception as e:
        print(f"  {ip}:22  FAILED ({e})")

print()
print("--- Phase 2: fio port check (polling) ---")
pending = set(workers)
start = time.monotonic()
while pending and time.monotonic() < deadline:
    still_pending = set()
    for ip in sorted(pending):
        try:
            s = socket.create_connection((ip, fio_port), timeout=2)
            s.close()
            elapsed = time.monotonic() - start
            print(f"  {ip}:{fio_port}  READY after {elapsed:.0f}s")
        except Exception:
            still_pending.add(ip)
    pending = still_pending
    if pending:
        time.sleep(5)

print()
if pending:
    print(f"--- FAILED: {len(pending)} workers never became ready ---")
    for ip in sorted(pending):
        print(f"  {ip}:{fio_port}  TIMED OUT")

    print()
    print("--- Phase 3: Diagnosing failed workers ---")
    for ip in sorted(pending):
        print(f"  [{ip}] Checking cloud-init and fio status...")
        try:
            r = subprocess.run(
                ["ssh", "-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null",
                 "-o", "LogLevel=ERROR", "-o", "ConnectTimeout=10",
                 f"ubuntu@{ip}",
                 "echo CLOUD_INIT_STATUS=\$(cloud-init status 2>/dev/null || echo unknown);"
                 "echo FIO_PROCESS=\$(pgrep -c fio 2>/dev/null || echo 0);"
                 "echo FIO_LISTEN=\$(ss -tlnp 2>/dev/null | grep $FIO_PORT || echo none);"
                 "echo DISKS=\$(lsblk -dnpo NAME,TYPE 2>/dev/null | awk '\$2==\"disk\"{print \$1}' | tr '\\n' ' ');"
                 "echo CLOUD_INIT_LOG_TAIL=;"
                 "tail -5 /var/log/cloud-init-output.log 2>/dev/null || echo 'no log'"],
                capture_output=True, text=True, timeout=30
            )
            for line in r.stdout.strip().split("\\n"):
                print(f"    {line}")
            if r.stderr.strip():
                print(f"    STDERR: {r.stderr.strip()[:200]}")
        except Exception as e:
            print(f"    SSH failed: {e}")
    sys.exit(1)
else:
    print(f"--- ALL {len(workers)} workers ready ---")
PYEOF
)

ssh $SSH_OPTS -i "$KEY_FILE" "ubuntu@$CONTROLLER_FIP" \
    python3 - "${WORKER_IPS[@]}" <<< "$DIAG_SCRIPT"

rm -f "$USERDATA_FILE"
echo ""
echo "=== Diagnosis complete ==="

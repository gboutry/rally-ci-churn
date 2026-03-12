#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_DIR="${ROOT_DIR}/.venv"
DEFAULT_SCENARIO="autonomous_vm"
DEFAULT_ARGS_OUTPUT="${ROOT_DIR}/args/sunbeam.local.yaml"
DEFAULT_ADMINRC_OUTPUT="${ROOT_DIR}/adminrc"
RALLY_OPENSTACK_REPO_URL="${RALLY_OPENSTACK_REPO_URL:-https://opendev.org/openstack/rally-openstack}"
RALLY_OPENSTACK_REF="${RALLY_OPENSTACK_REF:-4ceffd8c39414c1a8ede884c62bf06080aede5cd}"
RALLY_OPENSTACK_REQUIREMENT="rally-openstack @ git+${RALLY_OPENSTACK_REPO_URL}@${RALLY_OPENSTACK_REF}"

usage() {
    cat <<EOF
Usage: $0 <clouds.yaml> [scenario-id] [output-args.yaml]

Bootstraps a local .venv, installs Rally/OpenStack dependencies and the local
plugin package, then generates Sunbeam-oriented args and adminrc files.

Supported scenario ids:
  autonomous_vm
  spiky_autonomous_vm
EOF
}

if [ "${1:-}" = "-h" ] || [ "${1:-}" = "--help" ]; then
    usage
    exit 0
fi

if [ $# -lt 1 ] || [ $# -gt 3 ]; then
    usage >&2
    exit 1
fi

CLOUDS_YAML="$(realpath "$1")"
SCENARIO_ID="${2:-${DEFAULT_SCENARIO}}"
ARGS_OUTPUT="${3:-${DEFAULT_ARGS_OUTPUT}}"
ARGS_OUTPUT="$(realpath -m "${ARGS_OUTPUT}")"
ADMINRC_OUTPUT="${ADMINRC_OUTPUT:-${DEFAULT_ADMINRC_OUTPUT}}"
ADMINRC_OUTPUT="$(realpath -m "${ADMINRC_OUTPUT}")"

if [ ! -f "${CLOUDS_YAML}" ]; then
    echo "clouds.yaml not found: ${CLOUDS_YAML}" >&2
    exit 1
fi

if ! command -v uv >/dev/null 2>&1; then
    echo "uv is required but was not found in PATH." >&2
    echo "Install it first: https://docs.astral.sh/uv/getting-started/installation/" >&2
    exit 1
fi

cd "${ROOT_DIR}"

if [ ! -d "${VENV_DIR}" ]; then
    uv venv "${VENV_DIR}"
fi

uv pip install --python "${VENV_DIR}/bin/python" \
    "pip>=24.2" \
    "setuptools>=75.0" \
    "wheel>=0.44" \
    "paramiko<4" \
    "${RALLY_OPENSTACK_REQUIREMENT}" \
    "python-openstackclient>=7.0.0" \
    "openstacksdk>=4.0.0" \
    "jinja2>=3.1.0" \
    "pyyaml>=6.0.0" \
    -e .

export RALLY_CI_CHURN_OPENSTACK_BIN="${VENV_DIR}/bin/openstack"

"${VENV_DIR}/bin/python" -m rally_ci_churn.bootstrap.sunbeam \
    --clouds-yaml "${CLOUDS_YAML}" \
    --scenario "${SCENARIO_ID}" \
    --output-args "${ARGS_OUTPUT}" \
    --output-adminrc "${ADMINRC_OUTPUT}"

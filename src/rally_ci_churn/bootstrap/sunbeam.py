"""Sunbeam-oriented bootstrap helper."""

from __future__ import annotations

import argparse
import ast
import base64
import json
import os
import shlex
import stat
import subprocess
import tempfile
import textwrap
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Generator, cast
from urllib.parse import urlparse

import yaml

_cloud_overrides: dict[str, str] = {}


@contextmanager
def cloud_overrides(
    *,
    image: str | None = None,
    flavor: str | None = None,
) -> Generator[None, None, None]:
    """Temporarily override image/flavor selection inside preset builders."""
    prev = dict(_cloud_overrides)
    if image:
        _cloud_overrides["image"] = image
    if flavor:
        _cloud_overrides["flavor"] = flavor
    try:
        yield
    finally:
        _cloud_overrides.clear()
        _cloud_overrides.update(prev)


DEFAULT_PRESET = "smoke"
SUPPORTED_PRESETS = {
    "smoke",
    "steady",
    "spiky",
    "stress-ng",
    "fio-distributed",
    "mixed-pressure",
    "net-many-to-one",
    "net-many-to-one-http",
    "net-ring",
    "failure-storm",
    "quota-edge",
    "tenant-churn",
}

PRESET_BUILDERS = {}


PresetBuilder = Callable[[Path, dict[str, object]], tuple[dict[str, object], str]]


@dataclass(frozen=True)
class PresetDefinition:
    preset_id: str
    scenario_name: str
    task_path: str
    usage_tier: str
    summary: str
    operator_guidance: tuple[str, ...] = ()
    required_images: tuple[str, ...] = ()
    required_services: tuple[str, ...] = ()
    first_knobs: tuple[str, ...] = ()
    focus_sections: tuple[str, ...] = ()
    section_notes: tuple[tuple[str, str], ...] = ()
    recommended_next_preset: str | None = None


PRESET_DEFINITIONS: dict[str, PresetDefinition] = {}


SECTION_DESCRIPTIONS: dict[str, str] = {
    "scenario": "Autonomous VM scenario shape and timeouts.",
    "cloud": "Cloud-specific image, flavor, and external network selection. Set these once per cloud.",
    "network": "Tenant network defaults for this preset. Usually leave these alone unless the CIDR clashes with existing networks.",
    "boot_volume": "Root-disk choice for every VM in this scenario. Leave disabled until the ephemeral path is validated.",
    "users": "Rally tenant and user context. Leave at one tenant and one user unless multi-tenant behavior is the point of the run.",
    "quotas": "Quota overrides for the Rally-created tenant. Keep these broad for baseline runs unless you are probing failure behavior.",
    "storage": "Artifact upload settings for guest-side logs and benchmark payloads.",
    "workload": "Guest-side workload profile for the autonomous VM family.",
    "schedule": "Time-based arrival pattern for spiky autonomous VM churn.",
    "controller": "Controller VM access and in-scenario provisioning parallelism.",
    "cinder": "Extra data-volume defaults for distributed fio workers.",
    "fio": "Block benchmark topology and fio profile matrix.",
    "traffic": "Network traffic mode, protocol mix, and stream settings.",
    "many_to_one": "One-server-many-clients benchmark shape.",
    "server_volume": "Optional HTTP payload backing volume for the many-to-one HTTP mode.",
    "ring": "Participant count and east-west ring shape.",
    "mixed": "Top-level mixed-pressure runtime and failure handling.",
    "churn": "Embedded autonomous churn settings used inside mixed-pressure.",
    "artifacts": "Local artifact staging for controller-driven scenarios.",
    "quota_edge": "Launch-until-refusal pacing and stop conditions.",
    "tenant_churn": "Short-lived tenant lifecycle loops and per-cycle VM count.",
}


def _run_openstack(clouds_yaml: Path, cloud_name: str, *args: str) -> str:
    env = os.environ.copy()
    env["OS_CLIENT_CONFIG_FILE"] = str(clouds_yaml)
    env["OS_CLOUD"] = cloud_name
    command = env.get("RALLY_CI_CHURN_OPENSTACK_BIN", "openstack")
    result = subprocess.run(
        [command, *args],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )
    return result.stdout.strip()


def _pick_exact_or_prefix(names: list[str], exact: tuple[str, ...], prefix: str) -> str:
    for candidate in exact:
        if candidate in names:
            return candidate
    for name in names:
        if name.startswith(prefix):
            return name
    raise RuntimeError(f"Unable to choose a value from {names!r}")


def _resolve_cacert(source_path: Path, value: str) -> str:
    if not value:
        return ""
    candidate = Path(value).expanduser()
    if candidate.is_file():
        return str(candidate.resolve())
    relative_candidate = (source_path.parent / value).resolve()
    if relative_candidate.is_file():
        return str(relative_candidate)
    basename_candidate = (source_path.parent / candidate.name).resolve()
    if basename_candidate.is_file():
        return str(basename_candidate)
    return ""


def _normalize_clouds(source_path: Path) -> dict[str, object]:
    config = yaml.safe_load(source_path.read_text(encoding="utf-8")) or {}
    for cloud_name in ("sunbeam", "sunbeam-admin"):
        cloud_entry = config.get("clouds", {}).get(cloud_name)
        if isinstance(cloud_entry, dict):
            cloud_entry["cacert"] = _resolve_cacert(source_path, str(cloud_entry.get("cacert", "")))
    return config


def _select_sunbeam_dns(clouds_yaml: Path) -> list[str]:
    subnets = json.loads(_run_openstack(clouds_yaml, "sunbeam", "subnet", "list", "-f", "json", "-c", "ID", "-c", "Name", "-c", "Network"))
    networks = json.loads(_run_openstack(clouds_yaml, "sunbeam", "network", "list", "-f", "json", "-c", "ID", "-c", "Name"))
    network_ids_by_name = {network["Name"]: network["ID"] for network in networks}
    preferred_subnet_id = ""
    for subnet_name in ("gtestos-subnet",):
        for subnet in subnets:
            if subnet.get("Name") == subnet_name:
                preferred_subnet_id = subnet["ID"]
                break
    if not preferred_subnet_id:
        network_id = network_ids_by_name.get("gtestos-network")
        if network_id:
            for subnet in subnets:
                if subnet.get("Network") == network_id:
                    preferred_subnet_id = subnet["ID"]
                    break
    if not preferred_subnet_id and len(subnets) == 1:
        preferred_subnet_id = subnets[0]["ID"]
    if not preferred_subnet_id:
        return []
    raw = _run_openstack(clouds_yaml, "sunbeam", "subnet", "show", "-f", "value", "-c", "dns_nameservers", preferred_subnet_id)
    if not raw:
        return []
    try:
        return list(ast.literal_eval(raw))
    except (SyntaxError, ValueError):
        return []


def _build_base_args(clouds_yaml: Path, config: dict[str, object]) -> dict[str, object]:
    sunbeam = _mapping_value(_mapping_value(config, "clouds"), "sunbeam")
    sunbeam_auth = _mapping_value(sunbeam, "auth")
    image_names = _run_openstack(clouds_yaml, "sunbeam-admin", "image", "list", "-f", "value", "-c", "Name").splitlines()
    flavor_names = _run_openstack(clouds_yaml, "sunbeam-admin", "flavor", "list", "-f", "value", "-c", "Name").splitlines()
    external_networks = _run_openstack(clouds_yaml, "sunbeam-admin", "network", "list", "--external", "-f", "value", "-c", "Name").splitlines()
    image_name = _pick_exact_or_prefix([name for name in image_names if name], ("ubuntu",), "ubuntu")
    flavor_name = _pick_exact_or_prefix([name for name in flavor_names if name], ("m1.benchmark", "m1.tiny", "m1.small"), "m1.")
    if "external-network" in external_networks:
        external_network = "external-network"
    elif len([name for name in external_networks if name]) == 1:
        external_network = [name for name in external_networks if name][0]
    else:
        raise RuntimeError(f"Unable to determine external network from {external_networks!r}")
    external_network_id = _run_openstack(clouds_yaml, "sunbeam-admin", "network", "show", "-f", "value", "-c", "id", external_network)
    swift_cacert = str(sunbeam.get("cacert", "") or "")
    swift_cacert_b64 = ""
    if swift_cacert:
        swift_cacert_b64 = base64.b64encode(Path(swift_cacert).read_bytes()).decode("ascii")
    return {
        "title": "Rally CI churn",
        "description": "Autonomous CI-like runner churn without floating IPs",
        "scenario": {
            "family": "autonomous_vm",
            "name": "CIChurn.boot_autonomous_vm",
            "waves": 1,
            "vm_count": 1,
            "task_concurrency": 1,
            "timeout_seconds": 3600,
            "timeout_mode": "fail",
            "console_log_length": 400,
            "allow_guest_errors": False,
            "allow_guest_timeouts": False,
        },
        "cloud": {
            "image_name": image_name,
            "flavor_name": flavor_name,
            "external_network_name": external_network,
            "external_network_id": external_network_id,
        },
        "network": {
            "dns_nameservers": _select_sunbeam_dns(clouds_yaml),
        },
        "boot_volume": {
            "enabled": False,
            "size_gib": 20,
            "volume_type": None,
        },
        "users": {
            "tenants": 1,
            "users_per_tenant": 1,
        },
        "quotas": {
            "nova": {
                "instances": -1,
                "cores": -1,
                "ram": -1,
                "floating_ips": -1,
                "fixed_ips": -1,
                "key_pairs": -1,
                "security_groups": -1,
                "security_group_rules": -1,
            },
            "cinder": {
                "gigabytes": -1,
                "snapshots": -1,
                "volumes": -1,
            },
            "neutron": {
                "network": -1,
                "subnet": -1,
                "port": -1,
                "router": -1,
                "floatingip": -1,
                "security_group": -1,
                "security_group_rule": -1,
            },
        },
        "storage": {
            "artifact_container": "rally-ci-churn",
            "artifact_ttl_seconds": 0,
            "swift_auth_url": sunbeam_auth["auth_url"],
            "swift_username": sunbeam_auth["username"],
            "swift_password": sunbeam_auth["password"],
            "swift_project_name": sunbeam_auth["project_name"],
            "swift_user_domain_name": sunbeam_auth["user_domain_name"],
            "swift_project_domain_name": sunbeam_auth["project_domain_name"],
            "swift_interface": "public",
            "swift_region_name": sunbeam.get("region_name", "") or "",
            "swift_cacert": swift_cacert,
            "swift_cacert_b64": swift_cacert_b64,
        },
        "workload": {
            "profile": "smoke",
            "params": {},
        },
    }


def _select_sections(rendered: dict[str, object], *section_names: str) -> dict[str, object]:
    return {section_name: rendered[section_name] for section_name in section_names if section_name in rendered}


def _mapping_value(mapping: dict[str, object], key: str) -> dict[str, object]:
    return cast(dict[str, object], mapping[key])


def _comment_lines(text: str) -> list[str]:
    if not text:
        return ["#"]
    wrapped = textwrap.wrap(text, width=88)
    return [f"# {line}" for line in wrapped] or ["#"]


def _render_comment_block(lines: tuple[str, ...] | list[str]) -> list[str]:
    rendered: list[str] = []
    for line in lines:
        if not line:
            rendered.append("#")
            continue
        rendered.extend(_comment_lines(line))
    return rendered


def _section_comment(definition: PresetDefinition, key: str) -> list[str]:
    overrides = dict(definition.section_notes)
    note = overrides.get(key, SECTION_DESCRIPTIONS.get(key, ""))
    if not note:
        return []
    if key in definition.focus_sections:
        note = f"Tune first. {note}"
    return _render_comment_block([note])


def render_preset_args(preset: str, args_data: dict[str, object]) -> str:
    definition = get_preset_definition(preset)
    lines: list[str] = []
    lines.extend(
        _render_comment_block(
            [
                "Generated by rally_ci_churn.bootstrap.sunbeam.",
                f"Preset: {definition.preset_id}",
                f"Scenario: {definition.scenario_name}",
                f"Task template: {definition.task_path}",
                f"Usage tier: {definition.usage_tier}",
                f"Summary: {definition.summary}",
            ]
        )
    )
    if definition.operator_guidance:
        lines.append("#")
        lines.extend(_render_comment_block(["Guidance:", *[f"- {line}" for line in definition.operator_guidance]]))
    if definition.required_services:
        lines.append("#")
        lines.extend(_render_comment_block([f"Required services: {', '.join(definition.required_services)}"]))
    if definition.required_images:
        lines.append("#")
        lines.extend(_render_comment_block([f"Required images: {', '.join(definition.required_images)}"]))
    if definition.first_knobs:
        lines.append("#")
        lines.extend(_render_comment_block([f"Tune first: {', '.join(definition.first_knobs)}"]))
    if definition.recommended_next_preset:
        lines.append("#")
        lines.extend(_render_comment_block([f"Recommended next preset: {definition.recommended_next_preset}"]))

    for index, (key, value) in enumerate(args_data.items()):
        lines.append("")
        lines.extend(_section_comment(definition, key))
        fragment = yaml.safe_dump({key: value}, sort_keys=False).rstrip()
        lines.extend(fragment.splitlines())
        if index == len(args_data) - 1:
            continue
    return "\n".join(lines) + "\n"


def get_preset_definition(preset: str) -> PresetDefinition:
    if preset not in PRESET_DEFINITIONS:
        raise RuntimeError(f"Unsupported preset selector: {preset}")
    return PRESET_DEFINITIONS[preset]


def _pick_custom_image(clouds_yaml: Path, desired_name: str) -> str:
    if "image" in _cloud_overrides:
        return _cloud_overrides["image"]
    image_names = _run_openstack(
        clouds_yaml, "sunbeam-admin", "image", "list", "-f", "value", "-c", "Name"
    ).splitlines()
    if desired_name in image_names:
        return desired_name
    raise RuntimeError(
        f"Required image '{desired_name}' was not found. Build and upload it before using this preset."
    )


def _pick_preferred_flavor(clouds_yaml: Path, preferred: tuple[str, ...], fallback_prefix: str = "m1.") -> str:
    if "flavor" in _cloud_overrides:
        return _cloud_overrides["flavor"]
    flavor_names = _run_openstack(
        clouds_yaml, "sunbeam-admin", "flavor", "list", "-f", "value", "-c", "Name"
    ).splitlines()
    return _pick_exact_or_prefix([name for name in flavor_names if name], preferred, fallback_prefix)


def _build_smoke_preset(clouds_yaml: Path, config: dict[str, object]) -> tuple[dict[str, object], str]:
    rendered = _build_base_args(clouds_yaml, config)
    rendered["description"] = "Autonomous CI-like runner churn smoke"
    workload = _mapping_value(rendered, "workload")
    workload["profile"] = "smoke"
    return (
        _select_sections(
            rendered,
            "title",
            "description",
            "scenario",
            "cloud",
            "network",
            "boot_volume",
            "users",
            "quotas",
            "storage",
            "workload",
        ),
        "tasks/autonomous_vm_waves.yaml.j2",
    )


def _build_steady_preset(clouds_yaml: Path, config: dict[str, object]) -> tuple[dict[str, object], str]:
    rendered = _build_base_args(clouds_yaml, config)
    scenario = _mapping_value(rendered, "scenario")
    workload = _mapping_value(rendered, "workload")
    rendered["description"] = "Autonomous CI-like runner churn with steady waves"
    scenario["waves"] = 5
    scenario["vm_count"] = 5
    scenario["task_concurrency"] = 5
    workload["profile"] = "synthetic_ci"
    return (
        _select_sections(
            rendered,
            "title",
            "description",
            "scenario",
            "cloud",
            "network",
            "boot_volume",
            "users",
            "quotas",
            "storage",
            "workload",
        ),
        "tasks/autonomous_vm_waves.yaml.j2",
    )


def _build_spiky_preset(clouds_yaml: Path, config: dict[str, object]) -> tuple[dict[str, object], str]:
    rendered = _build_base_args(clouds_yaml, config)
    workload = _mapping_value(rendered, "workload")
    rendered["description"] = "Autonomous CI-like runner churn with a spiky arrival schedule"
    rendered["scenario"] = {
        "family": "autonomous_vm",
        "name": "CIChurn.spiky_autonomous_vm",
        "timeout_seconds": 3600,
        "timeout_mode": "fail",
        "console_log_length": 400,
        "allow_guest_errors": False,
        "allow_guest_timeouts": False,
    }
    rendered["schedule"] = {
        "duration_seconds": 300,
        "max_active_vms": 10,
        "baseline_launches_per_minute": 12,
        "launch_tick_seconds": 1,
        "burst_windows": [
            {"start_second": 30, "end_second": 60, "launch_rate_multiplier": 4.0},
            {"start_second": 150, "end_second": 180, "launch_rate_multiplier": 2.5},
        ],
    }
    workload["profile"] = "synthetic_ci"
    return (
        _select_sections(
            rendered,
            "title",
            "description",
            "scenario",
            "cloud",
            "network",
            "boot_volume",
            "users",
            "quotas",
            "storage",
            "workload",
            "schedule",
        ),
        "tasks/spiky_autonomous_vm.yaml.j2",
    )


def _build_stress_ng_preset(clouds_yaml: Path, config: dict[str, object]) -> tuple[dict[str, object], str]:
    rendered = _build_base_args(clouds_yaml, config)
    cloud = _mapping_value(rendered, "cloud")
    scenario = _mapping_value(rendered, "scenario")
    rendered["description"] = "Autonomous stress-ng runner churn on a pre-baked image"
    cloud["image_name"] = _pick_custom_image(clouds_yaml, "ubuntu-stress-ng")
    cloud["flavor_name"] = "m1.stress-ng"
    scenario["waves"] = 1
    scenario["vm_count"] = 3
    scenario["task_concurrency"] = 3
    rendered["workload"] = {
        "profile": "stress_ng",
        "params": {
            "duration_seconds": 120,
            "cpu_workers": 2,
            "vm_workers": 1,
            "vm_bytes": "256M",
        },
    }
    return (
        _select_sections(
            rendered,
            "title",
            "description",
            "scenario",
            "cloud",
            "network",
            "boot_volume",
            "users",
            "quotas",
            "storage",
            "workload",
        ),
        "tasks/autonomous_vm_waves.yaml.j2",
    )


def _build_fio_distributed_preset(
    clouds_yaml: Path,
    config: dict[str, object],
) -> tuple[dict[str, object], str]:
    rendered = _build_base_args(clouds_yaml, config)
    cloud = _mapping_value(rendered, "cloud")
    network = _mapping_value(rendered, "network")
    rendered["title"] = "Distributed FIO benchmark"
    rendered["description"] = "Controller/worker fio benchmark with attached block devices"
    rendered["cloud"] = {
        "controller_image_name": _pick_custom_image(clouds_yaml, "ubuntu-fio"),
        "controller_flavor_name": "m1.small",
        "worker_image_name": "ubuntu-fio",
        "worker_flavor_name": "m1.small",
        "external_network_name": cloud["external_network_name"],
        "external_network_id": cloud["external_network_id"],
    }
    network["start_cidr"] = "10.77.0.0/22"
    rendered["controller"] = {
        "ssh_user": "ubuntu",
        "ssh_connect_timeout_seconds": 300,
        "command_timeout_seconds": 1800,
        "boot_concurrency": 4,
        "volume_concurrency": 4,
    }
    rendered["cinder"] = {
        "volume_size_gib": 10,
        "volume_type": None,
    }
    rendered["fio"] = {
        "client_counts": [1, 2],
        "volumes_per_client": [1],
        "profile_names": ["mixed-workload", "db-workload"],
        "numjobs": [1, 2],
        "iodepths": [1],
        "runtime_seconds": 30,
        "ramp_time_seconds": 5,
        "fio_port": 8765,
        "ioengine": "io_uring",
    }
    rendered["artifacts"] = {"root_dir": "artifacts"}
    return (
        _select_sections(
            rendered,
            "title",
            "description",
            "cloud",
            "network",
            "boot_volume",
            "users",
            "quotas",
            "controller",
            "cinder",
            "fio",
            "artifacts",
        ),
        "tasks/fio_distributed.yaml.j2",
    )


def _build_net_many_to_one_preset(
    clouds_yaml: Path,
    config: dict[str, object],
) -> tuple[dict[str, object], str]:
    rendered = _build_base_args(clouds_yaml, config)
    cloud = _mapping_value(rendered, "cloud")
    network = _mapping_value(rendered, "network")
    image_name = _pick_custom_image(clouds_yaml, "ubuntu-netbench")
    flavor_name = _pick_preferred_flavor(clouds_yaml, ("m1.netbench", "m1.small"))
    rendered["title"] = "Many-to-one network benchmark"
    rendered["description"] = "One server, many clients, and one controller over a tenant overlay network"
    rendered["cloud"] = {
        "controller_image_name": image_name,
        "controller_flavor_name": flavor_name,
        "server_image_name": image_name,
        "server_flavor_name": flavor_name,
        "client_image_name": image_name,
        "client_flavor_name": flavor_name,
        "external_network_name": cloud["external_network_name"],
        "external_network_id": cloud["external_network_id"],
    }
    network["start_cidr"] = "10.78.0.0/22"
    rendered["controller"] = {
        "ssh_user": "ubuntu",
        "ssh_connect_timeout_seconds": 300,
        "command_timeout_seconds": 1800,
        "boot_concurrency": 4,
    }
    rendered["traffic"] = {
        "mode": "iperf3",
        "protocols": ["tcp", "udp"],
        "duration_seconds": 20,
        "ramp_time_seconds": 5,
        "base_port": 5201,
        "flow_direction": "server_to_client",
        "parallel_streams": [4],
        "udp_target_mbps": [500],
    }
    rendered["many_to_one"] = {
        "client_count": 8,
    }
    rendered["server_volume"] = {
        "size_gib": 2,
        "volume_type": None,
        "file_count": 4,
        "file_size_mib": 128,
    }
    rendered["artifacts"] = {"root_dir": "artifacts"}
    return (
        _select_sections(
            rendered,
            "title",
            "description",
            "cloud",
            "network",
            "boot_volume",
            "users",
            "quotas",
            "controller",
            "traffic",
            "many_to_one",
            "server_volume",
            "artifacts",
        ),
        "tasks/net_many_to_one.yaml.j2",
    )


def _build_net_many_to_one_http_preset(
    clouds_yaml: Path,
    config: dict[str, object],
) -> tuple[dict[str, object], str]:
    rendered, task_path = _build_net_many_to_one_preset(clouds_yaml, config)
    traffic = _mapping_value(rendered, "traffic")
    rendered["description"] = "One volume-backed HTTP server, many clients, and one controller"
    traffic["mode"] = "http_volume"
    traffic["protocols"] = []
    return rendered, task_path


def _build_net_ring_preset(
    clouds_yaml: Path,
    config: dict[str, object],
) -> tuple[dict[str, object], str]:
    rendered = _build_base_args(clouds_yaml, config)
    cloud = _mapping_value(rendered, "cloud")
    network = _mapping_value(rendered, "network")
    image_name = _pick_custom_image(clouds_yaml, "ubuntu-netbench")
    flavor_name = _pick_preferred_flavor(clouds_yaml, ("m1.netbench", "m1.small"))
    rendered["title"] = "Ring east-west network benchmark"
    rendered["description"] = "One controller plus many participants communicating in a bounded ring topology"
    rendered["cloud"] = {
        "controller_image_name": image_name,
        "controller_flavor_name": flavor_name,
        "participant_image_name": image_name,
        "participant_flavor_name": flavor_name,
        "external_network_name": cloud["external_network_name"],
        "external_network_id": cloud["external_network_id"],
    }
    network["start_cidr"] = "10.79.0.0/22"
    rendered["controller"] = {
        "ssh_user": "ubuntu",
        "ssh_connect_timeout_seconds": 300,
        "command_timeout_seconds": 1800,
        "boot_concurrency": 4,
    }
    rendered["traffic"] = {
        "protocols": ["tcp", "udp"],
        "duration_seconds": 20,
        "ramp_time_seconds": 5,
        "base_port": 5201,
        "parallel_streams": [4],
        "udp_target_mbps": [300],
    }
    rendered["ring"] = {
        "participant_count": 8,
        "neighbors_per_vm": 1,
        "bidirectional": True,
    }
    rendered["artifacts"] = {"root_dir": "artifacts"}
    return (
        _select_sections(
            rendered,
            "title",
            "description",
            "cloud",
            "network",
            "boot_volume",
            "users",
            "quotas",
            "controller",
            "traffic",
            "ring",
            "artifacts",
        ),
        "tasks/net_ring.yaml.j2",
    )


def _build_mixed_pressure_preset(
    clouds_yaml: Path,
    config: dict[str, object],
) -> tuple[dict[str, object], str]:
    rendered = _build_base_args(clouds_yaml, config)
    cloud = _mapping_value(rendered, "cloud")
    network = _mapping_value(rendered, "network")
    net_image = _pick_custom_image(clouds_yaml, "ubuntu-netbench")
    fio_worker_image = _pick_custom_image(clouds_yaml, "ubuntu-fio")
    churn_image = _pick_custom_image(clouds_yaml, "ubuntu-stress-ng")
    controller_flavor = _pick_preferred_flavor(clouds_yaml, ("m1.netbench", "m1.small"))
    fixed_group_flavor = _pick_preferred_flavor(clouds_yaml, ("m1.netbench", "m1.small"))
    churn_flavor = _pick_preferred_flavor(clouds_yaml, ("m1.netbench", "m1.small"))
    rendered["title"] = "Mixed cloud pressure benchmark"
    rendered["description"] = "Concurrent stress-ng churn, fio throughput, and network traffic over one tenant overlay network"
    rendered["cloud"] = {
        "controller_image_name": fio_worker_image,
        "net_image_name": net_image,
        "fio_worker_image_name": fio_worker_image,
        "churn_image_name": churn_image,
        "controller_flavor_name": controller_flavor,
        "fixed_group_flavor_name": fixed_group_flavor,
        "churn_flavor_name": churn_flavor,
        "external_network_name": cloud["external_network_name"],
        "external_network_id": cloud["external_network_id"],
    }
    network["start_cidr"] = "10.80.0.0/20"
    rendered["controller"] = {
        "ssh_user": "ubuntu",
        "ssh_connect_timeout_seconds": 300,
        "command_timeout_seconds": 1800,
        "boot_concurrency": 4,
        "volume_concurrency": 4,
    }
    rendered["mixed"] = {
        "duration_seconds": 25,
        "subbenchmark_failure_mode": "fail",
    }
    rendered["churn"] = {
        "max_active_vms": 1,
        "baseline_launches_per_minute": 2,
        "burst_windows": [{"start_second": 10, "end_second": 20, "launch_rate_multiplier": 2.0}],
        "launch_tick_seconds": 1,
        "timeout_seconds": 3600,
        "timeout_mode": "fail",
        "workload_params": {
            "duration_seconds": 15,
            "cpu_workers": 1,
            "vm_workers": 1,
            "vm_bytes": "256M",
        },
    }
    rendered["fio"] = {
        "volume_size_gib": 2,
        "volume_type": None,
        "client_counts": [1],
        "volumes_per_client": [1],
        "profile_names": ["mixed-workload"],
        "numjobs": [1],
        "iodepths": [1],
        "runtime_seconds": 15,
        "ramp_time_seconds": 3,
        "fio_port": 8765,
        "ioengine": "io_uring",
    }
    rendered["many_to_one"] = {
        "client_count": 1,
        "mode": "iperf3",
        "protocols": ["tcp"],
        "duration_seconds": 10,
        "ramp_time_seconds": 2,
        "base_port": 5201,
        "flow_direction": "server_to_client",
        "parallel_streams": [1],
        "udp_target_mbps": [500],
        "server_volume_size_gib": 2,
        "server_volume_type": None,
        "http_file_count": 4,
        "http_file_size_mib": 128,
    }
    rendered["ring"] = {
        "participant_count": 2,
        "protocols": ["tcp"],
        "duration_seconds": 10,
        "ramp_time_seconds": 2,
        "base_port": 6201,
        "neighbors_per_vm": 1,
        "bidirectional": True,
        "parallel_streams": [1],
        "udp_target_mbps": [300],
    }
    rendered["artifacts"] = {"root_dir": "artifacts"}
    return (
        _select_sections(
            rendered,
            "title",
            "description",
            "cloud",
            "network",
            "boot_volume",
            "users",
            "quotas",
            "storage",
            "controller",
            "mixed",
            "churn",
            "fio",
            "many_to_one",
            "ring",
            "artifacts",
        ),
        "tasks/mixed_pressure.yaml.j2",
    )


def _build_failure_storm_preset(
    clouds_yaml: Path,
    config: dict[str, object],
) -> tuple[dict[str, object], str]:
    rendered = _build_base_args(clouds_yaml, config)
    rendered["description"] = "Spiky autonomous runner churn with injected guest failures and hangs"
    rendered["scenario"] = {
        "family": "autonomous_vm",
        "name": "CIChurn.spiky_autonomous_vm",
        "timeout_seconds": 180,
        "timeout_mode": "fail",
        "console_log_length": 400,
        "allow_guest_errors": True,
        "allow_guest_timeouts": True,
    }
    rendered["schedule"] = {
        "duration_seconds": 180,
        "max_active_vms": 8,
        "baseline_launches_per_minute": 10,
        "launch_tick_seconds": 1,
        "burst_windows": [{"start_second": 45, "end_second": 90, "launch_rate_multiplier": 3.0}],
    }
    rendered["workload"] = {
        "profile": "synthetic_ci",
        "params": {},
        "mix": [
            {"profile": "synthetic_ci", "weight": 7, "params": {}},
            {"profile": "synthetic_ci", "weight": 2, "params": {"failure_mode": "fail_fast"}},
            {
                "profile": "synthetic_ci",
                "weight": 1,
                "params": {"failure_mode": "hang", "hang_seconds": 600},
            },
        ],
    }
    return (
        _select_sections(
            rendered,
            "title",
            "description",
            "scenario",
            "cloud",
            "network",
            "boot_volume",
            "users",
            "quotas",
            "storage",
            "workload",
            "schedule",
        ),
        "tasks/spiky_autonomous_vm.yaml.j2",
    )


def _build_quota_edge_preset(
    clouds_yaml: Path,
    config: dict[str, object],
) -> tuple[dict[str, object], str]:
    rendered = _build_base_args(clouds_yaml, config)
    workload = _mapping_value(rendered, "workload")
    rendered["description"] = "Autonomous runner churn until quota or scheduler failures accumulate"
    rendered["scenario"] = {
        "family": "autonomous_vm",
        "name": "CIChurn.quota_edge_autonomous_vm",
        "timeout_seconds": 3600,
        "timeout_mode": "fail",
        "console_log_length": 400,
        "allow_guest_errors": False,
        "allow_guest_timeouts": False,
    }
    workload["profile"] = "smoke"
    rendered["quota_edge"] = {
        "duration_seconds": 900,
        "launches_per_tick": 2,
        "launch_tick_seconds": 1,
        "max_consecutive_launch_failures": 10,
    }
    return (
        _select_sections(
            rendered,
            "title",
            "description",
            "scenario",
            "cloud",
            "network",
            "boot_volume",
            "users",
            "quotas",
            "storage",
            "workload",
            "quota_edge",
        ),
        "tasks/quota_edge_autonomous_vm.yaml.j2",
    )


def _build_tenant_churn_preset(
    clouds_yaml: Path,
    config: dict[str, object],
) -> tuple[dict[str, object], str]:
    rendered = _build_base_args(clouds_yaml, config)
    scenario = _mapping_value(rendered, "scenario")
    workload = _mapping_value(rendered, "workload")
    rendered["description"] = "Autonomous runner churn across short-lived tenants"
    scenario["waves"] = 1
    scenario["vm_count"] = 1
    scenario["task_concurrency"] = 1
    workload["profile"] = "smoke"
    rendered["tenant_churn"] = {
        "cycles": 10,
        "vms_per_cycle": 2,
        "task_concurrency": 2,
    }
    return (
        _select_sections(
            rendered,
            "title",
            "description",
            "scenario",
            "cloud",
            "network",
            "boot_volume",
            "users",
            "quotas",
            "storage",
            "workload",
            "tenant_churn",
        ),
        "tasks/tenant_churn_autonomous_vm.yaml.j2",
    )


def _write_adminrc(path: Path, admin_cloud: dict[str, object]) -> None:
    auth = _mapping_value(admin_cloud, "auth")
    auth_url = str(auth["auth_url"])
    lines = [
        "# Generated by rally_ci_churn.bootstrap.sunbeam",
        "unset OS_CLOUD",
        "export OS_AUTH_TYPE=password",
        "export OS_IDENTITY_API_VERSION=3",
        f"export OS_AUTH_URL={shlex.quote(auth_url)}",
        f"export OS_USERNAME={shlex.quote(str(auth['username']))}",
        f"export OS_PASSWORD={shlex.quote(str(auth['password']))}",
        f"export OS_PROJECT_NAME={shlex.quote(str(auth['project_name']))}",
        f"export OS_USER_DOMAIN_NAME={shlex.quote(str(auth['user_domain_name']))}",
        f"export OS_PROJECT_DOMAIN_NAME={shlex.quote(str(auth['project_domain_name']))}",
        "export OS_INTERFACE=public",
    ]
    if admin_cloud.get("region_name"):
        lines.append(f"export OS_REGION_NAME={shlex.quote(str(admin_cloud['region_name']))}")
    if admin_cloud.get("cacert"):
        lines.append(f"export OS_CACERT={shlex.quote(str(admin_cloud['cacert']))}")
    elif urlparse(auth_url).scheme.lower() == "https":
        lines.append("# HTTPS is enabled; system CA trust will be used.")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    path.chmod(stat.S_IRUSR | stat.S_IWUSR)


PRESET_BUILDERS = {
    "smoke": _build_smoke_preset,
    "steady": _build_steady_preset,
    "spiky": _build_spiky_preset,
    "stress-ng": _build_stress_ng_preset,
    "fio-distributed": _build_fio_distributed_preset,
    "mixed-pressure": _build_mixed_pressure_preset,
    "net-many-to-one": _build_net_many_to_one_preset,
    "net-many-to-one-http": _build_net_many_to_one_http_preset,
    "net-ring": _build_net_ring_preset,
    "failure-storm": _build_failure_storm_preset,
    "quota-edge": _build_quota_edge_preset,
    "tenant-churn": _build_tenant_churn_preset,
}

PRESET_DEFINITIONS = {
    "smoke": PresetDefinition(
        preset_id="smoke",
        scenario_name="CIChurn.boot_autonomous_vm",
        task_path="tasks/autonomous_vm_waves.yaml.j2",
        usage_tier="connectivity check",
        summary="Smallest autonomous VM run for bootstrap validation and low-resource CI bring-up.",
        operator_guidance=(
            "Use this to verify image selection, tenant networking, Swift artifact upload, and one VM lifecycle.",
            "Move to steady once smoke passes and you want the first meaningful autonomous VM baseline.",
        ),
        required_images=("ubuntu",),
        required_services=("Nova", "Neutron", "Swift"),
        first_knobs=("scenario.waves", "scenario.vm_count", "scenario.task_concurrency", "workload.profile"),
        focus_sections=("scenario", "workload"),
        recommended_next_preset="steady",
    ),
    "steady": PresetDefinition(
        preset_id="steady",
        scenario_name="CIChurn.boot_autonomous_vm",
        task_path="tasks/autonomous_vm_waves.yaml.j2",
        usage_tier="baseline",
        summary="Recommended first real autonomous VM baseline after smoke succeeds.",
        operator_guidance=(
            "Start here when you want repeatable low-resource churn that actually overlaps Rally iterations.",
            "Tune waves, vm_count, and task_concurrency from this preset before moving to spiky or quota-edge.",
        ),
        required_images=("ubuntu",),
        required_services=("Nova", "Neutron", "Swift"),
        first_knobs=("scenario.waves", "scenario.vm_count", "scenario.task_concurrency", "workload.profile"),
        focus_sections=("scenario", "workload"),
    ),
    "spiky": PresetDefinition(
        preset_id="spiky",
        scenario_name="CIChurn.spiky_autonomous_vm",
        task_path="tasks/spiky_autonomous_vm.yaml.j2",
        usage_tier="burst model",
        summary="Bursty autonomous VM churn for CI-like arrival spikes after the steady baseline is known.",
        operator_guidance=(
            "Use this when you care about burst handling instead of steady-state runner churn.",
            "Validate steady first so max_active_vms and launch rates are grounded in a known-good baseline.",
        ),
        required_images=("ubuntu",),
        required_services=("Nova", "Neutron", "Swift"),
        first_knobs=("schedule.max_active_vms", "schedule.baseline_launches_per_minute", "schedule.burst_windows"),
        focus_sections=("schedule",),
    ),
    "stress-ng": PresetDefinition(
        preset_id="stress-ng",
        scenario_name="CIChurn.boot_autonomous_vm",
        task_path="tasks/autonomous_vm_waves.yaml.j2",
        usage_tier="workload variant",
        summary="Autonomous VM baseline that replaces the guest payload with stress-ng pressure.",
        operator_guidance=(
            "Use this after smoke or steady when you want guest CPU and memory pressure, not just lifecycle churn.",
            "Keep the VM count small until the stress-ng image and flavor are validated on the target cloud.",
        ),
        required_images=("ubuntu-stress-ng",),
        required_services=("Nova", "Neutron", "Swift"),
        first_knobs=(
            "scenario.vm_count",
            "scenario.task_concurrency",
            "workload.params.duration_seconds",
            "workload.params.cpu_workers",
            "workload.params.vm_workers",
        ),
        focus_sections=("scenario", "workload"),
    ),
    "fio-distributed": PresetDefinition(
        preset_id="fio-distributed",
        scenario_name="CIChurn.fio_distributed",
        task_path="tasks/fio_distributed.yaml.j2",
        usage_tier="standalone block benchmark",
        summary="One controller plus fio workers with attached Cinder volumes for standalone block-pressure sizing.",
        operator_guidance=(
            "Use this to establish a block benchmark baseline before folding fio into mixed-pressure.",
            "Keep client_counts and volumes_per_client small first, then raise controller boot and volume concurrency if setup is the bottleneck.",
        ),
        required_images=("ubuntu-fio",),
        required_services=("Nova", "Neutron", "Cinder", "Floating IPs"),
        first_knobs=(
            "controller.boot_concurrency",
            "controller.volume_concurrency",
            "fio.client_counts",
            "fio.volumes_per_client",
            "fio.profile_names",
        ),
        focus_sections=("controller", "cinder", "fio"),
    ),
    "mixed-pressure": PresetDefinition(
        preset_id="mixed-pressure",
        scenario_name="CIChurn.mixed_pressure",
        task_path="tasks/mixed_pressure.yaml.j2",
        usage_tier="advanced composite",
        summary="Composite pressure preset that overlaps churn, block, and network load on one tenant network.",
        operator_guidance=(
            "Treat this as an advanced scenario after standalone spiky, fio-distributed, net-many-to-one, and net-ring baselines are known.",
            "Keep every embedded sub-benchmark intentionally small on low-resource clouds and scale one axis at a time.",
        ),
        required_images=("ubuntu-fio", "ubuntu-netbench", "ubuntu-stress-ng"),
        required_services=("Nova", "Neutron", "Swift", "Cinder", "Floating IPs"),
        first_knobs=(
            "controller.boot_concurrency",
            "controller.volume_concurrency",
            "mixed.duration_seconds",
            "churn.max_active_vms",
            "fio.client_counts",
            "many_to_one.client_count",
            "ring.participant_count",
        ),
        focus_sections=("controller", "mixed", "churn", "fio", "many_to_one", "ring"),
        section_notes=(
            ("many_to_one", "Embedded many-to-one shape inside mixed-pressure. Keep this below the standalone net-many-to-one sizing until that benchmark is calibrated."),
            ("ring", "Embedded ring shape inside mixed-pressure. Keep this below the standalone net-ring sizing until that benchmark is calibrated."),
            ("fio", "Embedded fio worker group inside mixed-pressure. Keep this below the standalone fio-distributed sizing until that benchmark is calibrated."),
        ),
    ),
    "net-many-to-one": PresetDefinition(
        preset_id="net-many-to-one",
        scenario_name="CIChurn.net_many_to_one",
        task_path="tasks/net_many_to_one.yaml.j2",
        usage_tier="standalone network benchmark",
        summary="One server, many clients, and one controller for aggregate overlay traffic sizing.",
        operator_guidance=(
            "Use this to size one-to-many overlay traffic before using the mixed-pressure scenario.",
            "Tune client_count first, then streams and protocols once the basic topology is healthy.",
        ),
        required_images=("ubuntu-netbench",),
        required_services=("Nova", "Neutron", "Floating IPs"),
        first_knobs=("controller.boot_concurrency", "many_to_one.client_count", "traffic.parallel_streams", "traffic.protocols"),
        focus_sections=("controller", "many_to_one", "traffic"),
        section_notes=(("server_volume", "Only used when traffic.mode is http_volume. Leave this alone for the default iperf3 preset."),),
    ),
    "net-many-to-one-http": PresetDefinition(
        preset_id="net-many-to-one-http",
        scenario_name="CIChurn.net_many_to_one",
        task_path="tasks/net_many_to_one.yaml.j2",
        usage_tier="standalone network benchmark",
        summary="HTTP download variant of the many-to-one topology with a volume-backed payload server.",
        operator_guidance=(
            "Use this when the benchmark needs a volume-backed HTTP payload instead of pure iperf3 flows.",
            "Tune the client count first, then adjust the backing file count and size only if the HTTP path is too small.",
        ),
        required_images=("ubuntu-netbench",),
        required_services=("Nova", "Neutron", "Cinder", "Floating IPs"),
        first_knobs=("controller.boot_concurrency", "many_to_one.client_count", "traffic.duration_seconds", "server_volume.file_count", "server_volume.file_size_mib"),
        focus_sections=("controller", "many_to_one", "traffic", "server_volume"),
    ),
    "net-ring": PresetDefinition(
        preset_id="net-ring",
        scenario_name="CIChurn.net_ring",
        task_path="tasks/net_ring.yaml.j2",
        usage_tier="standalone network benchmark",
        summary="Bounded east-west participant ring for overlay traffic sizing without a full mesh explosion.",
        operator_guidance=(
            "Use this to size east-west traffic after the cloud can already handle the many-to-one baseline.",
            "Tune participant_count first and keep neighbors_per_vm at one until the baseline is stable.",
        ),
        required_images=("ubuntu-netbench",),
        required_services=("Nova", "Neutron", "Floating IPs"),
        first_knobs=("controller.boot_concurrency", "ring.participant_count", "ring.neighbors_per_vm", "traffic.parallel_streams"),
        focus_sections=("controller", "ring", "traffic"),
    ),
    "failure-storm": PresetDefinition(
        preset_id="failure-storm",
        scenario_name="CIChurn.spiky_autonomous_vm",
        task_path="tasks/spiky_autonomous_vm.yaml.j2",
        usage_tier="failure injection",
        summary="Spiky autonomous VM churn with injected guest failures and hangs.",
        operator_guidance=(
            "Use this only after the normal spiky preset is healthy.",
            "The point is failure handling and cleanup behavior, not maximum launch rate.",
        ),
        required_images=("ubuntu",),
        required_services=("Nova", "Neutron", "Swift"),
        first_knobs=("schedule.max_active_vms", "schedule.baseline_launches_per_minute", "workload.mix", "scenario.allow_guest_errors"),
        focus_sections=("schedule", "workload"),
    ),
    "quota-edge": PresetDefinition(
        preset_id="quota-edge",
        scenario_name="CIChurn.quota_edge_autonomous_vm",
        task_path="tasks/quota_edge_autonomous_vm.yaml.j2",
        usage_tier="limit probe",
        summary="Launch-until-refusal preset for quota ceilings and scheduler saturation behavior.",
        operator_guidance=(
            "Use this after the steady baseline is healthy and cleanup behavior is trusted.",
            "Start with small launch batches and only raise aggressiveness once the refusal mode is understood.",
        ),
        required_images=("ubuntu",),
        required_services=("Nova", "Neutron", "Swift"),
        first_knobs=("quota_edge.launches_per_tick", "quota_edge.launch_tick_seconds", "quota_edge.max_consecutive_launch_failures"),
        focus_sections=("quota_edge",),
    ),
    "tenant-churn": PresetDefinition(
        preset_id="tenant-churn",
        scenario_name="tenant_churn_autonomous_vm",
        task_path="tasks/tenant_churn_autonomous_vm.yaml.j2",
        usage_tier="tenant lifecycle",
        summary="Short-lived project, network, user, and VM churn around autonomous runner batches.",
        operator_guidance=(
            "Use this when tenant and network lifecycle churn is the behavior under test, not just VM lifecycle overlap.",
            "Keep cycles and vms_per_cycle small first because each cycle tears down more control-plane state than steady churn.",
        ),
        required_images=("ubuntu",),
        required_services=("Nova", "Neutron", "Swift"),
        first_knobs=("tenant_churn.cycles", "tenant_churn.vms_per_cycle", "tenant_churn.task_concurrency"),
        focus_sections=("tenant_churn",),
    ),
}

if set(PRESET_DEFINITIONS) != SUPPORTED_PRESETS:
    raise RuntimeError("Preset definitions must stay in sync with SUPPORTED_PRESETS.")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate Sunbeam benchmark args and adminrc.")
    parser.add_argument("--clouds-yaml", required=True)
    parser.add_argument("--preset", default=DEFAULT_PRESET, choices=sorted(SUPPORTED_PRESETS))
    parser.add_argument("--output-args", required=True)
    parser.add_argument("--output-adminrc", required=True)
    return parser


def _apply_cloud_overrides(
    args_data: dict[str, object],
    image_override: str | None,
    flavor_override: str | None,
) -> None:
    """Replace all image/flavor fields in the cloud dict with overrides."""
    cloud = args_data.get("cloud")
    if not isinstance(cloud, dict):
        return
    if image_override:
        for key in list(cloud):
            if key == "image_name" or key.endswith("_image_name"):
                cloud[key] = image_override
    if flavor_override:
        for key in list(cloud):
            if key == "flavor_name" or key.endswith("_flavor_name"):
                cloud[key] = flavor_override


def build_preset(
    preset: str,
    clouds_yaml: Path,
    config: dict[str, object],
    *,
    image_override: str | None = None,
    flavor_override: str | None = None,
) -> tuple[dict[str, object], str]:
    definition = get_preset_definition(preset)
    with cloud_overrides(image=image_override, flavor=flavor_override):
        args, task_path = PRESET_BUILDERS[preset](clouds_yaml, config)
    if task_path != definition.task_path:
        raise RuntimeError(
            f"Preset {preset!r} declared task path {definition.task_path!r} but builder returned {task_path!r}"
        )
    # Catch hardcoded values not routed through _pick_custom_image / _pick_preferred_flavor.
    _apply_cloud_overrides(args, image_override, flavor_override)
    return args, task_path


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    clouds_yaml = Path(args.clouds_yaml).resolve()
    output_args = Path(args.output_args).resolve()
    output_adminrc = Path(args.output_adminrc).resolve()
    config = _normalize_clouds(clouds_yaml)
    with tempfile.TemporaryDirectory(prefix="rally-ci-clouds-") as temp_dir:
        normalized_clouds_yaml = Path(temp_dir) / "clouds.yaml"
        normalized_clouds_yaml.write_text(
            yaml.safe_dump(config, sort_keys=False),
            encoding="utf-8",
        )
        rendered_args, task_path = build_preset(args.preset, normalized_clouds_yaml, config)
        definition = get_preset_definition(args.preset)
        output_args.parent.mkdir(parents=True, exist_ok=True)
        output_adminrc.parent.mkdir(parents=True, exist_ok=True)
        output_args.write_text(render_preset_args(args.preset, rendered_args), encoding="utf-8")
        output_args.chmod(stat.S_IRUSR | stat.S_IWUSR)
        admin_cloud = _mapping_value(_mapping_value(config, "clouds"), "sunbeam-admin")
        _write_adminrc(output_adminrc, admin_cloud)
    print("Environment ready.\n")
    print(f"Generated:\n  {output_args}\n  {output_adminrc}\n")
    print(
        "Preset guidance:\n"
        f"  role: {definition.usage_tier}\n"
        f"  scenario: {definition.scenario_name}\n"
        f"  tune first: {', '.join(definition.first_knobs)}"
    )
    if definition.recommended_next_preset:
        print(f"  next: {definition.recommended_next_preset}")
    print("")
    print(
        "Next steps:\n"
        "  source .venv/bin/activate\n"
        f"  source {output_adminrc}\n"
        f"  rally task validate {task_path} "
        f"--task-args-file {output_args}\n"
        f"  rally task start {task_path} "
        f"--task-args-file {output_args}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

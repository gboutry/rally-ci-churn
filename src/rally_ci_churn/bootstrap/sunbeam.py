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
from pathlib import Path
from urllib.parse import urlparse

import yaml


DEFAULT_PRESET = "smoke"
SUPPORTED_PRESETS = {
    "smoke",
    "steady",
    "spiky",
    "stress-ng",
    "fio-distributed",
    "failure-storm",
    "quota-edge",
    "tenant-churn",
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
    sunbeam = config["clouds"]["sunbeam"]
    image_names = _run_openstack(clouds_yaml, "sunbeam-admin", "image", "list", "-f", "value", "-c", "Name").splitlines()
    flavor_names = _run_openstack(clouds_yaml, "sunbeam-admin", "flavor", "list", "-f", "value", "-c", "Name").splitlines()
    external_networks = _run_openstack(clouds_yaml, "sunbeam-admin", "network", "list", "--external", "-f", "value", "-c", "Name").splitlines()
    image_name = _pick_exact_or_prefix([name for name in image_names if name], ("ubuntu",), "ubuntu")
    flavor_name = _pick_exact_or_prefix([name for name in flavor_names if name], ("m1.tiny", "m1.small"), "m1.")
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
            "concurrency": 1,
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
        "storage": {
            "artifact_container": "rally-ci-churn",
            "artifact_ttl_seconds": 0,
            "swift_auth_url": sunbeam["auth"]["auth_url"],
            "swift_username": sunbeam["auth"]["username"],
            "swift_password": sunbeam["auth"]["password"],
            "swift_project_name": sunbeam["auth"]["project_name"],
            "swift_user_domain_name": sunbeam["auth"]["user_domain_name"],
            "swift_project_domain_name": sunbeam["auth"]["project_domain_name"],
            "swift_interface": "public",
            "swift_region_name": sunbeam.get("region_name", "") or "",
            "swift_cacert": swift_cacert,
            "swift_cacert_b64": swift_cacert_b64,
        },
        "workload": {
            "profile": "smoke",
            "params": {},
        },
        "image_prep": {
            "base_image_name": image_name,
            "build_image_name": f"{image_name}-rally-build",
            "build_image_flavor_name": flavor_name,
        },
    }


def _pick_custom_image(clouds_yaml: Path, desired_name: str) -> str:
    image_names = _run_openstack(
        clouds_yaml, "sunbeam-admin", "image", "list", "-f", "value", "-c", "Name"
    ).splitlines()
    if desired_name in image_names:
        return desired_name
    raise RuntimeError(
        f"Required image '{desired_name}' was not found. Build and upload it before using this preset."
    )


def _build_smoke_preset(clouds_yaml: Path, config: dict[str, object]) -> tuple[dict[str, object], str]:
    rendered = _build_base_args(clouds_yaml, config)
    rendered["description"] = "Autonomous CI-like runner churn smoke"
    rendered["workload"]["profile"] = "smoke"
    return rendered, "tasks/autonomous_vm_waves.yaml.j2"


def _build_steady_preset(clouds_yaml: Path, config: dict[str, object]) -> tuple[dict[str, object], str]:
    rendered = _build_base_args(clouds_yaml, config)
    rendered["description"] = "Autonomous CI-like runner churn with steady waves"
    rendered["scenario"]["waves"] = 5
    rendered["scenario"]["concurrency"] = 5
    rendered["workload"]["profile"] = "synthetic_ci"
    return rendered, "tasks/autonomous_vm_waves.yaml.j2"


def _build_spiky_preset(clouds_yaml: Path, config: dict[str, object]) -> tuple[dict[str, object], str]:
    rendered = _build_base_args(clouds_yaml, config)
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
    rendered["workload"]["profile"] = "synthetic_ci"
    return rendered, "tasks/spiky_autonomous_vm.yaml.j2"


def _build_stress_ng_preset(clouds_yaml: Path, config: dict[str, object]) -> tuple[dict[str, object], str]:
    rendered = _build_base_args(clouds_yaml, config)
    rendered["description"] = "Autonomous stress-ng runner churn on a pre-baked image"
    rendered["cloud"]["image_name"] = _pick_custom_image(clouds_yaml, "ubuntu-stress-ng")
    rendered["cloud"]["flavor_name"] = "m1.stress-ng"
    rendered["scenario"]["waves"] = 1
    rendered["scenario"]["concurrency"] = 3
    rendered["workload"] = {
        "profile": "stress_ng",
        "params": {
            "duration_seconds": 120,
            "cpu_workers": 2,
            "vm_workers": 1,
            "vm_bytes": "256M",
        },
    }
    return rendered, "tasks/autonomous_vm_waves.yaml.j2"


def _build_fio_distributed_preset(
    clouds_yaml: Path,
    config: dict[str, object],
) -> tuple[dict[str, object], str]:
    rendered = _build_base_args(clouds_yaml, config)
    rendered["title"] = "Distributed FIO benchmark"
    rendered["description"] = "Controller/worker fio benchmark with attached block devices"
    rendered["cloud"] = {
        "controller_image_name": _pick_custom_image(clouds_yaml, "ubuntu-fio"),
        "controller_flavor_name": "m1.small",
        "worker_image_name": "ubuntu-fio",
        "worker_flavor_name": "m1.small",
        "external_network_name": rendered["cloud"]["external_network_name"],
        "external_network_id": rendered["cloud"]["external_network_id"],
    }
    rendered["network"]["start_cidr"] = "10.77.0.0/22"
    rendered["controller"] = {
        "ssh_user": "ubuntu",
        "ssh_connect_timeout_seconds": 300,
        "command_timeout_seconds": 0,
    }
    rendered["cinder"] = {
        "volume_size_gib": 10,
        "volume_type": None,
    }
    rendered["fio"] = {
        "client_counts": [1, 2],
        "volumes_per_client": [1],
        "rw_modes": ["write", "read"],
        "block_sizes": ["1M"],
        "numjobs": [1, 2],
        "iodepths": [1, 32],
        "runtime_seconds": 30,
        "ramp_time_seconds": 5,
        "fio_port": 8765,
        "ioengine": "io_uring",
    }
    rendered["artifacts"] = {"root_dir": "artifacts"}
    rendered.pop("storage", None)
    rendered.pop("workload", None)
    rendered.pop("image_prep", None)
    return rendered, "tasks/fio_distributed.yaml.j2"


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
    return rendered, "tasks/spiky_autonomous_vm.yaml.j2"


def _build_quota_edge_preset(
    clouds_yaml: Path,
    config: dict[str, object],
) -> tuple[dict[str, object], str]:
    rendered = _build_base_args(clouds_yaml, config)
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
    rendered["workload"]["profile"] = "smoke"
    rendered["quota_edge"] = {
        "duration_seconds": 900,
        "launches_per_tick": 2,
        "launch_tick_seconds": 1,
        "max_consecutive_launch_failures": 10,
    }
    return rendered, "tasks/quota_edge_autonomous_vm.yaml.j2"


def _build_tenant_churn_preset(
    clouds_yaml: Path,
    config: dict[str, object],
) -> tuple[dict[str, object], str]:
    rendered = _build_base_args(clouds_yaml, config)
    rendered["description"] = "Autonomous runner churn across short-lived tenants"
    rendered["scenario"]["waves"] = 1
    rendered["scenario"]["concurrency"] = 1
    rendered["workload"]["profile"] = "smoke"
    rendered["tenant_churn"] = {
        "cycles": 10,
        "vms_per_cycle": 2,
    }
    return rendered, "tasks/tenant_churn_autonomous_vm.yaml.j2"


def _write_adminrc(path: Path, admin_cloud: dict[str, object]) -> None:
    auth = admin_cloud["auth"]
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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate Sunbeam benchmark args and adminrc.")
    parser.add_argument("--clouds-yaml", required=True)
    parser.add_argument("--preset", default=DEFAULT_PRESET, choices=sorted(SUPPORTED_PRESETS))
    parser.add_argument("--output-args", required=True)
    parser.add_argument("--output-adminrc", required=True)
    return parser


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
        preset_builders = {
            "smoke": _build_smoke_preset,
            "steady": _build_steady_preset,
            "spiky": _build_spiky_preset,
            "stress-ng": _build_stress_ng_preset,
            "fio-distributed": _build_fio_distributed_preset,
            "failure-storm": _build_failure_storm_preset,
            "quota-edge": _build_quota_edge_preset,
            "tenant-churn": _build_tenant_churn_preset,
        }
        if args.preset not in preset_builders:
            raise RuntimeError(f"Unsupported preset selector: {args.preset}")
        rendered_args, task_path = preset_builders[args.preset](normalized_clouds_yaml, config)
        output_args.parent.mkdir(parents=True, exist_ok=True)
        output_adminrc.parent.mkdir(parents=True, exist_ok=True)
        output_args.write_text(yaml.safe_dump(rendered_args, sort_keys=False), encoding="utf-8")
        output_args.chmod(stat.S_IRUSR | stat.S_IWUSR)
        _write_adminrc(output_adminrc, config["clouds"]["sunbeam-admin"])
    print("Environment ready.\n")
    print(f"Generated:\n  {output_args}\n  {output_adminrc}\n")
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

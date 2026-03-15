"""Verify capacity sweep generates correct args for every scenario."""

from __future__ import annotations

import json
import textwrap
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
import yaml

from rally_ci_churn.bootstrap import capacity_sweep, sunbeam


# ---------------------------------------------------------------------------
# Fake OpenStack responses
# ---------------------------------------------------------------------------

FAKE_IMAGES = [
    "ubuntu",
    "ubuntu-stress-ng",
    "ubuntu-fio",
    "ubuntu-netbench",
]

FAKE_FLAVORS = [
    "m1.tiny",
    "m1.small",
    "m1.benchmark",
    "m1.stress-ng",
    "m1.netbench",
]

FAKE_EXTERNAL_NETWORKS = ["external-network"]
FAKE_EXTERNAL_NETWORK_ID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"

FAKE_SUBNETS = json.dumps([
    {"ID": "sub-1", "Name": "gtestos-subnet", "Network": "net-1"},
])
FAKE_NETWORKS = json.dumps([
    {"ID": "net-1", "Name": "gtestos-network"},
])
FAKE_DNS = "['10.0.0.1']"

FAKE_FLAVOR_SHOW = json.dumps({
    "name": "m1.benchmark",
    "vcpus": 4,
    "ram": 8192,
    "disk": 40,
})


def _fake_run_openstack(_clouds_yaml: Path, cloud_name: str, *args: str) -> str:
    """Return canned responses for every openstack CLI call the sweep makes."""
    joined = " ".join(args)
    if "image" in joined and "list" in joined:
        return "\n".join(FAKE_IMAGES)
    if "flavor" in joined and "list" in joined:
        return "\n".join(FAKE_FLAVORS)
    if "flavor" in joined and "show" in joined:
        return FAKE_FLAVOR_SHOW
    if "network" in joined and "list" in joined and "--external" in joined:
        return "\n".join(FAKE_EXTERNAL_NETWORKS)
    if "network" in joined and "list" in joined:
        return FAKE_NETWORKS
    if "network" in joined and "show" in joined:
        return FAKE_EXTERNAL_NETWORK_ID
    if "subnet" in joined and "list" in joined:
        return FAKE_SUBNETS
    if "subnet" in joined and "show" in joined:
        return FAKE_DNS
    raise RuntimeError(f"Unmocked openstack call: cloud={cloud_name} args={args}")


@pytest.fixture()
def fake_clouds(tmp_path: Path) -> Path:
    """Write a minimal clouds.yaml and return its path."""
    clouds_yaml = tmp_path / "clouds.yaml"
    clouds_yaml.write_text(
        textwrap.dedent("""\
            clouds:
              sunbeam:
                auth:
                  auth_url: https://keystone.example.com:5000/v3
                  username: admin
                  password: secret
                  project_name: admin
                  user_domain_name: default
                  project_domain_name: default
                region_name: RegionOne
              sunbeam-admin:
                auth:
                  auth_url: https://keystone.example.com:5000/v3
                  username: admin
                  password: secret
                  project_name: admin
                  user_domain_name: default
                  project_domain_name: default
                region_name: RegionOne
        """),
        encoding="utf-8",
    )
    return clouds_yaml


# ---------------------------------------------------------------------------
# Expected cloud-dict field names per scenario
# ---------------------------------------------------------------------------

EXPECTED_IMAGE_FIELDS: dict[str, list[str]] = {
    "spiky": ["image_name"],
    "fio-distributed": ["controller_image_name", "worker_image_name"],
    "net-many-to-one": ["controller_image_name", "server_image_name", "client_image_name"],
    "net-ring": ["controller_image_name", "participant_image_name"],
    "mixed-pressure": ["controller_image_name", "net_image_name", "fio_worker_image_name", "churn_image_name"],
}

EXPECTED_FLAVOR_FIELDS: dict[str, list[str]] = {
    "spiky": ["flavor_name"],
    "fio-distributed": ["controller_flavor_name", "worker_flavor_name"],
    "net-many-to-one": ["controller_flavor_name", "server_flavor_name", "client_flavor_name"],
    "net-ring": ["controller_flavor_name", "participant_flavor_name"],
    "mixed-pressure": ["controller_flavor_name", "fixed_group_flavor_name", "churn_flavor_name"],
}

# Non-cloud keys that must never be touched by overrides
NETWORK_FIELDS = ["external_network_name", "external_network_id"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_args_for_scenario(
    fake_clouds: Path,
    scenario: str,
    overrides: dict[str, str] | None = None,
) -> dict[str, Any]:
    config = yaml.safe_load(fake_clouds.read_text(encoding="utf-8"))
    args, _task_path = capacity_sweep._build_base_args(
        fake_clouds, config, scenario, overrides=overrides,
    )
    return args


# ---------------------------------------------------------------------------
# Tests: default (no overrides) — correct images and flavors selected
# ---------------------------------------------------------------------------

class TestDefaultImageAndFlavor:
    """Without overrides, each scenario must use its own dedicated images."""

    @patch.object(sunbeam, "_run_openstack", side_effect=_fake_run_openstack)
    def test_spiky_defaults(self, _mock, fake_clouds: Path) -> None:
        args = _build_args_for_scenario(fake_clouds, "spiky")
        assert args["cloud"]["image_name"] == "ubuntu-stress-ng"
        # flavor preference: m1.stress-ng first
        assert args["cloud"]["flavor_name"] == "m1.stress-ng"

    @patch.object(sunbeam, "_run_openstack", side_effect=_fake_run_openstack)
    def test_fio_defaults(self, _mock, fake_clouds: Path) -> None:
        args = _build_args_for_scenario(fake_clouds, "fio-distributed")
        assert args["cloud"]["controller_image_name"] == "ubuntu-fio"
        assert args["cloud"]["worker_image_name"] == "ubuntu-fio"
        assert args["cloud"]["controller_flavor_name"] == "m1.small"
        assert args["cloud"]["worker_flavor_name"] == "m1.small"

    @patch.object(sunbeam, "_run_openstack", side_effect=_fake_run_openstack)
    def test_net_many_to_one_defaults(self, _mock, fake_clouds: Path) -> None:
        args = _build_args_for_scenario(fake_clouds, "net-many-to-one")
        for field in EXPECTED_IMAGE_FIELDS["net-many-to-one"]:
            assert args["cloud"][field] == "ubuntu-netbench", f"{field} wrong"
        for field in EXPECTED_FLAVOR_FIELDS["net-many-to-one"]:
            assert args["cloud"][field] == "m1.netbench", f"{field} wrong"

    @patch.object(sunbeam, "_run_openstack", side_effect=_fake_run_openstack)
    def test_net_ring_defaults(self, _mock, fake_clouds: Path) -> None:
        args = _build_args_for_scenario(fake_clouds, "net-ring")
        for field in EXPECTED_IMAGE_FIELDS["net-ring"]:
            assert args["cloud"][field] == "ubuntu-netbench", f"{field} wrong"
        for field in EXPECTED_FLAVOR_FIELDS["net-ring"]:
            assert args["cloud"][field] == "m1.netbench", f"{field} wrong"

    @patch.object(sunbeam, "_run_openstack", side_effect=_fake_run_openstack)
    def test_mixed_pressure_defaults(self, _mock, fake_clouds: Path) -> None:
        args = _build_args_for_scenario(fake_clouds, "mixed-pressure")
        assert args["cloud"]["controller_image_name"] == "ubuntu-fio"
        assert args["cloud"]["net_image_name"] == "ubuntu-netbench"
        assert args["cloud"]["fio_worker_image_name"] == "ubuntu-fio"
        assert args["cloud"]["churn_image_name"] == "ubuntu-stress-ng"
        for field in EXPECTED_FLAVOR_FIELDS["mixed-pressure"]:
            assert args["cloud"][field] == "m1.netbench", f"{field} wrong"


# ---------------------------------------------------------------------------
# Tests: with overrides — every image/flavor field must use the override
# ---------------------------------------------------------------------------

OVERRIDE = {"image_name": "ubuntu-mixed-benchmark", "flavor_name": "m1.benchmark"}


class TestOverrideImageAndFlavor:
    """With overrides, every scenario must use the override image and flavor."""

    @pytest.mark.parametrize("scenario", list(EXPECTED_IMAGE_FIELDS.keys()))
    @patch.object(sunbeam, "_run_openstack", side_effect=_fake_run_openstack)
    def test_all_image_fields_overridden(self, _mock, fake_clouds: Path, scenario: str) -> None:
        args = _build_args_for_scenario(fake_clouds, scenario, overrides=OVERRIDE)
        for field in EXPECTED_IMAGE_FIELDS[scenario]:
            assert args["cloud"][field] == "ubuntu-mixed-benchmark", (
                f"{scenario}: cloud.{field} = {args['cloud'][field]!r}, expected override"
            )

    @pytest.mark.parametrize("scenario", list(EXPECTED_FLAVOR_FIELDS.keys()))
    @patch.object(sunbeam, "_run_openstack", side_effect=_fake_run_openstack)
    def test_all_flavor_fields_overridden(self, _mock, fake_clouds: Path, scenario: str) -> None:
        args = _build_args_for_scenario(fake_clouds, scenario, overrides=OVERRIDE)
        for field in EXPECTED_FLAVOR_FIELDS[scenario]:
            assert args["cloud"][field] == "m1.benchmark", (
                f"{scenario}: cloud.{field} = {args['cloud'][field]!r}, expected override"
            )

    @pytest.mark.parametrize("scenario", list(EXPECTED_IMAGE_FIELDS.keys()))
    @patch.object(sunbeam, "_run_openstack", side_effect=_fake_run_openstack)
    def test_network_fields_not_touched(self, _mock, fake_clouds: Path, scenario: str) -> None:
        args = _build_args_for_scenario(fake_clouds, scenario, overrides=OVERRIDE)
        assert args["cloud"]["external_network_name"] == "external-network"
        assert args["cloud"]["external_network_id"] == FAKE_EXTERNAL_NETWORK_ID


# ---------------------------------------------------------------------------
# Tests: image-only and flavor-only overrides
# ---------------------------------------------------------------------------

class TestPartialOverrides:
    """Override only image or only flavor, not both."""

    @patch.object(sunbeam, "_run_openstack", side_effect=_fake_run_openstack)
    def test_image_only_override(self, _mock, fake_clouds: Path) -> None:
        overrides = {"image_name": "ubuntu-mixed-benchmark"}
        args = _build_args_for_scenario(fake_clouds, "fio-distributed", overrides=overrides)
        # Images should be overridden
        assert args["cloud"]["controller_image_name"] == "ubuntu-mixed-benchmark"
        assert args["cloud"]["worker_image_name"] == "ubuntu-mixed-benchmark"
        # Flavors should keep defaults
        assert args["cloud"]["controller_flavor_name"] == "m1.small"
        assert args["cloud"]["worker_flavor_name"] == "m1.small"

    @patch.object(sunbeam, "_run_openstack", side_effect=_fake_run_openstack)
    def test_flavor_only_override(self, _mock, fake_clouds: Path) -> None:
        overrides = {"flavor_name": "m1.benchmark"}
        args = _build_args_for_scenario(fake_clouds, "net-ring", overrides=overrides)
        # Images should keep defaults
        for field in EXPECTED_IMAGE_FIELDS["net-ring"]:
            assert args["cloud"][field] == "ubuntu-netbench", f"{field} wrong"
        # Flavors should be overridden
        for field in EXPECTED_FLAVOR_FIELDS["net-ring"]:
            assert args["cloud"][field] == "m1.benchmark", f"{field} wrong"


# ---------------------------------------------------------------------------
# Tests: correct task paths returned per scenario
# ---------------------------------------------------------------------------

EXPECTED_TASK_PATHS = {
    "spiky": "tasks/spiky_autonomous_vm.yaml.j2",
    "fio-distributed": "tasks/fio_distributed.yaml.j2",
    "net-many-to-one": "tasks/net_many_to_one.yaml.j2",
    "net-ring": "tasks/net_ring.yaml.j2",
    "mixed-pressure": "tasks/mixed_pressure.yaml.j2",
}


class TestTaskPaths:
    @pytest.mark.parametrize("scenario,expected_path", EXPECTED_TASK_PATHS.items())
    @patch.object(sunbeam, "_run_openstack", side_effect=_fake_run_openstack)
    def test_task_path(self, _mock, fake_clouds: Path, scenario: str, expected_path: str) -> None:
        config = yaml.safe_load(fake_clouds.read_text(encoding="utf-8"))
        _, task_path = capacity_sweep._build_base_args(fake_clouds, config, scenario)
        assert task_path == expected_path


# ---------------------------------------------------------------------------
# Tests: scenario-specific structural checks
# ---------------------------------------------------------------------------

class TestScenarioStructure:
    """Each scenario must have the right top-level keys after building."""

    @patch.object(sunbeam, "_run_openstack", side_effect=_fake_run_openstack)
    def test_spiky_has_schedule_and_workload(self, _mock, fake_clouds: Path) -> None:
        args = _build_args_for_scenario(fake_clouds, "spiky")
        assert "schedule" in args
        assert "workload" in args
        assert args["workload"]["profile"] == "stress_ng"

    @patch.object(sunbeam, "_run_openstack", side_effect=_fake_run_openstack)
    def test_fio_has_controller_and_fio(self, _mock, fake_clouds: Path) -> None:
        args = _build_args_for_scenario(fake_clouds, "fio-distributed")
        assert "controller" in args
        assert "fio" in args
        assert "cinder" in args
        assert "artifacts" in args

    @patch.object(sunbeam, "_run_openstack", side_effect=_fake_run_openstack)
    def test_net_many_to_one_has_traffic(self, _mock, fake_clouds: Path) -> None:
        args = _build_args_for_scenario(fake_clouds, "net-many-to-one")
        assert "traffic" in args
        assert "many_to_one" in args
        assert "controller" in args
        assert args["traffic"]["mode"] == "iperf3"

    @patch.object(sunbeam, "_run_openstack", side_effect=_fake_run_openstack)
    def test_net_ring_has_ring(self, _mock, fake_clouds: Path) -> None:
        args = _build_args_for_scenario(fake_clouds, "net-ring")
        assert "ring" in args
        assert "traffic" in args
        assert args["ring"]["bidirectional"] is True

    @patch.object(sunbeam, "_run_openstack", side_effect=_fake_run_openstack)
    def test_mixed_has_all_sub_benchmarks(self, _mock, fake_clouds: Path) -> None:
        args = _build_args_for_scenario(fake_clouds, "mixed-pressure")
        assert "mixed" in args
        assert "churn" in args
        assert "fio" in args
        assert "many_to_one" in args
        assert "ring" in args


# ---------------------------------------------------------------------------
# Tests: planning functions produce sane sizing
# ---------------------------------------------------------------------------

class TestPlanSizing:
    """Verify the planning functions calculate reasonable values."""

    @patch.object(sunbeam, "_run_openstack", side_effect=_fake_run_openstack)
    def test_plan_spiky_sizing(self, _mock, fake_clouds: Path) -> None:
        args = _build_args_for_scenario(fake_clouds, "spiky")
        cluster = capacity_sweep.DEFAULT_CLUSTER
        limits = capacity_sweep.DEFAULT_LIMITS
        sizing = capacity_sweep._plan_spiky(args, cluster, fake_clouds, 10, limits)
        assert sizing["planned_max_active_vms"] >= 1
        assert sizing["capacity"]["vm_capacity"] >= 1
        assert args["schedule"]["max_active_vms"] == sizing["planned_max_active_vms"]

    @patch.object(sunbeam, "_run_openstack", side_effect=_fake_run_openstack)
    def test_plan_fio_sizing(self, _mock, fake_clouds: Path) -> None:
        args = _build_args_for_scenario(fake_clouds, "fio-distributed")
        cluster = capacity_sweep.DEFAULT_CLUSTER
        limits = capacity_sweep.DEFAULT_LIMITS
        calibration = {"fio_worker_gbps": 2.0}
        sizing = capacity_sweep._plan_fio(args, cluster, fake_clouds, 25, limits, calibration)
        assert sizing["planned_workers"] >= 1
        assert sizing["target_ceph_gbps"] > 0
        assert args["fio"]["client_counts"][0] == sizing["planned_workers"]

    @patch.object(sunbeam, "_run_openstack", side_effect=_fake_run_openstack)
    def test_plan_many_to_one_sizing(self, _mock, fake_clouds: Path) -> None:
        args = _build_args_for_scenario(fake_clouds, "net-many-to-one")
        cluster = capacity_sweep.DEFAULT_CLUSTER
        limits = capacity_sweep.DEFAULT_LIMITS
        calibration = {"many_to_one_client_gbps": 10.0}
        sizing = capacity_sweep._plan_many_to_one(args, cluster, fake_clouds, 25, limits, calibration)
        assert sizing["planned_clients"] >= 1
        assert args["many_to_one"]["client_count"] == sizing["planned_clients"]

    @patch.object(sunbeam, "_run_openstack", side_effect=_fake_run_openstack)
    def test_plan_ring_sizing(self, _mock, fake_clouds: Path) -> None:
        args = _build_args_for_scenario(fake_clouds, "net-ring")
        cluster = capacity_sweep.DEFAULT_CLUSTER
        limits = capacity_sweep.DEFAULT_LIMITS
        calibration = {"ring_participant_gbps": 20.0}
        sizing = capacity_sweep._plan_ring(args, cluster, fake_clouds, 25, limits, calibration)
        assert sizing["planned_participants"] >= 2
        assert args["ring"]["participant_count"] == sizing["planned_participants"]


# ---------------------------------------------------------------------------
# Tests: generate-only end-to-end (writes files, no Rally calls)
# ---------------------------------------------------------------------------

class TestGenerateOnly:
    """Run the full main() in generate-only mode and verify outputs."""

    @patch.object(sunbeam, "_run_openstack", side_effect=_fake_run_openstack)
    def test_generate_only_with_overrides(self, _mock, fake_clouds: Path, tmp_path: Path) -> None:
        output_dir = tmp_path / "sweep-out"
        argv = [
            "--clouds-yaml", str(fake_clouds),
            "--generate-only",
            "--levels", "10",
            "--scenarios", "spiky,fio-distributed",
            "--image", "ubuntu-mixed-benchmark",
            "--flavor", "m1.benchmark",
            "--output-dir", str(output_dir),
        ]
        rc = capacity_sweep.main(argv)
        assert rc == 0

        manifest_path = output_dir / "manifest.json"
        assert manifest_path.exists()
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

        # Overrides recorded in manifest
        assert manifest["overrides"]["image_name"] == "ubuntu-mixed-benchmark"
        assert manifest["overrides"]["flavor_name"] == "m1.benchmark"

        # Check spiky level args
        spiky_args_path = output_dir / "runs" / "spiky" / "level-10" / "args.yaml"
        assert spiky_args_path.exists()
        spiky_args = yaml.safe_load(spiky_args_path.read_text(encoding="utf-8"))
        assert spiky_args["cloud"]["image_name"] == "ubuntu-mixed-benchmark"
        assert spiky_args["cloud"]["flavor_name"] == "m1.benchmark"

        # Check fio level args
        fio_args_path = output_dir / "runs" / "fio-distributed" / "level-10" / "args.yaml"
        assert fio_args_path.exists()
        fio_args = yaml.safe_load(fio_args_path.read_text(encoding="utf-8"))
        assert fio_args["cloud"]["controller_image_name"] == "ubuntu-mixed-benchmark"
        assert fio_args["cloud"]["worker_image_name"] == "ubuntu-mixed-benchmark"
        assert fio_args["cloud"]["controller_flavor_name"] == "m1.benchmark"
        assert fio_args["cloud"]["worker_flavor_name"] == "m1.benchmark"

    @patch.object(sunbeam, "_run_openstack", side_effect=_fake_run_openstack)
    def test_generate_only_without_overrides(self, _mock, fake_clouds: Path, tmp_path: Path) -> None:
        output_dir = tmp_path / "sweep-out-default"
        argv = [
            "--clouds-yaml", str(fake_clouds),
            "--generate-only",
            "--levels", "10",
            "--scenarios", "spiky",
            "--output-dir", str(output_dir),
        ]
        rc = capacity_sweep.main(argv)
        assert rc == 0

        spiky_args_path = output_dir / "runs" / "spiky" / "level-10" / "args.yaml"
        spiky_args = yaml.safe_load(spiky_args_path.read_text(encoding="utf-8"))
        assert spiky_args["cloud"]["image_name"] == "ubuntu-stress-ng"
        assert spiky_args["cloud"]["flavor_name"] == "m1.stress-ng"

    @patch.object(sunbeam, "_run_openstack", side_effect=_fake_run_openstack)
    def test_generate_only_all_scenarios_with_overrides(self, _mock, fake_clouds: Path, tmp_path: Path) -> None:
        """Run all five scenarios in generate-only mode with overrides."""
        output_dir = tmp_path / "sweep-out-all"
        argv = [
            "--clouds-yaml", str(fake_clouds),
            "--generate-only",
            "--levels", "10",
            "--image", "ubuntu-mixed-benchmark",
            "--flavor", "m1.benchmark",
            "--output-dir", str(output_dir),
        ]
        rc = capacity_sweep.main(argv)
        assert rc == 0

        manifest = json.loads((output_dir / "manifest.json").read_text(encoding="utf-8"))

        for scenario in capacity_sweep.DEFAULT_SCENARIOS:
            level_dir = output_dir / "runs" / scenario / "level-10"
            args_path = level_dir / "args.yaml"
            assert args_path.exists(), f"Missing args.yaml for {scenario}"
            args = yaml.safe_load(args_path.read_text(encoding="utf-8"))
            cloud = args["cloud"]

            # Every image field must be overridden
            for key in EXPECTED_IMAGE_FIELDS[scenario]:
                assert cloud[key] == "ubuntu-mixed-benchmark", (
                    f"{scenario}: cloud.{key} = {cloud[key]!r}"
                )

            # Every flavor field must be overridden
            for key in EXPECTED_FLAVOR_FIELDS[scenario]:
                assert cloud[key] == "m1.benchmark", (
                    f"{scenario}: cloud.{key} = {cloud[key]!r}"
                )

            # Network fields must be preserved
            assert cloud["external_network_name"] == "external-network"
            assert cloud["external_network_id"] == FAKE_EXTERNAL_NETWORK_ID

    @patch.object(sunbeam, "_run_openstack", side_effect=_fake_run_openstack)
    def test_calibration_args_also_use_overrides(self, _mock, fake_clouds: Path, tmp_path: Path) -> None:
        """Calibration runs must also honour the overrides."""
        output_dir = tmp_path / "sweep-cal"
        argv = [
            "--clouds-yaml", str(fake_clouds),
            "--generate-only",
            "--levels", "10",
            "--scenarios", "fio-distributed",
            "--image", "ubuntu-mixed-benchmark",
            "--flavor", "m1.benchmark",
            "--output-dir", str(output_dir),
        ]
        rc = capacity_sweep.main(argv)
        assert rc == 0

        cal_path = output_dir / "runs" / "fio-distributed" / "calibration" / "args.yaml"
        assert cal_path.exists()
        cal_args = yaml.safe_load(cal_path.read_text(encoding="utf-8"))
        assert cal_args["cloud"]["controller_image_name"] == "ubuntu-mixed-benchmark"
        assert cal_args["cloud"]["worker_image_name"] == "ubuntu-mixed-benchmark"
        assert cal_args["cloud"]["controller_flavor_name"] == "m1.benchmark"
        assert cal_args["cloud"]["worker_flavor_name"] == "m1.benchmark"

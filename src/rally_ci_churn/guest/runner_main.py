"""Autonomous guest runner for VM benchmark scenarios."""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import shutil
import socket
import ssl
import subprocess
import sys
import tarfile
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from pathlib import Path


RESULT_PREFIX = "RALLY_CI_RESULT="


SYNTHETIC_PROFILES = {
    "smoke": {
        "directories": 2,
        "files_per_directory": 8,
        "bytes_per_file": 16 * 1024,
        "hash_rounds": 1,
    },
    "synthetic_ci": {
        "directories": 12,
        "files_per_directory": 48,
        "bytes_per_file": 128 * 1024,
        "hash_rounds": 2,
    },
}


VALID_FAILURE_MODES = {"success", "fail_fast", "hang"}


def getenv(name: str, default: str | None = None) -> str:
    return os.environ.get(name, default or "") or ""


def build_ssl_context(config: dict[str, object]) -> ssl.SSLContext:
    cacert = str(config.get("swift_cacert") or getenv("SWIFT_CACERT") or getenv("OS_CACERT"))
    if cacert and Path(cacert).is_file():
        return ssl.create_default_context(cafile=cacert)
    return ssl.create_default_context()


def deterministic_bytes(seed: str, length: int) -> bytes:
    chunks = []
    block = 0
    while sum(len(chunk) for chunk in chunks) < length:
        chunks.append(hashlib.sha256(f"{seed}:{block}".encode("utf-8")).digest())
        block += 1
    return b"".join(chunks)[:length]


def build_synthetic_workspace(root: Path, profile: dict[str, int]) -> dict[str, int]:
    source_root = root / "source"
    build_root = root / "build"
    source_root.mkdir(parents=True, exist_ok=True)
    build_root.mkdir(parents=True, exist_ok=True)
    manifest = build_root / "manifest.txt"

    total_files = 0
    total_bytes = 0
    with manifest.open("w", encoding="utf-8") as manifest_stream:
        for directory_idx in range(profile["directories"]):
            directory = source_root / f"module-{directory_idx:02d}"
            directory.mkdir(parents=True, exist_ok=True)
            for file_idx in range(profile["files_per_directory"]):
                path = directory / f"file-{file_idx:04d}.dat"
                payload = deterministic_bytes(
                    f"{directory_idx}:{file_idx}",
                    profile["bytes_per_file"],
                )
                path.write_bytes(payload)
                manifest_stream.write(
                    f"{path.relative_to(root)} {hashlib.sha256(payload).hexdigest()}\n"
                )
                total_files += 1
                total_bytes += len(payload)
    return {"files": total_files, "source_bytes": total_bytes}


def run_hash_rounds(root: Path, rounds: int) -> dict[str, int]:
    manifest = (root / "build" / "manifest.txt").read_text(encoding="utf-8").splitlines()
    output = root / "build" / "hash-rounds.txt"
    source_root = root / "source"
    with output.open("w", encoding="utf-8") as stream:
        for round_idx in range(rounds):
            for line in manifest:
                relative_path, digest = line.split(" ", 1)
                payload = (root / relative_path).read_bytes()
                stream.write(
                    f"{round_idx} {relative_path} "
                    f"{hashlib.sha256(payload + digest.encode('utf-8') + str(round_idx).encode('utf-8')).hexdigest()}\n"
                )
            tree_digest = hashlib.sha256()
            for file_path in sorted(source_root.rglob("*.dat")):
                tree_digest.update(file_path.read_bytes())
            stream.write(f"tree {round_idx} {tree_digest.hexdigest()}\n")
    return {"hash_rounds": rounds}


def archive_workspace(root: Path) -> Path:
    artifact = root / "artifact.tar.gz"
    with tarfile.open(artifact, "w:gz") as tar:
        tar.add(root / "source", arcname="source")
        tar.add(root / "build", arcname="build")
    return artifact


def run_stress_ng(profile_config: dict[str, object]) -> dict[str, object]:
    if not shutil.which("stress-ng"):
        raise RuntimeError("stress-ng is not installed in the guest image")
    duration = int(profile_config.get("duration_seconds", 60))
    command = [
        "stress-ng",
        "--timeout",
        f"{duration}s",
    ]
    cpu_workers = int(profile_config.get("cpu_workers", 0))
    vm_workers = int(profile_config.get("vm_workers", 0))
    vm_bytes = str(profile_config.get("vm_bytes", "0"))
    if cpu_workers > 0:
        command.extend(["--cpu", str(cpu_workers)])
    if vm_workers > 0:
        command.extend(["--vm", str(vm_workers), "--vm-bytes", vm_bytes])
    subprocess.run(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    return {
        "duration_seconds": duration,
        "cpu_workers": cpu_workers,
        "vm_workers": vm_workers,
        "vm_bytes": vm_bytes,
    }


def run_libvirt_package_build(root: Path) -> dict[str, int]:
    commands = [
        ["bash", "-lc", "apt-get source libvirt"],
        ["bash", "-lc", "sudo apt-get build-dep -y libvirt"],
        ["bash", "-lc", "cd libvirt-* && dpkg-buildpackage -b -uc -us"],
    ]
    for command in commands:
        subprocess.run(
            command,
            cwd=root,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
    artifacts = list(root.glob("*.deb"))
    if not artifacts:
        raise RuntimeError("libvirt build completed without producing .deb artifacts")
    return {"deb_artifacts": len(artifacts)}


def normalize_auth_url(auth_url: str) -> str:
    auth_url = auth_url.rstrip("/")
    if auth_url.endswith("/v3"):
        return auth_url + "/auth/tokens"
    return auth_url + "/v3/auth/tokens"


def request_json(
    method: str,
    url: str,
    headers: dict[str, str],
    context: ssl.SSLContext,
    data: bytes | None = None,
) -> tuple[object, dict[str, object]]:
    request = urllib.request.Request(url=url, method=method, headers=headers, data=data)
    with urllib.request.urlopen(request, context=context) as response:
        body = response.read()
        return response, json.loads(body.decode("utf-8")) if body else {}


def authenticate(config: dict[str, object], context: ssl.SSLContext) -> tuple[str, str]:
    auth = {
        "auth": {
            "identity": {
                "methods": ["password"],
                "password": {
                    "user": {
                        "name": config["swift_username"],
                        "password": config["swift_password"],
                        "domain": {"name": config["swift_user_domain_name"]},
                    }
                },
            },
            "scope": {
                "project": {
                    "name": config["swift_project_name"],
                    "domain": {"name": config["swift_project_domain_name"]},
                }
            },
        }
    }
    response, body = request_json(
        "POST",
        normalize_auth_url(str(config["swift_auth_url"])),
        {"Content-Type": "application/json"},
        context,
        json.dumps(auth).encode("utf-8"),
    )
    token = response.headers.get("X-Subject-Token")
    if not token:
        raise RuntimeError("Keystone response did not include X-Subject-Token")
    for service in body.get("token", {}).get("catalog", []):
        if service.get("type") != "object-store":
            continue
        for endpoint in service.get("endpoints", []):
            if endpoint.get("interface") != config.get("swift_interface", "public"):
                continue
            region_name = config.get("swift_region_name", "")
            if region_name and endpoint.get("region") != region_name:
                continue
            return token, endpoint["url"].rstrip("/")
    raise RuntimeError("Unable to find a Swift endpoint in Keystone catalog")


def swift_request(
    method: str,
    url: str,
    token: str,
    context: ssl.SSLContext,
    data: bytes | None = None,
    extra_headers: dict[str, str] | None = None,
) -> tuple[int, bytes]:
    headers = {"X-Auth-Token": token}
    if extra_headers:
        headers.update(extra_headers)
    request = urllib.request.Request(url=url, method=method, headers=headers, data=data)
    try:
        with urllib.request.urlopen(request, context=context) as response:
            return response.getcode(), response.read()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read()


def ensure_container(config: dict[str, object], token: str, endpoint: str, context: ssl.SSLContext) -> None:
    container_url = endpoint + "/" + urllib.parse.quote(str(config["artifact_container"]), safe="")
    status, body = swift_request("PUT", container_url, token, context)
    if status not in (201, 202):
        raise RuntimeError(f"Unable to create Swift container: {status} {body!r}")


def upload_object(
    config: dict[str, object],
    token: str,
    endpoint: str,
    path: Path,
    context: ssl.SSLContext,
    object_name: str | None = None,
) -> dict[str, object]:
    if not object_name:
        object_name = "-".join(
            [
                "runner",
                str(config.get("wave", 0)),
                socket.gethostname(),
                str(int(time.time())),
                uuid.uuid4().hex[:8],
                path.name,
            ]
        )
    object_url = endpoint + "/" + "/".join(
        [
            urllib.parse.quote(str(config["artifact_container"]), safe=""),
            urllib.parse.quote(object_name, safe=""),
        ]
    )
    payload = path.read_bytes()
    headers = {
        "Content-Type": "application/octet-stream",
        "Content-Length": str(len(payload)),
    }
    ttl_seconds = int(config.get("artifact_ttl_seconds", 0))
    if ttl_seconds > 0:
        headers["X-Delete-After"] = str(ttl_seconds)
    start = time.perf_counter()
    status, body = swift_request("PUT", object_url, token, context, data=payload, extra_headers=headers)
    duration = time.perf_counter() - start
    if status not in (201, 202):
        raise RuntimeError(f"Swift upload failed with status {status}: {body!r}")
    return {
        "object_name": object_name,
        "artifact_bytes": len(payload),
        "upload_seconds": round(duration, 3),
        "upload_mib_per_second": round(len(payload) / 1048576 / max(duration, 1e-9), 3),
    }


def upload_result(
    config: dict[str, object],
    token: str,
    endpoint: str,
    result: dict[str, object],
    context: ssl.SSLContext,
) -> dict[str, object]:
    with tempfile.NamedTemporaryFile(prefix="rally-ci-result-", suffix=".json", delete=False) as handle:
        temp_path = Path(handle.name)
        handle.write(json.dumps(result, sort_keys=True).encode("utf-8"))
    try:
        uploaded = upload_object(
            config,
            token,
            endpoint,
            temp_path,
            context,
            object_name=str(config.get("result_object_name") or ""),
        )
    finally:
        temp_path.unlink(missing_ok=True)
    return uploaded


def maybe_apply_failure_mode(
    config: dict[str, object],
    stages: list[dict[str, object]],
) -> None:
    workload_params = dict(config.get("workload_params", {}))
    failure_mode = str(workload_params.get("failure_mode", "success") or "success")
    if failure_mode not in VALID_FAILURE_MODES:
        raise RuntimeError(f"Unknown failure_mode: {failure_mode}")
    if failure_mode == "success":
        return
    with stage("fault_injection", stages) as stage_data:
        stage_data["failure_mode"] = failure_mode
        if failure_mode == "fail_fast":
            raise RuntimeError("Injected fail_fast fault")
        hang_seconds = int(workload_params.get("hang_seconds", 7200))
        stage_data["hang_seconds"] = hang_seconds
        time.sleep(hang_seconds)


@contextlib.contextmanager
def stage(name: str, stages: list[dict[str, object]]):
    start = time.perf_counter()
    stage_data: dict[str, object] = {"stage": name}
    try:
        yield stage_data
    finally:
        stage_data["seconds"] = round(time.perf_counter() - start, 3)
        stages.append(stage_data)


def build_failure_result(
    config: dict[str, object], start_time: float, stages: list[dict[str, object]], error: str
) -> dict[str, object]:
    return {
        "schema_version": 1,
        "scenario_family": "autonomous_vm",
        "scenario_name": str(config.get("scenario_name", "")),
        "status": "error",
        "timeout": False,
        "wave": config.get("wave", 0),
        "iteration": config.get("iteration", 0),
        "hostname": socket.gethostname(),
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(start_time)),
        "finished_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "duration_seconds": round(time.time() - start_time, 3),
        "artifact_refs": [],
        "metrics": {},
        "diagnostics": {"error": error},
        "stages": stages,
    }


def load_config(argv: list[str]) -> dict[str, object]:
    if len(argv) > 1:
        return json.loads(Path(argv[1]).read_text(encoding="utf-8"))
    workload_params = getenv("WORKLOAD_PARAMETERS_JSON", "{}")
    return {
        "scenario_name": getenv("SCENARIO_NAME", "CIChurn.boot_autonomous_vm"),
        "wave": int(getenv("CURRENT_WAVE", "0")),
        "iteration": int(getenv("CURRENT_ITERATION", "0")),
        "workload_profile": getenv("WORKLOAD_PROFILE", "smoke"),
        "workload_params": json.loads(workload_params),
        "artifact_container": getenv("ARTIFACT_CONTAINER", "rally-ci-churn"),
        "artifact_ttl_seconds": int(getenv("ARTIFACT_TTL_SECONDS", "0")),
        "swift_auth_url": getenv("SWIFT_AUTH_URL"),
        "swift_username": getenv("SWIFT_USERNAME"),
        "swift_password": getenv("SWIFT_PASSWORD"),
        "swift_project_name": getenv("SWIFT_PROJECT_NAME"),
        "swift_user_domain_name": getenv("SWIFT_USER_DOMAIN_NAME", "Default"),
        "swift_project_domain_name": getenv("SWIFT_PROJECT_DOMAIN_NAME", "Default"),
        "swift_interface": getenv("SWIFT_INTERFACE", "public"),
        "swift_region_name": getenv("SWIFT_REGION_NAME", ""),
        "swift_cacert": getenv("SWIFT_CACERT", ""),
        "result_object_name": getenv("RESULT_OBJECT_NAME", ""),
    }


def main(argv: list[str] | None = None) -> int:
    argv = argv or sys.argv
    config = load_config(argv)
    start_time = time.time()
    stages: list[dict[str, object]] = []
    artifact_refs: list[dict[str, object]] = []
    metrics: dict[str, object] = {}
    token = ""
    endpoint = ""
    context: ssl.SSLContext | None = None
    try:
        workload_profile = str(config.get("workload_profile", "smoke"))
        with tempfile.TemporaryDirectory(prefix="rally-ci-churn-") as tmpdir:
            root = Path(tmpdir)
            artifact_path: Path
            if workload_profile == "package_build_libvirt":
                with stage("prepare_workspace", stages) as stage_data:
                    stage_data["workspace"] = tmpdir
                with stage("build", stages) as stage_data:
                    stage_data.update(run_libvirt_package_build(root))
                artifacts = sorted(root.glob("*.deb"))
                if not artifacts:
                    raise RuntimeError("No .deb artifacts were produced")
                artifact_path = artifacts[0]
            elif workload_profile == "stress_ng":
                with stage("prepare_workspace", stages) as stage_data:
                    stage_data["workspace"] = tmpdir
                with stage("build", stages) as stage_data:
                    stage_data.update(run_stress_ng(dict(config.get("workload_params", {}))))
                artifact_path = root / "stress-ng.txt"
                artifact_path.write_text(json.dumps(stage_data, sort_keys=True) + "\n", encoding="utf-8")
            else:
                profile = SYNTHETIC_PROFILES.get(workload_profile)
                if profile is None:
                    raise RuntimeError(f"Unknown workload profile: {workload_profile}")
                with stage("prepare_workspace", stages) as stage_data:
                    stage_data["workspace"] = tmpdir
                with stage("fetch", stages) as stage_data:
                    stage_data.update(build_synthetic_workspace(root, profile))
                with stage("build", stages) as stage_data:
                    stage_data.update(run_hash_rounds(root, profile["hash_rounds"]))
                with stage("archive", stages) as stage_data:
                    artifact_path = archive_workspace(root)
                    stage_data["artifact_bytes"] = artifact_path.stat().st_size

            maybe_apply_failure_mode(config, stages)

            context = build_ssl_context(config)
            with stage("auth", stages):
                token, endpoint = authenticate(config, context)
            with stage("upload", stages) as stage_data:
                ensure_container(config, token, endpoint, context)
                uploaded = upload_object(config, token, endpoint, artifact_path, context)
                stage_data.update(uploaded)
                artifact_refs.append(uploaded)
                metrics.update(uploaded)

        result = {
            "schema_version": 1,
            "scenario_family": "autonomous_vm",
            "scenario_name": str(config.get("scenario_name", "")),
            "status": "success",
            "timeout": False,
            "wave": config.get("wave", 0),
            "iteration": config.get("iteration", 0),
            "hostname": socket.gethostname(),
            "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(start_time)),
            "finished_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "duration_seconds": round(time.time() - start_time, 3),
            "artifact_refs": artifact_refs,
            "metrics": metrics,
            "diagnostics": {},
            "stages": stages,
        }
    except Exception as exc:  # noqa: BLE001
        result = build_failure_result(config, start_time, stages, str(exc))
    if context and token and endpoint and config.get("result_object_name"):
        try:
            result_upload = upload_result(config, token, endpoint, result, context)
            result["diagnostics"]["result_object_name"] = result_upload["object_name"]
        except Exception as exc:  # noqa: BLE001
            diagnostics = result.setdefault("diagnostics", {})
            if isinstance(diagnostics, dict):
                diagnostics["result_upload_error"] = str(exc)
    print(RESULT_PREFIX + json.dumps(result, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

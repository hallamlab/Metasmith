import subprocess
import shutil
import sys
import os
from pathlib import Path

from ..logging import Log
from .._build_hash import write_build_hash

REPO_ROOT = Path(__file__).resolve().parents[3]


def get_full_version() -> str:
    semver = (REPO_ROOT / "src/metasmith/version.txt").read_text().strip()
    bh_path = REPO_ROOT / "src/metasmith/build_hash.txt"
    bh = bh_path.read_text().strip() if bh_path.exists() else ""
    return f"{semver}+{bh}" if bh else semver


def get_git_version() -> str:
    return get_full_version()


def get_docker_tag(version: str|None = None) -> str:
    if version is None:
        version = get_full_version()
    return f"quay.io/hallamlab/metasmith:{version.replace('+', '-')}"


def image_exists(tag: str) -> bool:
    result = subprocess.run(
        ["docker", "image", "inspect", tag],
        capture_output=True, timeout=10,
    )
    return result.returncode == 0


def build_pip_package(version: str|None = None) -> Path:
    version_file = REPO_ROOT / "src/metasmith/version.txt"
    original_version = version_file.read_text()

    dist_dir = REPO_ROOT / "dist"
    build_dir = REPO_ROOT / "build"

    try:
        if version is not None:
            semver = version.split("+", 1)[0]
            version_file.write_text(semver)

        if build_dir.exists():
            shutil.rmtree(build_dir)
        if dist_dir.exists():
            shutil.rmtree(dist_dir)

        write_build_hash(REPO_ROOT / "src/metasmith")

        Log.Info(f"building pip package")
        result = subprocess.run(
            [sys.executable, "-m", "build"],
            cwd=REPO_ROOT,
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(f"pip build failed:\n{result.stderr}")
    finally:
        version_file.write_text(original_version)

    return dist_dir


def ensure_lib_prerequisites() -> Path:
    lib_dir = REPO_ROOT / "lib"
    lib_dir.mkdir(exist_ok=True)

    tini_path = lib_dir / "tini"
    if not tini_path.exists():
        tini_version = "v0.19.0"
        Log.Info(f"downloading tini {tini_version}")
        subprocess.run(
            ["wget", "-q", f"https://github.com/krallin/tini/releases/download/{tini_version}/tini",
             "-O", str(tini_path)],
            check=True,
        )
        os.chmod(tini_path, 0o755)

    nxf_version = None
    base_yml = REPO_ROOT / "envs/base.yml"
    if base_yml.exists():
        for line in base_yml.read_text().splitlines():
            line = line.strip()
            if "nextflow" in line and "=" in line:
                nxf_version = line.split("=")[-1].strip()
                break

    nxf_path = lib_dir / "nextflow"
    if not nxf_path.exists() and nxf_version:
        Log.Info(f"downloading nextflow {nxf_version}")
        subprocess.run(
            ["wget", "-q",
             f"https://github.com/nextflow-io/nextflow/releases/download/v{nxf_version}/nextflow",
             "-O", str(nxf_path)],
            check=True,
        )
        os.chmod(nxf_path, 0o755)

    globus_dir = lib_dir / "globusconnectpersonal-latest"
    if not globus_dir.exists():
        globus_dir.mkdir()
        (globus_dir / "README").write_text("stub for testing")

    relay_base = REPO_ROOT / "src/bash_relay/target"
    for platform in [
        "x86_64-unknown-linux-musl",
        "aarch64-unknown-linux-musl",
        "x86_64-apple-darwin",
        "aarch64-apple-darwin",
    ]:
        relay_dir = relay_base / platform / "release"
        relay_dir.mkdir(parents=True, exist_ok=True)
        relay_bin = relay_dir / "msm_relay"
        if not relay_bin.exists():
            relay_bin.write_text("#!/bin/sh\necho 'stub relay'\n")
            os.chmod(relay_bin, 0o755)

    return lib_dir


def build_docker_image(tag: str|None = None, version: str|None = None) -> str:
    write_build_hash(REPO_ROOT / "src/metasmith")
    if version is None:
        version = get_full_version()
    if tag is None:
        tag = get_docker_tag(version)

    if image_exists(tag):
        Log.Info(f"image [{tag}] already exists, skipping build")
        return tag

    Log.Info(f"building Docker image [{tag}]")

    ensure_lib_prerequisites()
    build_pip_package(version)

    result = subprocess.run(
        [
            "docker", "build",
            # The build context is the repo root; the Dockerfile is not.
            "-f", str(REPO_ROOT / "docker/metasmith/Dockerfile"),
            "--build-arg", "CONDA_ENV=metasmith_env",
            "--build-arg", "PACKAGE=metasmith",
            "--build-arg", f"VERSION={version}",
            "--network=host",
            "-t", tag,
            ".",
        ],
        cwd=REPO_ROOT,
        capture_output=True, text=True,
        # A cold conda solve is the slow part; 10 minutes was not enough for it
        # on a busy machine, and a killed build looks exactly like a broken one.
        timeout=3600,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Docker build failed:\n{result.stderr}")

    Log.Info(f"built [{tag}]")
    return tag

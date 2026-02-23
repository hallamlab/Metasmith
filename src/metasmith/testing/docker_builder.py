"""Docker image build infrastructure with git-hash versioning.

Provides utilities to build and cache Docker images for integration testing,
using PEP 440 local version format (e.g. 0.15.1+abc1234).
"""

import subprocess
import shutil
import sys
import os
from pathlib import Path

from ..logging import Log

REPO_ROOT = Path(__file__).resolve().parents[3]  # src/metasmith/testing -> repo root


def get_git_version() -> str:
    """Get package version with git short hash appended.

    Returns:
        Version string like "0.15.1+abc1234"
    """
    base = (REPO_ROOT / "src/metasmith/version.txt").read_text().strip()
    result = subprocess.run(
        ["git", "rev-parse", "--short", "HEAD"],
        capture_output=True, text=True, cwd=REPO_ROOT,
    )
    git_hash = result.stdout.strip()
    if not git_hash:
        return base
    return f"{base}+{git_hash}"


def get_docker_tag(version: str|None = None) -> str:
    """Get the full Docker image tag for testing.

    Args:
        version: Override version string. Uses get_git_version() if None.

    Returns:
        Full image tag like "quay.io/hallamlab/metasmith:0.15.1+abc1234"
    """
    if version is None:
        version = get_git_version()
    # Docker tags cannot contain '+', replace with '-'
    return f"quay.io/hallamlab/metasmith:{version.replace('+', '-')}"


def image_exists(tag: str) -> bool:
    """Check if a Docker image exists locally.

    Args:
        tag: Full image tag to check.

    Returns:
        True if image exists locally.
    """
    result = subprocess.run(
        ["docker", "image", "inspect", tag],
        capture_output=True, timeout=10,
    )
    return result.returncode == 0


def build_pip_package(version: str|None = None) -> Path:
    """Build pip package, optionally with a custom version.

    Temporarily writes version to version.txt, runs `python -m build`,
    then restores the original version.

    Args:
        version: Version to write. Uses current version.txt if None.

    Returns:
        Path to the dist/ directory containing built packages.
    """
    version_file = REPO_ROOT / "src/metasmith/version.txt"
    original_version = version_file.read_text()

    dist_dir = REPO_ROOT / "dist"
    build_dir = REPO_ROOT / "build"

    try:
        if version is not None:
            version_file.write_text(version)

        # Clean previous builds
        if build_dir.exists():
            shutil.rmtree(build_dir)
        if dist_dir.exists():
            shutil.rmtree(dist_dir)

        Log.Info(f"building pip package")
        result = subprocess.run(
            [sys.executable, "-m", "build"],
            cwd=REPO_ROOT,
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(f"pip build failed:\n{result.stderr}")
    finally:
        # Always restore original version
        version_file.write_text(original_version)

    return dist_dir


def ensure_lib_prerequisites() -> Path:
    """Download tini and nextflow, create stub globus dir.

    Downloads prerequisites matching dev.sh -bd logic into lib/.
    Creates stub relay binaries (sufficient for stub/mock runs).

    Returns:
        Path to the lib/ directory.
    """
    lib_dir = REPO_ROOT / "lib"
    lib_dir.mkdir(exist_ok=True)

    # tini
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

    # nextflow version from envs/base.yml
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

    # globus stub
    globus_dir = lib_dir / "globusconnectpersonal-latest"
    if not globus_dir.exists():
        globus_dir.mkdir()
        (globus_dir / "README").write_text("stub for testing")

    # relay binary stubs (sufficient for -stub runs)
    relay_base = REPO_ROOT / "main/relay_agent/target"
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
    """Build Docker image matching dev.sh -bd logic.

    Args:
        tag: Full image tag. Auto-generated from version if None.
        version: Version for pip package. Uses get_git_version() if None.

    Returns:
        The image tag that was built.
    """
    if version is None:
        version = get_git_version()
    if tag is None:
        tag = get_docker_tag(version)

    if image_exists(tag):
        Log.Info(f"image [{tag}] already exists, skipping build")
        return tag

    Log.Info(f"building Docker image [{tag}]")

    # Prerequisites
    ensure_lib_prerequisites()
    build_pip_package(version)

    # Build
    result = subprocess.run(
        [
            "docker", "build",
            "--build-arg", "CONDA_ENV=metasmith_env",
            "--build-arg", "PACKAGE=metasmith",
            "--build-arg", f"VERSION={version}",
            "--network=host",
            "-t", tag,
            ".",
        ],
        cwd=REPO_ROOT,
        capture_output=True, text=True,
        timeout=600,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Docker build failed:\n{result.stderr}")

    Log.Info(f"built [{tag}]")
    return tag

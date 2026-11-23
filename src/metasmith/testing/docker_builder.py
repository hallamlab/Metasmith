import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

from filelock import FileLock

from ..logging import Log
from .._build_hash import compute_build_hash, write_build_hash

REPO_ROOT = Path(__file__).resolve().parents[3]
PACKAGE = REPO_ROOT / "src/metasmith"
LIB = REPO_ROOT / "lib"
RELAY_TARGETS = [
    "x86_64-unknown-linux-musl",
    "aarch64-unknown-linux-musl",
    "x86_64-apple-darwin",
    "aarch64-apple-darwin",
]
RELAY_LABEL = "msm.relay"


def get_full_version() -> str:
    semver = (PACKAGE / "version.txt").read_text().strip()
    bh_path = PACKAGE / "build_hash.txt"
    bh = bh_path.read_text().strip() if bh_path.exists() else ""
    return f"{semver}+{bh}" if bh else semver


def get_docker_tag(version: str|None = None) -> str:
    if version is None:
        version = get_full_version()
    return f"quay.io/hallamlab/metasmith:{version.replace('+', '-')}"


def _relay_path(root: Path, target: str) -> Path:
    return root / "src/bash_relay/target" / target / "release/msm_relay"


def relay_kind() -> str:
    return "real" if all(_relay_path(REPO_ROOT, t).exists() for t in RELAY_TARGETS) else "stub"


def image_exists(tag: str, relay: str|None = None) -> bool:
    result = subprocess.run(
        ["docker", "image", "inspect", "--format", f'{{{{index .Config.Labels "{RELAY_LABEL}"}}}}', tag],
        capture_output=True, text=True, timeout=10,
    )
    if result.returncode != 0:
        return False
    return relay is None or result.stdout.strip() == relay


def _ensure_lib() -> None:
    LIB.mkdir(exist_ok=True)
    tini = LIB / "tini"
    if not tini.exists():
        Log.Info("downloading tini v0.19.0")
        subprocess.run(
            ["wget", "-q", "https://github.com/krallin/tini/releases/download/v0.19.0/tini", "-O", str(tini)],
            check=True,
        )
        os.chmod(tini, 0o755)
    globus = LIB / "globusconnectpersonal-latest"
    if not globus.exists():
        globus.mkdir()
        (globus / "README").write_text("stub for testing")


def stage_build_context(dest: Path) -> None:
    shutil.copytree(PACKAGE, dest / "src/metasmith", ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
    for name in ["setup.py", "pyproject.toml", "README.md", "LICENSE", ".dockerignore"]:
        shutil.copy2(REPO_ROOT / name, dest / name)
    shutil.copytree(REPO_ROOT / "envs/metasmith", dest / "envs/metasmith")
    shutil.copytree(LIB, dest / "lib", ignore=shutil.ignore_patterns(".*.lock"))
    for target in RELAY_TARGETS:
        staged = _relay_path(dest, target)
        staged.parent.mkdir(parents=True)
        real = _relay_path(REPO_ROOT, target)
        if real.exists():
            shutil.copy2(real, staged)
        else:
            staged.write_text("#!/bin/sh\necho 'stub relay'\n")
            os.chmod(staged, 0o755)


def build_docker_image() -> str:
    LIB.mkdir(exist_ok=True)
    with FileLock(LIB / ".docker_build.lock"):
        write_build_hash(PACKAGE)
        version = get_full_version()
        tag = get_docker_tag(version)
        relay = relay_kind()
        if image_exists(tag, relay):
            return tag

        Log.Info(f"building Docker image [{tag}] with {relay} relays")
        _ensure_lib()
        with tempfile.TemporaryDirectory(prefix="msm_image_") as tmp:
            stage = Path(tmp)
            stage_build_context(stage)
            staged_hash = compute_build_hash(stage / "src/metasmith")
            if staged_hash != version.rsplit("+", 1)[-1]:
                raise RuntimeError(f"staged package hashes to [{staged_hash}], checkout is [{version}]")

            result = subprocess.run([sys.executable, "-m", "build", "--wheel"], cwd=stage, capture_output=True, text=True)
            if result.returncode != 0:
                raise RuntimeError(f"pip build failed:\n{result.stderr}")

            result = subprocess.run(
                [
                    "docker", "build",
                    "-f", str(REPO_ROOT / "docker/metasmith/Dockerfile"),
                    "--build-arg", "CONDA_ENV=metasmith_env",
                    "--build-arg", "PACKAGE=metasmith",
                    "--build-arg", f"VERSION={version}",
                    "--label", f"{RELAY_LABEL}={relay}",
                    "--network=host",
                    "-t", tag,
                    ".",
                ],
                cwd=stage,
                capture_output=True, text=True,
                # A cold conda solve is the slow part; 10 minutes was not enough for it
                # on a busy machine, and a killed build looks exactly like a broken one.
                timeout=3600,
            )
        if result.returncode != 0:
            raise RuntimeError(f"Docker build failed:\n{result.stderr}")

        Log.Info(f"built [{tag}]")
        return tag

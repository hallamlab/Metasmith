from __future__ import annotations

import os
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path


class PreflightError(RuntimeError):
    pass
@dataclass(frozen=True)
class InstallContext:
    version: str
    project_root: Path
    image_tag: str
    sif_path: Path | None
    channel_dir: Path
    docs_dir: Path


def _read_version(project_root: Path) -> str:
    p = project_root / "src" / "metasmith" / "version.txt"
    if not p.exists():
        raise PreflightError(f"missing {p}; cannot determine target version")
    semver = p.read_text().strip()
    bh = project_root / "src" / "metasmith" / "build_hash.txt"
    if bh.exists():
        h = bh.read_text().strip()
        if h:
            return f"{semver}+{h}"
    return semver


def _docker_image_present(tag: str) -> bool:
    return subprocess.run(
        ["docker", "image", "inspect", tag],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    ).returncode == 0


def _channel_has_pkg(channel_dir: Path, version: str) -> bool:
    if not channel_dir.is_dir():
        return False
    pkgs = list(channel_dir.rglob(f"metasmith-{version}*.tar.bz2"))
    return bool(pkgs)


def _opencode_has_credentials() -> bool:
    if os.environ.get("OPENCODE_API_KEY"):
        return True
    auth_path = Path.home() / ".local" / "share" / "opencode" / "auth.json"
    if auth_path.is_file() and auth_path.stat().st_size > 2:
        return True
    for k in ("OPENROUTER_API_KEY", "ANTHROPIC_API_KEY", "OPENAI_API_KEY",
              "GROQ_API_KEY", "TOGETHER_API_KEY"):
        if os.environ.get(k):
            return True
    return False


def verify(
    project_root: Path,
    *,
    runtime: str = "DOCKER",
    agent: str = "opencode",
) -> InstallContext:
    project_root = project_root.resolve()
    version = _read_version(project_root)
    semver = version.split("+", 1)[0]
    image_tag = f"quay.io/hallamlab/metasmith:{version.replace('+', '-')}"
    channel_dir = project_root / "conda_build"

    if runtime.upper() == "DOCKER" and not _docker_image_present(image_tag):
        raise PreflightError(
            f"docker image {image_tag} not found locally.\n"
            f"  run: tests/e2e/agentic/install_mock/build_local_artifacts.sh"
        )

    if not _channel_has_pkg(channel_dir, semver):
        raise PreflightError(
            f"no metasmith-{semver}*.tar.bz2 in conda channel {channel_dir}.\n"
            f"  run: tests/e2e/agentic/install_mock/build_local_artifacts.sh"
        )

    sif_path: Path | None = None
    if runtime.upper() == "APPTAINER":
        sif_path = project_root / "metasmith.sif"
        if not sif_path.exists():
            raise PreflightError(
                f"apptainer runtime requested but {sif_path} is missing.\n"
                f"  run: tests/e2e/agentic/install_mock/build_local_artifacts.sh --apptainer"
            )

    if shutil.which("mamba") is None:
        raise PreflightError(
            "`mamba` not on host PATH.\n"
            "  install miniforge: https://github.com/conda-forge/miniforge"
        )
    if shutil.which("apptainer") is None:
        if runtime.upper() == "APPTAINER":
            raise PreflightError(
                "`apptainer` not on host PATH but runtime=APPTAINER.\n"
                "  install: see https://apptainer.org/docs/admin/main/installation.html"
            )

    agent = agent.lower()
    if agent == "opencode":
        if shutil.which("opencode") is None:
            raise PreflightError(
                "`opencode` binary not on PATH.\n"
                "  install: curl -fsSL https://opencode.ai/install | bash"
            )
        if not _opencode_has_credentials():
            raise PreflightError(
                "opencode has no credentials configured.\n"
                "  run: opencode auth login   (or export OPENCODE_API_KEY)"
            )
    elif agent == "claude":
        if shutil.which("claude") is None:
            raise PreflightError("`claude` binary not on PATH; install Claude Code")
        if not os.environ.get("ANTHROPIC_API_KEY"):
            pass
    else:
        raise PreflightError(f"unknown agent {agent!r}; expected opencode|claude")

    return InstallContext(
        version=semver,
        project_root=project_root,
        image_tag=image_tag,
        sif_path=sif_path,
        channel_dir=channel_dir,
        docs_dir=project_root / "docs" / "source",
    )


if __name__ == "__main__":
    import argparse, sys
    ap = argparse.ArgumentParser()
    ap.add_argument("--project-root", type=Path,
                    default=Path(__file__).resolve().parents[5])
    ap.add_argument("--runtime", default="DOCKER", choices=("DOCKER", "APPTAINER"))
    ap.add_argument("--agent", default="opencode", choices=("opencode", "claude"))
    a = ap.parse_args()
    try:
        ctx = verify(a.project_root, runtime=a.runtime, agent=a.agent)
    except PreflightError as exc:
        print(f"preflight FAILED:\n  {exc}", file=sys.stderr)
        sys.exit(1)
    print(f"OK  version={ctx.version}  image={ctx.image_tag}")
    print(f"    channel={ctx.channel_dir}")
    if ctx.sif_path:
        print(f"    sif={ctx.sif_path}")

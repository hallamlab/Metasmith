from __future__ import annotations

import os
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path

from tests.metasmith.e2e.agentic.install_mock.verify_local_artifacts import InstallContext

from .bootstrap_env import bootstrap_env_path, ensure_bootstrap_env
from .container_spoof import expected_sif_path, preplace_sif


@dataclass(frozen=True)
class SandboxLayout:
    root: Path
    home: Path
    workspace: Path
    agent_home: Path
    docs: Path
    data_types: Path
    transforms: Path
    local_channels: Path
    condarc: Path
    bootstrap_env: Path
    sif_in_cache: Path | None
    bash_env: Path | None = None


def _copy_tree(src: Path, dst: Path) -> None:
    if not src.exists():
        raise FileNotFoundError(f"sandbox source missing: {src}")
    if dst.exists():
        shutil.rmtree(dst)
    shutil.copytree(src, dst, symlinks=True)


def _hardlink_or_copy(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists():
        dst.unlink()
    try:
        os.link(src, dst)
    except OSError:
        shutil.copy2(src, dst)


def _hardlink_channel(src_channel: Path, dst_channel: Path) -> None:
    if dst_channel.exists():
        shutil.rmtree(dst_channel)
    for sub in src_channel.iterdir():
        if sub.is_dir():
            target = dst_channel / sub.name
            target.mkdir(parents=True, exist_ok=True)
            for f in sub.iterdir():
                if f.is_file():
                    _hardlink_or_copy(f, target / f.name)
        elif sub.is_file():
            _hardlink_or_copy(sub, dst_channel / sub.name)


def _resolve_data_types_src(project_root: Path) -> Path:
    canonical = project_root / "lib" / "data_types"
    if canonical.exists():
        return canonical
    fallback = project_root / "examples" / "data_types"
    if fallback.exists():
        return fallback
    return canonical


def _render_condarc(template: str, sandbox: Path, channel_root: Path) -> str:
    return (
        template
        .replace("<SANDBOX>", str(sandbox))
        .replace("<LOCAL_CHANNEL_ROOT>", str(channel_root))
    )


def build_sandbox(
    root: Path,
    ctx: InstallContext,
    *,
    runtime: str = "DOCKER",
) -> SandboxLayout:
    root = root.resolve()
    home = root / "home"
    workspace = root / "workspace"
    agent_home = root / "agent_home"
    docs = root / "docs"
    data_types = root / "data_types"
    transforms = root / "transforms"
    local_channels = root / "local-channels"
    envs_dir = root / "envs"
    pkgs_dir = root / "pkgs"
    apptainer_cache = home / ".apptainer" / "cache"

    for p in (home, workspace, agent_home, envs_dir, pkgs_dir,
              apptainer_cache, local_channels):
        p.mkdir(parents=True, exist_ok=True)

    _copy_tree(ctx.docs_dir, docs)
    _copy_tree(_resolve_data_types_src(ctx.project_root), data_types)
    _copy_tree(ctx.project_root / "main" / "transforms" / "std" / "transforms",
               transforms)

    _hardlink_channel(ctx.channel_dir, local_channels / "hallamlab")

    template_path = (Path(__file__).resolve().parent.parent
                     / "install_mock" / "condarc_template.yaml")
    condarc_text = _render_condarc(template_path.read_text(), root, local_channels)
    condarc = home / ".condarc"
    condarc.write_text(condarc_text)

    sif_in_cache: Path | None = None
    if runtime.upper() == "APPTAINER":
        if ctx.sif_path is None or not ctx.sif_path.exists():
            raise FileNotFoundError(
                f"APPTAINER sandbox requested but ctx.sif_path is missing: "
                f"{ctx.sif_path}"
            )
        image = f"docker://{ctx.image_tag}"
        sif_in_cache = preplace_sif(agent_home, image, ctx.sif_path,
                                    apptainer_cachedir=apptainer_cache)

    boot_env = ensure_bootstrap_env()

    (root / "PROGRESS.md").touch()

    bash_env = home / ".bash_env"
    bash_env.write_text(
        "# Auto-sourced via BASH_ENV by every non-interactive bash invocation.\n"
        f'export CONDARC="{condarc}"\n'
        "# shellcheck disable=SC1091\n"
        f'source "{boot_env}/etc/profile.d/conda.sh" 2>/dev/null || true\n'
        f'if [ -d "{root}/envs/msm_env" ]; then\n'
        f'    conda activate "{root}/envs/msm_env" 2>/dev/null || true\n'
        "fi\n"
    )

    return SandboxLayout(
        root=root,
        home=home,
        workspace=workspace,
        agent_home=agent_home,
        docs=docs,
        data_types=data_types,
        transforms=transforms,
        local_channels=local_channels,
        condarc=condarc,
        bootstrap_env=boot_env,
        sif_in_cache=sif_in_cache,
        bash_env=bash_env,
    )


def env_for_agent(layout: SandboxLayout) -> dict[str, str]:
    out = dict(os.environ)
    out["PYTHONPATH"] = ""
    for k in list(out):
        if k.startswith("CONDA_"):
            del out[k]
    out["HOME"] = str(layout.home)
    out["CONDARC"] = str(layout.condarc)
    out["PATH"] = f"{layout.bootstrap_env}/bin:{out.get('PATH', '')}"
    out["APPTAINER_CACHEDIR"] = str(layout.home / ".apptainer" / "cache")
    if layout.bash_env is not None:
        out["BASH_ENV"] = str(layout.bash_env)
    return out


def install_metasmith_into_sandbox(
    layout: SandboxLayout,
    ctx: InstallContext,
    env_name: str = "msm_env",
) -> Path:
    env = env_for_agent(layout)
    cmd = [
        "mamba", "create", "-y",
        "-n", env_name,
        "-c", "hallamlab",
        "-c", "conda-forge",
        "-c", "bioconda",
        f"metasmith={ctx.version}",
    ]
    r = subprocess.run(cmd, env=env, capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(
            f"sandbox pre-install of metasmith failed (exit {r.returncode}):\n"
            f"  stdout: {r.stdout[-500:]}\n  stderr: {r.stderr[-500:]}"
        )
    return layout.root / "envs" / env_name

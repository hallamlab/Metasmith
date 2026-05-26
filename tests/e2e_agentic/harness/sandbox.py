"""Per-test ephemeral sandbox builder.

Every test gets its own sandbox tree. Inside it:

    * a private ``HOME`` with a ``.condarc`` that aliases ``hallamlab`` to a
      local file:// channel and redirects conda's envs/pkgs dirs into the
      sandbox, so the agent's `mamba` calls never touch host state
    * a copy of ``docs/``, ``data_types/``, ``transforms/`` (NOT symlinks —
      opencode's `external_directory` permission gate fires on cross-`--dir`
      reads, so the agent must stay inside the sandbox boundary)
    * a pre-created ``agent_home`` with the metasmith sif hardlinked into
      its ``container_images/`` (apptainer runs only) so `agent deploy`
      finds it without pulling
    * a ``workspace/`` for the agent's cwd

Teardown is a single ``rm -rf`` of the sandbox root.
"""
from __future__ import annotations

import os
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path

from tests.e2e_agentic.install_mock.verify_local_artifacts import InstallContext

from .bootstrap_env import bootstrap_env_path, ensure_bootstrap_env
from .container_spoof import expected_sif_path, preplace_sif


@dataclass(frozen=True)
class SandboxLayout:
    root: Path
    home: Path
    workspace: Path
    agent_home: Path        # where `metasmith agent save --home <this>` should point
    docs: Path
    data_types: Path
    transforms: Path
    local_channels: Path    # parent of the `hallamlab` subdir
    condarc: Path
    bootstrap_env: Path
    sif_in_cache: Path | None  # the pre-placed metasmith sif, if APPTAINER


def _copy_tree(src: Path, dst: Path) -> None:
    """Copy a directory tree; raise if src is missing."""
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
    """Mirror the conda_build channel layout into the sandbox via hardlinks.

    Conda channels are: <root>/{noarch,linux-64,...}/<pkg>.tar.bz2 plus
    repodata.json files. We need the whole layout, not just the pkg.
    """
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
    """Materialize a fresh sandbox at ``root``.

    ``runtime`` determines whether the metasmith sif is pre-placed in
    ``agent_home/container_images/``. The agent is expected to use
    ``--home <root>/agent_home`` when running ``metasmith agent save``.
    """
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

    # --- 1. docs / data_types / transforms — copies, not symlinks
    _copy_tree(ctx.docs_dir, docs)
    _copy_tree(ctx.project_root / "lib" / "data_types", data_types)
    _copy_tree(ctx.project_root / "main" / "transforms" / "std" / "transforms",
               transforms)

    # --- 2. local conda channel: hardlinks into <root>/local-channels/hallamlab/
    _hardlink_channel(ctx.channel_dir, local_channels / "hallamlab")

    # --- 3. .condarc
    template_path = (Path(__file__).resolve().parent.parent
                     / "install_mock" / "condarc_template.yaml")
    condarc_text = _render_condarc(template_path.read_text(), root, local_channels)
    condarc = home / ".condarc"
    condarc.write_text(condarc_text)

    # --- 4. metasmith sif pre-placement (APPTAINER only)
    sif_in_cache: Path | None = None
    if runtime.upper() == "APPTAINER":
        if ctx.sif_path is None or not ctx.sif_path.exists():
            raise FileNotFoundError(
                f"APPTAINER sandbox requested but ctx.sif_path is missing: "
                f"{ctx.sif_path}"
            )
        # The image Agent.Deploy will check for is the canonical metasmith
        # container reference baked into src/metasmith/agents.py.
        image = f"docker://{ctx.image_tag}"
        sif_in_cache = preplace_sif(agent_home, image, ctx.sif_path)

    # --- 5. bootstrap env (host-shared)
    boot_env = ensure_bootstrap_env()

    # --- 6. stash empty PROGRESS.md (agent appends to it)
    (root / "PROGRESS.md").touch()

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
    )


def env_for_agent(layout: SandboxLayout) -> dict[str, str]:
    """Env dict to pass to the agent's shell-tool calls.

    HOME is redirected to the sandbox so the spoofed ``.condarc`` takes
    effect: `-c hallamlab` resolves to the local file:// channel and
    `envs_dirs` / `pkgs_dirs` redirect conda's state into the sandbox.

    HTTPS to conda-forge/bioconda is NOT blocked — metasmith's transitive
    deps (nextflow, pyyaml, etc.) come from those public channels just as
    they would for a real user. The only spoof points are `hallamlab`
    (channel-aliased) and `quay.io/hallamlab/metasmith` (host docker tag +
    pre-placed sif).
    """
    out = dict(os.environ)
    out.pop("PYTHONPATH", None)
    out["HOME"] = str(layout.home)
    out["PATH"] = f"{layout.bootstrap_env}/bin:{out.get('PATH', '')}"
    out["APPTAINER_CACHEDIR"] = str(layout.home / ".apptainer" / "cache")
    return out


def install_metasmith_into_sandbox(
    layout: SandboxLayout,
    ctx: InstallContext,
    env_name: str = "msm_env",
) -> Path:
    """Pre-install metasmith into a sandbox-local env.

    Used by scenarios that don't exercise the install path themselves
    (e.g. harness smoke, deploy). The install command mirrors what the docs
    instruct a user to run, so the verify path is identical: it just runs
    in the harness layer instead of the agent layer.

    Returns the env path.
    """
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

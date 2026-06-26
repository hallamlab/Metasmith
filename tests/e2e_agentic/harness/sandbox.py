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
    bash_env: Path | None = None  # auto-sourced before every `bash -c`; carries
                                  # the conda init so non-login shells can
                                  # `conda activate` without re-sourcing manually


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

    # The metasmith image store now honors APPTAINER_CACHEDIR (set in the
    # agent env below to `apptainer_cache`), so the APPTAINER branch pre-places
    # the sif under that cache dir — the deliberate spoof point so deploy skips
    # the container pull (`[ -e {sif} ] || pull`). `Agent.Deploy()` has no
    # home-level short-circuit, so pre-placing the sif doesn't short-circuit
    # the deploy itself — it just means the image is already cached when deploy
    # gets there. The relay binary extraction is independently gated on
    # relay/msm_relay so it self-heals if missing. agent_home is pre-created so
    # the layout is well-formed before any deploy runs.
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
        # Mirror the agent env's APPTAINER_CACHEDIR (set in env_for_agent) so
        # the sif lands at the store path Agent.Deploy will compute.
        sif_in_cache = preplace_sif(agent_home, image, ctx.sif_path,
                                    apptainer_cachedir=apptainer_cache)

    # --- 5. bootstrap env (host-shared)
    boot_env = ensure_bootstrap_env()

    # --- 6. stash empty PROGRESS.md (agent appends to it)
    (root / "PROGRESS.md").touch()

    # --- 7. bash_env: auto-sourced by every `bash -c` (BASH_ENV semantics).
    # Each opencode/claude tool call spawns a fresh shell, so activation done
    # in one call doesn't persist to the next. BASH_ENV sources the conda
    # hook and activates msm_env *every* time a shell is opened, so the
    # agent's verbatim docs commands work even when split across calls.
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
    # PYTHONPATH on the host may shadow the installed metasmith with a
    # different version's source tree (e.g. /home/tony/lib/locals/metasmith).
    # Setting it to empty (rather than .pop) is load-bearing: downstream
    # `run_streaming` re-merges os.environ, so a popped key reappears.
    # An explicit empty value overrides os.environ in that merge.
    out["PYTHONPATH"] = ""
    # Drop inherited CONDA_* vars from the test runner's own conda env. If
    # left in place, `conda activate msm_env` sees a pre-existing activation
    # and stacks instead of replacing, leaving the parent env's bin first on
    # PATH and breaking bare-name resolution.
    for k in list(out):
        if k.startswith("CONDA_"):
            del out[k]
    out["HOME"] = str(layout.home)
    # CONDARC is set explicitly because conda's shell hook (sourced before
    # `conda activate`) reads it directly. HOME redirection alone is
    # unreliable across conda installations. With CONDARC pinned, bare
    # `conda activate msm_env` resolves the path-based env in
    # <sandbox>/envs/ via the spoofed envs_dirs entry.
    out["CONDARC"] = str(layout.condarc)
    out["PATH"] = f"{layout.bootstrap_env}/bin:{out.get('PATH', '')}"
    out["APPTAINER_CACHEDIR"] = str(layout.home / ".apptainer" / "cache")
    # BASH_ENV is sourced by every non-interactive `bash -c` invocation.
    # We use it to re-apply `conda activate msm_env` in each shell the
    # agent's bash tool spawns, so PATH carries metasmith even when the
    # agent splits its commands across multiple tool calls.
    if layout.bash_env is not None:
        out["BASH_ENV"] = str(layout.bash_env)
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

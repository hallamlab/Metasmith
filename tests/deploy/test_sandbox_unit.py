"""Unit tests for sandbox materialization. No model call; runs by default."""
from __future__ import annotations

from pathlib import Path

import pytest

from tests.e2e.agentic.harness.container_spoof import (
    expected_sif_path,
    sif_basename,
)
from tests.e2e.agentic.harness.sandbox import build_sandbox, env_for_agent
from tests.e2e.agentic.install_mock.verify_local_artifacts import (
    PreflightError,
    verify as verify_install,
)


@pytest.fixture(scope="module")
def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def install_ctx(project_root):
    try:
        return verify_install(project_root, runtime="APPTAINER", agent="opencode")
    except PreflightError as exc:
        pytest.skip(f"preflight not satisfied: {exc}")


def test_sif_basename_matches_metasmith_sanitizer():
    # Mirrors src/metasmith/env/environment.py — keep this in sync.
    assert sif_basename("docker://quay.io/hallamlab/metasmith:0.18.1") == (
        "docker..quay.io_hallamlab_metasmith..0.18.1.sif"
    )


def test_expected_sif_path_lives_under_container_images(tmp_path):
    # Fallback: no APPTAINER_CACHEDIR → store is <agent_home>/container_images.
    p = expected_sif_path(tmp_path / "agent_home",
                          "docker://quay.io/hallamlab/metasmith:1.2.3")
    assert p.parent == tmp_path / "agent_home" / "container_images"
    assert p.name == "docker..quay.io_hallamlab_metasmith..1.2.3.sif"


def test_expected_sif_path_honors_apptainer_cachedir(tmp_path):
    # Override: APPTAINER_CACHEDIR set → store is that dir, mirroring
    # Environment._store_root() in src/metasmith/env/environment.py.
    cache = tmp_path / "scratch" / "apptainer"
    p = expected_sif_path(tmp_path / "agent_home",
                          "docker://quay.io/hallamlab/metasmith:1.2.3",
                          apptainer_cachedir=cache)
    assert p.parent == cache
    assert p.name == "docker..quay.io_hallamlab_metasmith..1.2.3.sif"


def test_build_sandbox_apptainer(install_ctx, tmp_path):
    root = tmp_path / "sb"
    layout = build_sandbox(root, install_ctx, runtime="APPTAINER")

    # Tree shape
    assert (root / "home" / ".condarc").is_file()
    assert (root / "envs").is_dir()
    assert (root / "pkgs").is_dir()
    assert (root / "workspace").is_dir()
    assert (root / "agent_home").is_dir()
    assert (root / "PROGRESS.md").is_file()

    # docs / data_types / transforms are real dirs (not symlinks) with content
    for sub in ("docs", "data_types", "transforms"):
        d = root / sub
        assert d.is_dir() and not d.is_symlink(), f"{sub} should be a real dir"
        assert any(d.iterdir()), f"{sub} is empty"

    # .condarc interpolation
    condarc = (root / "home" / ".condarc").read_text()
    assert str(root) in condarc, "SANDBOX placeholder not interpolated"
    assert "<SANDBOX>" not in condarc, "leftover SANDBOX placeholder in .condarc"
    assert "<LOCAL_CHANNEL_ROOT>" not in condarc
    assert "custom_channels:" in condarc
    assert "hallamlab:" in condarc

    # Local channel populated
    pkgs = list((root / "local-channels" / "hallamlab").rglob("metasmith-*.tar.bz2"))
    assert pkgs, "no metasmith pkg in spoofed hallamlab channel"

    # Sif pre-placed at the path Agent.Deploy will compute. The harness sets
    # APPTAINER_CACHEDIR under the sandbox home, and the store honors it, so
    # the sif lands there rather than under agent_home/container_images.
    assert layout.sif_in_cache is not None
    assert layout.sif_in_cache.exists()
    assert layout.sif_in_cache.parent == root / "home" / ".apptainer" / "cache"


def test_build_sandbox_docker_skips_sif(install_ctx, tmp_path):
    root = tmp_path / "sb_docker"
    layout = build_sandbox(root, install_ctx, runtime="DOCKER")
    assert layout.sif_in_cache is None
    # No sif pre-placement for DOCKER
    assert not list((root / "agent_home").rglob("*.sif"))


def test_env_for_agent_redirects_home(install_ctx, tmp_path):
    layout = build_sandbox(tmp_path / "sb_env", install_ctx, runtime="DOCKER")
    env = env_for_agent(layout)
    assert env["HOME"] == str(layout.home)
    # env_for_agent sets PYTHONPATH="" rather than popping it: subprocess.Popen
    # re-merges os.environ when env= is passed, so a popped key reappears. The
    # empty-string override is the contract; assert that, not absence.
    assert env.get("PYTHONPATH", "") == "", "host PYTHONPATH should not leak"
    assert str(layout.bootstrap_env / "bin") in env["PATH"]
    assert env["APPTAINER_CACHEDIR"].startswith(str(layout.home))


def test_env_for_agent_sets_condarc(install_ctx, tmp_path):
    layout = build_sandbox(tmp_path / "sb_condarc", install_ctx, runtime="DOCKER")
    env = env_for_agent(layout)
    # Without CONDARC, conda's shell hook falls back to the bootstrap install's
    # default config and `conda activate msm_env` reports EnvironmentNameNotFound
    # even with HOME redirected. The harness pins CONDARC to the spoofed file
    # so the bare-name activation works without env juggling in the prompt.
    assert env["CONDARC"] == str(layout.condarc)


def test_bare_conda_activate_works_with_env_for_agent(install_ctx, tmp_path):
    """End-to-end: after install_metasmith_into_sandbox, bare
    `conda activate msm_env` resolves under env_for_agent alone (no extra
    HOME/CONDARC/CONDA_ENVS_PATH set by the caller).

    This is the contract the stripped scenario prompts depend on.
    """
    import subprocess
    from tests.e2e.agentic.harness.sandbox import install_metasmith_into_sandbox

    layout = build_sandbox(tmp_path / "sb_activate", install_ctx, runtime="DOCKER")
    install_metasmith_into_sandbox(layout, install_ctx, env_name="msm_env")
    env = env_for_agent(layout)

    cmd = ("source $(conda info --base)/etc/profile.d/conda.sh "
           "&& conda activate msm_env "
           "&& which python")
    r = subprocess.run(["bash", "-lc", cmd], env=env, capture_output=True, text=True)
    assert r.returncode == 0, f"activation failed: {r.stderr}"
    resolved = r.stdout.strip().splitlines()[-1]
    expected_prefix = str(layout.root / "envs" / "msm_env")
    assert resolved.startswith(expected_prefix), (
        f"activate landed in unexpected env: {resolved!r}, "
        f"expected prefix {expected_prefix!r}"
    )

"""`src/metasmith/bin/sbatch` lines 42-50 must map container cwd to its
host path regardless of which bind Nextflow resolved through.

Bug reported by scope:scadc/metagenome (inbox msg #7, 2026-05-26) on
fir / workflow ``siT7Ffuc`` running 0.18.1. Apptainer mounts the same
host dir twice — once at ``WORK_ROOT`` (``/ws``) and once at
``HOME_ROOT`` (``/msm_home``). When Nextflow resolved the process
work-dir through the home-bind (cwd =
``/msm_home/runs/<KEY>/nxf_work/...``), the original
``is_relative_to(work_root)`` check fell through and ``external_cwd``
stayed as the container-side path. The relay then emitted
``cd /msm_home/runs/...`` on the host, which doesn't exist → silent
sbatch failure → downstream chain stuck.

We test the mapping rule by source-inspection (``sbatch`` is a script,
not importable) plus a behavioural check that mirrors the new branch
logic against the three input shapes (WORK_ROOT, HOME_ROOT, foreign).
"""
from __future__ import annotations

import inspect
from pathlib import Path

from metasmith.constants import AgentPaths


def _sbatch_source() -> str:
    # bin/sbatch is sibling to the metasmith package; reach it via the
    # constants module location to avoid hard-coding repo layout.
    import metasmith
    pkg = Path(inspect.getfile(metasmith)).parent
    sbatch = pkg / "bin" / "sbatch"
    assert sbatch.exists(), f"sbatch script not found at [{sbatch}]"
    return sbatch.read_text()


def test_sbatch_has_home_root_cwd_branch() -> None:
    """The relay shim must reroute HOME_ROOT-relative cwds via
    agent.real_path. Pinned at source level because the shim runs
    inside the container; spinning up an agent stub to import it
    here would over-mock the path resolution we're trying to test.
    """
    src = _sbatch_source()
    assert "AgentPaths.HOME_ROOT" in src, (
        "sbatch must reference AgentPaths.HOME_ROOT in its cwd mapping"
    )
    assert "agent.real_path" in src, (
        "sbatch must reroute HOME_ROOT-relative cwds via agent.real_path"
    )
    assert "elif cwd.is_relative_to(AgentPaths.HOME_ROOT)" in src, (
        "sbatch must have an elif branch for cwd under HOME_ROOT before "
        "the foreign-cwd fallthrough — otherwise /msm_home/... cwds leak "
        "through to the relay (msg #7, fir/siT7Ffuc)"
    )


def _map_cwd(cwd: Path, *, extern_work: Path, real_path: Path) -> Path:
    """Mirror of bin/sbatch:42-50 cwd → external mapping. Kept here so
    we can exercise it against fixture inputs without spinning up an
    Agent in-container.
    """
    if cwd.is_relative_to(AgentPaths.WORK_ROOT):
        return extern_work / cwd.relative_to(AgentPaths.WORK_ROOT)
    if cwd.is_relative_to(AgentPaths.HOME_ROOT):
        return real_path / cwd.relative_to(AgentPaths.HOME_ROOT)
    return cwd


def test_cwd_under_work_root_maps_to_extern_work() -> None:
    extern_work = Path("/scratch/phyberos/metasmith/runs/siT7Ffuc")
    real_path = Path("/scratch/phyberos/metasmith")
    cwd = AgentPaths.WORK_ROOT / "nxf_work/6b/8e9389"
    assert _map_cwd(cwd, extern_work=extern_work, real_path=real_path) == (
        extern_work / "nxf_work/6b/8e9389"
    )


def test_cwd_under_home_root_maps_via_real_path() -> None:
    """The fir/siT7Ffuc repro: Nextflow resolved the work-dir through
    the /msm_home bind. Pre-fix, external_cwd stayed as
    /msm_home/runs/...; post-fix it routes via agent.real_path.
    """
    extern_work = Path("/scratch/phyberos/metasmith/runs/siT7Ffuc")
    real_path = Path("/scratch/phyberos/metasmith")
    cwd = AgentPaths.HOME_ROOT / "runs/siT7Ffuc/nxf_work/6b/8e9389"
    assert _map_cwd(cwd, extern_work=extern_work, real_path=real_path) == (
        real_path / "runs/siT7Ffuc/nxf_work/6b/8e9389"
    )


def test_cwd_already_host_path_passes_through() -> None:
    """When sbatch is invoked from a host cwd (no container at all),
    the cwd is already external and stays unchanged.
    """
    extern_work = Path("/scratch/phyberos/metasmith/runs/siT7Ffuc")
    real_path = Path("/scratch/phyberos/metasmith")
    cwd = Path("/scratch/phyberos/metasmith/runs/siT7Ffuc/nxf_work/aa/bb")
    assert _map_cwd(cwd, extern_work=extern_work, real_path=real_path) == cwd

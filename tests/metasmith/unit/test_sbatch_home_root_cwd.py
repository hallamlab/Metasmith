from __future__ import annotations

import inspect
from pathlib import Path

from metasmith.constants import AgentPaths


def _sbatch_source() -> str:
    import metasmith
    pkg = Path(inspect.getfile(metasmith)).parent
    sbatch = pkg / "bin" / "sbatch"
    assert sbatch.exists(), f"sbatch script not found at [{sbatch}]"
    return sbatch.read_text()


def test_sbatch_has_home_root_cwd_branch() -> None:
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
    extern_work = Path("/scratch/phyberos/metasmith/runs/siT7Ffuc")
    real_path = Path("/scratch/phyberos/metasmith")
    cwd = AgentPaths.HOME_ROOT / "runs/siT7Ffuc/nxf_work/6b/8e9389"
    assert _map_cwd(cwd, extern_work=extern_work, real_path=real_path) == (
        real_path / "runs/siT7Ffuc/nxf_work/6b/8e9389"
    )


def test_cwd_already_host_path_passes_through() -> None:
    extern_work = Path("/scratch/phyberos/metasmith/runs/siT7Ffuc")
    real_path = Path("/scratch/phyberos/metasmith")
    cwd = Path("/scratch/phyberos/metasmith/runs/siT7Ffuc/nxf_work/aa/bb")
    assert _map_cwd(cwd, extern_work=extern_work, real_path=real_path) == cwd

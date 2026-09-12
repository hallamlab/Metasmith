from pathlib import Path

from metasmith.models.paths import PathMap


class _StubSource:
    def __init__(self, path: Path) -> None:
        self._path = path

    def GetPath(self) -> Path:
        return self._path


class _StubAgent:
    def __init__(self, home_path: Path) -> None:
        self.home = _StubSource(home_path)


def test_path_map_picks_correct_run_key_when_sample_dir_named_ws() -> None:
    extern_home = Path("/host/agent")
    run_key = "REALKEY"
    cwd = extern_home / "runs" / run_key / "ws" / "nxf_work/aa/bb"

    path_map = PathMap.FromExternalCwd(cwd=cwd, agent=_StubAgent(extern_home))

    assert path_map.task_key == run_key, (
        f"PathMap picked wrong task_key: got [{path_map.task_key}], expected [{run_key}]"
    )
    assert path_map.extern_work == extern_home / "runs" / run_key


def test_path_map_picks_correct_run_key_with_extra_subdir() -> None:
    extern_home = Path("/scratch/site")
    run_key = "REALKEY"
    cwd = extern_home / "runs" / run_key / "sample_a" / "nxf_work/aa/bb"

    path_map = PathMap.FromExternalCwd(cwd=cwd, agent=_StubAgent(extern_home))

    assert path_map.task_key == run_key, (
        f"PathMap picked wrong task_key: got [{path_map.task_key}], expected [{run_key}]"
    )

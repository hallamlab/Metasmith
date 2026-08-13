"""`bin/sbatch:36,40` uses `r"/\\w*/nxf_work/.*"` to extract the path
tail starting from the run key. The regex returns the *first* leftmost
match, which silently picks the wrong segment when any directory
between the run key and `nxf_work/` happens to look like a word.

The canonical pathological case: a sample / sub-workdir literally named
`ws` (the same name the in-container workdir is bound under). With cwd
`/host/runs/<KEY>/ws/nxf_work/<hash>`, the regex extracts
`ws/nxf_work/<hash>` and the downstream
`external_workspace = staged_dir / tail.split('/nxf_work/')[0]`
produces `staged_dir/ws` instead of `staged_dir/<KEY>`.

This test pins the bug; a `PathMap.FromExternalCwd` replacement will
identify the run key by ancestry (the directory whose name matches the
task key passed to the agent), not by regex on the segment shape.

Reference: audit Category B (regex parses).
"""
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
    """The :func:`PathMap.FromExternalCwd` replacement identifies the
    run key by ancestry (first segment under ``<extern_home>/runs/``),
    not by regex on the segment shape. A sub-directory literally named
    ``ws`` between the run key and ``nxf_work/`` no longer hijacks the
    extraction.
    """
    extern_home = Path("/host/agent")
    run_key = "REALKEY"
    cwd = extern_home / "runs" / run_key / "ws" / "nxf_work/aa/bb"

    path_map = PathMap.FromExternalCwd(cwd=cwd, agent=_StubAgent(extern_home))

    assert path_map.task_key == run_key, (
        f"PathMap picked wrong task_key: got [{path_map.task_key}], expected [{run_key}]"
    )
    assert path_map.extern_work == extern_home / "runs" / run_key


def test_path_map_picks_correct_run_key_with_extra_subdir() -> None:
    """Any extra segment between the run key and ``nxf_work/`` is
    irrelevant — only the first segment under ``runs/`` is the key.
    """
    extern_home = Path("/scratch/site")
    run_key = "REALKEY"
    cwd = extern_home / "runs" / run_key / "sample_a" / "nxf_work/aa/bb"

    path_map = PathMap.FromExternalCwd(cwd=cwd, agent=_StubAgent(extern_home))

    assert path_map.task_key == run_key, (
        f"PathMap picked wrong task_key: got [{path_map.task_key}], expected [{run_key}]"
    )

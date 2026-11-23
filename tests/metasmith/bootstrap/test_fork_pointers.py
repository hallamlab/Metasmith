from __future__ import annotations

import os
from fnmatch import fnmatch
from pathlib import Path

import pytest

from metasmith.bootstrap import point_fork_groups
from metasmith.models.libraries import ExecutionResult
from metasmith.models.solver import Dependency, Endpoint
from metasmith.models.workflow.payload import ZERO_MARK, output_file_name, zero_file_name

A, B, C = (Dependency({x}, set()) for x in "ABC")
GROUPS = [[A, B], [A, C]]
OUT = [{A: Endpoint({"A", "g0"}), B: Endpoint({"B"})}, {A: Endpoint({"A", "g1"}), C: Endpoint({"C"})}]
ENTRY = {"seed": ["x"], "KEY": "ab" * 32}


@pytest.fixture
def cwd(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    for n in "abc":
        (tmp_path/f"{n}.out").write_text(n)
    return tmp_path


def _name(g: int, d: Dependency) -> str:
    return output_file_name(ENTRY, OUT[g][d], batch=0, item=0, branch=g)


def _point(cwd: Path, *entries) -> bool:
    manifest = [{d: cwd/f"{n}.out" for d, n in e.items()} for e in entries]
    return point_fork_groups(GROUPS, [ExecutionResult(manifest=manifest)], OUT, [ENTRY], {})


def test_each_written_group_gets_its_own_pointer_to_one_shared_file(cwd):
    assert _point(cwd, {A: "a", B: "b"}, {A: "a", C: "c"})
    a0, a1 = cwd/_name(0, A), cwd/_name(1, A)
    assert a0.read_text() == "a" and os.path.samefile(a0, a1)
    assert (cwd/_name(0, B)).exists() and (cwd/_name(1, C)).exists()


def test_a_group_the_task_did_not_write_gets_no_pointer(cwd):
    assert _point(cwd, {A: "a", C: "c"})
    assert not (cwd/_name(0, A)).exists() and not (cwd/_name(0, B)).exists()
    assert (cwd/_name(1, A)).exists()


def test_a_group_the_task_did_not_write_gets_an_empty_marker_under_its_own_glob(cwd):
    assert _point(cwd, {A: "a", B: "b"})
    zeros = {p.name for p in cwd.iterdir() if ZERO_MARK in p.name}
    assert zeros == {zero_file_name(ENTRY, OUT[1][d], batch=0, branch=1) for d in (A, C)}
    for d in (A, C):
        assert fnmatch(zero_file_name(ENTRY, OUT[1][d], batch=0, branch=1), f"*-2.*-{OUT[1][d].key}")


@pytest.mark.parametrize("entries", [
    [],
    [{A: "a"}],
    [{A: "a", B: "b", C: "c"}],
    [{A: "a", B: "b"}, {A: "b", C: "c"}],
    [{A: "a", B: "missing"}],
], ids=["none", "partial", "undeclared", "two-paths", "missing-file"])
def test_a_manifest_that_is_not_whole_declared_groups_fails(cwd, entries):
    assert not _point(cwd, *entries)

"""`msm data invalidate` -- the lever stat-only leaf identity needs.

A leaf's identity is the absolute path plus the mtime of the top node, which is
what keeps a large reference from costing a tree walk on every run. The price
is that a change below the top node is invisible, so there has to be a way to
say "this changed" out loud.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from metasmith.models.libraries import DataInstanceLibrary
from metasmith.models.libraries.identity import stat_leaf_id
from metasmith.models.libraries.pinned import PinnedLibraryError

from metasmith.models.workflow import restat_leaf_ids
from metasmith.models.workflow.cache_decisions import compute_cache_decisions

from tests.metasmith.cache._cache_harness import (
    build_samples_library,
    build_types_library,
)
from tests.metasmith.cache.fixtures.cache_fixtures import linear_3step
from tests.metasmith.cache.test_empty_index import _context


def _library(tmp_path: Path) -> DataInstanceLibrary:
    types_path = build_types_library(tmp_path, ("seed",))
    return build_samples_library(
        tmp_path, types_path, count=2, input_type="seed"
    )


def test_invalidate_moves_the_id_to_the_one_the_agent_will_derive(tmp_path):
    lib = _library(tmp_path)
    item = next(iter(lib.manifest))
    before = lib.instance_meta[item]["instance_id"]

    report = lib.Invalidate([item])

    after = lib.instance_meta[item]["instance_id"]
    assert after != before, "invalidate left the identity where it was"
    assert report["moved"][str(item)] == {"from": before, "to": after}
    # The same formula the agent re-derives with at staging time; a different
    # one here means client and agent disagree about what the file is.
    assert after == stat_leaf_id(lib.location / item, lib.fork_id)


def test_invalidate_all_moves_every_leaf(tmp_path):
    lib = _library(tmp_path)
    before = {p: m["instance_id"] for p, m in lib.instance_meta.items()}

    lib.Invalidate()

    for path, old in before.items():
        assert lib.instance_meta[path]["instance_id"] != old, (
            f"[{path}] kept its identity through an invalidate --all"
        )


def test_invalidate_survives_a_reload(tmp_path):
    lib = _library(tmp_path)
    item = next(iter(lib.manifest))
    lib.Invalidate([item])
    expected = lib.instance_meta[item]["instance_id"]

    reloaded = DataInstanceLibrary.Load(lib.location)
    assert reloaded.instance_meta[item]["instance_id"] == expected, (
        "the moved identity was not saved; the next run would not see it"
    )


def test_invalidate_refuses_a_pinned_library(tmp_path):
    lib = _library(tmp_path)
    lib.Pin()
    reloaded = DataInstanceLibrary.Load(
        lib.location, check_pinned_stamps=False
    )
    with pytest.raises(PinnedLibraryError):
        reloaded.Invalidate()


def test_invalidate_reports_what_it_could_not_do(tmp_path):
    lib = _library(tmp_path)
    item = next(iter(lib.manifest))
    (lib.location / item).unlink()

    report = lib.Invalidate([item, Path("nothing/here.txt")])

    assert report["moved"] == {}, (
        "a file this host cannot see was given a fresh identity anyway; that "
        "is how a false cache hit is built"
    )
    assert set(report["skipped"]) == {str(item), "nothing/here.txt"}


def test_invalidating_a_given_moves_the_cache_key(tmp_path):
    """The production path: invalidate, and the agent's own re-stat moves it.

    `restat_leaf_ids` re-derives every leaf id from this host's view of the
    file before a run compiles, so a moved mtime lands in the lineage key and
    nothing built on the old one matches.
    """
    task = linear_3step.build_task(tmp_path)
    workspace = tmp_path / "ws"
    workspace.mkdir(exist_ok=True)
    cache_root = tmp_path / "task_cache"
    cache_root.mkdir(exist_ok=True)

    decisions = compute_cache_decisions(task, _context(workspace, cache_root))
    first = min(decisions)
    before = decisions[first]["cache_key"]

    assert task.data_libraries[0].Invalidate()["moved"], "invalidate moved nothing"
    restat_leaf_ids(task)

    after = compute_cache_decisions(
        task, _context(workspace, cache_root)
    )[first]["cache_key"]
    assert after != before, (
        "the cache key did not move, so every shard built on the old data "
        "would still be served"
    )

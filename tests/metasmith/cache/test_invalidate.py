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

from tests.metasmith.cache._cache_harness import (
    build_samples_library,
    build_types_library,
)
from tests.metasmith.cache.fixtures.cache_fixtures import linear_3step


def _library(tmp_path: Path) -> DataInstanceLibrary:
    types_path = build_types_library(tmp_path, ("seed",))
    return build_samples_library(
        tmp_path, types_path, count=2, input_type="seed", pooled=False,
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


def test_invalidate_refuses_an_import_and_names_the_way_to_move_it(tmp_path):
    from metasmith.testing.pool_fixtures import pool_backed

    lib = _library(tmp_path)
    pool_backed(lib)
    report = lib.Invalidate()

    assert report["moved"] == {}
    assert all("importing it again" in why for why in report["skipped"].values())


def test_re_importing_a_given_moves_the_member_key(tmp_path):
    """The production path: import again, then plan.

    An import assigns an identity rather than deriving one, so there is nothing
    for an invalidate to re-derive. A caller says the data changed by importing
    it a second time, and the pool records a second entry. A plan already built
    holds its own copy of every id and is stale by construction, so the order
    that matters is import first, plan second, and that is what this asserts.
    """
    from metasmith.caching.invocation import member_key
    from metasmith.testing.pool_fixtures import reimport
    from tests.metasmith.cache._cache_harness import build_workflow_task

    task = linear_3step.build_task(tmp_path)

    def _first_step_keys(t) -> set[bytes]:
        step = min(t.plan.steps, key=lambda s: s.order)
        keys = set()
        for dep in step.transform.model.requires:
            for inst in step.dependency_map.get(dep, []):
                keys.add(member_key("trA", "sig", {inst.dtype.key: [inst.instance_id]}))
        return keys

    before = _first_step_keys(task)
    lib = task.data_libraries[0]
    before_ids = {p: lib.instance_meta[p]["instance_id"] for p in lib.manifest}
    reimport(lib)
    assert all(
        lib.instance_meta[p]["instance_id"] != before_ids[p] for p in lib.manifest
    ), "a second import handed back the first import's identity"

    replanned = build_workflow_task(
        lib,
        task.transform_libraries[0],
        sample_type="seed",
        target_specs=[("step_c_target", {"step_c"})],
    )
    after = _first_step_keys(replanned)
    assert after, "the re-plan produced no member keys"
    assert not (before & after), (
        "a member key did not move, so every shard built on the old data "
        "would still be served"
    )

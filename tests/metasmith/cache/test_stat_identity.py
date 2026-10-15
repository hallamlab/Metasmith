from __future__ import annotations

import os
from pathlib import Path

from metasmith.models.libraries import DataInstanceLibrary
from metasmith.models.libraries.identity import stat_leaf_id
from metasmith.models.workflow import restat_leaf_ids

from tests.metasmith.cache._cache_harness import (
    build_samples_library,
    build_transform_library,
    build_types_library,
    build_workflow_task,
    identity_transform_code,
)


TYPE_NAMES = ("seed", "mid")


class TestStatLeafId:
    def test_stable_and_moves_with_mtime(self, tmp_path):
        f = tmp_path / "data.txt"
        f.write_text("payload\n", encoding="utf-8")

        first = stat_leaf_id(f)
        assert first is not None
        assert stat_leaf_id(f) == first

        os.utime(f, ns=(0, 1234567890))
        assert stat_leaf_id(f) != first

    def test_content_is_not_read(self, tmp_path):
        f = tmp_path / "data.txt"
        f.write_text("payload\n", encoding="utf-8")
        before = stat_leaf_id(f)
        st = f.stat()
        f.write_text("DIFFERENT payload\n", encoding="utf-8")
        os.utime(f, ns=(st.st_atime_ns, st.st_mtime_ns))
        assert stat_leaf_id(f) == before, (
            "identity is the stat, not the bytes; an edit that restores mtime is "
            "invisible here by construction"
        )

    def test_directory_costs_one_stat(self, tmp_path):
        d = tmp_path / "tree"
        (d / "nested").mkdir(parents=True)
        (d / "nested" / "a.txt").write_text("a\n", encoding="utf-8")
        assert stat_leaf_id(d) is not None

    def test_unreachable_path_is_none(self, tmp_path):
        assert stat_leaf_id(tmp_path / "absent.txt") is None

    def test_path_is_part_of_the_identity(self, tmp_path):
        a, b = tmp_path / "a.txt", tmp_path / "b.txt"
        a.write_text("same\n", encoding="utf-8")
        b.write_text("same\n", encoding="utf-8")
        st = a.stat()
        os.utime(b, ns=(st.st_atime_ns, st.st_mtime_ns))
        assert stat_leaf_id(a) != stat_leaf_id(b)

    def test_fork_id_separates(self, tmp_path):
        f = tmp_path / "data.txt"
        f.write_text("payload\n", encoding="utf-8")
        assert stat_leaf_id(f) != stat_leaf_id(f, "deadbeef")


def _task(root: Path):
    types_path = build_types_library(root, TYPE_NAMES)
    samples = build_samples_library(
        root, types_path, count=1, input_type="seed", pooled=False,
    )
    tr_lib = build_transform_library(
        root / "tr", types_path, {"trA": identity_transform_code("trA", "seed", "mid")}
    )
    return build_workflow_task(
        samples, tr_lib, sample_type="seed", target_specs=[("mid_target", {"mid"})]
    )


def _leaves(task):
    # Every instance the restat pass would touch: a present, non-deferred leaf.
    # `plan.given` and each step's `dependency_map` hold separate objects for one
    # file, joined only by id, so both are here.
    from metasmith.models.paths import is_deferred

    seen = {}
    everything = list(task.plan.given)
    for step in task.plan.steps:
        for insts in step.dependency_map.values():
            everything += insts
    for inst in everything:
        if inst.origin != "leaf" or is_deferred(inst.path):
            continue
        if not inst.ResolvePath().exists():
            continue
        seen.setdefault(id(inst), inst)
    return list(seen.values())


def _as_index_only(task):
    """Spell these inputs the way an input the client could NOT see arrives.

    A library pulled as a bare index carries the manifest and none of the files,
    so its entries reach the agent on placeholder ids and the agent is the first
    host that can derive a real one. `remote_src` is what says so.
    """
    from metasmith.models.remote import Source

    for inst in _leaves(task):
        inst.parent_lib.remote_src = Source.FromLocal(inst.parent_lib.location)
    return _leaves(task)


def _as_absolute(task):
    """The other kind the pass is for: a path given outside any library."""
    for inst in _leaves(task):
        inst.path = inst.ResolvePath()
    return _leaves(task)


class TestRestat:
    def test_leaves_an_input_the_client_staged_itself_alone(self, tmp_path):
        # The whole of the fix. A library-owned input was staged here out of a
        # library the client built, so the client had the file and its reading is
        # the authoritative one. Re-deriving it here would read the staged path,
        # which is named after the plan.
        task = _task(tmp_path)
        leaves = _leaves(task)
        assert leaves, "fixture produced no leaf instances"

        for inst in leaves:
            inst.instance_id = "0" * 64
        report = restat_leaf_ids(task)

        assert report["restated"] == 0
        # One file reaches the pass through more than one object -- `plan.given`
        # and the step's `dependency_map` -- so the count is of visits, not files.
        assert report["kept"] >= len(leaves)
        for inst in leaves:
            assert inst.instance_id == "0" * 64

    def test_replaces_a_foreign_id_with_this_hosts_view(self, tmp_path):
        task = _task(tmp_path)
        leaves = _as_index_only(task)
        assert leaves, "fixture produced no leaf instances"

        for inst in leaves:
            inst.instance_id = "0" * 64
        report = restat_leaf_ids(task)

        assert report["restated"] == len(leaves)
        assert report["kept"] == 0
        assert report["unreachable"] == []
        for inst in leaves:
            assert inst.instance_id == stat_leaf_id(
                inst.ResolvePath(), inst.parent_lib.fork_id
            )

    def test_an_absolute_path_is_still_re_derived(self, tmp_path):
        # An input named by an absolute path is not owned by any library, so
        # nothing about it says the client could read it.
        task = _task(tmp_path)
        leaves = _as_absolute(task)
        for inst in leaves:
            inst.instance_id = "0" * 64
        report = restat_leaf_ids(task)

        assert report["restated"] == len(leaves)
        for inst in leaves:
            assert inst.instance_id == stat_leaf_id(
                inst.path, inst.parent_lib.fork_id
            )

    def test_separate_objects_for_one_file_converge(self, tmp_path):
        task = _task(tmp_path)
        by_path: dict[str, set[str]] = {}
        for inst in _as_index_only(task):
            inst.instance_id = f"stale-{id(inst)}"
        restat_leaf_ids(task)
        for inst in _leaves(task):
            by_path.setdefault(str(inst.ResolvePath()), set()).add(inst.instance_id)
        for path, ids in by_path.items():
            assert len(ids) == 1, f"[{path}] left with {len(ids)} identities: {ids}"

    def test_is_idempotent(self, tmp_path):
        task = _task(tmp_path)
        for inst in _as_index_only(task):
            inst.instance_id = "0" * 64
        assert restat_leaf_ids(task)["restated"] > 0, (
            "nothing was re-derived, so idempotence here asserts nothing"
        )
        assert restat_leaf_ids(task)["restated"] == 0

    def test_library_meta_follows(self, tmp_path):
        task = _task(tmp_path)
        for inst in _as_index_only(task):
            inst.instance_id = "0" * 64
        restat_leaf_ids(task)
        for inst in _leaves(task):
            meta = inst.parent_lib.instance_meta.get(inst.path)
            if meta is None:
                continue
            assert meta["instance_id"] == inst.instance_id

    def test_unreachable_input_keeps_its_id(self, tmp_path):
        task = _task(tmp_path)
        _as_index_only(task)
        leaf = _leaves(task)[0]
        leaf.path = Path("/nonexistent/somewhere/absent.txt")
        leaf.instance_id = "1" * 64
        report = restat_leaf_ids(task)
        assert leaf.instance_id == "1" * 64
        assert str(leaf.path) in report["unreachable"]

    def test_an_absent_client_input_is_counted(self, tmp_path):
        # Kept, because nothing here can do better -- but said out loud, because
        # it will never reuse a shard and the run is about to fail on it.
        task = _task(tmp_path)
        leaf = _leaves(task)[0]
        gone = leaf.ResolvePath()
        gone.unlink()
        report = restat_leaf_ids(task)
        assert str(gone) in report["absent"]

    def test_pinned_library_is_untouched(self, tmp_path):
        task = _task(tmp_path)
        leaves = _as_index_only(task)
        for inst in leaves:
            inst.instance_id = "2" * 64
            inst.parent_lib._pinned = {"entries": {}}
        restat_leaf_ids(task)
        for inst in leaves:
            assert inst.instance_id == "2" * 64

    def test_opt_out_disables_it(self, tmp_path, monkeypatch):
        monkeypatch.setenv("METASMITH_LEAF_RANDOM", "1")
        task = _task(tmp_path)
        leaves = _as_index_only(task)
        for inst in leaves:
            inst.instance_id = "3" * 64
        assert restat_leaf_ids(task)["restated"] == 0
        for inst in leaves:
            assert inst.instance_id == "3" * 64


class TestStagedPlanPersistence:
    def test_rewrite_round_trips_the_restated_ids(self, tmp_path):
        # CollectResults reloads the task after the run and joins the trace's
        # ids against the plan's, so what staging re-derived has to reach disk.
        from metasmith.agents.runner import _rewrite_staged_plan
        from metasmith.models.remote import Source
        from metasmith.models.workflow import WorkflowTask

        task = _task(tmp_path / "build")
        staged_at = tmp_path / "staged"
        task.SaveAs(Source.FromLocal(staged_at))

        loaded = WorkflowTask.Load(staged_at)
        for inst in _as_index_only(loaded):
            inst.instance_id = "0" * 64
        restat_leaf_ids(loaded)
        want = {str(i.ResolvePath()): i.instance_id for i in _leaves(loaded)}
        assert "0" * 64 not in want.values()

        _rewrite_staged_plan(staged_at, loaded)
        again = WorkflowTask.Load(staged_at)
        assert {str(i.ResolvePath()): i.instance_id for i in _leaves(again)} == want

    def test_the_key_survives_the_rewrite(self, tmp_path):
        # The key names the staged directory. `WorkflowPlan._update_hash` folds
        # the given ids in, so re-minting them would move it out from under
        # `PathMap` on the next stage if the bundle did not record it.
        from metasmith.agents.runner import _rewrite_staged_plan
        from metasmith.models.remote import Source
        from metasmith.models.workflow import WorkflowTask

        task = _task(tmp_path / "build")
        staged_at = tmp_path / "staged"
        task.SaveAs(Source.FromLocal(staged_at))

        loaded = WorkflowTask.Load(staged_at)
        key = loaded.GetKey()
        for inst in _as_index_only(loaded):
            inst.instance_id = "0" * 64
        restat_leaf_ids(loaded)
        _rewrite_staged_plan(staged_at, loaded)

        assert WorkflowTask.Load(staged_at).GetKey() == key


class TestLibraryMinting:
    def test_add_item_mints_the_stat_id(self, tmp_path):
        types_path = build_types_library(tmp_path, TYPE_NAMES)
        lib = DataInstanceLibrary(tmp_path / "samples.xgdb")
        lib.AddTypeLibrary(types_path, namespace="cf")
        f = lib.location / "data.txt"
        f.write_text("payload\n", encoding="utf-8")

        lib.AddItem(Path("data.txt"), "cf::seed")
        assert lib.Get(Path("data.txt")).instance_id == stat_leaf_id(f)

    def test_add_value_mints_the_stat_id(self, tmp_path):
        types_path = build_types_library(tmp_path, TYPE_NAMES)
        lib = DataInstanceLibrary(tmp_path / "samples.xgdb")
        lib.AddTypeLibrary(types_path, namespace="cf")

        at = lib.AddValue("param", "42", "cf::seed")
        assert lib.Get(at).instance_id == stat_leaf_id(lib.location / at), (
            "a value was registered before it was written, so its id is random"
        )

    def test_absent_file_falls_back_to_a_unique_id(self, tmp_path):
        types_path = build_types_library(tmp_path, TYPE_NAMES)

        def _mint(where: str) -> str:
            lib = DataInstanceLibrary(tmp_path / where)
            lib.AddTypeLibrary(types_path, namespace="cf")
            absent = Path("/nonexistent/somewhere/absent.txt")
            lib.AddItem(absent, "cf::seed")
            return lib.Get(absent).instance_id

        assert _mint("a.xgdb") != _mint("b.xgdb"), (
            "a path nothing can stat must not collapse to a shared id; staging "
            "is what gives it a real one"
        )


def _two_plans_over_one_library(root: Path):
    # Two workflows that read the same samples and disagree about everything
    # after that: one stops at `mid`, the other carries on to `end`. Their plan
    # keys differ, so they stage into different directories.
    types_path = build_types_library(root, ("seed", "mid", "end"))
    samples = build_samples_library(
        root, types_path, count=2, input_type="seed", pooled=False,
    )
    tr_lib = build_transform_library(
        root / "tr",
        types_path,
        {
            "trA": identity_transform_code("trA", "seed", "mid"),
            "trB": identity_transform_code("trB", "mid", "end"),
        },
    )
    short = build_workflow_task(
        samples, tr_lib, sample_type="seed", target_specs=[("mid_target", {"mid"})]
    )
    longer = build_workflow_task(
        samples, tr_lib, sample_type="seed", target_specs=[("end_target", {"end"})]
    )
    return short, longer


class TestIdentityIsIndependentOfThePlan:
    def test_one_library_staged_under_two_plans_keeps_its_leaf_ids(self, tmp_path):
        # The invariant the member cache rests on: a leaf's identity is a fact
        # about the file, not about the workflow that happens to read it. The
        # staged directory is named after the whole plan, so anything that reads
        # identity off that path answers differently for the same bytes as soon
        # as a step is added anywhere -- and every sample already computed stops
        # matching its shard.
        from metasmith.models.remote import Source
        from metasmith.models.workflow import WorkflowTask

        first, second = _two_plans_over_one_library(tmp_path / "build")
        assert first.GetKey() != second.GetKey(), (
            "the two plans must stage under different names, or this asserts"
            " nothing"
        )

        def stage(task) -> dict[str, str]:
            at = tmp_path / "home" / "runs" / task.GetKey() / "task"
            at.parent.mkdir(parents=True, exist_ok=True)
            task.SaveAs(Source.FromLocal(at))
            loaded = WorkflowTask.Load(at)
            restat_leaf_ids(loaded)
            return {str(i.path): i.instance_id for i in _leaves(loaded)}

        a, b = stage(first), stage(second)
        shared = sorted(set(a) & set(b))
        assert shared, "the two plans have no leaf input in common"
        moved = [p for p in shared if a[p] != b[p]]
        assert not moved, (
            f"[{len(moved)}] of [{len(shared)}] leaf input(s) changed identity"
            f" because the plan around them changed: {moved}"
        )

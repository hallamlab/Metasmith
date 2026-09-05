from __future__ import annotations

from pathlib import Path

import pytest

from metasmith.caching.admission import IMPORTED, PRODUCT
from metasmith.caching.layout import CACHE_DIR_NAME
from metasmith.caching.projection import project_store
from metasmith.models.libraries import DataTypeLibrary

from tests.metasmith.cache._cache_harness import (
    build_samples_library,
    build_transform_library,
    build_types_library,
    build_workflow_task,
    capture_run,
    identity_transform_code,
)


TYPE_NAMES = ("seed", "mid", "out")


def _run_a_pipeline(root: Path, virtual_runtime, target: str = "out"):
    types_path = build_types_library(root, TYPE_NAMES)
    samples = build_samples_library(root, types_path, count=2, input_type="seed")
    tr_lib = build_transform_library(root / "tr", types_path, {
        "trA": identity_transform_code("trA", "seed", "mid"),
        "trB": identity_transform_code("trB", "mid", "out"),
    })
    task = build_workflow_task(
        samples, tr_lib, sample_type="seed",
        target_specs=[(f"{target}_target", {target})],
    )
    capture_run(virtual_runtime, task)
    return types_path, samples, tr_lib


def _project(virtual_runtime, types_path, **kw):
    return project_store(
        virtual_runtime.home / CACHE_DIR_NAME,
        types={"cf": DataTypeLibrary.Load(types_path)},
        **kw,
    )


class TestProjection:
    def test_products_come_back_as_typed_instances(self, tmp_path, virtual_runtime):
        types_path, _samples, _tr = _run_a_pipeline(tmp_path / "a", virtual_runtime)
        proj = _project(virtual_runtime, types_path)

        assert proj.library.manifest, proj.skipped
        names = sorted({n for _p, n, _e in proj.library.Iterate()})
        assert names == ["cf::mid", "cf::out"]
        for path in proj.library.manifest:
            assert path.is_absolute()
            assert path.exists(), f"[{path}] is not where the store says it is"

    def test_an_instance_carries_the_identity_the_run_recorded(
        self, tmp_path, virtual_runtime,
    ):
        types_path, _samples, _tr = _run_a_pipeline(tmp_path / "a", virtual_runtime)
        proj = _project(virtual_runtime, types_path)

        for path, item in proj.items.items():
            inst = proj.library.Get(path)
            assert inst.instance_id == item.instance_id
            assert inst.origin == PRODUCT

    def test_a_products_parents_are_its_own_ancestors(self, tmp_path, virtual_runtime):
        types_path, _samples, _tr = _run_a_pipeline(tmp_path / "a", virtual_runtime)
        proj = _project(virtual_runtime, types_path)

        # trB consumed trA's product, so every `out` descends from a `mid` that
        # is itself in the pool. That edge is the whole point of the store being
        # the index: the ancestry survives without a second record of it.
        outs = [p for p, n, _e in proj.library.Iterate() if n == "cf::out"]
        assert outs
        for p in outs:
            parents = proj.library.parents.get(p, [])
            assert [pm.name for pm in parents] == ["cf::mid"], p

    def test_a_masked_projection_is_exactly_the_named_subset(
        self, tmp_path, virtual_runtime,
    ):
        types_path, _samples, _tr = _run_a_pipeline(tmp_path / "a", virtual_runtime)
        proj = _project(virtual_runtime, types_path)

        wanted = {p for p, n, _e in proj.library.Iterate() if n == "cf::out"}
        view = proj.library.AsView(wanted)
        assert {p for p, _n, _e in view.Iterate()} == wanted
        assert len(wanted) < len(proj.library.manifest)

    def test_the_planner_takes_a_projection_as_its_input_library(
        self, tmp_path, virtual_runtime,
    ):
        types_path, _samples, tr_lib = _run_a_pipeline(tmp_path / "a", virtual_runtime)
        proj = _project(virtual_runtime, types_path)

        # The projection is a data instance library, so it is the thing the
        # planner reads inputs from -- no special case, no `_as_data_lib` branch.
        from metasmith.models.solver import Transform
        from metasmith.models.workflow import WorkflowPlan

        given = [[sv] for sv in proj.library.AsSamples("cf::mid")]
        assert given, "the projection surfaced no samples to plan from"
        target = Transform()
        target.AddRequirement(properties={"out"})
        plan = WorkflowPlan.Generate(
            given=given,
            transforms=[tr_lib],
            target_names=["out_target"],
            target_model=target,
        )
        assert isinstance(plan, WorkflowPlan), f"did not converge: {plan!r}"

    def test_a_spec_solves_from_a_projection(self, tmp_path, virtual_runtime):
        # The same thing through the entry point a caller actually uses.
        # `_as_data_lib` returns a DataInstanceLibrary unchanged, so a
        # projection needs no branch of its own.
        #
        # The first run stops at `mid`, so the store holds a real intermediate
        # and nothing downstream of it -- which is the case worth planning
        # from, and the one a store-as-input exists to serve.
        from metasmith.agents.spec import Spec

        types_path, _samples, tr_lib = _run_a_pipeline(
            tmp_path / "a", virtual_runtime, target="mid",
        )
        proj = _project(virtual_runtime, types_path)
        assert sorted({n for _p, n, _e in proj.library.Iterate()}) == ["cf::mid"]

        task = Spec(
            input_library=proj.library,
            target_types=["cf::out"],
            transform_libraries=[tr_lib],
            sample_type="cf::mid",
        ).Solve()
        assert task.ok, task.plan.dropped_targets
        assert [s.transform.name for s in task.plan.steps] == ["trB"]

    def test_an_untyped_entry_is_reported_not_raised(self, tmp_path, virtual_runtime):
        types_path, _samples, _tr = _run_a_pipeline(tmp_path / "a", virtual_runtime)
        # A namespace the projection was not handed. Every entry names `cf`, so
        # asking for nothing must give an empty library and a full skip list.
        proj = project_store(
            virtual_runtime.home / CACHE_DIR_NAME, types={},
        )
        assert proj.library.manifest == {}
        assert proj.skipped.get("unknown type"), proj.skipped


class TestImportedProjection:
    def test_an_imported_item_projects_where_its_bytes_are(self, tmp_path):
        from metasmith.models.libraries import DataInstanceLibrary
        from metasmith.ops.data import admit_library_items

        types_path = build_types_library(tmp_path, TYPE_NAMES)
        lib = DataInstanceLibrary(tmp_path / "in.xgdb")
        lib.AddTypeLibrary(types_path, namespace="cf")
        (lib.location / "a.txt").write_text("a\n")
        lib.AddItem(Path("a.txt"), "cf::seed")
        lib.Save()

        cache_root = tmp_path / CACHE_DIR_NAME
        res = admit_library_items(lib, cache_root, include_leaves=True)
        assert res["admitted"] == 1

        proj = project_store(
            cache_root, types={"cf": DataTypeLibrary.Load(types_path)},
        )
        assert list(proj.library.manifest) == [lib.location / "a.txt"]
        (path,) = proj.library.manifest
        assert proj.items[path].origin == IMPORTED
        assert proj.library.Get(path).instance_id == lib.Get(Path("a.txt")).instance_id

    def test_importing_a_folder_costs_what_one_file_costs(self, tmp_path):
        from metasmith.models.libraries import DataInstanceLibrary
        from metasmith.ops.data import admit_library_items

        types_path = build_types_library(tmp_path, TYPE_NAMES)
        lib = DataInstanceLibrary(tmp_path / "in.xgdb")
        lib.AddTypeLibrary(types_path, namespace="cf")
        big = lib.location / "refs"
        big.mkdir(parents=True)
        for i in range(500):
            (big / f"{i}.hmm").write_text("x")
        lib.AddItem(Path("refs"), "cf::seed")
        lib.Save()

        cache_root = tmp_path / CACHE_DIR_NAME
        res = admit_library_items(lib, cache_root, include_leaves=True)
        assert res["admitted"] == 1

        # Nothing was copied and nothing was walked: the shard holds a manifest
        # and an empty out/, and the bytes never moved.
        names = sorted(p.name for p in cache_root.rglob("*") if p.is_file())
        assert "manifest.cbor" in names
        assert not [n for n in names if n.endswith(".hmm")], names
        assert len(list(big.iterdir())) == 500

        # And the size it recorded is the one stat it took, not a tree total.
        proj = project_store(
            cache_root, types={"cf": DataTypeLibrary.Load(types_path)},
        )
        (path,) = proj.library.manifest
        assert proj.items[path].size_bytes == big.stat().st_size


class TestGroupingByRun:
    def test_a_product_names_the_run_that_made_it(self, tmp_path, virtual_runtime):
        from metasmith.caching.store import CacheStore

        _types, _samples, _tr = _run_a_pipeline(tmp_path / "a", virtual_runtime)
        with CacheStore.open(virtual_runtime.home / CACHE_DIR_NAME) as s:
            entries = list(s.iter_entries())
        assert entries
        runs = {e.run for e in entries}
        assert len(runs) == 1 and "" not in runs, runs
        # And it is the run as the agent names it on disk.
        assert (virtual_runtime.home / "runs" / runs.pop()).is_dir()

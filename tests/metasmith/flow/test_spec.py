from __future__ import annotations

from pathlib import Path

import pytest

from metasmith.agents import Spec
from metasmith.models.libraries import DataInstanceLibrary, DataInstanceLibraryView
from metasmith.testing import mock_transforms as mt
from metasmith.testing.pool_fixtures import pool_backed

from .conftest import _build_samples_lib, _build_transform_lib, _build_type_lib


def _setup(tmp_path: Path):
    types_path = _build_type_lib(tmp_path / "types.yml")
    samples = _build_samples_lib(tmp_path, types_path, n_samples=2, dtype="assembly")
    tr_lib = _build_transform_lib(
        tmp_path / "tr", types_path,
        dict(mt.identity_transform("mock::assembly", "mock::bam")),
    )
    return samples, tr_lib


def test_pack_unpack_round_trips(tmp_path: Path):
    spec = Spec(
        input_library="/data/input.xgdb",
        target_types=["mock::bam", {"type": "mock::merged", "parents": [0], "when": ["mock::reads"]}],
        transform_libraries=["/lib/transforms.xgdb"],
        resource_libraries=["/lib/refs.xgdb"],
        sample_type="mock::reads",
        shared_input_paths=["/data/input.xgdb/db.fa"],
    )
    assert Spec.Unpack(spec.Pack()).Pack() == spec.Pack()


def test_a_spec_carries_no_store_bookkeeping(tmp_path: Path):
    record = {
        "schema": "v1", "name": "brave-otter", "created_at": "2026-01-01",
        "forked_from": "shy-otter",
        "sample_type": "mock::reads", "target_types": ["mock::bam"],
        "transform_libraries": [], "resource_libraries": [],
        "input_library": "input.xgdb",
    }
    packed = Spec.Unpack(record).Pack()
    for owned_by_the_store in ("name", "created_at", "forked_from", "schema"):
        assert owned_by_the_store not in packed


def test_pack_writes_a_location_for_a_library_given_as_an_object(tmp_path: Path):
    samples, tr_lib = _setup(tmp_path)
    spec = Spec(input_library=samples, transform_libraries=[tr_lib],
                target_types=["mock::bam"])
    packed = spec.Pack()
    assert packed["input_library"] == str(samples.location)
    assert packed["transform_libraries"] == [str(tr_lib.location)]


def test_both_doors_reach_the_same_plan(tmp_path: Path):
    samples, tr_lib = _setup(tmp_path)

    from_refs = Spec(
        input_library=str(samples.location),
        target_types=["mock::bam"],
        transform_libraries=[str(tr_lib.location)],
        sample_type="mock::assembly",
    ).Solve()

    reloaded_samples = type(samples).Load(samples.location)
    from_objects = Spec.SolveViews(
        samples=list(reloaded_samples.AsSamples("mock::assembly")),
        resources=[],
        transforms=[tr_lib],
        targets=["mock::bam"],
    )
    assert from_refs.plan._key == from_objects.plan._key


def test_a_spec_solves_from_a_stored_record(tmp_path: Path):
    samples, tr_lib = _setup(tmp_path)
    record = {
        "name": "brave-otter", "created_at": "2026-01-01",
        "sample_type": "mock::assembly",
        "target_types": ["mock::bam"],
        "transform_libraries": [str(tr_lib.location)],
        "resource_libraries": [],
    }
    task = Spec.Unpack(record, input_library=str(samples.location)).Solve()
    assert task.ok and task.plan.steps


def test_an_empty_target_list_is_refused(tmp_path: Path):
    samples, tr_lib = _setup(tmp_path)
    with pytest.raises(AssertionError, match="can not be empty"):
        Spec(input_library=samples, transform_libraries=[tr_lib]).Solve()


def test_the_input_library_is_listed_once_when_it_is_also_shared(tmp_path: Path):
    types_path = _build_type_lib(tmp_path / "types.yml")
    samples = _build_samples_lib(tmp_path, types_path, n_samples=2, dtype="assembly")
    shared = Path("shared.fa")
    (samples.location / shared).write_text(">c\nACGT\n", encoding="utf-8")
    samples.AddItem(shared, "mock::sample_metadata")
    samples.Save()
    tr_lib = _build_transform_lib(
        tmp_path / "tr", types_path,
        dict(mt.identity_transform("mock::assembly", "mock::bam")),
    )

    task = Spec(
        input_library=str(samples.location),
        target_types=["mock::bam"],
        transform_libraries=[str(tr_lib.location)],
        sample_type="mock::assembly",
        shared_input_paths=[str(shared)],
    ).Solve()
    locations = [lib.location for lib in task.data_libraries]
    assert len(locations) == len(set(locations)), locations


def test_a_conditional_target_reaches_only_the_shapes_that_carry_its_given(tmp_path: Path):
    types_path = _build_type_lib(tmp_path / "types.yml")
    lib = DataInstanceLibrary(tmp_path / "samples.xgdb")
    lib.AddTypeLibrary(types_path, namespace="mock")
    for sid, kinds in (("short", ["assembly"]), ("hybrid", ["assembly", "reads"])):
        (lib.location / sid).mkdir(parents=True)
        for f, text in (("meta.json", f'{{"id": "{sid}"}}'), *((f"{k}.txt", f">{sid}\nACGT\n") for k in kinds)):
            (lib.location / sid / f).write_text(text, encoding="utf-8")
        meta = lib.AddItem(Path(f"{sid}/meta.json"), "mock::sample_metadata")
        for k in kinds:
            lib.AddItem(Path(f"{sid}/{k}.txt"), f"mock::{k}", parents=[meta])
    pool_backed(lib)
    lib.Save()
    tr_lib = _build_transform_lib(
        tmp_path / "tr", types_path,
        mt.identity_transform("mock::assembly", "mock::bam")
        | mt.identity_transform("mock::reads", "mock::h1"),
    )

    def spec(targets):
        return Spec(
            input_library=lib, transform_libraries=[tr_lib],
            sample_type="mock::sample_metadata", target_types=targets,
        )

    task = spec(["mock::bam", {"type": "mock::h1", "when": ["mock::reads"]}]).Solve()
    assert task.ok, task.plan.dropped_targets
    cases = {c: "reads" in c for c in task.plan.cases}
    assert len(cases) == 2
    by_product = {
        next(i.dtype_name for g in s.produces for i in g): {cases[c] for c in s.cases}
        for s in task.plan.steps
    }
    assert by_product == {"mock::bam": {True, False}, "mock::h1": {True}}

    with pytest.raises(AssertionError, match=r"no target applies to the samples of shape \[.*assembly"):
        spec([{"type": "mock::h1", "when": "mock::reads"}]).Solve()


def test_one_endpoint_fills_a_target_and_the_target_anchored_to_it(tmp_path: Path):
    samples, tr_lib = _setup(tmp_path)
    task = Spec(
        input_library=samples, transform_libraries=[tr_lib],
        target_types=["mock::bam", {"type": "mock::bam", "parents": [0]}],
    ).Solve()
    assert task.ok, task.plan.dropped_targets
    assert len(task.plan.steps) == 1

"""`Spec`: the one pre-solve representation.

Two things worth pinning. That the spec is *only* the ask -- a workflow record
carries a name and a creation time and a spec must not, or every shared copy
arrives with the sender's workflow name on it. And that the notebook's door and
the veneers' door now reach the same solver, which they did not before: the
bodies behind them had drifted, and nothing failed when they disagreed -- the
plan was merely different.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from metasmith.agents import Spec
from metasmith.models.libraries import DataInstanceLibraryView
from metasmith.testing import mock_transforms as mt

from .conftest import _build_samples_lib, _build_transform_lib, _build_type_lib


def _setup(tmp_path: Path):
    types_path = _build_type_lib(tmp_path / "types.yml")
    samples = _build_samples_lib(tmp_path, types_path, n_samples=2, dtype="assembly")
    tr_lib = _build_transform_lib(
        tmp_path / "tr", types_path,
        dict(mt.identity_transform("mock::assembly", "mock::bam")),
    )
    return samples, tr_lib


# -- serialization -----------------------------------------------------------


def test_pack_unpack_round_trips(tmp_path: Path):
    spec = Spec(
        input_library="/data/input.xgdb",
        target_types=["mock::bam", {"type": "mock::merged", "parents": [0]}],
        transform_libraries=["/lib/transforms.xgdb"],
        resource_libraries=["/lib/refs.xgdb"],
        sample_type="mock::reads",
        shared_input_paths=["/data/input.xgdb/db.fa"],
    )
    assert Spec.Unpack(spec.Pack()).Pack() == spec.Pack()


def test_a_spec_carries_no_store_bookkeeping(tmp_path: Path):
    """A workflow record is an envelope around a spec, not a spec.

    `name`, `created_at` and `forked_from` belong to whoever is storing the
    workflow: a spec that carried them would hand every exported copy the
    sender's workflow name, to collide with on import.
    """
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


# -- solving -----------------------------------------------------------------


def test_both_doors_reach_the_same_plan(tmp_path: Path):
    """`Spec.Solve` from references == `SolveViews` from objects in hand.

    The notebook calls the second through `Agent.GenerateWorkflow`, the GUI and
    the CLI call the first. Before this they were two copies of the same twenty
    lines, and only one of them knew about shared inputs.
    """
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
    """A shared-input mask is a view of the input library, not another library.

    Listed twice it would be staged twice.
    """
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

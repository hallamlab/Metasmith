from __future__ import annotations

from pathlib import Path

import pytest

from metasmith.agents import CaseViews, Plan, Spec, plan_views
from metasmith.agents.spec import case_records
from metasmith.testing import mock_transforms as mt

from .conftest import _build_samples_lib, _build_transform_lib, _build_type_lib


def _setup(tmp_path: Path, n_samples: int = 2, shared_root: bool = False, transforms=None):
    types_path = _build_type_lib(tmp_path / "types.yml")
    samples = _build_samples_lib(
        tmp_path, types_path, n_samples=n_samples, dtype="assembly", shared_root=shared_root,
    )
    tr_lib = _build_transform_lib(
        tmp_path / "tr", types_path,
        transforms or dict(mt.identity_transform("mock::assembly", "mock::bam")),
    )
    return samples, tr_lib


def test_pack_unpack_round_trips():
    spec = Spec(
        input_library="/data/input.xgdb",
        name="short_reads",
        target_types=["mock::bam", {"type": "mock::merged", "parents": [0]}],
        sample_type="mock::reads",
        input_paths=["s0/meta.json"],
        shared_input_paths=["db.fa"],
    )
    assert Spec.Unpack(spec.Pack(), input_library=spec.input_library) == spec


def test_a_request_from_before_cases_reads_as_one_case():
    record = {
        "schema": "v1", "name": "brave-otter", "created_at": "2026-01-01",
        "forked_from": "shy-otter",
        "sample_type": "mock::reads", "target_types": ["mock::bam"],
        "transform_libraries": [], "resource_libraries": [],
    }
    (case,) = case_records(record)
    assert case == {"sample_type": "mock::reads", "target_types": ["mock::bam"]}
    assert Spec.Unpack(case, input_library="input.xgdb").name == "case"


def test_both_doors_reach_the_same_plan(tmp_path: Path):
    samples, tr_lib = _setup(tmp_path)

    from_refs = Plan(
        [Spec(input_library=str(samples.location), target_types=["mock::bam"],
              sample_type="mock::assembly")],
        transform_libraries=[str(tr_lib.location)],
    )

    reloaded = type(samples).Load(samples.location)
    from_views = plan_views(
        [CaseViews("case", list(reloaded.AsSamples("mock::assembly")), [], ["mock::bam"])],
        resources=[], transforms=[tr_lib],
    )
    assert from_refs.plan._key == from_views.plan._key


def test_an_empty_target_list_is_refused(tmp_path: Path):
    samples, tr_lib = _setup(tmp_path)
    with pytest.raises(AssertionError, match="can not be empty"):
        Plan([Spec(input_library=samples)], transform_libraries=[tr_lib])


def test_two_cases_need_distinct_names(tmp_path: Path):
    samples, tr_lib = _setup(tmp_path)
    case = Spec(input_library=samples, target_types=["mock::bam"])
    with pytest.raises(AssertionError, match="name each case"):
        Plan([case, case], transform_libraries=[tr_lib])


def test_the_input_library_is_listed_once_when_it_is_also_shared(tmp_path: Path):
    samples, tr_lib = _setup(tmp_path)
    shared = Path("shared.fa")
    (samples.location / shared).write_text(">c\nACGT\n", encoding="utf-8")
    samples.AddItem(shared, "mock::sample_metadata")
    samples.Save()

    task = Plan(
        [Spec(input_library=str(samples.location), target_types=["mock::bam"],
              sample_type="mock::assembly", shared_input_paths=[str(shared)])],
        transform_libraries=[str(tr_lib.location)],
    )
    locations = [lib.location for lib in task.data_libraries]
    assert len(locations) == len(set(locations)), locations


def test_one_endpoint_fills_a_target_and_the_target_anchored_to_it(tmp_path: Path):
    samples, tr_lib = _setup(tmp_path)
    task = Plan(
        [Spec(input_library=samples,
              target_types=["mock::bam", {"type": "mock::bam", "parents": [0]}])],
        transform_libraries=[tr_lib],
    )
    assert task.ok, task.plan.dropped_targets
    assert len(task.plan.steps) == 1


def test_two_cases_plan_once_and_share_the_step_both_need(tmp_path: Path):
    samples, tr_lib = _setup(
        tmp_path, n_samples=4, shared_root=True,
        transforms=mt.identity_transform("mock::assembly", "mock::bam")
        | mt.identity_transform("mock::bam", "mock::h1"),
    )
    location = str(samples.location)
    task = Plan(
        [
            Spec(input_library=location, name="short", sample_type="mock::assembly",
                 input_paths=["sample_00/assembly.txt", "sample_01/assembly.txt"],
                 target_types=["mock::bam"]),
            Spec(input_library=location, name="long", sample_type="mock::assembly",
                 input_paths=["sample_02/assembly.txt", "sample_03/assembly.txt"],
                 target_types=["mock::bam", {"type": "mock::h1", "parents": [0]}]),
        ],
        transform_libraries=[tr_lib],
    )
    assert task.ok, task.plan.dropped_targets
    assert sorted(task.plan.cases) == ["long", "short"]
    by_product = {
        next(i.dtype_name for g in s.produces for i in g): sorted(s.cases)
        for s in task.plan.steps
    }
    assert by_product == {"mock::bam": ["long", "short"], "mock::h1": ["long"]}
    assert len(task.data_libraries) == 1

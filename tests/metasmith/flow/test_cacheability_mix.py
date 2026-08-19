from __future__ import annotations

from pathlib import Path

import pytest

from metasmith.testing.contract_runtime import ContractRuntime

from tests.metasmith.flow.conftest import (
    BuiltPlan,
    build_batched_plan,
    build_mixed_cacheability_plan,
    run_and_load,
)


def test_c1_non_cacheable_in_middle_chain(tmp_path, monkeypatch):
    bp = build_mixed_cacheability_plan(tmp_path)
    assert isinstance(bp, BuiltPlan)
    task = bp.as_task()

    runtime = ContractRuntime(tmp_path / "cr", monkeypatch)
    compiled = runtime.stage(task)
    report = runtime.validate(compiled)

    assert report.nf_compiles, f"workflow.nf failed to compile: {report.errors}"
    assert report.produces_match_plan, (
        f"produces did not match plan: {report.errors}"
    )
    assert report.cacheable_flags_propagated, (
        f"cacheable flag did not propagate to step meta: {report.errors}"
    )

    flags = [bool(getattr(s.transform, "cacheable", True)) for s in compiled.task.plan.steps]
    assert flags == [True, False, True], (
        f"mixed_cacheability builder did not yield T-F-T cacheable flags: {flags}"
    )


def test_c2_re_run_only_cacheable_hits(tmp_path, virtual_runtime):
    bp = build_mixed_cacheability_plan(tmp_path)
    flags = [bool(getattr(s.transform, "cacheable", True)) for s in bp.plan.steps]
    assert flags == [True, False, True]
    transform_keys = [s.transform.GetKey() for s in bp.plan.steps]
    cacheable_keys = {transform_keys[0], transform_keys[2]}
    non_cacheable_key = transform_keys[1]

    _task, lib1 = run_and_load(virtual_runtime, bp)
    promoted_first = lib1.find_invocations(status="promoted")
    promoted_keys_first = {e.transform_key for e in promoted_first}
    assert promoted_keys_first == cacheable_keys, (
        f"first run promoted keys {promoted_keys_first!r}; "
        f"expected exactly the cacheable subset {cacheable_keys!r}"
    )
    assert non_cacheable_key not in promoted_keys_first, (
        f"cacheable=False transform unexpectedly promoted: {non_cacheable_key!r}"
    )

    bp2 = build_mixed_cacheability_plan(tmp_path)
    _task2, lib2 = run_and_load(virtual_runtime, bp2)

    hits_second = lib2.find_invocations(status="hit")
    hit_keys_second = {e.transform_key for e in hits_second}
    assert hit_keys_second, (
        f"expected at least one cacheable step to hit on rerun; "
        f"got hits={hit_keys_second!r}"
    )
    assert hit_keys_second <= cacheable_keys, (
        f"hit keys leaked beyond cacheable set: "
        f"hits={hit_keys_second!r} cacheable={cacheable_keys!r}"
    )
    assert non_cacheable_key not in hit_keys_second, (
        f"cacheable=False transform unexpectedly hit cache: "
        f"key={non_cacheable_key!r} hits={hit_keys_second!r}"
    )


def test_c4_batched_non_cacheable_no_collisions(tmp_path, monkeypatch):
    bp = build_batched_plan(tmp_path, n_inputs=4, batch_size=2)
    task = bp.as_task()
    runtime = ContractRuntime(tmp_path / "cr", monkeypatch)
    compiled = runtime.stage(task)
    report = runtime.validate(compiled)
    assert report.nf_compiles, f"workflow.nf failed to compile: {report.errors}"
    assert report.produces_match_plan, (
        f"produces did not match plan: {report.errors}"
    )

    produced_ids: list[str] = []
    for step in compiled.task.plan.steps:
        for group in step.produces:
            for inst in group:
                produced_ids.append(inst.instance_id)
    assert len(produced_ids) == len(set(produced_ids)), (
        f"per-batch derived instance_ids collided: {produced_ids}"
    )

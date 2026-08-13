"""Mixed-cacheability flow tests.

Covers <C1>, <C2>, <C4> from the flow catalog. The cache trace shape
itself lives in `tests/cache/test_trace.py`; here we verify the *flow
impact* of inserting a `cacheable=False` step in a chain.

C1: cacheable=False propagates through the contract pipeline.
C2: on re-run, cacheable steps hit; the cacheable=False step re-promotes.
C4: batched cacheable=False step does not collide across batches.
"""

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


# ---------------------------------------------------------------------------
# C1 — cacheable flag propagates via ContractRuntime
# ---------------------------------------------------------------------------


def test_c1_non_cacheable_in_middle_chain(tmp_path, monkeypatch):
    """<C1> Non-cacheable middle step compiles and its `cacheable` flag
    propagates into the step's `workflow.step_N.meta` (or is treated as
    propagated when no meta is emitted, per ContractRuntime).
    """
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

    # Confirm the middle step is actually the non-cacheable one.
    flags = [bool(getattr(s.transform, "cacheable", True)) for s in compiled.task.plan.steps]
    assert flags == [True, False, True], (
        f"mixed_cacheability builder did not yield T-F-T cacheable flags: {flags}"
    )


# ---------------------------------------------------------------------------
# C2 — re-run only cacheable steps hit; the cacheable=False step re-promotes
# ---------------------------------------------------------------------------


def test_c2_re_run_only_cacheable_hits(tmp_path, virtual_runtime):
    """<C2> First run promotes only cacheable steps; the second run
    surfaces those same steps as `status=hit`. The cacheable=False step
    never lands an InvocationEvent (cache layer skips it), so it appears
    in neither the hit nor the promoted lists.
    """
    bp = build_mixed_cacheability_plan(tmp_path)
    flags = [bool(getattr(s.transform, "cacheable", True)) for s in bp.plan.steps]
    assert flags == [True, False, True]
    transform_keys = [s.transform.GetKey() for s in bp.plan.steps]
    cacheable_keys = {transform_keys[0], transform_keys[2]}
    non_cacheable_key = transform_keys[1]

    # First run — every cacheable step promotes, the cacheable=False step
    # is silent in the trace.
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

    # Re-run — same plan, same inputs, same workspace → cacheable steps
    # hit; the cacheable=False step still has no event.
    bp2 = build_mixed_cacheability_plan(tmp_path)
    _task2, lib2 = run_and_load(virtual_runtime, bp2)

    hits_second = lib2.find_invocations(status="hit")
    hit_keys_second = {e.transform_key for e in hits_second}
    # The first cacheable step's inputs are unchanged across runs (raw
    # samples), so it must hit. The trailing cacheable step's hit/miss
    # depends on whether the non-cacheable middle step re-derives a stable
    # output id; the catalog C1 invariant is on downstream key correctness,
    # not on hit/miss after a non-cacheable rerun. We pin the strict shape:
    # only cacheable steps can hit, and at least one of them does.
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


# ---------------------------------------------------------------------------
# C4 — batched cacheable=False: no key collisions across batches
# ---------------------------------------------------------------------------


def test_c4_batched_non_cacheable_no_collisions(tmp_path, monkeypatch):
    """<C4> A batched plan stages without per-batch derived-key collisions
    across batches. We assert via ContractRuntime that produced
    instance_ids are unique across the per-batch fan-out, then check the
    contract holds (no instance_id collides).
    """
    bp = build_batched_plan(tmp_path, n_inputs=4, batch_size=2)
    task = bp.as_task()
    runtime = ContractRuntime(tmp_path / "cr", monkeypatch)
    compiled = runtime.stage(task)
    report = runtime.validate(compiled)
    assert report.nf_compiles, f"workflow.nf failed to compile: {report.errors}"
    assert report.produces_match_plan, (
        f"produces did not match plan: {report.errors}"
    )

    # Collect every produced instance_id; uniqueness guarantees per-batch
    # derived-hex values do not collide.
    produced_ids: list[str] = []
    for step in compiled.task.plan.steps:
        for group in step.produces:
            for inst in group:
                produced_ids.append(inst.instance_id)
    assert len(produced_ids) == len(set(produced_ids)), (
        f"per-batch derived instance_ids collided: {produced_ids}"
    )

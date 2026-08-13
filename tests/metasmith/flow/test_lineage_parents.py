"""Lineage-fork-via-parents= flow tests.

Covers <LP2>, <LP3>, <LP4>, <LP5> from the flow catalog. <LP1> is
already pinned by `tests/flow/repro/repro_135_duplicate_producer.py`.

The conftest `build_lineage_fork_plan` skips (mock-shape gap). For
LP2/LP3 we inline a minimal plan via `binner_transforms` — the three
method-tagged binner dtypes (`metabat2_bins`, `maxbin2_bins`,
`concoct_bins`) act as distinct-parent stand-ins because each output
endpoint carries `{"bins", "method:<method>"}` properties, so the
solver picks a distinct producer per target.
"""

from __future__ import annotations

import shutil
import textwrap
from pathlib import Path

import pytest
import yaml

from metasmith.models.libraries import (
    DataInstance,
    DataInstanceLibrary,
    DataInstanceLibraryView,
    DataTypeLibrary,
    TransformInstanceLibrary,
)
from metasmith.models.solver import Endpoint, Transform
from metasmith.models.workflow import WorkflowPlan
from metasmith.testing import mock_transforms as mt

# Reuse internal conftest helpers via direct import — the tests need the
# library + plan scaffolding without parametrising the existing builders.
from tests.metasmith.flow.conftest import (
    _MOCK_TYPE_PROPERTIES,
    _build_samples_lib,
    _build_transform_lib,
    _build_type_lib,
    _make_target_model,
)


# ---------------------------------------------------------------------------
# Helper: build a binner-shape plan that produces metabat2_bins + maxbin2_bins.
# ---------------------------------------------------------------------------


def _build_binner_plan(
    tmp_path: Path,
    *,
    target_bin_types: list[str],
):
    """Build (lib, transforms_lib, plan) for binner_transforms with N targets."""
    types_path = _build_type_lib(tmp_path / "types.yml")
    # The binners consume both `assembly` and `bam`; build the sample lib so
    # both are present with bam descending from assembly (LP lineage is via
    # the bam producer chain).
    lib = DataInstanceLibrary(tmp_path / "samples.xgdb")
    lib.AddTypeLibrary(types_path, namespace="mock")
    sdir = lib.location / "sample_00"
    sdir.mkdir(parents=True, exist_ok=True)
    (sdir / "assembly.fa").write_text(">a\nACGT\n", encoding="utf-8")
    (sdir / "aln.bam").write_text("mock bam", encoding="utf-8")
    asm = lib.AddItem(Path("sample_00/assembly.fa"), "mock::assembly")
    lib.AddItem(Path("sample_00/aln.bam"), "mock::bam", parents=[asm])
    lib.Save()

    tr_lib = _build_transform_lib(
        tmp_path / "tr", types_path, mt.binner_transforms()
    )

    target_model = _make_target_model([_MOCK_TYPE_PROPERTIES[t] for t in target_bin_types])
    given = [[sv] for sv in lib.AsSamples("mock::assembly")]
    plan = WorkflowPlan.Generate(
        given=given,
        transforms=[tr_lib],
        target_names=list(target_bin_types),
        target_model=target_model,
    )
    return lib, tr_lib, plan


def _instance_ids_for_dtype(plan: WorkflowPlan, dtype_substr: str) -> list[str]:
    """Collect produced instance_ids whose dtype_name contains `dtype_substr`."""
    ids: list[str] = []
    for step in plan.steps:
        for group in step.produces:
            for inst in group:
                if dtype_substr in (inst.dtype_name or ""):
                    ids.append(inst.instance_id)
    return ids


# ---------------------------------------------------------------------------
# LP2 — distinct parents (distinct bin-method tags) → distinct instance_ids
# ---------------------------------------------------------------------------


def test_lp2_distinct_parents_distinct_ids(tmp_path):
    """<LP2> Distinct-parent-tagged targets produce distinct instance_ids.

    Two targets (`metabat2_bins`, `maxbin2_bins`) of the structurally-same
    family but distinct method tags trigger two distinct producers; each
    produces a distinct output instance with a distinct instance_id.
    """
    _lib, _tr, plan = _build_binner_plan(
        tmp_path, target_bin_types=["metabat2_bins", "maxbin2_bins"]
    )
    assert isinstance(plan, WorkflowPlan), f"planner did not converge: {plan!r}"

    mb_ids = _instance_ids_for_dtype(plan, "metabat2_bins")
    xb_ids = _instance_ids_for_dtype(plan, "maxbin2_bins")
    assert mb_ids, "no metabat2_bins instance produced"
    assert xb_ids, "no maxbin2_bins instance produced"

    # Distinct parent-typed targets → instance ids cannot collide.
    assert set(mb_ids).isdisjoint(set(xb_ids)), (
        f"instance_ids leaked across distinct-parent targets: "
        f"metabat2={mb_ids} maxbin2={xb_ids}"
    )


# ---------------------------------------------------------------------------
# LP3 — walk_ancestors does not cross-contaminate
# ---------------------------------------------------------------------------


def test_lp3_walk_ancestors_no_cross_contamination(tmp_path):
    """<LP3> Per-target ancestor sets are disjoint at the produced-bin level.

    Each binner's produced instance must not appear in the other binner's
    lineage chain. We compare via the step-level produces sets: the
    metabat2 producer's outputs cannot be in maxbin2's consume set, and
    vice-versa.
    """
    _lib, _tr, plan = _build_binner_plan(
        tmp_path, target_bin_types=["metabat2_bins", "maxbin2_bins"]
    )

    mb_ids = set(_instance_ids_for_dtype(plan, "metabat2_bins"))
    xb_ids = set(_instance_ids_for_dtype(plan, "maxbin2_bins"))

    # Each step's `uses` (input instances) should not include the *other*
    # branch's produced instance_ids — that would indicate cross-contamination
    # of the lineage chain.
    for step in plan.steps:
        used_ids = {inst.instance_id for inst in step.uses}
        # Find which dtype this step ultimately produces.
        produced_dtypes = set()
        for g in step.produces:
            for inst in g:
                produced_dtypes.add(inst.dtype_name or "")
        if any("metabat2_bins" in d for d in produced_dtypes):
            assert used_ids.isdisjoint(xb_ids), (
                f"metabat2 step consumed maxbin2 outputs: {used_ids & xb_ids}"
            )
        if any("maxbin2_bins" in d for d in produced_dtypes):
            assert used_ids.isdisjoint(mb_ids), (
                f"maxbin2 step consumed metabat2 outputs: {used_ids & mb_ids}"
            )


# ---------------------------------------------------------------------------
# LP4 — impossible parents → PlanHint, not silent drop
# ---------------------------------------------------------------------------


def test_lp4_impossible_parents_raises_planhint(tmp_path):
    """<LP4> Requiring a target whose property bag no producer satisfies
    yields a PlanHint with `kind in {unreachable_target, missing_input,
    lineage_mismatch}` rather than a silent empty plan.
    """
    types_path = _build_type_lib(tmp_path / "types.yml")
    samples = _build_samples_lib(tmp_path, types_path, dtype="assembly")
    # Use the binner_transforms library — it produces *_bins types from
    # assembly + bam. Target a type that no transform in this library can
    # synthesize (no `bam` is given, no producer for it exists in the lib).
    tr_lib = _build_transform_lib(
        tmp_path / "tr", types_path, mt.binner_transforms()
    )

    target_model = _make_target_model([_MOCK_TYPE_PROPERTIES["metabat2_bins"]])
    given = [[sv] for sv in samples.AsSamples("mock::assembly")]
    plan = WorkflowPlan.Generate(
        given=given,
        transforms=[tr_lib],
        target_names=["metabat2_bins"],
        target_model=target_model,
    )

    # Planner returns a (typically empty) plan decorated with hints — not
    # a raise. The hint must surface the failure mode explicitly.
    assert isinstance(plan, WorkflowPlan)
    assert plan.hints, (
        f"expected non-empty PlanHint list for unreachable target, "
        f"got {plan.hints!r}; steps={len(plan.steps)}"
    )
    allowed_kinds = {"unreachable_target", "missing_input", "lineage_mismatch"}
    seen_kinds = {h.kind for h in plan.hints}
    assert seen_kinds & allowed_kinds, (
        f"no PlanHint of an expected kind: got {seen_kinds!r}, "
        f"expected any of {allowed_kinds!r}"
    )


# ---------------------------------------------------------------------------
# LP5 — WithDType preserves instance_id (relocated assertion)
# ---------------------------------------------------------------------------


def test_lp5_with_dtype_preserves_id(tmp_path):
    """<LP5> `DataInstance.WithDType(new_ep)` retypes without changing the
    instance_id. The existing test in test_lineage_roundtrip.py covers the
    lineage-retyping case; this asserts the same invariant directly on a
    raw retype with a distinct property bag.
    """
    lib = DataInstanceLibrary(tmp_path / "samples.xgdb")
    types = DataTypeLibrary()
    types["assembly"] = Endpoint(properties={"assembly"})
    types["assembly_alt"] = Endpoint(properties={"assembly", "alt"})
    lib.AddTypeLibrary(namespace="mock", lib=types)
    (lib.location / "asm.fa").write_text(">c\nACGT\n", encoding="utf-8")
    lib.AddItem(path=Path("asm.fa"), dtype="mock::assembly")

    inst = lib.Get(Path("asm.fa"))
    new_ep = types["assembly_alt"]
    retyped = inst.WithDType(new_ep)
    assert retyped.instance_id == inst.instance_id, (
        f"WithDType mutated instance_id: {inst.instance_id} -> {retyped.instance_id}"
    )
    assert retyped.dtype.key != inst.dtype.key, (
        "WithDType did not actually change the dtype.key — test is moot"
    )

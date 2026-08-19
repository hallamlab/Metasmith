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

from tests.metasmith.flow.conftest import (
    _MOCK_TYPE_PROPERTIES,
    _build_samples_lib,
    _build_transform_lib,
    _build_type_lib,
    _make_target_model,
)


def _build_binner_plan(
    tmp_path: Path,
    *,
    target_bin_types: list[str],
):
    types_path = _build_type_lib(tmp_path / "types.yml")
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
    ids: list[str] = []
    for step in plan.steps:
        for group in step.produces:
            for inst in group:
                if dtype_substr in (inst.dtype_name or ""):
                    ids.append(inst.instance_id)
    return ids


def test_lp2_distinct_parents_distinct_ids(tmp_path):
    _lib, _tr, plan = _build_binner_plan(
        tmp_path, target_bin_types=["metabat2_bins", "maxbin2_bins"]
    )
    assert isinstance(plan, WorkflowPlan), f"planner did not converge: {plan!r}"

    mb_ids = _instance_ids_for_dtype(plan, "metabat2_bins")
    xb_ids = _instance_ids_for_dtype(plan, "maxbin2_bins")
    assert mb_ids, "no metabat2_bins instance produced"
    assert xb_ids, "no maxbin2_bins instance produced"

    assert set(mb_ids).isdisjoint(set(xb_ids)), (
        f"instance_ids leaked across distinct-parent targets: "
        f"metabat2={mb_ids} maxbin2={xb_ids}"
    )


def test_lp3_walk_ancestors_no_cross_contamination(tmp_path):
    _lib, _tr, plan = _build_binner_plan(
        tmp_path, target_bin_types=["metabat2_bins", "maxbin2_bins"]
    )

    mb_ids = set(_instance_ids_for_dtype(plan, "metabat2_bins"))
    xb_ids = set(_instance_ids_for_dtype(plan, "maxbin2_bins"))

    for step in plan.steps:
        used_ids = {inst.instance_id for inst in step.uses}
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


def test_lp4_impossible_parents_raises_planhint(tmp_path):
    types_path = _build_type_lib(tmp_path / "types.yml")
    samples = _build_samples_lib(tmp_path, types_path, dtype="assembly")
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


def test_lp5_with_dtype_preserves_id(tmp_path):
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

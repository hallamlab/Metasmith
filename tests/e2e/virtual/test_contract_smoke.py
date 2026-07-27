"""Smoke tests for `ContractRuntime` and the new `mock_transforms` shapes."""

from __future__ import annotations

from pathlib import Path

import pytest

from metasmith.constants import AgentPaths
from metasmith.env import Runtime
from metasmith.models.libraries import (
    DataInstanceLibrary,
    DataTypeLibrary,
    TransformInstanceLibrary,
)
from metasmith.models.solver import Endpoint, Transform
from metasmith.models.workflow import WorkflowPlan, WorkflowTask
from metasmith.testing.contract_runtime import ContractRuntime
from metasmith.testing.mock_transforms import (
    group_then_unfold,
    identity_transform,
    multi_slot_producer,
)
from metasmith.testing.plan_oracle import PlanExecutionOracle

from .conftest import create_transform_library


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _configure_paths(monkeypatch, home: Path) -> None:
    monkeypatch.setattr(AgentPaths, "HOME_ROOT", home)
    monkeypatch.setattr(AgentPaths, "WORK_ROOT", home / "_ws")


def _make_types_file(tmp_path: Path, names: dict[str, set[str]]) -> Path:
    """Write a DataTypeLibrary file with each name mapped to its property set."""
    types = DataTypeLibrary()
    for name, props in names.items():
        types[name] = Endpoint(properties=props or {name})
    p = tmp_path / "types.yml"
    types.Save(p)
    return p


def _make_samples(
    tmp_path: Path, types_path: Path, namespace: str = "mock"
) -> DataInstanceLibrary:
    lib = DataInstanceLibrary(tmp_path / "samples.xgdb")
    lib.AddTypeLibrary(types_path, namespace=namespace)
    for i in range(2):
        sid = f"sample_{i:02d}"
        sdir = lib.location / sid
        sdir.mkdir(parents=True, exist_ok=True)
        (sdir / "metadata.json").write_text(f'{{"id":"{sid}"}}', encoding="utf-8")
        (sdir / "reads.fq").write_text(f">read_{i}\nACGT\n", encoding="utf-8")
        (sdir / "assembly.fa").write_text(
            f">contig_{i}\nACGTACGT\n", encoding="utf-8"
        )
        m = lib.AddItem(Path(f"{sid}/metadata.json"), f"{namespace}::sample_metadata")
        r = lib.AddItem(Path(f"{sid}/reads.fq"), f"{namespace}::reads", parents=[m])
        lib.AddItem(Path(f"{sid}/assembly.fa"), f"{namespace}::assembly", parents=[r])
    lib.Save()
    return lib


# ---------------------------------------------------------------------------
# A. ContractRuntime: 2-step linear identity plan
# ---------------------------------------------------------------------------


def test_contract_runtime_two_step_linear_identity(tmp_path, monkeypatch):
    """2-step linear plan stages cleanly and the report passes core checks."""
    home = tmp_path / "contract_home"
    home.mkdir(parents=True, exist_ok=True)
    _configure_paths(monkeypatch, home)

    types_path = _make_types_file(
        tmp_path,
        {
            "sample_metadata": {"sample_metadata"},
            "reads": {"reads"},
            "assembly": {"assembly"},
            "intermediate": {"intermediate"},
            "final": {"final"},
        },
    )
    samples = _make_samples(tmp_path, types_path)

    transforms = (
        identity_transform("mock::assembly", "mock::intermediate")
        | identity_transform("mock::intermediate", "mock::final")
    )
    tr_lib = create_transform_library(tmp_path / "tr", types_path, transforms)

    given = [[sv] for sv in samples.AsSamples("mock::assembly")]
    target_model = Transform()
    target_model.AddRequirement(properties={"final"})
    plan = WorkflowPlan.Generate(
        given=given,
        transforms=[tr_lib],
        target_names=["final_target"],
        target_model=target_model,
    )
    assert isinstance(plan, WorkflowPlan), f"planner did not converge: {plan!r}"
    assert len(plan.steps) == 2

    task = WorkflowTask(
        ok=True,
        plan=plan,
        data_libraries=[samples],
        transform_libraries=[tr_lib],
    )

    runtime = ContractRuntime(tmp_path, monkeypatch)
    compiled = runtime.stage(task)
    report = runtime.validate(compiled)

    assert report.nf_compiles is True, f"nf compile failed: {report.errors}"
    assert report.produces_match_plan is True, (
        f"produces mismatch: {report.errors}"
    )
    assert report.cacheable_flags_propagated is True, (
        f"cacheable propagation failed: {report.errors}"
    )
    # Every step's inputs should be reachable from upstream channels.
    for step in compiled.task.plan.steps:
        key = f"step_{step.order}"
        assert key in report.step_inputs_reachable

    # Oracle bridge: same result via PlanExecutionOracle.
    oracle = PlanExecutionOracle(compiled.task)
    report2 = oracle.validate_contract_only(compiled.task.plan, compiled)
    assert report2.nf_compiles is True
    assert report2.produces_match_plan is True


# ---------------------------------------------------------------------------
# B. multi_slot_producer: 3 distinct dtypes in the produced library
# ---------------------------------------------------------------------------


def test_multi_slot_producer_emits_three_distinct_dtypes(tmp_path):
    """`multi_slot_producer(slots=3)` declares 3 slots, each its own dtype."""
    types_path = _make_types_file(
        tmp_path,
        {
            "sample_metadata": {"sample_metadata"},
            "reads": {"reads"},
            "assembly": {"assembly"},
            "slot_0": {"slot_0"},
            "slot_1": {"slot_1"},
            "slot_2": {"slot_2"},
        },
    )
    transforms = multi_slot_producer(slots=3)
    tr_lib = create_transform_library(tmp_path / "tr_multi", types_path, transforms)

    # Walk all transforms in the library; there should be exactly one,
    # producing three branches each with one distinct dtype.
    found = list(tr_lib.IterateTransforms())
    assert len(found) == 1, f"expected one transform, got {len(found)}"
    _, inst = found[0]
    produces = inst.model.produces
    assert len(produces) == 3, f"expected 3 product groups, got {len(produces)}"
    dtype_keys: set[str] = set()
    for group in produces:
        assert len(group) == 1, "each slot should have one dep"
        dtype_keys.add(group[0].key)
    assert len(dtype_keys) == 3, (
        f"expected 3 distinct dtype keys, got {dtype_keys}"
    )


# ---------------------------------------------------------------------------
# C. group_then_unfold: the two transforms compose
# ---------------------------------------------------------------------------


def test_group_then_unfold_pair_composes(tmp_path):
    """`group_then_unfold()` returns two transforms wired through `mock::grouped`."""
    types_path = _make_types_file(
        tmp_path,
        {
            "sample_metadata": {"sample_metadata"},
            "reads": {"reads"},
            "assembly": {"assembly"},
            "grouped": {"grouped"},
            "unfolded": {"unfolded"},
        },
    )
    transforms = group_then_unfold()
    assert set(transforms.keys()) == {"group_aggregate", "unfold_batch"}
    tr_lib = create_transform_library(
        tmp_path / "tr_group", types_path, transforms
    )
    found = list(tr_lib.IterateTransforms())
    assert len(found) == 2
    by_name = {tr.name: tr for _, tr in found}
    agg = by_name["group_aggregate"]
    uf = by_name["unfold_batch"]

    # group_aggregate produces mock::grouped
    agg_outs = {dep.key for g in agg.model.produces for dep in g}
    # unfold_batch consumes mock::grouped
    uf_ins = {dep.key for dep in uf.model.requires}
    assert agg_outs & uf_ins, (
        f"group_aggregate outputs {agg_outs} must intersect "
        f"unfold_batch inputs {uf_ins}"
    )

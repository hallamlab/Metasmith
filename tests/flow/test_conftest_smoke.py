"""Smoke gate for tests/flow/conftest.py.

One tiny test per builder family: each builder must return either a
non-None BuiltPlan or raise pytest.skip cleanly (the skip path is
intentional — downstream tests get to collect even when a stimulus
shape is not yet wired). One end-to-end assertion confirms
`run_and_load` works on the simplest linear plan.
"""

from __future__ import annotations

import pytest

from .conftest import (
    BuiltPlan,
    build_5hop_dag_plan,
    build_batched_plan,
    build_branching_plan,
    build_branching_with_failure_plan,
    build_dead_output_plan,
    build_empty_plan,
    build_fan_out_plan,
    build_group_then_split_plan,
    build_lineage_fork_plan,
    build_linear_plan,
    build_mixed_cacheability_plan,
    build_multi_input_plan,
    run_and_load,
)


# ---------------------------------------------------------------------------
# Builder smoke checks — every builder either returns BuiltPlan or skips.
# ---------------------------------------------------------------------------


def test_build_linear_plan_returns_plan(tmp_path):
    bp = build_linear_plan(tmp_path, n_steps=2)
    assert isinstance(bp, BuiltPlan)
    assert bp.plan is not None
    assert len(bp.plan.steps) == 2


def test_build_linear_plan_single_step(tmp_path):
    bp = build_linear_plan(tmp_path, n_steps=1)
    assert isinstance(bp, BuiltPlan)
    assert bp.plan is not None


def test_build_multi_input_plan_returns_plan(tmp_path):
    bp = build_multi_input_plan(tmp_path, slots=2)
    assert isinstance(bp, BuiltPlan)
    assert bp.plan is not None


def test_build_branching_plan_returns_plan(tmp_path):
    bp = build_branching_plan(tmp_path, fanout=2)
    assert isinstance(bp, BuiltPlan)
    assert bp.plan is not None


def test_build_branching_with_failure_plan_returns_plan(tmp_path):
    bp = build_branching_with_failure_plan(tmp_path)
    assert isinstance(bp, BuiltPlan)
    assert bp.plan is not None


def test_build_fan_out_plan_returns_plan(tmp_path):
    bp = build_fan_out_plan(tmp_path, n_slots=2)
    assert isinstance(bp, BuiltPlan)
    assert bp.plan is not None


def test_build_batched_plan_returns_plan(tmp_path):
    bp = build_batched_plan(tmp_path, n_inputs=3, batch_size=2)
    assert isinstance(bp, BuiltPlan)
    assert bp.plan is not None


def test_build_group_then_split_plan_returns_plan(tmp_path):
    bp = build_group_then_split_plan(tmp_path)
    assert isinstance(bp, BuiltPlan)
    assert bp.plan is not None


def test_build_lineage_fork_plan_skips_cleanly(tmp_path):
    # Expected to skip until a parents= producer shape lands in mock_transforms.
    with pytest.raises(pytest.skip.Exception):
        build_lineage_fork_plan(tmp_path, parent_count=2)


def test_build_mixed_cacheability_plan_returns_plan(tmp_path):
    bp = build_mixed_cacheability_plan(tmp_path)
    assert isinstance(bp, BuiltPlan)
    assert bp.plan is not None


def test_build_empty_plan_skips_cleanly(tmp_path):
    with pytest.raises(pytest.skip.Exception):
        build_empty_plan(tmp_path)


def test_build_dead_output_plan_skips_cleanly(tmp_path):
    with pytest.raises(pytest.skip.Exception):
        build_dead_output_plan(tmp_path)


def test_build_5hop_dag_plan_returns_plan(tmp_path):
    bp = build_5hop_dag_plan(tmp_path)
    assert isinstance(bp, BuiltPlan)
    assert bp.plan is not None
    assert len(bp.plan.steps) == 5


# ---------------------------------------------------------------------------
# End-to-end gate: run_and_load on the simplest linear plan.
# ---------------------------------------------------------------------------


def test_run_and_load_on_linear_plan(tmp_path, virtual_runtime):
    bp = build_linear_plan(tmp_path, n_steps=2)
    task, lib = run_and_load(virtual_runtime, bp)
    assert task is not None
    assert lib is not None
    # Telemetry surface is reachable.
    summary = lib.summary()
    assert "counts" in summary

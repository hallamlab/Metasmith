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


def test_run_and_load_on_linear_plan(tmp_path, virtual_runtime):
    bp = build_linear_plan(tmp_path, n_steps=2)
    task, lib = run_and_load(virtual_runtime, bp)
    assert task is not None
    assert lib is not None
    summary = lib.summary()
    assert "counts" in summary

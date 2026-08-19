from __future__ import annotations

import inspect

import pytest

from metasmith.models.workflow import WorkflowPlan
from metasmith.ops import workflow as op_workflow


EXPECTED = {"lib", "containers", "env"}


@pytest.mark.parametrize("fn", [WorkflowPlan.BuildDAG, WorkflowPlan.RenderDAG])
def test_plan_entry_points_blacklist_the_plumbing(fn):
    default = inspect.signature(fn).parameters["blacklist_namespaces"].default
    assert set(default) == EXPECTED, f"{fn.__qualname__} default drifted"


def test_ops_render_dag_falls_back_to_the_same_set():
    src = inspect.getsource(op_workflow.render_dag)
    for ns in EXPECTED:
        assert f'"{ns}"' in src, (
            f"ops.workflow.render_dag's fallback set is missing {ns!r}"
        )


def test_a_real_plan_renders_no_env_nodes(tmp_path):
    from tests.metasmith.cache._cache_harness import (
        build_samples_library,
        build_transform_library,
        build_types_library,
        build_workflow_task,
        identity_transform_code,
    )

    types_path = build_types_library(tmp_path, ("seed", "out"))
    samples = build_samples_library(tmp_path, types_path, count=1, input_type="seed")
    tr_lib = build_transform_library(
        tmp_path / "tr", types_path, {"tr": identity_transform_code("tr", "seed", "out")}
    )
    task = build_workflow_task(
        samples, tr_lib, sample_type="seed", target_specs=[("out_target", {"out"})]
    )

    text = task.plan.BuildDAG().to_text()
    assert "env::" not in text, f"env plumbing rendered into the DAG:\n{text}"

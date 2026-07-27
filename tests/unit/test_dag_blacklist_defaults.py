"""Every DAG entry point must hide the same plumbing namespaces.

`env` joined `lib` and `containers` on the blacklist when environments
became declared dependencies -- an environment is a dependency like any
other, so without it every plan DAG grows an `env::*` node per step. The
new layout engine arrived from a branch that predated `env` and replaced
all three defaults with its own two-element set, which merges perfectly
cleanly and starts rendering plumbing into every diagram.

Three defaults have to agree and nothing else makes them: `BuildDAG`,
`RenderDAG`, and `ops.workflow.render_dag`.
"""

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
    """The ops layer re-states the default rather than deferring to the model.

    That is the whole reason this test exists: a caller passing
    `blacklist_namespaces=None` gets the ops copy, not the signature's.
    """
    src = inspect.getsource(op_workflow.render_dag)
    for ns in EXPECTED:
        assert f'"{ns}"' in src, (
            f"ops.workflow.render_dag's fallback set is missing {ns!r}"
        )


def test_a_real_plan_renders_no_env_nodes(tmp_path):
    """End to end: no `env::` token survives a default render of a real plan.

    The signature checks above catch the common regression; this catches a
    default that is right but no longer consulted.
    """
    from tests.cache._cache_harness import (
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

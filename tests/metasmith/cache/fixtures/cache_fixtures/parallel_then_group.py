"""Parallel-then-group fixture: 3 parallel A_i -> grouped B -> C.

Exercises the synthetic-channel-through-o.post -> o.group reduction path
that is the load-bearing topology pin for Critic E#1 / G4. When step A is
cached and emits a synthetic Channel.of(...), each per-sample tuple must
still re-enter o.post() so index_history populates and the downstream
grouping completes.

Topology:
  * one shared `root` instance is the declared lineage parent of every
    per-sample seed, so a `group_by=root` reduction collapses all three
    parallel branches into one invocation.
  * trA: seed -> step_a (3 parallel invocations, one per seed)
  * trB: root + step_a (parents={root}) -> step_b, group_by=root (1 inv.)
  * trC: step_b -> step_c (1 invocation)
"""

from __future__ import annotations

from pathlib import Path

from metasmith.models.workflow import WorkflowTask

from tests.metasmith.cache._cache_harness import (
    build_samples_library,
    build_transform_library,
    build_types_library,
    build_workflow_task,
    grouping_transform_code,
    identity_transform_code,
)


TYPE_NAMES = ("root", "seed", "step_a", "step_b", "step_c")


def build_task(tmp_path: Path) -> WorkflowTask:
    types_path = build_types_library(tmp_path, TYPE_NAMES)
    samples = build_samples_library(
        tmp_path,
        types_path,
        count=3,
        input_type="seed",
        shared_root_type="root",
    )
    transforms = {
        "trA": identity_transform_code("trA", "seed", "step_a"),
        "trB": grouping_transform_code(
            "trB", root_type="root", input_type="step_a", output_type="step_b"
        ),
        "trC": identity_transform_code("trC", "step_b", "step_c"),
    }
    tr_lib = build_transform_library(tmp_path / "tr", types_path, transforms)

    return build_workflow_task(
        samples,
        tr_lib,
        sample_type="seed",
        target_specs=[("step_c_target", {"step_c"})],
    )

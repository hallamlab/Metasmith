from __future__ import annotations

from pathlib import Path

from metasmith.models.workflow import WorkflowTask

from tests.metasmith.cache._cache_harness import (
    build_samples_library,
    build_transform_library,
    build_types_library,
    build_workflow_task,
    identity_transform_code,
)


TYPE_NAMES = ("seed", "step_a", "step_b", "step_c")


def build_task(tmp_path: Path, n_samples: int = 1) -> WorkflowTask:
    types_path = build_types_library(tmp_path, TYPE_NAMES)
    samples = build_samples_library(
        tmp_path, types_path, count=n_samples, input_type="seed"
    )
    transforms = {
        "trA": identity_transform_code("trA", "seed", "step_a"),
        "trB": identity_transform_code("trB", "step_a", "step_b"),
        "trC": identity_transform_code("trC", "step_b", "step_c"),
    }
    tr_lib = build_transform_library(tmp_path / "tr", types_path, transforms)

    return build_workflow_task(
        samples,
        tr_lib,
        sample_type="seed",
        target_specs=[("step_c_target", {"step_c"})],
    )

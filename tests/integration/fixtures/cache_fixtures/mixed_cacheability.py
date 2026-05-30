"""Mixed-cacheability fixture: A (True) -> B (False) -> C (True).

Exercises that an uncacheable middle transform still propagates its outputs
into a downstream cacheable transform's lineage key. Today (pre-S4) all
transforms have the same caching posture so this fixture's behavior is
identical to linear_3step; the test difference shows up once the
`cacheable` field exists.

On main, the fixture is constructed without any opt-out annotation (the
`cacheable` field doesn't exist yet). Once S4 lands, tests/integration/
test_cache_execution.py::test_cacheable_false_skips_publishDir will rebuild
this fixture and toggle trB.cacheable = False before staging.
"""

from __future__ import annotations

from pathlib import Path

from metasmith.models.workflow import WorkflowTask

from tests.integration._cache_harness import (
    build_samples_library,
    build_transform_library,
    build_types_library,
    build_workflow_task,
    identity_transform_code,
)


TYPE_NAMES = ("seed", "step_a", "step_b", "step_c")


def build_task(tmp_path: Path) -> WorkflowTask:
    types_path = build_types_library(tmp_path, TYPE_NAMES)
    samples = build_samples_library(
        tmp_path, types_path, count=1, input_type="seed"
    )
    transforms = {
        "trA": identity_transform_code("trA", "seed", "step_a"),
        "trB": identity_transform_code(
            "trB", "step_a", "step_b", cacheable=False
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

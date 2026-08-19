from __future__ import annotations

from pathlib import Path

from tests.metasmith.cache._cache_harness import (
    build_samples_library,
    build_transform_library,
    build_types_library,
    build_workflow_task,
    capture_run,
    clear_trace,
    identity_transform_code,
)


TYPE_NAMES = ("seed", "mid", "out")


def _build_pipeline_task(root: Path, fork_id: str | None = None):
    types_path = build_types_library(root, TYPE_NAMES)
    samples = build_samples_library(root, types_path, count=2, input_type="seed")
    if fork_id is not None:
        samples.fork_id = fork_id
        samples.Save()
    transforms = {
        "trA": identity_transform_code("trA", "seed", "mid"),
        "trB": identity_transform_code("trB", "mid", "out"),
    }
    tr_lib = build_transform_library(root / "tr", types_path, transforms)
    return build_workflow_task(
        samples,
        tr_lib,
        sample_type="seed",
        target_specs=[("out_target", {"out"})],
    )


def test_forked_library_misses_a_cache_its_twin_would_hit(tmp_path, virtual_runtime):
    task_a = _build_pipeline_task(tmp_path / "a")
    snap_a = capture_run(virtual_runtime, task_a)
    assert snap_a.executed_steps, "run A executed zero steps (bad fixture)"

    task_control = _build_pipeline_task(tmp_path / "control")
    assert task_control.GetKey() == task_a.GetKey()
    clear_trace(virtual_runtime)
    assert capture_run(virtual_runtime, task_control).executed_steps == ()

    task_b = _build_pipeline_task(tmp_path / "b", fork_id="deadbeef")
    assert task_b.GetKey() != task_a.GetKey(), (
        "a fork left the task key untouched; the fork id is not reaching "
        "instance ids, so it cannot reach cache keys either"
    )

    clear_trace(virtual_runtime)
    snap_b = capture_run(virtual_runtime, task_b)
    assert snap_b.executed_steps == snap_a.executed_steps, (
        f"forked run executed {snap_b.executed_steps} but the unforked run "
        f"executed {snap_a.executed_steps}; the fork served cached output"
    )

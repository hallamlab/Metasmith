from __future__ import annotations

import textwrap
from pathlib import Path

from tests.metasmith.cache._cache_harness import (
    build_samples_library,
    build_transform_library,
    build_types_library,
    build_workflow_task,
    capture_run,
    clear_trace,
)

TYPE_NAMES = ("seed", "out")


def _variant_transform_code(body_marker: str) -> str:
    return textwrap.dedent(
        f"""
        from pathlib import Path
        from metasmith.models.libraries import (
            TransformInstanceLibrary,
            TransformInstance,
            ExecutionContext,
            ExecutionResult,
        )
        from metasmith.models.solver import Transform

        lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
        model = Transform()
        dep = model.AddRequirement(lib.GetType("cf::seed"))
        out = model.AddProduct(lib.GetType("cf::out"))

        def protocol(context: ExecutionContext):
            inp = context.Input(dep)
            payload = inp.local.read_text() if inp.local.exists() else "no input"
            out_path = Path("out_out.txt")
            out_path.write_text("BODY={body_marker}\\n" + payload)
            return ExecutionResult(manifest=[{{out: out_path}}], success=True)

        TransformInstance(protocol=protocol, model=model, group_by=dep, cacheable=True)
        """
    )


def _build_task(root: Path, body_marker: str, tr_name: str):
    types_path = build_types_library(root, TYPE_NAMES)
    samples = build_samples_library(root, types_path, count=1, input_type="seed")
    tr_lib = build_transform_library(
        root / "tr", types_path, {tr_name: _variant_transform_code(body_marker)}
    )
    return build_workflow_task(
        samples, tr_lib, sample_type="seed", target_specs=[("out_target", {"out"})]
    )


def test_identical_protocol_still_hits_cross_run(tmp_path, virtual_runtime):
    task_a = _build_task(tmp_path / "a", "SAME", tr_name="tr_hit")
    snap_a = capture_run(virtual_runtime, task_a)
    assert snap_a.executed_steps, "run A executed zero steps (bad fixture)"

    task_b = _build_task(tmp_path / "b", "SAME", tr_name="tr_hit")
    clear_trace(virtual_runtime)
    snap_b = capture_run(virtual_runtime, task_b)
    assert snap_b.executed_steps == (), (
        "identical protocol over identical inputs should be a full cross-run "
        f"cache hit; instead re-executed {snap_b.executed_steps}"
    )


def test_changed_protocol_body_misses(tmp_path, virtual_runtime):
    task_a = _build_task(tmp_path / "a", "ONE", tr_name="tr_one")
    snap_a = capture_run(virtual_runtime, task_a)
    assert snap_a.executed_steps, "run A executed zero steps (bad fixture)"

    task_b = _build_task(tmp_path / "b", "TWO", tr_name="tr_two")
    clear_trace(virtual_runtime)
    snap_b = capture_run(virtual_runtime, task_b)
    assert snap_b.executed_steps != (), (
        "changed protocol body false-hit run A's cache (executed 0 steps); "
        "the lineage signature must fold in protocol identity so an edited "
        "transform re-executes instead of serving stale output"
    )

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


def _batched_transform_code(tr_name: str, *, batch_size: int) -> str:
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
            out_path.write_text("structural\\n" + payload)
            return ExecutionResult(manifest=[{{out: out_path}}], success=True)

        TransformInstance(
            protocol=protocol, model=model, group_by=dep,
            batch_size={batch_size}, cacheable=True,
        )
        """
    )


def _build_task(root: Path, *, batch_size: int, tr_name: str):
    types_path = build_types_library(root, TYPE_NAMES)
    samples = build_samples_library(root, types_path, count=2, input_type="seed")
    tr_lib = build_transform_library(
        root / "tr",
        types_path,
        {tr_name: _batched_transform_code(tr_name, batch_size=batch_size)},
    )
    return build_workflow_task(
        samples, tr_lib, sample_type="seed", target_specs=[("out_target", {"out"})]
    )


def test_same_batch_size_hits_cross_run(tmp_path, virtual_runtime):
    task_a = _build_task(tmp_path / "a", batch_size=1, tr_name="tr_bs_ctl")
    snap_a = capture_run(virtual_runtime, task_a)
    assert snap_a.executed_steps, "run A executed zero steps (bad fixture)"

    task_b = _build_task(tmp_path / "b", batch_size=1, tr_name="tr_bs_ctl")
    clear_trace(virtual_runtime)
    snap_b = capture_run(virtual_runtime, task_b)
    assert snap_b.executed_steps == (), (
        "identical batch_size over identical inputs should be a full cross-run "
        f"hit; instead re-executed {snap_b.executed_steps}"
    )


def test_changed_batch_size_misses(tmp_path, virtual_runtime):
    task_a = _build_task(tmp_path / "a", batch_size=1, tr_name="tr_bs_one")
    snap_a = capture_run(virtual_runtime, task_a)
    assert snap_a.executed_steps, "run A executed zero steps (bad fixture)"

    task_b = _build_task(tmp_path / "b", batch_size=2, tr_name="tr_bs_two")
    clear_trace(virtual_runtime)
    snap_b = capture_run(virtual_runtime, task_b)
    assert snap_b.executed_steps != (), (
        "a batch_size change false-hit the prior run's cache (executed 0 "
        "steps); structural transform parameters must enter the lineage key "
        "via the definition-file source digest"
    )

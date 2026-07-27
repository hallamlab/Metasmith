"""Protocol-body identity in the lineage cache key (F1 regression).

Pre-R5 the cache signature was the transform's I/O type topology only
(`signature = str(model._hash)`), so editing a transform's protocol — or
swapping in a different tool with the same declared in/out types — did NOT
change the cache_key. A cross-run rerun then short-circuited and served the
*old* protocol's cached output (a silent false hit).

R5 folds a digest of the transform definition-file bytes
(`_protocol_source_hash`) into the lineage signature. These tests pin both
directions:

  * identical protocol bytes across two independent runs still HIT
    (cross-run reuse must survive the fix), and
  * a changed protocol body with unchanged I/O types MISSES (re-executes)
    rather than false-hitting.

The reliable observable is `executed_steps`: a full cache hit drops it to
(); a miss re-runs the step. (The virtual runtime stubs protocol output, so
output *content* is not a reliable discriminator here — that is covered by
the docker e2e. The short-circuit vs re-execution is the false-hit
mechanism, and it is what these assert.)
"""

from __future__ import annotations

import textwrap
from pathlib import Path

from tests.cache._cache_harness import (
    build_samples_library,
    build_transform_library,
    build_types_library,
    build_workflow_task,
    capture_run,
    clear_trace,
)

TYPE_NAMES = ("seed", "out")


def _variant_transform_code(body_marker: str) -> str:
    """A seed->out transform whose PROTOCOL BODY varies by `body_marker`.

    The I/O type topology (seed -> out) is identical across markers; only
    the protocol's runtime behaviour differs. This is the shape that a
    bugfix to a transform's command — or a different tool with the same
    declared types — takes.
    """
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
    # `tr_name` is unique per scenario so the transform's module stem does not
    # alias across tests via sys.modules (TransformInstance.Load imports by
    # bare stem + reload()). The transform *file name* does not enter the
    # cache key (identity = model topology + definition-BYTES digest), so a
    # rename with identical bytes still collides — cross-run reuse is intact.
    types_path = build_types_library(root, TYPE_NAMES)
    samples = build_samples_library(root, types_path, count=1, input_type="seed")
    tr_lib = build_transform_library(
        root / "tr", types_path, {tr_name: _variant_transform_code(body_marker)}
    )
    return build_workflow_task(
        samples, tr_lib, sample_type="seed", target_specs=[("out_target", {"out"})]
    )


def test_identical_protocol_still_hits_cross_run(tmp_path, virtual_runtime):
    """Cross-run reuse survives the F1 fix: same bytes + same protocol -> hit."""
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
    """The F1 fix: a protocol-body edit (same I/O types) busts the cache.

    Pre-R5 this re-executed 0 steps (false hit on run A's cached output);
    post-R5 the differing definition-file bytes change the lineage
    signature, so run B is a MISS and re-runs the (fixed) protocol.
    """
    task_a = _build_task(tmp_path / "a", "ONE", tr_name="tr_one")
    snap_a = capture_run(virtual_runtime, task_a)
    assert snap_a.executed_steps, "run A executed zero steps (bad fixture)"

    # same types, different body AND distinct module stem (avoid import alias)
    task_b = _build_task(tmp_path / "b", "TWO", tr_name="tr_two")
    clear_trace(virtual_runtime)
    snap_b = capture_run(virtual_runtime, task_b)
    assert snap_b.executed_steps != (), (
        "changed protocol body false-hit run A's cache (executed 0 steps); "
        "the lineage signature must fold in protocol identity so an edited "
        "transform re-executes instead of serving stale output"
    )

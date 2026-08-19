from __future__ import annotations
import json
import shutil
from pathlib import Path
import pytest

from tests.metasmith.cache.fixtures.cache_fixtures.linear_3step import build_task
from tests.metasmith.cache._cache_harness import capture_run

OUTDIR = Path("/tmp/quadrant_audit_data")


def test_dump_virtual_runtime_quadrants(virtual_runtime, tmp_path):
    OUTDIR.mkdir(parents=True, exist_ok=True)

    task = build_task(tmp_path)

    snap1 = capture_run(virtual_runtime, task)
    trace1 = virtual_runtime.trace_file.read_text()
    (OUTDIR / "virtual_run1_cold_trace.jsonl").write_text(trace1)
    print(f"\n=== RUN 1 COLD: {len(trace1.splitlines())} lines, {len(snap1.executed_steps)} executed steps ===")
    for line in trace1.splitlines()[:20]:
        print(f"  {line[:200]}")

    workspace = virtual_runtime.home / "_ws"
    for results_dir in (virtual_runtime.home).rglob("_manifests"):
        if results_dir.is_dir():
            print(f"  Found _manifests at: {results_dir}")
            sample = sorted(results_dir.glob("*.json"))[:2]
            for s in sample:
                target = OUTDIR / f"virtual_manifest_{s.name}"
                shutil.copy(s, target)
                print(f"    sampled: {s.name} -> {target.name}")
            break

    from tests.metasmith.cache._cache_harness import clear_trace
    clear_trace(virtual_runtime)
    snap2 = capture_run(virtual_runtime, task)
    trace2 = virtual_runtime.trace_file.read_text()
    (OUTDIR / "virtual_run2_warm_trace.jsonl").write_text(trace2)
    print(f"\n=== RUN 2 WARM: {len(trace2.splitlines())} lines, {len(snap2.executed_steps)} executed steps ===")
    for line in trace2.splitlines()[:20]:
        print(f"  {line[:200]}")


def test_s3_batches_decomposition_n3(virtual_runtime, tmp_path):
    task = build_task(tmp_path, n_samples=3)
    capture_run(virtual_runtime, task)
    workspace = sorted((virtual_runtime.home / "runs").glob("*"))[-1]
    step_metas = sorted(workspace.glob("workflow.step_*.meta"))
    assert len(step_metas) == 3, f"expected 3 step metas, got {len(step_metas)}"
    for sm in step_metas:
        text = sm.read_text()
        batch_line = next(
            (l for l in text.splitlines() if l.startswith("batches ")),
            None,
        )
        assert batch_line is not None, f"no batches line in {sm.name}: {text}"
        batches = json.loads(batch_line.partition(" ")[2])
        assert isinstance(batches, list) and batches, (
            f"{sm.name}: batches empty/non-list: {batches!r}"
        )
        for b in batches:
            assert {"batch_idx", "start", "end", "sorted_inputs"} <= set(b.keys()), (
                f"{sm.name} batch missing keys: {b!r}"
            )
        print(f"\n  {sm.name}: {len(batches)} batches")
        for b in batches:
            print(f"    batch_idx={b['batch_idx']} [{b['start']},{b['end']}) "
                  f"sorted_inputs={b['sorted_inputs']}")

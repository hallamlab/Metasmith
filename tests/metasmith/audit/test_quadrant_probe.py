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

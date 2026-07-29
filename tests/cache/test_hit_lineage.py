"""The lineage a cache hit puts back on the channel.

A hit replays a step's outputs without running it. If it replays them with
an empty index, a downstream `o.group` keyed on an ancestor drops every
tuple as a LINEAGE_VIOLATION (proved against real Nextflow in
tests/e2e/docker/test_orchestrator_exec.py::TestCacheHitLineage) and the
consuming step never runs — a warm run silently computing less than a cold
one. So promote captures the index each output file travelled with, and the
hit path replays it.
"""

from __future__ import annotations

import re
from pathlib import Path

from tests.cache._cache_harness import capture_run, clear_trace
from tests.cache.fixtures.cache_fixtures import parallel_then_group


def _manifests(cache_root: Path) -> list[dict]:
    from metasmith.caching.store import decode_manifest

    return [
        decode_manifest(p.read_bytes())
        for p in sorted(cache_root.rglob("manifest.cbor"))
    ]


def test_promote_records_the_index_each_output_travelled_with(
    tmp_path, virtual_runtime
):
    """Every matched output file gets an `index` row in its shard manifest.

    `index_payload` was reserved for this from the start and written as `[]`
    ever since, which is why the hit path had nothing to emit.
    """
    task = parallel_then_group.build_task(tmp_path)
    capture_run(virtual_runtime, task)

    manifests = _manifests(virtual_runtime.home / "task_cache")
    assert manifests, "fixture promoted nothing"
    for m in manifests:
        need = {
            f["relpath"] for f in m.get("files", [])
            if not f.get("unmatched")
        }
        got = {row["relpath"] for row in m.get("index", [])}
        assert need and need <= got, (
            f"shard {m['key'].hex()[:8]} indexes {sorted(got)} but has "
            f"outputs {sorted(need)}"
        )


def test_a_hit_emits_the_ancestry_a_run_would_have(tmp_path, virtual_runtime):
    """The generated `.nf` no longer puts `[[:], file(...)]` on the wire.

    trB groups by `root`, of which trA's output is a descendant, so trA's
    replayed tuples must carry `root` or trB gets nothing.
    """
    task = parallel_then_group.build_task(tmp_path)
    capture_run(virtual_runtime, task)
    clear_trace(virtual_runtime)
    capture_run(virtual_runtime, task)

    nf = sorted(virtual_runtime.home.rglob("workflow.nf"))
    assert nf, "no generated workflow.nf"
    body = nf[-1].read_text()
    synthetic = [l for l in body.splitlines() if "Channel.of(" in l]
    assert synthetic, f"no cache-hit channel in:\n{body}"
    assert "[[:], file(" not in body, (
        "a cache hit is still emitting an empty index:\n"
        + "\n".join(synthetic)
    )
    assert any(re.search(r"\['\w+': \[", l) for l in synthetic), (
        "expected a populated index map on the synthetic channel, got:\n"
        + "\n".join(synthetic)
    )


def test_a_hit_replays_every_batch_member_not_just_the_first(tmp_path):
    """Which shard files a hit puts back for one produced branch.

    Canonical output names are `{member+1}-{i+1}-{branch+1}.{hash}-{key}{ext}`
    (`bootstrap._get_output_paths`), so a task at `batch_size=B` writes
    `1-...` through `B-...` and a transform writing several files per member
    varies the second field too. The hit path matched a literal `1-1-<branch>.`
    prefix, which is member 0 item 0 of every task — the same "a batch is one
    item long" mistake the lin wire had, on the other side of the cache.
    """
    from metasmith.models.workflow.nextflow_codegen import (
        cached_files_for_branch,
    )

    out = tmp_path / "out"
    out.mkdir()
    names = [
        "1-1-1.aaaa-step_a.txt",   # member 0, item 0
        "2-1-1.bbbb-step_a.txt",   # member 1 — dropped by the old prefix
        "1-2-1.cccc-step_a.txt",   # member 0, second file — also dropped
        "1-1-2.dddd-step_b.txt",   # a different produced branch
    ]
    for n in names:
        (out / n).write_text(n)

    assert [
        f.name for f in cached_files_for_branch(out, 0, "-step_a.txt")
    ] == ["1-1-1.aaaa-step_a.txt", "1-2-1.cccc-step_a.txt",
          "2-1-1.bbbb-step_a.txt"]
    # The old literal prefix would have found exactly one of those three.
    assert len([n for n in names if n.startswith("1-1-1.")]) == 1
    # Branch selection still discriminates.
    assert [
        f.name for f in cached_files_for_branch(out, 1, "-step_b.txt")
    ] == ["1-1-2.dddd-step_b.txt"]

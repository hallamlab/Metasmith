from __future__ import annotations

import re
from pathlib import Path

from tests.metasmith.cache._cache_harness import capture_run, clear_trace
from tests.metasmith.cache.fixtures.cache_fixtures import parallel_then_group


def _manifests(cache_root: Path) -> list[dict]:
    from metasmith.caching.store import decode_manifest

    return [
        decode_manifest(p.read_bytes())
        for p in sorted(cache_root.rglob("manifest.cbor"))
    ]


def test_promote_records_the_index_each_output_travelled_with(
    tmp_path, virtual_runtime
):
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
    from metasmith.models.workflow.nextflow_codegen import (
        cached_files_for_branch,
    )

    out = tmp_path / "out"
    out.mkdir()
    names = [
        "1-1-1.aaaa-step_a.txt",
        "2-1-1.bbbb-step_a.txt",
        "1-2-1.cccc-step_a.txt",
        "1-1-2.dddd-step_b.txt",
    ]
    for n in names:
        (out / n).write_text(n)

    assert [
        f.name for f in cached_files_for_branch(out, 0, "-step_a.txt")
    ] == ["1-1-1.aaaa-step_a.txt", "1-2-1.cccc-step_a.txt",
          "2-1-1.bbbb-step_a.txt"]
    assert len([n for n in names if n.startswith("1-1-1.")]) == 1
    assert [
        f.name for f in cached_files_for_branch(out, 1, "-step_b.txt")
    ] == ["1-1-2.dddd-step_b.txt"]


def test_key_attribution_follows_lineage_not_list_order(tmp_path):
    from metasmith.models.workflow.grouping import (
        positional_slice,
        select_for_key,
    )

    task = parallel_then_group.build_task(tmp_path)
    step = next(
        s for s in task.plan.steps
        if len(s.group_by_instances) > 1
        and any(
            len(s.dependency_map.get(d, [])) > 1
            for d in s.transform.model.requires
        )
    )
    keys = list(step.group_by_instances)
    dep = next(
        d for d in step.transform.model.requires
        if len(step.dependency_map.get(d, [])) > 1
    )
    shuffled = list(reversed(step.dependency_map[dep]))

    for key_idx, key in enumerate(keys):
        picked = select_for_key(shuffled, key, key_idx)
        assert [i.path for i in picked] == [key.path], (
            f"key {key.path} got {[i.path for i in picked]}"
        )

    disagreements = sum(
        1 for key_idx, key in enumerate(keys)
        if [i.path for i in positional_slice(shuffled, key_idx, key_idx + 1)]
        != [key.path]
    )
    assert disagreements > 0, (
        "reversing the dependency list did not change the positional answer; "
        "the test is not exercising what it claims"
    )

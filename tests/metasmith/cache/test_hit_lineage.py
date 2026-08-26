from __future__ import annotations

from pathlib import Path

from tests.metasmith.cache._cache_harness import capture_run, clear_trace
from tests.metasmith.cache.fixtures.cache_fixtures import parallel_then_group


def _manifests(cache_root: Path) -> list[dict]:
    from metasmith.caching.store import decode_manifest

    return [
        decode_manifest(p.read_bytes())
        for p in sorted(cache_root.rglob("manifest.cbor"))
    ]


def test_promote_records_the_lineage_each_member_ran_with(tmp_path, virtual_runtime):
    task = parallel_then_group.build_task(tmp_path)
    capture_run(virtual_runtime, task)

    manifests = _manifests(virtual_runtime.home / "task_cache")
    assert manifests, "fixture promoted nothing"
    for m in manifests:
        assert m.get("lineage"), f"shard {m['key'].hex()[:8]} carries no lineage index"
        assert m.get("consumes"), f"shard {m['key'].hex()[:8]} records nothing consumed"
        for f in m.get("files", []):
            assert f.get("parents"), f"{f['relpath']} in {m['key'].hex()[:8]} has no parents"


def test_a_hit_emits_the_ancestry_a_run_would_have(tmp_path, virtual_runtime):
    from metasmith.telemetry import TraceIndex

    task = parallel_then_group.build_task(tmp_path)
    first = capture_run(virtual_runtime, task)
    clear_trace(virtual_runtime)
    second = capture_run(virtual_runtime, task)

    def _by_key(ws):
        trace = TraceIndex.read(ws / "_metasmith" / "trace.jsonl")
        return {
            e.task_hash: {(p.file_instance_id, tuple(sorted(p.parents))) for p in e.produces}
            for e in trace.events if e.status in ("promoted", "hit")
        }

    cold, warm = _by_key(first.workspace), _by_key(second.workspace)
    assert warm and set(warm) == set(cold), "the second run's members are not the first run's"
    for key, produced in cold.items():
        assert warm[key] == produced, f"hit {key[:8]} replays a different ancestry"


def test_member_outputs_are_matched_by_position(tmp_path):
    from metasmith.caching.promote import member_outputs

    for n in ["1-1-1.aaaa-step_a.txt", "2-1-1.bbbb-step_a.txt", "1-2-1.cccc-step_a.txt",
              "1-1-2.dddd-step_b.txt", "in.txt"]:
        (tmp_path / n).write_text(n)
    (tmp_path / "link").symlink_to(tmp_path / "in.txt")
    assert [f.name for f in member_outputs(tmp_path, 1)] == [
        "1-1-1.aaaa-step_a.txt", "1-1-2.dddd-step_b.txt", "1-2-1.cccc-step_a.txt",
    ]
    assert [f.name for f in member_outputs(tmp_path, 2)] == ["2-1-1.bbbb-step_a.txt"]


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

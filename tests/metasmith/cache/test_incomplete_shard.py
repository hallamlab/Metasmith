"""A shard that cannot serve every slot the step declares must not be replayed.

The emitter turns a hit into `Channel.of(...)` per produce slot. A slot the
shard holds nothing for used to become `Channel.empty()`, and the consumer's
input group then never completes: nextflow declares the process, never submits
it, writes no trace row and no error, and the run still reports completed.
Demotion has to happen at decision time, and all-or-nothing -- half a step from
the shard and half from a fresh execution emits a channel whose members came
from two different runs of the transform.
"""

from __future__ import annotations

from pathlib import Path

from metasmith.models.workflow.cache_decisions import compute_cache_decisions

from tests.metasmith.cache.fixtures.cache_fixtures import linear_3step
from tests.metasmith.cache.test_empty_index import (
    _context,
    _probe_output_name,
    _seed_shard,
)

_REAL_INDEX = {"seed": ["1e20aaaa"]}


def _probe_with_shard_named(tmp_path: Path, rename) -> dict:
    task = linear_3step.build_task(tmp_path)
    name, dtype_key = _probe_output_name(task)
    workspace = tmp_path / "ws"
    workspace.mkdir(exist_ok=True)
    cache_root = tmp_path / "task_cache"
    cache_root.mkdir(exist_ok=True)

    cold = compute_cache_decisions(task, _context(workspace, cache_root))
    first = min(cold)
    assert not cold[first]["hit"], "the cold probe hit an empty cache"

    _seed_shard(
        cache_root, cold[first]["cache_key"], _REAL_INDEX,
        rename(name), dtype_key,
    )
    warm = compute_cache_decisions(task, _context(workspace, cache_root))
    return warm[first]


def test_the_rig_hits_when_the_shard_serves_the_slot(tmp_path):
    decision = _probe_with_shard_named(tmp_path, lambda n: n)
    assert decision["hit"], (
        "the rig cannot produce a hit at all, so the demotions below prove "
        "nothing"
    )


def test_a_shard_holding_only_another_branch_is_demoted(tmp_path):
    decision = _probe_with_shard_named(
        tmp_path, lambda n: n.replace("1-1-1.", "1-1-2.")
    )
    assert not decision["hit"], (
        "a shard whose only output belongs to a branch the step does not "
        "declare was replayed; the declared branch would emit an empty "
        "channel and hang the consumer"
    )
    assert decision["out_indexes"] == {}, (
        f"a demoted hit must carry no indexes forward: {decision['out_indexes']}"
    )


def test_a_shard_holding_another_dtype_is_demoted(tmp_path):
    decision = _probe_with_shard_named(tmp_path, lambda n: n + "_other")
    assert not decision["hit"], (
        "a shard whose output does not end with the slot's dtype key was "
        "replayed; the emitter's glob finds nothing under that name"
    )

"""A shard that cannot serve every slot the member declares is never made or served.

A member whose required branch produced nothing mints no shard. A shard one
of whose listed files has gone is not a hit. A member with an empty optional
branch is still whole.
"""

from __future__ import annotations

from pathlib import Path

from metasmith.caching.invocation import TOMBSTONE_NAME, probe
from metasmith.caching.layout import shard_dir
from metasmith.caching.promote import StepCacheMeta, promote_members
from metasmith.models.lineage import LinPayload
from metasmith.models.workflow.payload import build_entry

KEY = "1e20" + "cd" * 32


def _meta(branches: int) -> StepCacheMeta:
    return StepCacheMeta(
        order=1, transform_key="trA", signature="sig", step_name="trA", cacheable=True,
        slot_files=[
            {"dtype_key": f"out{b}", "ext": ".txt", "branch_idx": b, "slot_id": "a" * 64}
            for b in range(branches)
        ],
        slot_channels={"seed_dep": "seed"},
    )


def _entry() -> dict:
    e = build_entry([("seed", [("/w/in.txt", {"seed": ["1e20aaaa"]})])])
    e[LinPayload.KEY_KEY] = KEY
    return e


def _promote(tmp_path: Path, names: list[str], branches: int):
    cwd = tmp_path / "task"
    cwd.mkdir()
    for n in names:
        (cwd / n).write_text("payload")
    cache_root = tmp_path / "task_cache"
    records = promote_members(
        cwd=cwd, entries=[_entry()], meta=_meta(branches), cache_root=cache_root, successes=[True],
    )
    return records, cache_root


def test_a_member_whose_required_branch_is_empty_mints_no_shard(tmp_path):
    records, cache_root = _promote(tmp_path, [], branches=1)
    assert [r["status"] for r in records] == ["incomplete"]
    assert not shard_dir(cache_root, KEY).exists()
    assert probe(cache_root, bytes.fromhex(KEY)) is None


def test_a_member_whose_only_product_is_another_dtype_mints_no_shard(tmp_path):
    records, cache_root = _promote(tmp_path, ["1-1-1.abcdef-other.txt"], branches=1)
    assert [r["status"] for r in records] == ["incomplete"]
    assert not shard_dir(cache_root, KEY).exists()


def test_an_empty_optional_branch_still_promotes(tmp_path):
    records, cache_root = _promote(tmp_path, ["1-1-1.abcdef-out0.txt"], branches=2)
    assert [r["status"] for r in records] == ["promoted"]
    assert probe(cache_root, bytes.fromhex(KEY)) is not None


def test_a_shard_missing_a_listed_file_is_not_a_hit(tmp_path):
    _records, cache_root = _promote(tmp_path, ["1-1-1.abcdef-out0.txt"], branches=1)
    shard = shard_dir(cache_root, KEY)
    assert probe(cache_root, bytes.fromhex(KEY)) == shard
    (shard / "out" / "1-1-1.abcdef-out0.txt").unlink()
    assert probe(cache_root, bytes.fromhex(KEY)) is None


def test_a_tombstoned_shard_is_not_a_hit(tmp_path):
    _records, cache_root = _promote(tmp_path, ["1-1-1.abcdef-out0.txt"], branches=1)
    shard = shard_dir(cache_root, KEY)
    (shard / TOMBSTONE_NAME).touch()
    assert probe(cache_root, bytes.fromhex(KEY)) is None

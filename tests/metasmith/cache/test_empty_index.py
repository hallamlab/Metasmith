"""A member with no identity to key on is never cached, and never breaks a run.

The key folds the own-id of every item a member consumed. A member whose
payload carries no `PROV`, or an item that never got an identity on the
channel, has nothing to key on: it runs, its products are recorded so its
descendants can name them, and no shard is written for it.
"""

from __future__ import annotations

import json
from pathlib import Path

from metasmith.caching.invocation import consumed_of, probe, read_manifest
from metasmith.caching.layout import shard_dir
from metasmith.caching.promote import CACHE_RECORD_FILE, StepCacheMeta, promote_members
from metasmith.models.lineage import LinPayload
from metasmith.models.workflow.payload import build_entry


META = StepCacheMeta(
    order=1, transform_key="trA", signature="sig", step_name="trA", cacheable=True,
    slot_files=[{"dtype_key": "step_a", "ext": ".txt", "branch_idx": 0, "slot_id": "a" * 64}],
    slot_channels={"seed_dep": "seed"},
)


def _member(with_prov: bool, own_id: bool = True) -> dict:
    if not with_prov:
        return {"seed": ["1e20aaaa"], LinPayload.FILES_KEY: [["/w/in.txt"]]}
    return build_entry([("seed", [("/w/in.txt", {"seed": ["1e20aaaa"]} if own_id else {"root": ["r"]})])])


def _run(tmp_path: Path, entry: dict, key: str) -> tuple[list[dict], Path]:
    cwd = tmp_path / "task"
    cwd.mkdir()
    (cwd / "1-1-1.abcdef-step_a.txt").write_text("payload")
    entry[LinPayload.KEY_KEY] = key
    cache_root = tmp_path / "task_cache"
    records = promote_members(cwd=cwd, entries=[entry], meta=META, cache_root=cache_root, successes=[True])
    return records, cache_root


def test_a_member_without_prov_has_no_key():
    assert consumed_of(_member(with_prov=False), ["seed"]) is None


def test_an_item_without_its_own_id_leaves_the_member_unkeyed():
    assert consumed_of(_member(with_prov=True, own_id=False), ["seed"]) is None


def test_an_unkeyed_member_is_recorded_but_not_promoted(tmp_path):
    records, cache_root = _run(tmp_path, _member(with_prov=False), "-")
    assert [r["status"] for r in records] == ["uncacheable"]
    assert not any(cache_root.rglob("manifest.cbor")), "an unkeyed member wrote a shard"
    assert records[0]["produces"], "its products must still be recorded for its descendants"
    assert (tmp_path / "task" / CACHE_RECORD_FILE).read_text().strip()


def test_a_keyed_member_writes_a_shard_that_probes_as_a_hit(tmp_path):
    key = "1e20" + "ab" * 32
    records, cache_root = _run(tmp_path, _member(with_prov=True), key)
    assert [r["status"] for r in records] == ["promoted"]
    shard = shard_dir(cache_root, key)
    assert probe(cache_root, bytes.fromhex(key)) == shard
    manifest = read_manifest(shard)
    assert manifest["consumes"] == {"seed": ["1e20aaaa"]}
    assert manifest["lineage"] == {"seed": ["1e20aaaa"]}, "the manifest carries the member's index, without the reserved keys"
    assert [f["relpath"] for f in manifest["files"]] == ["out/1-1-1.abcdef-step_a.txt"]
    assert manifest["files"][0]["parents"] == ["1e20aaaa"]

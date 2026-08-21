from __future__ import annotations

import json
from pathlib import Path

from metasmith.caching.layout import shard_dir
from metasmith.caching.store import CacheStore, decode_manifest, encode_manifest
from metasmith.constants import AgentPaths
from metasmith.env import Runtime
from metasmith.models.workflow import NextflowGenContext
from metasmith.models.workflow.cache_decisions import compute_cache_decisions

from tests.metasmith.cache.fixtures.cache_fixtures import linear_3step


# The promote fixtures below declare their own slot, so this pairs with the
# `dtype_key`/`ext` in `_promote_one_output`'s StepPromoteSpec. The probe
# fixtures run against a real plan and must use that plan's own dtype key.
_OUTPUT_NAME = "1-1-1.abcdef-step_a.txt"


def _probe_output_name(task) -> tuple[str, str]:
    """The name the first step's product really takes, and its dtype key.

    `payload.output_file_name` ends every product with `-<dtype key><ext>`, and
    both the process output glob and the cache-hit probe match on that tail. A
    made-up tail names a file the engine never writes.
    """
    step = min(task.plan.steps, key=lambda s: s.order)
    for dep_group in step.transform.model.produces:
        for dep in dep_group:
            insts = step.dependency_map.get(dep, [])
            if not insts:
                continue
            dtype = insts[0].dtype
            ext = dtype.GetPreferredFileExtension()
            return f"1-1-1.abcdef-{dtype.key}{ext}", dtype.key
    raise AssertionError("the fixture's first step declares no product")


def _context(workspace: Path, cache_root: Path) -> NextflowGenContext:
    return NextflowGenContext(
        workflow_file=AgentPaths.NXF_WORKFLOW,
        work_dir=workspace,
        external_work=workspace,
        home_dir=AgentPaths.HOME_ROOT,
        external_home=AgentPaths.HOME_ROOT,
        runtime=Runtime.DOCKER,
        resources_file=AgentPaths.NXF_RES,
        cache_root=cache_root,
    )


def _seed_shard(
    cache_root: Path, cache_key: bytes, index: dict, name: str, dtype_key: str
) -> None:
    key_hex = cache_key.hex()
    final = shard_dir(cache_root, key_hex)
    (final / "out").mkdir(parents=True)
    (final / "out" / name).write_text("payload")
    relpath = f"out/{name}"
    manifest = encode_manifest(
        cache_key=cache_key,
        transform_key="trA",
        signature="sig",
        lineage_payload=b"",
        output_files=[{
            "relpath": relpath,
            "slot_id": "a" * 64,
            "dtype_key": dtype_key,
            "branch_idx": 0,
            "batch_idx": 0,
        }],
        out_identities={f"{dtype_key}::0": "a" * 64},
        index_payload=[{"relpath": relpath, "index": index}],
    )
    (final / "manifest.cbor").write_bytes(manifest)
    store = CacheStore.open(cache_root)
    try:
        store.upsert(
            key=cache_key,
            transform_key="trA",
            payload=manifest,
            output_root=str(final.relative_to(cache_root)),
            size_bytes=7,
            origin="lineage",
        )
    finally:
        store.close()


def _probe_first_step(tmp_path: Path, index: dict) -> tuple[dict, str]:
    task = linear_3step.build_task(tmp_path)
    name, dtype_key = _probe_output_name(task)
    workspace = tmp_path / "ws"
    workspace.mkdir(exist_ok=True)
    cache_root = tmp_path / "task_cache"
    cache_root.mkdir(exist_ok=True)

    cold = compute_cache_decisions(task, _context(workspace, cache_root))
    first = min(cold)
    assert not cold[first]["hit"], "the cold probe hit an empty cache"

    _seed_shard(cache_root, cold[first]["cache_key"], index, name, dtype_key)
    warm = compute_cache_decisions(task, _context(workspace, cache_root))
    return warm[first], name


def test_a_shard_whose_index_row_is_empty_is_demoted(tmp_path):
    decision, _name = _probe_first_step(tmp_path, {})

    assert not decision["hit"], (
        "a shard whose only output carries an empty index was replayed; "
        "the hit emits `[[:], file(...)]` and the run stops at the first "
        f"descendant group. out_indexes={decision['out_indexes']}"
    )
    assert decision["out_indexes"] == {}, (
        "a demoted hit must carry no indexes forward, got "
        f"{decision['out_indexes']}"
    )


def test_a_shard_with_a_real_index_row_still_hits(tmp_path):
    decision, name = _probe_first_step(tmp_path, {"seed": ["1e20aaaa"]})

    assert decision["hit"], (
        "the rig cannot produce a hit at all, so the demotion test above "
        "proves nothing"
    )
    assert decision["out_indexes"] == {name: {"seed": ["1e20aaaa"]}}


def _work_tree(workspace: Path, entry: dict) -> None:
    from metasmith.caching.keys import LIN_PAYLOAD_VERSION
    from metasmith.models.workflow import METADATA_FILE

    task_dir = workspace / "nxf_work" / "step_00" / "batch_0000_0000"
    task_dir.mkdir(parents=True)
    (task_dir / _OUTPUT_NAME).write_text("payload")
    (task_dir / METADATA_FILE).write_text(
        "lin "
        + json.dumps({"v": LIN_PAYLOAD_VERSION, "entries": [entry]})
        + "\n"
    )


def _promote_one_output(tmp_path, monkeypatch, entry: dict) -> tuple[dict, list]:
    from metasmith.caching import promote as promote_mod

    workspace = tmp_path / "ws"
    workspace.mkdir()
    cache_root = tmp_path / "task_cache"
    cache_root.mkdir()
    (workspace / "workflow.step_00.meta").write_text("{}")
    _work_tree(workspace, entry)

    spec = promote_mod.StepPromoteSpec(
        order=0,
        cache_key=bytes.fromhex("c0ffee11"),
        cacheable=True,
        transform_key="trA",
        signature="sig",
        out_identities={"step_a::0": "a" * 64},
        dep_out=[],
        slot_files=[{
            "dtype_key": "step_a",
            "ext": ".txt",
            "branch_idx": 0,
            "slot_id": "a" * 64,
        }],
    )
    monkeypatch.setattr(promote_mod, "_read_step_meta", lambda _p: spec)

    log: list = []
    summary = promote_mod.promote_run(
        workspace=workspace, cache_root=cache_root, log=log
    )
    assert summary["promoted"] == ["c0ffee11"], (
        f"the fixture promoted nothing: {summary}, log={log}"
    )
    manifest = decode_manifest(
        (shard_dir(cache_root, "c0ffee11") / "manifest.cbor").read_bytes()
    )
    return manifest, log


def test_an_empty_collected_index_is_never_written_to_a_manifest(
    tmp_path, monkeypatch
):
    manifest, log = _promote_one_output(tmp_path, monkeypatch, {})

    assert manifest["index"] == [], (
        "an empty collected index was written into the manifest as if it "
        f"were ancestry: {manifest['index']}"
    )
    assert [f["relpath"] for f in manifest["files"]] == [f"out/{_OUTPUT_NAME}"], (
        "the output itself must still be cached; only its index is missing"
    )
    assert any(
        _OUTPUT_NAME in str(msg) and "no on-channel index" in str(msg)
        for _level, msg in log
    ), f"promote stored the shard without reporting the gap: {log}"


def test_a_real_collected_index_is_written_to_the_manifest(
    tmp_path, monkeypatch
):
    manifest, log = _promote_one_output(
        tmp_path, monkeypatch, {"seed": ["1e20aaaa"], "FILES": [["/w/in.txt"]]}
    )

    assert manifest["index"] == [
        {"relpath": f"out/{_OUTPUT_NAME}", "index": {"seed": ["1e20aaaa"]}}
    ], f"the reserved-key strip or the capture changed shape: {manifest['index']}"
    assert not any("no on-channel index" in str(msg) for _l, msg in log), log

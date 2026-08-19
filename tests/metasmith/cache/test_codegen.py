from __future__ import annotations

import json
from pathlib import Path

from tests.metasmith.cache.fixtures.cache_fixtures import linear_3step, mixed_cacheability


def _make_context(workspace: Path, external_home: Path, external_work: Path):
    from metasmith.constants import AgentPaths
    from metasmith.env import Runtime
    from metasmith.models.workflow import NextflowGenContext

    return NextflowGenContext(
        workflow_file=AgentPaths.NXF_WORKFLOW,
        work_dir=workspace,
        external_work=external_work,
        home_dir=AgentPaths.HOME_ROOT,
        external_home=external_home,
        runtime=Runtime.DOCKER,
        resources_file=AgentPaths.NXF_RES,
    )


def _stage(task, tmp_path: Path) -> Path:
    from metasmith.constants import AgentPaths

    workspace = tmp_path / "ws"
    workspace.mkdir()
    task.PrepareNextflow(
        _make_context(workspace, AgentPaths.HOME_ROOT, workspace)
    )
    return workspace


def _stage_split_home(task, agent_home: Path, run_dir: str = "run") -> Path:
    from metasmith.constants import AgentPaths

    external_work = agent_home / AgentPaths.STAGED / run_dir
    external_work.mkdir(parents=True, exist_ok=True)
    task.PrepareNextflow(
        _make_context(external_work, agent_home, external_work)
    )
    return external_work


def _seed_cache_from_meta(agent_home: Path, workspace: Path, step_name: str):
    from metasmith.caching.layout import default_cache_root, out_dir, shard_dir
    from metasmith.caching.store import CacheStore, encode_manifest

    key_hex = ""
    slot_files: list[dict] = []
    for meta in sorted(workspace.glob("workflow.step_*.meta")):
        fields: dict[str, str] = {}
        for line in meta.read_text().splitlines():
            head, _, rest = line.partition(" ")
            fields[head] = rest
        if fields.get("step_name", "").strip() != step_name:
            continue
        key_hex = fields.get("cache_key", "").strip()
        slot_files = json.loads(fields.get("slot_files", "[]"))
        break
    assert key_hex and slot_files, (
        f"no step named {step_name!r} with cache_key + slot_files in {workspace}"
    )

    cache_root = default_cache_root(agent_home)
    shard = shard_dir(cache_root, key_hex)
    outs = out_dir(shard)
    outs.mkdir(parents=True, exist_ok=True)

    seeded: list[Path] = []
    files_meta: list[dict] = []
    index_meta: list[dict] = []
    for sf in slot_files:
        name = (
            f"1-1-{sf['branch_idx'] + 1}.cachedseed"
            f"-{sf['dtype_key']}{sf['ext']}"
        )
        fp = outs / name
        fp.write_text("cached\n")
        seeded.append(fp)
        relpath = str(fp.relative_to(shard))
        files_meta.append({
            "relpath": relpath,
            "slot_id": sf["slot_id"],
            "dtype_key": sf["dtype_key"],
            "branch_idx": sf["branch_idx"],
        })
        index_meta.append({
            "relpath": relpath,
            "index": {sf["dtype_key"]: ["cachedseed"]},
        })

    payload = encode_manifest(
        cache_key=bytes.fromhex(key_hex),
        transform_key="",
        signature="",
        lineage_payload=b"",
        output_files=files_meta,
        out_identities={},
        index_payload=index_meta,
    )
    (shard / "manifest.cbor").write_bytes(payload)
    store = CacheStore.open(cache_root)
    try:
        store.upsert(
            key=bytes.fromhex(key_hex),
            transform_key="",
            payload=payload,
            output_root=str(shard.relative_to(cache_root)),
            size_bytes=sum(p.stat().st_size for p in seeded),
            origin="lineage",
        )
    finally:
        store.close()
    return cache_root, seeded, key_hex


def test_no_plugin_in_generated_nf(tmp_path):
    task = linear_3step.build_task(tmp_path)
    workspace = _stage(task, tmp_path)

    body = (workspace / "workflow.nf").read_text()
    assert "plugins {" not in body
    assert "task.ext" not in body
    assert "nf-metasmith" not in body


def test_generated_config_has_no_process_cache_directive(tmp_path):
    from metasmith.constants import AgentPaths

    task = linear_3step.build_task(tmp_path)
    workspace = _stage(task, tmp_path)
    cfg = workspace / AgentPaths.NXF_CONFIG
    if cfg.exists():
        body = cfg.read_text()
        for line in body.splitlines():
            stripped = line.strip()
            assert not stripped.startswith("process.cache"), (
                f"process.cache directive leaked: {stripped!r}"
            )


def test_cache_hit_channel_is_home_rooted(tmp_path):
    from metasmith.constants import AgentPaths

    agent_home = tmp_path / "agent_home"
    agent_home.mkdir()

    task = mixed_cacheability.build_task(tmp_path / "src")
    ws1 = _stage_split_home(task, agent_home, run_dir="run1")
    cache_root, seeded, _ = _seed_cache_from_meta(agent_home, ws1, "trA")
    assert seeded, "seeded no cached files for trA"

    ws2 = _stage_split_home(task, agent_home, run_dir="run2")
    body = (ws2 / "workflow.nf").read_text()

    channel_lines = [l for l in body.splitlines() if "Channel.of(" in l]
    assert channel_lines, (
        "step 1 did not become a cache hit; no synthetic channel emitted"
    )
    for line in channel_lines:
        assert str(agent_home) not in line, (
            "cache-hit channel carries the HOST spelling of the agent home, "
            "which the bootstrap container cannot resolve:\n"
            f"  {line.strip()}"
        )
        assert str(AgentPaths.HOME_ROOT) in line, (
            f"cache-hit channel is not rooted at HOME_ROOT:\n  {line.strip()}"
        )

    from metasmith.models.paths import PathMap

    path_map = PathMap(extern_home=agent_home, task_key=ws2.name)
    for fp in seeded:
        expected = path_map.ExternalToLocal(fp)
        assert f"file('{expected}')" in body, (
            f"expected cached file literal {expected} not in workflow.nf"
        )

    from metasmith.testing.contract_runtime import (
        CompiledTask,
        check_emitted_addresses,
    )

    violations = check_emitted_addresses(CompiledTask(
        workspace=ws2,
        key=ws2.name,
        task=task,
        workflow_nf=body,
        home_root=AgentPaths.HOME_ROOT,
        work_root=AgentPaths.WORK_ROOT,
        external_home=agent_home,
    ))
    assert violations == [], f"address checker flagged the fixed output: {violations}"


def test_cache_hit_follows_the_store_row_not_the_key(tmp_path):
    from metasmith.caching.layout import default_cache_root, out_dir, shard_dir
    from metasmith.caching.store import CacheStore
    from metasmith.constants import AgentPaths

    agent_home = tmp_path / "agent_home"
    agent_home.mkdir()
    task = mixed_cacheability.build_task(tmp_path / "src")
    ws1 = _stage_split_home(task, agent_home, run_dir="run1")
    cache_root, seeded, key_hex = _seed_cache_from_meta(agent_home, ws1, "trA")

    derived = shard_dir(cache_root, key_hex)
    relocated = cache_root / "relocated" / key_hex
    relocated.parent.mkdir(parents=True, exist_ok=True)
    derived.rename(relocated)
    assert not derived.exists()

    store = CacheStore.open(cache_root)
    try:
        row = store.probe(bytes.fromhex(key_hex))
        store.upsert(
            key=row.key,
            transform_key=row.transform_key,
            payload=row.payload,
            output_root=str(relocated.relative_to(cache_root)),
            size_bytes=row.size_bytes,
            origin=row.origin,
        )
    finally:
        store.close()

    ws2 = _stage_split_home(task, agent_home, run_dir="run2")
    body = (ws2 / "workflow.nf").read_text()
    assert "Channel.empty()" not in body, (
        "cache hit globbed the key-derived shard, which no longer exists"
    )

    from metasmith.models.paths import PathMap

    path_map = PathMap(extern_home=agent_home, task_key=ws2.name)
    expected = path_map.ExternalToLocal(
        out_dir(relocated) / Path(seeded[0]).name
    )
    assert f"file('{expected}')" in body, (
        f"emitted literal does not follow the store row; expected {expected}"
    )
    assert str(AgentPaths.HOME_ROOT) in body


def test_publish_dir_stays_external(tmp_path):
    from metasmith.constants import AgentPaths

    agent_home = tmp_path / "agent_home"
    agent_home.mkdir()
    task = mixed_cacheability.build_task(tmp_path / "src")
    ws = _stage_split_home(task, agent_home)

    publish_lines = [
        l for l in (ws / "workflow.nf").read_text().splitlines()
        if "publishDir" in l
    ]
    assert publish_lines, "no publishDir emitted for a cacheable miss step"
    for line in publish_lines:
        assert str(agent_home) in line, (
            f"publishDir lost the host spelling:\n  {line.strip()}"
        )
        assert str(AgentPaths.HOME_ROOT) not in line, (
            "publishDir carries the container spelling; the head process "
            f"writes through the host path:\n  {line.strip()}"
        )

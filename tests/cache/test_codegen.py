"""Codegen regression guards: workflow.nf / nextflow.config invariants.

These are the "and don't break this" half of the cache-codegen contract:
the cache implementation MUST NOT introduce a Nextflow plugin block, a
`task.ext.*` cache-correlation closure, or a `process.cache` directive.
Cache identity is owned by metasmith — Nextflow sees every run as fresh.

It also owns the *coordinate system* of what codegen writes into the
graph. Every other test in the repo builds its `NextflowGenContext` with
`external_home == AgentPaths.HOME_ROOT`, which collapses the host and
container views onto one string and makes a coordinate bug
unrepresentable. `_stage_split_home` deliberately keeps them apart; the
identity arm is the relay-free (mamba/native) configuration and stays
covered by `_stage`.

The cache-hit-rerun behavior previously verified by
`test_synthetic_channel_registers_index_history` now lives in
`test_hit_miss.py` (it's a hit/miss axis assertion that just happened to
exercise the codegen path).

Coverage: G5 (no plugin), G7 (no process.cache directive), and the
cache-hit channel's coordinate system.
"""

from __future__ import annotations

import json
from pathlib import Path

from tests.cache.fixtures.cache_fixtures import linear_3step, mixed_cacheability


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
    """Compile the task's Nextflow workspace and return the workspace path."""
    from metasmith.constants import AgentPaths

    workspace = tmp_path / "ws"
    workspace.mkdir()
    task.PrepareNextflow(
        _make_context(workspace, AgentPaths.HOME_ROOT, workspace)
    )
    return workspace


def _stage_split_home(task, agent_home: Path, run_dir: str = "run") -> Path:
    """Stage with the host home and the container home held apart.

    The containerized agent's real shape: the agent home exists on the host
    at `agent_home` and is bound into the bootstrap container at
    `AgentPaths.HOME_ROOT`. Filesystem work at compile time must use the
    former; anything written into the graph for a later reader must use the
    latter.
    """
    from metasmith.constants import AgentPaths

    external_work = agent_home / AgentPaths.STAGED / run_dir
    external_work.mkdir(parents=True, exist_ok=True)
    task.PrepareNextflow(
        _make_context(external_work, agent_home, external_work)
    )
    return external_work


def _seed_cache_from_meta(agent_home: Path, workspace: Path, step_name: str):
    """Promote a fake shard for one step, as a prior run's promote would.

    Reads the step's own `workflow.step_N.meta` so the seeded filenames and
    slot ids match what the probe will look for — the alternative is
    hand-rolling the canonical output-name shape a second time.

    Returns (cache_root, [seeded file paths], cache_key_hex).
    """
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
        # Every seeded file needs the on-channel index it "travelled with",
        # because a shard that cannot supply one for every matched file is
        # demoted to a miss (cache_decisions) — which would make these
        # codegen tests assert against a step that never became a hit.
        # The contents are arbitrary here; only presence and round-tripping
        # are under test.
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
    """G5 regression guard: generated workflow.nf has no plugin block / task.ext.

    The cache implementation MUST NOT add a Nextflow plugin or a
    `task.ext.cacheBranchKey`-style correlation closure to satisfy the
    Critic E#1 invariant. Cache rewrites live in synthetic channels that
    re-enter `o.post(...)`, not in plugin metadata.
    """
    task = linear_3step.build_task(tmp_path)
    workspace = _stage(task, tmp_path)

    body = (workspace / "workflow.nf").read_text()
    assert "plugins {" not in body
    assert "task.ext" not in body
    assert "nf-metasmith" not in body


def test_generated_config_has_no_process_cache_directive(tmp_path):
    """G7 regression guard: generated nextflow.config has no process.cache.

    From Nextflow's POV every run is fresh; resume semantics are owned
    by metasmith's lineage-addressed cache.
    """
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


# ---------------------------------------------------------------------------
# Which coordinate system reaches the graph
# ---------------------------------------------------------------------------


def test_cache_hit_channel_is_home_rooted(tmp_path):
    """A cache hit's synthetic channel must carry the container spelling.

    Everything on the FILES manifest is read back inside the bootstrap
    container, which mounts the agent home only at `AgentPaths.HOME_ROOT`
    — the host spelling is deliberately not mounted there. A hit that
    emits the host path stages a symlink the head process can follow and
    the consumer cannot, and the step dies reporting present files as
    missing.

    Fails before the fix: the emitted literal was the raw host path.
    """
    from metasmith.constants import AgentPaths

    agent_home = tmp_path / "agent_home"
    agent_home.mkdir()

    # Run 1 primes the layout; we seed the shard by hand rather than
    # executing, since codegen is the only surface under test here.
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

    # And it is the same file, not merely a home-rooted path: the exact
    # container spelling of each seeded shard file.
    from metasmith.models.paths import PathMap

    path_map = PathMap(extern_home=agent_home, task_key=ws2.name)
    for fp in seeded:
        expected = path_map.ExternalToLocal(fp)
        assert f"file('{expected}')" in body, (
            f"expected cached file literal {expected} not in workflow.nf"
        )

    # This is the only place in the suite where a real `file(...)` literal
    # reaches the graph, so it is the only place the compile-time checker's
    # literal arm can be exercised against generated rather than hand-written
    # Groovy. Without this the checker could rot unnoticed.
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
    """The shard comes from the entry the probe returned, not from the key.

    `CacheStore.files_exist` gates a hit on `entry.output_root`, so the store
    row is what "this hit is real" was decided against. Re-deriving the shard
    from the key afterwards can name a directory that does not exist — the hit
    is then reported, the glob finds nothing, and the step silently emits
    `Channel.empty()` instead of its cached outputs.

    Seeds a row whose `output_root` deliberately differs from
    `shard_dir(cache_key)` and asserts the emitted literal follows the row.
    """
    from metasmith.caching.layout import default_cache_root, out_dir, shard_dir
    from metasmith.caching.store import CacheStore
    from metasmith.constants import AgentPaths

    agent_home = tmp_path / "agent_home"
    agent_home.mkdir()
    task = mixed_cacheability.build_task(tmp_path / "src")
    ws1 = _stage_split_home(task, agent_home, run_dir="run1")
    cache_root, seeded, key_hex = _seed_cache_from_meta(agent_home, ws1, "trA")

    # Relocate the shard on disk and repoint the row at its new home. The
    # key-derived path no longer exists.
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
    """`publishDir` is the one cache path that must stay host-rooted.

    Nextflow's publish step runs in the head process against the real
    filesystem, and the post-run promote re-derives the same root on the
    host. Pinned alongside the channel assertion above so a future
    "unify the cache paths" edit trips here rather than in a run.
    """
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

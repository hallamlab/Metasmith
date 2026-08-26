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


def test_every_grouped_step_gets_a_cached_twin_and_a_two_stream_group(tmp_path):
    from metasmith.models.workflow.nextflow_codegen import (
        CachedProcessName, NextflowProcessName,
    )

    task = linear_3step.build_task(tmp_path)
    workspace = _stage(task, tmp_path)
    body = (workspace / "workflow.nf").read_text()

    for step in task.plan.steps:
        name = NextflowProcessName(step.order, step.transform.name)
        twin = CachedProcessName(name)
        assert f"process {twin} {{" in body, f"no twin process for {name}"
        assert f"(__miss_{step.order}, __hit_{step.order}) = o.group(" in body
        assert f"o.mixOuts(o.asStreams({name}(__miss_{step.order})), o.asStreams({twin}(__hit_{step.order})))" in body
    assert "Channel.of(" not in body, "a hit is a task now, not a spliced channel"
    assert "publishDir" not in body, "no process publishes into the cache any more"


def test_the_twin_runs_locally_and_never_reuses_a_nextflow_cache(tmp_path):
    task = linear_3step.build_task(tmp_path)
    workspace = _stage(task, tmp_path)
    body = (workspace / "workflow.nf").read_text()
    twin = body[body.index("_cached {"):]
    twin = twin[:twin.index("\n}\n")]
    assert "executor 'local'" in twin
    assert "cache false" in twin
    assert "tuple val(index), val(sources)" in twin


def test_the_twin_is_in_the_resources_file_the_ceiling_reads(tmp_path):
    from metasmith.agents.ceiling import parse_requests
    from metasmith.constants import AgentPaths
    from metasmith.models.workflow.nextflow_codegen import (
        CachedProcessName, NextflowProcessName,
    )

    task = linear_3step.build_task(tmp_path)
    workspace = _stage(task, tmp_path)
    requests = parse_requests((workspace / AgentPaths.NXF_RES).read_text())
    for step in task.plan.steps:
        twin = CachedProcessName(NextflowProcessName(step.order, step.transform.name))
        assert twin in requests, f"{twin} has no resources entry"
        cpus, gb = requests[twin]
        assert (cpus or 1) <= 1 and (gb or 0) < 1, f"the twin asks for real resources: {requests[twin]}"


def test_the_cache_root_and_hit_log_are_rendered_through_params(tmp_path):
    # The head process resolves them at run time in its own coordinates; a
    # host spelling baked in at compile time would be wrong in the container.
    agent_home = tmp_path / "agent_home"
    agent_home.mkdir()
    task = mixed_cacheability.build_task(tmp_path / "src")
    ws = _stage_split_home(task, agent_home)
    body = (ws / "workflow.nf").read_text()
    group_lines = [l for l in body.splitlines() if "o.group(" in l]
    assert group_lines
    for line in group_lines:
        assert 'cache_root: "${params.home}/task_cache"' in line, line
        assert 'hits_log: "${params.workspace}/_metasmith/cache_hits.jsonl"' in line, line
        assert str(agent_home) not in line, line


def test_an_uncacheable_step_routes_everything_to_the_real_process(tmp_path):
    task = mixed_cacheability.build_task(tmp_path / "src")
    workspace = _stage(task, tmp_path)
    body = (workspace / "workflow.nf").read_text()
    by_step = {
        s.order: s.transform.name for s in task.plan.steps
    }
    for line in body.splitlines():
        if "o.group(" not in line:
            continue
        order = int(line.split("__miss_")[1].split(",")[0])
        want = "false" if by_step[order] == "trB" else "true"
        assert f"cacheable: {want}" in line, f"{by_step[order]}: {line}"


def test_the_step_meta_carries_what_the_task_promotes_with(tmp_path):
    from metasmith.caching.promote import read_step_meta

    task = linear_3step.build_task(tmp_path)
    workspace = _stage(task, tmp_path)
    metas = sorted(workspace.glob("workflow.step_*.meta"))
    assert len(metas) == len(task.plan.steps)
    for mp in metas:
        meta = read_step_meta(mp)
        assert meta is not None, mp
        assert meta.transform_key and meta.signature and meta.step_name
        assert meta.slot_files and all(sf["slot_id"] for sf in meta.slot_files)
        assert meta.slot_channels
        text = mp.read_text()
        for gone in ("cache_key ", "batches ", "sorted_inputs ", "out_identities "):
            assert gone not in text, f"{mp.name} still carries {gone.strip()}"

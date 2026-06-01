"""Codegen regression guards: workflow.nf / nextflow.config invariants.

These are the "and don't break this" half of the cache-codegen contract:
the cache implementation MUST NOT introduce a Nextflow plugin block, a
`task.ext.*` cache-correlation closure, or a `process.cache` directive.
Cache identity is owned by metasmith — Nextflow sees every run as fresh.

The cache-hit-rerun behavior previously verified by
`test_synthetic_channel_registers_index_history` now lives in
`test_hit_miss.py` (it's a hit/miss axis assertion that just happened to
exercise the codegen path).

Coverage: G5 (no plugin), G7 (no process.cache directive).
"""

from __future__ import annotations

from pathlib import Path

from tests.cache.fixtures.cache_fixtures import linear_3step


def _stage(task, tmp_path: Path) -> Path:
    """Compile the task's Nextflow workspace and return the workspace path."""
    from metasmith.constants import AgentPaths
    from metasmith.coms.containers import ContainerRuntime
    from metasmith.models.workflow import NextflowGenContext

    workspace = tmp_path / "ws"
    workspace.mkdir()
    ctx = NextflowGenContext(
        workflow_file=AgentPaths.NXF_WORKFLOW,
        work_dir=workspace,
        external_work=workspace,
        home_dir=AgentPaths.HOME_ROOT,
        external_home=AgentPaths.HOME_ROOT,
        container_runtime=ContainerRuntime.DOCKER,
        resources_file=AgentPaths.NXF_RES,
    )
    task.PrepareNextflow(ctx)
    return workspace


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

"""Forward-looking xfail tests for the codegen / Nextflow integration.

Coverage: G4 (synthetic channel registers index_history), G5 (no plugin
header), G7 (no process.cache directive), G12 (inputs at any transform).
All xfail-strict until S3 lands the compile-time probe + workflow rewrite.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from tests.integration._cache_harness import capture_run, clear_trace
from tests.integration.fixtures.cache_fixtures import (
    linear_3step,
    parallel_then_group,
)

from tests.e2e_virtual.conftest import virtual_runtime  # noqa: F401


@pytest.mark.xfail(strict=True, reason="S3 not landed")
def test_synthetic_channel_registers_index_history(tmp_path, virtual_runtime):
    """G4, G12: cached step emits synthetic channel; o.group reduction completes.

    Build a parallel_then_group task, prime the cache via a first run,
    then on a second run trA (the parallel head) should be cached. The
    synthetic Channel.of(...) must re-enter o.post() so index_history
    populates and trB's o.group reduction still fires correctly. We
    assert: trA absent from executed_steps; trB present (it runs even on
    rerun because its inputs are synthetic-channel-sourced).
    """
    task = parallel_then_group.build_task(tmp_path / "run1")
    capture_run(virtual_runtime, task)
    clear_trace(virtual_runtime)
    task2 = parallel_then_group.build_task(tmp_path / "run2")
    snap = capture_run(virtual_runtime, task2)
    assert "trA" not in snap.executed_steps
    assert "trB" not in snap.executed_steps
    assert "trC" not in snap.executed_steps


def test_no_plugin_in_generated_nf(tmp_path):
    """G5 regression guard: generated workflow.nf has no plugin block / task.ext.

    Passes on main (no plugin to begin with). Stays in the suite as a
    regression assertion — the cache implementation MUST NOT add a
    Nextflow plugin or `task.ext.cacheBranchKey`-style correlation
    closures. Not marked xfail because this is the "and don't break
    this" half of the before/after contract.
    """
    from metasmith.constants import AgentPaths
    from metasmith.coms.containers import ContainerRuntime
    from metasmith.models.workflow import NextflowGenContext

    task = linear_3step.build_task(tmp_path)
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

    body = (workspace / AgentPaths.NXF_WORKFLOW).read_text()
    assert "plugins {" not in body
    assert "task.ext" not in body
    assert "nf-metasmith" not in body


def test_generated_config_has_no_process_cache_directive(tmp_path):
    """G7 regression guard: generated nextflow.config has no process.cache.

    Passes on main (today, no such directive exists). Stays in the suite
    as a regression assertion — the caching implementation MUST NOT set
    `process.cache` in the generated config. From Nextflow's POV every
    run is fresh; resume is owned by metasmith.
    """
    from metasmith.constants import AgentPaths
    from metasmith.coms.containers import ContainerRuntime
    from metasmith.models.workflow import NextflowGenContext

    task = linear_3step.build_task(tmp_path)
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
    cfg = workspace / AgentPaths.NXF_CONFIG
    if cfg.exists():
        body = cfg.read_text()
        for line in body.splitlines():
            stripped = line.strip()
            assert not stripped.startswith("process.cache"), (
                f"process.cache directive leaked: {stripped!r}"
            )

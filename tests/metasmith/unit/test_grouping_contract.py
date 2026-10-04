from __future__ import annotations

import re


def _emit(tmp_path):
    from metasmith.constants import AgentPaths
    from metasmith.env import Runtime
    from metasmith.models.workflow import NextflowGenContext
    from tests.metasmith.cache.fixtures.cache_fixtures import parallel_then_group

    task = parallel_then_group.build_task(tmp_path)
    ws = tmp_path / "ws"
    ws.mkdir()
    task.PrepareNextflow(NextflowGenContext(
        workflow_file=AgentPaths.NXF_WORKFLOW,
        work_dir=ws,
        external_work=ws,
        home_dir=AgentPaths.HOME_ROOT,
        external_home=AgentPaths.HOME_ROOT,
        runtime=Runtime.DOCKER,
        resources_file=AgentPaths.NXF_RES,
    ))
    return (ws / "workflow.nf").read_text()


def test_group_is_called_without_a_plan_time_count(tmp_path):
    # The count comes from the sibling stamps at run time; the plan cannot
    # know it, because outputs are globs and optional branches emit nothing.
    body = _emit(tmp_path)
    calls = [l for l in body.splitlines() if "o.group(" in l]
    assert calls, "no o.group call was emitted"
    for call in calls:
        assert re.search(r"o\.group\('\w+', \[[^\]]*\], k, \d+, \[tk:", call), (
            f"o.group must take (by, streams, k, batch_size, cache) and nothing else: {call}"
        )


def test_the_seal_is_the_last_statement_of_the_workflow_body(tmp_path):
    body = _emit(tmp_path)
    main = body[body.index("main:"):body.index("publish:")]
    statements = [l.strip() for l in main.splitlines()[1:] if l.strip()]
    assert statements[-1] == "o.seal()", (
        f"the workflow body must end with o.seal(), so no release decision "
        f"reads a registry the body is still writing; it ends with {statements[-1]!r}"
    )
    assert sum(s == "o.seal()" for s in statements) == 1

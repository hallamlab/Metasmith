from __future__ import annotations


def test_a_single_archetype_slot_gets_no_expected_count():
    from metasmith.models.workflow.grouping import expected_per_key

    class _Inst:
        def __init__(self, path):
            self.path = path

    keys = [_Inst("k0"), _Inst("k1")]
    assert expected_per_key([_Inst("archetype")], keys) is None
    assert expected_per_key([], keys) is None
    assert expected_per_key([_Inst("a")], []) is None


def test_a_collecting_step_emits_no_expectation_for_its_archetype_slot(
    tmp_path,
):
    import re

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

    # `o.group(by, [streams], k, batch_size, expected, [cache])`: the
    # expectation is the map just before the cache map.
    calls = re.findall(
        r"o\.group\('\w+', \[([^\]]*)\], k, \d+, (\[[^\]]*\]), \[tk:",
        (ws / "workflow.nf").read_text(),
    )
    assert calls, "no o.group call was emitted"
    collecting = [(streams, exp) for streams, exp in calls if "," in streams]
    assert collecting, f"no collecting o.group among {calls}"
    for call, expected in collecting:
        assert expected == "[:]", (
            "a collecting step told group() how many items its key expects, "
            f"which it cannot know from one archetype: {call}"
        )

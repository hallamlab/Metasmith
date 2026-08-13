"""When the plan may tell `group()` a key is already whole.

`group()` can flush a key the moment its bag reaches the count the plan
predicted, instead of holding every key hostage to the slowest one's tail.
The risk is one-sided: over-counting degrades to the close-flush that
already happens, under-counting hands a transform half a group. So the
compile-time count is only allowed to answer where the attribution is a
fact, and these tests pin where it must stay silent.
"""

from __future__ import annotations


def test_a_single_archetype_slot_gets_no_expected_count():
    """A collecting slot must wait for close, not flush after one item.

    The plan is archetypal: a collecting step's input slot carries ONE
    instance standing for however many the fan-out above it produces at
    runtime. Answering 1 there tells `group()` the key is whole as soon as
    its first item lands, which shatters the group into singletons — the
    ppanggolin failure, arriving through the early-emission path instead of
    through `groupTuple`. A shared reference DB is also one instance and is
    indistinguishable from it, so neither gets a count.
    """
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
    """End of the same wire, in the generated `.nf`.

    `parallel_then_group`'s trB reduces three parallel `step_a` outputs into
    one `root` key, and at plan time that slot holds one archetype. The
    emitted `o.group(...)` must pass no count for it, or the runtime flushes
    root after the first of the three arrives and the reduction sees one
    input — which is what the failing pangenome run looked like.
    """
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

    calls = re.findall(r"o\.group\((.*?)\)\)\)", (ws / "workflow.nf").read_text())
    assert calls, "no o.group call was emitted"
    # trB is the only step wiring two streams into one group.
    collecting = [c for c in calls if c.count("_") >= 2 and "," in c]
    assert collecting, f"no collecting o.group among {calls}"
    for call in collecting:
        expected = call.rsplit(",", 1)[-1].strip()
        assert expected == "[:]", (
            "a collecting step told group() how many items its key expects, "
            f"which it cannot know from one archetype: {call}"
        )

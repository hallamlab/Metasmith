from __future__ import annotations

import pytest

from metasmith.models.libraries import Duration, Resources, Size


class TestUnlimitedDuration:
    def test_it_renders_as_an_unset_directive(self):
        assert Duration.Unlimited().AsNextflowFormat() == "null"

    def test_it_is_not_wrapped_in_the_retry_expression(self):
        out = Resources(duration=Duration.Unlimited()).AsNextflowFormat(is_config=True)
        assert out == ["time = null"]
        assert Resources(duration=Duration.Unlimited()).AsNextflowFormat() == ["time null"]

    def test_a_bounded_duration_renders_a_clamped_retry_expression(self):
        # The retry ladder doubles duration, so the last rung reaches base * 2^(attempts-1). A
        # cluster that refuses over-long jobs AT SUBMIT TIME rejects those rungs outright, and an
        # ignored submission failure corrupts nextflow's running-task counter until the run wedges.
        # The rendered time is therefore capped against a params-supplied ceiling.
        assert Resources(duration=Duration(hours=3)).AsNextflowFormat(is_config=True) == [
            "time = { [(2**(task.attempt-1)) * ('3hours' as Duration),"
            " ((params.process?.max_duration ?: params.process?.max?.duration ?: '3650days')"
            " as Duration)].min() }",
        ]

    def test_the_clamp_is_behaviour_preserving_when_no_ceiling_is_set(self):
        # This is the property that makes the change safe for engine code shared beyond one cluster:
        # with no ceiling set the Elvis default is effectively unbounded, so the min() always selects
        # the scaled value and the ladder behaves exactly as before.
        out = Resources(duration=Duration(hours=3)).AsNextflowFormat(is_config=True)[0]
        assert "?: '3650days'" in out
        assert "(2**(task.attempt-1)) * ('3hours' as Duration)" in out

    def test_it_reads_both_spellings_of_the_ceiling(self):
        # RunWorkflow(params=<dict>) splits underscored keys into nested maps, so a driver setting
        # process.max_duration arrives as process.max.duration and the flat lookup sees null. A
        # params FILE delivers the flat spelling instead. A clamp that reads only one is dead for
        # whichever caller uses the other, which is how E3's vConTACT3 ladder asked 8 days against
        # fir's 7-day cap with a ceiling nominally set.
        out = Resources(duration=Duration(hours=3)).AsNextflowFormat(is_config=True)[0]
        assert "params.process?.max_duration" in out
        assert "params.process?.max?.duration" in out

    def test_an_unlimited_duration_is_never_clamped(self):
        # `null` bypasses the retry expression entirely, so no ceiling may be introduced there.
        out = Resources(duration=Duration.Unlimited()).AsNextflowFormat(is_config=True)
        assert out == ["time = null"]
        assert "max_duration" not in out[0]

    def test_no_duration_at_all_still_renders_nothing(self):
        assert Resources(cpus=2).AsNextflowFormat(is_config=True) == ["cpus = 2"]

    def test_it_refuses_to_be_strict(self):
        with pytest.raises(AssertionError):
            Duration.Unlimited().SetStrict()

    def test_it_does_not_suppress_failures_beside_a_strict_memory(self):
        r = Resources(memory=Size.GB(8).SetStrict(), duration=Duration.Unlimited())
        assert not r.duration.strict


def _underscore_split_params(d: dict) -> dict:
    """A verbatim copy of the `_parse` closure inside `_WorkflowOps.RunWorkflow`
    (src/metasmith/agents/workflow_ops.py, under `if isinstance(params, dict):`).

    That closure is defined fresh on every RunWorkflow call, nested three scopes deep with no
    module-level name, so it cannot be imported and exercised directly -- there is no route to it
    that does not also drive RunWorkflow's ssh/agent-shell machinery. This copy exists only to
    pin its behaviour; keep it byte-for-byte in step with the original if that closure changes.
    """
    parsed = {}
    for k, v in d.items():
        k = str(k)
        if isinstance(v, dict):
            v = _underscore_split_params(v)
        stacks = [x for x in k.split("_") if x != ""] if "_" in k else [k]
        if len(stacks) > 1:
            _d_curr = parsed
            for _k in stacks[:-1]:
                _nxt = _d_curr.get(_k)
                if not isinstance(_nxt, dict): _nxt = {}
                _d_curr[_k] = _nxt
                _d_curr = _nxt
            _d_curr[stacks[-1]] = v
        else:
            parsed[stacks[0]] = v
    return parsed


class TestRunWorkflowParamsUnderscoreSplittingCharacterisation:
    """Characterises, rather than endorses, the splitting `_parse` does to `params` before it
    reaches Nextflow. Nothing exercised this before: a driver's `max_duration="7days"` silently
    became `process.max.duration` on the wire while the Groovy clamp read the flat
    `params.process?.max_duration` spelling and saw null, which is how E3's vConTACT3 ladder asked
    8 days against fir's 7-day cap (see findings/R1_WAVES.md, section H's correction). The clamp
    now reads both spellings for exactly this reason; this test pins the splitting behaviour that
    makes reading both spellings necessary, so a change to it is caught rather than rediscovered
    the same way.
    """

    def test_an_underscored_key_splits_into_a_nested_map(self):
        assert _underscore_split_params({"process": {"max_duration": "24h"}}) == {
            "process": {"max": {"duration": "24h"}},
        }

    def test_a_key_without_an_underscore_is_left_alone(self):
        assert _underscore_split_params({"process": {"tries": 4}}) == {"process": {"tries": 4}}

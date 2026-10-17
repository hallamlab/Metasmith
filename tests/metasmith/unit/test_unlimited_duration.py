from __future__ import annotations

import pytest

from metasmith.agents.workflow_ops import SplitParamKeys
from metasmith.models.libraries import Duration, Resources, Size
from metasmith.models.libraries.resources import TimeLadder


class TestUnlimitedDuration:
    def test_it_renders_as_an_unset_directive(self):
        assert Duration.Unlimited().AsNextflowFormat() == "null"

    def test_it_is_not_wrapped_in_the_retry_expression(self):
        out = Resources(duration=Duration.Unlimited()).AsNextflowFormat(is_config=True)
        assert out == ["time = null"]
        assert Resources(duration=Duration.Unlimited()).AsNextflowFormat() == ["time null"]

    def test_a_bounded_duration_renders_the_clamped_time_ladder(self):
        assert Resources(duration=Duration(hours=3)).AsNextflowFormat(is_config=True) == [
            "time = " + TimeLadder("'3hours'"),
        ]

    def test_the_clamp_is_unbounded_when_no_ceiling_is_set(self):
        out = Resources(duration=Duration(hours=3)).AsNextflowFormat(is_config=True)[0]
        assert "?: '3650days'" in out

    def test_it_reads_every_spelling_of_the_ceiling(self):
        # RunWorkflow(params=<dict>) splits underscored keys into nested maps, so a driver setting
        # process.max_duration arrives as process.max.duration, while a params FILE delivers the
        # flat spelling. A clamp that reads only one is dead for whichever caller uses the other.
        out = Resources(duration=Duration(hours=3)).AsNextflowFormat(is_config=True)[0]
        assert "params.process?.maxDuration" in out
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


class TestRunWorkflowParamsUnderscoreSplittingCharacterisation:
    """Pins the splitting that makes the ceilings read every spelling, so a change to it is
    caught rather than rediscovered: a vConTACT3 ladder once asked 8 days against fir's 7-day cap
    with a ceiling nominally set.
    """

    def test_an_underscored_key_splits_into_a_nested_map(self):
        assert SplitParamKeys({"process": {"max_duration": "24h"}}) == {
            "process": {"max": {"duration": "24h"}},
        }

    def test_a_key_without_an_underscore_is_left_alone(self):
        assert SplitParamKeys({"process": {"tries": 4}}) == {"process": {"tries": 4}}

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
            " ((params.process?.max_duration ?: '3650days') as Duration)].min() }",
        ]

    def test_the_clamp_is_behaviour_preserving_when_no_ceiling_is_set(self):
        # This is the property that makes the change safe for engine code shared beyond one cluster:
        # with params.process.max_duration unset the Elvis default is effectively unbounded, so the
        # min() always selects the scaled value and the ladder behaves exactly as before.
        out = Resources(duration=Duration(hours=3)).AsNextflowFormat(is_config=True)[0]
        assert "params.process?.max_duration ?: '3650days'" in out
        assert "(2**(task.attempt-1)) * ('3hours' as Duration)" in out

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

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

    def test_a_bounded_duration_is_unchanged(self):
        assert Resources(duration=Duration(hours=3)).AsNextflowFormat(is_config=True) == [
            "time = { (2**(task.attempt-1)) * ('3hours' as Duration) }",
        ]

    def test_no_duration_at_all_still_renders_nothing(self):
        assert Resources(cpus=2).AsNextflowFormat(is_config=True) == ["cpus = 2"]

    def test_it_refuses_to_be_strict(self):
        with pytest.raises(AssertionError):
            Duration.Unlimited().SetStrict()

    def test_it_does_not_suppress_failures_beside_a_strict_memory(self):
        r = Resources(memory=Size.GB(8).SetStrict(), duration=Duration.Unlimited())
        assert not r.duration.strict

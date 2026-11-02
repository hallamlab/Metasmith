"""Which steps a run reports as failed and ignored, from nextflow's task table."""

import pandas as pd

from metasmith.agents.runner import _failed_steps


def _tasks(*rows):
    return pd.DataFrame(rows, columns=["name", "status", "exit"])


def test_a_retry_that_passed_is_not_reported_as_ignored():
    df = _tasks(
        ("p35__memote_score (25)", "FAILED", "1"),
        ("p35__memote_score (25)", "COMPLETED", "0"),
    )
    assert _failed_steps(df) == []


def test_a_step_whose_every_attempt_failed_is_reported_once():
    df = _tasks(
        ("p34__carveme_from_orfs (3)", "FAILED", "140"),
        ("p34__carveme_from_orfs (3)", "FAILED", "140"),
        ("p34__carveme_from_orfs (4)", "COMPLETED", "0"),
    )
    assert _failed_steps(df) == ["p34__carveme_from_orfs (3)"]


def test_an_aborted_step_and_a_nonzero_completion_are_failures():
    df = _tasks(
        ("p09__clean (1)", "ABORTED", "-"),
        ("p10__checkm (2)", "COMPLETED", "1"),
    )
    assert _failed_steps(df) == ["p09__clean (1)", "p10__checkm (2)"]

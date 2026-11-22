"""Which steps a run reports as failed, from nextflow's task table and its log."""

import pandas as pd

from metasmith.agents.runner import _failed_steps, _nextflow_failure


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


def test_an_aborted_nextflow_names_its_first_error(tmp_path):
    log = tmp_path / "nxf.log"
    log.write_text(
        "Nov-19 10:00:00.000 [main] DEBUG nextflow.cli.Launcher - $> nextflow run\n"
        "Nov-19 10:30:00.000 [Task monitor] ERROR nextflow.processor.TaskProcessor"
        " - Error executing process > 'p03__fetch_source (1)'\n"
        "Nov-19 10:30:01.000 [main] ERROR nextflow.cli.Launcher - @unknown\n"
    )
    assert _nextflow_failure(1, log) == "nextflow exited [1]: Error executing process > 'p03__fetch_source (1)'"


def test_an_aborted_nextflow_with_no_log_points_at_it(tmp_path):
    log = tmp_path / "nxf.log"
    assert _nextflow_failure(1, log) == f"nextflow exited [1]: see [{log}]"

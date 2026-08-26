"""How often a quiet run says it is still working.

Criterion 7 of the annotation-trio investigation. The reporter watched
ninety-five minutes of nothing, because `wget -q` says nothing and the agent
log is exactly as chatty as nextflow is. The fix has to stay cheap: a line
every five minutes for a twelve-hour InterProScan step is 144 lines of noise
that hides everything else in the log.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from metasmith.agents.runner import (
    HEARTBEAT_GAPS_S, _heartbeat_line, _running_tasks, heartbeat_marks,
)


HOUR = 60 * 60


def test_a_run_that_is_talking_never_beats():
    assert heartbeat_marks(0) == []
    assert heartbeat_marks(HEARTBEAT_GAPS_S[0] - 1) == []


def test_the_first_word_comes_after_five_minutes():
    assert heartbeat_marks(HEARTBEAT_GAPS_S[0]) == [HEARTBEAT_GAPS_S[0]]


def test_twelve_silent_hours_cost_about_fifteen_lines():
    marks = heartbeat_marks(12 * HOUR)
    assert 10 <= len(marks) <= 20, [m / 60 for m in marks]


def test_the_gaps_only_widen():
    marks = heartbeat_marks(12 * HOUR)
    gaps = [b - a for a, b in zip(marks, marks[1:])]
    assert gaps == sorted(gaps), [g / 60 for g in gaps]
    assert gaps[-1] == HEARTBEAT_GAPS_S[-1]


class _Plan:
    steps: list = []


class _Task:
    plan = _Plan()


def test_a_step_with_an_exit_code_is_not_running(tmp_path):
    work = tmp_path / "nxf_work" / "ab" / "cdef01"
    work.mkdir(parents=True)
    (work / ".command.begin").write_text("")
    assert len(_running_tasks(tmp_path, _Task())) == 1
    (work / ".exitcode").write_text("0\n")
    assert _running_tasks(tmp_path, _Task()) == []


def test_the_line_says_so_when_nothing_is_running(tmp_path):
    (tmp_path / "nxf_work").mkdir()
    line = _heartbeat_line(tmp_path, _Task(), 20 * 60)
    assert "no step is running" in line
    assert "20 min" in line

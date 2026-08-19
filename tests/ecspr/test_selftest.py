from __future__ import annotations

import io

import pytest

from ecspr import selftest


def test_this_env_can_actually_measure():
    assert selftest.run(io.StringIO()) == 0


def test_a_failing_check_stops_the_command_and_the_rest_still_report(monkeypatch):
    ran = []

    def ok():
        ran.append("ok")
        return "fine"

    def broken():
        ran.append("broken")
        raise RuntimeError("no cholmod here")

    monkeypatch.setattr(selftest, "CHECKS", [broken, ok])
    out = io.StringIO()
    with pytest.raises(SystemExit) as e:
        selftest.run(out)
    assert "1 of 2 install checks failed" in str(e.value)
    assert ran == ["broken", "ok"], "a failure must not hide the checks after it"
    assert "no cholmod here" in out.getvalue()

"""Deploy resolves three paths on the agent host, and says which one it could not."""

from __future__ import annotations

import pytest

from metasmith.agents.agent import _resolve_deploy_paths
from metasmith.coms.terminals import ShellResult


class _Shell:
    def __init__(self, out: list[str]):
        self.out = out

    def Exec(self, cmd, **_):
        return ShellResult(out=self.out, err=[], exit_code=0)


def test_a_login_banner_does_not_shift_the_answers():
    shell = _Shell([
        "Welcome to fir",
        "MSM_PROBE|agent home|/scratch/me/msm",
        "MSM_PROBE|user home|/home/me",
        "MSM_PROBE|hostname|fir2",
    ])
    assert _resolve_deploy_paths(shell, "/scratch/me/msm") == ["/scratch/me/msm", "/home/me", "fir2"]


def test_a_dropped_answer_is_named():
    shell = _Shell([
        "MSM_PROBE|agent home|/scratch/me/msm",
        "MSM_PROBE|hostname|fir2",
    ])
    with pytest.raises(RuntimeError, match="user home"):
        _resolve_deploy_paths(shell, "/scratch/me/msm")


def test_a_failed_realpath_is_named_with_its_error():
    shell = _Shell([
        "MSM_PROBE|agent home|realpath: /gone: No such file or directory",
        "MSM_PROBE|user home|/home/me",
        "MSM_PROBE|hostname|fir2",
    ])
    with pytest.raises(RuntimeError, match="agent home.*No such file"):
        _resolve_deploy_paths(shell, "/gone")

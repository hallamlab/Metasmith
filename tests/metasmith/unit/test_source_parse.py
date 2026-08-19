from __future__ import annotations

import os
from pathlib import Path

from metasmith.models.remote import Source, SourceType


def test_parse_relative_path_resolves(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    s = Source.Parse("./out")
    assert s.type in (SourceType.DIRECT, SourceType.SYMLINK)
    assert Path(s.address).is_absolute()
    assert Path(s.address) == (tmp_path / "out").resolve()


def test_parse_bare_relative_path_resolves(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    s = Source.Parse("out/sub")
    assert Path(s.address).is_absolute()
    assert Path(s.address) == (tmp_path / "out" / "sub").resolve()


def test_parse_absolute_path_unchanged(tmp_path):
    abs_path = str(tmp_path / "x")
    s = Source.Parse(abs_path)
    assert Path(s.address) == Path(abs_path).resolve()


def test_fromlocal_still_rejects_relative_when_called_directly(tmp_path, monkeypatch):
    import pytest

    monkeypatch.chdir(tmp_path)
    with pytest.raises(AssertionError, match="Path must be absolute"):
        Source.FromLocal("./relative")


def test_parse_expands_home(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.chdir(tmp_path)
    s = Source.Parse("~/msm_home")
    assert Path(s.address) == (tmp_path / "msm_home").resolve()


class TestSshRoundTrip:
    def test_colon_form_parses(self):
        s = Source.Parse("ssh://sockeye:/scratch/you/msm_home")
        assert s.type is SourceType.SSH
        assert s.address == "ssh://sockeye:/scratch/you/msm_home"

    def test_repeated_parse_is_a_fixed_point(self):
        address = "ssh://sockeye:/scratch/you/msm_home"
        for _ in range(5):
            address = Source.Parse(address).address
        assert address == "ssh://sockeye:/scratch/you/msm_home"

    def test_remote_home_relative_path_survives(self):
        assert Source.Parse("ssh://sockeye:~/msm_home").address == "ssh://sockeye:~/msm_home"

    def test_slash_form_still_parses(self):
        assert Source.Parse("ssh://sockeye/scratch/x").address == "ssh://sockeye:/scratch/x"

    def test_host_survives_to_the_ssh_command(self):
        from metasmith.models.remote import SshSource

        src = SshSource.Parse(Source.Parse("ssh://sockeye:/scratch/x").address)
        assert src.host == "sockeye"
        assert str(src.path) == "/scratch/x"

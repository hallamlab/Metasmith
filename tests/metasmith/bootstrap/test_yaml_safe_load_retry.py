import builtins
from pathlib import Path

import pytest

import metasmith.models.libraries as libmod
from metasmith.models.libraries import yaml_safe_load


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    monkeypatch.setattr(libmod.time, "sleep", lambda *_: None)


def test_retries_transient_oserror_then_succeeds(tmp_path, monkeypatch):
    p = tmp_path / "index.yml"
    p.write_text("a: 1\n")

    real_open = builtins.open
    calls = {"n": 0}

    def flaky_open(file, *args, **kwargs):
        if Path(file) == p and calls["n"] < 3:
            calls["n"] += 1
            raise OSError(108, "Cannot send after transport endpoint shutdown")
        return real_open(file, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", flaky_open)

    assert yaml_safe_load(p) == {"a": 1}
    assert calls["n"] == 3


def test_persistent_oserror_eventually_raises(tmp_path, monkeypatch):
    p = tmp_path / "index.yml"
    p.write_text("a: 1\n")

    def always_fail(file, *args, **kwargs):
        raise OSError(108, "Cannot send after transport endpoint shutdown")

    monkeypatch.setattr(builtins, "open", always_fail)

    with pytest.raises(AssertionError):
        yaml_safe_load(p)


def test_still_retries_empty_parse(tmp_path, monkeypatch):
    p = tmp_path / "index.yml"
    p.write_text("a: 1\n")

    real_open = builtins.open
    reads = {"n": 0}

    def empty_then_full(file, *args, **kwargs):
        if Path(file) == p and reads["n"] < 2:
            reads["n"] += 1
            empty = tmp_path / "_empty.yml"
            empty.write_text("")
            return real_open(empty, *args, **kwargs)
        return real_open(file, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", empty_then_full)

    assert yaml_safe_load(p) == {"a": 1}

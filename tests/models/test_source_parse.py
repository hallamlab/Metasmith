"""Regression tests for `Source.Parse` accepting relative local paths.

Bug C (session #168, v0.18.1): `metasmith workflow collect --dest ./out`
crashed because `Source.Parse` forwarded the raw string to `FromLocal`,
whose `assert path.is_absolute()` fired on relative URIs.

The fix resolves the path at the CLI ingress point (`Source.Parse`),
keeping the absolute-path invariant inside `FromLocal` as
defence-in-depth for direct callers.
"""
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
    """The invariant inside FromLocal is preserved as defence-in-depth."""
    import pytest

    monkeypatch.chdir(tmp_path)
    with pytest.raises(AssertionError, match="Path must be absolute"):
        Source.FromLocal("./relative")

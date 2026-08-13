"""Regression test for Bug F — malformed `_metadata/index.yml` should raise
a self-diagnosing `ValueError`, not a bare `KeyError: 'manifest'`.

The original failure surfaced in session #168
(2026-05-26, custom_transforms[DOCKER]): the agent hand-crafted an
`index.yml` without a `manifest` key (because the transform-lib
directory was empty — Bug B), then `metasmith transform scaffold` died
with `KeyError: 'manifest'` from `DataInstanceLibrary.Unpack`. That
error gave no hint about *which* file was wrong or that the directory
just needed `metasmith build`. This test pins the better error message
so future driftless agents (and humans) get a pointer instead of a
stack trace.
"""
from __future__ import annotations

import pytest
import yaml

from metasmith.models.libraries import DataInstanceLibrary


def _write_index(meta_dir, payload: dict) -> None:
    meta_dir.mkdir(parents=True, exist_ok=True)
    (meta_dir / "index.yml").write_text(yaml.safe_dump(payload))


def test_unpack_missing_manifest_raises_self_diagnosing_error(tmp_path):
    lib_dir = tmp_path / "broken_lib"
    meta_dir = lib_dir / "_metadata"
    types_dir = meta_dir / "types"
    types_dir.mkdir(parents=True)
    _write_index(meta_dir, {"schema": "1.0"})

    with pytest.raises(ValueError, match="missing 'manifest' key"):
        DataInstanceLibrary.Load(lib_dir)


def test_unpack_malformed_error_names_the_file(tmp_path):
    lib_dir = tmp_path / "broken_lib"
    meta_dir = lib_dir / "_metadata"
    types_dir = meta_dir / "types"
    types_dir.mkdir(parents=True)
    _write_index(meta_dir, {"schema": "1.0"})

    with pytest.raises(ValueError) as exc:
        DataInstanceLibrary.Load(lib_dir)
    msg = str(exc.value)
    assert "index.yml" in msg
    assert "metasmith build" in msg

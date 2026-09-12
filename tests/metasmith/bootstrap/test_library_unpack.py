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

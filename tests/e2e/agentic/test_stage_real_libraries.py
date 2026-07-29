"""Unit test for stage_real_libraries: full MetasmithLibraries clone in sandbox."""
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from tests.e2e.agentic.scenarios._fixture_utils import (
    _metasmith_libraries_root,
    stage_real_libraries,
)


def test_stage_real_libraries_clones_into_sandbox(tmp_path: Path) -> None:
    try:
        _metasmith_libraries_root()
    except RuntimeError as exc:
        pytest.skip(f"MetasmithLibraries unavailable: {exc}")

    layout = SimpleNamespace(root=tmp_path)
    dest = stage_real_libraries(layout)  # type: ignore[arg-type]

    assert dest == tmp_path / "MetasmithLibraries"
    assert (dest / ".git").exists(), "sandbox checkout should preserve .git provenance"
    assert (dest / "data_types" / "containers.yml").exists()
    assert (dest / "resources" / "containers" / "_metadata" / "index.yml").exists()
    assert (dest / "resources" / "lib" / "_metadata").exists()
    assert (dest / "transforms" / "logistics").is_dir()
    assert (dest / "transforms" / "pangenome").is_dir()

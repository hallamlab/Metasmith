"""Tests for the decomposed Build pipeline in models/build_libraries.py."""
from __future__ import annotations

from pathlib import Path

import pytest

from metasmith.models.build_libraries import (
    LoadTypeLibraries,
    CompileUniqueLibrary,
    CompileTransformLibrary,
    Build,
)
from metasmith.models.libraries import DataTypeLibrary, TransformInstanceLibrary
from metasmith.models.solver import Endpoint


@pytest.fixture
def type_dir(tmp_path):
    d = tmp_path / "types"
    d.mkdir()
    good = DataTypeLibrary()
    good["foo"] = Endpoint(properties={"foo"})
    good.Save(d / "good.yml")

    disabled = DataTypeLibrary()
    disabled["bar"] = Endpoint(properties={"bar"})
    disabled.Save(d / "_disabled.yml")
    return d


class TestLoadTypeLibraries:
    def test_loads_good_skips_disabled(self, type_dir):
        types = LoadTypeLibraries([type_dir])
        assert "good" in types
        assert "_disabled" not in types

    def test_skips_dotfiles(self, type_dir):
        (type_dir / ".hidden.yml").touch()
        types = LoadTypeLibraries([type_dir])
        assert "good" in types and len(types) == 1

    def test_duplicate_namespace_raises(self, tmp_path):
        d1 = tmp_path / "a"; d2 = tmp_path / "b"
        d1.mkdir(); d2.mkdir()
        lib = DataTypeLibrary()
        lib["x"] = Endpoint(properties={"x"})
        lib.Save(d1 / "shared.yml")
        lib.Save(d2 / "shared.yml")
        with pytest.raises(ValueError, match="duplicate type namespace"):
            LoadTypeLibraries([d1, d2])


class TestCompileUniqueLibrary:
    def test_missing_namespace_clear_error(self, tmp_path, type_dir):
        u = tmp_path / "nonexistent_ns"
        u.mkdir()
        types = LoadTypeLibraries([type_dir])
        with pytest.raises(KeyError, match="no matching type library"):
            CompileUniqueLibrary(u, types)

    def test_skips_filtered_dir(self, tmp_path):
        d = tmp_path / "_skipme"
        d.mkdir()
        r = CompileUniqueLibrary(d, {})
        assert r["count"] == 0 and r.get("skipped") == "filtered"


_EXAMPLE_TRANSFORM_SRC = '''from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
dep   = model.AddRequirement(lib.GetType("transforms::example input"))
out   = model.AddProduct(lib.GetType("transforms::example output"))

def protocol(context: ExecutionContext):
    return ExecutionResult(manifest=[], success=True)

TransformInstance(protocol=protocol, model=model, group_by=dep)
'''


class TestCompileTransformLibrary:
    def test_returns_count_and_skips_disabled(self, tmp_path, type_dir):
        d = tmp_path / "tr"
        d.mkdir()
        (d / "t1.py").write_text(_EXAMPLE_TRANSFORM_SRC)
        (d / "_disabled_skip.py").write_text(_EXAMPLE_TRANSFORM_SRC)
        types = LoadTypeLibraries([type_dir])
        r = CompileTransformLibrary(d, types)
        assert r["count"] == 1

    def test_no_transforms_no_save(self, tmp_path, type_dir):
        d = tmp_path / "empty"
        d.mkdir()
        types = LoadTypeLibraries([type_dir])
        r = CompileTransformLibrary(d, types)
        assert r["count"] == 0
        # nothing should have been written
        assert not (d / "_metadata").exists()


class TestBuildOrchestrator:
    def test_aggregates_results(self, tmp_path, type_dir):
        tr = tmp_path / "tr"; tr.mkdir()
        (tr / "x.py").write_text(_EXAMPLE_TRANSFORM_SRC)
        r = Build(
            data_type_dirs=[type_dir],
            transform_dirs=[tr],
            unique_dirs=[],
        )
        assert "good" in r["types"]
        assert r["transforms"][0]["count"] == 1
        assert r["uniques"] == []

    def test_fails_fast_on_unknown_unique_ns(self, tmp_path, type_dir):
        u = tmp_path / "ghost_ns"
        u.mkdir()
        with pytest.raises(KeyError, match="without matching type namespaces"):
            Build(data_type_dirs=[type_dir], transform_dirs=[], unique_dirs=[u])

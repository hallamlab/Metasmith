from __future__ import annotations

from pathlib import Path

import pytest

from metasmith.models.libraries import DataInstanceLibrary, DataTypeLibrary
from metasmith.models.solver import Endpoint, Transform


@pytest.fixture
def types_dir(tmp_path) -> Path:
    d = tmp_path/"types"
    d.mkdir()
    for namespace, names in {
        "used": ["reads", "assembly"],
        "declared": ["tool_env"],
        "orphan": ["nothing_points_here"],
    }.items():
        lib = DataTypeLibrary()
        for n in names:
            lib[n] = Endpoint(properties={namespace, n})
        lib.Save(d/f"{namespace}.yml")
    return d


def _build(lib_path: Path, types_dir: Path) -> DataInstanceLibrary:
    lib = DataInstanceLibrary(lib_path)
    for p in sorted(types_dir.iterdir()):
        lib.AddTypeLibrary(p, namespace=p.stem)
    (lib_path/"reads.fq").write_text("")
    lib.AddItem(Path("reads.fq"), "used::reads")
    lib.Save()
    return lib


def _on_disk(lib: DataInstanceLibrary) -> set[str]:
    return {p.stem for p in (lib.location/lib._path_to_types).iterdir()}


def test_namespace_named_by_no_one_is_dropped_from_memory_and_disk(tmp_path, types_dir):
    lib = _build(tmp_path/"lib", types_dir)
    lib.PruneTypes(save=True)

    assert set(lib.types) == {"used"}
    assert _on_disk(lib) == {"used"}
    assert set(DataInstanceLibrary.Load(lib.location).types) == {"used"}


def test_a_whitelisted_dependency_keeps_its_namespace(tmp_path, types_dir):
    lib = _build(tmp_path/"lib", types_dir)
    model = Transform()
    dep = model.AddRequirement(lib.GetType("declared::tool_env"))

    lib.PruneTypes(save=True, whitelist={dep})

    # The Dependency carries the caller's parents, so it does not hash equal to
    # the Endpoint it was cloned from -- matching has to be on properties.
    assert set(lib.types) == {"used", "declared"}
    assert _on_disk(lib) == {"used", "declared"}


def test_a_type_named_only_by_a_parent_entry_survives(tmp_path, types_dir):
    upstream = _build(tmp_path/"upstream", types_dir)
    upstream_reads = upstream.Get(Path("reads.fq"))

    lib = DataInstanceLibrary(tmp_path/"downstream")
    for p in sorted(types_dir.iterdir()):
        lib.AddTypeLibrary(p, namespace=p.stem)
    (lib.location/"asm.fa").write_text("")
    lib.AddItem(Path("asm.fa"), "declared::tool_env")
    lib.AddParentsTo(Path("asm.fa"), [upstream_reads])
    lib.Save()

    lib.PruneTypes(save=True)

    # `used::reads` is named by no manifest entry of this library -- only by the
    # index's `parents:` block, which Unpack dereferences before any of them.
    assert "used" in lib.types
    reloaded = DataInstanceLibrary.Load(lib.location)
    assert reloaded.parents[Path("asm.fa")][0].name == "used::reads"


def test_save_false_leaves_the_type_files_alone(tmp_path, types_dir):
    lib = _build(tmp_path/"lib", types_dir)
    lib.PruneTypes(save=False)

    assert set(lib.types) == {"used"}
    assert _on_disk(lib) == {"used", "declared", "orphan"}


def test_pruning_types_does_not_move_the_library_key(tmp_path, types_dir):
    lib = _build(tmp_path/"lib", types_dir)
    before = lib.GetKey()
    lib.PruneTypes(save=True)

    assert DataInstanceLibrary.Load(lib.location).GetKey() == before

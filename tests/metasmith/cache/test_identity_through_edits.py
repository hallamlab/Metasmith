from __future__ import annotations

from pathlib import Path

import pytest

from metasmith.models.libraries import DataInstanceLibrary, DataTypeLibrary
from metasmith.models.solver import Endpoint


@pytest.fixture
def types_path(tmp_path) -> Path:
    types = DataTypeLibrary()
    types["reads"] = Endpoint(properties={"reads"})
    types["assembly"] = Endpoint(properties={"assembly"})
    p = tmp_path / "types.yml"
    types.Save(p)
    return p


def _lib(location: Path, types_path: Path) -> DataInstanceLibrary:
    lib = DataInstanceLibrary(location)
    lib.AddTypeLibrary(types_path, namespace="mock")
    (lib.location / "s0").mkdir(parents=True, exist_ok=True)
    (lib.location / "s0" / "reads.fq").write_text(">r\nACGT\n")
    lib.AddItem(Path("s0/reads.fq"), "mock::reads")
    lib.Save()
    return lib


def test_rename_re_mints_a_leaf_id_to_match_a_fresh_registration(tmp_path, types_path):
    from metasmith.models.libraries.identity import stat_leaf_id

    lib = _lib(tmp_path / "a.xgdb", types_path)
    old_id = lib.Get(Path("s0/reads.fq")).instance_id

    lib.Rename(Path("s0/reads.fq"), Path("s0/renamed.fq"))
    new_id = lib.Get(Path("s0/renamed.fq")).instance_id

    assert new_id != old_id, "leaf id ignored the path it folds in"
    assert new_id == stat_leaf_id(lib.location / "s0" / "renamed.fq"), (
        "a renamed leaf and a fresh registration of the file now at that path "
        "disagree on identity"
    )


def test_rename_carries_a_lineage_id_verbatim(tmp_path, types_path):
    lib = _lib(tmp_path / "a.xgdb", types_path)
    (lib.location / "s0" / "out.fa").write_text(">c\nACGTACGT\n")
    lib.AddItem(Path("s0/out.fa"), "mock::assembly")
    lib.SetLineageInstance(
        Path("s0/out.fa"),
        instance_id="1e20cafe",
        lineage_payload=b"payload",
    )

    lib.Rename(Path("s0/out.fa"), Path("s0/moved.fa"))

    meta = lib.instance_meta[Path("s0/moved.fa")]
    assert meta["instance_id"] == "1e20cafe"
    assert meta["origin"] == "lineage"
    assert Path("s0/out.fa") not in lib.instance_meta


def test_remove_drops_the_identity_entry(tmp_path, types_path):
    lib = _lib(tmp_path / "a.xgdb", types_path)
    p = Path("s0/reads.fq")
    old_id = lib.Get(p).instance_id

    lib.Remove(p)
    assert p not in lib.instance_meta

    (lib.location / "s0" / "reads.fq").write_text(">r\nTTTTTTTT\n")
    lib.AddItem(p, "mock::reads")
    assert lib.Get(p).instance_id != old_id, (
        "re-adding different bytes at a removed path reused the stale id"
    )


def test_rename_by_parent_migrates_identity(tmp_path, types_path):
    lib = DataInstanceLibrary(tmp_path / "a.xgdb")
    lib.AddTypeLibrary(types_path, namespace="mock")
    (lib.location / "s0").mkdir(parents=True, exist_ok=True)
    (lib.location / "s0" / "sample_alpha.fa").write_text(">a\nAAAA\n")
    (lib.location / "s0" / "reads.fq").write_text(">r\nACGT\n")
    parent = lib.AddItem(Path("s0/sample_alpha.fa"), "mock::assembly")
    lib.AddItem(Path("s0/reads.fq"), "mock::reads", parents=[parent])
    lib.Save()

    lib.RenameByParent("mock::assembly")

    renamed = Path("s0/sample_alpha.fq")
    assert renamed in lib.manifest, f"expected rename, got {list(lib.manifest)}"
    assert Path("s0/reads.fq") not in lib.instance_meta, (
        "the pre-rename identity entry was left behind"
    )
    assert lib.Get(renamed).instance_id

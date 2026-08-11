"""Identity entries must survive the library's own edit operations.

`Rename`, `Remove` and `RenameByParent` migrate `manifest` and `parents`.
`instance_meta` arrived later and was not added to any of them, which was
harmless while it was only bookkeeping and stopped being harmless once
instance ids became what cache keys are made of. The GUI's retype and
repoint controls promote the latent bug to a one-click action.

Two rules, and they differ because the two id kinds mean different things.
A leaf id folds the library-relative path, so it re-derives on a rename.
A lineage or imported id hashes how the output was produced and follows
the file wherever it goes.
"""

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


def test_rename_re_mints_a_leaf_id_to_match_a_fresh_build(tmp_path, types_path):
    """After a rename the id is what a library built at the new path holds.

    Anything else leaves one library state with two possible ids depending
    on how it got there, and two runs that should share a cache key stop
    sharing one.
    """
    lib = _lib(tmp_path / "a.xgdb", types_path)
    old_id = lib.Get(Path("s0/reads.fq")).instance_id

    lib.Rename(Path("s0/reads.fq"), Path("s0/renamed.fq"))
    new_id = lib.Get(Path("s0/renamed.fq")).instance_id

    assert new_id != old_id, "leaf id ignored the path it folds in"

    fresh = DataInstanceLibrary(tmp_path / "b.xgdb")
    fresh.AddTypeLibrary(types_path, namespace="mock")
    (fresh.location / "s0").mkdir(parents=True, exist_ok=True)
    (fresh.location / "s0" / "renamed.fq").write_text(">r\nACGT\n")
    fresh.AddItem(Path("s0/renamed.fq"), "mock::reads")

    assert new_id == fresh.Get(Path("s0/renamed.fq")).instance_id, (
        "a renamed library and a fresh one over the same bytes at the same "
        "path disagree on identity"
    )


def test_rename_carries_a_lineage_id_verbatim(tmp_path, types_path):
    """A produced output's id hashes its provenance, not its location."""
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
    """A removed path must not hand its id to whatever is added there next.

    Without this, remove-then-re-add at the same path with different bytes
    resurrects the old id -- and a cache key built from it hits a shard
    produced from content that is no longer there.
    """
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
    """The bulk rename path gets the same treatment as the single one."""
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

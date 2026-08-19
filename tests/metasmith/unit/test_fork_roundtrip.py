# A forked library's ids survive a save/load round trip.
#
# `Pack` re-derives any entry whose `fork_id` disagrees with the library's before
# writing, so the ids on disk ARE the forked ids -- but `Unpack` never restored
# `fork_id` onto the entries it rebuilt. Every leaf therefore came back looking
# stale, and the first `Get()` after a `Load()` re-minted it: same file, same
# fork, a different id each time the library was reloaded, and a cache key that
# moved with it.
#
# Stamping the fork back on at unpack is idempotent precisely because `Pack`
# already reconciled them. The property this file pins is the round trip, not the
# fork mechanism itself: fork *changes* an id on purpose, and reloading must not.

from __future__ import annotations

from pathlib import Path

import yaml

from metasmith.models.libraries import DataInstanceLibrary


TYPES = {"schema": "v1", "types": {"thing": {"properties": ["thing"]}}}


def _library(tmp_path: Path, fork: str | None = None) -> DataInstanceLibrary:
    tmp_path.mkdir(parents=True, exist_ok=True)
    types_yml = tmp_path / "t.yml"
    types_yml.write_text(yaml.safe_dump(TYPES))
    lib = DataInstanceLibrary(tmp_path / "lib.xgdb")
    lib.AddTypeLibrary(types_yml, namespace="t")
    if fork is not None:
        lib.fork_id = fork
    (lib.location / "item.txt").write_text("contents\n")
    lib.AddItem(Path("item.txt"), "t::thing")
    lib.Save()
    return lib


def test_a_forked_library_reloads_to_the_same_ids(tmp_path):
    lib = _library(tmp_path, fork="experiment-2")
    before = lib.Get(Path("item.txt")).instance_id

    first = DataInstanceLibrary.Load(lib.location)
    second = DataInstanceLibrary.Load(lib.location)
    assert first.Get(Path("item.txt")).instance_id == before
    assert second.Get(Path("item.txt")).instance_id == before


def test_forking_still_changes_the_id(tmp_path):
    plain = _library(tmp_path / "a").Get(Path("item.txt")).instance_id
    forked = _library(tmp_path / "b", fork="experiment-2").Get(Path("item.txt")).instance_id
    assert plain != forked

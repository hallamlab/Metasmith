# A pinned library serves its recorded ids and refuses to change.
#
# What these cover is the *mechanism*: the refusals, the round trip, and that a
# pinned `Get()` never opens a file. What they cannot cover is whether the pin
# protects anything -- every failure mode listed at the top of
# `models/libraries/pinned.py` is outside what a test can reach (a same-mtime
# edit, a caller going around the API, a stamp taken on another host). That is
# precisely why they are documented rather than asserted, and a green run here is
# not evidence the data is untouched. `metasmith data verify --deep` is the only
# thing that answers that question.

from __future__ import annotations

import os
from pathlib import Path

import pytest
import yaml

from metasmith.models.libraries import DataInstanceLibrary
from metasmith.models.libraries.pinned import PinnedLibraryError


TYPES = {
    "schema": "v1",
    "types": {
        "thing": {"properties": ["thing"]},
        "other": {"properties": ["other"]},
    },
}


def _library(tmp_path: Path, n: int = 2) -> DataInstanceLibrary:
    types_yml = tmp_path / "t.yml"
    types_yml.write_text(yaml.safe_dump(TYPES))
    lib = DataInstanceLibrary(tmp_path / "lib.xgdb")
    lib.AddTypeLibrary(types_yml, namespace="t")
    for i in range(n):
        f = lib.location / f"item_{i}.txt"
        f.write_text(f"contents {i}\n")
        lib.AddItem(Path(f.name), "t::thing")
    lib.Save()
    return lib


def test_a_library_is_not_pinned_by_default(tmp_path):
    lib = _library(tmp_path)
    assert lib.is_pinned is False
    raw = yaml.safe_load((lib.location / "_metadata" / "index.yml").read_text())
    assert "pinned" not in raw, (
        "an unpinned library gained a `pinned:` key, so every committed index.yml"
        " now differs from what the code writes"
    )


@pytest.mark.parametrize("verb", ["AddItem", "AddValue", "Save", "Purge", "Remove",
                                  "RegisterItem", "PruneTypes"])
def test_a_pinned_library_refuses_every_mutation(tmp_path, verb):
    lib = _library(tmp_path)
    lib.Pin()
    calls = {
        "AddItem": lambda: lib.AddItem(Path("new.txt"), "t::thing"),
        "AddValue": lambda: lib.AddValue("v.txt", "x", "t::thing"),
        "Save": lambda: lib.Save(),
        "Purge": lambda: lib.Purge(),
        "Remove": lambda: lib.Remove(Path("item_0.txt")),
        "RegisterItem": lambda: lib.RegisterItem(Path("n.txt"), "t::thing", instance_id="ab"),
        "PruneTypes": lambda: lib.PruneTypes(),
    }
    with pytest.raises(PinnedLibraryError) as e:
        calls[verb]()
    assert verb in str(e.value)


def test_a_view_of_a_pinned_library_refuses_too(tmp_path):
    # The view delegates, and the delegation IS the enforcement.
    #
    # `DataInstanceLibraryView.__getattr__` forwards anything it does not override
    # to the wrapped library, so the refusal is inherited for free. Adding an
    # overriding wrapper on the view would reopen the bypass, which is what this
    # guards.
    lib = _library(tmp_path)
    lib.Pin()
    view = lib.AsView({Path("item_0.txt")})
    with pytest.raises(PinnedLibraryError):
        view.AddItem(Path("new.txt"), "t::thing")
    with pytest.raises(PinnedLibraryError):
        view.Save()


def test_the_copy_constructor_does_not_launder_a_pin(tmp_path):
    lib = _library(tmp_path)
    lib.Pin()
    copy = DataInstanceLibrary(lib)
    assert copy.is_pinned
    with pytest.raises(PinnedLibraryError):
        copy.AddItem(Path("new.txt"), "t::thing")


def test_a_pinned_get_touches_no_file(tmp_path, monkeypatch):
    # The whole point: a pinned library answers from what it recorded, without
    # consulting the filesystem at all -- so a `dvc checkout` that restores the
    # identical bytes under 24 GB of references cannot move a single id.
    lib = _library(tmp_path)
    lib.Pin()
    before = {p: lib.Get(p).instance_id for p in lib.manifest}

    import metasmith.models.libraries.identity as identity

    def _boom(*a, **k):
        raise AssertionError("a pinned library re-derived an identity")

    monkeypatch.setattr(identity, "stat_leaf_id", _boom)
    reloaded = DataInstanceLibrary.Load(lib.location)
    after = {p: reloaded.Get(p).instance_id for p in reloaded.manifest}
    assert after == before


def test_a_pinned_directory_entry_keeps_one_identity(tmp_path):
    # Directories are the weak case a pin most has to cover: a directory's own
    # mtime moves on any change to its immediate entries, and says nothing about
    # what is nested inside. A recorded id sidesteps both.
    types_yml = tmp_path / "t.yml"
    types_yml.write_text(yaml.safe_dump(TYPES))
    lib = DataInstanceLibrary(tmp_path / "lib.xgdb")
    lib.AddTypeLibrary(types_yml, namespace="t")
    d = lib.location / "adir"
    d.mkdir()
    (d / "inner.txt").write_text("x")
    lib.RegisterItem(Path("adir"), "t::thing", instance_id="deadbeef")
    lib.Pin()

    first = DataInstanceLibrary.Load(lib.location).Get(Path("adir")).instance_id
    second = DataInstanceLibrary.Load(lib.location).Get(Path("adir")).instance_id
    assert first == second == "deadbeef"


def test_a_moved_entry_makes_load_raise_and_restamp_clears_it(tmp_path):
    lib = _library(tmp_path)
    lib.Pin()
    target = lib.location / "item_0.txt"
    target.write_text("different contents entirely\n")

    with pytest.raises(PinnedLibraryError) as e:
        DataInstanceLibrary.Load(lib.location)
    assert "item_0.txt" in str(e.value)
    assert "restamp" in str(e.value), "the refusal must name the remedy, not just refuse"

    os.environ["METASMITH_PINNED_NOCHECK"] = "1"
    try:
        stale = DataInstanceLibrary.Load(lib.location)
    finally:
        del os.environ["METASMITH_PINNED_NOCHECK"]
    ids_before = {p: stale.Get(p).instance_id for p in stale.manifest}
    stale.Restamp()
    reloaded = DataInstanceLibrary.Load(lib.location)
    assert {p: reloaded.Get(p).instance_id for p in reloaded.manifest} == ids_before, (
        "Restamp moved an instance_id; it exists precisely because it must not"
    )


def test_a_missing_entry_is_skipped_not_raised(tmp_path):
    # A pinned library staged to an agent names paths that host does not have.
    lib = _library(tmp_path)
    lib.Pin()
    (lib.location / "item_0.txt").unlink()
    DataInstanceLibrary.Load(lib.location)  # must not raise


def test_a_stamp_from_another_host_warns_instead_of_raising(tmp_path):
    lib = _library(tmp_path)
    lib.Pin()
    index = lib.location / "_metadata" / "index.yml"
    raw = yaml.safe_load(index.read_text())
    raw["pinned"]["host"] = "some-other-machine"
    index.write_text(yaml.safe_dump(raw))
    (lib.location / "item_0.txt").write_text("changed\n")
    DataInstanceLibrary.Load(lib.location)  # warns; mtime is not comparable across hosts


def test_pinning_does_not_move_the_library_key(tmp_path):
    # The key must not see the stamp.
    #
    # The library key flows into the task key, so a legitimate re-stamp that moved
    # it would re-break the plan stability pinning exists to buy.
    lib = _library(tmp_path)
    before = lib.GetKey()
    lib.Pin()
    assert DataInstanceLibrary.Load(lib.location).GetKey() == before
    DataInstanceLibrary.Load(lib.location).Restamp()
    assert DataInstanceLibrary.Load(lib.location).GetKey() == before


def test_unpinning_allows_mutation_again(tmp_path):
    lib = _library(tmp_path)
    lib.Pin()
    lib.Unpin()
    assert lib.is_pinned is False
    lib.AddItem(Path("new.txt"), "t::thing")


def test_a_legacy_frozen_block_still_loads_and_keys_the_same(tmp_path):
    # `pinned:` was called `frozen:` before the rename.
    #
    # An index written under the old spelling is read, not migrated -- a staged
    # copy on a host nobody can re-pin from would otherwise load unpinned and pay
    # the per-plan re-hash the block exists to remove. Both spellings stay out of
    # the library key, or the rename alone moves every task key built on one.
    lib = _library(tmp_path)
    before = lib.GetKey()
    lib.Pin()
    index = lib.location / "_metadata" / "index.yml"
    raw = yaml.safe_load(index.read_text())
    raw["frozen"] = raw.pop("pinned")
    index.write_text(yaml.safe_dump(raw))

    legacy = DataInstanceLibrary.Load(lib.location)
    assert legacy.is_pinned
    assert legacy.GetKey() == before


def test_a_staged_pinned_library_does_not_need_to_write(tmp_path):
    # `PrepTransfer` saves as a side effect, and a pinned library still stages.
    #
    # Its index is authoritative by definition, so there is nothing to write --
    # but a hard refusal there would have broken every workflow that stages one.
    from metasmith.models.remote import Source

    lib = _library(tmp_path)
    lib.Pin()
    lib.PrepTransfer(Source.FromLocal(tmp_path / "dest"))


def test_restamp_cannot_launder_a_deep_baseline(tmp_path):
    # A restamp clears the cheap check and must NOT clear the expensive one.
    #
    # `Restamp` exists for the false positive -- the stamp moved, the bytes did
    # not -- and a caller reaching for it is asserting exactly that. If it
    # re-derived the content digest it would bless whatever is on disk; if it
    # dropped it, a real DRIFTED verdict would become UNVERIFIABLE. Both turn "we
    # did not check" into "it is fine", which is the failure this whole mechanism
    # is documented not to commit.
    lib = _library(tmp_path)
    lib.Pin(deep=True)
    target = lib.location / "item_0.txt"
    target.write_text("substantially different\n")

    reloaded = DataInstanceLibrary.Load(lib.location, check_pinned_stamps=False)
    reloaded.Restamp()
    report = DataInstanceLibrary.Load(lib.location).Verify(deep=True)
    row = report["entries"]["item_0.txt"]
    assert row["stamp"] == "OK", "the restamp did not clear the cheap check"
    assert row["verdict"] == "DRIFTED", (
        "the deep baseline did not survive a restamp, so a real content change"
        " now reads as unverified instead of as changed"
    )


def test_verify_and_restamp_are_reachable_on_a_drifted_library(tmp_path):
    # The remedy must not be gated behind the check it exists to clear.
    #
    # `Load` raises on drift, which is right for a planner and fatal for the two
    # verbs whose job is to adjudicate one -- they would die before reporting.
    lib = _library(tmp_path)
    lib.Pin()
    (lib.location / "item_0.txt").write_text("changed\n")

    with pytest.raises(PinnedLibraryError):
        DataInstanceLibrary.Load(lib.location)
    report = DataInstanceLibrary.Load(lib.location, check_pinned_stamps=False).Verify()
    assert report["entries"]["item_0.txt"]["verdict"] == "DRIFTED"
    DataInstanceLibrary.Load(lib.location, check_pinned_stamps=False).Restamp()
    DataInstanceLibrary.Load(lib.location)  # cleared

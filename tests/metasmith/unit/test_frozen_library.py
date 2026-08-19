"""A frozen library serves its recorded ids and refuses to change.

What these pin is the *mechanism*: the refusals, the round trip, and that a
frozen `Get()` never opens a file. What they cannot pin is whether the freeze
actually protects anything -- every failure mode listed at the top of
`models/libraries/frozen.py` is outside what a test can reach (a same-mtime
edit, a chmod by the owner, a caller going around the API, a stamp taken on
another host). That is precisely why they are documented rather than asserted,
and a green run here is not evidence the data is untouched. `metasmith data
verify --deep` is the only thing that answers that question.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest
import yaml

from metasmith.models.libraries import DataInstanceLibrary
from metasmith.models.libraries.frozen import FrozenLibraryError


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


def test_a_library_is_not_frozen_by_default(tmp_path):
    lib = _library(tmp_path)
    assert lib.is_frozen is False
    raw = yaml.safe_load((lib.location / "_metadata" / "index.yml").read_text())
    assert "frozen" not in raw, (
        "an unfrozen library gained a `frozen:` key, so every committed index.yml"
        " now differs from what the code writes"
    )


@pytest.mark.parametrize("verb", ["AddItem", "AddValue", "Save", "Purge", "Remove",
                                  "RegisterItem", "PruneTypes"])
def test_a_frozen_library_refuses_every_mutation(tmp_path, verb):
    lib = _library(tmp_path)
    lib.Freeze()
    calls = {
        "AddItem": lambda: lib.AddItem(Path("new.txt"), "t::thing"),
        "AddValue": lambda: lib.AddValue("v.txt", "x", "t::thing"),
        "Save": lambda: lib.Save(),
        "Purge": lambda: lib.Purge(),
        "Remove": lambda: lib.Remove(Path("item_0.txt")),
        "RegisterItem": lambda: lib.RegisterItem(Path("n.txt"), "t::thing", instance_id="ab"),
        "PruneTypes": lambda: lib.PruneTypes(),
    }
    with pytest.raises(FrozenLibraryError) as e:
        calls[verb]()
    assert verb in str(e.value)


def test_a_view_of_a_frozen_library_refuses_too(tmp_path):
    """The view delegates, and the delegation IS the enforcement.

    `DataInstanceLibraryView.__getattr__` forwards anything it does not override
    to the wrapped library, so the refusal is inherited for free. Adding an
    overriding wrapper on the view would reopen the bypass, which is what this
    guards.
    """
    lib = _library(tmp_path)
    lib.Freeze()
    view = lib.AsView({Path("item_0.txt")})
    with pytest.raises(FrozenLibraryError):
        view.AddItem(Path("new.txt"), "t::thing")
    with pytest.raises(FrozenLibraryError):
        view.Save()


def test_the_copy_constructor_does_not_launder_a_freeze(tmp_path):
    lib = _library(tmp_path)
    lib.Freeze()
    copy = DataInstanceLibrary(lib)
    assert copy.is_frozen
    with pytest.raises(FrozenLibraryError):
        copy.AddItem(Path("new.txt"), "t::thing")


def test_a_frozen_get_reads_no_file(tmp_path, monkeypatch):
    """The whole point: 24 GB of references cost one stat, not one blake3.

    Monkeypatching the digest to raise is a deterministic stand-in for the
    10 seconds this removes, which is not something a test can assert on.
    """
    lib = _library(tmp_path)
    lib.Freeze()
    before = {p: lib.Get(p).instance_id for p in lib.manifest}

    import metasmith.models.libraries.identity as identity

    def _boom(*a, **k):
        raise AssertionError("a frozen library re-hashed a file")

    monkeypatch.setattr(identity, "content_multihash_key", _boom)
    reloaded = DataInstanceLibrary.Load(lib.location)
    after = {p: reloaded.Get(p).instance_id for p in reloaded.manifest}
    assert after == before


def test_a_frozen_directory_entry_keeps_one_identity(tmp_path):
    """Directories were the worst case, not merely an unhandled one.

    `_mint_leaf_id` gates content addressing on `is_file()`, so a directory fell
    through to `uuid4 + time_ns` and got a fresh id on every build -- which made
    the plan key non-deterministic on one machine, before any question of two.
    """
    types_yml = tmp_path / "t.yml"
    types_yml.write_text(yaml.safe_dump(TYPES))
    lib = DataInstanceLibrary(tmp_path / "lib.xgdb")
    lib.AddTypeLibrary(types_yml, namespace="t")
    d = lib.location / "adir"
    d.mkdir()
    (d / "inner.txt").write_text("x")
    lib.RegisterItem(Path("adir"), "t::thing", instance_id="deadbeef")
    lib.Freeze()

    first = DataInstanceLibrary.Load(lib.location).Get(Path("adir")).instance_id
    second = DataInstanceLibrary.Load(lib.location).Get(Path("adir")).instance_id
    assert first == second == "deadbeef"


def test_a_moved_entry_makes_load_raise_and_restamp_clears_it(tmp_path):
    lib = _library(tmp_path)
    lib.Freeze()
    target = lib.location / "item_0.txt"
    os.chmod(target, 0o644)
    target.write_text("different contents entirely\n")

    with pytest.raises(FrozenLibraryError) as e:
        DataInstanceLibrary.Load(lib.location)
    assert "item_0.txt" in str(e.value)
    assert "restamp" in str(e.value), "the refusal must name the remedy, not just refuse"

    os.environ["METASMITH_FROZEN_NOCHECK"] = "1"
    try:
        stale = DataInstanceLibrary.Load(lib.location)
    finally:
        del os.environ["METASMITH_FROZEN_NOCHECK"]
    ids_before = {p: stale.Get(p).instance_id for p in stale.manifest}
    stale.Restamp()
    reloaded = DataInstanceLibrary.Load(lib.location)
    assert {p: reloaded.Get(p).instance_id for p in reloaded.manifest} == ids_before, (
        "Restamp moved an instance_id; it exists precisely because it must not"
    )


def test_a_missing_entry_is_skipped_not_raised(tmp_path):
    """A frozen library staged to an agent names paths that host does not have."""
    lib = _library(tmp_path)
    lib.Freeze()
    os.chmod(lib.location / "item_0.txt", 0o644)
    (lib.location / "item_0.txt").unlink()
    DataInstanceLibrary.Load(lib.location)  # must not raise


def test_a_stamp_from_another_host_warns_instead_of_raising(tmp_path):
    lib = _library(tmp_path)
    lib.Freeze()
    index = lib.location / "_metadata" / "index.yml"
    raw = yaml.safe_load(index.read_text())
    raw["frozen"]["host"] = "some-other-machine"
    index.write_text(yaml.safe_dump(raw))
    os.chmod(lib.location / "item_0.txt", 0o644)
    (lib.location / "item_0.txt").write_text("changed\n")
    DataInstanceLibrary.Load(lib.location)  # warns; mtime is not comparable across hosts


def test_freezing_does_not_move_the_library_key(tmp_path):
    """The key must not see the stamp.

    The library key flows into the task key, so a legitimate re-stamp that moved
    it would re-break the plan stability freezing exists to buy.
    """
    lib = _library(tmp_path)
    before = lib.GetKey()
    lib.Freeze()
    assert DataInstanceLibrary.Load(lib.location).GetKey() == before
    DataInstanceLibrary.Load(lib.location).Restamp()
    assert DataInstanceLibrary.Load(lib.location).GetKey() == before


def test_the_read_only_mark_does_not_recurse(tmp_path):
    """Pinned as behaviour, because it is a documented hole and a deliberate one.

    `kofam_ref/profiles` holds 27,756 files. Walking them would reintroduce the
    per-plan cost this exists to remove, so nested files keep their own modes --
    which is exactly what the module docstring warns is not covered. Anyone
    "fixing" that has to argue with this test first.
    """
    types_yml = tmp_path / "t.yml"
    types_yml.write_text(yaml.safe_dump(TYPES))
    lib = DataInstanceLibrary(tmp_path / "lib.xgdb")
    lib.AddTypeLibrary(types_yml, namespace="t")
    d = lib.location / "adir"
    d.mkdir()
    inner = d / "inner.txt"
    inner.write_text("x")
    os.chmod(inner, 0o644)
    lib.RegisterItem(Path("adir"), "t::thing", instance_id="cafe")
    lib.Freeze()
    assert not (d.stat().st_mode & 0o222), "the directory itself was not marked"
    assert inner.stat().st_mode & 0o200, (
        "the mark recursed; that is 27k chmods per freeze and it is not what the"
        " docstring promises"
    )


def test_unfreeze_restores_write_and_mutation(tmp_path):
    lib = _library(tmp_path)
    lib.Freeze()
    lib.Unfreeze()
    assert lib.is_frozen is False
    lib.AddItem(Path("new.txt"), "t::thing")
    assert (lib.location / "item_0.txt").stat().st_mode & 0o200


def test_a_staged_frozen_library_does_not_need_to_write(tmp_path):
    """`PrepTransfer` saves as a side effect, and a frozen library still stages.

    Its index is authoritative by definition, so there is nothing to write --
    but a hard refusal there would have broken every workflow that stages one.
    """
    from metasmith.models.remote import Source

    lib = _library(tmp_path)
    lib.Freeze()
    lib.PrepTransfer(Source.FromLocal(tmp_path / "dest"))


def test_restamp_cannot_launder_a_deep_baseline(tmp_path):
    """A restamp clears the cheap check and must NOT clear the expensive one.

    `Restamp` exists for the false positive -- the stamp moved, the bytes did
    not -- and a caller reaching for it is asserting exactly that. If it
    re-derived the content digest it would bless whatever is on disk; if it
    dropped it, a real DRIFTED verdict would become UNVERIFIABLE. Both turn "we
    did not check" into "it is fine", which is the failure this whole mechanism
    is documented not to commit.
    """
    lib = _library(tmp_path)
    lib.Freeze(deep=True)
    target = lib.location / "item_0.txt"
    os.chmod(target, 0o644)
    target.write_text("substantially different\n")

    reloaded = DataInstanceLibrary.Load(lib.location, check_frozen_stamps=False)
    reloaded.Restamp()
    report = DataInstanceLibrary.Load(lib.location).Verify(deep=True)
    row = report["entries"]["item_0.txt"]
    assert row["stamp"] == "OK", "the restamp did not clear the cheap check"
    assert row["verdict"] == "DRIFTED", (
        "the deep baseline did not survive a restamp, so a real content change"
        " now reads as unverified instead of as changed"
    )


def test_verify_and_restamp_are_reachable_on_a_drifted_library(tmp_path):
    """The remedy must not be gated behind the check it exists to clear.

    `Load` raises on drift, which is right for a planner and fatal for the two
    verbs whose job is to adjudicate one -- they would die before reporting.
    """
    lib = _library(tmp_path)
    lib.Freeze()
    os.chmod(lib.location / "item_0.txt", 0o644)
    (lib.location / "item_0.txt").write_text("changed\n")

    with pytest.raises(FrozenLibraryError):
        DataInstanceLibrary.Load(lib.location)
    report = DataInstanceLibrary.Load(lib.location, check_frozen_stamps=False).Verify()
    assert report["entries"]["item_0.txt"]["verdict"] == "DRIFTED"
    DataInstanceLibrary.Load(lib.location, check_frozen_stamps=False).Restamp()
    DataInstanceLibrary.Load(lib.location)  # cleared

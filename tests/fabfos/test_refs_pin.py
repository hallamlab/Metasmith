# The reference library's identity construction, pinned.
#
# No real data: a fake `processed/` root with fake `.dvc` files is enough, because
# nothing here reads a reference byte -- which is the property being tested.
#
# The one thing to be careful about: `dvc_ref_id` is asserted against a literal.
# That is not a tautology test. Changing the construction silently re-keys every
# cached run that ever touched a reference, and a failure here is the reminder
# that the change costs a cluster a round of recomputation.
#
# The literals moved once, deliberately, when reference registration stopped
# minting its own ids and started calling the engine's derived import id. The
# information hashed is the same three things it always was. References carrying
# a published-provenance entry did not move: that path never derived an id.
#
# They did NOT move when an ordinary import began assigning a fresh identity per
# act: a pin is a content address and stays derived, so `pinned_import_id` kept
# the payload the renamed `structural_import_id` had. A failure here after that
# change means the pinned path was migrated by accident.

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from fabfos import refs


def _fake_root(tmp_path: Path) -> Path:
    root = tmp_path / "processed"
    (root / "kofam_ref").mkdir(parents=True)
    (root / "kofam_ref" / "profiles").mkdir()
    (root / "kofam_ref" / "profiles" / "K00001.hmm").write_text("hmm\n")
    (root / "kofam_ref" / "ko_list.tsv").write_text("ko\n")
    (root / "kofam_ref.dvc").write_text(yaml.safe_dump(
        {"outs": [{"md5": "59e24a1aeb8fdc243b698f42e50468d9.dir", "path": "kofam_ref"}]}))
    # present, but with no pin covering it
    (root / "mnxr_lookup").mkdir()
    (root / "mnxr_lookup" / "mnxr_lookup.parquet").write_text("pq\n")
    return root


MD5 = "59e24a1aeb8fdc243b698f42e50468d9.dir"


def test_the_id_construction_is_exactly_this(tmp_path):
    assert refs.dvc_ref_id("ref::kofamscan_profiles", MD5, "kofam_ref/profiles") == (
        "1e20ad3855670216a5869d0023ff41691f25f614e0e283a29e8b9083f7c01437dd37"
    )


def test_two_files_under_one_pin_get_distinct_ids(tmp_path):
    # A `.dvc` covers a whole chunk, so the relative path has to be folded in.
    #
    # Without it `kofam_ref/profiles` and `kofam_ref/ko_list.tsv` collapse to one
    # identity -- the same fan-out corruption `_mint_leaf_id` folds the path to
    # avoid.
    assert refs.dvc_ref_id("ref::a", MD5, "kofam_ref/profiles") != \
        refs.dvc_ref_id("ref::a", MD5, "kofam_ref/ko_list.tsv")


def test_a_re_pin_moves_the_id(tmp_path):
    # The md5 is part of the declaration precisely so that changed reference
    # data re-runs everything that consumed it.
    assert refs.dvc_ref_id("ref::a", MD5, "x") != refs.dvc_ref_id("ref::a", "ff" * 16, "x")


def test_the_type_is_part_of_the_declaration(tmp_path):
    assert refs.dvc_ref_id("ref::a", MD5, "x") != refs.dvc_ref_id("ref::b", MD5, "x")


def test_pinning_registers_only_what_a_pin_covers(tmp_path):
    root = _fake_root(tmp_path)
    report = refs.pin_refs(root, tmp_path / "refs.xgdb")
    assert set(report["added"]) == {"ref::kofamscan_profiles", "ref::kofamscan_ko_list"}
    assert "no .dvc pin" in report["skipped"]["ref::mnxr_lookup"], (
        "an unpinned reference must be skipped, not given a weaker id -- an id"
        " nobody can re-derive elsewhere is worse than no entry"
    )


def test_pinning_twice_after_touching_everything_yields_the_same_ids(tmp_path):
    root = _fake_root(tmp_path)
    out = tmp_path / "refs.xgdb"
    refs.pin_refs(root, out)
    first = _ids(out)
    for p in root.rglob("*"):
        if p.is_file():
            p.touch()
    refs.pin_refs(root, out)
    assert _ids(out) == first, "an id moved when only mtime did"


def test_a_changed_pin_moves_the_id(tmp_path):
    root = _fake_root(tmp_path)
    out = tmp_path / "refs.xgdb"
    refs.pin_refs(root, out)
    first = _ids(out)
    (root / "kofam_ref.dvc").write_text(yaml.safe_dump(
        {"outs": [{"md5": "0000000000000000000000000000ffff.dir", "path": "kofam_ref"}]}))
    refs.pin_refs(root, out)
    assert _ids(out) != first


def test_a_re_materialised_pin_self_heals_instead_of_raising(tmp_path):
    # The expected day-to-day drift, and it must not need a human.
    #
    # `dvc checkout` of the SAME pin re-links the files and moves mtime. The ids
    # are provably still correct, because the value they were minted from has not
    # changed -- so the stamp is re-recorded and nothing is re-hashed.
    root = _fake_root(tmp_path)
    out = tmp_path / "refs.xgdb"
    refs.pin_refs(root, out)
    before = _ids(out)
    for p in root.rglob("*"):
        if p.is_file():
            p.touch()
    lib = refs.load_pinned_refs(root, out)
    assert lib is not None
    assert {d: lib.Get(p).instance_id for p, d, _ in lib.Iterate()} == before


def test_a_changed_pin_under_a_pinned_library_raises_naming_the_fix(tmp_path):
    root = _fake_root(tmp_path)
    out = tmp_path / "refs.xgdb"
    refs.pin_refs(root, out)
    for p in root.rglob("*"):
        if p.is_file():
            p.touch()
    (root / "kofam_ref.dvc").write_text(yaml.safe_dump(
        {"outs": [{"md5": "0000000000000000000000000000ffff.dir", "path": "kofam_ref"}]}))
    from metasmith.models.libraries.pinned import PinnedLibraryError

    with pytest.raises(PinnedLibraryError) as e:
        refs.load_pinned_refs(root, out)
    assert "refs pin" in str(e.value)


def test_a_recorded_provenance_id_beats_the_pin_derived_one(tmp_path):
    # A published reference's real lineage id, when the publish step kept it.
    root = _fake_root(tmp_path)
    out = tmp_path / "refs.xgdb"
    refs.record_published_provenance(
        root, "kofam_ref/profiles", instance_id="1e20aaaa", run="run-x")
    refs.pin_refs(root, out)
    ids = _ids(out)
    assert ids["ref::kofamscan_profiles"] == "1e20aaaa"
    assert ids["ref::kofamscan_ko_list"] != "1e20aaaa"


def test_no_module_keeps_its_own_copy_of_the_layout_table(tmp_path):
    # One table. Two would mis-key an entry rather than fail.
    from fabfos.pipelines import annotation, ecspr

    assert annotation.REF_LAYOUT == {
        k: refs.relpaths_for(k)[0] for k in refs.ANNOTATION_REFS
    }
    assert ecspr.DEFAULT_ATOM_PAIRS.name == Path(refs.relpaths_for("ecspr::atom_pairs")[0]).name


def _ids(out: Path) -> dict[str, str]:
    from metasmith.models.libraries import DataInstanceLibrary

    lib = DataInstanceLibrary.Load(out)
    return {d: lib.Get(p).instance_id for p, d, _ in lib.Iterate()}


def test_pinning_can_register_the_references_in_a_pool(tmp_path):
    # A reference is data the user already has under a declared type, which is
    # what the pool's imported origin is. Registering it there is the same act
    # `metasmith data import` performs, through the same door.
    from metasmith.caching.admission import IMPORTED
    from metasmith.caching.projection import project_store

    root = _fake_root(tmp_path)
    cache_root = tmp_path / "task_cache"
    report = refs.pin_refs(root, tmp_path / "refs.xgdb", cache_root=cache_root)
    assert report["admitted"]["admitted"] == len(report["added"])

    ids = _ids(tmp_path / "refs.xgdb")
    proj = project_store(cache_root, types=refs.load_pinned_refs(root, tmp_path / "refs.xgdb").types)
    assert {it.instance_id for it in proj.items.values()} == set(ids.values())
    assert {it.origin for it in proj.items.values()} == {IMPORTED}


def test_the_pin_is_taken_after_admission(tmp_path):
    # A pinned library refuses mutation, so admitting after the pin would read
    # a library that has already been frozen. The order matters and is asserted
    # by the fact that admission saw every entry.
    root = _fake_root(tmp_path)
    cache_root = tmp_path / "task_cache"
    report = refs.pin_refs(root, tmp_path / "refs.xgdb", cache_root=cache_root)
    assert report["pinned"] == report["admitted"]["admitted"]

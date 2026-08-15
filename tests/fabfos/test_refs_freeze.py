"""The reference library's identity construction, pinned.

No real data: a fake `processed/` root with fake `.dvc` files is enough, because
nothing here reads a reference byte -- which is the property being tested.

The one thing to be careful about: `dvc_leaf_id` is asserted against a literal.
That is not a tautology test. Changing the construction silently re-keys every
cached run that ever touched a reference, and a failure here is the reminder
that the change costs a cluster a round of recomputation.
"""

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


def test_the_id_construction_is_exactly_this(tmp_path):
    assert refs.dvc_leaf_id("59e24a1aeb8fdc243b698f42e50468d9.dir", "kofam_ref/profiles") == (
        "1e2000670fe67f828fbe24cb42a96cdaaae9041284a9b4bb9a786a589421ceddb5ba"
    )


def test_two_files_under_one_pin_get_distinct_ids(tmp_path):
    """A `.dvc` covers a whole chunk, so the relative path has to be folded in.

    Without it `kofam_ref/profiles` and `kofam_ref/ko_list.tsv` collapse to one
    identity -- the same fan-out corruption `_mint_leaf_id` folds the path to
    avoid.
    """
    md5 = "59e24a1aeb8fdc243b698f42e50468d9.dir"
    assert refs.dvc_leaf_id(md5, "kofam_ref/profiles") != refs.dvc_leaf_id(md5, "kofam_ref/ko_list.tsv")


def test_freeze_registers_only_what_a_pin_covers(tmp_path):
    root = _fake_root(tmp_path)
    report = refs.freeze_refs(root, tmp_path / "refs.xgdb")
    assert set(report["added"]) == {"ref::kofamscan_profiles", "ref::kofamscan_ko_list"}
    assert "no .dvc pin" in report["skipped"]["ref::mnxr_lookup"], (
        "an unpinned reference must be skipped, not given a weaker id -- an id"
        " nobody can re-derive elsewhere is worse than no entry"
    )


def test_freezing_twice_after_touching_everything_yields_the_same_ids(tmp_path):
    root = _fake_root(tmp_path)
    out = tmp_path / "refs.xgdb"
    refs.freeze_refs(root, out)
    first = _ids(out)
    for p in root.rglob("*"):
        if p.is_file():
            p.chmod(0o644)
            p.touch()
    refs.freeze_refs(root, out)
    assert _ids(out) == first, "an id moved when only mtime did"


def test_a_changed_pin_moves_the_id(tmp_path):
    root = _fake_root(tmp_path)
    out = tmp_path / "refs.xgdb"
    refs.freeze_refs(root, out)
    first = _ids(out)
    (root / "kofam_ref.dvc").write_text(yaml.safe_dump(
        {"outs": [{"md5": "0000000000000000000000000000ffff.dir", "path": "kofam_ref"}]}))
    refs.freeze_refs(root, out)
    assert _ids(out) != first


def test_a_re_materialised_pin_self_heals_instead_of_raising(tmp_path):
    """The expected day-to-day drift, and it must not need a human.

    `dvc checkout` of the SAME pin re-links the files and moves mtime. The ids
    are provably still correct, because the value they were minted from has not
    changed -- so the stamp is re-recorded and nothing is re-hashed.
    """
    root = _fake_root(tmp_path)
    out = tmp_path / "refs.xgdb"
    refs.freeze_refs(root, out)
    before = _ids(out)
    for p in root.rglob("*"):
        if p.is_file():
            p.chmod(0o644)
            p.touch()
    lib = refs.load_frozen_refs(root, out)
    assert lib is not None
    assert {d: lib.Get(p).instance_id for p, d, _ in lib.Iterate()} == before


def test_a_changed_pin_under_a_frozen_library_raises_naming_the_fix(tmp_path):
    root = _fake_root(tmp_path)
    out = tmp_path / "refs.xgdb"
    refs.freeze_refs(root, out)
    for p in root.rglob("*"):
        if p.is_file():
            p.chmod(0o644)
            p.touch()
    (root / "kofam_ref.dvc").write_text(yaml.safe_dump(
        {"outs": [{"md5": "0000000000000000000000000000ffff.dir", "path": "kofam_ref"}]}))
    from metasmith.models.libraries.frozen import FrozenLibraryError

    with pytest.raises(FrozenLibraryError) as e:
        refs.load_frozen_refs(root, out)
    assert "refs freeze" in str(e.value)


def test_a_recorded_provenance_id_beats_the_pin_derived_one(tmp_path):
    """A published reference's real lineage id, when the publish step kept it."""
    root = _fake_root(tmp_path)
    out = tmp_path / "refs.xgdb"
    refs.record_published_provenance(
        root, "kofam_ref/profiles", instance_id="1e20aaaa", run="run-x")
    refs.freeze_refs(root, out)
    ids = _ids(out)
    assert ids["ref::kofamscan_profiles"] == "1e20aaaa"
    assert ids["ref::kofamscan_ko_list"] != "1e20aaaa"


def test_no_module_keeps_its_own_copy_of_the_layout_table(tmp_path):
    """One table. Two would mis-key an entry rather than fail."""
    from fabfos.pipelines import annotation, ecspr

    assert annotation.REF_LAYOUT == {
        k: refs.relpaths_for(k)[0] for k in refs.ANNOTATION_REFS
    }
    assert ecspr.DEFAULT_ATOM_PAIRS.name == Path(refs.relpaths_for("ecspr::atom_pairs")[0]).name


def _ids(out: Path) -> dict[str, str]:
    from metasmith.models.libraries import DataInstanceLibrary

    lib = DataInstanceLibrary.Load(out)
    return {d: lib.Get(p).instance_id for p, d, _ in lib.Iterate()}

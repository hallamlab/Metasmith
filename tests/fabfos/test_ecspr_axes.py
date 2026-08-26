# The curated ECSPr axis set: set4 is frozen, and the extension is additive.
#
# set4 carries a sha rather than a producer, so the only thing that can go wrong with it is
# an edit. These tests are that guard, plus the invariants a consumer relies on when it
# turns the table into one measurement per row.
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
# The loader is a research-side helper, beside `bake_identity` and `bake_pairs`, because
# both the scadc arm and the nostoc panel read it and neither is a package.
BENCH = str(REPO / "research/fabfos/benchmarks")
if BENCH not in sys.path:
    sys.path.append(BENCH)

import ecspr_axes  # noqa: E402

# From the incumbent provenance, preserved in `src/fabfos/_deprecated_canon.py`
# (`AXES_N`, `AXES_PER_ELEMENT`) -- restated here so a change to set4 fails a test rather
# than a deprecated constant nothing reads.
SET4_PER_ELEMENT = {"C": 40, "N": 23, "P": 9, "S": 7}

LIPID_EXT_AXES = {
    "catabolic__MNXM1364061__MNXM77__C",
    "phospholipid__MNXM77__MNXM115__C",
    "phospholipid__MNXM77__MNXM725992__C",
}


def test_set4_is_frozen_at_its_pinned_sha():
    assert ecspr_axes.sha256(ecspr_axes.SET4) == ecspr_axes.SET4_SHA


def test_set4_is_tracked_git_text():
    # REFERENCES.md C1 declares it git text rather than a DVC chunk. `data/fabfos` admits
    # only `.dvc` pins by default, so this fails the moment that negation is dropped.
    rel = ecspr_axes.SET4.relative_to(REPO)
    out = subprocess.run(["git", "-C", str(REPO), "ls-files", "--error-unmatch", str(rel)],
                         capture_output=True, text=True)
    assert out.returncode == 0, f"{rel} is not tracked: {out.stderr.strip()}"


def test_set4_alone_has_the_declared_shape():
    df = ecspr_axes.load(lipid_ext=False)
    assert len(df) == ecspr_axes.SET4_N
    assert df.element.value_counts().to_dict() == SET4_PER_ELEMENT
    assert set(df.axis_set) == {"set4"}


def test_extension_is_additive_and_carbon_only():
    df = ecspr_axes.load()
    ext = df[df.axis_set == "lipid_ext"]
    assert set(ext.axis_id) == LIPID_EXT_AXES
    assert set(ext.element) == {"C"}
    base = ecspr_axes.load(lipid_ext=False)
    assert df[df.axis_set == "set4"].reset_index(drop=True).equals(base)


def test_the_backbone_axes_enter_at_dhap():
    df = ecspr_axes.load()
    lipid = df[(df.category == "phospholipid") & (df.axis_set == "lipid_ext")]
    # MNXM77 is DHAP. The last assertion is the point of the extension: set4's own
    # phospholipid axes arrive from AcCoA, L-serine or phosphate, never the backbone.
    assert set(lipid.src_mnxm) == {"MNXM77"}
    assert set(lipid.dst_mnxm) == {"MNXM115", "MNXM725992"}
    set4_lipid = df[(df.category == "phospholipid") & (df.axis_set == "set4")]
    assert "MNXM77" not in set(set4_lipid.src_mnxm)


def test_every_axis_is_one_to_one():
    for path in (ecspr_axes.SET4, ecspr_axes.LIPID_EXT):
        for axis_id, r in json.loads(path.read_text()).items():
            assert len(r["source"]) == 1 and len(r["sink"]) == 1, axis_id


def test_axes_are_unique_on_element_source_sink():
    df = ecspr_axes.load()
    assert not df.duplicated(["element", "src_mnxm", "dst_mnxm"]).any()


def test_a_duplicate_axis_is_refused(tmp_path, monkeypatch):
    dup = json.loads(ecspr_axes.SET4.read_text())["phospholipid__MNXM1104266__MNXM115__C"]
    clash = tmp_path / "clash.json"
    clash.write_text(json.dumps({"restated__AcCoA__PE__C": dup}))
    monkeypatch.setattr(ecspr_axes, "LIPID_EXT", clash)
    with pytest.raises(ValueError, match="duplicated on"):
        ecspr_axes.load()


def test_an_edited_set4_is_refused(tmp_path, monkeypatch):
    edited = tmp_path / "set4.json"
    spec = json.loads(ecspr_axes.SET4.read_text())
    spec.pop(next(iter(spec)))
    edited.write_text(json.dumps(spec))
    monkeypatch.setattr(ecspr_axes, "SET4", edited)
    with pytest.raises(SystemExit, match="frozen"):
        ecspr_axes.load()

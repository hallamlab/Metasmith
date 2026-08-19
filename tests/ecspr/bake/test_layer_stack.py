from __future__ import annotations

import pandas as pd
import pytest

from ecspr.bake.aam import layers as L


def _atoms(rows: list[tuple]) -> pd.DataFrame:
    out = [dict(zip(("mnxr", "element", "substrate", "product", "sub_idx", "prod_idx"), r),
                pair_w=1.0, method="test", source="test", confidence=1.0) for r in rows]
    return pd.DataFrame(out, columns=list(L.ATOM_COLS))


ONE = ("R1", "S", "M1", "M2", 0, 0)


def test_gate_refuses_a_layer_that_reclaims_a_lower_layer_s_reaction_element():
    below = _atoms([ONE])
    added = _atoms([("R1", "S", "M1", "M3", 0, 1)])
    with pytest.raises(SystemExit, match="not additive"):
        L.additive_gates(below, added, "partial")


def test_gate_refuses_the_same_atom_correspondence_from_two_layers():
    below = _atoms([ONE])
    with pytest.raises(SystemExit):
        L.additive_gates(below, _atoms([ONE]), "dup")


def test_gate_3_cannot_fire_and_is_pinned_as_inert():
    below = _atoms([ONE, ("R2", "S", "M1", "M2", 0, 0)])
    assert L.additive_gates(below, _atoms([]), "empty") is True
    assert L.additive_gates(below, _atoms([("R3", "N", "M1", "M2", 0, 0)]), "add") is True


def test_gate_refuses_negative_canonical_ranks():
    with pytest.raises(SystemExit):
        L.additive_gates(_atoms([]), _atoms([("R1", "S", "M1", "M2", -1, 0)]), "neg")


def test_gate_passes_a_genuinely_additive_layer():
    below = _atoms([ONE])
    added = _atoms([("R2", "N", "M1", "M2", 0, 0)])
    assert L.additive_gates(below, added, "next") is True


def test_consensus_full_weight_single_member_half_disagreement_diluted():
    agree = {"a": _atoms([ONE]), "b": _atoms([ONE])}
    df, tally = L.fuse_members(agree)
    assert tally == {"consensus": 1}
    assert list(df["pair_w"]) == [1.0]

    df, tally = L.fuse_members({"a": _atoms([ONE])})
    assert tally == {"a_only": 1}
    assert list(df["pair_w"]) == [L.SINGLE_MEMBER_CREDIT]
    assert list(df["method"]) == ["a_only"]

    disagree = {"a": _atoms([ONE]), "b": _atoms([("R1", "S", "M1", "M3", 0, 0)])}
    df, tally = L.fuse_members(disagree)
    assert tally == {"disagree_diluted": 1}
    assert len(df) == 2, "a disputed atom must spread, not resolve to one row"
    assert sum(df["pair_w"]) == pytest.approx(1.0), (
        "a disputed atom's total transfer must equal a confident one's -- the "
        "dilution redistributes weight, it does not create or destroy it")


def test_a_nan_confidence_cannot_poison_every_weight_in_the_reaction():
    a = _atoms([ONE])
    b = _atoms([("R1", "S", "M1", "M3", 0, 0)])
    a["confidence"] = float("nan")
    b["confidence"] = float("nan")
    df, tally = L.fuse_members({"a": a, "b": b})
    assert tally == {"disagree_diluted": 1}
    assert df["pair_w"].notna().all(), "a NaN confidence poisoned the pair weights"
    assert sum(df["pair_w"]) == pytest.approx(1.0)

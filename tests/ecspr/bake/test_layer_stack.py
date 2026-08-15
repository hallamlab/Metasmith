"""The layer stack: what a layer may add, and the four ways it may not.

`additive_gates` REFUSES rather than warns, and that is the property under test.
Each of the four checks is a way the stack could stop meaning what it says while
every table still looked well-formed -- and a warning about that gets read once
and scrolled past for the rest of the build. A test that only proved the gates
run would be worth nothing; these prove they raise.

The stack is also what T8's partial lane relies on for its central safety claim:
a partial map is laid down LAST and may only claim (reaction, element)
combinations nothing above it claimed. That is gate 1, so gate 1 is what makes
"a partial pair can never overwrite a real map" a fact about machinery rather
than a promise in a docstring.
"""
from __future__ import annotations

import pandas as pd
import pytest

from ecspr.bake.aam import layers as L


def _atoms(rows: list[tuple]) -> pd.DataFrame:
    """(mnxr, element, substrate, product, sub_idx, prod_idx) -> the ATOM_COLS frame."""
    out = [dict(zip(("mnxr", "element", "substrate", "product", "sub_idx", "prod_idx"), r),
                pair_w=1.0, method="test", source="test", confidence=1.0) for r in rows]
    return pd.DataFrame(out, columns=list(L.ATOM_COLS))


ONE = ("R1", "S", "M1", "M2", 0, 0)


# --- the four refusals ----------------------------------------------------

def test_gate_refuses_a_layer_that_reclaims_a_lower_layer_s_reaction_element():
    """Gate 1. An overwrite is a replacement, which is a larger claim than an append.

    This is the gate the partial lane's safety rests on: laid down last, it can
    only reach combinations nothing above it took.
    """
    below = _atoms([ONE])
    added = _atoms([("R1", "S", "M1", "M3", 0, 1)])
    with pytest.raises(SystemExit, match="not additive"):
        L.additive_gates(below, added, "partial")


def test_gate_refuses_the_same_atom_correspondence_from_two_layers():
    """Gate 2. The same atom accounted twice is the same transfer counted twice.

    Distinct from gate 1: this one fires on the full pair key, so it catches a
    duplicate even where the (reaction, element) claim looks legitimately new.
    """
    below = _atoms([ONE])
    with pytest.raises(SystemExit):
        L.additive_gates(below, _atoms([ONE]), "dup")


def test_gate_3_cannot_fire_and_is_pinned_as_inert():
    """Gate 3 -- "no element loses reactions" -- is unreachable as written.

    It compares `below` against `concat([below, added])`, which is a superset of
    `below` by construction, so the post-append count can never be lower. No
    caller can make it fire either: `stack` restricts `added` before calling and
    always passes the ACCUMULATED table as `below`.

    Pinned as inert rather than quietly fixed. This suite landed in a phase
    required to move zero numbers, and turning a dead check live is exactly the
    kind of change that would make a later regression ambiguous. The value of
    the test is that the next person to read the gate list learns from a test
    rather than from a bake, and that a fix will show up here as a failure.
    """
    below = _atoms([ONE, ("R2", "S", "M1", "M2", 0, 0)])
    assert L.additive_gates(below, _atoms([]), "empty") is True
    assert L.additive_gates(below, _atoms([("R3", "N", "M1", "M2", 0, 0)]), "add") is True


def test_gate_refuses_negative_canonical_ranks():
    """Gate 4. `CanonicalRankAtoms` is non-negative; a negative rank is a leaked
    sentinel, and the bake packs ranks UNSIGNED -- where it would wrap silently
    into a different atom rather than fail."""
    with pytest.raises(SystemExit):
        L.additive_gates(_atoms([]), _atoms([("R1", "S", "M1", "M2", -1, 0)]), "neg")


def test_gate_passes_a_genuinely_additive_layer():
    """The gates must not be so strict that a real append cannot land."""
    below = _atoms([ONE])
    added = _atoms([("R2", "N", "M1", "M2", 0, 0)])
    assert L.additive_gates(below, added, "next") is True


# --- the fusion arithmetic ------------------------------------------------

def test_consensus_full_weight_single_member_half_disagreement_diluted():
    """The three outcomes, and the one number each carries.

    Consensus is full weight; a lone member is present-but-uncorroborated at
    half; disagreement DILUTES across the disputed destinations rather than
    picking a winner, so a contested atom transfers less instead of committing.
    That last one is the model's whole stance on uncertainty -- dilute, never
    gap and never choose -- and it is why the disagreement case emits two rows.
    """
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
    """The division guard, which is load-bearing rather than defensive.

    `wsum` divides the disagreement weights and NaN is truthy, so `or` does not
    catch a missing confidence -- one member with no confidence would turn every
    weight in the reaction into NaN, and a NaN conductance is an edge that
    silently vanishes downstream rather than an error anyone sees.
    """
    a = _atoms([ONE])
    b = _atoms([("R1", "S", "M1", "M3", 0, 0)])
    a["confidence"] = float("nan")
    b["confidence"] = float("nan")
    df, tally = L.fuse_members({"a": a, "b": b})
    assert tally == {"disagree_diluted": 1}
    assert df["pair_w"].notna().all(), "a NaN confidence poisoned the pair weights"
    assert sum(df["pair_w"]) == pytest.approx(1.0)

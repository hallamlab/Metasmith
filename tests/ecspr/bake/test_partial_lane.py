"""The partial lane: a narrower claim, and everything that keeps it from reading as a full one.

Two things are being pinned and they are not the same. The first is that the ELEMENT
REDUCTION is sound -- an atom of X cannot come from a participant carrying no X, so
dropping the X-free participants removes no possible source and no possible destination
for an X atom, and the balance across the kept set is checked before anything is
submitted. The second is that a partial result is LABELLED everywhere it is read, which
is the campaign's stop line: a partially-mapped reaction that reads as banked overstates
coverage, and one that reads as dropped hides it.
"""
from __future__ import annotations

import pandas as pd
import pytest

pytest.importorskip("rdkit", reason="the lane parses submissions to size them")

from ecspr.bake import atom_pairs as AP
from ecspr.bake.aam import layers as L, partial as P, worklist as W


def _offer(*mnxrs):
    """What the forecast hands the lane: `{reaction -> elements offered}`.

    Every element, because these cases are about the REDUCTION rather than about which
    reactions the forecast picks -- that is `test_forecast.py`'s subject.
    """
    return {m: list(AP.ELEMENTS) for m in mnxrs}


# A small reaction with real chemistry in it: glucose -> pyruvate carries the carbon
# while water carries none, and an ATP/ADP pair rides along. That asymmetry is the whole
# point -- for carbon the water is irrelevant, for phosphorus the sugars are.
#
# The ids have to LOOK like MetaNetX ids: `parse_equation`'s term grammar matches only
# `MNXM...@compartment`, deliberately, because specials like `WATER@MNXD1` have no
# chem_prop SMILES. A fixture with invented ids parses to nothing and every assertion
# below passes vacuously.
FORMULAS = {
    "MNXM1001": "C6H12O6",
    "MNXM1002": "C3H4O3",
    "MNXM1003": "C10H16N5O13P3",
    "MNXM1004": "C10H15N5O10P2",
    "MNXM1005": "H2O",
    "MNXM1006": "*",
    "MNXM1007": "HO4P",
}
SMILES = {
    "MNXM1001": "OCC1OC(O)C(O)C(O)C1O",
    "MNXM1002": "CC(=O)C(=O)O",
    "MNXM1003": "NC1=NC=NC2=C1N=CN2C1OC(COP(=O)(O)OP(=O)(O)OP(=O)(O)O)C(O)C1O",
    "MNXM1004": "NC1=NC=NC2=C1N=CN2C1OC(COP(=O)(O)OP(=O)(O)O)C(O)C1O",
    "MNXM1005": "O",
    "MNXM1007": "OP(=O)(O)O",
}


# --- the reduction --------------------------------------------------------

def test_a_reduction_keeps_only_the_participants_that_carry_the_element():
    """For carbon, the water is irrelevant; for phosphorus, the sugars are.

    This is the property that makes a long reaction short: `3 NADPH + 3 NADP+` blows a
    512-token limit while contributing no sulfur at all.
    """
    subs = ["MNXM1001", "MNXM1003", "MNXM1005"]
    prods = ["MNXM1002", "MNXM1002", "MNXM1004"]
    kept = AP.reduce_for_element(subs, prods, FORMULAS, "C")
    assert kept is not None
    ks, kp = kept
    assert "MNXM1005" not in ks, "a participant carrying no carbon survived the C reduction"
    assert sorted(ks) == ["MNXM1001", "MNXM1003"]
    assert sorted(kp) == ["MNXM1002", "MNXM1002", "MNXM1004"]


def test_a_reduction_refuses_when_the_element_does_not_balance():
    """An unaccounted X means some X is entering or leaving through a participant the
    reduction dropped -- which is exactly the re-routing the `stripped` guard refuses.
    A reduction that submitted it anyway would be a strip wearing a reduction's name."""
    assert AP.reduce_for_element(["MNXM1001"], ["MNXM1002"], FORMULAS, "C") is None  # 6 vs 3
    assert AP.reduce_for_element(["MNXM1001"], ["MNXM1002", "MNXM1002"], FORMULAS, "C") is not None


def test_a_reduction_refuses_when_a_formula_cannot_be_trusted():
    """None must propagate to a refusal, never to a zero.

    A `*` polymer read as "carries no carbon" would be dropped from the kept set, and
    the reduction would then certify a carbon balance it never checked.
    """
    assert AP.reduce_for_element(["MNXM1001", "MNXM1006"], ["MNXM1002", "MNXM1002"],
                                 FORMULAS, "C") is None


def test_a_reduction_refuses_a_one_sided_result():
    """X on only one side is unbalanced by definition, and an empty side is not a
    reaction -- so neither reaches a mapper as if it were one."""
    assert AP.reduce_for_element(["MNXM1001"], ["MNXM1005"], FORMULAS, "C") is None
    assert AP.reduce_for_element(["MNXM1005"], ["MNXM1005"], FORMULAS, "C") is None


# --- the lane -------------------------------------------------------------

EQ = {"MNXR900001": "1 MNXM1001@MNXD1 + 1 MNXM1003@MNXD1 "
                    "= 2 MNXM1002@MNXD1 + 1 MNXM1004@MNXD1"}


def test_the_lane_settles_what_conservation_settles_and_submits_the_rest():
    """The exact arm goes FIRST, and what it settles never becomes a submission.

    Phosphorus here is one ATP in, one ADP out -- but three P atoms against two, so
    conservation does not balance it and there is no forced pair. Carbon does balance,
    16 against 16, but spans two metabolites on each side, so it needs a mapper. The
    tally is what makes the lane's yield a number rather than a hope.
    """
    formulas, smiles_of, ranks_of = FORMULAS, SMILES, _ranks()
    uni, forced, tally = P.build(_offer("MNXR900001"), EQ, formulas, smiles_of, ranks_of,
                                 char_limit=8000, atom_limit=600)
    submitted = {(r[3], r[4]) for r in uni}
    assert ("MNXR900001", "C") in submitted, "the balanced element produced no submission"
    assert tally["reduced submission"] >= 1
    # Whatever the forced arm settled must NOT also have been submitted.
    settled = {(r[0], r[1]) for r in forced}
    assert not (settled & submitted)


def test_a_submission_carries_the_reduced_lists_the_extractor_must_read():
    """The one coupling between the lane and the extractor.

    Re-deriving the participant list from `reac_prop` would hand every submission the
    FULL equation, and the strict guard would call every one of them `stripped`. So the
    lists the reduction actually made travel with it -- which is also what keeps the
    reduction logic in one file.
    """
    uni, _f, _t = P.build(_offer("MNXR900001"), EQ, FORMULAS, SMILES, _ranks(), 8000, 600)
    row = dict(zip(P.UNIVERSE_COLS, uni[0]))
    assert row["verdict"] == "mappable", "a member reads `verdict == mappable`"
    assert row["base_mnxr"] == "MNXR900001" and row["element"] in AP.ELEMENTS
    assert row["sub_mnxms"] and row["prod_mnxms"]
    assert all(m in SMILES for m in row["sub_mnxms"] + row["prod_mnxms"])
    assert P.split_key(row["mnxr"]) == (row["base_mnxr"], row["element"])


def test_the_composite_key_round_trips_and_refuses_anything_else():
    """A key that does not split is a partial product that reached a reader expecting a
    plain MNXR -- which would attribute one element's map to a whole reaction."""
    assert P.split_key(P.make_key("MNXR123", "N")) == ("MNXR123", "N")
    with pytest.raises(ValueError):
        P.split_key("MNXR123")
    with pytest.raises(ValueError):
        P.split_key("MNXR123#Q")          # Q is not an element the bake tracks


def test_the_forced_arm_is_emitted_in_the_extractor_s_shape():
    """So `aam_layers.explode` reads it with no special case.

    A second reader for an already-exploded shape would be a second place the atom
    identity contract is implemented, and the two would drift.
    """
    _u, forced, _t = P.build(_offer("MNXR900001"), EQ, FORMULAS, SMILES, _ranks(), 8000, 600)
    if not forced:
        pytest.skip("this fixture's elements are all mapper-bound; shape is pinned above")
    assert list(P.FORCED_COLS) == list(AP.PAIR_COLS)
    row = dict(zip(P.FORCED_COLS, forced[0]))
    assert row["mnxr"] == "MNXR900001", "the forced arm must key on the REAL reaction id"


# --- the reduction is a reaction too, so it gets the same second reading ---

# Nitrogenase's phosphorus, in miniature: 16 ATP in, 16 ADP + 16 phosphate out. The
# reduction for P keeps all three and drops nothing, so it is SOUND -- and it is also a
# thousand atoms of three molecules written sixteen times each, which is the exact
# measure this whole scope exists to stop applying.
EQ_REPEATED = {"MNXR900002": "16 MNXM1003@MNXD1 = 16 MNXM1004@MNXD1 + 16 MNXM1007@MNXD1"}

# The same shape with coefficients that are NOT a common multiple: 60 ATP (180 P) into
# 90 ADP (180 P) balances as written and does not balance per copy (3 into 2).
EQ_UNEVEN = {"MNXR900003": "60 MNXM1003@MNXD1 = 90 MNXM1004@MNXD1"}


def test_a_reduction_that_is_only_big_because_of_stoichiometry_is_collapsed():
    """Otherwise the partial lane is the last place still measuring copies.

    The expanded P reduction here is over a thousand atoms and would be refused for
    size; written once per participant it is around sixty, and every P atom keeps the
    destination it had, because the reaction is a whole multiple of a per-copy one.
    """
    uni, _f, tally = P.build(_offer("MNXR900002"), EQ_REPEATED, FORMULAS, SMILES, _ranks(),
                             char_limit=8000, atom_limit=600)
    rows = {r[4]: dict(zip(P.UNIVERSE_COLS, r)) for r in uni}
    assert "P" in rows, "the phosphorus reduction was refused for stoichiometric size"
    p = rows["P"]
    assert p["collapsed"] is True
    assert p["atoms"] <= 600
    assert p["sub_mnxms"] == ["MNXM1003"], "a collapsed submission still repeats a participant"
    assert sorted(p["prod_mnxms"]) == ["MNXM1004", "MNXM1007"]
    assert tally["reduced submission (collapsed)"] >= 1


def test_a_collapse_that_breaks_the_element_balance_is_not_taken():
    """The guard, and it is the only thing separating this from a strip.

    60 ATP into 90 ADP balances at 180 P each way. Collapsed it offers three P sources
    for two destinations, so the mapper would have to CHOOSE which of ATP's phosphates
    survived -- an invention at rank level, where nothing downstream can see it. A gap
    is the honest outcome, so the submission stays expanded and stays refused.
    """
    assert not P._still_balances(["MNXM1003"], ["MNXM1004"], FORMULAS, "P")
    assert P._still_balances(["MNXM1003"], ["MNXM1004", "MNXM1007"], FORMULAS, "P")

    uni, _f, tally = P.build(_offer("MNXR900003"), EQ_UNEVEN, FORMULAS, SMILES, _ranks(),
                             char_limit=8000, atom_limit=600)
    assert not [r for r in uni if r[4] == "P"], (
        "a collapse that changes the per-copy element balance was taken anyway")
    assert sum(v for k, v in tally.items() if k.startswith("reduction over")) >= 1


# --- the labelling, which is the stop line --------------------------------

def _partial_layer():
    return pd.DataFrame(
        [dict(mnxr="R1", element="S", substrate="MNXM2001", product="MNXM2002", sub_idx=0,
              prod_idx=0, pair_w=1.0, method="partial_reduced", source="partial",
              confidence=1.0)],
        columns=list(L.ATOM_COLS))


def test_a_partial_pair_can_never_overwrite_a_real_map():
    """The safety property, and it is machinery rather than a promise.

    `stack` restricts each layer to what nothing below it claimed, so a partial laid
    down LAST over a (reaction, element) a real member already reached contributes
    nothing at all -- not a lower weight, not a second row.
    """
    real = _partial_layer().assign(method="rxnmapper", source="rxnmapper")
    tbl, report = L.stack([("ensemble", real), ("partial", _partial_layer())])
    assert report[-1]["added"] == 0
    assert (tbl["source"] == "partial").sum() == 0
    assert len(tbl) == 1


def test_a_partial_pair_lands_where_nothing_else_reached():
    """The other half: the layer must actually be able to add, or it is decoration."""
    empty = _partial_layer().iloc[0:0]
    tbl, report = L.stack([("ensemble", empty), ("partial", _partial_layer())])
    assert report[-1]["added"] == 1
    assert list(tbl["source"]) == ["partial"] and list(tbl["method"]) == ["partial_reduced"]


def test_the_ledger_has_its_own_word_for_a_partial_bank():
    """`banked_partial` and `partial_declined` are in the closed outcome set.

    Folding either into `banked` or `mapped_nothing` is the stop line: one overstates
    coverage and the other hides it, and `close` refuses any outcome outside the set --
    so the words existing is what makes the distinction assignable at all.
    """
    assert "banked_partial" in W.OUTCOMES
    assert "partial_declined" in W.OUTCOMES
    assert W.PARTIAL_SOURCE == "partial", (
        "close tells a partial bank from a full one by this string; the lane stamps it, "
        "and two spellings of it is how an outcome silently stops being assigned")


def _ranks():
    """(metabolite, element) -> canonical ranks, from each metabolite's own molecule."""
    from rdkit import Chem
    out = {}
    for m, smi in SMILES.items():
        mol = Chem.MolFromSmiles(smi)
        rk = AP.canonical_ranks(mol)
        if rk is None:
            continue
        for X in AP.ELEMENTS:
            idx = [a.GetIdx() for a in mol.GetAtoms() if a.GetSymbol() == X]
            if idx:
                out[(m, X)] = [rk[i] for i in idx]
    return out

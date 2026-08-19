from __future__ import annotations

import pandas as pd
import pytest

pytest.importorskip("rdkit", reason="the lane parses submissions to size them")

from ecspr.bake import atom_pairs as AP
from ecspr.bake.aam import layers as L, partial as P, worklist as W


def _offer(*mnxrs):
    return {m: list(AP.ELEMENTS) for m in mnxrs}


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


def test_a_reduction_keeps_only_the_participants_that_carry_the_element():
    subs = ["MNXM1001", "MNXM1003", "MNXM1005"]
    prods = ["MNXM1002", "MNXM1002", "MNXM1004"]
    kept = AP.reduce_for_element(subs, prods, FORMULAS, "C")
    assert kept is not None
    ks, kp = kept
    assert "MNXM1005" not in ks, "a participant carrying no carbon survived the C reduction"
    assert sorted(ks) == ["MNXM1001", "MNXM1003"]
    assert sorted(kp) == ["MNXM1002", "MNXM1002", "MNXM1004"]


def test_a_reduction_refuses_when_the_element_does_not_balance():
    assert AP.reduce_for_element(["MNXM1001"], ["MNXM1002"], FORMULAS, "C") is None
    assert AP.reduce_for_element(["MNXM1001"], ["MNXM1002", "MNXM1002"], FORMULAS, "C") is not None


def test_a_reduction_refuses_when_a_formula_cannot_be_trusted():
    assert AP.reduce_for_element(["MNXM1001", "MNXM1006"], ["MNXM1002", "MNXM1002"],
                                 FORMULAS, "C") is None


def test_a_reduction_refuses_a_one_sided_result():
    assert AP.reduce_for_element(["MNXM1001"], ["MNXM1005"], FORMULAS, "C") is None
    assert AP.reduce_for_element(["MNXM1005"], ["MNXM1005"], FORMULAS, "C") is None


EQ = {"MNXR900001": "1 MNXM1001@MNXD1 + 1 MNXM1003@MNXD1 "
                    "= 2 MNXM1002@MNXD1 + 1 MNXM1004@MNXD1"}


def test_the_lane_settles_what_conservation_settles_and_submits_the_rest():
    formulas, smiles_of, ranks_of = FORMULAS, SMILES, _ranks()
    uni, forced, tally = P.build(_offer("MNXR900001"), EQ, formulas, smiles_of, ranks_of,
                                 char_limit=8000, atom_limit=600)
    submitted = {(r[3], r[4]) for r in uni}
    assert ("MNXR900001", "C") in submitted, "the balanced element produced no submission"
    assert tally["reduced submission"] >= 1
    settled = {(r[0], r[1]) for r in forced}
    assert not (settled & submitted)


def test_a_submission_carries_the_reduced_lists_the_extractor_must_read():
    uni, _f, _t = P.build(_offer("MNXR900001"), EQ, FORMULAS, SMILES, _ranks(), 8000, 600)
    row = dict(zip(P.UNIVERSE_COLS, uni[0]))
    assert row["verdict"] == "mappable", "a member reads `verdict == mappable`"
    assert row["base_mnxr"] == "MNXR900001" and row["element"] in AP.ELEMENTS
    assert row["sub_mnxms"] and row["prod_mnxms"]
    assert all(m in SMILES for m in row["sub_mnxms"] + row["prod_mnxms"])
    assert P.split_key(row["mnxr"]) == (row["base_mnxr"], row["element"])


def test_the_composite_key_round_trips_and_refuses_anything_else():
    assert P.split_key(P.make_key("MNXR123", "N")) == ("MNXR123", "N")
    with pytest.raises(ValueError):
        P.split_key("MNXR123")
    with pytest.raises(ValueError):
        P.split_key("MNXR123#Q")


def test_the_forced_arm_is_emitted_in_the_extractor_s_shape():
    _u, forced, _t = P.build(_offer("MNXR900001"), EQ, FORMULAS, SMILES, _ranks(), 8000, 600)
    if not forced:
        pytest.skip("this fixture's elements are all mapper-bound; shape is pinned above")
    assert list(P.FORCED_COLS) == list(AP.PAIR_COLS)
    row = dict(zip(P.FORCED_COLS, forced[0]))
    assert row["mnxr"] == "MNXR900001", "the forced arm must key on the REAL reaction id"


EQ_REPEATED = {"MNXR900002": "16 MNXM1003@MNXD1 = 16 MNXM1004@MNXD1 + 16 MNXM1007@MNXD1"}

EQ_UNEVEN = {"MNXR900003": "60 MNXM1003@MNXD1 = 90 MNXM1004@MNXD1"}


def test_a_reduction_that_is_only_big_because_of_stoichiometry_is_collapsed():
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
    assert not P._still_balances(["MNXM1003"], ["MNXM1004"], FORMULAS, "P")
    assert P._still_balances(["MNXM1003"], ["MNXM1004", "MNXM1007"], FORMULAS, "P")

    uni, _f, tally = P.build(_offer("MNXR900003"), EQ_UNEVEN, FORMULAS, SMILES, _ranks(),
                             char_limit=8000, atom_limit=600)
    assert not [r for r in uni if r[4] == "P"], (
        "a collapse that changes the per-copy element balance was taken anyway")
    assert sum(v for k, v in tally.items() if k.startswith("reduction over")) >= 1


def _partial_layer():
    return pd.DataFrame(
        [dict(mnxr="R1", element="S", substrate="MNXM2001", product="MNXM2002", sub_idx=0,
              prod_idx=0, pair_w=1.0, method="partial_reduced", source="partial",
              confidence=1.0)],
        columns=list(L.ATOM_COLS))


def test_a_partial_pair_can_never_overwrite_a_real_map():
    real = _partial_layer().assign(method="rxnmapper", source="rxnmapper")
    tbl, report = L.stack([("ensemble", real), ("partial", _partial_layer())])
    assert report[-1]["added"] == 0
    assert (tbl["source"] == "partial").sum() == 0
    assert len(tbl) == 1


def test_a_partial_pair_lands_where_nothing_else_reached():
    empty = _partial_layer().iloc[0:0]
    tbl, report = L.stack([("ensemble", empty), ("partial", _partial_layer())])
    assert report[-1]["added"] == 1
    assert list(tbl["source"]) == ["partial"] and list(tbl["method"]) == ["partial_reduced"]


def test_the_ledger_has_its_own_word_for_a_partial_bank():
    assert "banked_partial" in W.OUTCOMES
    assert "partial_declined" in W.OUTCOMES
    assert W.PARTIAL_SOURCE == "partial", (
        "close tells a partial bank from a full one by this string; the lane stamps it, "
        "and two spellings of it is how an outcome silently stops being assigned")


def _ranks():
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

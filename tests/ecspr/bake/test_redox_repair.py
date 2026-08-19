from __future__ import annotations

import pandas as pd
import pytest

pytest.importorskip("rdkit", reason="the repair re-derives through atom_pairs")

from ecspr.bake.aam import layers as L, redox as R, worklist as W


METS = pd.DataFrame([
    ("MNXM8",      "NAD(+)",  "C21H26N7O14P2"),
    ("MNXM10",     "NADH",    "C21H27N7O14P2"),
    ("MNXM5",      "NADP(+)", "C21H25N7O17P3"),
    ("MNXM738702", "NADPH",   "C21H26N7O17P3"),
    ("MNXM1364149", "FAD",    "C27H33N9O15P2"),
    ("MNXM1105762", "FADH2",  "C27H33N9O15P2"),
    ("MNXM2001",   "glyceraldehyde 3-phosphate", "C3H5O6P"),
    ("MNXM2002",   "3-phospho-D-glyceroyl phosphate", "C3H4O10P2"),
    ("MNXM2003",   "L-cysteine", "C3H7NO2S"),
], columns=["mnxm", "name", "formula"])


def _resolved():
    fam, state, rows = R.resolve_cofactors(METS)
    return fam, state, rows


def _pair(mnxr, el, sub, prod, si, pi, w, method="disagree_diluted", src="a+b"):
    return (mnxr, el, sub, prod, si, pi, w, method, src, 1.0)


def _frame(rows):
    return pd.DataFrame(rows, columns=list(L.ATOM_COLS))


def test_resolution_joins_the_name_to_the_signature():
    fam, state, _rows = _resolved()
    assert fam["MNXM10"] == "nad" and state["MNXM10"] == "red"
    assert fam["MNXM8"] == "nad" and state["MNXM8"] == "ox"
    assert fam["MNXM738702"] == "nadp" and state["MNXM738702"] == "red"
    assert "MNXM2001" not in fam


def test_a_formula_that_contradicts_the_name_is_refused_not_admitted():
    mets = pd.DataFrame([("MNXMX", "NADH", "C6H12O6")],
                        columns=["mnxm", "name", "formula"])
    fam, _state, rows = R.resolve_cofactors(mets)
    assert fam == {}
    assert rows[0][5] == "refuse:formula_contradicts_family"


def test_a_signature_under_an_unknown_name_is_reported_and_never_admitted():
    mets = pd.DataFrame([("MNXMY", "beta-nicotinamide dinucleotide", "C21H26N7O14P2")],
                        columns=["mnxm", "name", "formula"])
    fam, _state, rows = R.resolve_cofactors(mets)
    assert fam == {}
    assert rows[0][5] == "report:signature_matches_unknown_name"
    assert rows[0][3] == "nad"


def test_a_couple_needs_both_states_across_the_arrow():
    fam, state, _ = _resolved()
    assert R.couples_of(["MNXM10", "MNXM2001"], ["MNXM8", "MNXM2002"], fam, state) \
        == frozenset({"nad"})


def test_nad_as_a_genuine_substrate_is_not_a_couple():
    fam, state, _ = _resolved()
    assert R.couples_of(["MNXM8"], ["MNXM2001"], fam, state) == frozenset()


def test_the_same_state_on_both_sides_is_not_a_couple():
    fam, state, _ = _resolved()
    assert R.couples_of(["MNXM8", "MNXM2001"], ["MNXM8", "MNXM2002"], fam, state) \
        == frozenset()


GAPDH = "MNXR144947"
EQ = {GAPDH: ("1 MNXM2001@MNXD1 + 1 MNXM8@MNXD1 = 1 MNXM2002@MNXD1 + 1 MNXM10@MNXD1")}


def _gapdh_scope():
    fam, state, _ = _resolved()
    return R.scope(EQ, fam, state, [GAPDH]), fam


def test_the_worked_example_loses_exactly_the_crossing_arms():
    in_scope, fam = _gapdh_scope()
    assert in_scope[GAPDH] == frozenset({"nad"})
    pairs = _frame([
        _pair(GAPDH, "C", "MNXM2002", "MNXM2001", 0, 0, 0.5),
        _pair(GAPDH, "C", "MNXM2002", "MNXM10", 0, 3, 0.5),
        _pair(GAPDH, "C", "MNXM8", "MNXM10", 1, 1, 1.0, "consensus", "a+b+c"),
    ])
    kept, refused, _t = R.repair(pairs, in_scope, fam)
    got = {(r.substrate, r.product) for r in kept.itertuples(index=False)}
    assert ("MNXM2002", "MNXM10") not in got, "BPG -> NADH crosses the skeleton"
    assert ("MNXM2002", "MNXM2001") in got, "BPG -> G3P is the real correspondence"
    assert ("MNXM8", "MNXM10") in got, "the couple's own pair is not a crossing"
    assert list(refused["predicate"]) == [R.PREDICATE]


def test_the_surviving_arm_is_rescaled_rather_than_left_diluted():
    in_scope, fam = _gapdh_scope()
    pairs = _frame([
        _pair(GAPDH, "C", "MNXM2002", "MNXM2001", 0, 0, 0.5),
        _pair(GAPDH, "C", "MNXM2002", "MNXM10", 0, 3, 0.5),
    ])
    kept, _r, tally = R.repair(pairs, in_scope, fam)
    assert len(kept) == 1
    assert kept["pair_w"].iloc[0] == pytest.approx(1.0)
    assert tally["arms rescaled"] == 1


def test_an_uncorroborated_arm_that_survives_keeps_its_half_credit():
    in_scope, fam = _gapdh_scope()
    pairs = _frame([
        _pair(GAPDH, "C", "MNXM2002", "MNXM2001", 0, 0, 0.5, "indigo_only", "indigo"),
    ])
    kept, refused, _t = R.repair(pairs, in_scope, fam)
    assert not len(refused)
    assert kept["pair_w"].iloc[0] == pytest.approx(0.5)


def test_sulfur_is_untouched_because_the_cofactors_carry_none():
    in_scope, fam = _gapdh_scope()
    pairs = _frame([
        _pair(GAPDH, "S", "MNXM2003", "MNXM10", 0, 0, 1.0),
    ])
    kept, refused, _t = R.repair(pairs, in_scope, fam)
    assert not len(refused)
    assert len(kept) == 1


def test_a_reaction_outside_the_scope_is_not_touched():
    fam, state, _ = _resolved()
    eq = {"MNXR9": "1 MNXM8@MNXD1 = 1 MNXM2001@MNXD1"}
    in_scope = R.scope(eq, fam, state, ["MNXR9"])
    assert in_scope == {}
    pairs = _frame([_pair("MNXR9", "C", "MNXM8", "MNXM2001", 0, 0, 1.0)])
    kept, refused, _t = R.repair(pairs, in_scope, fam)
    assert not len(refused) and len(kept) == 1


def test_one_cofactor_into_another_is_refused_too():
    fam, state, _ = _resolved()
    eq = {"MNXR7": "1 MNXM10@MNXD1 + 1 MNXM5@MNXD1 = 1 MNXM8@MNXD1 + 1 MNXM738702@MNXD1"}
    in_scope = R.scope(eq, fam, state, ["MNXR7"])
    assert in_scope["MNXR7"] == frozenset({"nad", "nadp"})
    pairs = _frame([
        _pair("MNXR7", "C", "MNXM10", "MNXM738702", 0, 0, 1.0),
        _pair("MNXR7", "C", "MNXM10", "MNXM8", 1, 1, 1.0),
    ])
    kept, refused, _t = R.repair(pairs, in_scope, fam)
    assert len(refused) == 1
    assert refused["product"].iloc[0] == "MNXM738702"
    assert len(kept) == 1


def test_an_emptied_key_is_re_derived_when_conservation_settles_the_remainder():
    in_scope, fam = _gapdh_scope()
    pairs = _frame([
        _pair(GAPDH, "C", "MNXM2001", "MNXM10", 0, 0, 1.0, "indigo_only", "indigo"),
    ])
    kept, refused, _t = R.repair(pairs, in_scope, fam)
    assert len(refused) == 1 and not len(kept)
    assert R.emptied_keys(pairs, kept) == [(GAPDH, "C")]

    formulas = dict(zip(METS["mnxm"], METS["formula"]))
    ranks = {("MNXM2001", "C"): [0, 1, 2], ("MNXM2002", "C"): [0, 1, 2]}
    red, tally = R.rederive([(GAPDH, "C")], in_scope, EQ, formulas, ranks, fam)
    assert tally["re-derived from conservation"] == 1
    assert set(zip(red["substrate"], red["product"])) == {("MNXM2001", "MNXM2002")}
    assert red["method"].iloc[0] == R.REDERIVED_METHOD
    assert len(red) == 9
    assert red["pair_w"].iloc[0] == pytest.approx(1 / 3)


def test_a_remainder_conservation_cannot_settle_is_a_loss_and_says_so():
    fam, state, _ = _resolved()
    eq = {"MNXR6": "1 MNXM2001@MNXD1 + 1 MNXM2003@MNXD1 + 1 MNXM8@MNXD1 "
                   "= 1 MNXM2002@MNXD1 + 1 MNXM10@MNXD1"}
    in_scope = R.scope(eq, fam, state, ["MNXR6"])
    formulas = dict(zip(METS["mnxm"], METS["formula"]))
    red, tally = R.rederive([("MNXR6", "C")], in_scope, eq, formulas, {}, fam)
    assert not len(red)
    assert tally["conservation does not settle the remainder"] == 1


def test_the_repair_verb_writes_all_five_files_and_the_summary_reconciles(tmp_path):
    lk = tmp_path / "lookups"
    lk.mkdir()
    pd.DataFrame([(GAPDH, EQ[GAPDH])], columns=["mnxr", "equation"]) \
        .to_parquet(lk / "reactions.parquet", index=False)
    METS.to_parquet(lk / "metabolites.parquet", index=False)
    pd.DataFrame([("MNXM2001", "C", [0, 1, 2])],
                 columns=["mnxm", "element", "ranks"]) \
        .to_parquet(lk / "atom_ranks.parquet", index=False)

    _frame([
        _pair(GAPDH, "C", "MNXM2002", "MNXM2001", 0, 0, 0.5),
        _pair(GAPDH, "C", "MNXM2002", "MNXM10", 0, 3, 0.5),
        _pair(GAPDH, "S", "MNXM2003", "MNXM10", 0, 0, 1.0),
    ]).to_parquet(tmp_path / "stack.parquet", index=False)

    class A:
        pairs = tmp_path / "stack.parquet"
        lookups = lk
        out = tmp_path / "pairs.parquet"
        out_refusals = tmp_path / "refusals.parquet"
        out_cofactors = tmp_path / "cofactors.tsv"
        out_emptied = tmp_path / "emptied.txt"
        out_summary = tmp_path / "summary.tsv"

    assert R.cmd_repair(A) == 0
    for p in (A.out, A.out_refusals, A.out_cofactors, A.out_emptied, A.out_summary):
        assert p.exists()
    kept = pd.read_parquet(A.out)
    assert len(kept) == 2
    assert A.out_emptied.read_text() == ""
    summary = dict(l.split("\t")[1:] for l in
                   A.out_summary.read_text().strip().splitlines()[1:])
    assert summary[R.PREDICATE] == "1"


def test_an_emptied_reaction_gets_its_own_outcome_and_never_mapped_nothing(tmp_path):
    assert "redox_emptied" in W.OUTCOMES

    wl = pd.DataFrame([(GAPDH, "mappable", ""), ("MNXR5", "mappable", "")],
                      columns=["mnxr", "verdict", "blocker_families"])
    wl.to_parquet(tmp_path / "wl.parquet", index=False)
    pd.DataFrame(columns=["mnxr", "element", "method", "source"]) \
        .to_parquet(tmp_path / "pairs.parquet", index=False)
    (tmp_path / "emptied.txt").write_text(f"{GAPDH}\n")

    class A:
        worklist = tmp_path / "wl.parquet"
        pairs = tmp_path / "pairs.parquet"
        rescued = None
        forecast = None
        redox_emptied = tmp_path / "emptied.txt"
        out = tmp_path / "ledger.parquet"
        out_summary = tmp_path / "ledger.tsv"

    W.cmd_close(A)
    got = dict(zip(*[pd.read_parquet(A.out)[c] for c in ("mnxr", "outcome")]))
    assert got[GAPDH] == "redox_emptied"
    assert got["MNXR5"] == "mapped_nothing"


def test_close_reads_what_the_forecast_offered_not_what_the_lane_built(tmp_path):
    wl = pd.DataFrame([("MNXR4", "mappable", "")],
                      columns=["mnxr", "verdict", "blocker_families"])
    wl.to_parquet(tmp_path / "wl.parquet", index=False)
    pd.DataFrame(columns=["mnxr", "element", "method", "source"]) \
        .to_parquet(tmp_path / "pairs.parquet", index=False)
    pd.DataFrame([("MNXR4", "C", True)], columns=["base_mnxr", "element", "offer"]) \
        .to_parquet(tmp_path / "forecast.parquet", index=False)

    class A:
        worklist = tmp_path / "wl.parquet"
        pairs = tmp_path / "pairs.parquet"
        rescued = None
        forecast = tmp_path / "forecast.parquet"
        redox_emptied = None
        out = tmp_path / "ledger.parquet"
        out_summary = tmp_path / "ledger.tsv"

    W.cmd_close(A)
    d = pd.read_parquet(A.out)
    assert d["outcome"].iloc[0] == "partial_declined"

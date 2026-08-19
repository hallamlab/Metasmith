from __future__ import annotations

import pytest

pytest.importorskip("rdkit", reason="the extractor imports rdkit at module scope")

from ecspr.bake import atom_pairs as AP
from ecspr.bake.aam.partial import KEY_SEP as AP_KEY_SEP


def test_parse_equation_writes_a_coefficient_out_as_repeats():
    subs, prods = AP.parse_equation("1 MNXM1@MNXD1 + 16 MNXM2@MNXD1 = 1 MNXM3@MNXD1")
    assert subs == ["MNXM1"] + ["MNXM2"] * 16
    assert prods == ["MNXM3"]


def test_parse_equation_refuses_a_one_sided_equation():
    assert AP.parse_equation("1 MNXM1@MNXD1 = ") is None
    assert AP.parse_equation("no equals sign here") is None


def test_strict_mode_refuses_when_a_participant_went_missing():
    pairs, status = AP.pairs_from_mapped(
        "[CH4:1]>>[CH4:1]", ["MNXM1", "MNXM2"], ["MNXM3"], {}, align="strict")
    assert (pairs, status) == ({}, "stripped")


def test_structural_mode_does_not_apply_the_participant_count_guard():
    pairs, status = AP.pairs_from_mapped(
        "[CH4:1]>>[CH4:1]", ["MNXM1", "MNXM2"], ["MNXM3"], {}, align="structural")
    assert status != "stripped"


def test_an_absent_mapping_is_its_own_status():
    assert AP.pairs_from_mapped(None, [], [], {}) == ({}, "no_mapping")
    assert AP.pairs_from_mapped("", [], [], {}) == ({}, "no_mapping")
    assert AP.pairs_from_mapped("not a reaction", [], [], {})[1] in ("unparseable", "no_mapping")


@pytest.mark.parametrize("formula,element,expected", [
    ("C6H12O6", "C", 6),
    ("C6H12O6", "N", 0),
    ("H2O", "O", 1),
    ("C21H27N7O14P2", "N", 7),
    ("*", "C", None),
    ("C*H2", "C", None),
    ("Fe4S4(SR)4", "S", None),
    ("", "C", None),
    (None, "C", None),
])
def test_count_element_distinguishes_absent_from_untrusted(formula, element, expected):
    assert AP.count_element(formula, element) == expected


def test_forced_pairs_emits_the_unique_bijection_at_full_weight():
    out = AP.forced_pairs(
        ["M_h2s", "M_nadp"] * 3, ["M_so3", "M_nadph"] * 3,
        formulas={"M_h2s": "H2S", "M_so3": "O3S", "M_nadp": "C21H26N7O17P3",
                  "M_nadph": "C21H27N7O17P3"},
        ranks_of={("M_h2s", "S"): [0], ("M_so3", "S"): [0]})
    assert out == {("S", "M_h2s", "M_so3"): [(0, 0, 1.0)]}


def test_forced_pairs_spreads_a_multi_atom_transfer_instead_of_choosing():
    out = AP.forced_pairs(
        ["M_a"], ["M_b"],
        formulas={"M_a": "C2H6O", "M_b": "C2H4O2"},
        ranks_of={("M_a", "C"): [0, 1], ("M_b", "C"): [0, 1]})
    triples = out[("C", "M_a", "M_b")]
    assert len(triples) == 4, "every candidate pairing must be present"
    assert all(w == 0.5 for _s, _p, w in triples)
    for r in (0, 1):
        assert sum(w for s, _p, w in triples if s == r) == pytest.approx(1.0)
        assert sum(w for _s, p, w in triples if p == r) == pytest.approx(1.0)


def test_forced_pairs_refuses_when_a_formula_cannot_be_trusted():
    out = AP.forced_pairs(
        ["M_a", "M_poly"], ["M_b"],
        formulas={"M_a": "C2H6O", "M_poly": "*", "M_b": "C2H4O2"},
        ranks_of={("M_a", "C"): [0, 1], ("M_b", "C"): [0, 1]})
    assert out == {}, "a wildcard formula licensed a forced pairing"


def _mixed(tmp_path):
    import pandas as pd
    from ecspr.bake.aam import partial as P
    from ecspr.bake.aam import universe as U

    reac = tmp_path / "reac_prop.tsv"
    reac.write_text("#ID\tequation\n"
                    "MNXR1\t1 MNXM1@MNXD1 + 1 MNXM2@MNXD1 = "
                    "1 MNXM3@MNXD1 + 1 MNXM4@MNXD1\n")
    chem = tmp_path / "chem_prop.tsv"
    chem.write_text(
        "#ID\tname\treference\tformula\tcharge\tmass\tInChI\tInChIKey\tSMILES\n"
        "MNXM1\ta\t\tC2H6\t0\t0\t\t\tCC\n"
        "MNXM2\tb\t\tNH3\t0\t0\t\t\tN\n"
        "MNXM3\tc\t\tC2H6O\t0\t0\t\t\tCCO\n"
        "MNXM4\td\t\tCH5N\t0\t0\t\t\tCN\n")

    uni = pd.DataFrame(
        [("MNXR1", "mappable", "CC.N>>CCO.CN", "MNXR1", None, [], [], 10, 12, False,
          "whole"),
         (f"MNXR1{P.KEY_SEP}N", "mappable", "N>>CN", "MNXR1", "N",
          ["MNXM2"], ["MNXM4"], 3, 5, False, "reduced")],
        columns=list(U.UNIVERSE_COLS))
    up = tmp_path / "universe.parquet"
    uni.to_parquet(up, index=False)

    aam = tmp_path / "aam.tsv"
    aam.write_text(
        "mnxr\trxn_smiles\tmapped_rxn_smiles\tconfidence\n"
        "MNXR1\tCC.N>>CCO.CN\t[CH3:1][CH3:2].[NH3:3]>>[CH3:1][CH2:2][OH:4].[CH3:5][NH2:3]\t0.9\n"
        f"MNXR1{P.KEY_SEP}N\tN>>CN\t[NH3:1]>>[CH3:2][NH2:1]\t0.9\n")
    return reac, chem, up, aam


def test_one_extraction_gives_each_class_the_template_it_was_submitted_under(tmp_path):
    import argparse
    import pandas as pd

    reac, chem, up, aam = _mixed(tmp_path)
    out, status = tmp_path / "pairs.parquet", tmp_path / "status.tsv"
    AP.cmd_extract(argparse.Namespace(
        aam=[str(aam)], universe=str(up), reac_prop=str(reac), chem_prop=str(chem),
        out=str(out), out_status=str(status), align="strict",
        connectivity_fallback=False, fallback_forced=False,
        balance=None, placeholders=None, resolved=None, min_confidence=None))

    st = pd.read_csv(status, sep="\t").set_index("mnxr")["status"].to_dict()
    assert st["MNXR1"] == "ok"
    assert st[f"MNXR1{AP_KEY_SEP}N"] == "ok", \
        "the reduction must not be refused as `stripped` against the full equation"


def test_every_pair_row_says_which_class_of_submission_it_answers(tmp_path):
    import argparse
    import pandas as pd

    reac, chem, up, aam = _mixed(tmp_path)
    out, status = tmp_path / "pairs.parquet", tmp_path / "status.tsv"
    AP.cmd_extract(argparse.Namespace(
        aam=[str(aam)], universe=str(up), reac_prop=str(reac), chem_prop=str(chem),
        out=str(out), out_status=str(status), align="strict",
        connectivity_fallback=False, fallback_forced=False,
        balance=None, placeholders=None, resolved=None, min_confidence=None))

    d = pd.read_parquet(out)
    assert set(d["mnxr"]) == {"MNXR1"}, "a pair row is keyed on the REAL reaction"
    assert set(d["submission_class"]) == {"whole", "reduced"}
    assert set(d.loc[d["submission_class"] == "reduced", "element"]) == {"N"}


def test_a_table_extracted_without_a_universe_cannot_be_split_into_layers(tmp_path):
    import pandas as pd
    from ecspr.bake.aam import layers as L

    p = tmp_path / "nolabels.parquet"
    pd.DataFrame([dict(mnxr="MNXR1", element="C", substrate="MNXM1", product="MNXM3",
                       n_atoms=1, sub_idx="0", prod_idx="0", pair_w="1.0")]).to_parquet(
        p, index=False)
    assert len(L.explode(p, method="m", source="s")) == 1
    with pytest.raises(SystemExit, match="submission_class"):
        L.explode(p, method="m", source="s", only_class="whole")

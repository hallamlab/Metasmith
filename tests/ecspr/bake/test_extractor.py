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
        balance=None, placeholders=None, resolved=None, min_confidence=None,
        on_noninjective="trim"))

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
        balance=None, placeholders=None, resolved=None, min_confidence=None,
        on_noninjective="trim"))

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


# MNXR166070 (rhamnulokinase) as Indigo mapped it in the deployed bake: the ATP ribose
# carries L-rhamnulose-1-phosphate's map numbers 2/4/6/8/10, and ADP's own ribose carries
# none. `fabfos/bench-fba` found it as five sugar carbons split 0.5/0.5 into ATP.
_RHAMNULOSE_1P = "C[C@H](O)[C@H](O)[C@@H](O)C(=O)COP(=O)([O-])[O-]"
_RHAMNULOSE = "C[C@H](O)[C@H](O)[C@@H](O)C(=O)CO"
_ADP = "NC1=NC=NC2=C1N=CN2[C@@H]1O[C@H](COP(=O)([O-])OP(=O)([O-])[O-])[C@@H](O)[C@H]1O"
_ATP = ("NC1=NC=NC2=C1N=CN2[C@@H]1O[C@H](COP(=O)([O-])OP(=O)([O-])OP(=O)([O-])[O-])"
        "[C@@H](O)[C@H]1O")
_INDIGO_166070 = (
    "[CH3:1][C@@H:2]([C@@H:4]([C@H:6]([C:8]([CH2:10][O:11][P:12]([O-:15])([O-])=[O:13])"
    "=[O:9])[OH:7])[OH:5])[OH:3].[H+].[NH2:17][C:18]1[C:23]2[N:24]=[CH:25][N:26]([C@H]3"
    "[C@H](O)[C@H](O)[C@@H](C[O:31][P:32]([O:35][P:36]([O-:39])([O-:38])=[O:37])([O-:34])"
    "=[O:33])O3)[C:22]=2[N:21]=[CH:20][N:19]=1>>[NH2:17][C:18]1[C:23]2[N:24]=[CH:25]"
    "[N:26]([C@H:2]3[C@H:4]([OH:5])[C@H:6]([OH:7])[C@@H:8]([CH2:10][O:11][P:12]([O:31]"
    "[P:32]([O:35][P:36]([O-:39])([O-:38])=[O:37])([O-:34])=[O:33])([O-:15])=[O:13])"
    "[O:3]3)[C:22]=2[N:21]=[CH:20][N:19]=1.[CH3:1][C@@H:2]([C@@H:4]([C@H:6]([C:8]"
    "([CH2:10][OH:11])=[O:9])[OH:7])[OH:5])[OH:3]")

_SUB_166070 = ["MNXM1104186", "MNXM1", "MNXM40333"]
_PROD_166070 = ["MNXM3", "MNXM731755"]


def _canon(smi):
    from rdkit import Chem
    mol = Chem.MolFromSmiles(smi)
    for a in mol.GetAtoms():
        a.SetAtomMapNum(0)
    return Chem.MolToSmiles(mol, canonical=True)


def _canon_166070():
    return {"MNXM1104186": _canon(_RHAMNULOSE_1P), "MNXM1": _canon("[H+]"),
            "MNXM40333": _canon(_ADP), "MNXM3": _canon(_ATP),
            "MNXM731755": _canon(_RHAMNULOSE)}


def test_duplicate_map_numbers_reads_both_sides():
    from rdkit.Chem import AllChem
    rxn = AllChem.ReactionFromSmarts(_INDIGO_166070, useSmiles=True)
    subs = [rxn.GetReactantTemplate(i) for i in range(rxn.GetNumReactantTemplates())]
    prods = [rxn.GetProductTemplate(j) for j in range(rxn.GetNumProductTemplates())]
    assert AP.duplicate_map_numbers(subs, prods) == {2, 3, 4, 5, 6, 7, 8, 10, 11}
    assert AP.duplicate_map_numbers(subs, []) == set(), "the substrate side is injective"


def test_a_reused_map_number_never_becomes_a_transfer():
    pairs, status = AP.pairs_from_mapped(
        _INDIGO_166070, _SUB_166070, _PROD_166070, _canon_166070())
    assert status == "injectivity_trimmed"
    assert ("C", "MNXM1104186", "MNXM3") not in pairs, \
        "a sugar carbon reached ATP's ribose because one map number labelled two atoms"
    # What the member does NOT contradict itself about survives: rhamnulose-1-phosphate's
    # one unambiguous carbon, and the five ADP carbons Indigo did map.
    assert [s for s, _p, _w in pairs[("C", "MNXM1104186", "MNXM731755")]] == [0]
    assert len(pairs[("C", "MNXM40333", "MNXM3")]) == 5


def test_refuse_drops_the_whole_reaction_rather_than_the_atom():
    pairs, status = AP.pairs_from_mapped(
        _INDIGO_166070, _SUB_166070, _PROD_166070, _canon_166070(),
        on_noninjective="refuse")
    assert (pairs, status) == ({}, "non_injective")


def test_the_gate_counts_what_it_dropped():
    stats = {}
    AP.pairs_from_mapped(_INDIGO_166070, _SUB_166070, _PROD_166070, _canon_166070(),
                         stats=stats)
    assert stats["n_dup_map"] == 9


def test_an_injective_map_is_untouched():
    stats = {}
    pairs, status = AP.pairs_from_mapped(
        "[CH3:1][CH3:2].[NH3:3]>>[CH3:1][CH2:2][OH:4].[CH3:5][NH2:3]",
        ["MNXM1", "MNXM2"], ["MNXM3", "MNXM4"],
        {"MNXM1": _canon("CC"), "MNXM2": _canon("N"), "MNXM3": _canon("CCO"),
         "MNXM4": _canon("CN")}, stats=stats)
    assert status == "ok"
    assert stats["n_dup_map"] == 0
    assert pairs[("C", "MNXM1", "MNXM3")]


# A multi-component participant -- MetaNetX writes a salt as one metabolite with one
# `.`-joined SMILES -- reaches the mapper as several components and returns as several
# templates. `coalesce_components` merges them back before the count guard runs.

_SALT_CANON = {
    "MNXM_salt": _canon("[NH4+].[OH-]"),
    "MNXM_am": _canon("[NH4+]"),
    "MNXM_prod": _canon("CN"),
}


def test_a_salt_is_one_participant_and_several_templates():
    mols, merged = AP.coalesce_components(
        [AP.Chem.MolFromSmiles(s) for s in ("[NH4+]", "[OH-]")],
        ["MNXM_salt"], _SALT_CANON)
    assert merged is True
    assert len(mols) == 1
    assert mols[0].GetNumAtoms() == 2


def test_coalescing_leaves_a_single_component_side_alone():
    given = [AP.Chem.MolFromSmiles("CC"), AP.Chem.MolFromSmiles("N")]
    mols, merged = AP.coalesce_components(given, ["MNXM_am", "MNXM_prod"], _SALT_CANON)
    assert merged is False
    assert mols is given


def test_a_component_is_not_taken_from_the_participant_that_owns_it():
    # `[NH4+]` is both a participant of its own and a component of the salt. Claiming it
    # for the salt would leave the real participant unmatched, so nothing is merged.
    mols, merged = AP.coalesce_components(
        [AP.Chem.MolFromSmiles(s) for s in ("[NH4+]", "[OH-]")],
        ["MNXM_am", "MNXM_salt"], _SALT_CANON)
    assert merged is False


def test_a_salt_no_longer_strips_the_whole_reaction():
    pairs, status = AP.pairs_from_mapped(
        "[NH4+:1].[OH-:2]>>[CH3:3][NH2:1]",
        ["MNXM_salt"], ["MNXM_prod"], _SALT_CANON)
    assert status != "stripped"
    assert pairs[("N", "MNXM_salt", "MNXM_prod")]


def test_the_merged_ranks_are_the_metabolite_s_own():
    # The indices a pair row carries must be nodes `lookup::atom_ranks` publishes for the
    # WHOLE metabolite -- ranking each component alone would restart them from zero and
    # the row would claim an atom that has no node.
    whole = AP.canonical_ranks(AP.Chem.MolFromSmiles("[NH4+].[OH-]"))
    merged, _ = AP.coalesce_components(
        [AP.Chem.MolFromSmiles(s) for s in ("[OH-]", "[NH4+]")],
        ["MNXM_salt"], _SALT_CANON)
    assert sorted(AP.canonical_ranks(merged[0])) == sorted(whole)

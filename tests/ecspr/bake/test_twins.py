from __future__ import annotations

import pandas as pd
import pytest

pytest.importorskip("rdkit")

from ecspr.bake.aam import curation as C                              # noqa: E402
from ecspr.bake.aam import twins as T                                 # noqa: E402

MET_ROWS = [
    ("MNXM8975",    "Acceptor",   None,      None,                    None),
    ("MNXM35",      "A",          "H4*",     "[H][*]([H])([H])[H]",   None),
    ("MNXM1102421", "AH2",        None,      None,                    None),
    ("MNXM1105763", "AH2",        "H4*",     "[H][*]([H])([H])[H]",   None),
    ("MNXM900",     "hexadecenoate", None,   None,                    None),
    ("MNXM526245",  "hexadecenoate", "CO2*", "[*]C(=O)[O-]",          None),
    ("MNXM740696",  "ACP",        None,      None,                    None),
    ("MNXM1089660", "all holo-acyl carrier proteins", "C14H25N3O8PS*2",
     C._vehicle((14, 3, 1, 1), caps=2),                               None),
    ("MNXM1102130", "UDP",        None,      None,                    None),
    ("MNXM1102128", "UDP",        "C9H11N2O12P2",
     "OC1C(O)C(COP(=O)([O-])OP(=O)([O-])[O-])OC1n1ccc(=O)[nH]c1=O",   "XCCTYIAWTASOJW"),
    ("MNXMTEST1",   "ethanol",    None,      None,                    None),
    ("MNXMTEST2",   "ethanol",    "C2H6O",   "CCO",                   None),
    *[(f"MNXMFAN{i}", "a generic alcohol", "C2H6O", "CCO", None) for i in range(12)],
    ("MNXMFANQ",    "a generic alcohol", None,   None,                None),
    ("MNXMAMBQ",    "some carrier", None,      None,                  None),
    ("MNXMAMB1",    "some carrier", "H4*",     "[H][*]([H])([H])[H]", None),
    ("MNXMAMB2",    "some carrier", "H2*",     "[H][*][H]",           None),
    ("MNXM1",       "H(+)",       "H",        "[H+]",                 None),
    ("MNXM2",       "water",      "H2O",      "O",                    None),
]

XREF_ROWS = [
    ("kegg",     "C00028",  "MNXM8975",    "Acceptor||A||Hydrogen-acceptor||Oxidized donor"),
    ("seed",     "cpd00109","MNXM35",      "Acceptor||A||Hydrogen-acceptor||Oxidized donor"),
    ("kegg",     "C00030",  "MNXM1102421", "AH2||Reduced acceptor||Hydrogen-donor"),
    ("seed",     "cpd00110","MNXM1105763", "AH2||Reduced acceptor||Hydrogen-donor"),
    ("kegg",     "C08362",  "MNXM900",     "hexadecenoate"),
    ("seed",     "cpd15269","MNXM526245",  "hexadecenoate"),
    ("seed",     "cpd11493","MNXM740696",  "ACP||acyl-carrier protein"),
    ("kegg",     "C00229",  "MNXM1089660", "ACP||acyl-carrier protein"),
    ("kegg",     "C00015",  "MNXM1102128", "UDP||Uridine 5'-diphosphate"),
    ("metacyc",  "UDP-SUG", "MNXM1102130", "UDP"),
    ("chebi",    "CHEBI:16236", "MNXMTEST1", "ethanol"),
    ("chebi",    "CHEBI:16236", "MNXMTEST2", "ethanol"),
]

RXN_ROWS = [
    ("MNXR0001", ["MNXM8975", "MNXM2"],    ["MNXM1102421", "MNXM1"], ["MNXM8975", "MNXM1102421"]),
    ("MNXR0002", ["MNXM900", "MNXM2"],     ["MNXM1"],                ["MNXM900"]),
    ("MNXR0003", ["MNXM740696", "MNXM2"],  ["MNXM1"],                ["MNXM740696"]),
    ("MNXR0004", ["MNXM1102130", "MNXM2"], ["MNXM1"],                ["MNXM1102130"]),
    ("MNXR0005", ["MNXMTEST1", "MNXM2"],   ["MNXM1"],                ["MNXMTEST1"]),
    ("MNXR0006", ["MNXMFANQ", "MNXM2"],    ["MNXM1"],                ["MNXMFANQ"]),
    ("MNXR0007", ["MNXMAMBQ", "MNXM2"],    ["MNXM1"],                ["MNXMAMBQ"]),
]


def _counts(smiles, formula):
    from ecspr.bake.aam import recount as R
    return R.recount(formula, smiles, None)


@pytest.fixture(scope="module")
def built(tmp_path_factory):
    d = tmp_path_factory.mktemp("lookups")
    mets = pd.DataFrame(
        [dict(mnxm=m, name=n, formula=f, smiles=s, has_smiles=bool(s), inchikey_cc=k)
         for m, n, f, s, k in MET_ROWS])
    mets.to_parquet(d / "metabolites.parquet")

    pd.DataFrame([dict(mnxr=r, substrates=s, products=p, blockers=b,
                       n_blockers=len(b)) for r, s, p, b in RXN_ROWS]) \
        .to_parquet(d / "reactions.parquet")

    pd.DataFrame([dict(kind="chem", namespace=ns, foreign_id=fid, mnx_id=m,
                       description=desc) for ns, fid, m, desc in XREF_ROWS]) \
        .to_parquet(d / "xrefs.parquet")

    pd.DataFrame([dict(source="metanetx", source_id=m, raw_name=n)
                  for m, n, _f, _s, _k in MET_ROWS if n]) \
        .to_parquet(d / "synonyms.parquet")

    rows = []
    for m, _n, f, s, _k in MET_ROWS:
        counts, residue, source = _counts(s, f)
        for X in T.ELEMENTS:
            rows.append(dict(mnxm=m, element=X, n_atoms=counts[X],
                             n_residue=residue, source=source))
    ec = d / "element_counts.parquet"
    df = pd.DataFrame(rows)
    df["n_atoms"] = df["n_atoms"].astype("Int32")
    df["n_residue"] = df["n_residue"].astype("Int32")
    df.to_parquet(ec)

    tab = T.Tables(d, ec)
    of_blocker, carriers = T.alias_index(d, tab)
    blk = {r["mnxm"]: r for r in T.resolve_blockers(tab, of_blocker, carriers)[0]}
    accs = T.accessions_of(d, set(tab.blockers) | set(tab.smiles_of))
    nt = {r["mnxm"]: r for r in T.resolve_nametwins(tab, accs)[0]}
    return dict(dir=d, ec=ec, tab=tab, blockers=blk, nametwin=nt,
                cross=T.resolve_blockers(tab, of_blocker, carriers)[1])


@pytest.mark.parametrize("blocker,twin", [
    ("MNXM8975", "MNXM35"),
    ("MNXM1102421", "MNXM1105763"),
])
def test_a_structureless_role_takes_MNXrefs_own_neutral_record(built, blocker, twin):
    d = built["blockers"][blocker]
    assert d["decision"] == "accept"
    assert d["twin"] == twin


def test_the_accepted_row_asserts_no_atoms_and_still_resolves_the_metabolite(built):
    rows = [r for r in built["cross"] if r["mnxm"] == "MNXM8975"]
    assert len(rows) == 1, "one row, so `admit` records it as resolved"
    assert rows[0]["n_atoms"] == 0
    assert rows[0]["lane"] == "blockers"
    for X in T.ELEMENTS:
        assert C.count_struct(rows[0]["smiles"], X) == 0


def test_the_name_guard_runs_BEFORE_the_element_test(built):
    d = built["blockers"]["MNXM900"]
    assert d["decision"] == "refuse:name_contradicts_twin"
    assert "16 C" in d["evidence"]


def test_the_acyl_carrier_is_refused_by_arithmetic_and_not_by_a_list(built):
    d = built["blockers"]["MNXM740696"]
    assert d["decision"] == "refuse:not_element_neutral"
    assert d["twin"] == "MNXM1089660"
    assert built["tab"].counts_of("MNXM1089660") == (14, 3, 1, 1)


def test_a_key_carried_by_a_crowd_is_vocabulary_rather_than_evidence(built):
    assert built["blockers"]["MNXMFANQ"]["decision"] == "refuse:no_structured_twin"


def test_two_neutral_twins_that_disagree_are_a_refusal_not_a_coin_toss(built):
    assert built["blockers"]["MNXMAMBQ"]["decision"] == "refuse:ambiguous_twins"


def test_the_tie_break_is_the_accession_and_never_set_iteration_order():
    got = sorted(["MNXM1102421", "MNXM35", "MNXM8975", "MNXM9"], key=T._acc_key)
    assert got == ["MNXM9", "MNXM35", "MNXM8975", "MNXM1102421"]


def test_one_name_over_two_compounds_is_not_evidence(built):
    d = built["nametwin"]["MNXM1102130"]
    assert d["decision"] == "refuse:no_evidence"


def test_a_shared_accession_admits_the_duplicate(built):
    d = built["nametwin"]["MNXMTEST1"]
    assert d["decision"] == "accept"
    assert d["twin"] == "MNXMTEST2"
    assert "CHEBI:16236" in d["evidence"]


def test_an_element_neutral_twin_is_not_this_lanes_claim(built):
    assert built["nametwin"]["MNXM1102421"]["decision"] \
        == "refuse:twin_has_no_tracked_atoms"
    assert built["nametwin"]["MNXM8975"]["decision"] == "refuse:no_same_name_twin"


@pytest.mark.parametrize("name,expect", [
    ("hexadecenoate", (16, 0, 0, 0)),
    ("Acceptor", None),
    ("ACP", None),
    ("UDP", None),
])
def test_a_name_asserts_a_count_only_where_nomenclature_actually_states_one(name, expect):
    assert T.name_budget(name) == expect


def test_the_recount_is_consulted_before_the_formula_and_the_residues_must_cancel():
    subs, prods = ["A", "W"], ["B", "W"]
    formula_of = {"A": "C6H12O6*", "B": "C6H12O6*", "W": "H2O"}
    counts = {"A": (6, 0, 0, 0), "B": (6, 0, 0, 0)}

    assert C.concrete_balance(subs, prods, formula_of, set(), "C") is None

    assert C.concrete_balance(subs, prods, formula_of, set(), "C",
                              counts_of=counts,
                              residue_of={"A": 1, "B": 1}) is True

    assert C.concrete_balance(subs, prods, formula_of, set(), "C",
                              counts_of=counts,
                              residue_of={"A": 1, "B": 2}) is None

    assert C.concrete_balance(subs, prods, formula_of, set(), "C",
                              counts_of=counts,
                              residue_of={"A": None, "B": 1}) is None


def test_the_two_new_lanes_bracket_the_priority_order():
    p = C.LANE_PRIORITY
    assert p[0] == "nametwin"
    assert p.index("blockers") == p.index("acceptor") - 1
    assert set(p) >= {"twin", "carrier", "supplier", "override"}

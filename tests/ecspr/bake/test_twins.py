"""The twin searches: what they recover, and the four ways they must refuse.

Every id, name, formula and SMILES below was read out of the rebuilt MNXref 4.5 lookups
and is reproduced here as a fixture rather than joined against them, because a five-second
gate cannot open a 138 MB compound table. What the fixture cannot fake is the DECISION,
and that is the whole of what is asserted:

  * the accept -- MNXref's own element-neutral record, found through a cross-reference
    description the two ids share under DIFFERENT accessions;
  * the name guard beating the balance test to a case that balances (`MNXM900`);
  * the element-neutral predicate refusing the acyl carrier on its own numbers, with
    nobody having written `ACP` down anywhere;
  * a shared name that is not evidence (`UDP` the glycan against `UDP` the nucleotide).

Plus the two things a lane like this gets wrong silently: a tie broken on set iteration
order, and an unknown count read as a zero.
"""
from __future__ import annotations

import pandas as pd
import pytest

pytest.importorskip("rdkit")

from ecspr.bake.aam import curation as C                              # noqa: E402
from ecspr.bake.aam import twins as T                                 # noqa: E402

# (mnxm, name, formula, smiles, inchikey_cc)
MET_ROWS = [
    # -- A1: the two blockers and the twins MNXref itself holds for them ----------
    ("MNXM8975",    "Acceptor",   None,      None,                    None),
    ("MNXM35",      "A",          "H4*",     "[H][*]([H])([H])[H]",   None),
    ("MNXM1102421", "AH2",        None,      None,                    None),
    ("MNXM1105763", "AH2",        "H4*",     "[H][*]([H])([H])[H]",   None),
    # -- the P3 canary: the twin BALANCES and only the name refuses it ------------
    ("MNXM900",     "hexadecenoate", None,   None,                    None),
    ("MNXM526245",  "hexadecenoate", "CO2*", "[*]C(=O)[O-]",          None),
    # -- A2, held by arithmetic: the acyl carrier's twin carries real atoms -------
    # The twin's SMILES is a STAND-IN drawn to the record's own counts rather than the
    # real thioester, because what the predicate reads is the count and the fixture
    # should not pretend to a structure it did not copy. The formula is verbatim.
    ("MNXM740696",  "ACP",        None,      None,                    None),
    ("MNXM1089660", "all holo-acyl carrier proteins", "C14H25N3O8PS*2",
     C._vehicle((14, 3, 1, 1), caps=2),                               None),
    # -- the P5 canary: one name, two compounds ----------------------------------
    ("MNXM1102130", "UDP",        None,      None,                    None),
    ("MNXM1102128", "UDP",        "C9H11N2O12P2",
     "OC1C(O)C(COP(=O)([O-])OP(=O)([O-])[O-])OC1n1ccc(=O)[nH]c1=O",   "XCCTYIAWTASOJW"),
    # -- a synthetic accession-shared duplicate, to exercise the accept path ------
    ("MNXMTEST1",   "ethanol",    None,      None,                    None),
    ("MNXMTEST2",   "ethanol",    "C2H6O",   "CCO",                   None),
    # -- vocabulary, not evidence: one key on many structured records ------------
    *[(f"MNXMFAN{i}", "a generic alcohol", "C2H6O", "CCO", None) for i in range(12)],
    ("MNXMFANQ",    "a generic alcohol", None,   None,                None),
    # -- two neutral twins that disagree about the structure ---------------------
    ("MNXMAMBQ",    "some carrier", None,      None,                  None),
    ("MNXMAMB1",    "some carrier", "H4*",     "[H][*]([H])([H])[H]", None),
    ("MNXMAMB2",    "some carrier", "H2*",     "[H][*][H]",           None),
    # -- concrete partners, so a blocker has a reaction to gate ------------------
    ("MNXM1",       "H(+)",       "H",        "[H+]",                 None),
    ("MNXM2",       "water",      "H2O",      "O",                    None),
]

# (mnxm, description) -- the `||`-joined list a source database attached to its xref.
XREF_ROWS = [
    # MEASURED: MNXM8975 and MNXM35 carry this token list across kegg, seed and
    # sabiork under DIFFERENT accessions, which is why the description is the evidence
    # here and the accession is not.
    ("kegg",     "C00028",  "MNXM8975",    "Acceptor||A||Hydrogen-acceptor||Oxidized donor"),
    ("seed",     "cpd00109","MNXM35",      "Acceptor||A||Hydrogen-acceptor||Oxidized donor"),
    ("kegg",     "C00030",  "MNXM1102421", "AH2||Reduced acceptor||Hydrogen-donor"),
    ("seed",     "cpd00110","MNXM1105763", "AH2||Reduced acceptor||Hydrogen-donor"),
    ("kegg",     "C08362",  "MNXM900",     "hexadecenoate"),
    ("seed",     "cpd15269","MNXM526245",  "hexadecenoate"),
    ("seed",     "cpd11493","MNXM740696",  "ACP||acyl-carrier protein"),
    ("kegg",     "C00229",  "MNXM1089660", "ACP||acyl-carrier protein"),
    # The UDP pair shares NO accession -- that is the fact the lane turns on.
    ("kegg",     "C00015",  "MNXM1102128", "UDP||Uridine 5'-diphosphate"),
    ("metacyc",  "UDP-SUG", "MNXM1102130", "UDP"),
    # ... while the synthetic duplicate shares one.
    ("chebi",    "CHEBI:16236", "MNXMTEST1", "ethanol"),
    ("chebi",    "CHEBI:16236", "MNXMTEST2", "ethanol"),
]

# (mnxr, substrates, products, blockers)
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
    """What `lookup::element_counts` holds for a fixture row, by the real routes."""
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


# =====================================================================
# blockers -- the element-neutral twin
# =====================================================================

@pytest.mark.parametrize("blocker,twin", [
    ("MNXM8975", "MNXM35"),
    ("MNXM1102421", "MNXM1105763"),
])
def test_a_structureless_role_takes_MNXrefs_own_neutral_record(built, blocker, twin):
    """And it is found through a shared DESCRIPTION, not a shared accession.

    The two ids are filed under different accessions in every source that carries them;
    what they share is the token list those sources attached. An accession-only search
    finds neither of these.
    """
    d = built["blockers"][blocker]
    assert d["decision"] == "accept"
    assert d["twin"] == twin


def test_the_accepted_row_asserts_no_atoms_and_still_resolves_the_metabolite(built):
    rows = [r for r in built["cross"] if r["mnxm"] == "MNXM8975"]
    assert len(rows) == 1, "one row, so `admit` records it as resolved"
    assert rows[0]["n_atoms"] == 0
    assert rows[0]["lane"] == "blockers"
    # The atoms it declines to claim are exactly what makes it safe: `count_struct`
    # reads the same zero off the same SMILES, which is what `admit` checks.
    for X in T.ELEMENTS:
        assert C.count_struct(rows[0]["smiles"], X) == 0


def test_the_name_guard_runs_BEFORE_the_element_test(built):
    """MNXM900 'hexadecenoate' against `CO2*`: it balances, and the name is the refusal.

    Both predicates fire -- the twin carries a carbon, so it is not element-neutral
    either -- and the ORDER is what decides which reason the reader gets. The
    informative one is that a sixteen-carbon fatty acid was about to be drawn as a
    formate; `not_element_neutral` is true and says nothing.
    """
    d = built["blockers"]["MNXM900"]
    assert d["decision"] == "refuse:name_contradicts_twin"
    assert "16 C" in d["evidence"]


def test_the_acyl_carrier_is_refused_by_arithmetic_and_not_by_a_list(built):
    """A2 held, without `ACP` appearing anywhere in this module.

    MNXM1089660 carries C14 N3 S1 P1 through the exact thioester an acyl stand-in must
    never fabricate. The predicate refuses it on those numbers, so a carrier family
    nobody has thought of is refused on the same terms.
    """
    d = built["blockers"]["MNXM740696"]
    assert d["decision"] == "refuse:not_element_neutral"
    assert d["twin"] == "MNXM1089660"
    assert built["tab"].counts_of("MNXM1089660") == (14, 3, 1, 1)


def test_a_key_carried_by_a_crowd_is_vocabulary_rather_than_evidence(built):
    """Twelve structured records answer to 'a generic alcohol', so none of them is THE
    record for it, and borrowing an arbitrary member of a family is the failure a
    fanout gate exists to stop."""
    assert built["blockers"]["MNXMFANQ"]["decision"] == "refuse:no_structured_twin"


def test_two_neutral_twins_that_disagree_are_a_refusal_not_a_coin_toss(built):
    assert built["blockers"]["MNXMAMBQ"]["decision"] == "refuse:ambiguous_twins"


def test_the_tie_break_is_the_accession_and_never_set_iteration_order():
    """A set-iteration tie-break once made a reference lane's yield wander over
    887/891/893 on byte-identical inputs. Numeric, so MNXM35 precedes MNXM1102421 --
    string order would reverse them, which is not wrong so much as arbitrary."""
    got = sorted(["MNXM1102421", "MNXM35", "MNXM8975", "MNXM9"], key=T._acc_key)
    assert got == ["MNXM9", "MNXM35", "MNXM8975", "MNXM1102421"]


# =====================================================================
# nametwin -- a real structure, and therefore real evidence
# =====================================================================

def test_one_name_over_two_compounds_is_not_evidence(built):
    """`UDP` the glycan against `UDP` the nucleotide: zero shared accessions, and no
    reaction that balances only after the substitution."""
    d = built["nametwin"]["MNXM1102130"]
    assert d["decision"] == "refuse:no_evidence"


def test_a_shared_accession_admits_the_duplicate(built):
    d = built["nametwin"]["MNXMTEST1"]
    assert d["decision"] == "accept"
    assert d["twin"] == "MNXMTEST2"
    assert "CHEBI:16236" in d["evidence"]


def test_an_element_neutral_twin_is_not_this_lanes_claim(built):
    """`blockers` gates on neutrality and `nametwin` gates on evidence; a neutral twin
    reaching the evidence gate would be admitted on the weaker of the two arguments."""
    # `AH2` is the pair that shares a NAME as well as a role, so it is the one that
    # reaches this lane at all; `Acceptor`/`A` are two different names and stop earlier.
    assert built["nametwin"]["MNXM1102421"]["decision"] \
        == "refuse:twin_has_no_tracked_atoms"
    assert built["nametwin"]["MNXM8975"]["decision"] == "refuse:no_same_name_twin"


# =====================================================================
# the guards, on their own
# =====================================================================

@pytest.mark.parametrize("name,expect", [
    ("hexadecenoate", (16, 0, 0, 0)),
    ("Acceptor", None),
    ("ACP", None),
    ("UDP", None),
])
def test_a_name_asserts_a_count_only_where_nomenclature_actually_states_one(name, expect):
    """A guard that fired on a guess would refuse more than it protects, so only the
    two routes that read a number straight out of the name are consulted."""
    assert T.name_budget(name) == expect


def test_the_recount_is_consulted_before_the_formula_and_the_residues_must_cancel():
    """The A4 lever, landing in the arbiter.

    `C70H131N3O9PS*2` has no countable formula, so the balance gate used to abstain on
    every reaction touching one -- and an abstention is a refusal. With the recount the
    count is exact for the EXPLICIT atoms, which is why the unspecified slots have to
    cancel across the equation before the number means anything about conservation.
    """
    subs, prods = ["A", "W"], ["B", "W"]
    formula_of = {"A": "C6H12O6*", "B": "C6H12O6*", "W": "H2O"}
    counts = {"A": (6, 0, 0, 0), "B": (6, 0, 0, 0)}

    # Formula alone: `count_element` refuses a `*`, so nothing can be said.
    assert C.concrete_balance(subs, prods, formula_of, set(), "C") is None

    # With the recount and cancelling residues, it balances.
    assert C.concrete_balance(subs, prods, formula_of, set(), "C",
                              counts_of=counts,
                              residue_of={"A": 1, "B": 1}) is True

    # Residues that do NOT cancel: the two exact counts are about different molecules.
    assert C.concrete_balance(subs, prods, formula_of, set(), "C",
                              counts_of=counts,
                              residue_of={"A": 1, "B": 2}) is None

    # An UNKNOWN residue count is a refusal, never a zero.
    assert C.concrete_balance(subs, prods, formula_of, set(), "C",
                              counts_of=counts,
                              residue_of={"A": None, "B": 1}) is None


def test_the_two_new_lanes_bracket_the_priority_order():
    """`nametwin` recovers MNXref's own record for the same compound on hard evidence,
    so it outranks every name-stem inference. `blockers` makes the same claim as
    `lane_acceptor` from a record rather than from six hand-written spellings, so it
    sits directly above it and the regex becomes the fallback."""
    p = C.LANE_PRIORITY
    assert p[0] == "nametwin"
    assert p.index("blockers") == p.index("acceptor") - 1
    assert set(p) >= {"twin", "carrier", "supplier", "override"}

"""The extractor: the identity bookkeeper every AAM member runs through.

ONE module for all members, on purpose. Every member's correspondence has to be
expressed in the same identity -- `(metabolite, CanonicalRankAtoms(breakTies=True))`
-- or two mappers stop naming the same physical atom the same node while every
table still looks well-formed. So the functions pinned here are the ones a second
implementation would silently diverge on.

Three of them are also exactly what T7 and T8 change, and the pins are written to
be moved against rather than argued with: `parse_equation` expands stoichiometry
(what collapse stops doing), the `stripped` guard compares template count to
PARTICIPANT count (what collapse has to teach a second acceptable count), and
`count_element` returns None for anything it cannot trust (what the partial
lane's element reduction has to propagate to a refusal, never to a zero).
"""
from __future__ import annotations

import pytest

pytest.importorskip("rdkit", reason="the extractor imports rdkit at module scope")

from ecspr.bake import atom_pairs as AP
from ecspr.bake.aam.partial import KEY_SEP as AP_KEY_SEP


# --- the equation, expanded ------------------------------------------------

def test_parse_equation_writes_a_coefficient_out_as_repeats():
    """The behaviour the atom cap has been measuring, stated plainly.

    A coefficient of 16 puts the metabolite in the list sixteen times, and the
    reaction SMILES is built from that list -- which is why nitrogenase's
    string counts over a thousand atoms while its distinct chemistry is small.
    Nothing here is wrong for the extractor's purposes: the participant list has
    to be per-occurrence, because the mapper's templates are.
    """
    subs, prods = AP.parse_equation("1 MNXM1@MNXD1 + 16 MNXM2@MNXD1 = 1 MNXM3@MNXD1")
    assert subs == ["MNXM1"] + ["MNXM2"] * 16
    assert prods == ["MNXM3"]


def test_parse_equation_refuses_a_one_sided_equation():
    """A side with nothing on it is an exchange or a sink, not chemistry."""
    assert AP.parse_equation("1 MNXM1@MNXD1 = ") is None
    assert AP.parse_equation("no equals sign here") is None


# --- the stripped guard ----------------------------------------------------

def test_strict_mode_refuses_when_a_participant_went_missing():
    """The guard that exists because a dropped molecule re-routes the mapping.

    When WE built the SMILES, one fragment per participant, a template count
    that does not match the participant count means a molecule was dropped
    before mapping and the mapper sent its atoms onto whatever remained. The
    counts survive that; the pairs do not.

    Collapse has to keep this refusing for a genuinely stripped reaction while
    accepting a deliberately collapsed one -- so the failure mode being guarded
    is written here, not just the return value.
    """
    pairs, status = AP.pairs_from_mapped(
        "[CH4:1]>>[CH4:1]", ["MNXM1", "MNXM2"], ["MNXM3"], {}, align="strict")
    assert (pairs, status) == ({}, "stripped")


def test_structural_mode_does_not_apply_the_participant_count_guard():
    """A foreign database wrote its SMILES over its OWN participant set.

    MetaCyc writes the water MetaNetX leaves implicit and omits the proton
    MetaNetX lists, so the counts are EXPECTED to differ and carry no
    information. Applying strict here refused 9,173 of 16,526 curated records
    -- the single largest loss in the lane, and not a chemistry verdict at all.
    """
    pairs, status = AP.pairs_from_mapped(
        "[CH4:1]>>[CH4:1]", ["MNXM1", "MNXM2"], ["MNXM3"], {}, align="structural")
    assert status != "stripped"


def test_an_absent_mapping_is_its_own_status():
    """`no_mapping` is a mapper outcome that must stay distinguishable.

    It is what RXNMapper's 512-token limit produces on the long reactions, which
    is the population T8's partial lane targets -- so it cannot be folded into
    `unparseable` or into an empty result.
    """
    assert AP.pairs_from_mapped(None, [], [], {}) == ({}, "no_mapping")
    assert AP.pairs_from_mapped("", [], [], {}) == ({}, "no_mapping")
    assert AP.pairs_from_mapped("not a reaction", [], [], {})[1] in ("unparseable", "no_mapping")


# --- the formula arithmetic ------------------------------------------------

@pytest.mark.parametrize("formula,element,expected", [
    ("C6H12O6", "C", 6),
    ("C6H12O6", "N", 0),          # parsed and genuinely absent -- a real zero
    ("H2O", "O", 1),              # a bare symbol means one
    ("C21H27N7O14P2", "N", 7),
    ("*", "C", None),             # a polymer: an unknown count, not a zero
    ("C*H2", "C", None),          # a wildcard anywhere poisons the whole formula
    ("Fe4S4(SR)4", "S", None),    # nested groups are not parsed, so not trusted
    ("", "C", None),
    (None, "C", None),
])
def test_count_element_distinguishes_absent_from_untrusted(formula, element, expected):
    """None and 0 are DIFFERENT answers, and conflating them fabricates chemistry.

    0 means "parsed, and this element is genuinely not here". None means "this
    formula cannot be trusted to say". The partial lane balances an element
    across a reduced participant set, so a None silently read as 0 would let it
    certify a balance it never checked.
    """
    assert AP.count_element(formula, element) == expected


# --- conservation-forced pairing -------------------------------------------

def test_forced_pairs_emits_the_unique_bijection_at_full_weight():
    """One S in, one S out: conservation leaves exactly one possibility.

    Nothing is guessed here -- there is only one pairing that exists. This is
    the exact shape of MNXR104650 (sulfite reductase), where three NADPH blow
    the mapper's context window while carrying no sulfur at all, and it is the
    first arm of T8's partial lane for that reason.
    """
    out = AP.forced_pairs(
        ["M_h2s", "M_nadp"] * 3, ["M_so3", "M_nadph"] * 3,
        formulas={"M_h2s": "H2S", "M_so3": "O3S", "M_nadp": "C21H26N7O17P3",
                  "M_nadph": "C21H27N7O17P3"},
        ranks_of={("M_h2s", "S"): [0], ("M_so3", "S"): [0]})
    assert out == {("S", "M_h2s", "M_so3"): [(0, 0, 1.0)]}


def test_forced_pairs_spreads_a_multi_atom_transfer_instead_of_choosing():
    """n > 1: the METABOLITE pairing is forced, the ATOM correspondence is not.

    Picking one (by rank order, say) would fabricate the atom identity the graph
    exists to respect; refusing would be a gap, and the model's contract is
    dilute-not-gap. So every candidate is emitted at 1/n -- the doubly-stochastic
    completion, where each row and column of the n x n candidate matrix sums to
    1.0, so the per-atom margin is exactly a confident pairing's.
    """
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
    """An untrusted formula must reach a refusal, never a zero.

    A `*` polymer read as "carries no carbon" would make some other participant
    look like the unique carbon source and force a pairing that conservation
    never licensed.
    """
    out = AP.forced_pairs(
        ["M_a", "M_poly"], ["M_b"],
        formulas={"M_a": "C2H6O", "M_poly": "*", "M_b": "C2H4O2"},
        ranks_of={("M_a", "C"): [0, 1], ("M_b", "C"): [0, 1]})
    assert out == {}, "a wildcard formula licensed a forced pairing"


# --- one extraction, three submission classes -------------------------------
# The change one universe forced. Each member's cache now holds whole reactions AND
# element reductions in one file, so the extractor has to give each row the template it
# was actually submitted under -- and stamp which class it answered, because that is not
# recoverable afterwards.

def _mixed(tmp_path):
    """A whole reaction and a nitrogen reduction OF THE SAME reaction, mapped together."""
    import pandas as pd
    from ecspr.bake.aam import partial as P
    from ecspr.bake.aam import universe as U

    # Two carbons in, two out; one nitrogen in, one out. Small enough to read by eye.
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

    # The two submissions as a mapper would return them. The reduction's map names only
    # its own two participants, which is exactly why it needs its own template.
    aam = tmp_path / "aam.tsv"
    aam.write_text(
        "mnxr\trxn_smiles\tmapped_rxn_smiles\tconfidence\n"
        "MNXR1\tCC.N>>CCO.CN\t[CH3:1][CH3:2].[NH3:3]>>[CH3:1][CH2:2][OH:4].[CH3:5][NH2:3]\t0.9\n"
        f"MNXR1{P.KEY_SEP}N\tN>>CN\t[NH3:1]>>[CH3:2][NH2:1]\t0.9\n")
    return reac, chem, up, aam


def test_one_extraction_gives_each_class_the_template_it_was_submitted_under(tmp_path):
    """The reduction's participants are DELIBERATELY fewer than the equation's.

    Re-deriving them from `reac_prop` would compare a two-fragment map against a
    four-participant template and the strict guard would refuse it as `stripped` -- so
    before one universe the reduced pass needed its own extraction. Here both are in one
    call and each gets its own template, keyed on whether the row carries an element.
    """
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
    """Not recoverable afterwards, which is why it is written here.

    Both submissions above are for MNXR1, and the layer stack must put the whole map in
    L2 and the reduction in L4. Keyed on (mnxr, element) alone the two are
    indistinguishable, and a partial map sitting where a full map belongs is the one
    thing the additive stack exists to prevent.
    """
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
    # The reduction is read for ONE element and every other element's map is discarded
    # unread -- for those the reduction really is a strip.
    assert set(d.loc[d["submission_class"] == "reduced", "element"]) == {"N"}


def test_a_table_extracted_without_a_universe_cannot_be_split_into_layers(tmp_path):
    """`layers.explode` refuses rather than fusing it as one layer.

    Fusing it whole is the failure this guards: it would put every class in whichever
    layer asked first, and the additive gates only refuse a CLAIM they can see.
    """
    import pandas as pd
    from ecspr.bake.aam import layers as L

    p = tmp_path / "nolabels.parquet"
    pd.DataFrame([dict(mnxr="MNXR1", element="C", substrate="MNXM1", product="MNXM3",
                       n_atoms=1, sub_idx="0", prod_idx="0", pair_w="1.0")]).to_parquet(
        p, index=False)
    assert len(L.explode(p, method="m", source="s")) == 1
    with pytest.raises(SystemExit, match="submission_class"):
        L.explode(p, method="m", source="s", only_class="whole")

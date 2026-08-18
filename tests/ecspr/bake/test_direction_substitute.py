"""The substitution lane: every gate, and the guarantee that switching it off changes nothing.

Two claims are under test and they are not the same claim.

The first is that the EMPTY configuration is a strict no-op. Every baseline r9 will be
measured against was taken under it, so "nothing was substituted" has to mean the identical
code path rather than a path that happens to agree -- `rewrite` returns the same object, and
`props` returns an equal dict.

The second is that each gate FIRES. A gate nobody has seen refuse is a comment. These are
thermodynamic substitutions, so the dangerous row is not the one that fails to balance --
it is the one that balances perfectly and carries the wrong potential, which is exactly
what `[Fe+3]`/`[Fe+2]` would do. The gates that catch that are the potential gate, the
congener gate and the anchor requirement, and they get a test each.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from ecspr.bake.direction import canon, substitute as S


# --- fixtures: the smallest admissible pair, and ways to break it ----------

NAD_OX = "MODEL:NAD_ox"
NAD_RED = "MODEL:NAD_red"

MODELS = pd.DataFrame([
    dict(model_key=NAD_OX, name="NAD+", smiles="C1=CC(=C[N+](=C1)[C@H]2[C@@H]([C@@H]"
         "([C@H](O2)COP(=O)([O-])OP(=O)(O)OC[C@@H]3[C@H]([C@H]([C@@H](O3)N4C=NC5="
         "C(N=CN=C54)N)O)O)O)O)C(=O)N", inchi="", inchikey="BAWFJGJZGIEFAR-NNYOXOHSSA-O",
         basis="MetaCyc NAD+/NADH, E0' = -0.320 V"),
    dict(model_key=NAD_RED, name="NADH", smiles="C1CC(=CN(C1)[C@H]2[C@@H]([C@@H]"
         "([C@H](O2)COP(=O)([O-])OP(=O)(O)OC[C@@H]3[C@H]([C@H]([C@@H](O3)N4C=NC5="
         "C(N=CN=C54)N)O)O)O)O)C(=O)N", inchi="", inchikey="BOPGDPNILDQYTO-NNYOXOHSSA-N",
         basis="MetaCyc NAD+/NADH, E0' = -0.320 V"),
])


def _row(**kw):
    base = dict(kind="carrier", mnxm="MNXM137", mnx_name="NAD(P)",
                terms=f"1*{NAD_OX}", congener_terms="", couple_id="nadp", state="ox",
                e0_V=-0.324, e0_model_V=-0.320, n_e=2, n_h=1, anchor_mnxr="MNXR100001",
                sibling_mnxr="", sibling_e0_V="",
                # Both arms SCORED AND AGREEING, because that is the only state in which a
                # row is admissible for both members and these fixtures are about the other
                # gates. A row with an empty gap is refused as `member_unscored`, which is
                # its own test rather than an accident in every other one.
                congeners_eq="", gap_eq=0.0, congeners_dgbyg="", gap_dgbyg=0.0,
                basis="Fig. 1 of somewhere")
    base.update(kw)
    return base


def _pair(**ox_kw):
    """The couple both gates need: an ox row and a red row."""
    red = dict(mnxm="MNXM138", mnx_name="NAD(P)H", terms=f"1*{NAD_RED}", state="red")
    return pd.DataFrame([_row(**ox_kw), _row(**{**red, **{}})])


NAMES = {"MNXM137": "NAD(P)", "MNXM138": "NAD(P)H", "MNXM99": "ethanol"}
PROPS = {"MNXM99": {"smiles": "CCO"}}          # readable today, so not replaceable


def _written(tmp_path, rows, models=MODELS):
    models.to_csv(tmp_path / "models.tsv", sep="\t", index=False)
    rows.to_csv(tmp_path / "substitutions.tsv", sep="\t", index=False)
    return tmp_path


def _load(tmp_path, models=MODELS, rows=None, props=PROPS, names=NAMES, formulas=None,
          member="eq"):
    rows = _pair() if rows is None else rows
    return S.load(_written(tmp_path, rows, models), props, names, formulas=formulas,
                  member=member)


# --- the empty configuration ----------------------------------------------

def test_no_tables_is_the_identity_not_a_copy():
    """The baselines were taken under this, so it must be the same code path."""
    s = S.Substitutions()
    stoich = {"MNXM1": -1.0, "MNXM2": 1.0}
    assert s.rewrite(stoich) is stoich
    assert s.covers(stoich) is False
    assert s.sigma_sub(stoich) == 0.0


def test_no_tables_widens_props_by_nothing():
    base = {"MNXM1": {"smiles": "CCO"}}
    assert S.Substitutions().props(base) == base


def test_a_loaded_table_still_leaves_an_uncovered_reaction_untouched(tmp_path):
    """The property that makes this safe to switch on without re-validating a member."""
    s = _load(tmp_path)
    stoich = {"MNXM99": -1.0, "MNXM100": 1.0}
    assert s.rewrite(stoich) is stoich


def test_a_model_compound_never_overwrites_a_metanetx_key(tmp_path):
    s = _load(tmp_path)
    base = {"MNXM137": {"smiles": "CCO"}}
    out = s.props(base)
    assert out["MNXM137"] == {"smiles": "CCO"}, "MetaNetX chemistry was overwritten"
    assert out[NAD_OX]["inchikey"].startswith("BAWFJGJZGIEFAR")


# --- the rewrite ----------------------------------------------------------

def test_a_carrier_row_renames_and_preserves_the_coefficient(tmp_path):
    s = _load(tmp_path)
    out = s.rewrite({"MNXM137": -2.0, "MNXM99": 1.0})
    assert out == {NAD_OX: -2.0, "MNXM99": 1.0}


# --- the polymer scope ----------------------------------------------------
#
# A polymer row is not compound-keyed the way a carrier row is. It asserts something about
# the REACTION -- that MetaNetX wrote a chain increment as a fixed molecule and left the
# acceptor out -- so the fixtures below are reactions, and each test is one of the three
# conditions deciding whether the row acts.

POLY_MODELS = pd.concat([MODELS, pd.DataFrame([
    dict(model_key="MODEL:g6", name="alpha-maltohexaose", smiles="OCC1OC(O)C(O)C(O)C1O",
         inchi="", inchikey="", formula="C36H62O31",
         basis="MNXR95153 identifies it with 1,4-alpha-D-glucan"),
    dict(model_key="MODEL:g5", name="alpha-maltopentaose", smiles="OCC1OC(O)C(O)C(O)C1O",
         inchi="", inchikey="", formula="C30H52O26", basis="MODEL:g6 less one glucosyl"),
    dict(model_key="MODEL:g4", name="alpha-maltotetraose", smiles="OCC1OC(O)C(O)C(O)C1O",
         inchi="", inchikey="", formula="C24H42O21", basis="glycogen's own composition"),
    dict(model_key="MODEL:g3", name="alpha-maltotriose", smiles="OCC1OC(O)C(O)C(O)C1O",
         inchi="", inchikey="", formula="C18H32O16", basis="MODEL:g4 less one glucosyl"),
])])

# `Glycogen`, `Branching glycogen` and `D-cellotetraose` are all C24H42O21 -- and the first
# and last share the InChI CONNECTIVITY layer too, since MetaNetX built the flattened
# polymer by deleting a real oligomer's stereochemistry. Nineteen stereo-complete compounds
# sit on that skeleton block. Composition is therefore not a key, and these fixtures say so.
POLY_FORMULAS = {
    "MNXM738130": "C24H42O21",      # Glycogen, flattened
    "MNXM733515": "C36H62O31",      # 1,4-alpha-D-glucan, flattened
    "MNXM8348": "C24H42O21",        # Branching glycogen, a THIRD alias, not in the table
    "MNXM1371942": "C24H42O21",     # D-cellotetraose -- glycogen's formula exactly
    "MNXM1364212": "C6H11O9P",      # alpha-D-glucose 1-phosphate
    "MNXM9": "HO4P",                # phosphate
}


def _poly_row(mnxm, name, terms, **kw):
    return _row(kind="polymer", mnxm=mnxm, mnx_name=name, terms=terms, couple_id="glucan",
                state="", e0_V="", e0_model_V="", n_e="", n_h="", **kw)


GLYCOGEN_ROW = _poly_row("MNXM738130", "Glycogen", "1*MODEL:g4;-1*MODEL:g3")
GLUCAN_ROW = _poly_row("MNXM733515", "1,4-alpha-D-glucan", "1*MODEL:g6;-1*MODEL:g5")
POLY_NAMES = {**NAMES, "MNXM738130": "Glycogen", "MNXM733515": "1,4-alpha-D-glucan",
              "MNXM1371942": "D-cellotetraose"}


def _poly(tmp_path, rows=(GLYCOGEN_ROW,), models=POLY_MODELS, **kw):
    return _load(tmp_path, models=models, rows=pd.DataFrame(list(rows)),
                 names=POLY_NAMES, formulas=POLY_FORMULAS, **kw)


def test_a_polymer_row_inserts_the_acceptor_on_the_opposite_side(tmp_path):
    """`G1P = Glycogen + Pi` has no acceptor to underspecify -- it is absent.

    A props-level override cannot express that, which is why the operation rewrites
    stoichiometry and extends props together rather than either alone.
    """
    s = _poly(tmp_path)
    out = s.rewrite({"MNXM1364212": -1.0, "MNXM738130": 1.0, "MNXM9": 1.0})
    assert out == {"MNXM1364212": -1.0, "MNXM9": 1.0, "MODEL:g4": 1.0, "MODEL:g3": -1.0}
    assert s.covers({"MNXM1364212": -1.0, "MNXM738130": 1.0, "MNXM9": 1.0})


def test_a_reaction_that_already_balances_is_left_exactly_as_metanetx_wrote_it(tmp_path):
    """Condition (ii), and acceptance criterion 15's control in miniature.

    `MNXR145021` (glgB) moves glycogen to branching glycogen and MetaNetX balances it
    already -- it changes a LINKAGE, not a chain length. Inserting an acceptor there would
    unbalance a balanced equation and hand a member a reaction nobody wrote.
    """
    s = _poly(tmp_path)
    stoich = {"MNXM738130": -1.0, "MNXM8348": 1.0}
    assert s.rewrite(stoich) is stoich
    assert s.covers(stoich) is False
    assert s.sigma_sub(stoich) == 0.0


def test_two_flattened_aliases_of_one_polymer_are_left_alone(tmp_path):
    """Condition (i), and the defect class it exists for.

    MetaNetX aliases one physical polymer under several flattened accessions and writes
    reactions BETWEEN them -- `MNXR157776` is `1,4-alpha-D-glucan -> Glycogen`, a hexamer
    becoming a tetramer. The residual is an artifact of the aliasing, not a chemical
    deficit, and this test pins BOTH halves: substituting both sides balances the equation
    perfectly, which is exactly why balance cannot be the admission rule here.
    """
    s = _poly(tmp_path, rows=(GLYCOGEN_ROW, GLUCAN_ROW))
    stoich = {"MNXM733515": -1.0, "MNXM738130": 1.0}
    assert s.rewrite(stoich) is stoich

    both = S._apply(stoich, {m: s._by_mnxm[m] for m in stoich})
    assert s._residual(both) == {}, "the artifact balances -- that is the whole danger"


def test_the_acceptor_is_declared_by_linkage_because_composition_cannot_declare_it(tmp_path):
    """Acceptance criterion 5, as a mechanism rather than as a fact about a data file.

    D-cellotetraose carries glycogen's formula exactly, and would balance perfectly against
    glycogen's acceptor -- so nothing arithmetic separates them. What separates them is that
    the table names a compound and declares its linkage. Swap the declaration and the wrong
    acceptor fires with no gate objecting, which is why the declaration is the safety.
    """
    cellulose = {"MNXM1371942": -1.0, "MNXM1364212": 1.0, "MNXM9": -1.0}

    s = _poly(tmp_path)
    assert s.rewrite(cellulose) is cellulose, "a compound the table does not name"

    swapped = _poly(tmp_path, rows=(_poly_row("MNXM1371942", "D-cellotetraose",
                                              "1*MODEL:g4;-1*MODEL:g3"),))
    assert swapped.rewrite(cellulose) != cellulose, (
        "balance cannot tell the linkages apart -- if this ever stops firing, the test has "
        "stopped making its point rather than the code having got safer")


def test_the_shipped_table_names_glycogen_and_not_its_identically_composed_twin():
    """The other half of criterion 5: the declaration actually shipped."""
    rows = S.read_table(Path(S.__file__).with_name("substitutions.tsv"))
    poly = set(rows.loc[rows["kind"] == "polymer", "mnxm"])
    assert "MNXM738130" in poly and "MNXM733515" in poly
    assert "MNXM1371942" not in poly, "D-cellotetraose is beta-1,4 and has no row"
    assert "MNXM8348" not in poly, "branching glycogen's acceptor is an open decision"


def test_a_polymer_row_without_formulas_is_refused_rather_than_silently_inert(tmp_path):
    """Its scope cannot be computed without them, so every row would act nowhere -- and a
    table that loads and does nothing is the worst of the three outcomes."""
    with pytest.raises(SystemExit, match="needs chem_prop formulas"):
        _load(tmp_path, models=POLY_MODELS, rows=pd.DataFrame([GLYCOGEN_ROW]),
              names=POLY_NAMES)


def test_a_polymer_row_that_inserts_nothing_is_refused(tmp_path):
    """A rename closes no imbalance, so a row whose terms sum to the polymer's own
    composition can never fire. That is an authoring error, not a no-op."""
    with pytest.raises(SystemExit, match="inserts nothing"):
        _poly(tmp_path, rows=(_poly_row("MNXM738130", "Glycogen", "1*MODEL:g4"),))


def test_a_model_formula_disagreeing_with_its_source_accession_is_refused(tmp_path):
    """The transcription tripwire: `source_mnxm` says where the structure was copied from,
    so a formula that disagrees with it means the row was edited after the copy."""
    models = POLY_MODELS.copy()
    models.loc[models["model_key"] == "MODEL:g3", "source_mnxm"] = "MNXM1364212"
    with pytest.raises(SystemExit, match="disagrees with"):
        _poly(tmp_path, models=models)


def test_a_polymer_participant_is_readable_so_the_carrier_gate_would_refuse_it(tmp_path):
    """Why `_gate_replaceable` is scoped to carrier/thioester and not applied here.

    MetaNetX builds a flattened polymer by deleting a real oligomer's stereochemistry, so
    the SMILES is perfectly readable and wildcard-free. The carrier-lane rule would refuse
    every polymer row by construction -- and it should, because a polymer row is not making
    that claim.
    """
    with pytest.raises(S.Refused, match="already carries a usable structure"):
        S._gate_replaceable("MNXM738130", {"MNXM738130": {"smiles": "OCC1OC(O)C(O)C(O)C1O"}},
                            "carrier/MNXM738130")
    s = _poly(tmp_path, props={**PROPS,
                               "MNXM738130": {"smiles": "OCC1OC(O)C(O)C(O)C1O"}})
    assert "MNXM738130" in s._by_mnxm


def test_a_formula_carrying_a_residue_is_not_a_composition(tmp_path):
    """MetaNetX writes the genuinely generic glucans with an `R` group and a formula like
    `C24H40O21*2`. Reading that as a composition would balance an equation against a
    fiction, so it is unknowable rather than parseable -- and unknowable refuses to act."""
    assert S.heavy_formula("C24H40O21*2") is None
    assert S.heavy_formula("") is None
    assert S.heavy_formula("C24H42O21") == {"C": 24, "O": 21}
    assert S.heavy_formula("HO4P(2-)") == {"O": 4, "P": 1}, "a trailing charge is not an atom"


# --- the gates, each shown refusing ---------------------------------------

def test_replaceable_only_refuses_a_participant_the_member_can_already_read(tmp_path):
    """Displacing a readable compound is an override of MetaNetX chemistry, not a
    substitution, and nothing here reviewed that."""
    rows = _pair()
    rows.loc[0, ["mnxm", "mnx_name"]] = ["MNXM99", "ethanol"]
    with pytest.raises(SystemExit, match="already carries a usable structure"):
        _load(tmp_path, rows=rows)


def test_a_model_compound_carrying_a_wildcard_is_refused(tmp_path):
    """Tested with the member's own `_has_wildcard`, so the two cannot drift."""
    models = MODELS.copy()
    models.loc[0, "smiles"] = "*C(=O)O"
    with pytest.raises(SystemExit, match="carries a wildcard"):
        _load(tmp_path, models=models)


def test_the_stale_id_tripwire_fires_when_the_name_moved(tmp_path):
    rows = _pair()
    rows.loc[0, "mnx_name"] = "Reduced flavin"
    with pytest.raises(SystemExit, match="row says"):
        _load(tmp_path, rows=rows)


def test_a_row_without_a_basis_is_refused(tmp_path):
    """`pd.isna` first: an empty cell arrives as NaN and `str(nan)` is truthy."""
    rows = _pair()
    rows.loc[0, "basis"] = None
    with pytest.raises(SystemExit, match="no basis"):
        _load(tmp_path, rows=rows)


def test_a_row_without_an_anchor_is_refused(tmp_path):
    """Balance is necessary and worthless as evidence here."""
    rows = _pair()
    rows.loc[0, "anchor_mnxr"] = None
    with pytest.raises(SystemExit, match="no anchor_mnxr"):
        _load(tmp_path, rows=rows)


def test_a_one_sided_couple_is_refused(tmp_path):
    """Substituting the oxidised partner alone leaves the member abstaining anyway,
    while the table reads as covering the reaction."""
    rows = _pair().iloc[:1]
    with pytest.raises(SystemExit, match="expected exactly"):
        _load(tmp_path, rows=rows)


def test_a_generic_with_no_tabulated_potential_is_refused(tmp_path):
    """This is what declines `Acceptor`, `A` and `AH2` -- by the same rule that admits
    NAD, with no name list to keep in step with anything."""
    rows = _pair()
    rows.loc[0, "e0_V"] = None
    with pytest.raises(SystemExit, match="declares no e0_V"):
        _load(tmp_path, rows=rows)


def test_a_model_whose_potential_is_a_decade_off_the_real_carrier_is_refused(tmp_path):
    """THE REAL CARRIER AGAINST ITS STAND-IN, not the two states of one couple.

    A couple has one E0' carried by both its rows, so comparing the ox row's declaration
    to the red row's compares a value to itself and can never refuse anything. This is the
    comparison that can.
    """
    rows = _pair()
    rows.loc[0, "e0_model_V"] = -0.324 + (canon.DIR_DECADE / (S.FARADAY * 2)) * 1.5
    with pytest.raises(SystemExit, match="past DIR_DECADE"):
        _load(tmp_path, rows=rows)


def test_a_couple_carrying_only_its_own_potential_twice_is_not_admitted_vacuously(tmp_path):
    """The regression for the gate that could not fire: both rows declaring the same
    number on both columns must still be checked against the model, not against each
    other."""
    rows = _pair()
    rows["e0_model_V"] = None
    with pytest.raises(SystemExit, match="declares no e0_model_V"):
        _load(tmp_path, rows=rows)


def test_a_couple_transferring_heavy_atoms_is_refused(tmp_path):
    """A redox couple whose two states differ in a heavy element is not a lookup.

    The acyl-carrier case in miniature: a thioester genuinely carries its chain through,
    so a fixed model pair cannot represent it and the gate has to say so rather than let
    the atoms balance through fabricated bonds.
    """
    models = MODELS.copy()
    models.loc[1, "smiles"] = "CCO"
    with pytest.raises(SystemExit, match="differ by heavy atoms"):
        _load(tmp_path, models=models)


def test_a_member_that_cannot_place_the_stand_in_is_refused_for_that_member_alone(tmp_path):
    """The defect r9 was blocked on, as a test.

    dGbyG places the FMN model pair 9.60 kJ/mol from the potentials the flavin rows cite --
    the same offset to five decimals across four anchors, so a systematic property of the
    model pair. eQuilibrator places it 0.45 away. One `sigma_sub` column was written from
    the eQuilibrator run and read as though it described both, so the arm that was 1.68
    decades out was the one whose error went undisclosed.

    The refusal is ONE-SIDED on purpose: an ensemble whose members abstain independently
    should lose the vote, not the reaction.
    """
    rows = _pair()
    rows["gap_dgbyg"] = canon.DIR_DECADE * 1.2
    assert len(_load(tmp_path, rows=rows, member="eq")) == 2
    assert len(_load(tmp_path, rows=rows, member="dgbyg")) == 0


def test_a_member_drift_refusal_does_not_abort_the_build(tmp_path):
    """A table defect is the curator's to fix and aborts. A member refusal is the mechanism
    WORKING, so it drops the row and carries on -- otherwise one member's disagreement
    could stop the other member's bake."""
    rows = _pair()
    rows["gap_dgbyg"] = canon.DIR_DECADE * 1.2
    s = _load(tmp_path, rows=rows, member="dgbyg")          # no raise
    assert set(s.decisions["predicate"]) == {"member_drift"}


def test_an_unscored_member_arm_is_refused_rather_than_admitted_blind(tmp_path):
    """Absence of evidence is not evidence of absence: a row whose arm nobody has scored
    would otherwise assert a structure this member has never been checked against."""
    rows = _pair()
    rows["gap_dgbyg"] = ""
    with pytest.raises(SystemExit, match="has never been scored"):
        _load(tmp_path, rows=rows, member="dgbyg")


def test_load_refuses_to_guess_a_member(tmp_path):
    """The admitted set is per member, so reading a table without naming one is the exact
    silent reuse these columns replace."""
    with pytest.raises(SystemExit, match="needs a member"):
        S.load(_written(tmp_path, _pair()), PROPS, NAMES)


def test_congener_spread_is_carried_as_sigma_sub_not_discarded(tmp_path):
    """An asserted structure has a width; reporting it as zero would make it look like
    a measurement."""
    rows = _pair()
    rows.loc[0, "congeners_eq"] = "-30.0;-32.0"
    s = _load(tmp_path, rows=rows)
    assert s.sigma_sub({"MNXM137": -1.0}) == pytest.approx(1.0)
    assert s.sigma_sub({"MNXM138": 1.0}) == 0.0


def test_the_width_is_read_from_the_arm_that_is_loaded(tmp_path):
    """The two members carry different widths for the same row, and each must get its own.
    Reading one arm's width under the other member's name IS the defect."""
    rows = _pair()
    rows.loc[0, "congeners_eq"] = "-30.0;-32.0"          # eq half-width 1.0
    rows.loc[0, "congeners_dgbyg"] = "-30.0;-34.0"       # dgbyg half-width 2.0
    assert _load(tmp_path, rows=rows, member="eq").sigma_sub(
        {"MNXM137": -1.0}) == pytest.approx(1.0)
    assert _load(tmp_path, rows=rows, member="dgbyg").sigma_sub(
        {"MNXM137": -1.0}) == pytest.approx(2.0)


def test_sigma_sub_accumulates_in_quadrature_over_substituted_participants(tmp_path):
    rows = _pair()
    rows.loc[0, "congeners_eq"] = "-30.0;-32.0"
    rows.loc[1, "congeners_eq"] = "-10.0;-12.0"
    s = _load(tmp_path, rows=rows)
    assert s.sigma_sub({"MNXM137": -1.0, "MNXM138": 1.0}) == pytest.approx(2 ** 0.5)


def test_a_model_id_outside_the_synthetic_namespace_is_refused(tmp_path):
    models = MODELS.copy()
    models.loc[0, "model_key"] = "MNXM137"
    with pytest.raises(SystemExit, match="must start with"):
        _load(tmp_path, models=models)


def test_a_term_naming_an_undeclared_model_is_refused(tmp_path):
    rows = _pair()
    rows.loc[0, "terms"] = "1*MODEL:not_declared"
    with pytest.raises(SystemExit, match="models.tsv does not declare"):
        _load(tmp_path, rows=rows)


def test_a_participant_may_repeat_when_it_asks_for_the_same_rewrite(tmp_path):
    """MetaNetX carries one reduced thioredoxin as the partner of three separately
    accessioned disulfides, so the couple rows have to share it. What must stay true is
    that the compound rewrites one way."""
    second = _pair()
    second["couple_id"] = "nadp_again"
    s = _load(tmp_path, rows=pd.concat([_pair(), second]))
    assert s.rewrite({"MNXM137": -1.0}) == {NAD_OX: -1.0}


def test_a_participant_repeating_with_different_terms_is_refused(tmp_path):
    """A conflict no later stage could detect: `_by_mnxm` would silently keep one."""
    second = _pair()
    second["couple_id"] = "nadp_again"
    second.loc[0, "terms"] = f"1*{NAD_RED}"
    with pytest.raises(SystemExit, match="substituted twice with different terms"):
        _load(tmp_path, rows=pd.concat([_pair(), second]))


def test_one_bad_couple_does_not_condemn_a_good_one(tmp_path):
    """The ledger has to name the couple that is wrong. Emptying the table would send a
    curator to whichever couple it happened to list first."""
    bad = _pair()
    bad["couple_id"] = "broken"
    bad = bad.iloc[[0]]                                   # ox with no red: incomplete
    d = S.load(_written(tmp_path, pd.concat([_pair(), bad])), PROPS, NAMES,
               collect=True, member="eq").decisions
    assert set(d.loc[d.couple_id == "nadp", "verdict"]) == {"admitted"}
    assert set(d.loc[d.couple_id == "broken", "verdict"]) == {"refused"}


# --- the comment rule -----------------------------------------------------

def test_a_hash_inside_a_smiles_survives_the_comment_stripper(tmp_path):
    """`#` is a comment only at line start. An inline `comment='#'` truncates any nitrile
    or alkyne -- silently dropping exactly the rows a curator most needs to supply."""
    p = tmp_path / "t.tsv"
    p.write_text("# a real comment\nmodel_key\tsmiles\nMODEL:x\tC#N\n")
    df = S.read_table(p)
    assert list(df.smiles) == ["C#N"]


# --- the lanes that must hand the tables to the member --------------------

REPO = Path(__file__).resolve().parents[3]
LANES = REPO / "src" / "fabfos" / "build_references" / "transforms" / "bake"
STAGED_TABLES = "{libdir}/ecspr/bake/direction"


@pytest.mark.parametrize("lane", ["equilibrator.py", "dgbyg.py"])
def test_the_member_lanes_hand_the_tables_to_the_member(lane):
    """A member lane that omits `--substitutions` runs r8 chemistry and says nothing.

    The flag is wired in the module and priced in the forecast, and neither of those
    reaches the cluster: what the job runs is the command string in this file. The path
    is the STAGED one, so the tables the member reads are inside the tree
    `evidence fingerprint --package direction` hashes -- a table and the DIRVER that
    describes it cannot come apart.
    """
    src = (LANES / lane).read_text()
    assert "drive eval" in src
    assert f"--substitutions {STAGED_TABLES}" in src


@pytest.mark.parametrize("lane", ["equilibrator.py", "dgbyg.py"])
def test_both_member_lanes_shard_and_reap_their_shards(lane):
    """A bare `wait` returns 0 whatever the children did.

    That is how a short member reached the fusion once already, so both lanes collect the
    child pids and check each one. The eQuilibrator lane ran `cpus=1` while dGbyG ran
    twenty-wide, which made a members run cost whatever eQuilibrator cost serially -- this
    asserts the fan-out is in BOTH lanes rather than remembered for one.
    """
    src = (LANES / lane).read_text()
    assert "--shard $i/" in src, "lane does not partition its universe"
    assert 'for p in $pids; do wait $p || rc=1; done' in src, (
        "lane does not check each shard's exit status")


@pytest.mark.parametrize("lane", ["equilibrator.py", "dgbyg.py"])
def test_the_merge_expects_the_count_the_lane_actually_ran(lane):
    """`--expect` and the loop bound must be ONE symbol.

    They fail at opposite ends of a run: a loop that ran fewer shards than the merge
    expects fails after the whole member has been computed, which is the expensive place
    to discover a typo. Reading both off `SHARDS` is what makes the count un-driftable,
    so what is under test is that neither is a literal.
    """
    src = (LANES / lane).read_text()
    assert "--expect {SHARDS}" in src, "merge hardcodes a shard count"
    assert "$(seq 0 {SHARDS - 1})" in src, "loop hardcodes a shard count"


def test_the_staged_tree_carries_the_tables_it_is_pointed_at():
    """The vendored copy is where the lanes look. Build product, so absent is a skip."""
    staged = (REPO / "src" / "fabfos" / "build_references" / "resources"
              / "buildlib" / "ecspr" / "bake" / "direction")
    if not staged.is_dir():
        pytest.skip("buildlib not vendored in this checkout")
    for name in ("models.tsv", "substitutions.tsv"):
        assert (staged / name).is_file(), f"vendored tree lacks {name}"

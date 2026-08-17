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
                terms=f"1*{NAD_OX}", couple_id="nadp", state="ox",
                e0_V=-0.324, e0_model_V=-0.320, n_e=2, n_h=1, anchor_mnxr="MNXR100001",
                congeners="", basis="Fig. 1 of somewhere")
    base.update(kw)
    return base


def _pair(**ox_kw):
    """The couple both gates need: an ox row and a red row."""
    red = dict(mnxm="MNXM138", mnx_name="NAD(P)H", terms=f"1*{NAD_RED}", state="red")
    return pd.DataFrame([_row(**ox_kw), _row(**{**red, **{}})])


NAMES = {"MNXM137": "NAD(P)", "MNXM138": "NAD(P)H", "MNXM99": "ethanol"}
PROPS = {"MNXM99": {"smiles": "CCO"}}          # readable today, so not replaceable


def _load(tmp_path, models=MODELS, rows=None, props=PROPS, names=NAMES):
    rows = _pair() if rows is None else rows
    models.to_csv(tmp_path / "models.tsv", sep="\t", index=False)
    rows.to_csv(tmp_path / "substitutions.tsv", sep="\t", index=False)
    return S.load(tmp_path, props, names)


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


def test_a_polymer_row_inserts_the_acceptor_on_the_opposite_side(tmp_path):
    """`G1P = Glycogen + Pi` has no acceptor to underspecify -- it is absent.

    A props-level override cannot express that, which is why the operation rewrites
    stoichiometry and extends props together rather than either alone.
    """
    models = pd.concat([MODELS, pd.DataFrame([
        dict(model_key="MODEL:g4", name="maltotetraose", smiles="OCC1OC(O)C(O)C(O)C1O",
             inchi="", inchikey="", basis="MNXM738130 is C24H42O21"),
        dict(model_key="MODEL:g3", name="maltotriose", smiles="OCC1OC(O)C(O)C(O)C1O",
             inchi="", inchikey="", basis="the residual is exactly C18O16")])])
    rows = pd.DataFrame([_row(kind="polymer", mnxm="MNXM738130", mnx_name="Glycogen",
                              terms="1*MODEL:g4;-1*MODEL:g3", couple_id="", state="",
                              e0_V="", n_e="", n_h="")])
    s = _load(tmp_path, models=models, rows=rows,
              names={**NAMES, "MNXM738130": "Glycogen"})
    out = s.rewrite({"MNXM1364212": -1.0, "MNXM738130": 1.0})
    assert out == {"MNXM1364212": -1.0, "MODEL:g4": 1.0, "MODEL:g3": -1.0}


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


def test_congeners_spanning_more_than_a_decade_are_refused(tmp_path):
    rows = _pair()
    rows.loc[0, "congeners"] = f"-30.0;{-30.0 - canon.DIR_DECADE * 1.2}"
    with pytest.raises(SystemExit, match="span"):
        _load(tmp_path, rows=rows)


def test_congener_spread_is_carried_as_sigma_sub_not_discarded(tmp_path):
    """An asserted structure has a width; reporting it as zero would make it look like
    a measurement."""
    rows = _pair()
    rows.loc[0, "congeners"] = "-30.0;-32.0"
    s = _load(tmp_path, rows=rows)
    assert s.sigma_sub({"MNXM137": -1.0}) == pytest.approx(1.0)
    assert s.sigma_sub({"MNXM138": 1.0}) == 0.0


def test_sigma_sub_accumulates_in_quadrature_over_substituted_participants(tmp_path):
    rows = _pair()
    rows.loc[0, "congeners"] = "-30.0;-32.0"
    rows.loc[1, "congeners"] = "-10.0;-12.0"
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


def test_the_same_participant_cannot_be_substituted_twice(tmp_path):
    rows = pd.concat([_pair(), _pair()])
    with pytest.raises(SystemExit, match="substituted twice"):
        _load(tmp_path, rows=rows)


# --- the comment rule -----------------------------------------------------

def test_a_hash_inside_a_smiles_survives_the_comment_stripper(tmp_path):
    """`#` is a comment only at line start. An inline `comment='#'` truncates any nitrile
    or alkyne -- silently dropping exactly the rows a curator most needs to supply."""
    p = tmp_path / "t.tsv"
    p.write_text("# a real comment\nmodel_key\tsmiles\nMODEL:x\tC#N\n")
    df = S.read_table(p)
    assert list(df.smiles) == ["C#N"]

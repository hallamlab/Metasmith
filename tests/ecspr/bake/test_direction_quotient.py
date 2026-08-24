# The reaction quotient: the correction, its width, and the join that feeds it.
#
# Three things are pinned here because each one has already been got wrong once:
#   * a participant with no measurement DEFAULTS, never skips -- a one-sided quotient
#     invents a pool skew out of an absence,
#   * `MNXM1364212` resolves through MetaNetX's own alias list, because KEGG's C00103
#     resolves by accession to the ALPHA anomer instead,
#   * a single-condition metabolite gets the spread FLOOR, not a confident zero.
from __future__ import annotations

import math

import pandas as pd
import pytest

from ecspr.bake.direction import quotient as Q
from ecspr.bake.direction.canon import (DIR_CONC_SPREAD_DEFAULT, DIR_CONC_SPREAD_FLOOR,
                                        DIR_DECADE, DIR_RT)

G1P = "MNXM1364212"          # glucose 1-phosphate, the id the reaction universe uses
G1P_ALPHA = "MNXM1364214"    # the alpha anomer, which is what kegg:C00103 resolves to
PI = "MNXM9"
GLYCOGEN = "MNXM738130"      # unit activity: a polymer has no free-solute concentration


# =====================================================================
# the correction
# =====================================================================

def test_a_participant_with_no_measurement_defaults_rather_than_dropping_out():
    # THE DEFECT THIS PINS. Skipping an unmeasured participant leaves it at the 1 M
    # standard state while its partners move to millimolar, so the quotient becomes
    # one-sided and reports a pool skew that no measurement supports.
    stoich = {G1P: -1.0, PI: 1.0}
    one_sided = Q.correction(stoich, {PI: (5.0, 0.5)})
    assert one_sided["n_conc_defaulted"] == 1, "G1P must be counted, not dropped"
    # Defaulted to 1 mM, so it contributes nothing to the skew and only width.
    assert one_sided["skew"] == pytest.approx(DIR_DECADE * math.log10(5.0), rel=1e-9)


def test_the_measured_phosphorylase_skew_is_the_measured_pool_ratio():
    # G1P = Glycogen + Pi, with the polymer at unit activity. The whole correction is
    # the measured Pi/G1P ratio, in decades.
    stoich = {G1P: -1.0, GLYCOGEN: 1.0, PI: 1.0}
    conc = {PI: (5.0, 0.5), G1P: (5.0 / 46.9, 0.4)}
    got = Q.correction(stoich, conc)
    assert got["n_conc_excluded"] == 1, "the polymer carries no free-solute concentration"
    assert got["n_conc_measured"] == 2
    assert got["skew"] / DIR_DECADE == pytest.approx(math.log10(46.9), rel=1e-6)
    # Delta n is zero over the solutes, so molecularity contributes nothing -- which is
    # why `physiological_dg_prime` alone would not move this reaction at all.
    assert got["molecularity"] == pytest.approx(0.0, abs=1e-12)


def test_water_and_the_proton_are_already_in_the_prime_potentials():
    bare = Q.correction({G1P: -1.0, PI: 1.0}, {})
    wet = Q.correction({G1P: -1.0, PI: 1.0, "WATER": -1.0, "MNXM1": 2.0}, {})
    assert wet == bare, "counting water or the proton again double-counts them"


def test_molecularity_is_the_solute_count_and_needs_no_data():
    got = Q.correction({G1P: -1.0, PI: 1.0, "MNXM3": 1.0}, {})
    assert got["delta_n"] == pytest.approx(1.0)
    assert got["molecularity"] == pytest.approx(DIR_RT * math.log(1e-3), rel=1e-9)


# =====================================================================
# the width
# =====================================================================

def test_a_single_condition_metabolite_is_floored_not_trusted_at_zero():
    # 41% of the measured metabolites rest on one growth condition, so a zero spread is
    # the common case rather than the exotic one.
    got = Q.correction({G1P: -1.0}, {G1P: (0.1, 0.0)})
    assert got["sigma_conc"] == pytest.approx(DIR_DECADE * DIR_CONC_SPREAD_FLOOR, rel=1e-9)


def test_a_measured_spread_wider_than_the_floor_is_used_as_measured():
    got = Q.correction({G1P: -1.0}, {G1P: (0.1, 1.2)})
    assert got["sigma_conc"] == pytest.approx(DIR_DECADE * 1.2, rel=1e-9)


def test_a_defaulted_participant_is_priced_at_the_log_uniform_width():
    got = Q.correction({G1P: -1.0}, {})
    assert got["sigma_conc"] == pytest.approx(DIR_DECADE * DIR_CONC_SPREAD_DEFAULT, rel=1e-9)
    # 1 uM .. 10 mM is four decades; the sigma of a log-uniform over it is 4/sqrt(12).
    assert DIR_CONC_SPREAD_DEFAULT == pytest.approx(4.0 / math.sqrt(12.0))


def test_the_width_adds_in_quadrature_and_scales_with_the_coefficient():
    got = Q.correction({G1P: -2.0}, {G1P: (0.1, 1.0)})
    assert got["sigma_conc"] == pytest.approx(DIR_DECADE * 2.0, rel=1e-9)


# =====================================================================
# the MetaNetX join
# =====================================================================

# Column 3 carries the '||'-separated alias list. The real trap in one fixture: KEGG's
# C00103 sits in column 2 against the ALPHA anomer, and reaches the id the reaction
# universe actually uses only through the alias list.
XREF = "\n".join([
    "#source\tID\tdescription",
    f"kegg.compound:C00103\t{G1P_ALPHA}\talpha-D-glucose 1-phosphate||kegg.compound:C00103",
    f"chebi:29042\t{G1P}\tD-glucose 1-phosphate||kegg.compound:C00103||chebi:29042",
    f"kegg.compound:C00009\t{PI}\tphosphate||kegg.compound:C00009",
    "kegg.compound:C99999\tMNXM77777\tsomething||kegg.compound:C99999 (secondary)",
]) + "\n"

PROP = "\n".join([
    "#ID\tname\treference\tformula\tcharge\tmass\tInChI\tInChIKey\tSMILES",
    f"{G1P}\tD-glucose 1-phosphate\t\tC6H11O9P\t-2\t260.0\t\t\t",
    f"{G1P_ALPHA}\talpha-D-glucose 1-phosphate\t\tC6H11O9P\t-2\t260.0\t\t\t",
    f"{PI}\tphosphate\t\tHO4P\t-2\t96.0\t\t\t",
]) + "\n"


def _joiner(tmp_path, xref=XREF, prop=PROP):
    (tmp_path / "chem_xref.tsv").write_text(xref)
    (tmp_path / "chem_prop.tsv").write_text(prop)
    return Q.joiner(tmp_path / "chem_xref.tsv", tmp_path / "chem_prop.tsv")


def test_the_alias_list_reaches_the_id_the_reaction_universe_uses(tmp_path):
    # Both anomers share a formula and a charge, so the guard passes and the join
    # resolves. Without the alias list this accession reaches only the alpha anomer,
    # and the single most load-bearing metabolite in the phosphorylase case is missed.
    mnxm_of, _ = _joiner(tmp_path)
    assert mnxm_of("kegg.compound", "C00103") == G1P


def test_an_accession_naming_one_compound_resolves_to_it(tmp_path):
    mnxm_of, _ = _joiner(tmp_path)
    assert mnxm_of("kegg.compound", "C00009") == PI


def test_an_unknown_accession_is_a_miss_not_a_guess(tmp_path):
    mnxm_of, _ = _joiner(tmp_path)
    assert mnxm_of("kegg.compound", "C00000") is None


def test_a_secondary_alias_is_not_a_synonym(tmp_path):
    # MetaNetX marks superseded entries in the same list. A secondary id points at
    # history, not at the current compound.
    mnxm_of, _ = _joiner(tmp_path)
    assert mnxm_of("kegg.compound", "C99999 (secondary)") is None


def test_candidates_that_disagree_on_formula_are_refused(tmp_path):
    # An alias list spanning two distinct compounds is not a synonym set, and choosing
    # between them is not the join's call.
    xref = XREF + f"kegg.compound:C11111\t{PI}\tx||kegg.compound:C11111\n" \
                  f"kegg.compound:C11111\t{G1P}\tx||kegg.compound:C11111\n"
    mnxm_of, _ = _joiner(tmp_path, xref=xref)
    assert mnxm_of("kegg.compound", "C11111") is None


# =====================================================================
# aggregation
# =====================================================================

def _rows(*specs):
    return pd.DataFrame([
        dict(source=s, source_accession="X", id_namespace="kegg.compound", id=i,
             name="n", organism="Escherichia coli", strain="K-12", media="M9",
             growth_phase="mid-log", value_mM=v, sd_mM=None, citation=c)
        for s, i, v, c in specs])


def test_conditions_aggregate_by_geometric_mean(tmp_path):
    from ecspr.bake.direction import sources as S
    rows = _rows(("ecmdb", "C00009", 1.0, "PMID:1"), ("ecmdb", "C00009", 100.0, "PMID:2"))
    table = S.aggregate(rows, lambda ns, acc: PI)
    assert len(table) == 1
    # The geometric mean of 1 and 100 is 10; the arithmetic mean, 50.5, is a number
    # neither condition exhibits.
    assert table.conc_mM.iloc[0] == pytest.approx(10.0)
    assert table.log10_spread.iloc[0] == pytest.approx(2.0)
    assert table.n_conditions.iloc[0] == 2


def test_every_metabolite_names_the_databases_behind_it(tmp_path):
    from ecspr.bake.direction import sources as S
    rows = _rows(("ecmdb", "C00009", 1.0, "PMID:1"), ("bionumbers", "C00009", 4.0, "BNID:2"))
    table = S.aggregate(rows, lambda ns, acc: PI)
    assert table.n_sources.iloc[0] == 2
    assert table.sources.iloc[0] == "bionumbers|ecmdb"
    assert set(table.citations.iloc[0].split("|")) == {"PMID:1", "BNID:2"}


def test_a_unit_that_does_not_parse_is_refused_rather_than_guessed():
    from ecspr.bake.direction import sources as S
    assert S.to_mM(2510.0, "uM") == pytest.approx(2.510)
    with pytest.raises(S.UnitRefused):
        S.to_mM(1.0, "g/L")


def test_an_adapter_may_not_pass_a_null_concentration_through():
    from ecspr.bake.direction import sources as S
    rows = _rows(("ecmdb", "C00009", 1.0, "PMID:1"))
    rows.loc[0, "value_mM"] = None
    with pytest.raises(ValueError, match="null or non-positive"):
        S.validate(rows, "ecmdb")


# =====================================================================
# the frame
# =====================================================================

def test_the_summary_reads_the_skew_column_not_the_dataframe_method():
    # `skew` is also a DataFrame method, so attribute access silently returns a bound
    # method and every comparison against it raises rather than reporting a number.
    frame = pd.DataFrame([dict(mnxr="MNXR1", member="eq", molecularity=0.0, skew=9.5,
                               dG_correction=9.5, sigma_conc=1.0, n_conc_measured=2,
                               n_conc_defaulted=0, n_conc_excluded=1, delta_n=0.0)])
    assert float(frame["skew"].abs().median()) == pytest.approx(9.5)
    assert callable(frame.skew), "pandas still shadows this column with its method"
    assert isinstance(frame["skew"], pd.Series)

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

O2 = "MNXM735438"

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
    assert got["n_conc_gas_phase"] == 0
    assert got["n_conc_measured"] == 2
    assert got["skew"] / DIR_DECADE == pytest.approx(math.log10(46.9), rel=1e-6)
    # Delta n is zero over the solutes, so molecularity contributes nothing -- which is
    # why `physiological_dg_prime` alone would not move this reaction at all.
    assert got["molecularity"] == pytest.approx(0.0, abs=1e-12)


def test_a_dissolved_gas_participates_rather_than_dropping_out():
    # EXCLUDING A GAS IS ARITHMETICALLY IDENTICAL TO PRICING IT AT 1 M. For dissolved O2
    # that overstates the driving force of every oxidation by 3.58 decades, which is worse
    # than the flat default it was meant to avoid. It is labelled, not skipped.
    measured = Q.correction({O2: -1.0, PI: 1.0}, {O2: (0.264, 0.3), PI: (5.0, 0.5)})
    assert measured["n_conc_gas_phase"] == 1
    assert measured["n_conc_excluded"] == 0, "a gas is not excluded"
    assert measured["n_conc_measured"] == 2
    expected = DIR_DECADE * (math.log10(5.0) - math.log10(0.264))
    assert measured["skew"] == pytest.approx(expected, rel=1e-9)
    # It moves delta_n too, which an excluded participant would not.
    assert measured["delta_n"] == pytest.approx(0.0)


def test_a_polymer_is_the_only_thing_excluded_outright():
    # A polymer genuinely has no free-solute concentration, so there is nothing to price.
    got = Q.correction({GLYCOGEN: -1.0, PI: 1.0}, {PI: (5.0, 0.5)})
    assert got["n_conc_excluded"] == 1
    assert got["n_conc_gas_phase"] == 0
    assert got["delta_n"] == pytest.approx(1.0), "only the phosphate counts"


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

# COLUMN 3 IS A LIST OF NAMES, not of accessions -- that distinction is the join. The real
# trap in one fixture: KEGG's C00103 sits in column 1 against the ALPHA anomer, and the id
# the reaction universe actually uses is reached only because both carry the NAME
# 'D-Glucose 1-phosphate' and agree on formula and charge.
XREF = "\n".join([
    "#source\tID\tdescription",
    f"kegg.compound:C00103\t{G1P_ALPHA}\talpha-D-glucose 1-phosphate||D-Glucose 1-phosphate",
    f"chebi:16077\t{G1P}\tD-glucopyranose 1-phosphate||Cori ester||D-Glucose 1-phosphate",
    f"kegg.compound:C00009\t{PI}\tphosphate",
    f"chebi:12967\t{G1P}\tsecondary/obsolete/fantasy identifier",
    f"kegg.compound:C00095\tMNXM_FRU_A\tD-fructose||D-Fructose",
    "kegg.compound:C99999\tMNXM_HUB1\tpentose||D-ribose",
    "chebi:99999\tMNXM_HUB2\tlyxose||D-ribose",
    "chebi:99998\tMNXM_HUB3\taldehydo-L-ribose||D-ribose",
    "chebi:99997\tMNXM_HUB4\tD-ribopyranose||D-ribose",
]) + "\n"

PROP = "\n".join([
    "#ID\tname\treference\tformula\tcharge\tmass\tInChI\tInChIKey\tSMILES",
    f"{G1P}\tD-glucopyranose 1-phosphate\t\tC6H11O9P\t-2\t260.0\t\t\t",
    f"{G1P_ALPHA}\talpha-D-glucose 1-phosphate\t\tC6H11O9P\t-2\t260.0\t\t\t",
    f"{PI}\tphosphate\t\tHO4P\t-2\t96.0\t\t\t",
    f"{O2}\tO2\t\tO2\t0\t32.0\t\t\t",
    "MNXM_FRU_A\tD-fructose\t\tC6H12O6\t0\t180.0\t\t\t",
    "MNXM_HUB1\tpentose\t\tC5H10O5\t0\t150.0\t\t\t",
    "MNXM_HUB2\tlyxose\t\tC5H10O5\t0\t150.0\t\t\t",
    "MNXM_HUB3\taldehydo-L-ribose\t\tC5H10O5\t0\t150.0\t\t\t",
    "MNXM_HUB4\tD-ribopyranose\t\tC5H10O5\t0\t150.0\t\t\t",
]) + "\n"


def _joiner(tmp_path, xref=XREF, prop=PROP, cap=3):
    (tmp_path / "chem_xref.tsv").write_text(xref)
    (tmp_path / "chem_prop.tsv").write_text(prop)
    return Q.joiner(tmp_path / "chem_xref.tsv", tmp_path / "chem_prop.tsv",
                    max_expansion=cap)


def test_the_shared_name_reaches_the_id_the_reaction_universe_uses(tmp_path):
    # Without this the measured [G1P] is absent and the correction is applied to [Pi]
    # alone, which is worse than applying none: a one-sided Q term invents a pool skew.
    mnxms_of, _ = _joiner(tmp_path)
    assert set(mnxms_of("kegg.compound", "C00103")) == {G1P, G1P_ALPHA}


def test_a_measurement_belongs_to_every_id_metanetx_gives_the_compound(tmp_path):
    # Both anomers are the same pool, so both carry the measurement.
    mnxms_of, _ = _joiner(tmp_path)
    assert sorted(mnxms_of("kegg.compound", "C00103")) == sorted([G1P, G1P_ALPHA])


def test_an_accession_naming_one_compound_resolves_to_it(tmp_path):
    mnxms_of, _ = _joiner(tmp_path)
    assert mnxms_of("kegg.compound", "C00009") == [PI]


def test_an_unknown_accession_is_a_miss_not_a_guess(tmp_path):
    mnxms_of, _ = _joiner(tmp_path)
    assert mnxms_of("kegg.compound", "C00000") == []


def test_a_metanetx_accession_is_its_own_answer(tmp_path):
    # BioNumbers rows carry no chemical identifier, so their mapping is written in MNXM
    # directly. `chem_xref` has no `metanetx.chemical:` self-references to join through.
    mnxms_of, _ = _joiner(tmp_path)
    assert mnxms_of("metanetx.chemical", O2) == [O2]
    assert mnxms_of("metanetx.chemical", "MNXM_NOT_REAL") == []


def test_an_obsolete_row_contributes_no_names(tmp_path):
    # MetaNetX marks superseded entries with a literal marker in the name column. Reading
    # it as a name would make every obsolete id a synonym of every other.
    _, _ = _joiner(tmp_path)
    _, by_name = Q.read_xref(tmp_path / "chem_xref.tsv")
    assert Q._norm("secondary/obsolete/fantasy identifier") not in by_name


def test_the_expansion_is_capped_so_a_generic_hub_cannot_link_a_sugar_family(tmp_path):
    # Unbounded, the shared name 'D-ribose' links a pentose hub, lyxose (a C2 epimer) and
    # aldehydo-L-ribose (the enantiomer). Formula and charge cannot separate them.
    mnxms_of, _ = _joiner(tmp_path, cap=3)
    assert mnxms_of("kegg.compound", "C99999") == ["MNXM_HUB1"], "wide expansion refused"
    wide, _ = _joiner(tmp_path, cap=99)
    assert len(wide("kegg.compound", "C99999")) == 4, "and it is a real link, not absent"


def test_names_differing_only_in_case_and_punctuation_are_one_name(tmp_path):
    mnxms_of, _ = _joiner(tmp_path)
    assert Q._norm("D-Glucose 1-phosphate") == Q._norm("d_glucose-1-phosphate")


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
    table = S.aggregate(rows, lambda ns, acc: [PI])
    assert len(table) == 1
    # The geometric mean of 1 and 100 is 10; the arithmetic mean, 50.5, is a number
    # neither condition exhibits.
    assert table.conc_mM.iloc[0] == pytest.approx(10.0)
    assert table.log10_spread.iloc[0] == pytest.approx(2.0)
    assert table.n_conditions.iloc[0] == 2


def test_every_metabolite_names_the_databases_behind_it(tmp_path):
    from ecspr.bake.direction import sources as S
    rows = _rows(("ecmdb", "C00009", 1.0, "PMID:1"), ("bionumbers", "C00009", 4.0, "BNID:2"))
    table = S.aggregate(rows, lambda ns, acc: [PI])
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
                               n_conc_defaulted=0, n_conc_excluded=1,
                               n_conc_gas_phase=0, delta_n=0.0)])
    assert float(frame["skew"].abs().median()) == pytest.approx(9.5)
    assert callable(frame.skew), "pandas still shadows this column with its method"
    assert isinstance(frame["skew"], pd.Series)

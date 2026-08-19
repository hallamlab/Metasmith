# The direction lane's crosswalk loaders, at the one place they decide coverage.
#
# `load_mnxm_props` is the gate every thermo vote passes through: a compound absent
# from its output makes both members abstain on every reaction that carries it,
# before either does any chemistry. So what the loader ADMITS is a coverage
# decision, and until now nothing asserted it.
#
# MetaNetX 4.5 files water only as the pseudo-accession `WATER` -- not `MNXM2`, not
# anything beginning `MNXM` -- and `BIOMASS` is the only other non-MNXM row in the
# file. The two must be told apart by what they CARRY, never by their names: water
# has an InChI, an InChIKey and a SMILES and is ordinary chemistry; biomass has none
# of the three and is a bookkeeping placeholder. That is one assertion, in two
# directions, and it is the whole of this file.
#
# Driven by an inline two-line table rather than a fixture: the claim is about
# parsing rules, and a 809 MB reference file on disk would make the test a
# statement about DVC mount state instead.
from __future__ import annotations

from ecspr.bake.direction import refdata


HEADER = "#ID\tname\treference\tformula\tcharge\tmass\tInChI\tInChIKey\tSMILES\n"

# Verbatim from references/metanetx/4.5/chem_prop.tsv, trimmed to the nine columns.
WATER = ("WATER\tH2O\tmnx:WATER\tH2O\t0\t18.01500\t"
         "InChI=1S/H2O/h1H2\tXLYOFNOQVPJJNP-UHFFFAOYSA-N\t[H]O[H]\n")
BIOMASS = "BIOMASS\tBIOMASS\tmnx:BIOMASS\t\t\t\t\t\t\n"
GLUCOSE = ("MNXM1137670\talpha-D-glucose\tchebi:17925\tC6H12O6\t0\t180.06339\t"
           "InChI=1S/C6H12O6/c7-1-2-3(8)4(9)5(10)6(11)12-2/h2-11H,1H2\t"
           "WQZGKKKJIJFFOK-DVKNGEFBSA-N\tOC[C@H]1O[C@H](O)[C@H](O)[C@@H](O)[C@@H]1O\n")


def _props(tmp_path, *rows):
    p = tmp_path / "chem_prop.tsv"
    p.write_text(HEADER + "".join(rows))
    return refdata.load_mnxm_props(p)


def test_water_is_admitted_under_its_pseudo_accession(tmp_path):
    # The bug this test exists for, stated as the property it broke.
    #
    # An `MNXM`-prefix filter here abstained BOTH thermo members on every
    # water-bearing reaction -- 30,546 of the 83,795-reaction universe, 36% of it --
    # and did so before any chemistry, so the abstention was recorded as `no_props`
    # and read downstream as a fact about the reaction rather than about the loader.
    # `_split_terms` in the same module already carried the warning; the loader did
    # not honour it.
    props = _props(tmp_path, WATER, GLUCOSE)
    assert "WATER" in props, (
        "water is not an MNXM accession in MetaNetX 4.5, and dropping it here "
        "silences both thermo members on a third of the universe")
    assert props["WATER"]["inchikey"] == "XLYOFNOQVPJJNP-UHFFFAOYSA-N"
    assert props["WATER"]["smiles"] == "[H]O[H]"
    assert props["WATER"]["inchi"].startswith("InChI=1S/H2O")


def test_biomass_is_excluded_by_carrying_nothing_not_by_its_name(tmp_path):
    props = _props(tmp_path, WATER, BIOMASS, GLUCOSE)
    assert "BIOMASS" not in props
    assert set(props) == {"WATER", "MNXM1137670"}


def test_an_empty_field_is_dropped_rather_than_stored_as_an_empty_string(tmp_path):
    partial = ("MNXM01\tno-smiles\tmnx:x\tC\t0\t1.0\t"
               "InChI=1S/CH4/h1H4\tVNWKTOKETHGBQD-UHFFFAOYSA-N\t\n")
    props = _props(tmp_path, partial)
    assert props["MNXM01"] == {"inchi": "InChI=1S/CH4/h1H4",
                               "inchikey": "VNWKTOKETHGBQD-UHFFFAOYSA-N"}


def test_the_equation_parser_and_the_props_loader_share_one_namespace(tmp_path):
    assert refdata._split_terms("1 MNXM1@MNXD1 + 1 WATER@MNXD1") == {"MNXM1", "WATER"}

    reac_prop = tmp_path / "reac_prop.tsv"
    reac_prop.write_text(
        "#ID\tmnx_equation\treference\tclassifs\tis_balanced\tis_transport\n"
        "MNXR01\t1 MNXM1137670@MNXD1 + 1 WATER@MNXD1 = 2 MNXM01@MNXD1\t\t\tB\t\n")
    stoich, is_bal, is_tr = refdata.load_mnxr_stoich(reac_prop)["MNXR01"]
    props = _props(tmp_path, WATER, GLUCOSE)
    assert set(stoich) - set(props) == {"MNXM01"}, (
        "every structured participant the equation names must resolve in props")

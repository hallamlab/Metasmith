from __future__ import annotations

import math

import pytest

from ecspr.bake.direction import canon, combine as C


def test_a_measurement_is_used_at_its_own_sigma_not_averaged():
    mu, s, measured = C.thermo_vote(-30.0, 1.0, False, -12.0, 4.0)
    assert measured is True
    assert mu == -30.0, "a measurement was averaged with its own readback"
    assert s == max(1.0, C.S_MEAS_FLOOR)


def test_two_predictions_are_floored_by_their_shared_error_and_their_spread():
    mu, s, measured = C.thermo_vote(-10.0, 1.0, True, 10.0, 1.0)
    assert measured is False
    assert mu == 0.0
    spread = 10.0
    assert s == pytest.approx(math.sqrt(spread ** 2 + canon.DIR_TAU_SHARED ** 2))
    assert s > max(1.0, canon.DIR_TAU_SHARED), (
        "two correlated predictors voted more confidently than either alone")


def test_a_lone_prediction_still_pays_the_shared_error():
    mu, s, measured = C.thermo_vote(-5.0, 2.0, True, None, None)
    assert (mu, measured) == (-5.0, False)
    assert s == pytest.approx(math.sqrt(4.0 + canon.DIR_TAU_SHARED ** 2))


def test_no_member_speaks_is_none_not_zero():
    assert C.thermo_vote(None, None, None, None, None) is None


def test_no_evidence_defaults_to_reversible_as_a_limit_not_as_a_gap():
    row = C.combine_row({"mnxr": "R"}, calib={}, sigma_0=canon.DIR_SIGMA_0)
    assert row["ratio"] == 1.0
    assert row["dir_tier"] == 0
    assert row["dir_method"] == "no_evidence"
    assert row["dir_confidence"] == 0.0

    refused = C.combine_row({"mnxr": "R", "dgbyg_wildcard": True},
                            calib={}, sigma_0=canon.DIR_SIGMA_0)
    assert refused["dir_method"] == "refused"
    assert refused["ratio"] == 1.0


def test_the_ratio_is_clamped_so_it_stays_a_two_way_conductance_ratio():
    row = C.combine_row({"mnxr": "R", "eq_dg": -1e6, "eq_sigma": 0.1,
                         "eq_uses_gc": False}, calib={}, sigma_0=canon.DIR_SIGMA_0)
    assert row["clamped"] is True
    assert row["dG_prime"] == pytest.approx(-C.DG_CLAMP)
    assert 0.0 < row["ratio"] < 1.0
    assert math.isfinite(row["ratio"])


def test_shrinkage_pulls_an_uncertain_vote_toward_reversible():
    sharp = C.combine_row({"mnxr": "R", "eq_dg": -20.0, "eq_sigma": 0.5,
                           "eq_uses_gc": False}, calib={}, sigma_0=canon.DIR_SIGMA_0)
    vague = C.combine_row({"mnxr": "R", "eq_dg": -20.0, "eq_sigma": 200.0,
                           "eq_uses_gc": False}, calib={}, sigma_0=canon.DIR_SIGMA_0)
    assert sharp["dir_confidence"] > vague["dir_confidence"]
    assert abs(math.log(sharp["ratio"])) > abs(math.log(vague["ratio"]))
    assert vague["ratio"] == pytest.approx(1.0, abs=0.2), (
        "an almost uninformative vote should land near reversible")


def test_the_provenance_ladder_records_who_spoke_rather_than_selecting():
    # (centre, width, n, the scale the centre is stated on). `physiological` is r10's:
    # the bin was fitted on the same corrected number the members carry.
    calib = {"LEFT-TO-RIGHT": (-15.0, 5.0, 40, "physiological")}
    measured = C.combine_row({"mnxr": "R", "eq_dg": -20.0, "eq_sigma": 1.0,
                              "eq_uses_gc": False, "biocyc_category": "LEFT-TO-RIGHT"},
                             calib=calib, sigma_0=canon.DIR_SIGMA_0)
    assert measured["dir_tier"] == 1 and measured["dir_method"] == "eq_rc+biocyc"

    predicted = C.combine_row({"mnxr": "R", "eq_dg": -20.0, "eq_sigma": 1.0,
                               "eq_uses_gc": True, "dgbyg_dg": -18.0,
                               "dgbyg_sigma": 3.0}, calib={}, sigma_0=canon.DIR_SIGMA_0)
    assert predicted["dir_tier"] == 2 and predicted["dir_method"] == "eq_gc_x_dgbyg"

    curated_only = C.combine_row({"mnxr": "R", "biocyc_category": "LEFT-TO-RIGHT"},
                                 calib=calib, sigma_0=canon.DIR_SIGMA_0)
    assert curated_only["dir_tier"] == 3 and curated_only["dir_method"] == "biocyc_only"


def test_a_silent_member_arrives_as_nan_and_must_not_read_as_a_vote():
    # `build` left-merges, so an absent member is NaN -- and NaN is not None.
    #
    # The ladder used to test `is not None`, which every NaN passes: 13,479 rows of
    # the deployed bake carry a `dir_method` naming a member that never spoke, of
    # which 12,405 are dGbyG-only rows labelled as an eQ/dGbyG agreement. The RATIO
    # was never affected (the vote itself goes through `_num`), so this is
    # provenance alone -- which is exactly why it could sit there unnoticed, and
    # exactly why a before/after member accounting cannot be read until it is fixed.
    nan = float("nan")

    db_only = C.combine_row({"mnxr": "R", "eq_dg": nan, "eq_sigma": nan,
                             "eq_uses_gc": nan, "dgbyg_dg": -18.0,
                             "dgbyg_sigma": 3.0},
                            calib={}, sigma_0=canon.DIR_SIGMA_0)
    assert db_only["dir_tier"] == 2
    assert db_only["dir_method"] == "dgbyg", (
        "a NaN eQ column read as an eQuilibrator vote")

    eq_only = C.combine_row({"mnxr": "R", "eq_dg": -20.0, "eq_sigma": 1.0,
                             "eq_uses_gc": True, "dgbyg_dg": nan,
                             "dgbyg_sigma": nan},
                            calib={}, sigma_0=canon.DIR_SIGMA_0)
    assert eq_only["dir_tier"] == 2 and eq_only["dir_method"] == "eq_gc"

    measured_alone = C.combine_row({"mnxr": "R", "eq_dg": -20.0, "eq_sigma": 1.0,
                                    "eq_uses_gc": False, "dgbyg_dg": nan,
                                    "dgbyg_sigma": nan},
                                   calib={}, sigma_0=canon.DIR_SIGMA_0)
    assert measured_alone["dir_tier"] == 1 and measured_alone["dir_method"] == "eq_rc"


def test_a_group_cancellation_is_not_a_measurement():
    # eQuilibrator returns dG'=0 at the sigma floor when the groups cancel exactly.
    #
    # That is a statement about the equation -- both sides built from the same pieces --
    # not a measurement of it, and `calibrate` already drops exactly these rows from the
    # arm it fits sigma_0 on. The combiner used to promote them to tier 1 at
    # S_MEAS_FLOOR: 4,841 of r8's 7,012 tier-1 rows, so the tier a consumer reads as
    # MEASURED was 69% no-information.
    #
    # The normalisation has to happen above BOTH the vote and the ladder. Patching only
    # `thermo_vote` leaves `dir_method` reading the raw column, which trades one
    # provenance defect for another.
    floor = canon.DIR_SIGMA_FLOOR
    row = {"mnxr": "R", "eq_dg": 0.0, "eq_sigma": floor, "eq_uses_gc": False,
           "dgbyg_dg": -18.0, "dgbyg_sigma": 3.0}

    assert C.eq_vote(0.0, floor, False) == (None, None, None)
    got = C.combine_row(dict(row), calib={}, sigma_0=canon.DIR_SIGMA_0)
    assert got["dir_tier"] == 2, "a group cancellation was promoted to a measurement"
    assert got["dir_method"] == "dgbyg", (
        "the ladder named eQuilibrator after eq_vote refused to let it vote")

    assert got["eq_dg"] == 0.0 and got["eq_sigma"] == floor

    real = C.combine_row(dict(row, eq_sigma=2.0), calib={}, sigma_0=canon.DIR_SIGMA_0)
    assert real["dir_tier"] == 1 and real["dir_method"] == "eq_rc+dgbyg"

    gc = C.combine_row(dict(row, eq_uses_gc=True), calib={}, sigma_0=canon.DIR_SIGMA_0)
    assert gc["dir_method"] == "eq_gc_x_dgbyg"


def test_an_absent_dgbyg_table_does_not_relabel_silence_as_refusal():
    # `refused` is dGbyG declining on a wildcard. NaN is dGbyG not being there.
    #
    # dGbyG cannot coexist with the eQ stack, so `drive eval` writes an EMPTY member
    # table when the env lacks it -- every `dgbyg_wildcard` then arrives as a float
    # NaN, which is truthy. Under the old test that turned all 47,266 no-evidence
    # reactions into deliberate abstentions by a member that never ran.
    absent = C.combine_row({"mnxr": "R", "dgbyg_wildcard": float("nan")},
                           calib={}, sigma_0=canon.DIR_SIGMA_0)
    assert absent["dir_method"] == "no_evidence"

    declined = C.combine_row({"mnxr": "R", "dgbyg_wildcard": True},
                             calib={}, sigma_0=canon.DIR_SIGMA_0)
    assert declined["dir_method"] == "refused"


# --- the substitution width -----------------------------------------------
#
# r9 lets a curated model compound stand in for a participant MetaNetX underspecifies.
# That is an ASSERTION with a width, and where the width is folded in decides whether the
# lane repeats r8's defect. `eq_vote` recognises an eQuilibrator group cancellation by
# testing sigma against the floor, so widening the member's own sigma would lift a
# cancelling zero over that floor and hand it back the tier r8 was baked to take away.

def test_a_group_cancellation_stays_refused_however_wide_the_substitution():
    # The re-promotion r8 removed, attempted through the new column.
    r = {"mnxr": "R", "eq_dg": 0.0, "eq_sigma": C.SIGMA_FLOOR, "eq_uses_gc": False,
         "eq_sigma_sub": 5.0}
    row = C.combine_row(r, calib={}, sigma_0=canon.DIR_SIGMA_0)
    assert row["dir_tier"] == 0, "a group cancellation was voted through sigma_sub"
    assert row["ratio"] == 1.0


def test_a_substituted_measurement_is_not_reported_as_measured():
    # Tier 1 is the tier a consumer reads as MEASURED, and eQuilibrator measured the
    # MODEL equation. The anchor gate justifies the number, not the provenance.
    base = {"mnxr": "R", "eq_dg": -20.0, "eq_sigma": 1.0, "eq_uses_gc": False}
    plain = C.combine_row(base, calib={}, sigma_0=canon.DIR_SIGMA_0)
    subbed = C.combine_row({**base, "eq_sigma_sub": 2.0}, calib={},
                           sigma_0=canon.DIR_SIGMA_0)
    assert plain["dir_tier"] == 1 and plain["dir_method"] == "eq_rc"
    assert subbed["dir_tier"] == 2, "a substituted row was reported as measured"
    assert subbed["dir_method"] == "eq_rc_sub"


def test_the_substitution_width_widens_the_posterior_and_shrinks_the_ratio():
    # BELOW THE CLAMP ON PURPOSE. r10 re-fitted DIR_SIGMA_0 to 28.017 while DIR_DG_CLAMP
    # stayed at three decades (17.12), so a confident row at -20 kJ/mol now clamps whether
    # or not the width is applied -- and two clamped rows are equal, which would hide the
    # very effect this pins. The shrinkage and the clamp do different jobs, and this test
    # is about the shrinkage.
    base = {"mnxr": "R", "eq_dg": -12.0, "eq_sigma": 1.0, "eq_uses_gc": True}
    plain = C.combine_row(base, calib={}, sigma_0=canon.DIR_SIGMA_0)
    subbed = C.combine_row({**base, "dgbyg_sigma_sub": 8.0}, calib={},
                           sigma_0=canon.DIR_SIGMA_0)
    assert subbed["sigma"] > plain["sigma"]
    assert abs(subbed["dG_prime"]) < abs(plain["dG_prime"]), "shrinkage ignored the width"
    assert subbed["sigma_sub"] == 8.0


def test_an_absent_sigma_sub_column_reads_as_no_assertion():
    # r8's member tables predate the column; it must arrive as zero width, not NaN.
    r = {"mnxr": "R", "eq_dg": -20.0, "eq_sigma": 1.0, "eq_uses_gc": False}
    row = C.combine_row(r, calib={}, sigma_0=canon.DIR_SIGMA_0)
    assert row["sigma_sub"] == 0.0 and row["dir_tier"] == 1

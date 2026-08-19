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
    calib = {"LEFT-TO-RIGHT": (-15.0, 5.0, 40)}
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

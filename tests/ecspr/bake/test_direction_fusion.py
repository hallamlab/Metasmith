"""The direction combiner: two correlated thermo members and a curated prior.

The arithmetic is small and every branch of it encodes a claim about
independence. eQuilibrator's reactant-contribution arm is a MEASUREMENT and
dGbyG's number for the same reaction is a lossy readback of that same TECRDB
value, so averaging them adds noise rather than information. When both are
PREDICTIONS they genuinely compete, but they are both TECRDB-fitted, so their
spread cannot see the common-mode error and the vote is floored by TAU_SHARED --
two correlated predictors must not vote as two independent ones.

The no-evidence default is the one with the largest blast radius: ratio 1.0 is a
real physical statement (reversible), not a missing value, and a consumer that
filtered `dir_tier > 0` would silently drop every reversible reaction -- turning
"default reversible" into "default absent".
"""
from __future__ import annotations

import math

import pytest

from ecspr.bake.direction import canon, combine as C


# --- the vote -------------------------------------------------------------

def test_a_measurement_is_used_at_its_own_sigma_not_averaged():
    """eq_uses_gc is False -> reactant contribution -> a measurement.

    dGbyG's number for the same reaction is a readback of the same TECRDB
    value, so averaging it in only adds noise.
    """
    mu, s, measured = C.thermo_vote(-30.0, 1.0, False, -12.0, 4.0)
    assert measured is True
    assert mu == -30.0, "a measurement was averaged with its own readback"
    assert s == max(1.0, C.S_MEAS_FLOOR)


def test_two_predictions_are_floored_by_their_shared_error_and_their_spread():
    """Neither predictor's own sigma can see the common-mode TECRDB error.

    So the fused sigma is floored twice: by TAU_SHARED, and by the members'
    own disagreement, which is a lower bound on the error either one is making.
    """
    mu, s, measured = C.thermo_vote(-10.0, 1.0, True, 10.0, 1.0)
    assert measured is False
    assert mu == 0.0
    spread = 10.0                                  # |(-10 - 10)| / 2
    assert s == pytest.approx(math.sqrt(spread ** 2 + canon.DIR_TAU_SHARED ** 2))
    assert s > max(1.0, canon.DIR_TAU_SHARED), (
        "two correlated predictors voted more confidently than either alone")


def test_a_lone_prediction_still_pays_the_shared_error():
    mu, s, measured = C.thermo_vote(-5.0, 2.0, True, None, None)
    assert (mu, measured) == (-5.0, False)
    assert s == pytest.approx(math.sqrt(4.0 + canon.DIR_TAU_SHARED ** 2))


def test_no_member_speaks_is_none_not_zero():
    """`None` is "no vote"; 0.0 would be a vote for reversible at full weight."""
    assert C.thermo_vote(None, None, None, None, None) is None


# --- the row --------------------------------------------------------------

def test_no_evidence_defaults_to_reversible_as_a_limit_not_as_a_gap():
    """ratio 1.0, tier 0, method `no_evidence` -- and every one of those matters.

    The ratio is a real physical statement, so the row must exist and be read.
    The tier says "carries no directional information", NOT "unusable". The
    method distinguishes this from `refused`, which is what a dGbyG wildcard
    gets: one is silence and the other is an abstention, and a build that cannot
    tell them apart cannot say why coverage is what it is.
    """
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
    """A hard gate would be a zero conductance; the clamp keeps it finite.

    An edge whose backward conductance is zero is a one-way street, and the
    graph's whole stance is that direction is a ratio rather than a filter.
    """
    row = C.combine_row({"mnxr": "R", "eq_dg": -1e6, "eq_sigma": 0.1,
                         "eq_uses_gc": False}, calib={}, sigma_0=canon.DIR_SIGMA_0)
    assert row["clamped"] is True
    assert row["dG_prime"] == pytest.approx(-C.DG_CLAMP)
    assert 0.0 < row["ratio"] < 1.0
    assert math.isfinite(row["ratio"])


def test_shrinkage_pulls_an_uncertain_vote_toward_reversible():
    """lambda = sigma_0^2 / (sigma_0^2 + s^2): a vague vote barely moves the ratio.

    Two rows with the SAME mean and different confidence must not produce the
    same ratio, which is what a shrinkage that did nothing would give.
    """
    sharp = C.combine_row({"mnxr": "R", "eq_dg": -20.0, "eq_sigma": 0.5,
                           "eq_uses_gc": False}, calib={}, sigma_0=canon.DIR_SIGMA_0)
    vague = C.combine_row({"mnxr": "R", "eq_dg": -20.0, "eq_sigma": 200.0,
                           "eq_uses_gc": False}, calib={}, sigma_0=canon.DIR_SIGMA_0)
    assert sharp["dir_confidence"] > vague["dir_confidence"]
    assert abs(math.log(sharp["ratio"])) > abs(math.log(vague["ratio"]))
    assert vague["ratio"] == pytest.approx(1.0, abs=0.2), (
        "an almost uninformative vote should land near reversible")


def test_the_provenance_ladder_records_who_spoke_rather_than_selecting():
    """The method string is a record, not a tier that gates usability.

    Every row is used; the ladder is how a later reader learns which regime
    produced a number without re-running the fit.
    """
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


def test_a_silent_member_arrives_as_nan_and_must_not_read_as_a_vote():
    """`build` left-merges, so an absent member is NaN -- and NaN is not None.

    The ladder used to test `is not None`, which every NaN passes: 13,479 rows of
    the deployed bake carry a `dir_method` naming a member that never spoke, of
    which 12,405 are dGbyG-only rows labelled as an eQ/dGbyG agreement. The RATIO
    was never affected (the vote itself goes through `_num`), so this is
    provenance alone -- which is exactly why it could sit there unnoticed, and
    exactly why a before/after member accounting cannot be read until it is fixed.
    """
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


def test_an_absent_dgbyg_table_does_not_relabel_silence_as_refusal():
    """`refused` is dGbyG declining on a wildcard. NaN is dGbyG not being there.

    dGbyG cannot coexist with the eQ stack, so `drive eval` writes an EMPTY member
    table when the env lacks it -- every `dgbyg_wildcard` then arrives as a float
    NaN, which is truthy. Under the old test that turned all 47,266 no-evidence
    reactions into deliberate abstentions by a member that never ran.
    """
    absent = C.combine_row({"mnxr": "R", "dgbyg_wildcard": float("nan")},
                           calib={}, sigma_0=canon.DIR_SIGMA_0)
    assert absent["dir_method"] == "no_evidence"

    declined = C.combine_row({"mnxr": "R", "dgbyg_wildcard": True},
                             calib={}, sigma_0=canon.DIR_SIGMA_0)
    assert declined["dir_method"] == "refused"

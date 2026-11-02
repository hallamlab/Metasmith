# The curated prior is stated on the quantity the combiner averages it with.
#
# The defect this pins: `calibrate` fit every category's centre from the eQuilibrator
# member's standard-state dG, and the combiner then averaged that centre with a thermo vote
# carrying a reaction quotient. Two numbers about different things, averaged by precision.
#
# THE SCALE TRAVELS WITH THE TABLE, not with a flag. A calibration built by r9's code states
# none, and the combiner reads that as `unstated` and uses the centre verbatim -- which is
# what keeps the standard-state arm of the re-bake reproducible from its own artifacts.
from __future__ import annotations

import pandas as pd
import pytest

from ecspr.bake.direction import calibrate as K
from ecspr.bake.direction import combine as C
from ecspr.bake.direction import canon


def points(**over):
    # Two anchors per category on the measured arm, above the sigma floor.
    rows = []
    for cat, dg in (("PHYSIOL-LEFT-TO-RIGHT", -20.0), ("LEFT-TO-RIGHT", -30.0)):
        for i, bump in enumerate((0.0, 2.0)):
            rows.append(dict(mnxr=f"MNXR{cat[:2]}{i}", category=cat, dg=dg + bump,
                             sigma=3.0, uses_gc=False, reason="ok", is_transport=False))
    return pd.DataFrame(rows).assign(**over)


def test_with_no_quotient_the_two_quantities_are_equal_by_construction():
    got = K.attach_correction(points(), quotient=None)
    assert (got["dG_correction"] == 0.0).all()
    assert (got["dg_physiological"] == got["dg"]).all()


def test_the_correction_is_read_from_the_member_the_prior_is_fitted_against(tmp_path):
    # dGbyG's correction is a different number over a different restaged equation. Taking it
    # would fit the prior to a quantity no member on this path computed.
    p = points()
    q = tmp_path / "q.parquet"
    pd.DataFrame([dict(mnxr=m, member=member, dG_correction=(5.0 if member == "eq" else -99.0))
                  for m in p["mnxr"] for member in ("eq", "dgbyg")]).to_parquet(q)
    got = K.attach_correction(p, quotient=q)
    assert (got["dG_correction"] == 5.0).all()
    assert (got["dg_physiological"] == got["dg"] + 5.0).all()


def test_a_reaction_the_quotient_never_saw_defaults_to_no_correction(tmp_path):
    p = points()
    q = tmp_path / "q.parquet"
    pd.DataFrame([dict(mnxr=p["mnxr"].iat[0], member="eq", dG_correction=7.0)]).to_parquet(q)
    got = K.attach_correction(p, quotient=q)
    assert got["dG_correction"].tolist() == [7.0, 0.0, 0.0, 0.0]


def test_every_category_is_fitted_on_the_corrected_column_and_says_so():
    p = K.attach_correction(points(), quotient=None)
    p["dg_physiological"] = p["dg"] + 6.0
    cal = K.calibrate(p, prior_quantity="physiological").set_index("category")
    assert set(cal["prior_quantity"]) == {"physiological"}
    assert cal.loc["LEFT-TO-RIGHT", "median"] == pytest.approx(-23.0)      # -29 + 6
    # Both quantities stay on the table, so the choice can be re-read rather than inferred.
    assert cal.loc["LEFT-TO-RIGHT", "median_standard"] == pytest.approx(-29.0)
    assert cal.loc["LEFT-TO-RIGHT", "median_physiological"] == pytest.approx(-23.0)


def test_the_standard_arm_fits_on_the_raw_column_and_states_no_scale():
    p = K.attach_correction(points(), quotient=None)
    p["dg_physiological"] = p["dg"] + 6.0
    cal = K.calibrate(p, prior_quantity="standard").set_index("category")
    assert set(cal["prior_quantity"]) == {"unstated"}
    assert cal.loc["LEFT-TO-RIGHT", "median"] == pytest.approx(-29.0)


def test_an_unknown_prior_quantity_stops_rather_than_defaulting():
    with pytest.raises(ValueError):
        K.calibrate(K.attach_correction(points()), prior_quantity="whatever")


def test_the_combiner_takes_the_centre_verbatim_and_names_its_scale():
    # No conversion at this seam. A per-reaction transport of a standard-scale centre was
    # measured on held-out folds and rejected: it moved 16 calls, every one off the curated
    # side. `poc_prior_holdout.py` still scores that arm.
    row = dict(mnxr="R", biocyc_category="LEFT-TO-RIGHT", eq_dG_correction=9.0)
    for scale in ("physiological", "unstated"):
        got = C.combine_row(row, {"LEFT-TO-RIGHT": (-15.0, 5.0, 40, scale)},
                            canon.DIR_SIGMA_0)
        assert got["prior_mu"] == pytest.approx(-15.0)
        assert got["prior_quantity"] == scale


def test_a_calibration_that_states_no_scale_still_builds():
    # r9's tables carry no `prior_quantity` column at all.
    cal = pd.DataFrame([dict(category="LEFT-TO-RIGHT", median=-15.0, mad_spread=5.0,
                             tau=9.0, n=40)])
    out = C.build(["R"], pd.DataFrame(columns=["mnxr", "dg", "sigma", "flag"]),
                  pd.DataFrame(columns=["mnxr", "dg", "sigma", "flag"]),
                  pd.DataFrame([dict(mnxr="R", aligned="LEFT-TO-RIGHT", source="metacyc")]),
                  cal, canon.DIR_SIGMA_0)
    assert out["prior_quantity"].iat[0] == "unstated"

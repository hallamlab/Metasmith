# T4: fuse the three members into one directional ratio per base-graph reaction.
#
# The combiner is a weighted vote in dG' space (the functional-lane belief pattern,
# continuous analogue): each member votes weighted by its own precision, and the
# result is damped toward zero by total evidence -- so a reaction no member speaks
# to lands at ratio 1.0 (reversible) as a LIMIT of the single rule, not an if-branch.
#
# Two fusions, because the correlation structure differs:
#   * eQuilibrator and dGbyG are both TECRDB-fitted -> correlated. They are fused
#     into ONE thermo vote whose uncertainty is floored by TAU_SHARED (the common-
#     mode TECRDB bias their spread is blind to) and by their own disagreement, so
#     two correlated instruments cannot vote as two independent ones.
#   * The curated member is TECRDB-independent physiology -> genuinely independent,
#     so it fuses with the thermo vote by ordinary inverse-variance precision.
#
# The category vote uses the T3 calibration (category -> empirical dG' on the
# measured arm), never a direction classifier. Shrinkage is a decision rule, not a
# third prior: lambda = SIGMA_0^2 / (SIGMA_0^2 + s_post^2) damps mu toward 0 in
# proportion to how little evidence there is. The ratio is the median transform
# exp(mu_eff/RT) (transform-equivariant; never E[ratio], which inflates).
#
# Pure pandas -- runs in p312. Reads the two per-member parquets (eval_members.py),
# the curated per-MNXR table (T2), and the calibration (T3).
from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path

import numpy as np
import pandas as pd

from . import canon   # see its header for why it is a copy rather than an import of ecspr

# All knobs come from canon (committed before this run). Bound here to the names
# the combiner math uses.
RT = canon.DIR_RT
TAU_SHARED = canon.DIR_TAU_SHARED
TAU_CUR_FLOOR = canon.DIR_TAU_CUR_FLOOR
S_MEAS_FLOOR = canon.DIR_S_MEAS_FLOOR
SIGMA_FLOOR = canon.DIR_SIGMA_FLOOR
DG_CLAMP = canon.DIR_DG_CLAMP


def _num(x):
    return x is not None and not (isinstance(x, float) and math.isnan(x))


def _zero(x) -> float:
    # A missing width is zero width. r8's member tables predate `sigma_sub`, so its
    # column arrives absent or NaN there and must read as 'nothing was asserted'.
    return float(x) if _num(x) else 0.0


def eq_vote(eq_dg, eq_sig, eq_gc):
    # The eQuilibrator row as evidence, or (None, None, None) if it is none.
    #
    # A reactant-contribution row whose sigma sits at SIGMA_FLOOR is a group
    # cancellation: eQuilibrator has returned dG'=0 with no uncertainty because the
    # equation's groups cancel identically, which says the two sides are built from the
    # same pieces, not that anyone measured them. The calibration arm already rejects
    # exactly these rows; without this the combiner promoted them to authoritative
    # MEASUREMENTS, and they were 69% of tier 1.
    #
    # dGbyG almost always agrees on such a row (|dG'| < 1 kJ/mol) and that agreement is
    # not corroboration -- it is the same group cancellation seen through the second
    # TECRDB-fitted model. So the row is dropped rather than demoted to a prediction.
    #
    # Called ONCE per row, above both the vote and the dir_method label: normalising in
    # `thermo_vote` alone would leave the provenance ladder reading the raw column and
    # naming an estimator that no longer votes.
    if _num(eq_dg) and eq_gc is False and _num(eq_sig) and eq_sig <= SIGMA_FLOOR:
        return None, None, None
    return eq_dg, eq_sig, eq_gc


def thermo_vote(eq_dg, eq_sig, eq_gc, db_dg, db_sig):
    # One thermo vote (mu, s, is_measured) from the correlated eQ/dGbyG pair.
    #
    # Measurement precedence (A1): when eQ has MEASURED a reaction (reactant-
    # contribution arm), dGbyG's number is a lossy readback of that same TECRDB
    # value, so averaging it in only adds noise -- the measurement is used at its
    # own sigma. Only when both members are PREDICTIONS (eQ group-contribution arm
    # and/or dGbyG) do they genuinely compete, and then the vote is floored by
    # TAU_SHARED (the common-mode TECRDB error their spread cannot see) and by their
    # own disagreement, so two correlated predictors cannot vote as two independent.
    have_eq, have_db = _num(eq_dg), _num(db_dg)
    if have_eq and eq_gc is False:                 # measurement dominates
        return eq_dg, max(eq_sig, S_MEAS_FLOOR), True
    if have_eq and have_db:                        # two predictions competing
        mu = 0.5 * (eq_dg + db_dg)                 # equal weights (incommensurable sigmas)
        s_ind2 = (eq_sig ** 2 + db_sig ** 2) / 4.0  # if independent (an underestimate)
        spread2 = ((eq_dg - db_dg) / 2.0) ** 2      # disagreement = lower bound on error
        return mu, math.sqrt(max(s_ind2, spread2) + TAU_SHARED ** 2), False
    if have_eq:                                    # eQ GC prediction alone
        return eq_dg, math.sqrt(eq_sig ** 2 + TAU_SHARED ** 2), False
    if have_db:                                    # dGbyG prediction alone
        return db_dg, math.sqrt(db_sig ** 2 + TAU_SHARED ** 2), False
    return None


def combine_row(r, calib, sigma_0, dg_clamp=DG_CLAMP):
    # Normalise the eQ row ONCE, here: everything below reads eq_dg/eq_sig/eq_gc and
    # never the raw columns, so the vote and the provenance label cannot disagree about
    # whether eQuilibrator spoke. The raw columns are still emitted verbatim below.
    eq_dg, eq_sig, eq_gc = eq_vote(r.get("eq_dg"), r.get("eq_sigma"), r.get("eq_uses_gc"))
    tv = thermo_vote(eq_dg, eq_sig, eq_gc, r.get("dgbyg_dg"), r.get("dgbyg_sigma"))
    cat = r.get("biocyc_category")
    prior = calib.get(cat) if cat else None       # (mu, tau, n) or None

    votes = []                                    # (mu, s)
    if tv is not None:
        votes.append((tv[0], tv[1]))
    if prior is not None:
        mu_c, tau_c, n_c = prior
        votes.append((mu_c, max(tau_c, TAU_CUR_FLOOR)))

    if not votes:                                 # default reversible, as a limit
        mu_post, s_post = 0.0, None
    else:
        wsum = sum(1.0 / s ** 2 for _, s in votes)
        mu_post = sum(mu / s ** 2 for mu, s in votes) / wsum
        s_post = math.sqrt(1.0 / wsum)

    # THE SUBSTITUTION WIDTH, FOLDED IN HERE AND NOWHERE EARLIER. `eq_vote` has already
    # run above on the member's RAW sigma, so a group cancellation has already been
    # refused; widening before that point lifts a cancelling zero over SIGMA_FLOOR and
    # re-promotes it to tier 1, which is the defect r8 was baked to remove. An asserted
    # structure has a width and a posterior that ignored it would read as a measurement.
    sigma_sub = max(_zero(r.get("eq_sigma_sub")), _zero(r.get("dgbyg_sigma_sub")))
    if s_post is not None and sigma_sub > 0:
        s_post = math.hypot(s_post, sigma_sub)

    # shrinkage decision rule
    if s_post is None:
        lam, mu_eff, s_eff = 0.0, 0.0, sigma_0
    else:
        lam = sigma_0 ** 2 / (sigma_0 ** 2 + s_post ** 2)
        mu_eff = lam * mu_post
        s_eff = s_post
    # keep the ratio a finite two-way conductance ratio, never a hard gate
    clamped = abs(mu_eff) > dg_clamp
    if clamped:
        mu_eff = math.copysign(dg_clamp, mu_eff)
    ratio = math.exp(mu_eff / RT)

    # provenance ladder: a record of which regime spoke, NOT a selection.
    # Presence is tested with _num(), never `is not None`: these columns come from
    # a pandas LEFT MERGE, so an absent member arrives as NaN and `NaN is not None`
    # is True -- which credits every silent member with a vote it never cast. And it
    # reads eq_dg, the NORMALISED value, so a group cancellation is not named as eQ
    # evidence here after eq_vote() has already refused to let it vote above.
    have_eq, have_db = _num(eq_dg), _num(r.get("dgbyg_dg"))
    if tv is not None and tv[2] and sigma_sub == 0:
        tier, method = 1, ("eq_rc+dgbyg" if have_db else "eq_rc")
    elif tv is not None and tv[2]:
        # Tier 1 is the tier a consumer reads as MEASURED. eQuilibrator measured the
        # MODEL equation, not this one -- the anchor gate says the model reproduces a
        # real scored reaction, which justifies the NUMBER without making it an
        # observation of these reactants. Demoting is one-way: substitution can lower a
        # tier here, never raise one.
        tier, method = 2, ("eq_rc+dgbyg" if have_db else "eq_rc") + "_sub"
    elif tv is not None:
        method = ("eq_gc_x_dgbyg" if (have_eq and have_db)
                  else ("eq_gc" if have_eq else "dgbyg"))
        tier = 2
    elif prior is not None:
        tier, method = 3, "biocyc_only"
    else:
        tier = 0
        # Same NaN hazard: an absent dGbyG row leaves a float NaN here, which is
        # TRUTHY -- so a member that never ran would relabel every silent
        # reaction as its own deliberate abstention.
        wc = r.get("dgbyg_wildcard")
        method = "refused" if (_num(wc) and bool(wc)) else "no_evidence"
    if prior is not None and tier in (1, 2):
        method += "+biocyc"

    return dict(
        mnxr=r["mnxr"], dG_prime=mu_eff, sigma=s_eff, ratio=ratio,
        dir_tier=tier, dir_method=method, dir_confidence=lam,
        dG_raw=mu_post, lambda_shrink=lam, clamped=clamped,
        eq_dg=r.get("eq_dg"), eq_sigma=r.get("eq_sigma"), eq_uses_gc=r.get("eq_uses_gc"),
        dgbyg_dg=r.get("dgbyg_dg"), dgbyg_sigma=r.get("dgbyg_sigma"),
        dgbyg_wildcard=r.get("dgbyg_wildcard"), sigma_sub=sigma_sub,
        biocyc_category=cat, biocyc_source=r.get("biocyc_source"),
        prior_mu=(prior[0] if prior else None),
        prior_tau=(prior[1] if prior else None),
        prior_n=(prior[2] if prior else None),
    )


def build(base_mnxrs, eq_df, db_df, curated, calib_df, sigma_0,
          prior_width_kind=canon.DIR_PRIOR_WIDTH_KIND, dg_clamp=DG_CLAMP):
    eq = eq_df.rename(columns={"dg": "eq_dg", "sigma": "eq_sigma", "flag": "eq_uses_gc",
                               "sigma_sub": "eq_sigma_sub"})
    db = db_df.rename(columns={"dg": "dgbyg_dg", "sigma": "dgbyg_sigma",
                               "flag": "dgbyg_wildcard", "sigma_sub": "dgbyg_sigma_sub"})
    cur = curated.rename(columns={"aligned": "biocyc_category", "source": "biocyc_source"})
    # r8's member tables predate `sigma_sub`. Materialise it rather than branching on its
    # presence, so every row below reads one shape.
    for frame, col in ((eq, "eq_sigma_sub"), (db, "dgbyg_sigma_sub")):
        if col not in frame.columns:
            frame[col] = 0.0
    base = pd.DataFrame({"mnxr": base_mnxrs})
    df = (base
          .merge(eq[["mnxr", "eq_dg", "eq_sigma", "eq_uses_gc", "eq_sigma_sub"]],
                 on="mnxr", how="left")
          .merge(db[["mnxr", "dgbyg_dg", "dgbyg_sigma", "dgbyg_wildcard",
                     "dgbyg_sigma_sub"]], on="mnxr", how="left")
          .merge(cur[["mnxr", "biocyc_category", "biocyc_source"]], on="mnxr", how="left"))
    # A ROBUST CENTRE AND A NON-ROBUST SCALE ARE NOT A PAIR. `calibrate.fit` computes both
    # estimators from the same points and stores both; this lane read `median` for the
    # centre and `tau` -- a plain variance-minus-mean-sigma-squared -- for the width, so a
    # handful of outliers set the width of a bin whose centre had already been protected
    # from them. The robust spread runs 2x to 20x narrower, and under `tau` the shrinkage
    # annihilated the vote: PHYSIOL-RIGHT-TO-LEFT fits to +24.76 kJ/mol, a 21,734:1 ratio,
    # and shipped 1.37.
    #
    # SELECTABLE, because this moves 6,072 reactions by four orders of magnitude and a
    # change that large is priced as its own arm of the re-bake rather than asserted.
    if prior_width_kind not in canon.DIR_PRIOR_WIDTH_KINDS:
        raise ValueError(f"unknown prior width {prior_width_kind!r}; "
                         f"expected one of {canon.DIR_PRIOR_WIDTH_KINDS}")
    width = "mad_spread" if prior_width_kind == "robust" else "tau"
    if width not in calib_df.columns:
        raise ValueError(f"calibration table carries no {width!r} column")
    calib = {r.category: (r.median, getattr(r, width), int(r.n))
             for r in calib_df.itertuples()}
    rows = [combine_row(rec, calib, sigma_0, dg_clamp) for rec in df.to_dict("records")]
    out = pd.DataFrame(rows)
    # Named in the annotation, so a consumer reads which estimator produced the width it
    # is looking at rather than inferring it from the release number.
    out["prior_width_kind"] = prior_width_kind
    return out


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--base-mnxrs", required=True)
    ap.add_argument("--eq", required=True)
    ap.add_argument("--dgbyg", required=True)
    ap.add_argument("--curated", required=True)
    ap.add_argument("--calibration", required=True)
    ap.add_argument("--clamp", type=float, default=canon.DIR_DG_CLAMP,
                    help="magnitude bound on dG_prime, kJ/mol. The DEPLOYED r9 was baked "
                         "at 100.0 and canon has since moved to three decades (17.12) "
                         "without a re-bake, so reproducing r9 needs --clamp 100")
    ap.add_argument("--prior-width", default=canon.DIR_PRIOR_WIDTH_KIND,
                    choices=list(canon.DIR_PRIOR_WIDTH_KINDS),
                    help="which stored spread estimator the curated prior uses")
    ap.add_argument("--sigma0", type=float, default=canon.DIR_SIGMA_0,
                    help="reversible-default prior width; defaults to canon.DIR_SIGMA_0")
    ap.add_argument("--out", required=True)
    a = ap.parse_args(argv)
    lo, hi = canon.DIR_SIGMA_0_BAND
    if not (lo < a.sigma0 < hi):
        raise SystemExit(f"sigma0={a.sigma0} outside plausibility band {canon.DIR_SIGMA_0_BAND} "
                         f"-- that is a finding, not a constant; stop and look.")
    base = json.load(open(a.base_mnxrs))
    out = build(base, pd.read_parquet(a.eq), pd.read_parquet(a.dgbyg),
                pd.read_parquet(a.curated), pd.read_parquet(a.calibration), a.sigma0,
                prior_width_kind=a.prior_width, dg_clamp=a.clamp)
    out.to_parquet(a.out, index=False)
    print(f"[combine] {len(out)} base-graph reactions -> {a.out}")
    print(f"[combine] curated prior width: {a.prior_width}  "
          f"clamp: {a.clamp:.4g} kJ/mol ({a.clamp / canon.DIR_DECADE:.2f} decades)")
    print("[combine] dir_tier:\n" + out["dir_tier"].value_counts().sort_index().to_string())
    nonrev = out[out["ratio"] != 1.0]
    print(f"[combine] carry direction (ratio != 1.0): {len(nonrev)} ({len(nonrev)/len(out):.1%})")
    print(f"[combine] ratio range: {out['ratio'].min():.3g} .. {out['ratio'].max():.3g}")


if __name__ == "__main__":
    main()

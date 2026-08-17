"""T4: fuse the three members into one directional ratio per base-graph reaction.

The combiner is a weighted vote in dG' space (the functional-lane belief pattern,
continuous analogue): each member votes weighted by its own precision, and the
result is damped toward zero by total evidence -- so a reaction no member speaks
to lands at ratio 1.0 (reversible) as a LIMIT of the single rule, not an if-branch.

Two fusions, because the correlation structure differs:
  * eQuilibrator and dGbyG are both TECRDB-fitted -> correlated. They are fused
    into ONE thermo vote whose uncertainty is floored by TAU_SHARED (the common-
    mode TECRDB bias their spread is blind to) and by their own disagreement, so
    two correlated instruments cannot vote as two independent ones.
  * The curated member is TECRDB-independent physiology -> genuinely independent,
    so it fuses with the thermo vote by ordinary inverse-variance precision.

The category vote uses the T3 calibration (category -> empirical dG' on the
measured arm), never a direction classifier. Shrinkage is a decision rule, not a
third prior: lambda = SIGMA_0^2 / (SIGMA_0^2 + s_post^2) damps mu toward 0 in
proportion to how little evidence there is. The ratio is the median transform
exp(mu_eff/RT) (transform-equivariant; never E[ratio], which inflates).

Pure pandas -- runs in p312. Reads the two per-member parquets (eval_members.py),
the curated per-MNXR table (T2), and the calibration (T3).
"""
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
DG_CLAMP = canon.DIR_DG_CLAMP


def _num(x):
    return x is not None and not (isinstance(x, float) and math.isnan(x))


def thermo_vote(eq_dg, eq_sig, eq_gc, db_dg, db_sig):
    """One thermo vote (mu, s, is_measured) from the correlated eQ/dGbyG pair.

    Measurement precedence (A1): when eQ has MEASURED a reaction (reactant-
    contribution arm), dGbyG's number is a lossy readback of that same TECRDB
    value, so averaging it in only adds noise -- the measurement is used at its
    own sigma. Only when both members are PREDICTIONS (eQ group-contribution arm
    and/or dGbyG) do they genuinely compete, and then the vote is floored by
    TAU_SHARED (the common-mode TECRDB error their spread cannot see) and by their
    own disagreement, so two correlated predictors cannot vote as two independent.
    """
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


def combine_row(r, calib, sigma_0):
    tv = thermo_vote(r.get("eq_dg"), r.get("eq_sigma"), r.get("eq_uses_gc"),
                     r.get("dgbyg_dg"), r.get("dgbyg_sigma"))
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

    # shrinkage decision rule
    if s_post is None:
        lam, mu_eff, s_eff = 0.0, 0.0, sigma_0
    else:
        lam = sigma_0 ** 2 / (sigma_0 ** 2 + s_post ** 2)
        mu_eff = lam * mu_post
        s_eff = s_post
    # keep the ratio a finite two-way conductance ratio, never a hard gate
    clamped = abs(mu_eff) > DG_CLAMP
    if clamped:
        mu_eff = math.copysign(DG_CLAMP, mu_eff)
    ratio = math.exp(mu_eff / RT)

    # provenance ladder: a record of which regime spoke, NOT a selection.
    # Presence is tested with _num(), never `is not None`: these columns come from
    # a pandas LEFT MERGE, so an absent member arrives as NaN and `NaN is not None`
    # is True -- which credits every silent member with a vote it never cast.
    have_eq, have_db = _num(r.get("eq_dg")), _num(r.get("dgbyg_dg"))
    if tv is not None and tv[2]:
        tier, method = 1, ("eq_rc+dgbyg" if have_db else "eq_rc")
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
        dgbyg_wildcard=r.get("dgbyg_wildcard"),
        biocyc_category=cat, biocyc_source=r.get("biocyc_source"),
        prior_mu=(prior[0] if prior else None),
        prior_tau=(prior[1] if prior else None),
        prior_n=(prior[2] if prior else None),
    )


def build(base_mnxrs, eq_df, db_df, curated, calib_df, sigma_0):
    eq = eq_df.rename(columns={"dg": "eq_dg", "sigma": "eq_sigma", "flag": "eq_uses_gc"})
    db = db_df.rename(columns={"dg": "dgbyg_dg", "sigma": "dgbyg_sigma", "flag": "dgbyg_wildcard"})
    cur = curated.rename(columns={"aligned": "biocyc_category", "source": "biocyc_source"})
    base = pd.DataFrame({"mnxr": base_mnxrs})
    df = (base
          .merge(eq[["mnxr", "eq_dg", "eq_sigma", "eq_uses_gc"]], on="mnxr", how="left")
          .merge(db[["mnxr", "dgbyg_dg", "dgbyg_sigma", "dgbyg_wildcard"]], on="mnxr", how="left")
          .merge(cur[["mnxr", "biocyc_category", "biocyc_source"]], on="mnxr", how="left"))
    calib = {r.category: (r.median, r.tau, int(r.n)) for r in calib_df.itertuples()}
    rows = [combine_row(rec, calib, sigma_0) for rec in df.to_dict("records")]
    return pd.DataFrame(rows)


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--base-mnxrs", required=True)
    ap.add_argument("--eq", required=True)
    ap.add_argument("--dgbyg", required=True)
    ap.add_argument("--curated", required=True)
    ap.add_argument("--calibration", required=True)
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
                pd.read_parquet(a.curated), pd.read_parquet(a.calibration), a.sigma0)
    out.to_parquet(a.out, index=False)
    print(f"[combine] {len(out)} base-graph reactions -> {a.out}")
    print("[combine] dir_tier:\n" + out["dir_tier"].value_counts().sort_index().to_string())
    nonrev = out[out["ratio"] != 1.0]
    print(f"[combine] carry direction (ratio != 1.0): {len(nonrev)} ({len(nonrev)/len(out):.1%})")
    print(f"[combine] ratio range: {out['ratio'].min():.3g} .. {out['ratio'].max():.3g}")


if __name__ == "__main__":
    main()

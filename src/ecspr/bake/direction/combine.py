from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path

import numpy as np
import pandas as pd

from . import canon

RT = canon.DIR_RT
TAU_SHARED = canon.DIR_TAU_SHARED
TAU_CUR_FLOOR = canon.DIR_TAU_CUR_FLOOR
S_MEAS_FLOOR = canon.DIR_S_MEAS_FLOOR
DG_CLAMP = canon.DIR_DG_CLAMP


def _num(x):
    return x is not None and not (isinstance(x, float) and math.isnan(x))


def thermo_vote(eq_dg, eq_sig, eq_gc, db_dg, db_sig):
    have_eq, have_db = _num(eq_dg), _num(db_dg)
    if have_eq and eq_gc is False:
        return eq_dg, max(eq_sig, S_MEAS_FLOOR), True
    if have_eq and have_db:
        mu = 0.5 * (eq_dg + db_dg)
        s_ind2 = (eq_sig ** 2 + db_sig ** 2) / 4.0
        spread2 = ((eq_dg - db_dg) / 2.0) ** 2
        return mu, math.sqrt(max(s_ind2, spread2) + TAU_SHARED ** 2), False
    if have_eq:
        return eq_dg, math.sqrt(eq_sig ** 2 + TAU_SHARED ** 2), False
    if have_db:
        return db_dg, math.sqrt(db_sig ** 2 + TAU_SHARED ** 2), False
    return None


def combine_row(r, calib, sigma_0):
    tv = thermo_vote(r.get("eq_dg"), r.get("eq_sigma"), r.get("eq_uses_gc"),
                     r.get("dgbyg_dg"), r.get("dgbyg_sigma"))
    cat = r.get("biocyc_category")
    prior = calib.get(cat) if cat else None

    votes = []
    if tv is not None:
        votes.append((tv[0], tv[1]))
    if prior is not None:
        mu_c, tau_c, n_c = prior
        votes.append((mu_c, max(tau_c, TAU_CUR_FLOOR)))

    if not votes:
        mu_post, s_post = 0.0, None
    else:
        wsum = sum(1.0 / s ** 2 for _, s in votes)
        mu_post = sum(mu / s ** 2 for mu, s in votes) / wsum
        s_post = math.sqrt(1.0 / wsum)

    if s_post is None:
        lam, mu_eff, s_eff = 0.0, 0.0, sigma_0
    else:
        lam = sigma_0 ** 2 / (sigma_0 ** 2 + s_post ** 2)
        mu_eff = lam * mu_post
        s_eff = s_post
    clamped = abs(mu_eff) > DG_CLAMP
    if clamped:
        mu_eff = math.copysign(DG_CLAMP, mu_eff)
    ratio = math.exp(mu_eff / RT)

    if tv is not None and tv[2]:
        tier, method = 1, ("eq_rc+dgbyg" if r.get("dgbyg_dg") is not None else "eq_rc")
    elif tv is not None:
        both = (r.get("eq_dg") is not None) and (r.get("dgbyg_dg") is not None)
        method = "eq_gc_x_dgbyg" if both else ("eq_gc" if r.get("eq_dg") is not None else "dgbyg")
        tier = 2
    elif prior is not None:
        tier, method = 3, "biocyc_only"
    else:
        tier = 0
        method = "refused" if r.get("dgbyg_wildcard") else "no_evidence"
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

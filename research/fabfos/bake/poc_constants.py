# The three constants r10 invalidates, re-derived from r10's own artifacts.
#
# WHY A SCRIPT AND NOT A NOTE. `DIR_SIGMA_0` and `DIR_DG_CLAMP` are fitted quantities, and a
# fitted quantity whose derivation lives only in a commit message cannot be re-run when the
# distribution it describes moves. Every number in `canon.py`'s comments for these two comes
# out of here, against a run's own `_calibration_points.parquet` and `direction_annotation.parquet`.
#
# WHAT EACH ONE IS FITTED ON, WHICH IS NOT THE SAME THING:
#
#   DIR_SIGMA_0   the robust MARGINAL spread of measured dG' over the calibration anchors.
#                 It is a property of the points, not of the fit, so moving the curated
#                 prior does not move it -- which is worth stating, because the r10 plan
#                 expected it to.
#   DIR_TAU_CUR_FLOOR  swept by `poc_prior_holdout.py --floor-sweep`, which needs the folds.
#                 Not repeated here.
#   DIR_DG_CLAMP  the bound past which the graph stops changing. It is a property of the
#                 POSTERIOR, so the prior does move it, and it is re-swept here.
#
# THE CLAMP'S METRIC IS THE GRAPH'S, NOT THE ANNOTATION'S. A reaction enters the model as a
# two-way conductance whose forward share is 2r/(1+r) -- 1.0 at ratio 1, approaching 2 as the
# reaction becomes one-way. Bounding |dG'| moves that share, and the bound is earned where the
# mean share stops moving. `model/build.py`'s DIRECTION_DECADE_CAP is the same statement at
# the model seam and must agree with what this prints.
#
# Run: mamba run -n msm python research/fabfos/bake/poc_constants.py \
#          --points <run>/_calibration_points.parquet \
#          --annotation <run>/direction_annotation.parquet
from __future__ import annotations

import argparse
import math
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "src"))

from ecspr.bake.direction import canon                    # noqa: E402
from ecspr.bake.direction.calibrate import robust         # noqa: E402

RT = canon.DIR_RT
DECADE = canon.DIR_DECADE


def sigma_0(points: pd.DataFrame, column: str) -> tuple:
    # The same arm and the same floor the calibration fits on: reactant-contribution only,
    # above DIR_SIGMA_FLOOR. Anchors below that floor are group cancellations, and a quarter
    # of the mass sitting at exactly zero is what pinned an earlier fit's median to 0.000.
    ok = points[(points["reason"] == "ok") & points["dg"].notna()]
    measured = ok[(ok["uses_gc"] == False) &                       # noqa: E712
                  (ok["sigma"] > canon.DIR_SIGMA_FLOOR)]
    med, spread = robust(measured[column].to_numpy(float))
    return len(measured), med, spread


def clamp_sweep(annot: pd.DataFrame, decades) -> list:
    # mu_eff BEFORE the bound, recovered from the two columns the annotation already carries:
    # dG_prime is the clamped lambda*mu_post, and lambda_shrink and dG_raw are both stored.
    mu = (annot["lambda_shrink"].astype(float) * annot["dG_raw"].astype(float)).to_numpy()
    mu = mu[np.isfinite(mu)]
    out = []
    for d in decades:
        bound = (d * DECADE) if d is not None else np.inf
        with np.errstate(over="ignore", invalid="ignore"):
            r = np.exp(np.clip(mu, -bound, bound) / RT)
            # Unbounded, exp() overflows to inf on the polymer tail and 2r/(1+r) becomes
            # nan. That is the other half of why the bound exists, so it is reported as
            # what it is rather than silently dropped.
            share = float(np.mean(2.0 * r / (1.0 + r)))
        out.append((d, share, int((np.abs(mu) > bound).sum())))
    return out


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--points", required=True)
    ap.add_argument("--annotation", required=True)
    a = ap.parse_args(argv)

    points = pd.read_parquet(a.points)
    print("=== DIR_SIGMA_0: robust marginal spread of the measured arm ===")
    columns = [c for c in ("dg", "dg_physiological") if c in points.columns]
    for col in columns:
        n, med, spread = sigma_0(points, col)
        lo, hi = canon.DIR_SIGMA_0_BAND
        verdict = "inside the band" if lo < spread < hi else "OUTSIDE THE BAND -- a finding"
        print(f"  {col:<18} n={n:4d}  median {med:8.3f}  robust spread {spread:8.3f} "
              f"kJ/mol   {verdict}")
    print(f"  committed DIR_SIGMA_0 = {canon.DIR_SIGMA_0}")
    print("  The prior does not enter this fit -- it is the spread of the anchors themselves.")

    annot = pd.read_parquet(a.annotation)
    print("\n=== DIR_DG_CLAMP: where the mean forward share stops moving ===")
    rows = clamp_sweep(annot, [1.0, 2.0, 3.0, 4.0, 6.0, 9.0, None])
    base = next(s for d, s, _ in rows if d == 9.0)     # the widest bound that still evaluates
    print(f"  {'decades':>9} {'kJ/mol':>9} {'mean 2r/(1+r)':>15} {'vs 9 decades':>13} "
          f"{'rows clamped':>13}")
    for d, share, n in rows:
        label = "unbounded" if d is None else f"{d:.0f}"
        kj = "-" if d is None else f"{d * DECADE:.2f}"
        cell = "overflows exp()" if not np.isfinite(share) else f"{share:15.6f}"
        gap = "-" if not np.isfinite(share) else f"{abs(share - base) / base:.4%}"
        print(f"  {label:>9} {kj:>9} {cell:>15} {gap:>13} {n:13,}")
    print(f"  committed DIR_DG_CLAMP = {canon.DIR_DG_CLAMP:.4g} kJ/mol "
          f"({canon.DIR_DG_CLAMP / DECADE:.2f} decades)")

    print("\n=== DIR_TAU_CUR_FLOOR ===")
    print("  swept by `poc_prior_holdout.py --floor-sweep`, which needs the fold split; "
          f"committed {canon.DIR_TAU_CUR_FLOOR:.4g} kJ/mol "
          f"({canon.DIR_TAU_CUR_FLOOR / DECADE:.2f} decades)")
    return 0


if __name__ == "__main__":
    sys.exit(main())

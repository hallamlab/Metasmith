# Does the curated prior's WIDTH earn the four-order-of-magnitude change it proposes?
#
# WHY A HOLD-OUT AND NOT AN ARGUMENT. Reading `mad_spread` instead of `tau` moves 6,072
# tier-3 reactions by four orders of magnitude -- PHYSIOL-RIGHT-TO-LEFT goes from ratio 1.37
# to about 1.2e4. A change that size cannot land on being more internally consistent.
#
# AND IT CANNOT BE SCORED THE WAY THE CONCENTRATION TERM IS. The concentration term is
# checkable against the MetaCyc categories precisely because it never touches them. The
# curated prior IS those categories, so scoring it against them in place would be scoring a
# fit against its own training data. The fold split is what makes the label held out: fit
# the category prior on four folds, emit the fifth from that prior alone, score the fifth.
#
# TWO METRICS, because agreement alone can be bought with confidence. A width that is simply
# too tight ships larger ratios and gets the same calls right, so:
#   1. is the held-out call right, and
#   2. is |ratio| CALIBRATED -- a bin shipping 1e4 must be right far more often than one
#      shipping 20. If it is not, the new width is too tight rather than the old one too
#      wide, and that is the finding.
#
# IT ALSO SCORES THE PRIOR'S SCALE, on the same folds and for the same reason. r10 fits a
# `PHYSIOL-` category against the corrected number and transports the others onto that scale
# by adding each reaction's own correction, so a bin that used to make ONE prediction now
# makes one per reaction. That is a change to what the curated vote says, and the label it
# would be scored against in place is the label it was fitted on.
#
# THE FOLDS ARE STRATIFIED BY CATEGORY. RIGHT-TO-LEFT carries a few hundred measured
# anchors; an unstratified split lands one of the small bins entirely in a single fold and
# the fit for that fold has nothing to estimate from.
from __future__ import annotations

import argparse
import math
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "src"))

from ecspr.bake.direction import canon                       # noqa: E402
from ecspr.bake.direction.calibrate import MAD_K, robust     # noqa: E402

RT = canon.DIR_RT
DECADE = canon.DIR_DECADE
FORWARD = {"LEFT-TO-RIGHT", "PHYSIOL-LEFT-TO-RIGHT"}
REVERSE = {"RIGHT-TO-LEFT", "PHYSIOL-RIGHT-TO-LEFT"}


def fit_fold(points: pd.DataFrame, quantity="standard") -> dict:
    # (median, mad_spread, tau, n) per category, exactly as `calibrate.calibrate` computes
    # them -- including which column each category is fitted against.
    out = {}
    for cat, g in points.groupby("category"):
        col = "dg_physiological" if scale_of(cat, quantity) == "physiological" else "dg"
        dg = g[col].to_numpy(float)
        sig = g["sigma"].to_numpy(float)
        med, spread = robust(dg)
        var_obs = float(np.var(dg)) if len(dg) > 1 else 0.0
        tau2 = max(0.0, var_obs - float(np.mean(sig ** 2)))
        out[cat] = dict(median=med, mad_spread=spread, tau=float(np.sqrt(tau2)), n=len(g))
    return out


# `by_label` IS A REJECTED ALTERNATIVE, kept here and nowhere else. It splits the taxonomy by
# the `PHYSIOL-` prefix: the physiological bins fitted on the corrected number, the
# irreversible bins fitted on the standard one and then TRANSPORTED per reaction by that
# reaction's own correction. It is scored beside the two shipped options so the rejection is
# a measurement anyone can re-run.
PHYSIOL_PREFIX = "PHYSIOL-"


def scale_of(category: str, quantity: str) -> str:
    if quantity == "standard":
        return "unstated"
    if quantity == "physiological":
        return "physiological"
    return ("physiological" if str(category).startswith(PHYSIOL_PREFIX) else "standard")


def prior_mu(fitted: dict, category: str, correction: float, quantity: str) -> float:
    # The centre this reaction's curated vote actually carries. A standard-scale centre is
    # transported onto the physiological scale by the reaction's own correction, which is
    # what makes a bin's prediction vary across the bin instead of being one number.
    mu = fitted["median"]
    if scale_of(category, quantity) == "standard":
        mu += correction
    return mu


def shrink(mu: float, tau: float, sigma_0: float) -> tuple:
    # The combiner's one rule, on a single vote: shrink toward no-information.
    tau = max(tau, canon.DIR_TAU_CUR_FLOOR)
    lam = sigma_0 ** 2 / (sigma_0 ** 2 + tau ** 2)
    dg = lam * mu
    dg = max(-canon.DIR_DG_CLAMP, min(canon.DIR_DG_CLAMP, dg))
    return dg, lam


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--points", required=True,
                    help="_calibration_points.parquet from the deployed run")
    ap.add_argument("--folds", type=int, default=5)
    ap.add_argument("--sigma0", type=float, default=canon.DIR_SIGMA_0)
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--floor-sweep", action="store_true",
                    help="sweep DIR_TAU_CUR_FLOOR instead of comparing estimators")
    a = ap.parse_args(argv)

    points = pd.read_parquet(a.points)
    ok = points[(points["reason"] == "ok") & points["dg"].notna()]
    measured = ok[(ok["uses_gc"] == False) &                      # noqa: E712
                  (ok["sigma"] > canon.DIR_SIGMA_FLOOR)]
    directional = measured[measured["category"].isin(FORWARD | REVERSE)].copy()
    print(f"[holdout] {len(points):,} curated points, {len(measured):,} on the measured arm "
          f"above the sigma floor, {len(directional):,} of those directional")

    # Stratified by category, deterministic in the seed.
    rng = np.random.default_rng(a.seed)
    directional["fold"] = -1
    for cat, g in directional.groupby("category"):
        idx = g.index.to_numpy().copy()
        rng.shuffle(idx)
        directional.loc[idx, "fold"] = np.arange(len(idx)) % a.folds
    print("[holdout] per-category counts: "
          + ", ".join(f"{c}={n}" for c, n in directional["category"].value_counts().items()))

    quantities = (["standard", "physiological", "by_label"]
                  if "dg_physiological" in points.columns else ["standard"])
    if len(quantities) == 1:
        print("[holdout] these points carry no dg_physiological column -- scoring the "
              "standard-state prior alone")
    rows = []
    for fold in range(a.folds):
        train = measured[~measured.index.isin(
            directional[directional["fold"] == fold].index)]
        test = directional[directional["fold"] == fold]
        for quantity in quantities:
            fitted = fit_fold(train, quantity)
            for r in test.itertuples():
                f = fitted.get(r.category)
                if f is None:
                    continue
                truth = 1 if r.category in REVERSE else -1
                mu = prior_mu(f, r.category, getattr(r, "dG_correction", 0.0), quantity)
                for kind, width in (("robust", f["mad_spread"]), ("tau", f["tau"])):
                    dg, lam = shrink(mu, width, a.sigma0)
                    rows.append(dict(kind=kind, quantity=quantity, fold=fold,
                                     category=r.category, truth=truth,
                                     dg=dg, lam=lam, ratio=math.exp(dg / RT)))
    held = pd.DataFrame(rows)

    if a.floor_sweep:
        # THE FLOOR IS WHAT CAPS THE STRONGEST CURATED CALL once the width is robust.
        # PHYSIOL-RIGHT-TO-LEFT's robust spread is below one decade, so the floor binds and
        # the fitted ratio is the floor's, not the data's. Swept rather than inherited.
        quantity = canon.DIR_PRIOR_QUANTITY if len(quantities) > 1 else "standard"
        print(f"\n[holdout] floor swept against the {quantity} prior")
        print(f"{'floor kJ/mol':>13} {'decades':>8} {'decided':>8} {'accuracy':>9} "
              f"{'median |ratio|':>15} {'max |ratio|':>12}")
        for mult in (0.25, 0.5, 0.75, 1.0, 1.5, 2.0, 3.0):
            floor = mult * DECADE
            sub = []
            for fold in range(a.folds):
                train = measured[~measured.index.isin(
                    directional[directional["fold"] == fold].index)]
                # Swept against the prior the bake actually ships, which is the corrected
                # one -- a floor earned on the standard-state fit would be a floor for a
                # different distribution.
                fitted = fit_fold(train, quantity)
                for r in directional[directional["fold"] == fold].itertuples():
                    f = fitted.get(r.category)
                    if f is None:
                        continue
                    tau = max(f["mad_spread"], floor)
                    lam = a.sigma0 ** 2 / (a.sigma0 ** 2 + tau ** 2)
                    mu = prior_mu(f, r.category, getattr(r, "dG_correction", 0.0), quantity)
                    dg = max(-canon.DIR_DG_CLAMP, min(canon.DIR_DG_CLAMP, lam * mu))
                    sub.append((dg, 1 if r.category in REVERSE else -1))
            d = pd.DataFrame(sub, columns=["dg", "truth"])
            call = (d["dg"] > DECADE).astype(int) - (d["dg"] < -DECADE).astype(int)
            dec = call != 0
            acc = (call[dec] == d["truth"][dec]).mean() if dec.sum() else float("nan")
            mag = np.exp(np.abs(d["dg"]) / RT)
            print(f"{floor:13.2f} {mult:8.2f} {int(dec.sum()):8d} {acc:9.2%} "
                  f"{mag.median():15.4g} {mag.max():12.4g}")
        return 0

    print(f"\n{'estimator':>10} {'quantity':>10} {'n':>6} {'decided':>8} {'right':>7} "
          f"{'wrong':>7} {'accuracy':>9} {'median |ratio|':>15}")
    for (kind, quantity), g in held.groupby(["kind", "quantity"]):
        call = (g["dg"] > DECADE).astype(int) - (g["dg"] < -DECADE).astype(int)
        dec = call != 0
        right = int((call[dec] == g["truth"][dec]).sum())
        wrong = int(dec.sum()) - right
        acc = right / dec.sum() if dec.sum() else float("nan")
        mag = np.exp(np.abs(g["dg"]) / RT).median()
        print(f"{kind:>10} {quantity:>10} {len(g):6d} {int(dec.sum()):8d} {right:7d} "
              f"{wrong:7d} {acc:9.2%} {mag:15.4g}")

    # WHAT THIS HOLD-OUT CAN AND CANNOT SETTLE, UNDER THE STANDARD-SCALE PRIOR. That prior
    # makes ONE prediction per category, so every reaction in a bin gets the same dG and the
    # same call. Accuracy is a four-valued statistic dressed as a 181-valued one, and once
    # both estimators put every bin on the correct side it saturates. What it still measures
    # cleanly is DECISIVENESS at equal accuracy -- how much of the curated evidence survives
    # shrinkage instead of being hedged into reversible.
    #
    # The `by_label` prior is not four-valued: transporting a standard-scale centre by each
    # reaction's own correction gives every reaction in a bin its own number, so accuracy
    # discriminates there and the fold table below reports a RANGE rather than one value.
    #
    # The second metric is FOLD STABILITY. A width fitted so tight that the ratio swings
    # across folds is a width estimated from noise, and that is testable here.
    print(f"\n{'estimator':>10} {'quantity':>10} {'category':>24} "
          f"{'median ratio by fold':>44} {'spread':>8}")
    for (kind, quantity), g in held.groupby(["kind", "quantity"]):
        for cat, gg in g.groupby("category"):
            per = gg.groupby("fold")["ratio"].median()
            lo, hi = per.min(), per.max()
            print(f"{kind:>10} {quantity:>10} {cat:>24} "
                  f"{'  '.join(f'{v:7.3g}' for v in per):>44} {hi / lo:8.2f}x")

    # THE CALIBRATION CHECK. A bin shipping 1e4 must be right far more often than one
    # shipping 20, or its confidence is not evidence.
    print(f"\n{'estimator':>10} {'quantity':>10} {'category':>24} {'median ratio':>13} "
          f"{'decided':>8} {'accuracy':>9} {'n':>6}")
    for (kind, quantity), g in held.groupby(["kind", "quantity"]):
        for cat, gg in g.groupby("category"):
            call = (gg["dg"] > DECADE).astype(int) - (gg["dg"] < -DECADE).astype(int)
            dec = call != 0
            acc = ((call[dec] == gg["truth"][dec]).mean() if dec.sum() else float("nan"))
            mag = np.exp(np.abs(gg["dg"]) / RT).median()
            print(f"{kind:>10} {quantity:>10} {cat:>24} {mag:13.4g} "
                  f"{int(dec.sum()):8d} {acc:9.2%} {len(gg):6d}")

    # THE ONLY COMPARISON THE TWO SCALES CAN BOTH LOSE. A standard-scale prior emits one
    # number per category, so its held-out accuracy is a four-valued statistic and its sign
    # agrees with its own bin's label by construction -- it cannot be wrong, and a 100% there
    # is saturation rather than evidence. The transported prior emits one number per
    # REACTION, so it is scored on a strictly harder task and a raw accuracy against it is
    # not a like-for-like number.
    #
    # What IS like-for-like is the rows where the two disagree about the call. Both are
    # answering the same question about the same reaction there, and the curated label
    # decides between them.
    if len(set(held["quantity"])) > 1:
        wide = held[held["kind"] == "robust"].pivot_table(
            index=["fold", "category"], columns="quantity", values="dg", aggfunc=list)
        rows = []
        for (fold, cat), r in wide.iterrows():
            for triple in zip(r["standard"], r["physiological"], r["by_label"]):
                rows.append((cat, *triple, 1 if cat in REVERSE else -1))
        d = pd.DataFrame(rows, columns=["category", "standard", "physiological",
                                        "by_label", "truth"])
        for c in ("standard", "physiological", "by_label"):
            d[c + "_call"] = ((d[c] > DECADE).astype(int) - (d[c] < -DECADE).astype(int))
        for c in ("physiological", "by_label"):
            moved = d[d["standard_call"] != d[c + "_call"]]
            gained = int(((moved[c + "_call"] == moved["truth"])
                          & (moved["standard_call"] != moved["truth"])).sum())
            lost = int(((moved["standard_call"] == moved["truth"])
                        & (moved[c + "_call"] != moved["truth"])).sum())
            print(f"\n[holdout] robust width, {c} vs standard: {len(moved)} held-out "
                  f"reactions where the call differs -- {gained} newly right, {lost} newly "
                  f"wrong, {len(moved) - gained - lost} hedged either way")
            if len(moved):
                print("[holdout]   by category: "
                      + ", ".join(f"{k}={n}" for k, n
                                  in moved["category"].value_counts().items()))
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""Held-out validation: does the concentration term agree with curated physiology better?

The bake already carries MetaCyc/BioCyc's curated direction call for 16,300 reactions, and
the vocabulary is the point: `PHYSIOL-LEFT-TO-RIGHT` means *a curator asserted this runs
left-to-right under physiological conditions*, as distinct from `LEFT-TO-RIGHT`, which is a
statement about the equation. That is exactly the quantity a concentration term claims to
recover, written down by somebody who was not using thermodynamics to get it.

Scored on the reactions carrying BOTH a thermodynamic vote and a curated category, which
makes the category held-out with respect to the number: it enters the bake only as a
separate vote in the combiner, never into dG_raw. The comparison is therefore
standard-state dG vs concentration-corrected dG against the same labels.

REVERSIBLE is excluded from the accuracy score and reported separately. A curator calling a
reaction reversible is not a direction to get right, and counting it would let a method
score by being uninformative.
"""
from __future__ import annotations

import argparse
import math

import pandas as pd

RT = 8.314e-3 * 298.15
DECADE = RT * math.log(10.0)

FORWARD = {"LEFT-TO-RIGHT", "PHYSIOL-LEFT-TO-RIGHT"}
REVERSE = {"RIGHT-TO-LEFT", "PHYSIOL-RIGHT-TO-LEFT"}


def call(dg, band):
    if pd.isna(dg):
        return "silent"
    if abs(dg) < band:
        return "reversible"
    return "forward" if dg < 0 else "reverse"


def score(d, col, band):
    got = d[col].map(lambda x: call(x, band))
    right = (got == d["truth"]).sum()
    wrong = ((got != d["truth"]) & (got != "reversible")).sum()
    hedge = (got == "reversible").sum()
    return right, wrong, hedge


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--blast", required=True)
    ap.add_argument("--annotation", required=True)
    ap.add_argument("--band", type=float, default=DECADE)
    a = ap.parse_args(argv)

    df = pd.read_parquet(a.blast)
    df = df[df["dir_tier"].isin([1, 2])].copy()        # a thermo vote exists
    print(f"[val] {len(df):,} reactions with a thermodynamic vote")

    # The category is not in blast_radius; re-read it from the annotation the same run used.
    ap2 = pd.read_parquet(a.annotation)[["mnxr", "biocyc_category", "biocyc_source"]]
    df = df.merge(ap2, on="mnxr", how="left")
    df["truth"] = df["biocyc_category"].map(
        lambda c: "forward" if c in FORWARD else ("reverse" if c in REVERSE else None))
    lab = df[df["truth"].notna()].copy()
    print(f"[val] {len(lab):,} of those carry a directional curated category "
          f"({(lab['biocyc_category'].str.startswith('PHYSIOL')).sum():,} of them PHYSIOL-*)")
    print(f"[val] band = {a.band:.2f} kJ/mol (one decade of conductance)\n")

    groups = [("all curated-directional", lab),
              ("PHYSIOL-* only", lab[lab["biocyc_category"].str.startswith("PHYSIOL")]),
              ("non-PHYSIOL only", lab[~lab["biocyc_category"].str.startswith("PHYSIOL")]),
              ("tier 1 (measured dG)", lab[lab["dir_tier"] == 1]),
              ("tier 2 (predicted dG)", lab[lab["dir_tier"] == 2]),
              ("fully measured conc", lab[lab["n_default"] == 0]),
              ("skew > 1 decade", lab[lab["skew"].abs() > DECADE])]

    hdr = (f"{'population':<26} {'n':>6} | {'STD ok':>7} {'wrong':>6} {'hedge':>6} "
           f"| {'CONC ok':>7} {'wrong':>6} {'hedge':>6} | {'decided acc':>12}")
    print(hdr)
    print("-" * len(hdr))
    for name, d in groups:
        if not len(d):
            continue
        r0, w0, h0 = score(d, "dG_raw", a.band)
        r1, w1, h1 = score(d, "dG_conc", a.band)
        acc0 = r0 / (r0 + w0) if (r0 + w0) else float("nan")
        acc1 = r1 / (r1 + w1) if (r1 + w1) else float("nan")
        print(f"{name:<26} {len(d):6,} | {r0:7,} {w0:6,} {h0:6,} "
              f"| {r1:7,} {w1:6,} {h1:6,} | {acc0:5.1%} -> {acc1:5.1%}")
    print()

    # A method that simply flips more calls can gain accuracy by luck; the paired
    # disagreement is what says whether the change is doing work.
    c0 = lab["dG_raw"].map(lambda x: call(x, a.band))
    c1 = lab["dG_conc"].map(lambda x: call(x, a.band))
    moved = lab[c0 != c1]
    m0, m1 = c0[c0 != c1], c1[c0 != c1]
    gained = int(((m1 == moved["truth"]) & (m0 != moved["truth"])).sum())
    lost = int(((m0 == moved["truth"]) & (m1 != moved["truth"])).sum())
    print(f"[val] {len(moved):,} curated reactions where the CALL changes: "
          f"{gained:,} newly right, {lost:,} newly wrong, "
          f"{len(moved) - gained - lost:,} wrong or hedged both ways")
    if gained + lost:
        print(f"[val] net {gained - lost:+,}  (a coin flip on {len(moved):,} would give ~0)")

    flipped = lab[(c0.isin(["forward", "reverse"])) & (c1.isin(["forward", "reverse"]))
                  & (c0 != c1)]
    f0, f1 = c0[flipped.index], c1[flipped.index]
    fg = int(((f1 == flipped["truth"])).sum())
    print(f"[val] of {len(flipped):,} OUTRIGHT reversals (a decided call to the opposite "
          f"decided call), {fg:,} land on the curated side ({fg/max(1,len(flipped)):.1%})")


if __name__ == "__main__":
    main()

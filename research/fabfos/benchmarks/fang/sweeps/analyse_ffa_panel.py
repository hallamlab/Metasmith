#!/usr/bin/env python3
"""Does the ECSPr score track the measured titer?

Four numbers carry the answer and they are reported in this order, because each
one bounds what the next is worth:

  1. THE GATE -- the null's spread over the controls'. The controls are masks that
     reach no atom-mapped reaction, so they must return the baseline exactly and
     their spread is the solver's floor. A ratio near 1 means every z below is
     noise over noise and nothing after this line means anything.
  2. REACH -- how many tested clones the method could see at all. Not a footnote:
     if the genes that moved the phenotype are the ones with no metabolic edge,
     that is the finding, and it is the paper's own conclusion arriving by a
     different route.
  3. RANK CORRELATION between z and the measured fold change, over the clones the
     method can see.
  4. SEPARATION between the strains the paper scored as an increase and the rest,
     as the probability a hit outranks a non-hit -- a plain AUC, computed off the
     ranks rather than assumed normal.

And one control on the whole thing: the correlation between z and the clone's
REACTION COUNT. Under a two-point probe, Rayleigh monotonicity says any addition
raises the effective conductance, so a clone contributing thirty-two reactions
outranks one contributing a single reaction by construction. If z tracks reaction
count strongly and the titer weakly, the method is measuring how big a gene is,
and that is the result. The size-matched z (`z_stratum`) is reported beside the
global one for the same reason.
"""
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
OUT = HERE / "out"


def spearman(a: pd.Series, b: pd.Series) -> tuple:
    ok = a.notna() & b.notna()
    n = int(ok.sum())
    if n < 4:
        return float("nan"), n
    return float(a[ok].rank().corr(b[ok].rank())), n


def auc(pos: np.ndarray, neg: np.ndarray) -> tuple:
    if not len(pos) or not len(neg):
        return float("nan"), len(pos), len(neg)
    gt = (pos[:, None] > neg[None, :]).sum()
    eq = (pos[:, None] == neg[None, :]).sum()
    return float((gt + 0.5 * eq) / (len(pos) * len(neg))), len(pos), len(neg)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--run", type=Path, required=True,
                    help="a run directory, e.g. research/fabfos/benchmarks/fang/out/n1000")
    ap.add_argument("--probe", default="two-point")
    ap.add_argument("--readout", default="total")
    ap.add_argument("--out", type=Path, default=None)
    args = ap.parse_args()
    out = args.out or args.run

    gate = pd.read_csv(args.run / f"{args.probe}_gate.tsv", sep="\t")
    scored = pd.read_parquet(args.run / f"{args.probe}_scored.parquet")
    reach = pd.read_csv(OUT / "condition_reach.tsv", sep="\t", keep_default_na=False)

    print(f"=== 1. the gate ({args.probe}) ===")
    cols = [c for c in ("readout", "n_controls", "control_sd", "control_max_abs",
                        "null_n", "null_sd", "null_over_control") if c in gate.columns]
    with pd.option_context("display.width", 200, "display.float_format",
                           lambda v: f"{v:.4g}"):
        print(gate[cols].to_string(index=False))

    s = scored[(scored["probe"] == args.probe) & (scored["readout"] == args.readout)]
    reach = reach.drop(columns=["is_control", "control_kind"], errors="ignore")
    df = s.merge(reach, on="condition_id", how="left")
    df["measured_dir"] = df["measured_dir"].fillna("")
    real = df[df["measured_dir"] != ""].copy()
    seen = real[~real["is_control"].astype(bool)]
    strict = real[real["mappable"].astype(str).str.lower().isin(["true", "1"])]

    print(f"\n=== 2. reach (readout={args.readout}) ===")
    n_up = int((real["measured_dir"] == "+").sum())
    print(f"{len(real)} measured conditions scored")
    print(f"  ECSPr can move {len(seen)}; {len(real) - len(seen)} return the "
          f"baseline bit-for-bit and are invisible to it")
    print(f"  of those, only {len(strict)} survive the tier's stricter universe, "
          f"which drops transport -- the difference is transporter clones")
    inv_up = real[(real["measured_dir"] == "+")
                  & ~real["condition_id"].isin(seen["condition_id"])]
    print(f"  of the {n_up} increases the paper scores, {len(inv_up)} are "
          f"invisible: {', '.join(sorted(inv_up['clones'])) or 'none'}")

    print(f"\n=== 3. rank correlation, z vs measured fold change ===")
    for label, sub in (("all scored", real), ("ECSPr can move", seen), ("non-transport", strict)):
        for zcol in ("z", "z_stratum"):
            r, n = spearman(sub[zcol], sub["fold_change"])
            print(f"  {label:18s} {zcol:10s} rho={r:+.3f}  n={n}")

    print(f"\n=== 4. separation, increases vs the rest ===")
    for label, sub in (("all scored", real), ("ECSPr can move", seen), ("non-transport", strict)):
        for zcol in ("z", "z_stratum"):
            pos = sub[sub["measured_dir"] == "+"][zcol].dropna().to_numpy(float)
            neg = sub[sub["measured_dir"] != "+"][zcol].dropna().to_numpy(float)
            a, np_, nn = auc(pos, neg)
            print(f"  {label:18s} {zcol:10s} AUC={a:.3f}  ({np_} up vs {nn} other)")

    print(f"\n=== the size control: z vs the clone's reaction count ===")
    for label, sub in (("all scored", real), ("ECSPr can move", seen), ("non-transport", strict)):
        for zcol in ("z", "z_stratum"):
            r, n = spearman(sub[zcol], sub["n_ecspr_reactions"])
            print(f"  {label:18s} {zcol:10s} rho={r:+.3f}  n={n}")

    one = seen[seen["n_ecspr_reactions"] == 1]
    print(f"\n=== matched: clones adding exactly one atom-mapped reaction "
          f"(n={len(one)}) ===")
    if len(one):
        r, n = spearman(one["delta"], one["fold_change"])
        print(f"  delta vs fold change   rho={r:+.3f}  n={n}")
        pos = one[one["measured_dir"] == "+"]["delta"].dropna().to_numpy(float)
        neg = one[one["measured_dir"] != "+"]["delta"].dropna().to_numpy(float)
        a, npos, nneg = auc(pos, neg)
        print(f"  increases vs the rest  AUC={a:.3f}  ({npos} up vs {nneg} other)")
        with pd.option_context("display.float_format", lambda v: f"{v:.4g}"):
            print(one[["clones", "measured_dir", "fold_change", "delta", "z"]]
                  .sort_values("delta", ascending=False).to_string(index=False))

    keep = ["condition_id", "clones", "figure", "measured_dir", "ffa_mg_L",
            "n_ecspr_reactions",
            "fold_change", "n_reactions", "n_atom_mapped", "mappable", "value",
            "baseline", "delta", "z", "pct_rank", "p_emp", "q_bh", "p_floor",
            "z_stratum", "p_stratum", "stratum", "null_n", "null_sd"]
    tbl = real[[c for c in keep if c in real.columns]].sort_values(
        "z", ascending=False)
    path = out / f"{args.probe}_{args.readout}_ranked.tsv"
    tbl.to_csv(path, sep="\t", index=False)

    print(f"\n=== the ten highest z, and the paper's increases ===")
    show = ["clones", "figure", "measured_dir", "fold_change", "n_atom_mapped",
            "delta", "z", "z_stratum", "pct_rank"]
    with pd.option_context("display.width", 220, "display.float_format",
                           lambda v: f"{v:.4g}"):
        print(tbl[show].head(10).to_string(index=False))
        print("\n-- every condition the paper scores as an increase --")
        print(tbl[tbl["measured_dir"] == "+"][show].to_string(index=False))
    print(f"\nwrote {path}")


if __name__ == "__main__":
    main()

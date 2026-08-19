#!/usr/bin/env python3
"""How the belief allocation ranks de novo nominations, and what carries the top of it.

Scores the E. coli K-12 de novo GPR table against iML1515's own reactome. AUROC is
REPORTED, NOT GATED: the truth set is confounded against any change that discounts
repeated evidence, because the reactions with the most repeated annotations are the
well-studied ones and those are exactly the ones curated into iML1515. The
unconfounded number here is `single-assertion fraction of the top 100` -- what share
of the highest-conductance reactions rest on one (unit, channel, evidence_id) with no
corroboration at all.

With `--sweep`, jointly sweeps the pooling constants (lam0, lam1, tau) and prints the
same panel per point. tau and lam1 interact -- tau sets where saturation begins, lam1
the ceiling -- so they are swept together, never one at a time. A point where lam1 is
large and tau tiny is a hard count of distinct assertions wearing a sigmoid; recognise
it rather than shipping it.
"""
from __future__ import annotations

import argparse
import itertools
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / "src"))

from ecspr.model import evidence as _ev  # noqa: E402
from ecspr.model.gpr import _normalise, weights_from_rows  # noqa: E402

GPR = ROOT / "data/fabfos/runs/e_coli_k12/gpr"
WATCH = "MNXR172198"    # 65 ORFs, one evidence_id (EC 2.7.13.3), no corroboration


def auroc(truth: np.ndarray, score: np.ndarray) -> float:
    r = pd.Series(score).rank().to_numpy()
    y = np.asarray(truth, bool)
    n1, n0 = int(y.sum()), int((~y).sum())
    return float((r[y].sum() - n1 * (n1 + 1) / 2) / (n1 * n0))


def panel(E: pd.Series, meta: pd.DataFrame, name: str) -> dict:
    d = meta.join(E.rename("E"), how="inner").dropna(subset=["E"])
    d = d[d.in_universe]
    order = d.E.sort_values(ascending=False)
    top100 = order.index[:100]
    q = d.E.quantile([0.5, 0.9, 0.99])
    watch_rank = (int(list(order.index).index(WATCH)) + 1) if WATCH in order.index else None
    out = dict(
        name=name, n=len(d),
        E_max=float(d.E.max()), E_min=float(d.E.min()),
        E_p50=float(q[0.5]), E_p90=float(q[0.9]), E_p99=float(q[0.99]),
        spread=float(d.E.max() / d.E.min()),
        auroc=auroc(d.truth.to_numpy(), d.E.to_numpy()),
        p100=float(d.truth.reindex(top100).mean()),
        p500=float(d.truth.reindex(order.index[:500]).mean()),
        single_top100=float((d.n_assert.reindex(top100) == 1).mean()),
        multichannel_top100=float((d.n_channel.reindex(top100) > 1).mean()),
        watch_rank=watch_rank,
        watch_E=float(d.E.get(WATCH, np.nan)),
    )
    print(f"  {name:34s} max {out['E_max']:12.9g}  spread {out['spread']:9.3g}  "
          f"AUROC {out['auroc']:.3f}  P@100 {out['p100']:.2f}  P@500 {out['p500']:.2f}  "
          f"1-assert@100 {out['single_top100']:.0%}  >1-chan@100 {out['multichannel_top100']:.0%}  "
          f"{WATCH} rank {out['watch_rank']} E={out['watch_E']:.4g}")
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sweep", action="store_true")
    ap.add_argument("--out", type=Path)
    args = ap.parse_args()

    dn = pd.read_parquet(GPR / "gpr_denovo.parquet")
    gem = pd.read_parquet(GPR / "gpr_gem.parquet")
    norm = _normalise(dn)

    meta = pd.DataFrame({
        "in_universe": dn.groupby("mnxr")["in_atom_universe"].any(),
        "n_orf": dn.groupby("mnxr")["feature_id"].nunique(),
        "n_channel": dn.groupby("mnxr")["channel"].nunique(),
        "n_assert": norm.groupby("mnxr").apply(
            lambda g: g.drop_duplicates(["unit_id", "channel", "intermediate_id"]).shape[0],
            include_groups=False),
    })
    meta["in_universe"] = meta.in_universe.fillna(False).astype(bool)
    meta["truth"] = meta.index.isin(set(gem.mnxr.astype(str)))
    print(f"K-12 de novo: {len(meta)} MNXR, {int(meta.in_universe.sum())} in universe, "
          f"{int(meta[meta.in_universe].truth.sum())} of those in iML1515")

    rows = []
    print("\n== as built ==")
    rows.append(panel(pd.Series(weights_from_rows(dn, "belief")), meta, "belief (current)"))

    if args.sweep:
        contrib = _ev.nomination_contributions(norm)
        print("\n== sweep (lam0, lam1, tau) ==")
        grid = itertools.product((-4.0, -3.0, -2.0), (1.0, 2.0, 3.0, 4.0),
                                 (0.1, 0.25, 0.5, 1.0))
        for lam0, lam1, tau in grid:
            E = _ev.pool_logodds(contrib, lam0=lam0, lam1=lam1, tau=tau)
            rows.append(panel(E, meta, f"lam0={lam0:g} lam1={lam1:g} tau={tau:g}"))

    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(rows).to_csv(args.out, index=False)
        print(f"\nwrote {args.out}")


if __name__ == "__main__":
    main()

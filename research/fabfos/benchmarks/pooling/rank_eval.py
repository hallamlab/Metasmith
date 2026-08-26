#!/usr/bin/env python3
# How the belief allocation ranks de novo nominations, and what carries the top of it.
#
# Scores the E. coli K-12 de novo GPR table against iML1515's own reactome. AUROC is
# REPORTED, NOT GATED: the truth set is confounded against any change that discounts
# repeated evidence, because the reactions with the most repeated annotations are the
# well-studied ones and those are exactly the ones curated into iML1515. The
# unconfounded number here is `single-assertion fraction of the top 100` -- what share
# of the highest-conductance reactions rest on one (unit, channel, evidence_id) with no
# corroboration at all.
#
# With `--sweep`, sweeps the pooling constants (lam0, tau) and prints the same panel per
# point. `lam1` IS NOT SWEPT ANY MORE AND CANNOT BE: what one assertion is worth is now
# the row's own calibrated `llr`, measured per lane rather than chosen. What is left to
# choose is where saturation begins, and a tiny tau is a hard count of distinct
# assertions wearing a sigmoid -- recognise it rather than shipping it.
#
# `--before` adds the pre-calibration weighting on the OLD table beside the sweep, from
# a frozen copy in the calibration study. The two tables carry the same rows, so the
# pair isolates the weighting.
#
# **CAUTION** THE UNIVERSE FILTER DOES NOT COME FROM THE TABLE. `in_atom_universe` is
# null on every row of every de-novo table this reads -- it is on the schema's keep list
# and no producer has ever populated it -- so `dn.groupby(...).any()` returned False
# everywhere and this panel silently scored ZERO reactions. It is computed here from
# `ecspr::atom_pairs` instead, which is what the column was meant to cache.
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

sys.path.insert(0, str(ROOT / "src/fabfos/build_references/resources/buildlib"))
import bench_universe as bu  # noqa: E402
sys.path.insert(0, str(ROOT / "research/fabfos/annotation_lanes/calibration"))
import _legacy  # noqa: E402

GPR = ROOT / "data/fabfos/runs/e_coli_k12/gpr"
BAKE = ROOT / "data/fabfos/processed/metabolism_bake"
MNX = ROOT / "data/fabfos/originals/metanetx"
WATCH = "MNXR172198"    # 65 ORFs, one evidence_id (EC 2.7.13.3), no corroboration
# float64 saturates the sigmoid at exactly 1.0 once the pooled log-odds pass this, so
# every reaction above it ties with every other one above it.
TIE_POINT = 37.0


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
    ap.add_argument("--denovo", type=Path, default=GPR / "gpr_denovo.parquet",
                    help="the calibrated table; the default is the published one")
    ap.add_argument("--before", type=Path,
                    help="a pre-calibration table for the same host, scored under the "
                         "frozen legacy weighting beside the sweep")
    ap.add_argument("--out", type=Path)
    args = ap.parse_args()

    dn = pd.read_parquet(args.denovo)
    gem = pd.read_parquet(GPR / "gpr_gem.parquet")
    norm = _normalise(dn)

    meta = pd.DataFrame({
        "in_universe": dn.groupby("mnxr")["in_atom_universe"].any(),
        # `orf`, not `feature_id`: the latter is built by `_normalise` and has never
        # been a column of a GPR table, so this line raised on every table it was ever
        # pointed at.
        "n_orf": dn.groupby("mnxr")["orf"].nunique(),
        "n_channel": dn.groupby("mnxr")["channel"].nunique(),
        "n_assert": norm.groupby("mnxr").apply(
            lambda g: g.drop_duplicates(["genome_unit", "channel", "intermediate_id"]).shape[0],
            include_groups=False),
    })
    universe, ustats = bu.atom_universe(
        BAKE / "vocab.parquet", BAKE / "atom_pairs.parquet",
        exclude=bu.transport_mnxrs(bu.reac_prop_path(MNX)))
    print(bu.universe_line(ustats, "rank_eval"))
    meta["in_universe"] = meta.index.isin(universe)
    meta["truth"] = meta.index.isin(set(gem.mnxr.astype(str)))
    print(f"K-12 de novo: {len(meta)} MNXR, {int(meta.in_universe.sum())} in universe, "
          f"{int(meta[meta.in_universe].truth.sum())} of those in iML1515")

    rows = []
    print("\n== as built ==")
    rows.append(panel(pd.Series(weights_from_rows(dn, "belief")), meta, "belief (current)"))

    if args.before:
        old = _legacy.normalise(pd.read_parquet(args.before))
        oc = _legacy.contributions(old)
        print("\n== before: the pre-calibration weighting, frozen ==")
        rows.append(panel(_legacy.pool(oc), meta, "legacy lam1=1 tau=0.25"))
        lo = _legacy.pooled_logodds(oc)
        print(f"  tied at the float64 ceiling: {int((lo >= TIE_POINT).sum()):,} "
              f"of {len(lo):,} reactions  (max log-odds {lo.max():.4g})")

    if args.sweep:
        contrib = _ev.nomination_contributions(norm)
        print("\n== sweep (lam0, tau) -- lam1 is the calibrated llr and is not a knob ==")
        for lam0, tau in itertools.product((-4.0, -3.0, -2.0),
                                           (0.1, 0.25, 0.5, 1.0, 2.0)):
            E = _ev.pool_logodds(contrib, lam0=lam0, tau=tau)
            r = panel(E, meta, f"lam0={lam0:g} tau={tau:g}")
            lo = _legacy.new_pooled_logodds(contrib, lam0=lam0, tau=tau)
            r["n_tied"] = int((lo >= TIE_POINT).sum())
            r["max_logodds"] = float(lo.max())
            print(f"      tied at the float64 ceiling: {r['n_tied']:,} of {len(lo):,}"
                  f"  (max log-odds {r['max_logodds']:.4g})")
            rows.append(r)

    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(rows).to_csv(args.out, index=False)
        print(f"\nwrote {args.out}")


if __name__ == "__main__":
    main()

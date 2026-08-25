#!/usr/bin/env python3
"""ddg (lpxP) overexpression: delta draw across every metabolite, universal ground.

Two solves, glucose source, universal leak -- base (host GEM, unperturbed) and pert
(ddg's reaction folded up). `measure_leak`'s draw dict already covers every metabolite
that became a node, so subtracting the two gives one number per metabolite: how much of
the leak that species pulled in shifted when ddg was overexpressed. This is a specificity
read, not a no-op floor check -- see the README note below for what it does and does not
establish.

ddg is the historic gene-name synonym for lpxP (b2378, confirmed against
NC_000913.3.gbk's `/gene_synonym="ddg; ECK2374"`); the eydallin cohort's gene_norm column
carries `ddg`, but the host GEM's `feature_name` column only carries `lpxP`, so a lookup
by gene name alone misses it. Its reaction (`MNXR97903`) is therefore ALREADY in the host
GEM's reaction set at weight 1.0 -- this is an ordinary fold-change on an existing edge,
not an edge addition, and base/pert share the identical node and edge SET (only one
weight changes), so every metabolite that is a node in one solve is a node in the other.
No missing-key handling needed, unlike a true gene-not-in-GEM addition.

Base graph never touches glycogen through ddg directly: lpxP is lipid-A biosynthesis,
structurally unrelated to the glg operon's neighbourhood. Both ddg's own substrate/
cosubstrate/product (Kdo2-lipid IVA, palmitoleoyl-ACP, the acylated product) are each
independently anchored by OTHER host reactions (lpxL/lpxM's family), so nothing here is a
dangling edge.

    mamba run -n ecspr python main/benchmarks/eydallin/ddg_delta_panel.py --fold 2.0
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[5]
sys.path.insert(0, str(ROOT / "src"))

from ecspr.model.build import load_pairs, load_direction_ratios, graph_from_pairs  # noqa: E402
from ecspr.model.graph import Terminal, measure_leak                               # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import bake_pairs  # noqa: E402

ATOM_PAIRS = bake_pairs.atom_pairs()
HOST_GEM = ROOT / "data/fabfos/runs/e_coli_k12/gpr/gpr_gem.parquet"
OUT_DIR = Path(__file__).resolve().parents[1] / "cache"
OUT_DIR.mkdir(parents=True, exist_ok=True)

DDG_RXN = "MNXR97903"
SOURCE_MNXM = "MNXM1364061"
GLYCOGEN_MNXM = "MNXM738130"


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--fold", type=float, default=2.0,
                   help="conductance fold-change on MNXR97903 (ddg/lpxP)")
    p.add_argument("--orientation", choices=("as_written", "reversed"), default="as_written")
    p.add_argument("--element", default="C")
    p.add_argument("--leak", type=float, default=1e-6)
    args = p.parse_args()

    pairs = load_pairs(ATOM_PAIRS, element=args.element)
    ratios = load_direction_ratios(bake_pairs.direction_ratios())
    host = pd.read_parquet(HOST_GEM)
    base_w = {m: 1.0 for m in host["mnxr"].dropna().astype(str).unique()}

    if DDG_RXN not in base_w:
        raise SystemExit(f"[ddg-panel] {DDG_RXN} unexpectedly absent from the host GEM's "
                         f"reaction set -- the synonym resolution this script relies on "
                         f"may be stale, re-check against NC_000913.3.gbk")
    pert_w = dict(base_w)
    pert_w[DDG_RXN] = base_w[DDG_RXN] * args.fold
    print(f"[ddg-panel] ddg/lpxP {DDG_RXN}: weight {base_w[DDG_RXN]} -> {pert_w[DDG_RXN]} "
          f"(fold {args.fold})", file=sys.stderr)

    def solve_(weights, tag):
        g = graph_from_pairs(pairs, args.element, weights, ratios, orientation=args.orientation)
        print(f"[ddg-panel] {tag}: {g.n:,} nodes / {g.m:,} edges from "
              f"{g.meta['n_reactions_used']:,} reactions (AAM gap {g.meta['n_aam_gap']:,})",
              file=sys.stderr)
        src = Terminal.metabolite(g, SOURCE_MNXM, label="source")
        if src.missing:
            raise SystemExit(f"[ddg-panel] source {SOURCE_MNXM} absent from the built graph")
        r = measure_leak(g, src, [], leak=args.leak)
        return g, r

    g_base, r_base = solve_(base_w, "base")
    g_pert, r_pert = solve_(pert_w, "pert")

    base_keys, pert_keys = set(r_base["draw"]), set(r_pert["draw"])
    only_base, only_pert = base_keys - pert_keys, pert_keys - base_keys
    if only_base or only_pert:
        print(f"[ddg-panel] WARNING: node sets differ -- {len(only_base)} only in base, "
              f"{len(only_pert)} only in pert", file=sys.stderr)

    rows = []
    for m in sorted(base_keys & pert_keys):
        db, dp = r_base["draw"][m], r_pert["draw"][m]
        rows.append((m, db, dp, dp - db))
    df = pd.DataFrame(rows, columns=["mnxm", "draw_base", "draw_pert", "delta"])

    if GLYCOGEN_MNXM not in set(df.mnxm):
        raise SystemExit(f"[ddg-panel] {GLYCOGEN_MNXM} (glycogen) never became a node in "
                         f"this build -- cannot mark it on the panel")

    out = OUT_DIR / f"ddg_delta_panel_fold{args.fold}_{args.orientation}_{args.element}.parquet"
    df.to_parquet(out)
    print(f"\n[ddg-panel] {len(df)} metabolites with a delta; wrote {out}", file=sys.stderr)

    gly = df[df.mnxm == GLYCOGEN_MNXM].iloc[0]
    rank = int((df.delta.abs() >= abs(gly.delta)).sum())
    print(f"[ddg-panel] glycogen delta = {gly.delta:.6g} (base {gly.draw_base:.6g}, "
          f"pert {gly.draw_pert:.6g}); rank {rank} of {len(df)} by |delta| "
          f"({rank/len(df):.4%} at or above it)", file=sys.stderr)


if __name__ == "__main__":
    main()

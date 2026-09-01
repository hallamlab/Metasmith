#!/usr/bin/env python3
# End-to-end sanity check for the belief path: ag1 alone vs ag1 + one clone.
#
# Every other script in this directory hands `graph_from_pairs` a flat `{mnxr: 1.0}`
# over the curated k12 GEM, so none of them ever reaches
# `ecspr.model.gpr.weights_from_rows(..., "belief")`. This one does: weights come from
# the de novo `e_coli_ag1` proteome through `condition_weights`, and the perturbation is
# a clone's rows being selected alongside the host's -- the mask, two units, the belief
# allocation, the bake and the solve, in one run.
#
# Run it before and after any change to the evidence allocation and diff the output.
# Node/edge counts must be IDENTICAL across such a change: an allocation change moves
# the value attached to a reaction, never which reactions are in the graph.
#
#   --cohort   measure every eydallin condition, not just glgC, and rank them
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[5]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import bake_pairs  # noqa: E402
from ecspr.model.build import graph_from_pairs, load_direction_ratios, load_pairs  # noqa: E402
from ecspr.model.gpr import condition_weights, load_gpr  # noqa: E402
from ecspr.model.graph import Terminal, solve  # noqa: E402

HOST_GPR = ROOT / "data/fabfos/runs/e_coli_ag1/gpr/gpr_denovo.parquet"
CLONE_GPR = ROOT / "data/fabfos/runs/eydallin_clones/gpr/gpr_denovo.parquet"

HOST_UNIT = "proteome"
SOURCE = "MNXM1364061"    # D-glucose
TARGET = "MNXM738130"     # glycogen, the BiGG species iML1515 carries


def measure(pairs, ratios, df, label, **mask):
    w, cov = condition_weights(df, weighting="belief", background_column="unit_id",
                               background_values=(HOST_UNIT,), **mask)
    g = graph_from_pairs(pairs, "C", w, ratios)
    src = Terminal.metabolite(g, SOURCE, label="glucose")
    snk = Terminal.metabolite(g, TARGET, label="glycogen")
    if src.missing or snk.missing:
        raise SystemExit(f"{label}: terminal absent (src={src.missing} snk={snk.missing})")
    s = solve(g, src, snk)
    return dict(label=label, n_rows=cov["n_rows"], n_units=cov["n_units"],
                n_reactions=cov["n_reactions"], n_used=int(g.meta["n_reactions_used"]),
                nodes=int(g.n), edges=int(g.m),
                E_sum=float(sum(w.values())), E_max=float(max(w.values())),
                conductance=float(s.total))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cohort", action="store_true")
    ap.add_argument("--out", type=Path)
    args = ap.parse_args()

    df = load_gpr([HOST_GPR, CLONE_GPR])
    ratios = load_direction_ratios(bake_pairs.direction_ratios())
    pairs = load_pairs(bake_pairs.atom_pairs(), element="C")

    base = measure(pairs, ratios, df, "base (ag1 alone)")
    print(f"base   rows={base['n_rows']:6d} units={base['n_units']} "
          f"reactions={base['n_reactions']:5d} used={base['n_used']:5d} "
          f"nodes={base['nodes']:,} edges={base['edges']:,}")
    print(f"       sum(E)={base['E_sum']:.4f}  max(E)={base['E_max']:.6g}  "
          f"conductance={base['conductance']:.6g}")

    conds = ["eydallin:glgC"]
    if args.cohort:
        conds = sorted(df.condition_id.dropna().unique())

    rows = []
    for cond in conds:
        p = measure(pairs, ratios, df, cond, mask_column="condition_id",
                    mask_values=(cond,))
        p["d_abs"] = p["conductance"] - base["conductance"]
        p["d_rel"] = p["d_abs"] / base["conductance"]
        rows.append(p)
        if not args.cohort:
            print(f"\n{cond}")
            print(f"       rows={p['n_rows']:6d} units={p['n_units']} "
                  f"reactions={p['n_reactions']:5d} used={p['n_used']:5d} "
                  f"nodes={p['nodes']:,} edges={p['edges']:,}")
            print(f"       sum(E)={p['E_sum']:.4f}  max(E)={p['E_max']:.6g}  "
                  f"conductance={p['conductance']:.6g}")
            print(f"       delta={p['d_abs']:+.6g}  relative={p['d_rel']:+.6%}")

    if args.cohort:
        t = pd.DataFrame(rows).sort_values("d_rel", ascending=False)
        t["rank"] = range(1, len(t) + 1)
        print(f"\n== {len(t)} conditions, ranked by relative delta ==")
        print(t[["rank", "label", "n_reactions", "conductance", "d_abs",
                 "d_rel"]].to_string(index=False))
        g = t[t.label == "eydallin:glgC"]
        if len(g):
            print(f"\nglgC rank {int(g['rank'].iloc[0])} of {len(t)}")

    if args.out:
        args.out.write_text(json.dumps({"base": base, "conditions": rows}, indent=2))
        print(f"\nwrote {args.out}")


if __name__ == "__main__":
    main()

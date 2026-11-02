#!/usr/bin/env python3
"""Which reaction delivers the carbon that arrives at glycogen, under a given bake.

The readout r9 is judged on. `fabfos/bench-eydallin` reported that most of the modelled
carbon reaches glycogen by running glycogen phosphorylase backwards, because MetaNetX
writes `MNXR145036` as G1P -> glycogen and the direction ensemble abstains, leaving an
explicit ratio of 1.0 -- a symmetric edge, and so a free synthesis route.

**Their published 0.568 / 0.328 / 0.105 was measured on r7 ratios.** Their decode cache
carries 53,939 reactions at ratio 1.0 against deployed r8's 44,095 and no stamp saying
which bake it came from; `scan_direction_caches.py` shows it. So the split is re-taken
here, against a bake this script names, and the result is stamped. Anything comparing r9
to the published figure without this re-baseline bills a two-generation delta to one fix.

The route-usage decomposition is `glycogen_cut.py::_route_usage` in that scope, kept in
step deliberately rather than imported -- their worktree carries uncommitted work and this
scope must be able to re-measure without it. The annotation block beside it is this
scope's own: the acceptance criteria are about tiers and ratios, not only about shares.

    PYTHONPATH=src mamba run -n msm python \\
        research/fabfos/benchmarks/direction_rescue/glycogen_delivery.py
    ... --bake data/fabfos/processed/metabolism_bake_r9   # aimed at a staged chunk
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[3]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(HERE.parent))

import bake_identity                                                          # noqa: E402
from ecspr.model.build import (load_pairs, load_direction_ratios,             # noqa: E402
                               graph_from_pairs)
from ecspr.model.graph import Terminal, solve                                 # noqa: E402

RUNS = ROOT / "data/fabfos/runs"
CACHE = HERE / "cache"
OUT = HERE / "baselines"

SOURCE_MNXM = "MNXM1364061"          # D-glucose, as the atom-pair table carries it
GLYCOGEN_MNXM = "MNXM738130"         # the BiGG species iML1515 uses

MODULE = {
    "MNXR145036": "glgP/malP, glycogen phosphorylase (written G1P -> glycogen)",
    "MNXR145038": "glgP/malP, phosphorylase (second accession)",
    "MNXR145046": "glgA, glycogen synthase",
    "MNXR145021": "glgB/glgX, branching -- no chain-length change (control)",
    "MNXR145639": "maltodextrin ladder n=7, already scored (the polymer anchor)",
}


def route_usage(graph, sink: str) -> pd.Series:
    # Net current into `sink` per reaction, from the base solve. Sums to the injected 1.0.
    #
    # A degradative enzyme carrying most of the arriving carbon means the model is running it
    # backwards to synthesise, which is the whole finding this baseline exists to track.
    sol = solve(graph, Terminal.metabolite(graph, SOURCE_MNXM),
                Terminal.metabolite(graph, sink))
    if not sol.total > 0:
        return pd.Series(dtype=float)
    oe, cur = sol.edge_currents()
    nodes = graph.nodes
    shares = graph.meta["edge_reactions"].groupby("edge").apply(
        lambda d: dict(zip(d.mnxr, d.gp)), include_groups=False).to_dict()
    net: dict[str, float] = {}
    for k, e in enumerate(oe):
        a, b = graph.edges[e]
        ma, mb = nodes[a][0], nodes[b][0]
        if (ma == sink) == (mb == sink):
            continue
        i = cur[k] if mb == sink else -cur[k]
        sh = shares.get(e, {})
        tot = sum(sh.values()) or 1.0
        for r, gp in sh.items():
            net[r] = net.get(r, 0.0) + i * gp / tot
    return pd.Series(net).sort_values(ascending=False)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--bake", type=Path, default=bake_identity.DEPLOYED)
    ap.add_argument("--host", default="e_coli_ag1")
    ap.add_argument("--element", default="C")
    ap.add_argument("--sink", default=GLYCOGEN_MNXM)
    ap.add_argument("--tag", default=None,
                    help="output basename (default: the bake directory's name)")
    ap.add_argument("--against", type=Path, default=None,
                    help="a second decoded direction_ratios.parquet to re-solve over the "
                         "SAME atom pairs, so a split delta is attributable to the "
                         "direction table and to nothing else")
    args = ap.parse_args()

    tag = args.tag or args.bake.name
    stamp = bake_identity.identity(args.bake)
    sub = CACHE / tag
    pairs = load_pairs(bake_identity.build_atom_pairs(sub / "atom_pairs_bake.parquet",
                                                      args.bake), element=args.element)
    ratios = load_direction_ratios(
        bake_identity.build_direction_ratios(sub / "direction_ratios.parquet", args.bake))

    gpr = pd.read_parquet(RUNS / args.host / "gpr" / "gpr_gem.parquet")
    gene = gpr.groupby(gpr.mnxr.astype(str)).feature_name.apply(
        lambda s: ",".join(sorted({x for x in s if isinstance(x, str) and x})[:8]))
    weights = {m: 1.0 for m in gpr.mnxr.dropna().astype(str).unique()}

    graph = graph_from_pairs(pairs, args.element, weights, ratios, with_provenance=True)
    use = route_usage(graph, args.sink)

    ann = pd.read_parquet(args.bake / "seams/direction_annotation.parquet")
    ann = ann.set_index("mnxr")

    OUT.mkdir(parents=True, exist_ok=True)
    rows = []
    for mnxr, share in use.items():
        a = ann.loc[mnxr] if mnxr in ann.index else None
        rows.append(dict(mnxr=mnxr, genes=gene.get(mnxr, ""), share=float(share),
                         ratio=None if a is None else float(a.ratio),
                         dir_tier=None if a is None else int(a.dir_tier),
                         dir_method=None if a is None else a.dir_method))
    df = pd.DataFrame(rows)
    out_tsv = OUT / f"glycogen_delivery_{tag}_{args.host}_{args.element}.tsv"
    df.to_csv(out_tsv, sep="\t", index=False)
    bake_identity.keep(out_tsv, args.bake)

    print(f"bake      {args.bake}\nstamp     {stamp}\n"
          f"graph     {graph.n:,} nodes, {graph.m:,} edges, "
          f"{graph.meta['n_reactions_used']:,} reactions used\n")
    print(f"delivery into {args.sink} (injected current = 1.0):")
    for r in rows:
        print(f"  {r['share']:+.4f}  {r['mnxr']}  tier {r['dir_tier']}  "
              f"ratio {r['ratio']:.6f}  {r['genes']}")

    print("\nthe glycogen module, whatever the solve used:")
    for mnxr, what in MODULE.items():
        if mnxr not in ann.index:
            print(f"  {mnxr}  ABSENT from the annotation  -- {what}")
            continue
        a = ann.loc[mnxr]
        print(f"  {mnxr}  tier {int(a.dir_tier)}  ratio {a.ratio:.6f}  "
              f"dG' {a.dG_prime:+.4f}  {a.dir_method:<14}  {what}")

    against = None
    if args.against is not None:
        alt = route_usage(graph_from_pairs(pairs, args.element, weights,
                                           load_direction_ratios(args.against),
                                           with_provenance=True), args.sink)
        against = dict(path=str(args.against),
                       stamp=bake_identity.stamp_of(args.against),
                       delivery={k: float(v) for k, v in alt.items()})
        print(f"\nagainst {args.against}"
              f"\n        stamp {against['stamp']}")
        for mnxr in sorted(set(use.index) | set(alt.index)):
            here, there = float(use.get(mnxr, 0.0)), float(alt.get(mnxr, 0.0))
            print(f"  {mnxr}  {there:+.4f} -> {here:+.4f}   ({here - there:+.4f})")

    meta = dict(bake=str(args.bake), stamp=stamp, host=args.host, element=args.element,
                sink=args.sink, source=SOURCE_MNXM,
                n_background=len(weights),
                n_reactions_used=int(graph.meta["n_reactions_used"]),
                delivery=rows, against=against,
                module={m: (None if m not in ann.index else
                            dict(ratio=float(ann.loc[m].ratio),
                                 dir_tier=int(ann.loc[m].dir_tier),
                                 dir_method=str(ann.loc[m].dir_method),
                                 dG_prime=float(ann.loc[m].dG_prime)))
                        for m in MODULE})
    out_json = out_tsv.with_suffix(".json")
    out_json.write_text(json.dumps(meta, indent=2))
    print(f"\n[baseline] {out_tsv}\n[baseline] {out_json}")


if __name__ == "__main__":
    main()

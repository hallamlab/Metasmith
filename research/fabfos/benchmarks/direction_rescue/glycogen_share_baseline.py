#!/usr/bin/env python3
"""What share of the injected carbon reaches glycogen, against a named bake.

The second readout r9 moves, and the one the delivery split cannot stand in for. A
two-point conductance to glycogen is non-decreasing in every edge by Rayleigh, so it
cannot express a glycogen-DEFICIENT phenotype at all; a SHARE of the injected current can
fall, because a share is homogeneous of degree zero and its elasticities sum to zero
rather than to one. `fabfos/bench-eydallin` moved its cohort work onto that footing
(`glycogen_share.py`, `two_ground_panel.py`) and this is the scalar underneath it.

**Deliberately one number, not their panel.** Their apparatus sweeps folds, grounds and
leak magnitudes to ask whether talA's SIGN survives; that question is theirs and needs
their whole instrument. What r9 needs is a before-value on the quantity all of it rests
on, taken while r8 is still the deployed bake. The sign result stays uncovered here and
`baselines/README.md` says so.

Grounding is `biomass`: glycogen and the AG1 model's carbon-bearing biomass precursors are
real ports at conductance `port`, every other metabolite drains only at `leak`. That is
the competition Fig. 1 measures -- nmol glucose per mg protein is a fraction of the cell's
carbon, not a flux capacity. The precursors are bridged through `chem_xref`'s `biggM:`
rows rather than the model's own `metanetx.chemical` annotation, which is MetaNetX 3.x and
maps a third of the model; that rule is `glycogen_share.biomass_precursors`, kept in step
rather than imported, because that scope's worktree carries uncommitted work and this one
must be able to re-measure without it.

    PYTHONPATH=src mamba run -n msm python \\
        research/fabfos/benchmarks/direction_rescue/glycogen_share_baseline.py
    ... --bake data/fabfos/processed/metabolism_bake_r9
"""
from __future__ import annotations

import argparse
import json
import re
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
from ecspr.model.graph import Terminal, measure_leak                          # noqa: E402

RUNS = ROOT / "data/fabfos/runs"
AG1_GEM = ROOT / "data/fabfos/originals/genomes/e_coli_dh1/GEM/iECDH1ME8569_1439.json"
CHEM_XREF = ROOT / "data/fabfos/originals/metanetx/4.5/chem_xref.tsv"
CACHE = HERE / "cache"
OUT = HERE / "baselines"

SOURCE_MNXM = "MNXM1364061"          # D-glucose, as the atom-pair table carries it
GLYCOGEN_MNXM = "MNXM738130"
_COMP = re.compile(r"_([a-z]{1,2})$")


def biomass_precursors(universe: set) -> list:
    cache = CACHE / "biggM_bridge.parquet"
    if cache.exists():
        br = pd.read_parquet(cache)
    else:
        x = pd.read_csv(CHEM_XREF, sep="\t", comment="#", header=None,
                        names=["source", "mnxm", "description"], dtype=str,
                        low_memory=False)
        x = x[x.source.str.startswith("biggM:", na=False)]
        br = pd.DataFrame(dict(bigg=x.source.str[len("biggM:"):], mnxm=x.mnxm)) \
            .drop_duplicates()
        CACHE.mkdir(parents=True, exist_ok=True)
        br.to_parquet(cache, index=False)
    by_bigg = br.groupby("bigg").mnxm.apply(list).to_dict()

    model = json.loads(AG1_GEM.read_text())
    cands = [r for r in model["reactions"]
             if r.get("objective_coefficient", 0) or "BIOMASS" in r["id"].upper()]
    obj = [r for r in cands if r.get("objective_coefficient", 0)]
    rxn = (obj or [r for r in cands if "core" in r["id"].lower()] or cands)[0]
    subs = [m for m, c in rxn["metabolites"].items() if c < 0]
    out = set()
    for m in subs:
        out.update(c for c in by_bigg.get(_COMP.sub("", m), []) if c in universe)
    return sorted(out)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--bake", type=Path, default=bake_identity.DEPLOYED)
    ap.add_argument("--host", default="e_coli_ag1")
    ap.add_argument("--element", default="C")
    ap.add_argument("--leak", type=float, default=1e-3,
                    help="background drain. Rung G of bench-eydallin's monotonicity "
                         "ladder shows the shunt elasticity is zero to five decimals at "
                         "1e-6, so that value reads flat and is the wrong default")
    ap.add_argument("--port", type=float, default=1.0)
    ap.add_argument("--tag", default=None)
    args = ap.parse_args()

    tag = args.tag or args.bake.name
    stamp = bake_identity.identity(args.bake)
    sub = CACHE / tag
    pairs = load_pairs(bake_identity.build_atom_pairs(sub / "atom_pairs_bake.parquet",
                                                      args.bake), element=args.element)
    ratios = load_direction_ratios(
        bake_identity.build_direction_ratios(sub / "direction_ratios.parquet", args.bake))

    gpr = pd.read_parquet(RUNS / args.host / "gpr" / "gpr_gem.parquet")
    weights = {m: 1.0 for m in gpr.mnxr.dropna().astype(str).unique()}
    graph = graph_from_pairs(pairs, args.element, weights, ratios)

    universe = {m for m, _ in graph.nodes}
    prec = sorted(set(biomass_precursors(universe)) | {GLYCOGEN_MNXM})
    src = Terminal.metabolite(graph, SOURCE_MNXM, label="glucose")
    if src.missing:
        raise SystemExit("[share] D-glucose is not a node of this graph")
    r = measure_leak(graph, src, prec, leak=args.leak, port=args.port)
    share = float(r["draw"].get(GLYCOGEN_MNXM, 0.0))

    print(f"bake      {args.bake}\nstamp     {stamp}\n"
          f"graph     {graph.n:,} nodes, {graph.m:,} edges\n"
          f"grounding biomass · {len(prec)} ports (glycogen + {len(prec) - 1} precursors)"
          f" · leak {args.leak:g} · port {args.port:g}\n"
          f"converged {bool(r['converged'])} · precursor share {r['prec_share']:.6f}"
          f" · leak fraction {r['leak_frac']:.6f}\n\n"
          f"  share of injected carbon reaching glycogen   {share:.6f}")

    OUT.mkdir(parents=True, exist_ok=True)
    out = OUT / f"glycogen_share_{tag}_{args.host}_{args.element}.json"
    out.write_text(json.dumps(
        dict(bake=str(args.bake), stamp=stamp, host=args.host, element=args.element,
             leak=args.leak, port=args.port, n_ports=len(prec),
             source=SOURCE_MNXM, sink=GLYCOGEN_MNXM, share=share,
             prec_share=float(r["prec_share"]), leak_frac=float(r["leak_frac"]),
             converged=bool(r["converged"])), indent=2))
    bake_identity.keep(out, args.bake)
    print(f"\n[baseline] {out}")


if __name__ == "__main__":
    main()

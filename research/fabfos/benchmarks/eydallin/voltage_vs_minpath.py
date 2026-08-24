#!/usr/bin/env python3
from __future__ import annotations

import collections
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / "src"))

from ecspr.model.build import load_pairs, load_direction_ratios, graph_from_pairs  # noqa: E402
from ecspr.model.graph import Terminal, attach_leak, solve, OMEGA                  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parent))
import bake_pairs  # noqa: E402

ATOM_PAIRS = bake_pairs.atom_pairs()
HOST_GEM = ROOT / "data/fabfos/runs/e_coli_k12/gpr/gpr_gem.parquet"
CHEM_PROP = ROOT / "data/fabfos/originals/metanetx/4.5/chem_prop.tsv"
BAKE = ROOT / "data/fabfos/processed/metabolism_bake"
OUT_DIR = Path(__file__).resolve().parent / "cache"
OUT_DIR.mkdir(parents=True, exist_ok=True)

SOURCE_MNXM = "MNXM1364061"
GLYCOGEN_MNXM = "MNXM738130"
LEAK = 1e-6
ELEMENT = "C"

COFACTOR_NAMES = [
    "ATP", "ADP", "AMP", "CoA", "acetyl-CoA", "malonyl-CoA",
    "NAD(+)", "NADH", "NADP(+)", "NADPH", "FAD", "FADH2", "Flavin", "Reduced flavin",
    "CO2", "hydrogencarbonate", "UDP", "CMP", "GDP", "UMP", "UTP", "GTP", "CTP",
    "dTTP", "dTDP", "dTMP", "S-adenosyl-L-methionine", "S-adenosyl-L-homocysteine",
    "glutathione", "adenosine 3',5'-bisphosphate",
]


def main():
    pairs = load_pairs(ATOM_PAIRS, element=ELEMENT)
    ratios = load_direction_ratios(bake_pairs.direction_ratios())
    host = pd.read_parquet(HOST_GEM)
    base_w = {m: 1.0 for m in host["mnxr"].dropna().astype(str).unique()}

    v = pd.read_parquet(BAKE / "vocab.parquet")
    sym = v[v.kind == "met"].set_index("code").symbol.to_dict()
    rsym = v[v.kind == "rxn"].set_index("code").symbol.to_dict()
    ap = pd.read_parquet(BAKE / "atom_pairs.parquet")
    bake_c = (ap[ap.element == 0][["rxn", "tail_met", "head_met"]]
              .assign(rxn=lambda d: d.rxn.map(rsym), tail_met=lambda d: d.tail_met.map(sym),
                      head_met=lambda d: d.head_met.map(sym)).drop_duplicates())
    cp = pd.read_csv(CHEM_PROP, sep="\t", comment="#", header=None, usecols=[0, 1],
                     names=["id", "name"], dtype=str, low_memory=False)
    name_to_id = dict(zip(cp["name"], cp["id"]))
    barred = {name_to_id[n] for n in COFACTOR_NAMES if n in name_to_id}

    sub = bake_c[bake_c.rxn.isin(base_w)]
    m2r, r2m = collections.defaultdict(set), collections.defaultdict(set)
    for rx, t, h in sub.itertuples(index=False):
        for m in (t, h):
            if m in barred or m is None:
                continue
            m2r[m].add(rx)
            r2m[rx].add(m)
    dist = {r: 0 for r in m2r.get(SOURCE_MNXM, ())}
    frontier = set(dist)
    d = 0
    while frontier:
        d += 1
        nxt = set()
        for r in frontier:
            for m in r2m[r]:
                for r2 in m2r[m]:
                    if r2 not in dist:
                        dist[r2] = d
                        nxt.add(r2)
        frontier = nxt
    met_dist = {}
    for m, rs in m2r.items():
        ds = [dist[r] for r in rs if r in dist]
        if ds:
            met_dist[m] = min(ds)
    print(f"[minpath] hop distance resolved for {len(met_dist)} metabolites from "
          f"{SOURCE_MNXM} ({len(dist)} reactions reached)", file=sys.stderr)

    g = graph_from_pairs(pairs, ELEMENT, base_w, ratios)
    g2, _ = attach_leak(g, None, leak=LEAK)
    src = Terminal.metabolite(g2, SOURCE_MNXM, label="source")
    if src.missing:
        raise SystemExit(f"[minpath] source {SOURCE_MNXM} absent from built graph")
    omega_t = Terminal.of_nodes("OMEGA", [OMEGA])
    sol = solve(g2, src, omega_t)
    print(f"[minpath] base: {g.n:,} nodes / {g.m:,} edges, converged={sol.converged}",
          file=sys.stderr)

    rows = []
    for m in g.metabolites():
        vinfo = sol.voltage_metabolite(m)
        vm = vinfo["weighted_mean"]
        if vm != vm:
            continue
        rows.append((m, vm, vinfo["n_atoms"], met_dist.get(m)))
    df = pd.DataFrame(rows, columns=["mnxm", "voltage", "n_atoms", "hop_dist"])
    out = OUT_DIR / "voltage_vs_minpath_base.parquet"
    df.to_parquet(out)
    print(f"[minpath] {len(df)} metabolites with finite voltage; "
          f"{df.hop_dist.notna().sum()} also have a resolved hop distance; wrote {out}",
          file=sys.stderr)

    if GLYCOGEN_MNXM in set(df.mnxm):
        gly = df[df.mnxm == GLYCOGEN_MNXM].iloc[0]
        print(f"[minpath] glycogen: hop_dist={gly.hop_dist}, voltage={gly.voltage:.6g}",
              file=sys.stderr)


if __name__ == "__main__":
    main()

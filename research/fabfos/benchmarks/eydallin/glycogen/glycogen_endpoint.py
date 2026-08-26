#!/usr/bin/env python3
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[5]
sys.path.insert(0, str(ROOT / "src/ecspr"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import bake_pairs  # noqa: E402
from ecspr.model.build import graph_from_pairs, load_direction_ratios, load_pairs  # noqa: E402
from ecspr.model.graph import Terminal, solve  # noqa: E402

HOST_GEM = ROOT / "data/fabfos/runs/e_coli_k12/gpr/gpr_gem.parquet"

SOURCE = "MNXM1364061"
TARGETS = {
    "MNXM738130": "glycogen (BiGG, the iML1515 species)",
    "MNXM738131": "glycogen (KEGG C00182)",
    "MNXM8348": "branching glycogen",
    "MNXM1105977": "ADP-alpha-D-glucose (the pilot's fallback endpoint)",
    "MNXM1364212": "D-glucopyranose 1-phosphate (one step further out)",
}


def main():
    ratios = load_direction_ratios(bake_pairs.direction_ratios())
    host = pd.read_parquet(HOST_GEM)
    weights = {m: 1.0 for m in host.mnxr.dropna().astype(str).unique()}

    pairs = load_pairs(bake_pairs.atom_pairs(), element="C")
    g = graph_from_pairs(pairs, "C", weights, ratios)
    print(f"\n=== bake: {g.n:,} nodes / {g.m:,} edges "
          f"from {g.meta['n_reactions_used']:,} host reactions ===")
    src = Terminal.metabolite(g, SOURCE, label="glucose")
    if src.missing:
        print(f"  source {SOURCE} absent from the graph")
        return
    for mnxm, name in TARGETS.items():
        snk = Terminal.metabolite(g, mnxm, label=name)
        if snk.missing:
            print(f"  {mnxm:12} {name:48} NOT A NODE")
            continue
        s = solve(g, src, snk)
        note = getattr(s, "note", "") or ""
        print(f"  {mnxm:12} {name:48} atoms={len(snk):3} "
              f"conductance={s.total:.6g} {note}")


if __name__ == "__main__":
    main()

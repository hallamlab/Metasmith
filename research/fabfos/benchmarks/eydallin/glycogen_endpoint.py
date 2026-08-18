#!/usr/bin/env python3
"""Can glycogen BE the endpoint? A two-point solve glucose -> glycogen on the bake.

`run_pilot_glycogen.py` never grounded at glycogen. It ran `measure_leak` with a
universal ground and read glycogen's DRAW, so the polymer had to compete with every
other sink in the network for a 1e-6 leak -- which is why its number came back at 7e-10
of the solve total and negative. That is a statement about the probe, not about whether
glycogen can terminate one.

`ecspr two-point --sinks` takes any metabolite list, and the conditions schema carries
sink_hub per row, so naming glycogen as the ground needs no new machinery. This script
does exactly that.

The pilot's conclusion -- "nothing maps carbon into the polymer" -- was true of the
retired tier4 reference, where glgA (`MNXR145046`) and glgP (`MNXR145036`) carried zero
carbon rows and this solve returned a hard `terminals disconnected`. On the bake those
reactions are mapped and the solve returns a finite conductance, so the target is live.

Both glycogen ids are probed separately: MNXM738130 is the BiGG species iML1515 carries,
MNXM738131 the KEGG-keyed one, and they hold different reaction sets.
"""
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / "src/ecspr"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import bake_pairs  # noqa: E402
from ecspr.model.build import graph_from_pairs, load_direction_ratios, load_pairs  # noqa: E402
from ecspr.model.graph import Terminal, solve  # noqa: E402

HOST_GEM = ROOT / "data/fabfos/benchmarks/hosts/e_coli_k12/gpr_gem.parquet"

SOURCE = "MNXM1364061"                       # D-glucose, as the pilot resolved it
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

#!/usr/bin/env python3
"""Universal ground over the eydallin cohort, on r9, under three direction treatments.

`cohort_delta_panel.py` run on the pinned bake returns a Spearman of -0.027 against Fig. 1
where the committed REPORT records +0.21, and it puts a NEGATIVE delta on glgC and glgA --
the cohort's two strongest accumulators, 453% and 328% of wild type. Two explanations fit
that, and they have different consequences:

  the bake       r9's direction table re-signed the network, and the readout is reporting a
                 real change in the model.
  the probe      universal ground is structurally wrong for a terminal polymer, and the
                 committed +0.21 was noise around zero that r9 happened to move.

They are separated by one experiment: run the identical cohort with every ratio pinned to
1.0. A symmetric network has no direction evidence in it at all, so if the all-negative
pattern survives, it is the grounding geometry and not the bake. The capped arm rides along
because it costs one more pass and it is the transform this study is proposing.

The mechanism the symmetric arm tests for: under universal ground glycogen is not a sink,
it is one more leaking node, and a x2 fold scales a reaction's forward AND backward
conductance together. Widening the last edge into a near-terminal metabolite therefore also
widens the way back out of it, and the draw AT that metabolite falls. That is a property of
where the grounds are, not of what the reaction is.

Curated GEM arm (`lane_set=curated`); no pbert.

    mamba run -n ecspr python \
        research/fabfos/benchmarks/laser/conditioning/universal_ground_cohort.py --leak 1e-3
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import spearmanr

ROOT = Path(__file__).resolve().parents[5]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "research/fabfos/benchmarks/eydallin"))

from ecspr.model.build import (load_pairs, load_direction_ratios,       # noqa: E402
                               graph_from_pairs, cap_direction_ratios)
from ecspr.model.graph import Terminal, measure_leak                     # noqa: E402
from ecspr.model.scoring import responders                              # noqa: E402
import bake_pairs                                                       # noqa: E402

HOST_GEM = ROOT / "data/fabfos/runs/e_coli_k12/gpr/gpr_gem.parquet"
CLONE_GPR = ROOT / "data/fabfos/runs/eydallin_clones/gpr/gpr_gem.parquet"
MEASURED = ROOT / "data/fabfos/benchmarks/eydallin/Y/measured_glycogen.tsv"
OUT_DIR = Path(__file__).resolve().parent / "cache"

SOURCE_MNXM = "MNXM1364061"
GLYCOGEN_MNXM = "MNXM738130"


def arms(baked: dict, cap: float):
    return [("baked", baked),
            ("capped", cap_direction_ratios(baked, cap)),
            ("symmetric", {k: 1.0 for k in baked})]


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--fold", type=float, default=2.0)
    p.add_argument("--element", default="C")
    p.add_argument("--leak", type=float, default=1e-3)
    p.add_argument("--cap", type=float, default=3.0)
    p.add_argument("--coverage", type=float, default=0.90)
    p.add_argument("--out", type=Path, default=None)
    a = p.parse_args()

    pairs = load_pairs(bake_pairs.atom_pairs(), element=a.element)
    baked = load_direction_ratios(bake_pairs.direction_ratios())
    host = pd.read_parquet(HOST_GEM)
    assert set(host.lane_set.unique()) == {"curated"}, "not the curated arm"
    base_w = {m: 1.0 for m in host.mnxr.dropna().astype(str).unique()}

    clone = pd.read_parquet(CLONE_GPR)
    clone = clone[clone.in_atom_universe & (clone.channel == "gem_gpr")]
    genes = (clone.groupby("condition_id")
             .agg(gene=("feature_name", "first"), rxns=("mnxr", lambda s: sorted(set(s))))
             .reset_index())
    meas = pd.read_csv(MEASURED, sep="\t")
    pct = {str(g).lower(): v for g, v in zip(meas.condition_id, meas.pct_wt)}

    rows, summary = [], []
    for name, ratios in arms(baked, a.cap):
        def draw(weights):
            g = graph_from_pairs(pairs, a.element, weights, ratios)
            src = Terminal.metabolite(g, SOURCE_MNXM, label="glucose")
            r = measure_leak(g, src, [], leak=a.leak)
            return float(r["draw"].get(GLYCOGEN_MNXM, 0.0))

        base = draw(base_w)
        print(f"[ug] {name}: base draw at glycogen = {base:.9g}", file=sys.stderr)
        for _, row in genes.iterrows():
            w = dict(base_w)
            for r in row.rxns:
                if r in w:
                    w[r] = w[r] * a.fold
            v = draw(w)
            rows.append(dict(arm=name, condition_id=row.condition_id, gene=row.gene,
                             n_rxn=len(row.rxns), draw_base=base, draw_pert=v,
                             delta=v - base,
                             pct_wt=pct.get(str(row.condition_id).lower())))

        sub = pd.DataFrame([r for r in rows if r["arm"] == name]).dropna(subset=["pct_wt"])
        rho, pv = spearmanr(sub.delta, sub.pct_wt)
        mag = np.abs(sub.delta.to_numpy())
        keep = responders(mag, a.coverage)
        live = mag[mag > 0]
        summary.append(dict(
            arm=name, base=base, n=len(sub), rho=rho, p=pv,
            n_negative=int((sub.delta < 0).sum()), n_zero=int((sub.delta == 0).sum()),
            glgC=float(sub.set_index("gene").delta.get("glgC", np.nan)),
            glgA=float(sub.set_index("gene").delta.get("glgA", np.nan)),
            n_responders=int(keep.sum()),
            responder_decades=float(np.ptp(np.log10(mag[keep]))) if keep.sum() > 1 else 0.0,
            cohort_decades=float(np.ptp(np.log10(live))) if live.size > 1 else 0.0))
        s = summary[-1]
        print(f"  rho={s['rho']:+.4f} p={s['p']:.3g} n={s['n']}  "
              f"negative={s['n_negative']}/{s['n']} zero={s['n_zero']}  "
              f"glgC={s['glgC']:+.3e} glgA={s['glgA']:+.3e}  "
              f"responders={s['n_responders']} over {s['responder_decades']:.2f} decades "
              f"(cohort {s['cohort_decades']:.2f})", file=sys.stderr)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out = a.out or OUT_DIR / f"universal_ground_cohort_leak{a.leak:g}.tsv"
    pd.DataFrame(rows).to_csv(out, sep="\t", index=False)
    sm = OUT_DIR / f"universal_ground_summary_leak{a.leak:g}.tsv"
    pd.DataFrame(summary).to_csv(sm, sep="\t", index=False)
    print(f"\n[ug] wrote {out}\n[ug] wrote {sm}", file=sys.stderr)
    print(pd.DataFrame(summary).to_string(index=False), file=sys.stderr)


if __name__ == "__main__":
    main()

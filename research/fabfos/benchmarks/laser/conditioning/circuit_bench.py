#!/usr/bin/env python3
"""What sets the SCALE of an ECSPr response, on circuits small enough to check by hand.

`monotonicity_ladder.py` asked when a response can be negative. This asks how many decades
it spans, because a readout whose nonzero responses cover fifteen of them is not something a
phenotype can be regressed against -- on the 4,102-gene ASKA sweep 79.1% of genes return
exactly zero and the rest run from 1e-15 to 1e-0.3.

Four families, each isolating one candidate. The first three are hand-checkable and establish
mechanism; the fourth is a population, because "how wide is the distribution" is not a
question a four-node circuit can answer.

  chain     response vs distance from the terminals
  diode     response vs the backward ratio -- the decades-in/decades-out law
  fanout    pair_w dilution compounding along a path
  network   a metabolic-shaped random graph, sweeping the ratio's decade width W

THE RESPONSE IS READ AS A POWER SHARE, NOT A FINITE DIFFERENCE. Effective conductance is
homogeneous of degree one, so an edge's share of the dissipated power IS its
`dlog C_eff / dlog g_e` (Tellegen; `graph.Solution.edge_power`), exact on the symmetric
network and first order under the rectified law. That matters here rather than being a
convenience: `monotonicity_ladder.elasticity` takes a fold of 1.0001, which is pure
floating-point noise once the true response is below ~1e-10 -- precisely the regime this
bench exists to measure.

TWO POPULATIONS, AND ONLY ONE OF THEM IS THE PROBLEM. An edge carrying no current has a
response of exactly zero, and no rescaling changes that; a dead-end diversion is invisible to
a two-point probe (ladder rung A). So the floor mode is topology and is allowed to stay. The
spread being measured is the spread of the RESPONDERS.

    mamba run -n ecspr python research/fabfos/benchmarks/laser/conditioning/circuit_bench.py
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[5]
sys.path.insert(0, str(ROOT / "src"))

from ecspr.model.graph import AtomGraph, Terminal, solve  # noqa: E402

OUT_DIR = Path(__file__).resolve().parent / "cache"

# Responses below this are the solver's own floor, not a small answer. The reduced gradient
# converges to ~1e-8 in float64 on an ill-conditioned axis and power shares inherit that, so
# anything under it is reported as floor rather than as a measurement.
POWER_FLOOR = 1e-12


def circuit(edges: dict) -> AtomGraph:
    return AtomGraph.from_edge_records(
        [((a, 0), (b, 0), gp, gm) for (a, b), (gp, gm) in edges.items()],
        dict(kind="conditioning_bench"))


def responses(edges: dict, src: str, snk: str) -> tuple:
    g = circuit(edges)
    sol = solve(g, Terminal.metabolite(g, src), Terminal.metabolite(g, snk))
    ceff = float(sol.total)
    eps = np.zeros(g.m)
    if ceff > 0:
        oe, pw = sol.edge_power()
        tot = float(pw.sum())
        if tot > 0:
            np.add.at(eps, oe, pw / tot)
    return ceff, dict(zip(edges.keys(), eps))


def spread(eps) -> dict:
    a = np.abs(np.asarray(list(eps.values()), float))
    live = a[a > POWER_FLOOR]
    out = dict(n_edges=len(a), n_floor=int((a <= POWER_FLOOR).sum()),
               frac_floor=float((a <= POWER_FLOOR).mean()), n_live=len(live))
    if len(live) < 2:
        return dict(out, decades_full=np.nan, decades_5_95=np.nan, gap=np.nan)
    lg = np.sort(np.log10(live))
    out["decades_full"] = float(lg[-1] - lg[0])
    out["decades_5_95"] = float(np.percentile(lg, 95) - np.percentile(lg, 5))
    out["gap"] = float(np.max(np.diff(lg))) if len(lg) > 1 else np.nan
    return out


def family_chain(rows, lengths=(2, 4, 8, 16, 32)):
    for L in lengths:
        e = {(f"x{i}", f"x{i+1}"): (1.0, 1.0) for i in range(L)}
        for i in range(1, L):
            e[(f"x{i}", f"d{i}")] = (1.0, 1.0)
        ceff, eps = responses(e, "x0", f"x{L}")
        on = [v for k, v in eps.items() if k[1].startswith("x")]
        rows.append(dict(family="chain", knob="length", setting=L, ceff=ceff,
                         backbone_min=min(on), backbone_max=max(on), **spread(eps)))


def family_diode(rows, widths=(0, 1, 2, 3, 6, 9, 12, 15, 18)):
    for W in widths:
        r = 10.0 ** -W
        e = {("S", "A"): (1.0, 1.0), ("A", "T"): (1.0, 1.0),
             ("S", "B"): (1.0, 1.0), ("T", "B"): (1.0, r)}
        ceff, eps = responses(e, "S", "T")
        rows.append(dict(family="diode", knob="ratio_decades", setting=W, ceff=ceff,
                         eps_throttled=eps[("T", "B")], eps_open=eps[("A", "T")],
                         **spread(eps)))


def family_fanout(rows, lengths=(2, 4, 8, 16, 32), weights=(1.0, 0.5)):
    # The same backbone with every edge at `pair_w`, against one at 1.0.
    #
    # `pair_w` is bounded in [0.5, 1] by construction (consensus 1.0, lone-member 0.5), so the
    # most it can do is one bit per edge. Whether that compounds into decades along a path is
    # the question; a two-point conductance is a harmonic mean, so it should not.
    for L in lengths:
        for w in weights:
            e = {(f"x{i}", f"x{i+1}"): (w, w) for i in range(L)}
            ceff, eps = responses(e, "x0", f"x{L}")
            rows.append(dict(family="fanout", knob="pair_w", setting=w, length=L,
                             ceff=ceff, **spread(eps)))


def _metabolic_shaped(rng, n_met=400, extra=600, dead_frac=0.25):
    order = rng.permutation(n_met)
    e = {}
    for i in range(1, n_met):
        a, b = order[rng.integers(0, i)], order[i]
        e[(f"m{a}", f"m{b}")] = None
    for _ in range(extra):
        a, b = rng.integers(0, n_met, 2)
        if a != b:
            e[(f"m{a}", f"m{b}")] = None
    for j in range(int(n_met * dead_frac)):
        a = order[rng.integers(0, n_met)]
        e[(f"m{a}", f"dead{j}")] = None
    return list(e.keys())


def family_network(rows, widths=(0, 1, 2, 3, 6, 9, 12, 15, 18), seeds=(0, 1, 2),
                   directed_frac=0.85, gp_decades=0.6):
    # The decade width of the ratio table, swept, on a graph big enough to have a
    # distribution.
    #
    # `directed_frac` and `gp_decades` are set from the real k12 GEM carbon graph: 15% of its
    # edges sit at ratio exactly 1.0, and its existence conductance spans 0.6 decades between
    # the 5th and 95th percentiles. So the only knob being swept is the one the real table
    # disagrees with a well-conditioned instrument about.
    for seed in seeds:
        rng = np.random.default_rng(seed)
        keys = _metabolic_shaped(rng)
        gp = 10.0 ** rng.uniform(-gp_decades / 2, gp_decades / 2, len(keys))
        directed = rng.random(len(keys)) < directed_frac
        u = rng.random(len(keys))
        for W in widths:
            lr = np.where(directed, -W * u, 0.0)
            e = {k: (float(p), float(p * 10.0 ** l)) for k, p, l in zip(keys, gp, lr)}
            ceff, eps = responses(e, "m0", "m1")
            rows.append(dict(family="network", knob="ratio_decades", setting=W, seed=seed,
                             ceff=ceff, **spread(eps)))


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--out", default=str(OUT_DIR / "circuit_bench.tsv"))
    a = p.parse_args()

    rows: list = []
    for fn in (family_chain, family_diode, family_fanout, family_network):
        print(f"== {fn.__name__}", file=sys.stderr)
        fn(rows)

    df = pd.DataFrame(rows)
    out = Path(a.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, sep="\t", index=False)

    for fam, g in df.groupby("family", sort=False):
        cols = [c for c in ("setting", "length", "seed", "ceff", "n_live", "frac_floor",
                            "decades_full", "decades_5_95", "gap", "eps_throttled",
                            "backbone_min", "backbone_max") if c in g.columns]
        print(f"\n-- {fam}", file=sys.stderr)
        print(g[cols].to_string(index=False, float_format=lambda v: f"{v:.4g}"),
              file=sys.stderr)
    print(f"\nwrote {out}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

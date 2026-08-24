#!/usr/bin/env python3
"""Where the ECSPr response's decades actually come from, on the real host graphs.

`circuit_bench.py` ruled out two candidates on circuits and convicted a third. This puts the
same knob on the graphs the benchmarks read, and answers the question the synthetic bench
cannot: how much of the spread survives when the direction table is switched off entirely.

Two arms per host. The INPUT census measures the spread of every quantity that enters the
graph -- `pair_w`, the existence conductance `gp`, the per-edge backward ratio `gm/gp`, and
the per-reaction ratio -- because an input that spans a third of a decade cannot be
responsible for a response that spans eleven. The CAP SWEEP then bounds |log10 ratio| at a
series of widths and re-measures the response, with the width-0 arm (every ratio at 1.0) as
the floor no ratio transform can go below.

THE READOUT IS THE ELASTICITY, NOT A PERTURBATION. One solve carries every reaction's
`dlog C_eff / dlog g_r` as its share of the dissipated power, so the whole response
distribution costs one solve rather than one solve per reaction -- and it is exact where a
finite difference would be reporting float64 noise.

CURATED ARM ONLY. `gpr_gem.parquet` carries `lane_set=curated`, every row `score_kind=
presence`. The de-novo tables carry a `pbert` channel whose landmark index is under repair,
so a number taken from them today is not a number about this instrument.

    mamba run -n ecspr python research/fabfos/benchmarks/laser/conditioning/decade_budget.py
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[5]
sys.path.insert(0, str(ROOT / "src"))

import ecspr.model.build as EB                                   # noqa: E402
from ecspr.model.graph import Terminal, solve                    # noqa: E402

BAKE = ROOT / "data/fabfos/processed/metabolism_bake"
ANNOT = BAKE / "seams/direction_annotation.parquet"
RUNS = ROOT / "data/fabfos/runs"
OUT_DIR = Path(__file__).resolve().parent / "cache"
ASKA = ROOT / "data/fabfos/runs/eydallin_clones/ecspr/aska_sweep_gem_e_coli_ag1_fold2.0_C.tsv"

GLUCOSE = "MNXM1364061"
# Three terminals per host, deliberately different in character: glycogen is the eydallin
# target and sits behind the direction gap; pyruvate is ordinary central carbon three hops
# out; L-methionine sits at the median of the 1,059-target conductance distribution. A
# conditioning claim that holds on only one of them is a claim about that axis.
SINKS = {"glycogen": "MNXM738130", "pyruvate": "MNXM23", "methionine": "MNXM738804"}

# Dense below 4, because the cap sweep saturates there: past ~6 decades the extra asymmetry
# is already numerically dead and every arm returns the same graph.
WIDTHS = (0.0, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 4.0, 6.0, 9.0, np.inf)
POWER_FLOOR = 1e-12

# The responder set is defined by COVERAGE, not by a relative floor. A floor at 10**-k of
# the top lever bounds the set's span at k decades by construction, so it measures the
# definition rather than the network -- the first cut of this bench reported 3.00 decades at
# every width on every axis, which is what that tautology looks like. Coverage cannot do
# that: "the reactions carrying 90% of the response" is a set the network chooses.
COVERAGE = (0.90, 0.99)


def decades(a, lo=5, hi=95) -> tuple:
    v = np.abs(np.asarray(a, float))
    v = v[np.isfinite(v) & (v > POWER_FLOOR)]
    if v.size < 2:
        return np.nan, np.nan
    lg = np.log10(v)
    return float(lg.max() - lg.min()), float(np.percentile(lg, hi) - np.percentile(lg, lo))


def load_bake() -> tuple:
    v = pd.read_parquet(BAKE / "vocab.parquet")
    mol = v[v.kind == "met"].set_index("code").symbol.to_dict()
    rxn = v[v.kind == "rxn"].set_index("code").symbol.to_dict()
    ele = v[v.kind == "element"].set_index("code").symbol.to_dict()
    ccode = next(k for k, s in ele.items() if s == "C")

    ap = pd.read_parquet(BAKE / "atom_pairs.parquet")
    ap = ap[ap.element == ccode]
    pairs = pd.DataFrame({
        "mnxr": ap.rxn.map(rxn), "element": "C",
        "substrate": ap.tail_met.map(mol), "product": ap.head_met.map(mol),
        "sub_idx": ap.tail_rank.to_numpy(), "prod_idx": ap.head_rank.to_numpy(),
        "pair_w": ap.pair_w.to_numpy(),
    }).dropna(subset=["mnxr", "substrate", "product"])
    pairs = pairs[pairs.mnxr != "EMPTY"]

    ann = pd.read_parquet(ANNOT)[["mnxr", "ratio", "dir_tier", "dir_confidence"]]
    ann = ann[ann.mnxr != "EMPTY"]
    return pairs, ann


def capped(ann: pd.DataFrame, width: float) -> dict:
    r = ann.ratio.to_numpy(float)
    if np.isfinite(width):
        lg = np.clip(np.log10(np.maximum(r, np.finfo(float).tiny)), -width, width)
        r = 10.0 ** lg
    return dict(zip(ann.mnxr.to_numpy(), r))


def gene_responses(eps: pd.Series) -> np.ndarray:
    # Per-gene |dlog C_eff| under a fold, first order, for the whole ASKA library.
    #
    # A fold ``f`` on a clone's reactions moves the readout by ``(sum eps_r) * log f`` to first
    # order, so the library's whole response distribution falls out of the ONE solve that
    # produced ``eps`` -- 4,102 genes for free, where the committed sweep spent 0.65 s each.
    # First order is the right order: the sweep's fold is 2.0 and its own finding is that
    # almost every response is microscopic, which is exactly where a linearisation is tight.
    if not ASKA.exists():
        return np.zeros(0)
    a = pd.read_csv(ASKA, sep="\t", usecols=["gene", "rxns"]).drop_duplicates("gene")
    lut = eps.to_dict()
    return np.array([sum(lut.get(r, 0.0) for r in str(x).split(";") if r)
                     for x in a.rxns.fillna("")], float)


def input_census(rows, host, pairs, ann, weights):
    used = pairs[pairs.mnxr.isin(weights)]
    sub = ann[ann.mnxr.isin(weights)]
    for name, v in (("pair_w", used.pair_w.to_numpy()),
                    ("reaction_ratio", sub.ratio.to_numpy(float))):
        full, band = decades(v)
        rows.append(dict(host=host, arm="input", quantity=name, n=len(v),
                         decades_full=full, decades_5_95=band))
    for tier, g in sub.groupby("dir_tier"):
        full, band = decades(g.ratio.to_numpy(float))
        rows.append(dict(host=host, arm="input", quantity=f"reaction_ratio_tier{tier}",
                         n=len(g), decades_full=full, decades_5_95=band,
                         frac_at_one=float((g.ratio == 1.0).mean()),
                         median_confidence=float(g.dir_confidence.median())))


def gene_stats(g: np.ndarray) -> dict:
    if g.size == 0:
        return {}
    a = np.abs(g)
    full, band = decades(a)
    out = dict(n_genes=int(a.size), gene_frac_zero=float((a == 0).mean()),
               gene_frac_floor=float((a <= POWER_FLOOR).mean()),
               gene_decades_full=full, gene_decades_5_95=band)
    order = np.sort(a)[::-1]
    cum = np.cumsum(order) / max(order.sum(), np.finfo(float).tiny)
    for c in COVERAGE:
        k = int(np.searchsorted(cum, c) + 1)
        out[f"gene_n_cover{int(c * 100)}"] = k
        out[f"gene_decades_cover{int(c * 100)}"] = decades(order[:k])[0]
    return out


def sweep(rows, host, pairs, ann, weights):
    for width in WIDTHS:
        ratios = capped(ann, width)
        g = EB.graph_from_pairs(pairs, "C", weights, ratios, with_provenance=True)
        gp, gm = np.asarray(g.gp), np.asarray(g.gm)
        edge_full, edge_band = decades(gm / np.maximum(gp, np.finfo(float).tiny))
        gp_full, gp_band = decades(gp)
        for sink, mnxm in SINKS.items():
            try:
                sol = solve(g, Terminal.metabolite(g, GLUCOSE),
                            Terminal.metabolite(g, mnxm))
            except Exception as exc:
                print(f"  {host} {sink} w={width}: {type(exc).__name__} {exc}",
                      file=sys.stderr)
                continue
            if not sol.total > 0:
                print(f"  {host} {sink}: not reached at width={width}", file=sys.stderr)
                continue
            eps = EB.reaction_elasticities(g, sol)
            e = np.abs(eps.to_numpy())
            full, band = decades(e)
            live = e > POWER_FLOOR
            order = np.sort(e)[::-1]
            cum = np.cumsum(order) / max(order.sum(), np.finfo(float).tiny)
            rel = {}
            for c in COVERAGE:
                k = int(np.searchsorted(cum, c) + 1)
                rel[f"n_cover{int(c * 100)}"] = k
                rel[f"decades_cover{int(c * 100)}"] = decades(order[:k])[0]
            rows.append(dict(
                host=host, arm="sweep", quantity=f"elasticity_{sink}", width=width,
                n=int(eps.size), n_live=int(live.sum()),
                frac_floor=float(1 - live.mean()), ceff=float(sol.total),
                n_eff=float(1.0 / np.sum(eps.to_numpy() ** 2)),
                decades_full=full, decades_5_95=band, **rel,
                **gene_stats(gene_responses(eps)),
                edge_ratio_decades_full=edge_full, edge_ratio_decades_5_95=edge_band,
                gp_decades_full=gp_full, gp_decades_5_95=gp_band))
            print(f"  {host} {sink} width={width:>4}: C_eff={sol.total:.6g} "
                  f"n_eff={rows[-1]['n_eff']:.2f} decades(5-95)={band:.2f} "
                  f"cover90={rows[-1]['n_cover90']} rxn spanning "
                  f"{rows[-1]['decades_cover90']:.2f} decades", file=sys.stderr)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--hosts", nargs="+", default=["e_coli_k12", "e_coli_ag1"])
    p.add_argument("--out", default=str(OUT_DIR / "decade_budget.tsv"))
    a = p.parse_args()

    pairs, ann = load_bake()
    print(f"bake: {len(pairs):,} carbon pair rows, {len(ann):,} direction rows",
          file=sys.stderr)

    rows: list = []
    for host in a.hosts:
        gpr = pd.read_parquet(RUNS / host / "gpr" / "gpr_gem.parquet")
        assert set(gpr.lane_set.unique()) == {"curated"}, f"{host} is not the curated arm"
        weights = {m: 1.0 for m in gpr.mnxr.dropna().unique()}
        print(f"\n== {host}: {len(weights):,} reactions", file=sys.stderr)
        input_census(rows, host, pairs, ann, weights)
        sweep(rows, host, pairs, ann, weights)

    df = pd.DataFrame(rows)
    out = Path(a.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, sep="\t", index=False)
    print(f"\nwrote {out}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

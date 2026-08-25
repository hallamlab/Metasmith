#!/usr/bin/env python3
"""Every ASKA clone, TWO glycogen readouts each, scored as their ratio. The library sweep.

    mamba run -n ecspr python research/fabfos/benchmarks/eydallin/sweep_aska_ratio.py \
        --channel gem --workers 4
    ... --channel denovo --workers 4

`sweep_aska.py` is this measurement with one probe: glucose -> glycogen, and a clone's score
is how far a x2 fold on its reactions moves that conductance. Everything about the basis, the
background rule, the fold and the terminals is unchanged here. What changes is that a SECOND
two-point conductance is solved beside the first -- glycogen -> pyruvate, the catabolic arm --
and the score is the ratio of the two:

    ratio = C(glucose -> glycogen) / C(glycogen -> pyruvate)

A x2 fold on any reaction raises both conductances, because Rayleigh binds each of them
separately; what the ratio removes is the part of that rise which is network-wide rather than
about glycogen. On the 45-condition Eydallin panel the ratio's sign matched the measured
glycogen change 30/45 against 10/45 for the numerator alone, which is the whole reason to
carry a second solve per clone.

THE TWO CONDUCTANCES ARE INDEPENDENT SOLVES on the same graph, not two readings of one. Each
builds its own terminal pair; only the graph and its weights are shared. Doubling the probes
doubles the per-clone cost, and that is the price of the normalisation.

Everything `sweep_aska.py`'s docstring says about the background still holds, verbatim: each
channel folds against its own background and the two are never mixed, the fold is flat at 1.0
in both channels, a clone with no atom-mapped reaction gets a row carrying an exact zero
rather than an absence, and rows append as they finish so an interrupted sweep resumes.
"""
from __future__ import annotations

import os

for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS"):
    os.environ.setdefault(_v, "1")

import argparse                                                        # noqa: E402
import csv                                                             # noqa: E402
import multiprocessing as mp                                           # noqa: E402
import sys                                                             # noqa: E402
import time                                                            # noqa: E402
from pathlib import Path                                               # noqa: E402

import pandas as pd                                                    # noqa: E402

ROOT = Path(__file__).resolve().parents[5]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ecspr.model.build import load_pairs, load_direction_ratios, graph_from_pairs  # noqa: E402
from ecspr.model.graph import Terminal, solve                                      # noqa: E402
import bake_pairs                                                                  # noqa: E402

ASKA_GPR = ROOT / "data/fabfos/runs/aska/gpr"
HOST_GEM = ROOT / "data/fabfos/runs/e_coli_ag1/gpr/gpr_gem.parquet"
HOST_DENOVO = ROOT / "data/fabfos/runs/e_coli_ag1/gpr/gpr_denovo.parquet"
OUT_DIR = ROOT / "data/fabfos/runs/aska/ecspr"

GLUCOSE_MNXM = "MNXM1364061"
GLYCOGEN_MNXM = "MNXM738130"
PYRUVATE_MNXM = "MNXM23"

FIELDS = ("condition_id", "gene", "n_rxn", "over_gg", "over_gp", "over_ratio",
          "delta_ratio_pct", "rxns")

_S: dict = {}


def _probe(g, src_mnxm: str, snk_mnxm: str) -> float:
    src = Terminal.metabolite(g, src_mnxm, label=src_mnxm)
    snk = Terminal.metabolite(g, snk_mnxm, label=snk_mnxm)
    if src.missing or snk.missing:
        raise SystemExit(f"[sweep] terminal missing: src={src.missing} snk={snk.missing}")
    return float(solve(g, src, snk).total)


def _ratio(weights: dict) -> tuple:
    g = graph_from_pairs(_S["pairs"], _S["element"], weights, _S["ratios"])
    gg = _probe(g, GLUCOSE_MNXM, GLYCOGEN_MNXM)
    gp = _probe(g, GLYCOGEN_MNXM, PYRUVATE_MNXM)
    return gg, gp, gg / gp


def _one(task):
    gene, rxns = task
    base_w, fold = _S["base_w"], _S["fold"]
    gg, gp, r = _ratio({**base_w, **{x: base_w[x] * fold for x in rxns}})
    host_r = _S["host_ratio"]
    return dict(condition_id=f"aska:{gene}", gene=gene, n_rxn=len(rxns),
                over_gg=gg, over_gp=gp, over_ratio=r,
                delta_ratio_pct=100.0 * (r - host_r) / host_r,
                rxns=",".join(rxns))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--channel", choices=("gem", "denovo"), required=True)
    ap.add_argument("--fold", type=float, default=2.0)
    ap.add_argument("--element", default="C")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--min-lanes", type=int, default=1,
                    help="de-novo only: credit the CLONE with a reaction only when at "
                         "least this many of the four annotation lanes assert it. The "
                         "background is left whole -- applying the same rule to it "
                         "disconnects glycogen outright, because the glycogen-synthesis "
                         "step has single-lane support, so there would be no probe left "
                         "to run.")
    ap.add_argument("--limit", type=int, default=None, help="first N solvable clones (smoke test)")
    ap.add_argument("--ratio-cap", type=float, default=None,
                    help="bound |log10 direction ratio| at this many decades before the "
                         "graph is built (ecspr.model.build.cap_direction_ratios); the arm "
                         "is tagged so a capped run cannot be mistaken for an uncapped one")
    ap.add_argument("--out-dir", type=Path, default=OUT_DIR)
    a = ap.parse_args()
    a.out_dir.mkdir(parents=True, exist_ok=True)

    if a.min_lanes > 1 and a.channel != "denovo":
        raise SystemExit("--min-lanes applies to the de-novo channel; the GEM channel is "
                         "one curated lane and has nothing to agree with")
    tag = (f"aska_ratio_sweep_{a.channel}_e_coli_ag1_fold{a.fold}_{a.element}"
           + (f"_lanes{a.min_lanes}" if a.min_lanes > 1 else "")
           + (f"_cap{a.ratio_cap:g}" if a.ratio_cap is not None else ""))
    part = a.out_dir / f"{tag}.partial.tsv"
    final = a.out_dir / f"{tag}.tsv"

    t0 = time.time()
    _S["pairs"] = load_pairs(bake_pairs.atom_pairs(), element=a.element)
    _S["ratios"] = load_direction_ratios(bake_pairs.direction_ratios(), cap=a.ratio_cap)
    _S["element"], _S["fold"] = a.element, a.fold

    host_path = HOST_GEM if a.channel == "gem" else HOST_DENOVO
    host = pd.read_parquet(host_path, columns=["mnxr"])
    base_w = {m: 1.0 for m in host.mnxr.dropna().astype(str).unique()}
    _S["base_w"] = base_w

    clone = pd.read_parquet(ASKA_GPR / f"gpr_{a.channel}.parquet")
    clone = clone[clone.in_atom_universe.fillna(False)]
    if a.min_lanes > 1:
        agree = clone.groupby(["condition_id", "mnxr"]).channel.nunique()
        clone = clone[pd.MultiIndex.from_arrays([clone.condition_id, clone.mnxr])
                      .isin(agree[agree >= a.min_lanes].index)]
    absent = sorted(set(clone.mnxr.astype(str)) - set(base_w))
    if absent:
        raise SystemExit(f"[sweep] {len(absent)} clone reaction(s) are not in the "
                         f"{a.channel} background, e.g. {absent[:5]}")

    census = pd.read_csv(ASKA_GPR / "clone_census.tsv", sep="\t").drop_duplicates("gene")
    by_gene = (clone.assign(g=clone.condition_id.str.replace("aska:", "", regex=False))
               .groupby("g").mnxr.apply(lambda s: sorted(set(s.astype(str)))).to_dict())

    host_gg, host_gp, host_ratio = _ratio(base_w)
    _S["host_ratio"] = host_ratio
    print(f"[sweep] {a.channel}: {len(base_w):,} background reactions | "
          f"glucose->glycogen {host_gg:.9f} | glycogen->pyruvate {host_gp:.9f} | "
          f"host ratio {host_ratio:.9f} | setup {time.time() - t0:.1f}s", file=sys.stderr)

    done = set()
    if part.exists():
        done = set(pd.read_csv(part, sep="\t").gene.astype(str))
        print(f"[sweep] resuming: {len(done):,} clones already measured", file=sys.stderr)

    todo = [(g, by_gene[g]) for g in sorted(by_gene) if g not in done]
    if a.limit:
        todo = todo[:a.limit]
    print(f"[sweep] {len(census):,} clone genes | {len(by_gene):,} atom-mapped | "
          f"{len(todo):,} to solve on {a.workers} worker(s)", file=sys.stderr)

    new = part.exists() is False
    with part.open("a", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(FIELDS), delimiter="\t")
        if new:
            w.writeheader()
        t = time.time()
        with mp.Pool(a.workers) as pool:
            for i, rec in enumerate(pool.imap_unordered(_one, todo, chunksize=4), 1):
                w.writerow(rec)
                if i % 25 == 0:
                    fh.flush()
                    el = time.time() - t
                    print(f"  {i:5}/{len(todo)}  {el / i:.2f}s/clone  "
                          f"eta {(len(todo) - i) * el / i / 60:.1f} min", file=sys.stderr)

    solved = pd.read_csv(part, sep="\t")
    df = census[["gene", "condition_id", "b_number", "eydallin_gene",
                 "eydallin_phenotype"]].copy()
    df = df.merge(solved.drop(columns=["condition_id"]), on="gene", how="left")
    df["n_rxn"] = df.n_rxn.fillna(0).astype(int)
    df["host_gg"], df["host_gp"], df["host_ratio"] = host_gg, host_gp, host_ratio
    df["over_gg"] = df.over_gg.fillna(host_gg)
    df["over_gp"] = df.over_gp.fillna(host_gp)
    df["over_ratio"] = df.over_ratio.fillna(host_ratio)
    df["delta_ratio_pct"] = df.delta_ratio_pct.fillna(0.0)
    df["rxns"] = df.rxns.fillna("")
    df["channel"] = a.channel
    df["is_positive"] = df.eydallin_gene.notna() & (df.eydallin_gene.astype(str) != "")
    df = df.sort_values("delta_ratio_pct", ascending=False).reset_index(drop=True)
    df.to_csv(final, sep="\t", index=False)

    n_solved = int((df.n_rxn > 0).sum())
    print(f"\n[sweep] wrote {final}\n"
          f"    {len(df):,} clone genes | {n_solved:,} solved | "
          f"{len(df) - n_solved:,} exact zeros\n"
          f"    positives {int(df.is_positive.sum())} "
          f"({int((df.is_positive & (df.n_rxn > 0)).sum())} atom-mapped)\n"
          f"    total {(time.time() - t0) / 60:.1f} min", file=sys.stderr)
    print(df[df.is_positive].head(15)[["gene", "eydallin_phenotype", "n_rxn",
                                       "delta_ratio_pct"]].to_string(index=False),
          file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())

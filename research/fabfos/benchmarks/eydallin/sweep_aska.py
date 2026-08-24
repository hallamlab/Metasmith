#!/usr/bin/env python3
"""Every ASKA clone, one glucose -> glycogen readout each. The library sweep.

    mamba run -n ecspr python research/fabfos/benchmarks/eydallin/sweep_aska.py \
        --channel gem --workers 4
    ... --channel denovo --workers 4

`twopoint_cohort.py` runs this probe over the 25 eydallin genes the curated model can see.
This is the same probe over the whole library, and nothing about the measurement changes:
same basis, same graph builder, same two-point solve, same x2 fold on the named clone's
reactions, same terminals. Only the gene list widens -- from the genes the paper reported
to the genes the screen assayed. The point of the widening is the denominator: an AUC needs
four thousand measured negatives, and Eydallin's screen is the only cohort in this tree that
has them.

THE BACKGROUND IS THE HOST AS THAT CHANNEL SEES IT, and the two channels therefore have
different backgrounds -- 2,022 atom-mapped reactions curated, 10,998 de-novo. That is not a
inconsistency to be papered over by unioning them. `graph_from_pairs` builds a graph out of
exactly the reactions its weight dict names, so the weight dict IS the organism; mixing a
curated background with de-novo clone edges would let a clone introduce a reaction its own
background never had, which is a different perturbation (creation) than the one ASKA
performs (a second copy). Every ASKA gene is a chromosomal E. coli ORF, so under either
channel the clone's reactions are already in that channel's background and the fold is
purely a widening. The two channels' absolute conductances are consequently NOT comparable
to each other; only ranks within a channel are.

FOLD IS FLAT AT 1.0 IN BOTH CHANNELS. The de-novo table carries a per-row `raw_score`
spanning four orders of magnitude, and it is deliberately unused: it is not normalised
across the four lanes, so a score-weighted fold in one channel against a flat fold in the
other would make the two rankings incomparable in a way that has nothing to do with the
biology.

A CLONE WITH NO ATOM-MAPPED REACTION IS AN EXACT ZERO, not an absence. It is never solved --
folding nothing is the identity -- but it gets its row, because it is a clone the screen
built and assayed and the method cannot see. Those rows are most of the library and they are
what the AUC's tie structure is made of.

Rows are appended as they finish, so an interrupted sweep resumes for free.
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

import numpy as np                                                     # noqa: E402
import pandas as pd                                                    # noqa: E402

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from ecspr.model.build import load_pairs, load_direction_ratios, graph_from_pairs  # noqa: E402
from ecspr.model.graph import Terminal, solve, measure_leak                        # noqa: E402
import bake_pairs                                                                  # noqa: E402

ASKA_GPR = ROOT / "data/fabfos/runs/aska/gpr"
HOST_GEM = ROOT / "data/fabfos/runs/e_coli_ag1/gpr/gpr_gem.parquet"
HOST_DENOVO = ROOT / "data/fabfos/runs/e_coli_ag1/gpr/gpr_denovo.parquet"
OUT_DIR = ROOT / "data/fabfos/runs/eydallin_clones/ecspr"

SOURCE_MNXM = "MNXM1364061"
GLYCOGEN_MNXM = "MNXM738130"

FIELDS = ("condition_id", "gene", "n_rxn", "ieff_pert", "delta", "rxns")

_S: dict = {}


def _ieff(weights: dict) -> float:
    g = graph_from_pairs(_S["pairs"], _S["element"], weights, _S["ratios"])
    src = Terminal.metabolite(g, SOURCE_MNXM, label="glucose")
    if src.missing:
        raise SystemExit("[sweep] source missing")
    if _S["probe"] == "share":
        r = measure_leak(g, src, _S["prec"], leak=_S["leak"], port=1.0)
        return float(r["draw"].get(GLYCOGEN_MNXM, 0.0))
    snk = Terminal.metabolite(g, GLYCOGEN_MNXM, label="glycogen")
    if snk.missing:
        raise SystemExit(f"[sweep] terminal missing: {snk.missing}")
    return float(solve(g, src, snk).total)


def _one(task):
    gene, rxns = task
    base_w, fold = _S["base_w"], _S["fold"]
    v = _ieff({**base_w, **{r: base_w[r] * fold for r in rxns}})
    return dict(condition_id=f"aska:{gene}", gene=gene, n_rxn=len(rxns),
                ieff_pert=v, delta=v - _S["base"], rxns=",".join(rxns))


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
    ap.add_argument("--probe", choices=("twopoint", "share"), default="twopoint",
                    help="`share` grounds at the biomass precursors instead of at glycogen "
                         "and reads glycogen's fraction of the injected carbon, which is "
                         "what Fig. 1 measures and the only form of this readout that can "
                         "go DOWN (monotonicity_ladder.py)")
    ap.add_argument("--ground", choices=("leak", "glycogen", "biomass"), default="biomass",
                    help="share probe only: what competes with glycogen for the carbon")
    ap.add_argument("--leak", type=float, default=1e-3, help="share probe only")
    ap.add_argument("--ratio-override", default="",
                    help="`MNXR...:ratio,...` applied on top of the baked direction "
                         "ensemble. The ensemble ABSTAINS on the polymer reactions -- an "
                         "explicit ratio of 1.0 from zero votes -- so this is how a "
                         "direction it does not have gets supplied and the answer "
                         "re-measured under it. Recorded in the output filename, because a "
                         "sweep run under an override is a different measurement.")
    ap.add_argument("--limit", type=int, default=None, help="first N solvable clones (smoke test)")
    ap.add_argument("--ratio-cap", type=float, default=None,
                    help="bound |log10 direction ratio| at this many decades before the "
                         "graph is built (ecspr.model.build.cap_direction_ratios). The "
                         "shipped table spans 28.7 decades over a host's reactions and "
                         "saturates by 6; the arm is tagged so a capped run cannot be "
                         "mistaken for an uncapped one")
    ap.add_argument("--out-dir", type=Path, default=OUT_DIR)
    a = ap.parse_args()
    a.out_dir.mkdir(parents=True, exist_ok=True)

    if a.min_lanes > 1 and a.channel != "denovo":
        raise SystemExit("--min-lanes applies to the de-novo channel; the GEM channel is "
                         "one curated lane and has nothing to agree with")
    override = {}
    for item in filter(None, a.ratio_override.split(",")):
        k, v = item.split(":")
        override[k.strip()] = float(v)
    tag = (f"aska_sweep_{a.channel}_e_coli_ag1_fold{a.fold}_{a.element}"
           + (f"_{a.probe}{a.ground}{a.leak:g}" if a.probe != "twopoint" else "")
           + (f"_lanes{a.min_lanes}" if a.min_lanes > 1 else "")
           + (f"_dir{len(override)}x{min(override.values()):g}" if override else "")
           + (f"_cap{a.ratio_cap:g}" if a.ratio_cap is not None else ""))
    part = a.out_dir / f"{tag}.partial.tsv"
    final = a.out_dir / f"{tag}.tsv"

    t0 = time.time()
    _S["pairs"] = load_pairs(bake_pairs.atom_pairs(), element=a.element)
    _S["ratios"] = load_direction_ratios(bake_pairs.direction_ratios(),
                                         cap=a.ratio_cap)
    if override:
        was = {k: _S["ratios"].get(k) for k in override}
        _S["ratios"].update(override)
        print(f"[sweep] direction override: {was} -> {override}", file=sys.stderr)
    _S["element"], _S["fold"] = a.element, a.fold
    _S["probe"], _S["leak"] = a.probe, a.leak

    host_path = HOST_GEM if a.channel == "gem" else HOST_DENOVO
    host = pd.read_parquet(host_path, columns=["mnxr"])
    base_w = {m: 1.0 for m in host.mnxr.dropna().astype(str).unique()}
    _S["base_w"] = base_w

    _S["prec"] = None
    if a.probe == "share":
        import glycogen_share as GS
        g0 = graph_from_pairs(_S["pairs"], a.element, base_w, _S["ratios"])
        univ = set(g0.metabolites())
        _S["prec"] = {"leak": None, "glycogen": [GLYCOGEN_MNXM],
                      "biomass": sorted(set(GS.biomass_precursors(univ))
                                        | {GLYCOGEN_MNXM})}[a.ground]
        print(f"[sweep] share probe: ground={a.ground} leak={a.leak} "
              f"({len(_S['prec']) if _S['prec'] else 0} ports)", file=sys.stderr)

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

    _S["base"] = _ieff(base_w)
    print(f"[sweep] {a.channel}: {len(base_w):,} background reactions | "
          f"base glucose->glycogen conductance = {_S['base']:.9f} | "
          f"setup {time.time() - t0:.1f}s", file=sys.stderr)

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
    df["ieff_base"] = _S["base"]
    df["ieff_pert"] = df.ieff_pert.fillna(_S["base"])
    df["delta"] = df.delta.fillna(0.0)
    df["log2fc_ieff"] = np.log2(df.ieff_pert / df.ieff_base)
    df["rxns"] = df.rxns.fillna("")
    df["channel"] = a.channel
    df["is_positive"] = df.eydallin_gene.notna() & (df.eydallin_gene.astype(str) != "")
    df = df.sort_values("delta", ascending=False).reset_index(drop=True)
    df.to_csv(final, sep="\t", index=False)

    n_solved = int((df.n_rxn > 0).sum())
    print(f"\n[sweep] wrote {final}\n"
          f"    {len(df):,} clone genes | {n_solved:,} solved | "
          f"{len(df) - n_solved:,} exact zeros\n"
          f"    positives {int(df.is_positive.sum())} "
          f"({int((df.is_positive & (df.n_rxn > 0)).sum())} atom-mapped)\n"
          f"    total {(time.time() - t0) / 60:.1f} min", file=sys.stderr)
    print(df[df.is_positive].head(15)[["gene", "eydallin_phenotype", "n_rxn", "delta",
                                       "log2fc_ieff"]].to_string(index=False),
          file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())

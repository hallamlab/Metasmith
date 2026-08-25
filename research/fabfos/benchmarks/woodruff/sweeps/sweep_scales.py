#!/usr/bin/env python3
"""Every gene the SCALEs tolerance screen measured, over the whole declared axis panel.

    mamba run -n build-refs-cobra python research/fabfos/benchmarks/woodruff/sweeps/sweep_scales.py \
        --channel gem --workers 6
    ... --shard 3 --nshards 64        # one slice, for a cluster array

THE SWEEP IS ONE GRAPH BUILD PER GENE, NOT ONE PER (GENE, AXIS). Perturbing a gene changes
the weight dict, and the weight dict is the organism; the eight declared axes are eight
terminal pairs read off that one organism. Building per axis would repeat the expensive
half eight times for an identical graph. That is the whole reason this runs in minutes on a
workstation rather than needing the array the plan sized: 985 builds, not 7,880.

THE WEIGHT-DICT CONVENTION IS THE `in_atom_universe` FILTER, and this is the file that
settles it for the benchmark. `sweep_aska.py` takes every host reaction; `sink_panel.py`
takes the atom-universe subset. The two differ by transport, which carries atom pairs and
therefore real edges -- an unfiltered K-12 background uses 2,013 reactions against the
filtered 1,366. Transport moves a metabolite between compartments without transforming it,
so leaving it in lets carbon reach a sink through a periplasmic shortcut that performs no
chemistry. The mechanistic arm was run filtered; the classifier arm is run filtered; the
two arms therefore sit on one background and their numbers are comparable. Anything
compared against the eydallin sweep is NOT, and the report says so.

FOLD IS FLAT AT 1.0 ACROSS REACTIONS. The de-novo table's per-row `raw_score` spans four
orders of magnitude and is not normalised across the four lanes, so weighting by it in one
channel against a flat fold in the other would make the two rankings incomparable for
reasons that are not biology.

A GENE WITH NO ATOM-MAPPED REACTION IS AN EXACT ZERO, not an absence. It is never solved --
folding nothing is the identity -- but it gets its row on every axis, because it is a gene
the screen measured and this method cannot see. Those rows are two thirds of the population
and they are what the AUC's tie structure is made of. Dropping them would silently redefine
the population as "genes the model happens to carry".

SHARDING MUST NOT CHANGE A NUMBER. A shard is a contiguous slice of the sorted gene list;
each writes its own file and nothing appends to a shared one, so a failed index is
resubmitted alone. `--merge` refuses unless the shards it finds cover the gene list exactly
once -- a short shard is a refusal, never a silent gap.
"""
from __future__ import annotations

import os

for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS",
           "NUMEXPR_NUM_THREADS"):
    os.environ.setdefault(_v, "1")

import argparse                                                        # noqa: E402
import csv                                                             # noqa: E402
import json                                                            # noqa: E402
import multiprocessing as mp                                           # noqa: E402
import sys                                                             # noqa: E402
import time                                                            # noqa: E402
from pathlib import Path                                               # noqa: E402

import numpy as np                                                     # noqa: E402
import pandas as pd                                                    # noqa: E402

READ_EXACT = dict(sep="\t", float_precision="round_trip")


def _repo_root(start: Path) -> Path:
    for d in (start, *start.parents):
        if (d / "data" / "fabfos").is_dir():
            return d
    raise SystemExit(f"no ancestor of {start} contains data/fabfos")


ROOT = _repo_root(Path(__file__).resolve())
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "research/fabfos/benchmarks/eydallin"))

import bake_pairs                                                                  # noqa: E402
sys.path.insert(0, str(ROOT / "src/fabfos/build_references/resources/buildlib"))
import bench_universe as bu                                                        # noqa: E402
from ecspr.model.build import graph_from_pairs, load_direction_ratios, load_pairs  # noqa: E402
from ecspr.model.graph import Terminal, solve                                      # noqa: E402

GPR = ROOT / "data/fabfos/runs/woodruff_clones/gpr"
PANEL_TSV = ROOT / "data/fabfos/originals/benchmarks/woodruff/gof_scales.tsv"
BAKE = ROOT / "data/fabfos/processed/metabolism_bake"
METANETX = ROOT / "data/fabfos/originals/metanetx"
OUT_DIR = ROOT / "data/fabfos/runs/woodruff_clones/ecspr"

HOST = "e_coli_bw25113"
COHORT = "scales_tol"

FIELDS = ("gene", "gene_norm", "n_rxn", "axis", "sink_mnxm", "ieff_pert", "delta", "rxns")

_S: dict = {}


def host_weights(host: str, channel: str, universe: set | None = None) -> dict:
    if channel == "gem":
        p = ROOT / f"data/fabfos/runs/{host}/gpr/gpr_gem.parquet"
        df = pd.read_parquet(p, columns=["mnxr", "in_atom_universe"])
        keep = df.in_atom_universe.fillna(False).astype(bool)
    else:
        p = ROOT / f"data/fabfos/runs/{host}/gpr/gpr_denovo.parquet"
        df = pd.read_parquet(p, columns=["mnxr", "in_atom_universe"])
        if df.in_atom_universe.notna().any():
            raise SystemExit(
                f"{p.relative_to(ROOT)} has a non-null in_atom_universe; it used to arrive "
                f"entirely null and be recomputed here. Check which flag is authoritative "
                f"before trusting either.")
        if universe is None:
            raise SystemExit("the de-novo background needs the atom universe to recompute")
        keep = df.mnxr.astype(str).isin(universe)
    return {m: 1.0 for m in sorted(df[keep].mnxr.dropna().astype(str).unique())}


def read_axes(path: Path) -> list[dict]:
    df = pd.read_csv(path, sep="\t", dtype=str).fillna("")
    tgt = df[df.role == "target"]
    if tgt.empty:
        raise SystemExit(f"no role=target rows in {path}")
    src = set(tgt.src_mnxm)
    if len(src) != 1:
        raise SystemExit(f"panel declares more than one source: {sorted(src)}")
    return tgt.to_dict("records")


def _solve_all(weights: dict) -> dict[str, float]:
    g = graph_from_pairs(_S["pairs"], _S["element"], weights, _S["ratios"])
    src = Terminal.metabolite(g, _S["src_mnxm"], label="source")
    if src.missing:
        raise SystemExit(f"[sweep] source {_S['src_mnxm']} is not a node of this graph")
    out = {}
    for ax in _S["axes"]:
        snk = Terminal.metabolite(g, ax["sink_mnxm"], label=ax["sink_name"])
        if snk.missing:
            raise SystemExit(f"[sweep] sink {ax['sink_mnxm']} ({ax['gene']}) is not a node")
        out[ax["gene"]] = float(solve(g, src, snk).total)
    return out


def _one(task):
    gene, norm, rxns = task
    base_w, fold = _S["base_w"], _S["fold"]
    vals = _solve_all({**base_w, **{r: base_w[r] * fold for r in rxns}})
    return [dict(gene=gene, gene_norm=norm, n_rxn=len(rxns), axis=ax,
                 sink_mnxm=_S["sink_of"][ax], ieff_pert=v, delta=v - _S["base"][ax],
                 rxns=",".join(rxns))
            for ax, v in vals.items()]


def shard_slice(items: list, shard: int, nshards: int) -> list:
    if not 0 <= shard < nshards:
        raise SystemExit(f"--shard {shard} outside 0..{nshards - 1}")
    lo = (len(items) * shard) // nshards
    hi = (len(items) * (shard + 1)) // nshards
    return items[lo:hi]


def merge(out_dir: Path, tag: str, nshards: int, expect: list[str]) -> Path:
    found, missing = [], []
    for k in range(nshards):
        p = out_dir / f"{tag}.shard{k:04d}of{nshards:04d}.tsv"
        (found if p.exists() else missing).append(p)
    if missing:
        raise SystemExit(f"[merge] {len(missing)} shard(s) absent, e.g. "
                         f"{[p.name for p in missing[:5]]} -- resubmit those indices")
    df = pd.concat([pd.read_csv(p, **READ_EXACT) for p in found], ignore_index=True)
    got = sorted(df.gene_norm.astype(str).unique())
    if got != sorted(expect):
        extra, lost = set(got) - set(expect), set(expect) - set(got)
        raise SystemExit(f"[merge] shard union does not cover the gene list exactly once: "
                         f"{len(lost)} missing {sorted(lost)[:5]}, "
                         f"{len(extra)} unexpected {sorted(extra)[:5]}")
    dup = df.groupby(["gene_norm", "axis"]).size()
    if (dup > 1).any():
        raise SystemExit(f"[merge] {(dup > 1).sum()} (gene, axis) pair(s) appear twice")
    out = out_dir / f"{tag}.solved.tsv"
    df.to_csv(out, sep="\t", index=False)
    print(f"[merge] {len(found)} shards -> {len(df):,} rows, {len(got):,} genes -> {out}",
          file=sys.stderr)
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--channel", choices=("gem", "denovo"), default="gem")
    ap.add_argument("--fold", type=float, default=2.0)
    ap.add_argument("--element", default="C")
    ap.add_argument("--workers", type=int, default=6)
    ap.add_argument("--shard", type=int, default=None)
    ap.add_argument("--nshards", type=int, default=1)
    ap.add_argument("--merge", action="store_true",
                    help="union the shards of this tag and write the solved table")
    ap.add_argument("--universe", type=Path, default=None,
                    help="newline-separated MNXR list standing in for the computed atom "
                         "universe. Lets a cluster shard run without the 1.5 G MetaNetX "
                         "tree; write it with --dump-universe and stage that file.")
    ap.add_argument("--dump-universe", type=Path, default=None,
                    help="compute the atom universe, write it to this path, and exit")
    ap.add_argument("--limit", type=int, default=None, help="first N solvable genes (smoke)")
    ap.add_argument("--out-dir", type=Path, default=OUT_DIR)
    a = ap.parse_args()
    a.out_dir.mkdir(parents=True, exist_ok=True)

    tag = f"scales_sweep_{a.channel}_{HOST}_fold{a.fold}_{a.element}"
    t0 = time.time()

    if a.dump_universe is not None:
        u, ustats = bu.atom_universe(
            BAKE / "vocab.parquet", BAKE / "atom_pairs.parquet",
            exclude=bu.transport_mnxrs(bu.reac_prop_path(METANETX)))
        a.dump_universe.write_text("\n".join(sorted(u)) + "\n")
        print(bu.universe_line(ustats, "scales"), file=sys.stderr)
        print(f"[sweep] wrote {len(u):,} reactions -> {a.dump_universe}", file=sys.stderr)
        return 0

    census = pd.read_csv(GPR / "gene_census.tsv", sep="\t", dtype=str).fillna("")
    gpr_path = GPR / f"gpr_{a.channel}.parquet"
    if not gpr_path.exists():
        raise SystemExit(f"[sweep] {gpr_path.relative_to(ROOT)} does not exist -- "
                         f"build_scales_gpr.py has not built the {a.channel} channel")
    clone = pd.read_parquet(gpr_path)
    clone = clone[clone.in_atom_universe.fillna(False).astype(bool)]
    by_gene = (clone.assign(g=clone.condition_id.str.replace(f"{COHORT}:", "", regex=False))
               .groupby("g").mnxr.apply(lambda s: sorted(set(s.astype(str)))).to_dict())
    norm_to_gene = dict(zip(census.gene_norm, census.gene))
    todo_all = [(norm_to_gene[n], n, by_gene[n]) for n in sorted(by_gene)]
    if a.limit:
        todo_all = todo_all[:a.limit]

    if a.merge:
        merge(a.out_dir, tag, a.nshards, [n for _, n, _ in todo_all])
        return 0

    _S["pairs"] = load_pairs(bake_pairs.atom_pairs(), element=a.element)
    _S["ratios"] = load_direction_ratios(bake_pairs.direction_ratios())
    _S["element"], _S["fold"] = a.element, a.fold
    axes = read_axes(PANEL_TSV)
    _S["axes"] = axes
    _S["src_mnxm"] = axes[0]["src_mnxm"]
    _S["sink_of"] = {ax["gene"]: ax["sink_mnxm"] for ax in axes}

    universe = None
    if a.universe is not None:
        universe = {l.strip() for l in a.universe.open() if l.strip()}
        print(f"[sweep] atom universe read from {a.universe}: {len(universe):,} reactions",
              file=sys.stderr)
    elif a.channel == "denovo":
        universe, ustats = bu.atom_universe(
            BAKE / "vocab.parquet", BAKE / "atom_pairs.parquet",
            exclude=bu.transport_mnxrs(bu.reac_prop_path(METANETX)))
        print(bu.universe_line(ustats, "scales"), file=sys.stderr)
    base_w = host_weights(HOST, a.channel, universe)
    _S["base_w"] = base_w
    absent = sorted(set(clone.mnxr.astype(str)) - set(base_w))
    if absent:
        raise SystemExit(f"[sweep] {len(absent)} gene reaction(s) are not in the "
                         f"{a.channel} background, e.g. {absent[:5]}. A weight dict is the "
                         f"organism; an edge outside it is a refusal, not a dropped row.")
    _S["base"] = _solve_all(base_w)
    print(f"[sweep] {a.channel}: {len(base_w):,} background reactions, {len(axes)} axes | "
          f"setup {time.time() - t0:.1f}s", file=sys.stderr)
    for ax in axes:
        print(f"    base {ax['gene']:20} {_S['base'][ax['gene']]:.9f}", file=sys.stderr)

    if a.shard is None:
        todo, part = todo_all, a.out_dir / f"{tag}.partial.tsv"
    else:
        todo = shard_slice(todo_all, a.shard, a.nshards)
        part = a.out_dir / f"{tag}.shard{a.shard:04d}of{a.nshards:04d}.tsv"

    done = set()
    if part.exists():
        done = set(pd.read_csv(part, **READ_EXACT).gene_norm.astype(str))
        print(f"[sweep] resuming: {len(done):,} genes already solved", file=sys.stderr)
    todo = [t for t in todo if t[1] not in done]
    print(f"[sweep] {len(census):,} measured genes | {len(by_gene):,} atom-mapped | "
          f"{len(todo):,} to solve on {a.workers} worker(s)", file=sys.stderr)

    new = not part.exists()
    with part.open("a", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(FIELDS), delimiter="\t")
        if new:
            w.writeheader()
        t = time.time()
        with mp.Pool(a.workers) as pool:
            for i, recs in enumerate(pool.imap_unordered(_one, todo, chunksize=2), 1):
                w.writerows(recs)
                if i % 25 == 0:
                    fh.flush()
                    el = time.time() - t
                    print(f"  {i:5}/{len(todo)}  {el / i:.2f}s/gene  "
                          f"eta {(len(todo) - i) * el / i / 60:.1f} min", file=sys.stderr)

    if a.shard is not None:
        print(f"[sweep] shard {a.shard} done -> {part}", file=sys.stderr)
        return 0

    solved = pd.read_csv(part, **READ_EXACT)
    keep = ["gene", "gene_norm", "bnum_from_name", "phenotype", "fitness_15", "fitness_30",
            "gem_resolved_via", "gem_n_in_universe", "denovo_resolved_via",
            "denovo_n_in_universe"]
    frames = []
    for ax in axes:
        name = ax["gene"]
        d = census[[c for c in keep if c in census.columns]].copy()
        d["axis"] = name
        d["sink_mnxm"] = ax["sink_mnxm"]
        d["expected_dir"] = ax["expected_dir"]
        d = d.merge(solved[solved.axis == name].drop(columns=["gene", "axis", "sink_mnxm"]),
                    on="gene_norm", how="left")
        d["n_rxn"] = d.n_rxn.fillna(0).astype(int)
        d["ieff_base"] = _S["base"][name]
        d["ieff_pert"] = d.ieff_pert.fillna(_S["base"][name])
        d["delta"] = d.delta.fillna(0.0)
        d["log2fc_ieff"] = (np.log2(d.ieff_pert / d.ieff_base)
                            if _S["base"][name] > 0 else np.nan)
        d["rxns"] = d.rxns.fillna("")
        frames.append(d)
    df = pd.concat(frames, ignore_index=True)
    df["channel"] = a.channel
    df["host"] = HOST
    df["fold"] = a.fold
    final = a.out_dir / f"{tag}.tsv"
    df.to_csv(final, sep="\t", index=False)

    n_solved = int((df[df.axis == axes[0]["gene"]].n_rxn > 0).sum())
    manifest = dict(
        tag=tag, channel=a.channel, host=HOST, cohort=COHORT, fold=a.fold,
        element=a.element, background_reactions=len(base_w),
        weight_dict_convention="in_atom_universe filter, uniform 1.0",
        axes={ax["gene"]: dict(sink=ax["sink_mnxm"], expected_dir=ax["expected_dir"],
                               base=_S["base"][ax["gene"]]) for ax in axes},
        measured_genes=int(len(census)), solved_genes=n_solved,
        exact_zero_genes=int(len(census)) - n_solved,
        rows=int(len(df)), minutes=round((time.time() - t0) / 60, 2))
    (a.out_dir / f"{tag}.BUILD.json").write_text(json.dumps(manifest, indent=2))

    print(f"\n[sweep] wrote {final}\n"
          f"    {len(census):,} measured genes x {len(axes)} axes = {len(df):,} rows\n"
          f"    {n_solved:,} solved | {len(census) - n_solved:,} exact zeros\n"
          f"    total {(time.time() - t0) / 60:.1f} min", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())

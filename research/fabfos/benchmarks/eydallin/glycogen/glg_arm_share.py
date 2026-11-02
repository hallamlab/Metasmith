#!/usr/bin/env python3
"""Of the response the glucose -> glycogen probe has to give, how much is the glg arm?

`REPORT.md` answers "does ECSPr rank Eydallin's genes" and gets no. This answers the
question underneath: how much of the measurement is even AVAILABLE to a gene outside the
glycogen module, and how much of Eydallin's screen is metabolic at all.

The elasticities partition the probe exactly -- every reaction's share of `dlog C_eff`
sums to 1 -- so "how much of the response is the glg arm" is a fraction rather than a
ranking, and the same partition splits three ways: what the 86 hits can reach, what the
rest of the ASKA library can reach, and what no clone in the library carries.

    mamba run -n ecspr python research/fabfos/benchmarks/eydallin/glg_arm_share.py
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[5]
sys.path.insert(0, str(ROOT / "src"))

from ecspr.model.build import (load_pairs, load_direction_ratios,  # noqa: E402
                               graph_from_pairs, reaction_elasticities)
from ecspr.model.graph import Terminal, solve                       # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import bake_pairs  # noqa: E402

RUNS = ROOT / "data/fabfos/runs"
ASKA_GPR = ROOT / "data/fabfos/runs/aska/gpr"
EXTRACTION = ROOT / "data/fabfos/benchmarks/eydallin/extraction.tsv"
MEASURED = ROOT / "data/fabfos/benchmarks/eydallin/Y/measured_glycogen.tsv"
OUT_DIR = ROOT / "data/fabfos/runs/eydallin_clones/ecspr"

SOURCE_MNXM = "MNXM1364061"
GLYCOGEN_MNXM = "MNXM738130"
GLG_GENES = {"glgA", "glgB", "glgC", "glgP", "glgS", "malP", "glgX"}


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--host", default="e_coli_ag1")
    p.add_argument("--channel", default="gem", choices=("gem", "denovo"))
    p.add_argument("--element", default="C")
    p.add_argument("--out-dir", type=Path, default=OUT_DIR)
    args = p.parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)

    pairs = load_pairs(bake_pairs.atom_pairs(), element=args.element)
    ratios = load_direction_ratios(bake_pairs.direction_ratios())
    host = pd.read_parquet(RUNS / args.host / "gpr" / "gpr_gem.parquet")
    base_w = {m: 1.0 for m in host.mnxr.dropna().astype(str).unique()}
    g = graph_from_pairs(pairs, args.element, base_w, ratios, with_provenance=True)
    sol = solve(g, Terminal.metabolite(g, SOURCE_MNXM),
                Terminal.metabolite(g, GLYCOGEN_MNXM))
    eps = reaction_elasticities(g, sol)
    print(f"[glg-share] base conductance {sol.total:.6f}; "
          f"{len(eps)} reactions carry a share, summing to {eps.sum():.10f}", file=sys.stderr)

    gpr = pd.read_parquet(ASKA_GPR / f"gpr_{args.channel}.parquet")
    gpr = gpr[gpr.in_atom_universe.fillna(False)]
    gpr["mnxr"] = gpr.mnxr.astype(str)
    census = pd.read_csv(ASKA_GPR / "clone_census.tsv", sep="\t")
    pheno = dict(zip(census.condition_id, census.eydallin_phenotype))
    hit = {c for c, v in pheno.items() if isinstance(v, str) and v}

    per = gpr.groupby("mnxr").condition_id.apply(lambda s: sorted(set(s)))
    tab = pd.DataFrame({"elasticity": eps})
    tab["clones"] = tab.index.map(lambda r: per.get(r, []))
    tab["n_clone"] = tab.clones.str.len()
    tab["hit_clones"] = tab.clones.apply(lambda cs: sorted(c for c in cs if c in hit))
    tab["genes"] = tab.index.map(
        host.groupby(host.mnxr.astype(str)).feature_name.apply(
            lambda s: ",".join(sorted({x for x in s if isinstance(x, str) and x}))))
    tab["rxn_name"] = tab.index.map(
        host.groupby(host.mnxr.astype(str)).evidence_name.first())
    tab["is_glg"] = tab.genes.fillna("").apply(
        lambda s: bool(GLG_GENES & set(s.split(","))))
    # ratio 1.0 is the direction ensemble's ABSTENTION, not a measured symmetry -- worth
    # carrying beside the elasticity, because a lever the probe leans on hardest and has no
    # direction evidence for is a lever it may be running backwards.
    tab["ratio"] = tab.index.map(ratios).fillna(1.0)

    reachable_hit = tab.hit_clones.str.len() > 0
    reachable_any = tab.n_clone > 0
    split = dict(
        total=float(tab.elasticity.sum()),
        eydallin_86=float(tab.loc[reachable_hit, "elasticity"].sum()),
        eydallin_86_glg=float(tab.loc[reachable_hit & tab.is_glg, "elasticity"].sum()),
        eydallin_86_non_glg=float(tab.loc[reachable_hit & ~tab.is_glg, "elasticity"].sum()),
        aska_non_hit_only=float(tab.loc[reachable_any & ~reachable_hit, "elasticity"].sum()),
        no_clone=float(tab.loc[~reachable_any, "elasticity"].sum()),
    )

    ext = pd.read_csv(EXTRACTION, sep="\t")
    meas = pd.read_csv(MEASURED, sep="\t") if MEASURED.exists() else None
    r2e = eps.to_dict()
    rows = []
    for cid in sorted(hit):
        rs = sorted(set(gpr.loc[gpr.condition_id == cid, "mnxr"]))
        gene = census.loc[census.condition_id == cid, "eydallin_gene"].iloc[0]
        rows.append(dict(condition_id=cid, gene=gene,
                         phenotype=pheno[cid], n_rxn=len(rs),
                         elasticity=float(sum(r2e.get(r, 0.0) for r in rs)),
                         is_glg=gene in GLG_GENES,
                         rxns=",".join(rs)))
    hits = pd.DataFrame(rows).sort_values("elasticity", ascending=False)
    hits = hits.merge(ext[["gene_norm", "function_supplTableS1"]],
                      left_on="gene", right_on="gene_norm", how="left").drop(columns="gene_norm")
    if meas is not None:
        pct = {str(c).split(":")[-1].lower(): v for c, v in zip(meas.condition_id, meas.pct_wt)}
        hits["pct_wt"] = hits.gene.str.lower().map(pct)

    out = args.out_dir / f"glg_arm_share_{args.channel}_{args.host}_{args.element}.tsv"
    tab.reset_index(names="mnxr").to_csv(out, sep="\t", index=False)
    hits.to_csv(out.with_name(out.stem + "_hits.tsv"), sep="\t", index=False)
    with open(out.with_suffix(".json"), "w") as fh:
        json.dump(split, fh, indent=2)
    print(f"[glg-share] wrote {out} (+ _hits.tsv, .json)", file=sys.stderr)

    pd.set_option("display.width", 240)
    print("\n=== how the probe's response (which sums to 1) is spread ===", file=sys.stderr)
    for k, v in split.items():
        print(f"  {k:24} {v:.4f}", file=sys.stderr)

    print("\n=== the 86 hits, by the response mass they can reach ===", file=sys.stderr)
    cols = ["gene", "phenotype", "n_rxn", "elasticity", "is_glg", "function_supplTableS1"]
    if "pct_wt" in hits.columns:
        cols.insert(3, "pct_wt")
    print(hits[hits.elasticity > 0][cols].to_string(
        index=False, formatters={"elasticity": "{:.6f}".format}), file=sys.stderr)
    z = hits[hits.elasticity <= 0]
    print(f"\n{len(z)} of the {len(hits)} hits reach ZERO response mass "
          f"({(z.n_rxn == 0).sum()} carry no atom-mapped reaction at all).", file=sys.stderr)

    print("\n=== the top 20 levers, and whether the screen called them ===", file=sys.stderr)
    top = tab.nlargest(20, "elasticity").reset_index(names="mnxr")
    top["called"] = top.hit_clones.apply(lambda cs: ",".join(c.split(":")[-1] for c in cs))
    print(top[["mnxr", "genes", "rxn_name", "elasticity", "ratio", "is_glg",
               "n_clone", "called"]]
          .to_string(index=False, formatters={"elasticity": "{:.5f}".format,
                                              "ratio": "{:.3g}".format}),
          file=sys.stderr)


if __name__ == "__main__":
    main()

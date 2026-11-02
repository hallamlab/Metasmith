#!/usr/bin/env python3
"""Rebuild Table EV4's own AUC from this tree's adjacency, and see whether it comes back.

    mamba run -n msm-fabfos python \\
        research/fabfos/benchmarks/fuhrer/sweeps/reproduce_published_auc.py
    ... --publish     # writes data/fabfos/runs/fuhrer_clones/ecspr/published_auc_check.tsv

WHY THIS EXISTS. Table EV4 is this campaign's first external published baseline, and the
report is about to print it beside every ECSPr number. Before that is allowed, the number
has to be understood, and the only way to understand it is to recompute it. Fuhrer's recipe
is stated in the Methods and is short: rank every deletion mutant by its absolute z-score on
the ion, call an enzyme a TRUE POSITIVE when it "either uses the predicted metabolite as
substrate or product", and take the ROC AUC. Nothing there is ambiguous except the adjacency
set, which is exactly what this script substitutes: the genes whose curated reactions carry
a mapped carbon atom into or out of the metabolite -- the same `sink_module` the tautology
control strikes.

**CAUTION** THE TWO AUCs ARE NOT THE SAME MEASUREMENT AND THE REPORT MUST NOT PRINT THEM AS
ONE. Fuhrer ranks by the PHENOTYPE and labels by ADJACENCY. The ECSPr arm ranks by a
topology-derived conductance and labels by the phenotype. They are transposes of one
association, both monotone in it, so they are comparable as evidence about that association
-- but a gap between the numbers is not a performance gap at one task.

WHAT A DISAGREEMENT HERE WOULD MEAN, stated before the numbers are looked at so it cannot be
fitted afterwards. Fuhrer annotated against KEGG's `eco` compound set and the Orth 2011
model; this tree's curated background is iML1515 projected through MetaNetX 4.5. Those are
different adjacency definitions over the same organism. So a low correlation says the two
trees disagree about which enzymes touch a metabolite, NOT that the paper is wrong and NOT
that the recomputation is. It would mean the published column is a baseline computed on a
label set this arm cannot reproduce, and every use of it has to say so.

`module_size` IS THE COLUMN TO READ THE RESULT BY. An ion whose metabolite has exactly one
adjacent gene admits only one ROC, so the two definitions cannot disagree there; an ion with
five gives them room to. If the reproduction holds at size one and fails above it, the
disagreement is about adjacency and not about the arithmetic.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import spearmanr


def _repo_root(start: Path) -> Path:
    for d in [start, *start.parents]:
        if (d / "data/fabfos").is_dir():
            return d
    raise SystemExit(f"no ancestor of {start} contains data/fabfos")


ROOT = _repo_root(Path(__file__).resolve())
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "research/fabfos/benchmarks/eydallin"))
sys.path.insert(0, str(ROOT / "research/fabfos/benchmarks/eydallin/sweeps"))
sys.path.insert(0, str(ROOT / "research/fabfos/benchmarks/fuhrer/sweeps"))

import analyse_aska_sweep as A                                          # noqa: E402
import bake_pairs                                                       # noqa: E402
from analyse_fuhrer_ratio_sweep import sink_module                      # noqa: E402

SCREEN = ROOT / "data/fabfos/runs/fuhrer_clones/parse/screen"
RESOLUTION = ROOT / "data/fabfos/runs/fuhrer_clones/parse/readout_resolution.tsv"
OUT = ROOT / "data/fabfos/runs/fuhrer_clones/ecspr/published_auc_check.tsv"

COLS = ("mode", "ion_index", "kegg_id", "kegg_name", "mnxm", "n_compound",
        "module_size", "n_positive_z", "published_auc", "recomputed_auc", "delta")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--channel", default="gem", choices=("gem", "denovo"))
    ap.add_argument("--publish", action="store_true",
                    help=f"write {OUT.relative_to(ROOT)}")
    a = ap.parse_args()

    res = pd.read_csv(RESOLUTION, sep="\t", dtype=str).fillna("")
    cand = res[(res.resolution == "resolved") & (res.published_auc != "")
               & (res[f"node_{a.channel}"] == "True")].copy()
    roster = pd.read_csv(SCREEN / "gene_roster.tsv", sep="\t", dtype=str).fillna("")
    genes = [g for g in roster.gene if g != "wt"]
    gpr = pd.read_parquet(
        ROOT / f"data/fabfos/runs/fuhrer_clones/gpr/gpr_{a.channel}.parquet",
        columns=["condition_id", "mnxr"])
    pairs = pd.read_parquet(bake_pairs.atom_pairs(),
                            columns=["mnxr", "element", "substrate", "product"])
    pairs = pairs[pairs.element == "C"]

    modules = {m: sink_module(m, gpr, pairs) for m in sorted(set(cand.mnxm))}
    print(f"{len(cand):,} (ion, compound) pairs carry a published AUC and a "
          f"{a.channel} node, over {len(modules):,} distinct metabolites")

    rows = []
    for mode, grp in cand.groupby("mode"):
        z = pd.read_parquet(SCREEN / f"zscore_{mode}.parquet", columns=genes)
        arr = np.abs(z.to_numpy())
        for r in grp.itertuples():
            mod = modules[r.mnxm]
            pos = np.array([g in mod for g in genes])
            row = arr[int(r.ion_index) - 1]
            auc = A.auc(row, pos)[0] if pos.any() and not pos.all() else float("nan")
            rows.append(dict(mode=mode, ion_index=int(r.ion_index), kegg_id=r.kegg_id,
                             kegg_name=r.kegg_name, mnxm=r.mnxm,
                             n_compound=int(r.n_compound), module_size=int(pos.sum()),
                             n_positive_z=int((row > 2.765).sum()),
                             published_auc=float(r.published_auc), recomputed_auc=auc,
                             delta=auc - float(r.published_auc)))
        del z, arr

    df = pd.DataFrame(rows, columns=list(COLS))
    ok = df.dropna(subset=["recomputed_auc"])
    rho, p = spearmanr(ok.published_auc, ok.recomputed_auc)
    print(f"\n{len(ok):,} scorable | Spearman rho {rho:.4f} (p={p:.3g}) between the "
          f"published AUC and the same recipe over this tree's adjacency")
    print(f"median |difference| {ok.delta.abs().median():.4f}, "
          f"{int((ok.delta.abs() < 0.05).sum()):,} within 0.05 "
          f"({(ok.delta.abs() < 0.05).mean():.1%})")
    print(f"\n{'module_size':>12} {'n':>5} {'within 0.05':>12} {'median |d|':>11} "
          f"{'rho':>7}")
    for size, g in ok.groupby(ok.module_size.clip(upper=5)):
        r2 = spearmanr(g.published_auc, g.recomputed_auc)[0] if len(g) > 2 else float("nan")
        print(f"{size:>12}{'+' if size == 5 else ' '}{len(g):>5} "
              f"{(g.delta.abs() < 0.05).mean():>11.1%} {g.delta.abs().median():>11.4f} "
              f"{r2:>7.4f}")

    if not a.publish:
        print(f"\n(dry run -- pass --publish to write {OUT.relative_to(ROOT)})")
        return 0
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.unlink(missing_ok=True)
    df.to_csv(OUT, sep="\t", index=False)
    print(f"\n-> {OUT.relative_to(ROOT)}: {len(df):,} rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())

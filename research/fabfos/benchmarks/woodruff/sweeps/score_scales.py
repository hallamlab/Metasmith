#!/usr/bin/env python3
"""Score the SCALEs sweep: reach first, then rank, then the two controls.

    mamba run -n build-refs-cobra python research/fabfos/benchmarks/woodruff/sweeps/score_scales.py \
        --channel gem

THE ORDER OF THIS FILE IS THE ARGUMENT. The reach 2x2 is computed and printed before any
ranking statistic, because "ECSPr ranks the positives well" means nothing until "ECSPr can
see the positives at all" has a number. On the curated channel it can see 23% of the
screen, so a ranking statistic over the whole library is mostly a statement about which
genes have a reaction, not about which reaction.

EVERY AUC IS PRINTED BESIDE THE SAME AUC ON REACTION COUNT ALONE. Effective conductance is
monotone in every edge conductance (Rayleigh), so raising a gene's reactions can only raise
the readout and a gene touching more reactions wins by construction. Without the size
control an AUC of 0.7 is indistinguishable from "the positives happen to be polyfunctional
enzymes". That confound has already sunk one arm in this tree.

THE TAUTOLOGY CONTROL STRIKES THE AXIS'S OWN MODULE, defined mechanically as the genes
carrying a reaction incident to that axis's sink metabolite -- not a hand-listed gene set,
so it cannot be tuned per axis. A glucose-to-trehalose probe ranking otsA first is
arithmetic, and the number that matters is the one computed with otsA gone.

THREE NESTED POSITIVE SETS, ALL REPORTED. Fitness above one at 15 g/L (158 genes), the same
at 30 g/L (487), and the twelve genes of the nine clones that were individually rebuilt and
retested. The third is small and it is the only set that is not a screen artefact.

A DISCONNECTED AXIS IS REPORTED, NOT DROPPED. Betaine and choline have base conductance
exactly zero, so every gene's readout on them is identically zero and no ranking exists.
That is the mechanistic arm's decisive result and a null classifier result at the same
time; printing "AUC nan" with the reason beats omitting the axis and letting a reader
assume it was never run.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import fisher_exact


def _repo_root(start: Path) -> Path:
    for d in (start, *start.parents):
        if (d / "data" / "fabfos").is_dir():
            return d
    raise SystemExit(f"no ancestor of {start} contains data/fabfos")


ROOT = _repo_root(Path(__file__).resolve())
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "research/fabfos/benchmarks/eydallin"))
sys.path.insert(0, str(ROOT / "research/fabfos/benchmarks/eydallin/sweeps"))

import bake_pairs                                                      # noqa: E402
from ecspr.model.build import load_pairs                               # noqa: E402
from analyse_aska_sweep import auc, precision_at_k, resample           # noqa: E402

SWEEPS = ROOT / "data/fabfos/runs/woodruff_clones/ecspr"
GPR = ROOT / "data/fabfos/runs/woodruff_clones/gpr"
CLONES = ROOT / "data/fabfos/benchmarks/_extractions/scales_tol/clones.tsv"

HOST = "e_coli_bw25113"
KS = (10, 25, 50, 100)


def positive_sets(df: pd.DataFrame, census: pd.DataFrame) -> dict[str, set[str]]:
    ph = dict(zip(df.gene_norm, df.phenotype))
    tol15 = {g for g, p in ph.items() if p in ("tolerant_both", "tolerant_15")}
    tol30 = {g for g, p in ph.items() if p in ("tolerant_both", "tolerant_30")}

    clones = pd.read_csv(CLONES, sep="\t", dtype=str).fillna("")
    bn, nm = set(), set()
    for r in clones.itertuples(index=False):
        if str(r.confirmed).strip().upper() != "TRUE":
            continue
        bn.update(b for b in r.bnums.split(",") if b)
        nm.update(g.lower() for g in r.genes.split(",") if g)
    conf = set(census[census.bnum_from_name.isin(bn) | census.bnum_sheet.isin(bn)
                      | census.gene_norm.isin(nm)].gene_norm)
    if len(conf) != len(bn):
        raise SystemExit(f"confirmed set: {len(bn)} b-numbers matched {len(conf)} genes")
    if (len(tol15), len(tol30)) != (158, 487):
        raise SystemExit(f"positive counts {len(tol15)}/{len(tol30)} do not reproduce the "
                         f"paper's 158/487; the phenotype column has changed")
    return {"tolerant_15": tol15, "tolerant_30": tol30, "confirmed_clones": conf}


def module_genes(sub: pd.DataFrame, sink: str, incident: dict[str, set[str]]) -> set[str]:
    rx = incident.get(sink, set())
    out = set()
    for g, r in zip(sub.gene_norm, sub.rxns):
        if r and (set(str(r).split(",")) & rx):
            out.add(g)
    return out


def analyse(df: pd.DataFrame, label: str, log) -> dict:
    out = {"label": label, "n": len(df), "n_positive": int(df.is_positive.sum())}
    mapped = df.n_rxn > 0

    tab = [[int((df.is_positive & mapped).sum()), int((df.is_positive & ~mapped).sum())],
           [int((~df.is_positive & mapped).sum()), int((~df.is_positive & ~mapped).sum())]]
    orr, pf = fisher_exact(tab)
    out["reach"] = dict(table=tab, odds_ratio=float(orr), p=float(pf),
                        pos_mapped_frac=tab[0][0] / max(sum(tab[0]), 1),
                        neg_mapped_frac=tab[1][0] / max(sum(tab[1]), 1))
    log(f"  reach: {tab[0][0]}/{sum(tab[0])} positives atom-mapped "
        f"({out['reach']['pos_mapped_frac']:.1%}) vs {tab[1][0]}/{sum(tab[1])} of the rest "
        f"({out['reach']['neg_mapped_frac']:.1%})  OR={orr:.2f} p={pf:.3g}")

    if df.score.nunique() <= 1:
        out["degenerate"] = ("every gene scores identically, so no ranking exists on this "
                             "axis -- see the base conductance")
        log(f"  NO RANKING: every gene scores {df.score.iloc[0]:.6g}. {out['degenerate']}")
        return out

    for scope, sub in (("library", df), ("atom-mapped", df[mapped])):
        p = sub.is_positive.to_numpy()
        a_e, p_e = auc(sub.score.to_numpy(), p)
        a_n, p_n = auc(sub.n_rxn.to_numpy().astype(float), p)
        out[f"auc_{scope}"] = dict(n=len(sub), n_positive=int(p.sum()),
                                   ecspr=a_e, ecspr_p=p_e, size=a_n, size_p=p_n)
        log(f"  AUC over the {scope:11s} (n={len(sub):,}, {int(p.sum())} positive): "
            f"ECSPr {a_e:.4f} (p={p_e:.3g})   |   size control {a_n:.4f} (p={p_n:.3g})")

    out["precision_at_k"] = [precision_at_k(df, k) for k in KS]
    for r in out["precision_at_k"]:
        log(f"  top {r['k']:3d}: {r['hits']:2d} positives "
            f"(expected {r['expected']:.1f}, p={r['p']:.3g})")
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--channel", choices=("gem", "denovo"), default="gem")
    ap.add_argument("--fold", type=float, default=2.0)
    ap.add_argument("--element", default="C")
    ap.add_argument("--reps", type=int, default=200)
    ap.add_argument("--seed", type=int, default=20260817)
    a = ap.parse_args()

    tag = f"scales_sweep_{a.channel}_{HOST}_fold{a.fold}_{a.element}"
    sweep = SWEEPS / f"{tag}.tsv"
    if not sweep.exists():
        raise SystemExit(f"{sweep.relative_to(ROOT)} does not exist -- run sweep_scales.py")
    df = pd.read_csv(sweep, sep="\t", float_precision="round_trip")
    census = pd.read_csv(GPR / "gene_census.tsv", sep="\t", dtype=str).fillna("")
    base = json.loads((SWEEPS / f"{tag}.BUILD.json").read_text())

    lines: list[str] = []

    def log(s=""):
        print(s)
        lines.append(s)

    axes = list(base["axes"])
    pos_sets = positive_sets(df.drop_duplicates("gene_norm"), census)
    log(f"# {tag}")
    log(f"\n{base['measured_genes']:,} measured genes | "
        f"{base['solved_genes']:,} atom-mapped, {base['exact_zero_genes']:,} exact zeros | "
        f"{base['background_reactions']:,} background reactions "
        f"({base['weight_dict_convention']})")
    log(f"positive sets: " + ", ".join(f"{k} {len(v):,}" for k, v in pos_sets.items()))

    pairs = load_pairs(bake_pairs.atom_pairs(), element=a.element)
    incident = {}
    for ax, meta in base["axes"].items():
        s = meta["sink"]
        m = pairs[(pairs.substrate == s) | (pairs["product"] == s)]
        incident[s] = set(m.mnxr.astype(str))

    results = {}
    for ax in axes:
        meta = base["axes"][ax]
        sub = df[df.axis == ax].copy()
        sub["score"] = sub["delta"]
        mod = module_genes(sub, meta["sink"], incident)
        log(f"\n{'=' * 78}\n{ax}  ->  {meta['sink']}  "
            f"(expected {meta['expected_dir']}, base {meta['base']:.9g})")
        log(f"  module (genes carrying a reaction incident to the sink): "
            f"{len(mod)} {sorted(mod)[:12]}")
        if meta["base"] == 0.0:
            log(f"  BASE IS EXACTLY ZERO -- the sink is disconnected from glucose on this "
                f"host, so no perturbation of any gene can produce a readout. This axis is "
                f"the mechanistic arm's decisive negative and carries no ranking.")
        per_axis = {"sink": meta["sink"], "expected_dir": meta["expected_dir"],
                    "base": meta["base"], "module_n": len(mod),
                    "module": sorted(mod), "sets": {}}
        for name, genes in pos_sets.items():
            sub["is_positive"] = sub.gene_norm.isin(genes)
            log(f"\n-- {name} ({len(genes)} genes) --")
            per_axis["sets"][name] = analyse(sub, f"{ax}/{name}", log)
            struck = sub[~sub.gene_norm.isin(mod)]
            log(f"  [tautology control: {len(sub) - len(struck)} module gene(s) struck]")
            per_axis["sets"][name]["module_struck"] = analyse(
                struck, f"{ax}/{name}/no-module", log)
            if sub.score.nunique() > 1 and name == "confirmed_clones":
                per_axis["sets"][name]["resample"] = resample(
                    sub, n_neg=100, reps=a.reps, seed=a.seed, log=log)
        results[ax] = per_axis

    out_json = SWEEPS / f"SCORE_{tag}.json"
    out_json.write_text(json.dumps(
        dict(tag=tag, channel=a.channel, host=HOST, fold=a.fold, element=a.element,
             population=base["measured_genes"], solved=base["solved_genes"],
             positive_sets={k: sorted(v) for k, v in pos_sets.items()},
             axes=results), indent=2))
    (SWEEPS / f"SCORE_{tag}.txt").write_text("\n".join(lines) + "\n")
    print(f"\n-> {out_json.relative_to(ROOT)}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""Would ECSPr, run blind over the whole ASKA library, nominate Eydallin's genes?

    mamba run -n msm python research/fabfos/benchmarks/eydallin/analyse_aska_sweep.py

The regression framing is dead: the two-point probe is monotone in every edge conductance,
so a one-sided readout cannot express a two-sided phenotype, and three sessions of Spearman
rho have said so. This asks the question the probe CAN answer -- a ranking question. Eydallin
screened the whole library and reported 86 genes; the other four thousand clones were built,
assayed and did not move glycogen. Sort the library by the modelled glucose -> glycogen
response and ask where the 86 land.

THE PRECONDITION COMES BEFORE THE SCORE. If being an Eydallin hit and being visible to the
method at all are not independent, then part of any AUC below is that dependence rather than
the measurement, and it has to be on the page first. It is reported as a 2x2 with a Fisher
exact test, and every AUC is then repeated on the atom-mapped subset alone, where the
dependence cannot contribute.

EACH NUMBER CARRIES THE CONTROL THAT MAKES IT MEAN SOMETHING.

  * the SIZE control -- the same AUC scored on reaction count alone. The ASKA/FFA arm died
    here: its score tracked clone size at rho=+0.69 and the phenotype at +0.01. ECSPr carries
    information only insofar as it beats this number.
  * the TAUTOLOGY control -- the same AUC with the glycogen module struck from the positives.
    A glucose -> glycogen probe ranking glycogen synthase first is close to arithmetic; the
    interesting claim is about the other seventy-odd genes.

TIES ARE THE DOMINANT FEATURE OF THIS RANKING, not an edge case: most of the library reaches
no atom-mapped reaction and scores an exact zero. The AUC is therefore the mid-rank
Mann-Whitney form throughout. A hand-rolled strictly-greater-than count scores every one of
those ties as a loss and understates the result by a wide margin.

THE HUNDRED DRAWS ARE A SUBSET OF THIS, and are reported as such. The exhaustive sweep
contains any sample of it and carries no sampling error, so the resampled figure is here to
show the two agree, not to stand in for the exhaustive one.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import mannwhitneyu, fisher_exact, hypergeom, spearmanr

ROOT = Path(__file__).resolve().parents[5]
sys.path.insert(0, str(ROOT / "src"))
from ecspr.model.scoring import _stats                                  # noqa: E402

SWEEPS = ROOT / "data/fabfos/runs/eydallin_clones/ecspr"
OUT = SWEEPS

# The glycogen module itself. A probe that grounds at glycogen ranking these first is
# close to a tautology, so the interesting AUC is the one computed without them.
GLYCOGEN_MODULE = ("glgA", "glgB", "glgC", "glgP", "glgS", "glgX", "malP", "malQ")


def auc(score: np.ndarray, pos: np.ndarray) -> tuple[float, float]:
    # ``(AUC, p)`` -- mid-rank Mann-Whitney, positives ranked above the rest.
    a, b = score[pos], score[~pos]
    if a.size == 0 or b.size == 0:
        return float("nan"), float("nan")
    u, p = mannwhitneyu(a, b, alternative="greater")
    return float(u / (a.size * b.size)), float(p)


def precision_at_k(df: pd.DataFrame, k: int) -> dict:
    top = df.nlargest(k, "score", keep="all").head(k)
    hit = int(top.is_positive.sum())
    N, K = len(df), int(df.is_positive.sum())
    return dict(k=k, hits=hit, precision=hit / k, expected=K * k / N,
                p=float(hypergeom.sf(hit - 1, N, K, k)))


def analyse(df: pd.DataFrame, label: str, log) -> dict:
    out = {"label": label, "n": len(df), "n_positive": int(df.is_positive.sum())}
    mapped = df.n_rxn > 0

    tab = [[int((df.is_positive & mapped).sum()), int((df.is_positive & ~mapped).sum())],
           [int((~df.is_positive & mapped).sum()), int((~df.is_positive & ~mapped).sum())]]
    orr, pf = fisher_exact(tab)
    out["reach"] = dict(table=tab, odds_ratio=float(orr), p=float(pf),
                        pos_mapped_frac=tab[0][0] / max(sum(tab[0]), 1),
                        neg_mapped_frac=tab[1][0] / max(sum(tab[1]), 1))
    log(f"\n  reach: {tab[0][0]}/{sum(tab[0])} positives atom-mapped "
        f"({out['reach']['pos_mapped_frac']:.1%}) vs {tab[1][0]}/{sum(tab[1])} of the rest "
        f"({out['reach']['neg_mapped_frac']:.1%})  OR={orr:.2f} p={pf:.3g}")

    pos = df.is_positive.to_numpy()
    for scope, sub in (("library", df), ("atom-mapped", df[mapped])):
        p = sub.is_positive.to_numpy()
        a_e, p_e = auc(sub.score.to_numpy(), p)
        a_n, p_n = auc(sub.n_rxn.to_numpy().astype(float), p)
        out[f"auc_{scope}"] = dict(n=len(sub), n_positive=int(p.sum()),
                                   ecspr=a_e, ecspr_p=p_e, size=a_n, size_p=p_n)
        log(f"  AUC over the {scope:11s} (n={len(sub):,}, {int(p.sum())} positive): "
            f"ECSPr {a_e:.4f} (p={p_e:.3g})   |   size control {a_n:.4f} (p={p_n:.3g})")

    out["precision_at_k"] = [precision_at_k(df, k) for k in (10, 25, 50, 100)]
    for r in out["precision_at_k"]:
        log(f"  top {r['k']:3d}: {r['hits']:2d} positives "
            f"(expected {r['expected']:.1f}, p={r['p']:.3g})")

    null = df.loc[~pos, "score"].to_numpy()
    st = _stats(df.loc[pos, "score"].to_numpy(), null)
    ranked = (df[pos].assign(pct_rank=st["pct_rank"], z=st["z"], p_emp=st["p_emp"])
              .sort_values("score", ascending=False))
    out["null_n"] = st["null_n"]
    out["p_floor"] = st["p_floor"]
    out["positives"] = ranked[["gene", "eydallin_phenotype", "n_rxn", "score",
                               "pct_rank", "z", "p_emp"]].to_dict("records")
    # `_stats` ranks by strict inequality, and most of this library ties at exactly zero,
    # so its pct_rank is a lower bound. The mid-rank form is the one that matches the AUC.
    obs = df.loc[pos, "score"].to_numpy()
    mid = ((null[None, :] < obs[:, None]).sum(1)
           + 0.5 * (null[None, :] == obs[:, None]).sum(1)) / max(null.size, 1)
    out["n_pos_above_all_negatives"] = int((st["pct_rank"] >= 1.0).sum())
    out["median_pct_rank_strict"] = float(np.median(st["pct_rank"]))
    out["median_pct_rank_midrank"] = float(np.median(mid))
    log(f"  positives' median percentile rank in the library: "
        f"{out['median_pct_rank_midrank']:.4f} mid-rank "
        f"({out['median_pct_rank_strict']:.4f} strict)   "
        f"({out['n_pos_above_all_negatives']} above every negative)")
    return out


def resample(df: pd.DataFrame, *, n_neg: int, reps: int, seed: int, log) -> dict:
    rng = np.random.default_rng(seed)
    neg = df.loc[~df.is_positive, "score"].to_numpy()
    pos = df.loc[df.is_positive, "score"].to_numpy()
    vals = np.empty(reps)
    for i in range(reps):
        d = rng.choice(neg, size=n_neg, replace=False)
        u, _ = mannwhitneyu(pos, d, alternative="greater")
        vals[i] = u / (pos.size * n_neg)
    lo, hi = np.percentile(vals, [2.5, 97.5])
    out = dict(n_neg=n_neg, reps=reps, seed=seed, mean=float(vals.mean()),
               sd=float(vals.std(ddof=1)), lo=float(lo), hi=float(hi))
    log(f"  {reps} draws of {n_neg} random clones: AUC {out['mean']:.4f} "
        f"+/- {out['sd']:.4f}  (95% of draws in [{lo:.4f}, {hi:.4f}])")
    return out


MEASURED = ROOT / "data/fabfos/benchmarks/eydallin/Y/measured_glycogen.tsv"


def _direction(df: pd.DataFrame, log) -> dict:
    out = {}
    pos = df[df.is_positive & (df.n_rxn > 0)].copy()
    pos = pos[pos.delta.abs() > 1e-12]
    pos["pred_excess"] = pos.delta > 0
    pos["obs_excess"] = pos.eydallin_phenotype.astype(str).eq("glycogen_excess")
    tab = [[int((pos.pred_excess & pos.obs_excess).sum()),
            int((pos.pred_excess & ~pos.obs_excess).sum())],
           [int((~pos.pred_excess & pos.obs_excess).sum()),
            int((~pos.pred_excess & ~pos.obs_excess).sum())]]
    orr, pf = fisher_exact(tab)
    agree = tab[0][0] + tab[1][1]
    out["sign_2x2"] = dict(table=tab, odds_ratio=float(orr), p=float(pf),
                           n=len(pos), agree=agree)
    log(f"\n-- direction " + "-" * 63)
    log(f"  {len(pos)} labelled positives carry a non-zero response "
        f"({int(df.is_positive.sum()) - len(pos)} are exact zeros and have no sign)")
    log(f"  predicted excess / observed excess : {tab[0][0]:3d}      "
        f"predicted excess / observed deficient : {tab[0][1]:3d}")
    log(f"  predicted deficient / obs excess   : {tab[1][0]:3d}      "
        f"predicted deficient / obs deficient   : {tab[1][1]:3d}")
    log(f"  sign agreement {agree}/{len(pos)} = {agree / max(len(pos), 1):.1%}  "
        f"OR={orr:.3g}  Fisher p={pf:.3g}")

    if MEASURED.exists():
        meas = pd.read_csv(MEASURED, sep="\t")
        pct = {str(c).split(":")[-1].lower(): v
               for c, v in zip(meas.condition_id, meas.pct_wt)}
        pos["pct_wt"] = pos.gene.str.lower().map(pct)
        q = pos.dropna(subset=["pct_wt"])
        if len(q) >= 3:
            y = np.log2(q.pct_wt.to_numpy() / 100.0)
            rs, ps = spearmanr(y, q.delta.to_numpy())
            out["signed_spearman"] = dict(rho=float(rs), p=float(ps), n=len(q))
            log(f"  SIGNED Spearman vs the digitised Fig. 1: rho = {rs:+.4f}  "
                f"p = {ps:.3g}  (n={len(q)})")
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--fold", type=float, default=2.0)
    ap.add_argument("--element", default="C")
    ap.add_argument("--channels", nargs="+", default=["gem", "denovo"])
    ap.add_argument("--score", choices=("delta", "absdelta"), default="delta",
                    help="`absdelta` ranks by how far a clone moves glycogen in EITHER "
                         "direction. Only meaningful for a sweep whose probe can go down; "
                         "under the two-point probe every delta is >= 0 and the two agree.")
    ap.add_argument("--direction", action="store_true",
                    help="also ask whether the SIGN of the response matches the sign of "
                         "the phenotype -- the question a monotone probe cannot pose")
    ap.add_argument("--suffix", default="",
                    help="tag appended by a variant sweep (e.g. `_lanes2`, `_dir2x100`); "
                         "reads that sweep and writes its own report beside it")
    ap.add_argument("--reps", type=int, default=2000)
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--out-dir", type=Path, default=OUT)
    a = ap.parse_args()

    lines: list[str] = []

    def log(s=""):
        print(s)
        lines.append(s)

    report = {}
    for ch in a.channels:
        f = SWEEPS / f"aska_sweep_{ch}_e_coli_ag1_fold{a.fold}_{a.element}{a.suffix}.tsv"
        if not f.exists():
            log(f"\n### {ch}: {f.name} not present -- skipped")
            continue
        df = pd.read_csv(f, sep="\t")
        df["is_positive"] = df.is_positive.astype(bool)
        # The ranking statistic. delta and log2FC are monotone in each other here (one
        # shared baseline), so which one is ranked on cannot change any AUC; delta is
        # used because a clone that reaches nothing is an exact 0 rather than a log of 1.
        df["score"] = (df.delta.abs() if a.score == "absdelta"
                       else df.delta).astype(float)
        log(f"\n{'=' * 78}\n### channel {ch}  --  {len(df):,} clone genes, "
            f"{int((df.n_rxn > 0).sum()):,} atom-mapped, "
            f"{int(df.is_positive.sum())} Eydallin positives\n{'=' * 78}")

        report[ch] = {}
        log("\n-- all 86 positives " + "-" * 56)
        report[ch]["all"] = analyse(df, f"{ch}:all", log)
        report[ch]["all"]["resample"] = resample(df, n_neg=100, reps=a.reps,
                                                 seed=a.seed, log=log)

        mod = df.gene.isin(GLYCOGEN_MODULE) | df.eydallin_gene.astype(str).isin(GLYCOGEN_MODULE)
        struck = sorted(df.loc[mod & df.is_positive, "gene"])
        log(f"\n-- tautology control: {len(struck)} glycogen-module gene(s) removed "
            f"({', '.join(struck)}) " + "-" * 8)
        report[ch]["no_module"] = analyse(df[~mod].reset_index(drop=True),
                                          f"{ch}:no_module", log)
        report[ch]["no_module"]["struck"] = struck
        report[ch]["no_module"]["resample"] = resample(
            df[~mod].reset_index(drop=True), n_neg=100, reps=a.reps, seed=a.seed, log=log)

        log("\n  the top 25 of the library:")
        top = df.nlargest(25, "score")
        log(top[["gene", "n_rxn", "score", "is_positive", "eydallin_phenotype"]]
            .to_string(index=False, float_format=lambda v: f"{v:.6g}"))

        if a.direction:
            report[ch]["direction"] = _direction(df, log)

    a.out_dir.mkdir(parents=True, exist_ok=True)
    stem = f"aska_classifier_report{a.suffix}"
    (a.out_dir / f"{stem}.json").write_text(json.dumps(report, indent=2, default=float))
    (a.out_dir / f"{stem}.txt").write_text("\n".join(lines) + "\n")
    print(f"\n-> {a.out_dir}/{stem}.{{json,txt}}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

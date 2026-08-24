#!/usr/bin/env python3
"""Does the anabolic/catabolic RATIO nominate Eydallin's genes any better than the numerator?

    mamba run -n ecspr python research/fabfos/benchmarks/eydallin/analyse_aska_ratio_sweep.py

`analyse_aska_sweep.py` scores the one-probe sweep and is imported here whole -- same
mid-rank Mann-Whitney AUC, same reaction-count size control, same tautology control, same
resampled 100-clone draw. Only the score changes: |delta_ratio_pct| from `sweep_aska_ratio.py`
instead of the raw glucose -> glycogen delta. The ratio is two-sided by construction, so the
magnitude is what a ranking question can use.

TWO READINGS THE ONE-PROBE ANALYSIS DOES NOT NEED. The ratio is being considered as a SCREEN,
so the null tail is the number that decides whether it could be run:

  * how much of the 4,000-clone null moves the ratio as far as the five real glycogen genes
    (glgA, glgB, glgC, glgP, malP) do -- the false-positive rate a threshold at each hit
    would carry;
  * where malP and glgB in particular sit in that null. Both are still wrong-SIGNED under the
    ratio after the r10 bake fix, and whether their magnitudes are even distinguishable from
    the null decides how much that miss costs.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import analyse_aska_sweep as A                                          # noqa: E402

SWEEPS = ROOT / "data/fabfos/runs/aska/ecspr"
HITS = ("glgA", "glgB", "glgC", "glgP", "malP")


def _null_tail(df: pd.DataFrame, log) -> dict:
    null = df.loc[~df.is_positive, "score"].to_numpy()
    out = {"null_n": int(null.size), "hits": [], "watch": []}
    log(f"\n-- the null tail ({null.size:,} Eydallin-screened non-hits) " + "-" * 24)
    for gene in HITS:
        row = df[df.gene == gene]
        if row.empty:
            log(f"  {gene:6s}: not in the library table")
            continue
        s = float(row.score.iloc[0])
        n_ge = int((null >= s).sum())
        rec = dict(gene=gene, score=s, delta_ratio_pct=float(row.delta_ratio_pct.iloc[0]),
                   n_rxn=int(row.n_rxn.iloc[0]), n_null_ge=n_ge, fpr=n_ge / max(null.size, 1),
                   pct_rank=float((null < s).mean() + 0.5 * (null == s).mean()))
        out["hits"].append(rec)
        log(f"  {gene:6s}: |delta_ratio| = {s:.6g}% (signed {rec['delta_ratio_pct']:+.4g}%)  "
            f"-> {n_ge:,} of the null are >= it  (FPR {rec['fpr']:.3%}, "
            f"percentile {rec['pct_rank']:.4f})")
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--fold", type=float, default=2.0)
    ap.add_argument("--element", default="C")
    ap.add_argument("--channels", nargs="+", default=["gem", "denovo"])
    ap.add_argument("--suffix", default="")
    ap.add_argument("--reps", type=int, default=2000)
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--out-dir", type=Path, default=SWEEPS)
    a = ap.parse_args()

    lines: list[str] = []

    def log(s=""):
        print(s)
        lines.append(s)

    report = {}
    for ch in a.channels:
        f = SWEEPS / (f"aska_ratio_sweep_{ch}_e_coli_ag1_fold{a.fold}_"
                      f"{a.element}{a.suffix}.tsv")
        if not f.exists():
            log(f"\n### {ch}: {f.name} not present -- skipped")
            continue
        df = pd.read_csv(f, sep="\t")
        df["is_positive"] = df.is_positive.astype(bool)
        df["score"] = df.delta_ratio_pct.abs().astype(float)
        log(f"\n{'=' * 78}\n### channel {ch}  --  {len(df):,} clone genes, "
            f"{int((df.n_rxn > 0).sum()):,} atom-mapped, "
            f"{int(df.is_positive.sum())} Eydallin positives\n"
            f"    host ratio = {df.host_ratio.iloc[0]:.9f} "
            f"(glucose->glycogen {df.host_gg.iloc[0]:.6f} / "
            f"glycogen->pyruvate {df.host_gp.iloc[0]:.6f})\n{'=' * 78}")

        report[ch] = {}
        log("\n-- all 86 positives " + "-" * 56)
        report[ch]["all"] = A.analyse(df, f"{ch}:all", log)
        report[ch]["all"]["resample"] = A.resample(df, n_neg=100, reps=a.reps,
                                                   seed=a.seed, log=log)

        mod = (df.gene.isin(A.GLYCOGEN_MODULE)
               | df.eydallin_gene.astype(str).isin(A.GLYCOGEN_MODULE))
        struck = sorted(df.loc[mod & df.is_positive, "gene"])
        log(f"\n-- tautology control: {len(struck)} glycogen-module gene(s) removed "
            f"({', '.join(struck)}) " + "-" * 8)
        report[ch]["no_module"] = A.analyse(df[~mod].reset_index(drop=True),
                                            f"{ch}:no_module", log)
        report[ch]["no_module"]["struck"] = struck

        report[ch]["null_tail"] = _null_tail(df, log)

        log("\n  the top 25 of the library by |delta_ratio_pct|:")
        top = df.nlargest(25, "score")
        log(top[["gene", "n_rxn", "delta_ratio_pct", "is_positive", "eydallin_phenotype"]]
            .to_string(index=False, float_format=lambda v: f"{v:.6g}"))

        signed = df[df.is_positive & (df.n_rxn > 0)]
        log(f"\n  {len(signed)} positives carry a non-zero ratio response; "
            f"{int((signed.delta_ratio_pct > 0).sum())} of them positive-signed")

    a.out_dir.mkdir(parents=True, exist_ok=True)
    stem = f"aska_ratio_classifier_report{a.suffix}"
    (a.out_dir / f"{stem}.json").write_text(json.dumps(report, indent=2, default=float))
    (a.out_dir / f"{stem}.txt").write_text("\n".join(lines) + "\n")
    print(f"\n-> {a.out_dir}/{stem}.{{json,txt}}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""Does a modelled deletion tell glycogen-EXCESS mutants from glycogen-DEFICIENT ones?

    mamba run -n msm python research/fabfos/benchmarks/eydallin/sweeps/analyse_lof_cohort.py

This is the question the gain-of-function arm could not ask. There, every clone was a
doubling, effective conductance is non-decreasing in every edge conductance, and so a
signed correlation against a two-sided phenotype was undefined rather than weak -- three
sessions of Spearman rho said so before the theorem did. `--fold 0` is a different
operation: `graph_from_pairs` keeps only strictly positive weights, so a deleted
reaction's rows never become edges at all.

RAYLEIGH STILL BINDS THE TWO-POINT PROBE, and that has to be said before its numbers are
read. A deletion can only LOWER glucose -> glycogen conductance, so `log2fc_ieff` is a
magnitude with a fixed sign and cannot express a two-sided phenotype either. What it can
answer is a ranking question -- do the deficient mutants sit on more of the
glucose -> glycogen current than the excess ones? -- and that is scored here as an AUC,
not as a correlation.

THE RATIO IS THE TWO-SIDED READOUT. C(glucose -> glycogen) / C(glycogen -> pyruvate)
falls under Rayleigh in both legs and the theorem fixes neither the winner nor the sign of
their quotient, so a deletion can move it either way. That is the whole reason the second
solve is paid for.

EVERY NUMBER CARRIES TWO CONTROLS, because either one alone can manufacture this result:

  * the SIZE control -- the same score computed from reaction count. A gene carrying more
    reactions perturbs more of any network, and the gain-of-function arm died exactly
    here, tracking clone size at rho=+0.69 and the phenotype at +0.01.
  * the BASE-RATE control -- what "always predict excess" would score. The cohort is 35
    excess against 30 deficient overall but the measurable subset is not, and a sign
    agreement below its own majority class is worse than a coin flip dressed as a result.

AND THE TAUTOLOGY CONTROL ON TOP: a probe that grounds at glycogen ranking glycogen
synthase first is close to arithmetic. Every figure is repeated with the glycogen module
struck, and that second figure is the one that says whether anything was learned about the
other genes.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import fisher_exact, spearmanr

ROOT = Path(__file__).resolve().parents[5]
sys.path.insert(0, str(Path(__file__).resolve().parent))
from analyse_aska_sweep import GLYCOGEN_MODULE, auc                      # noqa: E402

ECSPR = ROOT / "data/fabfos/runs/eydallin_clones/ecspr"
MEASURED = ROOT / "data/fabfos/benchmarks/eydallin_2007/Y/measured_glycogen.tsv"

EXCESS, DEFICIENT = "glycogen_excess", "glycogen_deficient"


def _spearman(y: np.ndarray, x: np.ndarray) -> dict:
    """Rank correlation, which is why the glycogen-LESS mutants can stay in.

    Five of them measured 0 % of wild type, so their log2 fold change is -inf. A rank
    method reads that as "bottom of the order", which is exactly what it is; Pearson
    reads it as NaN and takes the whole correlation with it.
    """
    if len(y) < 3:
        return dict(n=len(y), rho=float("nan"), p=float("nan"))
    rho, p = spearmanr(y, x)
    return dict(n=int(len(y)), rho=float(rho), p=float(p))


def analyse(df: pd.DataFrame, label: str, log) -> dict:
    out = {"label": label, "n": int(len(df))}
    solved = df[df.n_rxn > 0].copy()
    out["n_measurable"] = int(len(solved))
    n_exc = int((solved.phenotype == EXCESS).sum())
    n_def = int((solved.phenotype == DEFICIENT).sum())
    log(f"\n  {len(solved)} of {len(df)} mutants carry an atom-mapped reaction "
        f"({n_exc} excess, {n_def} deficient)")
    if n_exc == 0 or n_def == 0:
        log("  one class is empty -- nothing to separate")
        return out

    # --- the two-point probe, as a ranking question
    deficient = (solved.phenotype == DEFICIENT).to_numpy()
    loss = (-solved.log2fc_ieff).to_numpy(dtype=float)      # >= 0 by Rayleigh
    a_e, p_e = auc(loss, deficient)
    a_n, p_n = auc(solved.n_rxn.to_numpy(dtype=float), deficient)
    out["twopoint_auc"] = dict(ecspr=a_e, ecspr_p=p_e, size=a_n, size_p=p_n,
                               n_positive=n_def, n=int(len(solved)))
    log(f"\n  two-point: AUC ranking DEFICIENT above EXCESS by conductance lost "
        f"{a_e:.4f} (p={p_e:.3g})   |   size control {a_n:.4f} (p={p_n:.3g})")

    # --- the ratio probe, as a direction question
    pred_excess = (solved.delta_ratio_pct > 0).to_numpy()
    obs_excess = (solved.phenotype == EXCESS).to_numpy()
    tab = [[int((pred_excess & obs_excess).sum()),
            int((pred_excess & ~obs_excess).sum())],
           [int((~pred_excess & obs_excess).sum()),
            int((~pred_excess & ~obs_excess).sum())]]
    orr, pf = fisher_exact(tab)
    agree = tab[0][0] + tab[1][1]
    base = max(n_exc, n_def) / len(solved)
    out["ratio_sign"] = dict(table=tab, odds_ratio=float(orr), p=float(pf),
                             agree=agree, n=int(len(solved)),
                             rate=agree / len(solved), base_rate=base,
                             beats_base=bool(agree / len(solved) > base))
    log(f"  ratio: predicted excess / observed excess {tab[0][0]:3d}     "
        f"predicted excess / observed deficient {tab[0][1]:3d}\n"
        f"         predicted deficient / obs excess {tab[1][0]:3d}     "
        f"predicted deficient / obs deficient   {tab[1][1]:3d}\n"
        f"         sign agreement {agree}/{len(solved)} = "
        f"{agree / len(solved):.1%}  vs a majority-class base rate of {base:.1%}  "
        f"OR={orr:.3g}  Fisher p={pf:.3g}")
    a_r, p_r = auc(solved.delta_ratio_pct.to_numpy(dtype=float), obs_excess)
    a_rn, p_rn = auc(solved.n_rxn.to_numpy(dtype=float), obs_excess)
    out["ratio_auc"] = dict(ecspr=a_r, ecspr_p=p_r, size=a_rn, size_p=p_rn,
                            n_positive=n_exc, n=int(len(solved)))
    log(f"         AUC ranking EXCESS above DEFICIENT by signed ratio change "
        f"{a_r:.4f} (p={p_r:.3g})   |   size control {a_rn:.4f} (p={p_rn:.3g})")

    # --- and against the digitised numbers rather than the two classes
    y = np.log2(solved.pct_wt.to_numpy(dtype=float) / 100.0)
    out["signed_spearman"] = dict(
        twopoint=_spearman(y, solved.log2fc_ieff.to_numpy(dtype=float)),
        ratio=_spearman(y, solved.delta_ratio_pct.to_numpy(dtype=float)),
        size=_spearman(y, solved.n_rxn.to_numpy(dtype=float)))
    for k, v in out["signed_spearman"].items():
        log(f"  signed Spearman, measured log2FC vs {k:9s}: rho={v['rho']:+.4f} "
            f"p={v['p']:.3g} (n={v['n']})")
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--host", default="e_coli_bw25113")
    ap.add_argument("--fold", type=float, default=0.0)
    ap.add_argument("--element", default="C")
    ap.add_argument("--label", default="eydallin2007")
    ap.add_argument("--ecspr-dir", type=Path, default=ECSPR)
    ap.add_argument("--measured", type=Path, default=MEASURED)
    ap.add_argument("--out-dir", type=Path, default=ECSPR)
    a = ap.parse_args()

    panel = a.ecspr_dir / f"twopoint_cohort_{a.host}_fold{a.fold}_{a.element}.tsv"
    ratio = a.ecspr_dir / (f"{a.label}_ratio_sweep_gem_{a.host}_fold{a.fold}_"
                           f"{a.element}.tsv")
    for f in (panel, ratio):
        if not f.exists():
            raise SystemExit(f"[lof] {f} is not there -- run the panel and the ratio "
                             f"sweep at --fold {a.fold} first")

    lines: list[str] = []

    def log(s=""):
        print(s)
        lines.append(s)

    # The ratio sweep is the wider table: it carries every mutant, including the 42 the
    # model cannot see. The panel carries only the solvable ones, so it joins onto it.
    #
    # THE JOIN KEY IS `condition_id`, NEVER `gene`. The panel labels a row with the
    # MODEL's current symbol and the sweep with the PAPER's 2007 one, and six of the 65
    # were renamed in between -- `ybhE` is `pgl` there, `cspC` is `cspE`. A join on
    # `gene` matches 59 of them, fills the rest with a plausible zero, and quietly
    # deletes the one non-module deficient mutant that carries a real response.
    r = pd.read_csv(ratio, sep="\t")
    p = pd.read_csv(panel, sep="\t")[["condition_id", "log2fc_ieff"]]
    meas = pd.read_csv(a.measured, sep="\t")[["condition_id", "pct_wt"]]

    df = (r.rename(columns={"eydallin_phenotype": "phenotype"})
           .merge(p, on="condition_id", how="left")
           .merge(meas, on="condition_id", how="left"))
    missing = sorted(df.loc[df.pct_wt.isna(), "gene"])
    if missing:
        raise SystemExit(f"[lof] no digitised phenotype for {missing}")
    unpanelled = sorted(df.loc[(df.n_rxn > 0) & df.log2fc_ieff.isna(), "gene"])
    if unpanelled:
        raise SystemExit(f"[lof] {unpanelled} carry reactions but are absent from the "
                         f"two-point panel -- the two runs disagree about the cohort")
    df["log2fc_ieff"] = df.log2fc_ieff.fillna(0.0)

    log(f"### the 2007 deletion cohort, {a.host} / iML1515, fold {a.fold}\n"
        f"    {len(df)} mutants | {int((df.phenotype == EXCESS).sum())} excess, "
        f"{int((df.phenotype == DEFICIENT).sum())} deficient\n"
        f"    host ratio {df.host_ratio.iloc[0]:.9f} "
        f"(glucose->glycogen {df.host_gg.iloc[0]:.6f} / "
        f"glycogen->pyruvate {df.host_gp.iloc[0]:.6f})")

    report = {"host": a.host, "fold": a.fold, "n": int(len(df))}
    log("\n-- all mutants " + "-" * 60)
    report["all"] = analyse(df, "all", log)

    mod = df.gene.isin(GLYCOGEN_MODULE) | df.gene.isin(("pgm", "galU"))
    struck = sorted(df.loc[mod, "gene"])
    log(f"\n-- tautology control: {len(struck)} glycogen-module gene(s) struck "
        f"({', '.join(struck)}) " + "-" * 8)
    report["no_module"] = analyse(df[~mod].reset_index(drop=True), "no_module", log)
    report["no_module"]["struck"] = struck

    solved = df[df.n_rxn > 0].sort_values("delta_ratio_pct", ascending=False)
    log("\n  every measurable mutant, by signed ratio change:")
    log(solved[["gene", "phenotype", "pct_wt", "n_rxn", "log2fc_ieff",
                "delta_ratio_pct"]]
        .to_string(index=False, float_format=lambda v: f"{v:.6g}"))

    a.out_dir.mkdir(parents=True, exist_ok=True)
    stem = f"{a.label}_direction_report_{a.host}_fold{a.fold}_{a.element}"
    out = a.out_dir / f"{stem}.tsv"
    df.sort_values("delta_ratio_pct", ascending=False).to_csv(out, sep="\t", index=False)
    (a.out_dir / f"{stem}.json").write_text(json.dumps(report, indent=2, default=float))
    (a.out_dir / f"{stem}.txt").write_text("\n".join(lines) + "\n")
    print(f"\n-> {a.out_dir}/{stem}.{{tsv,json,txt}}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

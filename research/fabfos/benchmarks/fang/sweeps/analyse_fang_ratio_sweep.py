#!/usr/bin/env python3
"""Does any Fang axis's RATIO nominate the genes Fang's own reverse genetics called
beneficial -- and where does rfaY itself land?

    mamba run -n msm python research/fabfos/benchmarks/fang/sweeps/analyse_fang_ratio_sweep.py

`analyse_aska_sweep.py` scores an eydallin sweep and is imported whole: same mid-rank
Mann-Whitney AUC, same reaction-count size control, same resampled draw. Only the score
changes -- `|delta_ratio_pct|` per axis, since the ratio is two-sided by construction and
magnitude is what a ranking question can use -- and the loop runs once per (channel, axis).

WHAT POPULATION THIS IS, AND WHAT IT LICENSES. Fang published NO per-gene enrichment score.
The screen sequenced the sorted top-0.1% pool only; Figs. 1b and 4b show a read-count
scatter with the top 24 marked and no table behind it, and the paper's data statement is
"Data will be made available on request". So there is no measured null DISTRIBUTION to
threshold against, and every AUC below rests on a binary label -- `up` versus everything
else -- over the 4,102-clone roster. That is a weaker footing than eydallin's, where each of
~4,000 non-hits was individually built, stained and scored. It is reported both ways:

  * WITHIN-HIT RANKING, over the 58 genes Fang actually rebuilt and put through GC. This
    needs no assumption at all: every gene in it was measured, and 9 of them raised the
    titer. It is the primary result of this arm.
  * ROSTER-WIDE AUC, over all 4,102 clones, which additionally assumes an unsequenced clone
    is a true negative. Reported, flagged, and never quoted without the assumption.

`--assayed-only` is the switch between them and it is not cosmetic here.

CHANNELS ARE NOT POOLED. The de-novo background carries six times the reactions of iML1515
and every conductance on it is larger; only ranks within one channel compare. Each channel
gets its own section and its own report file, and each axis is reported only on the channels
where `sinks/probe_axes.py` found its sink reachable.

THE CARBON-BOND RESTRICTION IS RUN AS ITS OWN SLICE. ECSPr moves carbon, so the genes it can
speak about at all are the ones whose reaction changes a carbon skeleton -- 9 of Fang's 59.
Scoring the full cohort and the `breaks`/`creates`/`both` subset separately is what
distinguishes "the method is wrong" from "the method was asked about genes it cannot see".
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd


def _repo_root(start: Path) -> Path:
    for d in [start, *start.parents]:
        if (d / "data/fabfos").is_dir():
            return d
    raise SystemExit(f"no ancestor of {start} contains data/fabfos")


ROOT = _repo_root(Path(__file__).resolve())
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "research/fabfos/benchmarks/eydallin/sweeps"))

import analyse_aska_sweep as A                                          # noqa: E402

SWEEPS = ROOT / "data/fabfos/runs/fang/ecspr"
GOF_DIR = ROOT / "data/fabfos/runs/fang_clones/parse/gof"
GOF_TABLES = ("gof.csv", "gof_round2.csv")
CARBON_CHANGE = ("breaks", "creates", "both")

# The gene the arm exists to place. Swept as an ordinary clone -- nothing nominates it.
FOCUS = "rfaY"

# THE TAUTOLOGY CONTROL. An axis grounded at the LPS inner core ranking the enzymes that
# BUILD the LPS inner core is close to arithmetic: `rfaY`'s own reaction is the last edge
# into the sink, so doubling it doubles the sink's supply almost by definition, the way a
# glucose->glycogen probe ranking glycogen synthase first told the eydallin arm nothing.
# The interesting claim is about everything else. Both spellings are listed because the ASKA
# roster carries the retired `rfa*` names and iML1515 the current `waa*` ones.
LPS_CORE_MODULE = (
    "waaA", "waaB", "waaC", "waaD", "waaE", "waaF", "waaG", "waaH", "waaI", "waaJ",
    "waaL", "waaP", "waaQ", "waaR", "waaS", "waaU", "waaY", "waaZ",
    "rfaB", "rfaC", "rfaD", "rfaE", "rfaF", "rfaG", "rfaH", "rfaI", "rfaJ", "rfaL",
    "rfaP", "rfaQ", "rfaS", "rfaY", "rfaZ",
    "lpcA", "gmhA", "gmhB", "hldD", "hldE", "kdsA", "kdsB", "kdsC", "kdsD",
    "lpxA", "lpxB", "lpxC", "lpxD", "lpxH", "lpxK", "lpxL", "lpxM", "lpxP",
    "msbA", "lptA", "lptB", "lptC", "lptD", "lptE", "yhbN", "yhbG", "imp", "rlpB",
)


def carbon_bond_map() -> dict[str, str]:
    """paper gene -> its primary reaction's carbon-bond call, from both gof tables. A gene
    in both keeps one call because both rows resolve the same b-number to the same reaction;
    the assertion below is what keeps that true rather than assumed."""
    out: dict[str, str] = {}
    for t in GOF_TABLES:
        for r in pd.read_csv(GOF_DIR / t, dtype=str).fillna("").to_dict("records"):
            prev = out.setdefault(r["gene"], r["carbon_bond_change"])
            if prev != r["carbon_bond_change"]:
                raise SystemExit(f"[analyse] {r['gene']} carries two different "
                                 f"carbon_bond_change calls across the gof tables")
    return out


def within_hits(df: pd.DataFrame, bond: dict, log) -> dict:
    """The ranking over the genes Fang rebuilt and assayed. No null assumption."""
    sub = df[df.assayed].copy()
    sub["carbon_bond_change"] = sub.fang_gene.map(bond).fillna("")
    sub = sub.sort_values("score", ascending=False).reset_index(drop=True)
    sub["rank"] = np.arange(1, len(sub) + 1)
    a_e, p_e = A.auc(sub.score.to_numpy(), sub.is_positive.to_numpy())
    a_n, p_n = A.auc(sub.n_rxn.to_numpy().astype(float), sub.is_positive.to_numpy())
    out = dict(n=len(sub), n_positive=int(sub.is_positive.sum()),
               auc=a_e, auc_p=p_e, size_auc=a_n, size_auc_p=p_n)
    log(f"\n  -- within the {len(sub)} genes Fang rebuilt and assayed "
        f"({int(sub.is_positive.sum())} raised the titer) " + "-" * 6)
    log(f"     AUC {a_e:.4f} (p={p_e:.3g})   |   size control {a_n:.4f} (p={p_n:.3g})")
    mapped = sub[sub.n_rxn > 0]
    if len(mapped) and mapped.is_positive.nunique() == 2:
        a_m, p_m = A.auc(mapped.score.to_numpy(), mapped.is_positive.to_numpy())
        out["auc_atom_mapped"] = dict(n=len(mapped), auc=a_m, p=p_m,
                                      n_positive=int(mapped.is_positive.sum()))
        log(f"     over the {len(mapped)} atom-mapped only: AUC {a_m:.4f} (p={p_m:.3g})")
    cbc = sub[sub.carbon_bond_change.isin(CARBON_CHANGE)]
    out["carbon_bond_change_subset"] = dict(
        n=len(cbc), n_positive=int(cbc.is_positive.sum()),
        genes=sorted(cbc.fang_gene.astype(str)))
    if len(cbc) and cbc.is_positive.nunique() == 2:
        a_c, p_c = A.auc(cbc.score.to_numpy(), cbc.is_positive.to_numpy())
        out["carbon_bond_change_subset"].update(auc=a_c, p=p_c)
        log(f"     restricted to the {len(cbc)} genes whose reaction changes a carbon "
            f"skeleton: AUC {a_c:.4f} (p={p_c:.3g})")
    else:
        log(f"     restricted to carbon-bond-changing genes: {len(cbc)} gene(s), "
            f"{int(cbc.is_positive.sum())} positive -- no AUC is defined")
    log("\n     the assayed genes by |delta_ratio_pct|:")
    log(sub[["rank", "fang_gene", "fang_direction", "fang_background", "fang_ffa_mg_l",
             "n_rxn", "carbon_bond_change", "delta_ratio_pct"]]
        .to_string(index=False, float_format=lambda v: f"{v:.6g}"))
    out["table"] = sub[["rank", "fang_gene", "fang_direction", "fang_background",
                        "fang_ffa_mg_l", "n_rxn", "carbon_bond_change",
                        "delta_ratio_pct", "score"]].to_dict("records")
    return out


def place_focus(df: pd.DataFrame, log) -> dict:
    row = df[df.fang_gene.astype(str) == FOCUS]
    if row.empty:
        log(f"\n  {FOCUS} is not in this sweep")
        return {}
    r = row.iloc[0]
    lib = df.sort_values("score", ascending=False).reset_index(drop=True)
    rank_lib = int(lib.index[lib.gene == r.gene][0]) + 1
    ass = df[df.assayed].sort_values("score", ascending=False).reset_index(drop=True)
    rank_ass = int(ass.index[ass.gene == r.gene][0]) + 1
    null = df.loc[~df.is_positive, "score"].to_numpy()
    pct = float((null < r.score).mean() + 0.5 * (null == r.score).mean())
    out = dict(gene=FOCUS, n_rxn=int(r.n_rxn), delta_ratio_pct=float(r.delta_ratio_pct),
               rank_in_library=rank_lib, library_n=len(lib),
               rank_among_assayed=rank_ass, assayed_n=len(ass),
               pct_rank_vs_nonpositives=pct)
    log(f"\n  -- where {FOCUS} lands " + "-" * 52)
    log(f"     delta_ratio_pct {r.delta_ratio_pct:+.6g}%  ({int(r.n_rxn)} atom-mapped "
        f"reaction(s))")
    log(f"     rank {rank_lib:,} of {len(lib):,} in the whole ASKA roster; "
        f"{rank_ass} of {len(ass)} among the genes Fang assayed")
    log(f"     percentile against the {null.size:,} non-positive clones: {pct:.4f}")
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--fold", type=float, default=2.0)
    ap.add_argument("--element", default="C")
    ap.add_argument("--channels", nargs="+", default=["gem", "denovo"])
    ap.add_argument("--suffix", nargs="+", default=["", "_lanes2"],
                    help="filename tags to try per channel, first match wins")
    ap.add_argument("--reps", type=int, default=2000)
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--host", default="e_coli_k12")
    ap.add_argument("--label", default="fang")
    ap.add_argument("--sweep-dir", type=Path, default=SWEEPS)
    ap.add_argument("--out-dir", type=Path, default=SWEEPS)
    a = ap.parse_args()

    lines: list[str] = []

    def log(s=""):
        print(s)
        lines.append(s)

    bond = carbon_bond_map()
    report: dict = {}
    for ch in a.channels:
        f = next((a.sweep_dir / f"{a.label}_ratio_sweep_{ch}_{a.host}_fold{a.fold}_"
                  f"{a.element}{s}.tsv" for s in a.suffix
                  if (a.sweep_dir / f"{a.label}_ratio_sweep_{ch}_{a.host}_fold{a.fold}_"
                      f"{a.element}{s}.tsv").exists()), None)
        if f is None:
            log(f"\n### {ch}: no sweep on disk -- skipped")
            continue
        all_df = pd.read_csv(f, sep="\t")
        all_df["is_positive"] = all_df.is_positive.astype(bool)
        all_df["assayed"] = all_df.assayed.astype(bool)
        report[ch] = {"file": f.name, "axes": {}}
        log(f"\n{'=' * 78}\n### channel {ch}  --  {f.name}\n{'=' * 78}")

        for axis in sorted(all_df.axis.unique()):
            df = all_df[all_df.axis == axis].reset_index(drop=True)
            df["score"] = df.delta_ratio_pct.abs().astype(float)
            # `analyse_aska_sweep.analyse` prints the positives beside their phenotype and
            # names that column for its own cohort. Aliasing here keeps one implementation
            # of the AUC rather than forking it for a column name.
            df["eydallin_phenotype"] = df.fang_direction
            log(f"\n{'-' * 78}\n## axis {axis}  ({ch})\n"
                f"   {len(df):,} clone genes, {int((df.n_rxn > 0).sum()):,} atom-mapped, "
                f"{int(df.assayed.sum())} assayed by Fang, "
                f"{int(df.is_positive.sum())} of those beneficial\n"
                f"   host ratio {df.host_ratio.iloc[0]:.9f} "
                f"(num {df.host_num.iloc[0]:.6f} / den {df.host_den.iloc[0]:.6f})\n"
                f"{'-' * 78}")
            r: dict = {}
            r["within_hits"] = within_hits(df, bond, log)
            r["focus"] = place_focus(df, log)
            log(f"\n  -- roster-wide, ASSUMING every unsequenced clone is a true negative "
                + "-" * 4)
            r["roster_wide"] = A.analyse(df, f"{ch}:{axis}", log)
            r["roster_wide"]["resample"] = A.resample(df, n_neg=100, reps=a.reps,
                                                      seed=a.seed, log=log)

            mod = (df.gene.isin(LPS_CORE_MODULE)
                   | df.fang_gene.astype(str).isin(LPS_CORE_MODULE))
            struck = sorted(df.loc[mod & df.is_positive, "fang_gene"].astype(str))
            log(f"\n  -- tautology control: {int(mod.sum())} LPS-core-module clone(s) "
                f"removed, {len(struck)} of them positive ({', '.join(struck) or 'none'}) "
                + "-" * 4)
            rest = df[~mod].reset_index(drop=True)
            if rest.is_positive.nunique() == 2:
                r["no_lps_module"] = A.analyse(rest, f"{ch}:{axis}:no_lps_module", log)
                r["no_lps_module"]["struck"] = struck
            else:
                log("     every positive is in the module; no AUC remains to compute")
                r["no_lps_module"] = dict(struck=struck, note="all positives struck")
            log(f"\n  the top 20 of the library by |delta_ratio_pct|:")
            log(df.nlargest(20, "score")[["gene", "fang_gene", "n_rxn", "delta_ratio_pct",
                                          "is_positive", "assayed"]]
                .to_string(index=False, float_format=lambda v: f"{v:.6g}"))
            report[ch]["axes"][axis] = r

    a.out_dir.mkdir(parents=True, exist_ok=True)
    stem = f"{a.label}_ratio_classifier_report"
    (a.out_dir / f"{stem}.json").write_text(json.dumps(report, indent=2, default=float))
    (a.out_dir / f"{stem}.txt").write_text("\n".join(lines) + "\n")
    print(f"\n-> {a.out_dir}/{stem}.{{json,txt}}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

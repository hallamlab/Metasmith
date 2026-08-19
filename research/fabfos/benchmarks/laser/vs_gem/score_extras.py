#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import common as C  # noqa: E402
import score as S  # noqa: E402

RNG = np.random.default_rng(23)


def coverage_table(pred: pd.DataFrame, idx: pd.DataFrame) -> pd.DataFrame:
    stage1 = pd.read_csv(C.OUT / "panel_coverage_stage1.tsv", sep="\t")
    rows = [dict(gate=r.gate, arm="(all)", n_obs=r.n_obs, n_targets=r.n_targets,
                 example_lost=r.example_lost) for r in stage1.itertuples(index=False)]
    tmap = {r.design_id: [m for m in str(r.target_mnxms).split(";") if m]
            for r in idx.itertuples(index=False)}
    nonprobe = set(idx[~idx.is_probe].design_id)
    for unit, u in pred.groupby("unit"):
        real = u[~u.is_counterfactual]
        if real.empty:
            continue
        have = set(zip(real.design_id, real.mnxm))
        scored = real[real.status != "not_in_base"]
        scored_set = set(zip(scored.design_id, scored.mnxm))
        n_obs = n_tgt = n_abs = 0
        tset = set()
        for did, ts in tmap.items():
            if did not in nonprobe:
                continue
            hit = [t for t in ts if (did, t) in scored_set]
            miss = [t for t in ts if (did, t) in have and (did, t) not in scored_set]
            if hit:
                n_obs += 1
                tset.update(hit)
            n_abs += len(miss)
        rows.append(dict(gate="target present in this arm's panel", arm=unit,
                         n_obs=n_obs, n_targets=len(tset),
                         example_lost=f"{n_abs} target cells abstained"))
    out = pd.DataFrame(rows)
    out.to_csv(C.OUT / "T1_panel_coverage.tsv", sep="\t", index=False)
    return out


def within_paper_series(pred: pd.DataFrame, idx: pd.DataFrame) -> pd.DataFrame:
    rows = []
    idx = idx.copy()
    idx["mag"] = pd.to_numeric(idx.magnitude, errors="coerce")
    for (rec, tgt, mt), g in idx.groupby(
            ["source_record", "target", "measurement_type"]):
        if len(g) < 2 or g.mag.notna().sum() < 2:
            continue
        if g.add_mnxr.fillna("").nunique() + g.del_mnxr.fillna("").nunique() < 3:
            continue
        for unit, u in pred[~pred.is_counterfactual].groupby("unit"):
            sub = u[u.design_id.isin(g.design_id)]
            tm = [m for m in str(g.target_mnxms.iloc[0]).split(";") if m]
            if not tm:
                continue
            sub = sub[sub.mnxm == tm[0]]
            sub = sub[sub.status != "not_in_base"]
            j = g[["design_id", "mag"]].merge(sub[["design_id", "value"]],
                                              on="design_id").dropna()
            if len(j) < 2:
                continue
            tau, n_pairs = kendall(j.value.to_numpy(), j.mag.to_numpy())
            rows.append(dict(unit=unit, source_record=rec, target=tgt,
                             measurement_type=mt, n_arms=len(j),
                             n_pairs=n_pairs, kendall_tau=tau))
    out = pd.DataFrame(rows)
    out.to_csv(C.OUT / "T8_within_paper_series.tsv", sep="\t", index=False)
    return out


def cross_paper_series(pred: pd.DataFrame, idx: pd.DataFrame) -> pd.DataFrame:
    rows = []
    idx = idx.copy()
    idx["mag"] = pd.to_numeric(idx.magnitude, errors="coerce")
    g0 = idx[(idx.measurement_type == "fold") & idx.mag.notna()]
    for (tgt, host), g in g0.groupby(["target", "host_dir"]):
        if g.source_record.nunique() < 2 or len(g) < 3:
            continue
        tm = [m for m in str(g.target_mnxms.iloc[0]).split(";") if m]
        if not tm:
            continue
        for unit, u in pred[~pred.is_counterfactual].groupby("unit"):
            sub = u[(u.design_id.isin(g.design_id)) & (u.mnxm == tm[0]) &
                    (u.status != "not_in_base")]
            j = g[["design_id", "mag"]].merge(sub[["design_id", "value"]],
                                              on="design_id").dropna()
            if len(j) < 3:
                continue
            tau, n_pairs = kendall(j.value.to_numpy(), j.mag.to_numpy())
            rows.append(dict(unit=unit, target=tgt, host=host, n_designs=len(j),
                             n_papers=g.source_record.nunique(), n_pairs=n_pairs,
                             kendall_tau=tau))
    out = pd.DataFrame(rows)
    out.to_csv(C.OUT / "T13_cross_paper_series.tsv", sep="\t", index=False)
    return out


def kendall(x, y) -> tuple:
    n = len(x)
    conc = disc = 0
    for i in range(n):
        for j in range(i + 1, n):
            a, b = x[i] - x[j], y[i] - y[j]
            if a == 0 or b == 0:
                continue
            if (a > 0) == (b > 0):
                conc += 1
            else:
                disc += 1
    tot = conc + disc
    return ((conc - disc) / tot if tot else np.nan), tot


def permutation_nulls(nulls: pd.DataFrame, pred: pd.DataFrame,
                      idx: pd.DataFrame, n_perm: int = 500) -> pd.DataFrame:
    rows = []
    for unit, g in nulls.groupby("unit"):
        obs = float(np.nanmedian(g.observed))
        u = pred[(pred.unit == unit) & (~pred.is_counterfactual)]
        wide = u.pivot_table(index="design_id", columns="mnxm", values="value")

        tgt_stats = []
        keys = g[["design_id", "mnxm", "size_bin"]].dropna()
        for _ in range(n_perm):
            vals = []
            for sb, sub in keys.groupby("size_bin"):
                perm = RNG.permutation(sub.mnxm.to_numpy())
                for did, m in zip(sub.design_id, perm):
                    if did in wide.index and m in wide.columns:
                        v = wide.loc[did, m]
                        if np.isfinite(v):
                            vals.append(v)
            if vals:
                tgt_stats.append(float(np.median(vals)))

        pan_stats = []
        arr = wide.to_numpy(float)
        cols = list(wide.columns)
        cidx = {c: i for i, c in enumerate(cols)}
        ridx = {r: i for i, r in enumerate(wide.index)}
        pick = [(ridx[d], cidx[m]) for d, m in zip(g.design_id, g.mnxm)
                if d in ridx and m in cidx]
        for _ in range(n_perm):
            vals = []
            for r, _c in pick:
                row = arr[r]
                fin = row[np.isfinite(row)]
                if fin.size:
                    vals.append(float(RNG.choice(fin)))
            if vals:
                pan_stats.append(float(np.median(vals)))

        rows.append(dict(
            unit=unit, arm=g.arm.iloc[0], observed_median=obs,
            target_perm_p=C.empirical_p(obs, np.array(tgt_stats), "greater"),
            target_perm_median=float(np.median(tgt_stats)) if tgt_stats else np.nan,
            panel_perm_p=C.empirical_p(obs, np.array(pan_stats), "greater"),
            panel_perm_median=float(np.median(pan_stats)) if pan_stats else np.nan))
    out = pd.DataFrame(rows)
    out.to_csv(C.OUT / "T9_permutation_nulls.tsv", sep="\t", index=False)
    return out


def percentile_rank_panel(pred: pd.DataFrame, idx: pd.DataFrame) -> pd.DataFrame:
    tmap = {r.design_id: [m for m in str(r.target_mnxms).split(";") if m]
            for r in idx.itertuples(index=False)}
    rows = []
    for unit, u in pred[~pred.is_counterfactual].groupby("unit"):
        if u.pct_rank.isna().all():
            continue
        vals = []
        for did, g in u.groupby("design_id"):
            gm = g.set_index("mnxm")
            for t in tmap.get(did, []):
                if t in gm.index and gm.loc[t, "status"] != "not_in_base":
                    v = gm.loc[t, "pct_rank"]
                    v = float(v.iloc[0]) if isinstance(v, pd.Series) else float(v)
                    if np.isfinite(v):
                        vals.append(v)
        if vals:
            rows.append(dict(unit=unit, n=len(vals),
                             median_percentile_rank=float(np.median(vals)),
                             chance=0.5,
                             frac_top_decile=float(np.mean(np.array(vals) >= 0.9))))
    out = pd.DataFrame(rows)
    out.to_csv(C.OUT / "T10_percentile_rank.tsv", sep="\t", index=False)
    return out


def main():
    pred = S.load_predictions()
    idx = S.design_index()
    pred = pd.concat([pred, S.trivial_arm([])], ignore_index=True)
    nulls = pd.read_csv(C.OUT / "counterfactual_nulls.tsv", sep="\t")

    print("== T1 coverage =="); print(coverage_table(pred, idx).to_string(index=False))
    print("\n== T8 within-paper series ==")
    print(within_paper_series(pred, idx).to_string(index=False))
    cp = cross_paper_series(pred, idx)
    print("\n== T13 cross-paper series (corroborating only) ==")
    print(cp.groupby("unit").agg(groups=("target", "size"),
                                 pairs=("n_pairs", "sum"),
                                 median_tau=("kendall_tau", "median")).to_string())
    print("\n== T9 permutation nulls ==")
    print(permutation_nulls(nulls, pred, idx).to_string(index=False))
    print("\n== T10 percentile rank (described, not scored) ==")
    print(percentile_rank_panel(pred, idx).to_string(index=False))


if __name__ == "__main__":
    main()

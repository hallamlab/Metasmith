#!/usr/bin/env python3
"""Score `data/fabfos/runs/scadc_ecspr/results.parquet` against the frozen null draws in
`data/fabfos/runs/scadc_ecspr/null/draws.parquet` (plan T3). Pure local join + stats pass
-- no fir job, no --preflight/--run/--retrieve/--publish shape (see plan's T3
approach note: "this step runs locally").

    python examples/scadc_ecspr_null_score.py

For each non-host `(unit, condition_id, metric)` row: match `n_orfs` to the
nearest null N-bucket actually drawn (never interpolate -- the bucket used is
recorded as a new `n_bucket` column), compute an empirical p-value against
that bucket's null draws -- POOLING both sampler styles A (uniform) and D
(contiguous window) together, since draws.parquet keeps the `style` column
and can be re-split/re-scored separately later if that combination turns out
to be the wrong default.
`delta_total` (direction='up') is up-tail only; `delta_clr`
(direction='two_sided') is two-sided. BH-FDR is applied separately within
each metric's family (732 non-host tests per metric) for `q`; `survives` is
`q < 0.05` -- also an open/reversible default, documented rather than
user-confirmed (autopilot stance: results.parquet can be trivially re-scored
with a different threshold later, this is not a one-way door).
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
RESULTS = ROOT / "data" / "fabfos" / "runs" / "scadc_ecspr" / "results.parquet"
NULL_DRAWS = ROOT / "data" / "fabfos" / "runs" / "scadc_ecspr" / "null" / "draws.parquet"
Q_THRESHOLD = 0.05


def nearest_bucket(n_orfs: int, buckets: list[int]) -> int:
    return min(buckets, key=lambda b: abs(b - n_orfs))


def empirical_p(delta_obs: float, null_vals: np.ndarray, two_sided: bool) -> float:
    n = len(null_vals)
    if two_sided:
        exceed = np.sum(np.abs(null_vals) >= abs(delta_obs))
    else:
        exceed = np.sum(null_vals >= delta_obs)
    return (exceed + 1) / (n + 1)


def bh_fdr(pvals: np.ndarray) -> np.ndarray:
    n = len(pvals)
    order = np.argsort(pvals)
    ranked = pvals[order]
    q = ranked * n / (np.arange(n) + 1)
    q = np.minimum.accumulate(q[::-1])[::-1]
    q = np.clip(q, 0, 1)
    out = np.empty(n)
    out[order] = q
    return out


def main() -> int:
    results = pd.read_parquet(RESULTS)
    null = pd.read_parquet(NULL_DRAWS)

    buckets = sorted(null["n_rep"].unique().tolist())
    print(f"[score] {len(buckets)} null N-buckets: {buckets}")

    host_mask = results["unit"] == "epi300_host"
    results["n_bucket"] = pd.array([pd.NA] * len(results), dtype="Int64")
    results.loc[~host_mask, "n_bucket"] = (
        results.loc[~host_mask, "n_orfs"].apply(lambda n: nearest_bucket(n, buckets))
    )

    # pool both styles together per (n_bucket, condition_id, metric)
    null_groups = {
        key: g["delta_null"].to_numpy()
        for key, g in null.groupby(["n_rep", "condition_id", "metric"])
    }

    p = np.full(len(results), np.nan)
    for i, row in results.loc[~host_mask].iterrows():
        key = (int(row["n_bucket"]), row["condition_id"], row["metric"])
        vals = null_groups.get(key)
        if vals is None:
            raise SystemExit(f"[score] no null draws for bucket {key}")
        two_sided = row["direction"] == "two_sided"
        p[i] = empirical_p(row["delta_obs"], vals, two_sided)
    results["p"] = p

    results["q"] = np.nan
    for metric, idx in results.loc[~host_mask].groupby("metric").groups.items():
        pv = results.loc[idx, "p"].to_numpy()
        results.loc[idx, "q"] = bh_fdr(pv)

    results["survives"] = pd.array([pd.NA] * len(results), dtype="boolean")
    results.loc[~host_mask, "survives"] = (results.loc[~host_mask, "q"] < Q_THRESHOLD)

    n_survive = int(results.loc[~host_mask, "survives"].sum())
    n_tested = int((~host_mask).sum())
    print(f"[score] {n_survive}/{n_tested} non-host rows survive q < {Q_THRESHOLD}")
    print(results.loc[~host_mask].groupby("metric")["survives"].sum())

    results.to_parquet(RESULTS, index=False)
    print(f"[score] wrote {RESULTS.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

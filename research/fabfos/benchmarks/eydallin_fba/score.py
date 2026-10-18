#!/usr/bin/env python3
"""Score the constructed-model cohort against Fig. 1.

    mamba run -n ecspr python research/fabfos/benchmarks/eydallin_fba/score.py

Reads `out/cohort.tsv` alone and reproduces every number in the README from it.

Two statistics per channel, arm, medium and growth pin.

  sign accuracy   The share of answered clones whose predicted direction matches the
                  measured one.  It is printed beside the base rate a constant predictor of
                  the same sign scores on the same answered clones.  A run that only equals
                  its own base rate has predicted nothing, and this arm's `force` numbers
                  are exactly that, so the two columns belong side by side.

  Spearman        Rank correlation between `pct_diff` and the measured `pct_wt`.  It is the
                  only statistic here that can separate a constant-sign predictor from a
                  real one, because it reads magnitude rather than direction.

**CAUTION** An unanswered clone is dropped, never counted as a miss.  The counts say how
many were dropped and why, because a small answered set is the more important result on
this cohort than any accuracy computed on it.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import common as C  # noqa: E402

WT = 100.0


def load() -> pd.DataFrame:
    return pd.read_csv(C.OUT / "cohort.tsv", sep="\t")


def score(d: pd.DataFrame) -> pd.DataFrame:
    from scipy.stats import spearmanr

    rows = []
    keys = ["channel", "arm", "medium", "pin"]
    for k, g in d.groupby(keys):
        ok = g[g.status == "ok"].dropna(subset=["pct_diff", "measured_pct_wt"])
        rec = dict(zip(keys, k), n_total=len(g), n_answered=len(ok))
        for why, n in g.status.value_counts().items():
            rec[f"n_{why}"] = int(n)
        if len(ok) >= 3:
            up_meas = ok.measured_pct_wt > WT
            up_pred = ok.pct_diff > 0
            rec["accuracy"] = float((up_meas == up_pred).mean())
            rec["base_rate"] = float(max(up_meas.mean(), 1 - up_meas.mean()))
            rec["pred_all_down"] = bool((~up_pred).all())
            rho, p = spearmanr(ok.pct_diff, ok.measured_pct_wt)
            rec["spearman"] = float(rho)
            rec["spearman_p"] = float(p)
        rows.append(rec)
    return pd.DataFrame(rows).sort_values(keys)


def main() -> int:
    d = load()
    s = score(d)
    s.to_csv(C.OUT / "score.tsv", sep="\t", index=False)

    cols = ["channel", "arm", "medium", "pin", "n_answered", "accuracy",
            "base_rate", "spearman", "spearman_p", "pred_all_down"]
    show = s[[c for c in cols if c in s.columns]].copy()
    for c in ("accuracy", "base_rate", "spearman", "spearman_p"):
        if c in show:
            show[c] = show[c].round(3)
    print(show.to_string(index=False))

    print("\nwhy a clone goes unanswered, by channel and arm:")
    why = (d[d.status != "ok"].groupby(["channel", "arm", "status"]).size()
           .unstack(fill_value=0))
    print(why.to_string())

    print(f"\nout/score.tsv holds {len(s)} rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

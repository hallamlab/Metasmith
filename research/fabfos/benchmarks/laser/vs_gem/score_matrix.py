#!/usr/bin/env python3
"""The design x target matrix itself, plus the two gates that license the null.

* `out/matrix_<unit>.parquet` -- the matrix as a first-class artifact, with the
  per-column z-score (so a target's baseline reachability cancels) and the
  per-condition empirical p joined on.
* `out/T11_size_matching.tsv` -- the gate the counterfactual null depends on. If a
  bin's real conditions sit at the top of its size range against counterfactuals
  clustered at the bottom, the null manufactures significance. This reports the
  per-bin size distributions for real against counterfactual designs, as a gate
  rather than an appendix.
* `out/T12_numerics.tsv` -- convergence, CHOLMOD warnings, and signal against the
  measured noise floor per unit.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import common as C  # noqa: E402
import score as S  # noqa: E402


def matrices(pred: pd.DataFrame, nulls: pd.DataFrame):
    pj = nulls[["unit", "design_id", "mnxm", "p_bin", "p_resid", "z_resid"]]
    for unit, u in pred.groupby("unit"):
        m = u[["design_id", "condition_id", "is_counterfactual", "n_add", "n_del",
               "size_bin", "composition", "mnxm", "value", "status"]].copy()
        # z-score WITHIN column: the target's own baseline reachability cancels,
        # which is what makes columns comparable at all.
        g = m.groupby("mnxm").value
        mu, sd = g.transform("mean"), g.transform("std")
        m["z"] = (m.value - mu) / sd.replace(0, np.nan)
        m = m.merge(pj[pj.unit == unit].drop(columns="unit"),
                    on=["design_id", "mnxm"], how="left")
        m.insert(0, "unit", unit)
        m.to_parquet(C.OUT / f"matrix_{unit}.parquet", index=False)
        yield unit, m


def size_matching(pred: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for unit, u in pred.groupby("unit"):
        d = u.drop_duplicates("design_id")
        for sb, g in d.groupby("size_bin"):
            r = g[~g.is_counterfactual]
            c = g[g.is_counterfactual]
            if r.empty or c.empty:
                continue
            tot_r = (r.n_add + r.n_del).to_numpy(float)
            tot_c = (c.n_add + c.n_del).to_numpy(float)
            rows.append(dict(
                unit=unit, size_bin=sb, n_real=len(r), n_cf=len(c),
                real_median_edits=float(np.median(tot_r)),
                cf_median_edits=float(np.median(tot_c)),
                real_mean_add=float(r.n_add.mean()), cf_mean_add=float(c.n_add.mean()),
                real_mean_del=float(r.n_del.mean()), cf_mean_del=float(c.n_del.mean()),
                median_shift=float(np.median(tot_r) - np.median(tot_c))))
    out = pd.DataFrame(rows)
    out.to_csv(C.OUT / "T11_size_matching.tsv", sep="\t", index=False)
    return out


def numerics(scal: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for unit, g in scal.groupby("unit"):
        ok = g[g.status == "ok"]
        d = dict(unit=unit, n_designs=len(g), n_ok=len(ok),
                 n_skipped=int((g.status != "ok").sum()))
        if "converged" in ok and ok.converged.notna().any():
            d["frac_converged"] = float(ok.converged.astype(float).mean())
        if "n_cholmod_warnings" in ok:
            d["cholmod_warnings"] = float(ok.n_cholmod_warnings.fillna(0).sum())
        if "noise_floor" in ok and ok.noise_floor.notna().any():
            d["noise_floor_median"] = float(ok.noise_floor.median())
            sig = (ok.i_eff_pert - ok.i_eff_base).abs() if "i_eff_pert" in ok else None
            if sig is not None:
                d["signal_median"] = float(sig.median())
                d["frac_above_10x_floor"] = float(
                    (sig > 10 * ok.noise_floor).mean())
        if "seconds" in ok and ok.seconds.notna().any():
            d["median_seconds"] = float(ok.seconds.median())
        rows.append(d)
    out = pd.DataFrame(rows)
    out.to_csv(C.OUT / "T12_numerics.tsv", sep="\t", index=False)
    return out


def main():
    pred = S.load_predictions()
    pred = pd.concat([pred, S.trivial_arm([])], ignore_index=True)
    nulls = pd.read_csv(C.OUT / "counterfactual_nulls.tsv", sep="\t")
    n = 0
    for unit, m in matrices(pred, nulls):
        n += 1
        print(f"  matrix_{unit}.parquet  {m.design_id.nunique()} x "
              f"{m.mnxm.nunique()}")
    print(f"{n} matrices written\n")
    print("== T11 size matching gate ==")
    print(size_matching(pred).to_string(index=False))
    scal = S.load_scalars()
    if len(scal):
        print("\n== T12 numerics ==")
        print(numerics(scal).to_string(index=False))
    # the name the plan's acceptance criterion asks for
    pd.read_csv(C.OUT / "T1_panel_coverage.tsv", sep="\t").to_csv(
        C.OUT / "panel_coverage.tsv", sep="\t", index=False)


if __name__ == "__main__":
    main()

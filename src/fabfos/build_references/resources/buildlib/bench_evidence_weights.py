from __future__ import annotations

import numpy as np
import pandas as pd


def nomination_contributions(df: pd.DataFrame) -> pd.DataFrame:
    d = df.drop_duplicates(["orf", "channel", "intermediate_id", "mnxr"]).copy()

    nom = (
        d.groupby(["orf", "channel", "intermediate_id"], sort=False)
        .agg(s_n=("raw_score", "max"), F_n=("mnxr", "nunique"))
        .reset_index()
    )
    grp = nom.groupby(["orf", "channel"], sort=False)
    nom["sum_s"] = grp["s_n"].transform("sum")
    nom["n_nom"] = grp["intermediate_id"].transform("count")

    nonzero = nom["sum_s"] > 0
    nom["w_n"] = np.where(nonzero, nom["s_n"] / nom["sum_s"], 1.0 / nom["n_nom"])
    nom["contrib"] = nom["w_n"] / nom["F_n"]

    out = d.merge(
        nom[["orf", "channel", "intermediate_id", "contrib"]],
        on=["orf", "channel", "intermediate_id"],
        how="left",
    )
    n_lanes = out.groupby("orf")["channel"].transform("nunique")
    out["contrib"] = out["contrib"] / n_lanes
    return out


def assert_conservation(contrib_rows: pd.DataFrame, label: str) -> None:
    per_orf = contrib_rows.groupby("orf")["contrib"].sum()
    worst = float((per_orf - 1.0).abs().max())
    assert worst < 1e-9, f"{label}: per-orf conservation off by {worst:.2e}"
    print(f"     [{label}] {len(per_orf):,} protein units, max |sum-1| = {worst:.2e}",
          flush=True)

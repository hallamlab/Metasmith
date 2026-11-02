from __future__ import annotations

import pandas as pd


def nomination_contributions(df: pd.DataFrame) -> pd.DataFrame:
    # The build-side copy of `ecspr.model.evidence.nomination_contributions`. The split
    # is UNIFORM: the score's discriminative job lives in `llr`, which `pool_logodds`
    # reads, and splitting by score here as well would count the same evidence twice.
    # See that module for the full argument.
    d = df.drop_duplicates(["orf", "channel", "intermediate_id", "mnxr"]).copy()

    nom = (
        d.groupby(["orf", "channel", "intermediate_id"], sort=False)
        .agg(F_n=("mnxr", "nunique"))
        .reset_index()
    )
    grp = nom.groupby(["orf", "channel"], sort=False)
    nom["n_nom"] = grp["intermediate_id"].transform("count")
    nom["contrib"] = 1.0 / (nom["n_nom"] * nom["F_n"])

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

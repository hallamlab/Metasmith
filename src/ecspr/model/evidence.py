"""ECSPr belief-conservation evidence weights.

Turns an annotation evidence table (one row per ORF/channel/nominated MNXR, as
`annotation::gpr_table` produces) into per-reaction conductances `E_r`, and into
per-unit (fosmid contig / metaG ORF) addition maps for the same allocation.

Ported from the archived `metasmith_libraries/resources/lib/ecspr_network.py`
(pre-atom-graph star lane) -- the belief-conservation math is topology-agnostic,
only the consumer changed (`ecspr.model.build.graph_from_pairs` takes `E_r` as `weights`
directly, in place of the retired reaction-hub star graph).

Env: numpy + pandas (CPU).
"""
from __future__ import annotations

from collections import defaultdict

import numpy as np
import pandas as pd


def nomination_contributions(df: pd.DataFrame) -> pd.DataFrame:
    """Per-row contribution = (w_n / F_n) / L_orf, where the nomination unit is
    (orf, channel, intermediate_id), w_n splits raw_score within (orf, channel),
    F_n = distinct-mnxr fanout, and L_orf = distinct channels for the ORF. Each
    ORF's contributions sum to 1.0 (belief conservation), making leave-one-out an
    exact subtraction downstream."""
    d = df.drop_duplicates(["orf", "channel", "intermediate_id", "mnxr"]).copy()
    nom = (d.groupby(["orf", "channel", "intermediate_id"], sort=False)
           .agg(s_n=("raw_score", "max"), F_n=("mnxr", "nunique"))
           .reset_index())
    grp = nom.groupby(["orf", "channel"], sort=False)
    nom["sum_s"] = grp["s_n"].transform("sum")
    nom["n_nom"] = grp["intermediate_id"].transform("count")
    nonzero = nom["sum_s"] > 0
    nom["w_n"] = np.where(nonzero, nom["s_n"] / nom["sum_s"], 1.0 / nom["n_nom"])
    nom["contrib"] = nom["w_n"] / nom["F_n"]
    out = d.merge(nom[["orf", "channel", "intermediate_id", "contrib"]],
                  on=["orf", "channel", "intermediate_id"], how="left")
    n_lanes = out.groupby("orf")["channel"].transform("nunique")
    out["contrib"] = out["contrib"] / n_lanes
    return out


def _assert_conservation(rows: pd.DataFrame, label: str) -> None:
    per_orf = rows.groupby("orf")["contrib"].sum()
    worst = float((per_orf - 1.0).abs().max()) if len(per_orf) else 0.0
    assert worst < 1e-9, f"{label}: per-orf conservation off by {worst:.2e}"


def compute_E(df_src: pd.DataFrame, label: str = "") -> pd.Series:
    rows = nomination_contributions(df_src)
    _assert_conservation(rows, label or "E")
    return rows.groupby("mnxr")["contrib"].sum()


def compute_weights(ev: pd.DataFrame) -> pd.DataFrame:
    """evidence_weights.parquet: per (source, mnxr) E_full/E_dlec/n_orf."""
    parts = []
    for source, g in ev.groupby("source"):
        e_full = compute_E(g, f"{source}/full")
        dlec = g[g["channel"] == "dl_ec"]
        e_dlec = compute_E(dlec, f"{source}/dlec") if len(dlec) else pd.Series(dtype=float)
        n_orf = g.groupby("mnxr")["orf"].nunique()
        part = (pd.DataFrame({"E_full": e_full})
                .join(e_dlec.rename("E_dlec"), how="outer")
                .join(n_orf.rename("n_orf"), how="left"))
        part["E_full"] = part["E_full"].fillna(0.0)
        part["E_dlec"] = part["E_dlec"].fillna(0.0)
        part["n_orf"] = part["n_orf"].fillna(0).astype(int)
        part.insert(0, "source", source)
        part = part.reset_index().rename(columns={"index": "mnxr"})
        parts.append(part)
    out = pd.concat(parts, ignore_index=True)
    return out[["source", "mnxr", "E_full", "E_dlec", "n_orf"]]


def per_unit_weights(df_src: pd.DataFrame, unit_col: str) -> dict:
    """{unit: {mnxr: E}} with per-ORF-normalized belief, re-aggregated by
    'contig' (per fosmid) or 'orf' (per metaG ORF)."""
    rows = nomination_contributions(df_src)
    if unit_col == "contig":
        rows = rows.copy()
        rows["contig"] = rows["orf"].str.rsplit("_", n=1).str[0]
    agg = rows.groupby([unit_col, "mnxr"], sort=False)["contrib"].sum()
    out: dict = defaultdict(dict)
    for (unit, mnxr), c in agg.items():
        if c > 0:
            out[unit][mnxr] = float(c)
    return dict(out)

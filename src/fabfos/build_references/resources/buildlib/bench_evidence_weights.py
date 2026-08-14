"""The deployed per-protein belief weight, ported verbatim.

``_nomination_contributions`` is lifted from the sibling project's
``04_reaction_network/10_evidence_weights.py``, which is the module the deployed
benchmark GPR imports rather than reimplements. It is copied here for the same reason
every other method module in ``buildlib::`` is: the scheme is the artifact's meaning, and
a second implementation of it would be a second thing to keep in step.

The scheme, and why each part is the way it is:

  * a NOMINATION is ``(protein, channel, intermediate_id)``; its fanout ``F_n`` is the
    count of distinct MNXR in that group.
  * each protein carries a TOTAL BELIEF OF 1.0, split equally across the lanes that
    annotated it -- so a protein resolved by one lane and a protein resolved by three
    carry identical mass. The contrast downstream measures topology, not how many lanes
    happened to fire.
  * within a lane, that share splits across nominations in proportion to ``raw_score``,
    then spreads evenly across each nomination's fanout.
  * the score-split denominator is strictly per ``(protein, channel)``. A global
    normalisation would re-introduce the cross-lane competition the scheme rejects.
  * FANOUT IS UNCAPPED. Dilution is the designed answer to promiscuous EC fanout; capping
    is a scoring-time policy decision and does not belong in an annotation artifact.

``_assert_conservation`` is the invariant that makes the above checkable: per protein the
contributions sum to 1.0. It asserts rather than warns because a scheme that quietly
fails to conserve is a scheme that has silently reweighted the benchmark.

The column is called ``orf`` throughout because that is the deployed function's parameter
name; callers rename their protein key onto it, exactly as ``60_build_gpr.py`` does.
"""
from __future__ import annotations

import numpy as np
import pandas as pd


def nomination_contributions(df: pd.DataFrame) -> pd.DataFrame:
    """Given evidence rows (any channel mix), return per-row contributions where each
    surviving row is one ``(orf, channel, intermediate_id, mnxr)`` nomination member
    carrying ``contrib = w_n / F_n``.
    """
    # one row per distinct mnxr inside a nomination
    d = df.drop_duplicates(["orf", "channel", "intermediate_id", "mnxr"]).copy()

    # nomination-level score s_n (constant within a nomination; max is defensive)
    # and fanout F_n = #distinct mnxr.
    nom = (
        d.groupby(["orf", "channel", "intermediate_id"], sort=False)
        .agg(s_n=("raw_score", "max"), F_n=("mnxr", "nunique"))
        .reset_index()
    )
    # per (orf, channel): score sum and nomination count for the split / fallback
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
    # Normalize per ORF: each lane sums to 1.0 within an ORF, so dividing by the
    # ORF's lane count makes the per-ORF total 1.0 (lanes sum to 1, NOT additive).
    # Single-lane ORFs divide by 1 -- a no-op.
    n_lanes = out.groupby("orf")["channel"].transform("nunique")
    out["contrib"] = out["contrib"] / n_lanes
    return out


def assert_conservation(contrib_rows: pd.DataFrame, label: str) -> None:
    """For each ORF the contributions must sum to 1.0 (lanes split that 1.0)."""
    per_orf = contrib_rows.groupby("orf")["contrib"].sum()
    worst = float((per_orf - 1.0).abs().max())
    assert worst < 1e-9, f"{label}: per-orf conservation off by {worst:.2e}"
    print(f"     [{label}] {len(per_orf):,} protein units, max |sum-1| = {worst:.2e}",
          flush=True)

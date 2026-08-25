# The PRE-CALIBRATION weighting, frozen here so a before/after runs in one process.
#
# This is `ecspr.model.evidence` as it stood at 37b1b72, verbatim except for the names.
# It is a copy on purpose: the shipped module now reads `llr`, and a comparison that
# reconstructed the old behaviour by passing arguments to the new code would be
# comparing the new code to itself. A frozen copy can only be wrong by being a bad copy,
# which is checkable by reading it.
#
# It reads `raw_score` and so runs only against a table built before the change. The
# two tables have the same rows -- the reparse changes what the weight column holds, not
# which calls survive -- so the pair is a clean A/B on the weighting alone.
from __future__ import annotations

import numpy as np
import pandas as pd

LEGACY_LAM0 = -3.0
LEGACY_LAM1 = 1.0
LEGACY_TAU = 0.25
ASSERTION_KEY = ("unit_id", "channel", "intermediate_id")


def normalise(df: pd.DataFrame) -> pd.DataFrame:
    """`ecspr.model.gpr._normalise` for a pre-calibration table.

    The shipped one coerces `llr`, which an old table does not have. Same shape
    otherwise -- a copy for the same reason the rest of this module is one."""
    out = df.copy()
    out["orf"] = out["orf"].astype(str)
    out["intermediate_id"] = out["intermediate_id"].astype(str)
    out["mnxr"] = out["mnxr"].astype(str)
    out["raw_score"] = pd.to_numeric(out["raw_score"], errors="coerce").fillna(1.0)
    return out.dropna(subset=["mnxr"])


def contributions(df: pd.DataFrame) -> pd.DataFrame:
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


def pool(rows: pd.DataFrame, *, lam0=LEGACY_LAM0, lam1=LEGACY_LAM1,
         tau=LEGACY_TAU) -> pd.Series:
    key = [c for c in ASSERTION_KEY if c in rows.columns] + ["mnxr"]
    s = rows.groupby(key, sort=False)["contrib"].sum()
    lam = pd.Series(lam1 * -np.expm1(-s.to_numpy() / tau),
                    index=s.index.get_level_values("mnxr"))
    return 1.0 / (1.0 + np.exp(-(lam.groupby(level=0).sum() + lam0)))


def pooled_logodds(rows: pd.DataFrame, *, lam0=LEGACY_LAM0, lam1=LEGACY_LAM1,
                   tau=LEGACY_TAU) -> pd.Series:
    """The summed log-odds before the sigmoid.

    The tie count is a statement about THIS quantity, not about E: float64 saturates
    the sigmoid at exactly 1.0 once it passes ~37, so two reactions tie when their
    log-odds both clear that, however far apart they are."""
    key = [c for c in ASSERTION_KEY if c in rows.columns] + ["mnxr"]
    s = rows.groupby(key, sort=False)["contrib"].sum()
    lam = pd.Series(lam1 * -np.expm1(-s.to_numpy() / tau),
                    index=s.index.get_level_values("mnxr"))
    return lam.groupby(level=0).sum() + lam0


def new_pooled_logodds(rows: pd.DataFrame, *, lam0, tau) -> pd.Series:
    """The same quantity under the calibrated weighting, for the same comparison."""
    key = [c for c in ASSERTION_KEY if c in rows.columns] + ["mnxr"]
    g = rows.groupby(key, sort=False)
    s = g["contrib"].sum()
    llr = g["llr"].max().to_numpy()
    lam = pd.Series(llr * -np.expm1(-s.to_numpy() / tau),
                    index=s.index.get_level_values("mnxr"))
    return lam.groupby(level=0).sum() + lam0

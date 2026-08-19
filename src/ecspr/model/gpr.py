from __future__ import annotations

import pandas as pd

from .evidence import per_unit_weights

WEIGHTINGS = ("belief", "uniform")

# `orf` NAMES THE NOMINATOR and is never null -- an ORF usually, a construct or model
# gene on a curated row. The schema populates it once, at the producer, so belief
# conservation groups per nominator without this layer deciding which column that is.
#
# The pre-schema layouts left `feature_id` null on curated rows, and the fallback below
# is what reads them. It is for tables written before the schema, nothing else: a
# fallback that reaches `feature_name` on a de-novo table would group by EC description
# and silently merge unrelated ORFs into one construct.
ORF_COL = "orf"
_LEGACY_ORF_FALLBACK = ("feature_id", "feature_name", "evidence_id")
UNIT_COL = "unit_id"


def load_gpr(paths) -> pd.DataFrame:
    if isinstance(paths, (str, bytes)) or hasattr(paths, "__fspath__"):
        paths = [paths]
    frames = [pd.read_parquet(p) for p in paths]
    df = pd.concat(frames, ignore_index=True, sort=False)
    missing = {"mnxr", "channel", "raw_score", UNIT_COL} - set(df.columns)
    if missing:
        raise ValueError(f"GPR table is missing required columns: {sorted(missing)}")
    return df


def _select(df, column, values):
    if not column or not len(values):
        return None
    if column not in df.columns:
        raise ValueError(f"mask column {column!r} is not in the GPR table "
                         f"(has {sorted(df.columns)})")
    return df[column].astype("object").isin(list(values))


def apply_mask(df: pd.DataFrame, *, background_column=None, background_values=(),
               mask_column=None, mask_values=(), drop_column=None,
               drop_values=()) -> pd.DataFrame:
    bg = _select(df, background_column, background_values)
    mk = _select(df, mask_column, mask_values)
    keep = None
    for part in (bg, mk):
        if part is not None:
            keep = part if keep is None else (keep | part)
    out = df if keep is None else df[keep]
    dr = _select(out, drop_column, drop_values)
    return out if dr is None else out[~dr]


def _normalise(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    orf = None
    if ORF_COL in out.columns:
        orf = out[ORF_COL]
    else:
        for c in _LEGACY_ORF_FALLBACK:
            if c not in out.columns:
                continue
            orf = out[c] if orf is None else orf.fillna(out[c])
    if orf is None:
        raise ValueError(f"GPR table carries neither {ORF_COL!r} nor any of "
                         f"{_LEGACY_ORF_FALLBACK}, so belief conservation has no "
                         f"construct to conserve over")
    out["orf"] = orf.astype(str)
    out["intermediate_id"] = (
        out["intermediate_id"] if "intermediate_id" in out.columns
        else out["evidence_id"] if "evidence_id" in out.columns
        else out["mnxr"]).astype(str)
    out["mnxr"] = out["mnxr"].astype(str)
    out["raw_score"] = pd.to_numeric(out["raw_score"], errors="coerce").fillna(1.0)
    return out.dropna(subset=["mnxr"])


def weights_from_rows(rows: pd.DataFrame, weighting: str = "belief") -> dict:
    if weighting not in WEIGHTINGS:
        raise ValueError(f"weighting must be one of {WEIGHTINGS}, got {weighting!r}")
    if rows.empty:
        return {}
    if weighting == "uniform":
        n = (rows.dropna(subset=["mnxr"])
                 .groupby(rows["mnxr"].astype(str))[UNIT_COL].nunique())
        return {str(r): float(v) for r, v in n.items() if v > 0}
    per_unit = per_unit_weights(_normalise(rows), UNIT_COL)
    out: dict = {}
    for unit_map in per_unit.values():
        for mnxr, e in unit_map.items():
            out[mnxr] = out.get(mnxr, 0.0) + float(e)
    return {k: v for k, v in out.items() if v > 0}


def condition_weights(df: pd.DataFrame, *, weighting="belief", **mask) -> tuple:
    rows = apply_mask(df, **mask)
    w = weights_from_rows(rows, weighting)
    in_universe = (int(rows["in_atom_universe"].fillna(False).astype(bool).sum())
                   if "in_atom_universe" in rows.columns else None)
    cov = dict(n_rows=int(len(rows)), n_units=int(rows[UNIT_COL].nunique()),
               n_reactions=len(w), n_rows_in_atom_universe=in_universe)
    return w, cov

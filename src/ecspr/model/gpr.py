"""The GPR table, and the mask a condition draws over it.

ONE TABLE IN, ONE ``{mnxr: E}`` OUT
-----------------------------------
Everything ECSPr measures starts as rows of the long GPR schema -- one row per
(unit, feature, channel, evidence, MNXR) -- and the only thing a condition does to
it is SELECT rows. There is no add, no delete, no policy and no edit list: an
overexpression is the host's row and the clone's row both being selected, and their
conductances summing; a knockout is the host's rows not being selected. Both are
properties of the mask the study wrote, visible in the data, rather than rules
buried in a solver argument.

THE MASK IS THREE (COLUMN, VALUES) PAIRS
----------------------------------------
``background`` is what is always in -- the host, named by ``unit_id`` -- and is
stated rather than inferred, because "the rows that name no condition" stops being
a usable rule the moment the null pool is another GPR table whose rows do name
something. ``mask`` is what this condition adds on top. ``drop`` is applied last
and withholds rows the other two let through, which is how a deletion is stated.

Deciding WHICH rows a deletion withholds is the experiment designer's job -- this
module applies the mask it is handed and interprets no ``action`` column.

TWO WEIGHTINGS
--------------
  * ``belief``  -- :mod:`ecspr.model.evidence`'s allocation: each feature's total
    nomination is 1.0 spread across the reactions it nominates, and that mass is then
    pooled in log-odds over distinct ``(unit, channel, evidence)`` assertions. The
    evidence lane: a promiscuous annotation must not out-vote a specific one, and a
    paralog family must not out-vote three methods agreeing. ``E`` is a probability.
  * ``uniform`` -- 1.0 per unit that nominates the reaction at all. The curated-GEM
    lane: a curated model asserts a reaction is PRESENT, not how much evidence
    there is for it, so weighting it by anything would be inventing a quantity.

Under either, a reaction nominated by both the host unit and a clone unit is carried by
both -- ``uniform`` by summing units, ``belief`` by pooling two assertions rather than
one. That the unit is inside ``belief``'s assertion key is what keeps an overexpression
from being a no-op.
"""
from __future__ import annotations

import pandas as pd

from .evidence import nomination_contributions, pool_logodds

WEIGHTINGS = ("belief", "uniform")

# The long GPR schema's own names, mapped onto the ones `ecspr.model.evidence` speaks.
# `feature_id` is null on curated rows (a curated set names a construct, not an
# ORF), so the fallback chain is what keeps belief conservation per-construct there
# instead of collapsing every curated row onto one null "ORF".
_ORF_FALLBACK = ("feature_id", "feature_name", "evidence_id")
UNIT_COL = "unit_id"


def load_gpr(paths) -> pd.DataFrame:
    """Read and concatenate GPR tables. Several paths is the normal case: the host
    background is one table and a study's clones are another, and concatenating them
    is the whole of "this condition runs against this host"."""
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
    """The rows one condition selects. See the module docstring for the semantics.

    An unmasked call returns the whole table, which is the explicit ``--source /
    --sinks`` form: measure this GPR table as one unit.
    """
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
    """Rename the long GPR schema onto the column names `ecspr.model.evidence` reads."""
    out = df.copy()
    orf = None
    for c in _ORF_FALLBACK:
        if c not in out.columns:
            continue
        orf = out[c] if orf is None else orf.fillna(out[c])
    if orf is None:
        raise ValueError(f"GPR table carries none of {_ORF_FALLBACK}, so belief "
                         f"conservation has no construct to conserve over")
    out["orf"] = orf.astype(str)
    out["intermediate_id"] = (out["evidence_id"] if "evidence_id" in out.columns
                              else out["mnxr"]).astype(str)
    out["mnxr"] = out["mnxr"].astype(str)
    out["raw_score"] = pd.to_numeric(out["raw_score"], errors="coerce").fillna(1.0)
    return out.dropna(subset=["mnxr"])


def weights_from_rows(rows: pd.DataFrame, weighting: str = "belief") -> dict:
    """``{mnxr: E}`` for one condition's selected rows."""
    if weighting not in WEIGHTINGS:
        raise ValueError(f"weighting must be one of {WEIGHTINGS}, got {weighting!r}")
    if rows.empty:
        return {}
    if weighting == "uniform":
        n = (rows.dropna(subset=["mnxr"])
                 .groupby(rows["mnxr"].astype(str))[UNIT_COL].nunique())
        return {str(r): float(v) for r, v in n.items() if v > 0}
    E = pool_logodds(nomination_contributions(_normalise(rows)))
    return {str(r): float(e) for r, e in E.items()}


def condition_weights(df: pd.DataFrame, *, weighting="belief", **mask) -> tuple:
    """``({mnxr: E}, coverage)`` for one condition. ``coverage`` is what the mask
    actually reached -- row and unit counts, and how many of the selected rows the
    table itself marks as outside the atom-mapped universe. A mask that reaches no
    atom-mapped reaction is a genuine no-op and must return the baseline exactly,
    which is the property the control conditions measure."""
    rows = apply_mask(df, **mask)
    w = weights_from_rows(rows, weighting)
    in_universe = (int(rows["in_atom_universe"].fillna(False).astype(bool).sum())
                   if "in_atom_universe" in rows.columns else None)
    cov = dict(n_rows=int(len(rows)), n_units=int(rows[UNIT_COL].nunique()),
               n_reactions=len(w), n_rows_in_atom_universe=in_universe)
    return w, cov

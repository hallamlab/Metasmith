from __future__ import annotations

import pandas as pd

from .evidence import nomination_contributions, pool_logodds

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

# ONE PHYSICALLY SEPARATE COPY OF DNA INSIDE ONE CELL -- a chromosome, a plasmid, a
# fosmid insert, an ASKA clone. Declared by the producer (`fabfos_evidence`'s
# `attribution` block), never inferred here from a file name or from `source`, which on
# a pipeline-written table is the engine's instance id.
#
# It is the grain evidence pools over, and `_normalise` namespaces `orf` by it. Two
# units are one cell and their conductances ADD; two organisms are two `member`s and
# get private metabolites joined by bridges (`ecspr.model.compose`).
UNIT_COL = "genome_unit"

# The authoring handle. A conditions table names a gene here, so this is the column that
# has to mean the same thing on a host table and on a clone table. Null wherever the
# producer knows no symbol, and null is not selectable.
GENE_COL = "gene"


# The column `ecspr.model.conditions.resolve` stamps on every row it returns. Imported
# by name rather than by module to keep `conditions` importable from here without a
# cycle -- it reads this module's `UNIT_COL`.
_WEIGHT_COL = "_condition_weight"


def load_gpr(paths) -> pd.DataFrame:
    if isinstance(paths, (str, bytes)) or hasattr(paths, "__fspath__"):
        paths = [paths]
    frames = [pd.read_parquet(p) for p in paths]
    df = pd.concat(frames, ignore_index=True, sort=False)
    missing = {"mnxr", "channel", "llr", UNIT_COL} - set(df.columns)
    if missing:
        raise ValueError(f"GPR table is missing required columns: {sorted(missing)}")
    return df


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
    out["llr"] = pd.to_numeric(out["llr"], errors="coerce").fillna(0.0)
    # DOSAGE ENTERS HERE, AND ONLY HERE. A condition's per-row weight scales what the
    # row's evidence is worth, which is exactly what a second physical copy of the gene
    # does -- `pool_logodds` reads `llr` linearly inside its saturating term, so
    # `2 x llr` and two relabelled copies agree to float equality. Applying it further
    # down, to the pooled conductance, would instead scale a probability and stop being
    # evidence at all.
    if _WEIGHT_COL in out.columns:
        w = pd.to_numeric(out[_WEIGHT_COL], errors="coerce").fillna(1.0)
        out["llr"] = out["llr"] * w
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
    E = pool_logodds(nomination_contributions(_normalise(rows)))
    return {str(r): float(e) for r, e in E.items()}


def coverage(rows: pd.DataFrame, w: dict) -> dict:
    """What the condition actually selected, for the results table to carry.

    `n_units` counts DISTINCT COPIES, not rows: it is the denominator a null draw is
    matched on, so a condition that adds a second copy of one gene must read as one more
    unit than the background and not as one more row.
    """
    in_universe = (int(rows["in_atom_universe"].fillna(False).astype(bool).sum())
                   if "in_atom_universe" in rows.columns else None)
    return dict(n_rows=int(len(rows)),
                n_units=int(rows[UNIT_COL].nunique()) if UNIT_COL in rows.columns else 0,
                n_reactions=len(w), n_rows_in_atom_universe=in_universe)


def resolve_weights(condition, corpus: dict, *, weighting="belief") -> tuple:
    """A `Condition` plus `{table: DataFrame}` -> `({mnxr: E}, coverage)`.

    THE ONLY WAY A CONSTRUCTION BECOMES WEIGHTS. The fixed three-part mask this replaced
    -- background OR mask MINUS drop, over one concatenated frame -- expressed no order,
    no intersection and no third step, and every caller of it is now on the algebra.
    """
    from .conditions import resolve
    rows = resolve(condition, corpus)
    w = weights_from_rows(rows, weighting)
    return w, coverage(rows, w)

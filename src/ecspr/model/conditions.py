# A condition is an ordered set algebra over GPR rows.
#
# WHAT A CONDITION IS. The rows of the annotation tables that describe one construction
# -- a strain, a strain carrying a plasmid, a knockout, a fosmid library member -- and
# how much of each. It says nothing about what is then MEASURED on it; that is a test
# (`ecspr.model.tests`), and one condition can carry several.
#
# THE TABLE IS LONG, one row per step:
#
#   condition_id  step  table            type          set     operation  factor
#   glgA_gof      0     gem_gpr.parquet  genome_unit   dh1     union      1.0
#   glgA_gof      1     clones.parquet   gene          glgA    union      1.0
#   glgA_2x       2     clones.parquet   gene          glgA    union      1.0
#   glgA_lof      1     gem_gpr.parquet  gene          glgA    subtract   1.0
#
# and a condition is the LEFT FOLD of its steps in `step` order. Each step is a
# predicate `P(r) = (r is in `table`) AND (r[`type`] is in `set`)`:
#
#   union      add `{r : P(r)}` from the corpus to the accumulator
#   intersect  keep `{r in acc : P(r)}`
#   subtract   keep `{r in acc : NOT P(r)}`
#
# ORDER MATTERS and that is why `step` is a column rather than a file position: a
# subtract before the union that would have added the rows is a no-op, and the two read
# identically in a diff.
#
# DOSAGE IS A WEIGHT, NOT A COPY. Every accumulator row carries a weight, and a `union`
# matching a row already present adds `factor` to it rather than duplicating the row.
# `load` multiplies `llr` by that weight. This is EXACT, not an approximation: measured
# to float equality against physically relabelling a second copy (0.899165658540 both
# ways), because `pool_logodds` reads `llr` linearly inside the saturating term. It is
# also why there is no `copy` operation -- there would be nothing for it to do.
#
# A WEIGHT IS NOT A FLUX BOUND. It is evidence, in nats, about whether the cell can run
# the reaction at all. Doubling a gene doubles the belief and not the throughput.
#
# **CAUTION** `subtract` KEYED ON `mnxr` RENORMALISES THE SURVIVORS. Belief conservation
# is recomputed on whatever rows remain, so removing one of a gene's reactions pushes the
# rest of that gene's belief onto its siblings -- measured at +41.1% for a fan-out of
# three. That is correct for "this reaction does not happen" and wrong for "this gene is
# gone". Subtract on `gene` or `genome_unit` for a knockout; the whole nominator leaves
# and nothing is renormalised onto it.
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

SEP = "|"

OPERATIONS = ("union", "intersect", "subtract")

#: The long table's columns. `type` and `set` may both be empty, which selects every row
#: of `table` -- a background that does not want to enumerate its units.
COLUMNS = ("condition_id", "step", "table", "type", "set", "operation", "factor",
           "allow_empty")

#: What one union of a row is worth when the table does not say.
DEFAULT_FACTOR = 1.0

#: The weight column `resolve` adds. Named for what it is, and prefixed so it cannot
#: collide with a GPR column a `type` might name.
WEIGHT_COL = "_condition_weight"

#: Which corpus table an accumulated row came from. `intersect` and `subtract` test it,
#: so it has to survive into the accumulator.
TABLE_COL = "_condition_table"


def _split(v) -> tuple:
    if v is None or (isinstance(v, float) and v != v) or v == "":
        return ()
    if isinstance(v, (list, tuple, set, frozenset)):
        return tuple(str(x) for x in v)
    return tuple(x for x in str(v).split(SEP) if x)


def _scalar(v, default=None):
    if v is None or (isinstance(v, float) and v != v) or v == "":
        return default
    return v


@dataclass(frozen=True)
class Step:
    """One predicate, and what to do with the rows it selects."""
    table: str
    type: str
    set: tuple
    operation: str
    factor: float = DEFAULT_FACTOR
    allow_empty: bool = False

    def describe(self) -> str:
        what = f"{self.type}={SEP.join(self.set)}" if self.type else "every row"
        return f"{self.operation} {what} from {self.table}"


@dataclass(frozen=True)
class Condition:
    """One construction: an ordered list of steps over the annotation tables."""
    condition_id: str
    steps: tuple

    @property
    def tables(self) -> tuple:
        """The corpus tables this condition reads, so a caller can stage only those.

        The metagenome table is 19.7M rows. A condition that never names it should not
        pay to load it, which is the whole reason `table` is a column of its own rather
        than folded into `(type, set)`.
        """
        return tuple(dict.fromkeys(s.table for s in self.steps))


def _select(df: pd.DataFrame, step: Step, condition_id: str) -> np.ndarray:
    """The positions in `df` that satisfy this step's predicate."""
    if not step.type:
        return np.arange(len(df))
    if step.type not in df.columns:
        raise ValueError(
            f"condition {condition_id!r} selects on {step.type!r}, which is not a "
            f"column of {step.table!r} (it has {sorted(df.columns)})")
    hit = df[step.type].astype("object").isin(list(step.set)).to_numpy()
    return np.flatnonzero(hit)


def resolve(condition: Condition, corpus: dict) -> pd.DataFrame:
    """Fold the condition's steps over `corpus` and return the weighted rows.

    `corpus` is `{table name: DataFrame}`. The result is those rows with two columns
    added: `_condition_table` and `_condition_weight`.

    A step that selects NOTHING raises. Three eval scripts in this tree have already
    been lost to a filter that silently matched no rows and reported a null result, so
    an empty selection is a broken reference until the row says otherwise.
    """
    # {table: Series(weight, index=positions in corpus[table])}. Positions rather than a
    # row key because `mnxr` is null on a roster row, and every key-based grouping in
    # pandas drops null keys silently.
    acc: dict = {}
    for i, step in enumerate(sorted_steps(condition)):
        if step.table not in corpus:
            raise ValueError(
                f"condition {condition.condition_id!r} step {i} reads table "
                f"{step.table!r}, which was not staged (have {sorted(corpus)})")
        df = corpus[step.table]
        sel = _select(df, step, condition.condition_id)
        if len(sel) == 0 and not step.allow_empty:
            raise ValueError(
                f"condition {condition.condition_id!r} step {i} ({step.describe()}) "
                f"selects no rows. A construction built on a reference nothing matches "
                f"is not a null result -- fix the reference, or set allow_empty on this "
                f"row to say the emptiness is intended")
        if step.operation == "union":
            add = pd.Series(float(step.factor), index=sel, dtype=float)
            acc[step.table] = (add if step.table not in acc
                               else acc[step.table].add(add, fill_value=0.0))
        elif step.operation == "intersect":
            # THE PREDICATE NAMES ONE TABLE, so intersecting drops every accumulated row
            # from any other -- `P` is false for them by its first clause. That is the
            # definition, and it is what makes `intersect` mean "restrict to".
            keep = acc.get(step.table)
            acc = {} if keep is None else {
                step.table: keep[keep.index.isin(sel)]}
        elif step.operation == "subtract":
            keep = acc.get(step.table)
            if keep is not None:
                acc[step.table] = keep[~keep.index.isin(sel)]
        else:
            raise ValueError(f"condition {condition.condition_id!r} step {i} has "
                             f"operation {step.operation!r}; one of {OPERATIONS}")

    parts = []
    for table, w in acc.items():
        w = w[w > 0]
        if not len(w):
            continue
        part = corpus[table].iloc[w.index].copy()
        part[TABLE_COL] = table
        part[WEIGHT_COL] = w.to_numpy()
        parts.append(part)
    if not parts:
        return pd.DataFrame(columns=list(next(iter(corpus.values())).columns)
                            + [TABLE_COL, WEIGHT_COL])
    return pd.concat(parts, ignore_index=True, sort=False)


def sorted_steps(condition: Condition) -> list:
    return list(condition.steps)


def read(path) -> list:
    """The long table -> one `Condition` per `condition_id`, steps in `step` order."""
    p = Path(path)
    df = (pd.read_parquet(p) if p.suffix == ".parquet"
          else pd.read_csv(p, sep="\t" if p.suffix in (".tsv", ".txt") else ","))
    missing = {"condition_id", "table", "operation"} - set(df.columns)
    if missing:
        raise ValueError(f"conditions table {p} has no {sorted(missing)} column(s). "
                         f"The long schema is {list(COLUMNS)}")
    if "step" not in df.columns:
        # File order is a defensible reading only when nothing claims otherwise. It is
        # not the same guarantee: a table edited in a spreadsheet keeps its rows and
        # loses its order, which is exactly the failure `step` exists to prevent.
        df = df.assign(step=range(len(df)))

    out = []
    # ORDER WITHIN A CONDITION, NOT BETWEEN THEM. Sorting the whole frame by
    # `condition_id` first would reorder the conditions alphabetically, which is a
    # different table from the one the author wrote and shows up as a reordered
    # results file for no reason anyone can see.
    for cid, g in df.groupby("condition_id", sort=False):
        steps = []
        for row in g.sort_values("step", kind="mergesort").to_dict("records"):
            op = str(_scalar(row.get("operation"), "")).strip()
            if op not in OPERATIONS:
                raise ValueError(f"condition {cid!r} step {row.get('step')} has "
                                 f"operation {op!r}; one of {OPERATIONS}")
            steps.append(Step(
                table=str(_scalar(row.get("table"), "")),
                type=str(_scalar(row.get("type"), "")),
                set=_split(row.get("set")),
                operation=op,
                factor=float(_scalar(row.get("factor"), DEFAULT_FACTOR)),
                allow_empty=bool(_scalar(row.get("allow_empty"), False)),
            ))
        out.append(Condition(condition_id=str(cid), steps=tuple(steps)))
    return out


def write(conditions, path):
    rows = []
    for c in conditions:
        for i, s in enumerate(c.steps):
            rows.append(dict(condition_id=c.condition_id, step=i, table=s.table,
                             type=s.type, set=SEP.join(s.set), operation=s.operation,
                             factor=float(s.factor),
                             allow_empty=bool(s.allow_empty)))
    df = pd.DataFrame(rows, columns=list(COLUMNS))
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    if p.suffix == ".parquet":
        # Object columns with mixed None/str do not round-trip stably through parquet,
        # and a null pool that is not byte-identical between two runs of one seed is not
        # a shared pool. Cast once, here.
        for col in ("condition_id", "table", "type", "set", "operation"):
            df[col] = df[col].fillna("").astype(str)
        df.to_parquet(p, index=False)
    else:
        df.to_csv(p, sep="\t", index=False)
    return p


def single(condition_id, table, *, type="", set=()) -> Condition:
    """One condition that is one whole table, or one selection from it."""
    return Condition(condition_id=condition_id,
                     steps=(Step(table=table, type=type, set=tuple(set),
                                 operation="union"),))

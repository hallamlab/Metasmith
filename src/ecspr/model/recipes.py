# The conditions and tests a sweep needs, from a background and a perturbation spec.
#
# WHY A BUILDER. Three research scripts in this tree hand-write the same table shape, so a
# fix to one never reaches the others -- and every one of them re-decides what a control
# is, how `n_units` is counted, and whether the baseline appears in its own results. Those
# are the three things a null draw and a scorer read, and they have to agree.
#
# THE FAMILIES ARE THE ONES THE TREE ALREADY WRITES BY HAND:
#
#   baseline    the background alone
#   gof_each    one condition per distinct value of a column -- "one of each gain of
#               function in B", the ASKA and Eydallin shape
#   gof_all     every value at once, as the ceiling the individual arms are read against
#   lof_each    one knockout per distinct value -- "one of each loss of function in B",
#               the Keio shape
#   lof_named   knockouts of a named list
#
# EVERY FAMILY TAKES THE SAME BACKGROUND and returns tests that `read_against` the
# baseline, so the arm is comparable by construction rather than by the driver
# remembering to wire it.
#
# **CAUTION** A KNOCKOUT SUBTRACTS ON THE NOMINATOR, never on `mnxr`. Belief conservation
# is recomputed on the survivors, so subtracting one of a gene's reactions pushes its
# belief onto the siblings -- +41.1% for a fan-out of three. `lof_*` therefore refuse a
# `column` that names a reaction.
from __future__ import annotations

from dataclasses import replace

import pandas as pd

from . import conditions as cond_mod
from . import tests as test_mod

#: Columns a knockout may key on. Each names a NOMINATOR or a set of them, so subtracting
#: removes whole constructs and nothing is renormalised onto what is left.
LOF_COLUMNS = ("gene", "orf", "genome_unit", "condition_id", "feature_name")

#: What a perturbation reaching no reaction of the atom universe is: a real arm of the
#: experiment that cannot move the measurement, which is what makes it the noise floor.
#: `is_control` is set from this rather than declared, so a control cannot be mislabelled.
UNIVERSE_COL = "in_atom_universe"


def distinct(corpus: dict, table: str, column: str) -> tuple:
    """The values a `*_each` family iterates, in the table's own order.

    Sorted would be tidier and wrong: a sweep's arm order is what its shard directory and
    its results file are laid out by, and re-sorting a table's own order makes a rerun
    look like a different experiment.
    """
    df = corpus[table]
    if column not in df.columns:
        raise ValueError(f"{table} has no {column!r} column (has {sorted(df.columns)})")
    v = df[column].dropna().astype(str)
    return tuple(dict.fromkeys(v))


def _reaches_universe(corpus: dict, table: str, column: str, value: str) -> bool:
    df = corpus[table]
    if UNIVERSE_COL not in df.columns:
        return True
    hit = df[df[column].astype("object") == value]
    return bool(hit[UNIVERSE_COL].fillna(False).astype(bool).any())


def _n_units(corpus: dict, table: str, column: str, value: str) -> int:
    """DISTINCT COPIES, which is what a null is size-matched on.

    Not rows and not reactions: a perturbation that adds one gene with forty reactions is
    one unit, and matching a null to forty would compare it against forty-gene draws.
    """
    df = corpus[table]
    hit = df[df[column].astype("object") == value]
    unit = cond_mod.UNIT_COL if hasattr(cond_mod, "UNIT_COL") else "genome_unit"
    return int(hit[unit].nunique()) if unit in hit.columns else 1


def baseline(name, background) -> cond_mod.Condition:
    return cond_mod.Condition(condition_id=name, steps=tuple(background))


def gof_each(corpus, table, column, *, background, baseline_id, values=None,
             prefix="", factor=cond_mod.DEFAULT_FACTOR) -> list:
    """One condition per value: the background, plus that value's rows added on top."""
    out = []
    for v in (values if values is not None else distinct(corpus, table, column)):
        out.append(cond_mod.Condition(
            condition_id=f"{prefix}{v}",
            steps=tuple(background) + (cond_mod.Step(
                table=table, type=column, set=(v,), operation="union", factor=factor),)))
    return out


def gof_all(corpus, table, column, *, background, name, values=None,
            factor=cond_mod.DEFAULT_FACTOR) -> cond_mod.Condition:
    """Every value at once -- the ceiling the individual arms are read against."""
    vals = tuple(values if values is not None else distinct(corpus, table, column))
    return cond_mod.Condition(
        condition_id=name,
        steps=tuple(background) + (cond_mod.Step(
            table=table, type=column, set=vals, operation="union", factor=factor),))


def lof_each(corpus, table, column, *, background, values=None, prefix="") -> list:
    """One knockout per value: the background, minus that value's rows."""
    if column not in LOF_COLUMNS:
        raise ValueError(
            f"a knockout may not key on {column!r}. Subtracting on a reaction leaves the "
            f"gene in place and renormalises its belief onto the reactions that survive, "
            f"which is a claim about a gene the experiment did not make. Use one of "
            f"{LOF_COLUMNS}")
    out = []
    for v in (values if values is not None else distinct(corpus, table, column)):
        out.append(cond_mod.Condition(
            condition_id=f"{prefix}{v}",
            steps=tuple(background) + (cond_mod.Step(
                table=table, type=column, set=(v,), operation="subtract"),)))
    return out


def lof_named(corpus, table, column, values, *, background, name) -> cond_mod.Condition:
    """One knockout of a named list, as a single construction."""
    if column not in LOF_COLUMNS:
        raise ValueError(f"a knockout may not key on {column!r}; use one of {LOF_COLUMNS}")
    return cond_mod.Condition(
        condition_id=name,
        steps=tuple(background) + (cond_mod.Step(
            table=table, type=column, set=tuple(values), operation="subtract"),))


def tests_for(conditions, template: test_mod.Test, *, baseline_id, corpus=None,
              table=None, column=None, prefix="") -> list:
    """One test per condition, all on `template`'s measurement.

    `is_control` and `n_units` are DERIVED, not declared. A control is an arm that reaches
    no reaction of the atom universe -- a real perturbation that cannot move the
    measurement, which is exactly what makes it the noise floor -- and `n_units` is the
    number of distinct copies the arm adds, because that is what a null draw is
    size-matched on. Declaring either by hand is how a sweep comes to have controls that
    are not controls.
    """
    out = []
    for c in conditions:
        is_baseline = c.condition_id == baseline_id
        meta = dict(template.meta)
        if corpus is not None and table is not None and column is not None \
                and not is_baseline:
            last = c.steps[-1]
            vals = last.set
            meta["is_control"] = bool(vals) and not any(
                _reaches_universe(corpus, table, column, v) for v in vals)
            meta["n_units"] = sum(_n_units(corpus, table, column, v) for v in vals)
            meta["stratum"] = meta["n_units"]
        else:
            meta.setdefault("is_control", False)
        out.append(replace(
            template,
            test_id=f"{prefix}{c.condition_id}",
            network=c.condition_id,
            read_against="" if is_baseline else baseline_id,
            meta=meta))
    return out


def sweep(corpus, table, column, *, background, template, family="gof_each",
          baseline_id="baseline", values=None, prefix="",
          factor=cond_mod.DEFAULT_FACTOR) -> tuple:
    """`(conditions, tests)` for one whole arm, baseline included.

    THE BASELINE IS A CONDITION AND A TEST LIKE ANY OTHER. It is measured through the same
    code path as the arms it is subtracted from -- a baseline computed some other way
    would put the two on different footings, and every delta would carry the difference.
    """
    base = baseline(baseline_id, background)
    if family == "gof_each":
        arms = gof_each(corpus, table, column, background=background,
                        baseline_id=baseline_id, values=values, prefix=prefix,
                        factor=factor)
    elif family == "gof_all":
        arms = [gof_all(corpus, table, column, background=background,
                        name=f"{prefix}all", values=values, factor=factor)]
    elif family == "lof_each":
        arms = lof_each(corpus, table, column, background=background, values=values,
                        prefix=prefix)
    elif family == "lof_named":
        if values is None:
            raise ValueError("lof_named needs the values it knocks out")
        arms = [lof_named(corpus, table, column, values, background=background,
                          name=f"{prefix}lof")]
    else:
        raise ValueError(f"unknown family {family!r}")

    conds = [base] + arms
    ts = tests_for(conds, template, baseline_id=baseline_id, corpus=corpus,
                   table=table, column=column, prefix=prefix)
    return conds, ts


def insert_column(df: pd.DataFrame, name: str, *, frm: str, sep: str, take=0):
    """Materialise a grouping the ORF id encodes, so the algebra can select on it.

    A fosmid library has no insert-id column: the insert is the head of the ORF name, and
    every consumer that needed it re-derived it with its own split. MATERIALISING IT IS
    THE POINT -- a condition selects on columns, not on string surgery, and a grouping
    that lives in three regexes is a grouping that disagrees with itself.
    """
    out = df.copy()
    parts = out[frm].astype(str).str.rsplit(sep, n=1) if take == 0 \
        else out[frm].astype(str).str.split(sep, n=1)
    out[name] = parts.str[take]
    return out

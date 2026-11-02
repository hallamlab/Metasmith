# The null arm: size-matched random constructions, in the same two tables the observed
# arm uses.
#
# THE NULL IS THE SAME COMMAND WITH DIFFERENT FILES. `draw` emits a conditions table and
# a tests table, and `ecspr ground --conditions <null> --tests <null>` measures them --
# no null mode, no second code path, nothing that can drift from the observed arm.
#
# SIZE MATCHING IS THE WHOLE POINT. A perturbation that adds 40 genes moves more current
# than one that adds 2, whatever the genes are, so a null drawn without matching `n_units`
# ranks big perturbations high by construction. `draw` refuses a test that carries
# neither `n_units` nor a `--size`.
from __future__ import annotations

import numpy as np
import pandas as pd

from . import conditions as cond_mod
from . import tests as test_mod
from .gpr import load_gpr


def _pool_values(pool: pd.DataFrame, draw_column: str) -> np.ndarray:
    if draw_column not in pool.columns:
        raise ValueError(f"draw column {draw_column!r} is not in the pool table")
    v = pool[draw_column].dropna().astype(str).unique()
    return np.array(sorted(v), dtype=object)


def size_in(pool: pd.DataFrame, condition, column: str, table: str) -> int:
    """How many distinct `column` values one construction's perturbation reaches.

    THE SIZE A NULL MATCHES HAS TO BE COUNTED IN THE UNITS THE DRAW PICKS. `n_units`
    counts genome copies, and for a fosmid library that is 1 for every insert while the
    inserts carry 9 to 81 ORFs -- so a per-unit draw over that library can only ever
    return one of the 170 inserts, which are the observed constructions themselves. Such
    a pool is not a null: every observation sits at its centre by construction.
    """
    vals: set = set()
    for st in condition.steps:
        if st.table != table or not st.set:
            continue
        sel = (pool[pool[st.type].astype(str).isin([str(x) for x in st.set])]
               if st.type and st.type in pool.columns else pool)
        vals |= set(sel[column].dropna().astype(str))
    return len(vals)


def _settings_key(t: test_mod.Test) -> tuple:
    """What makes two tests the same MEASUREMENT, ignoring which network they run on.

    A null has to be drawn once per distinct measurement, not once per test: fifty
    perturbations read the same way need one null, and fifty nulls would be the same
    numbers fifty times over.

    THE ARM IS PART OF THE KEY. A conductance spec and a flux balance spec have disjoint
    settings columns, so without it every gem prototype collapses into one -- and it
    collapses SILENTLY, because a settings dict read under the wrong schema is empty
    rather than wrong.
    """
    return (t.arm,) + tuple(sorted((k, str(v)) for k, v in t.settings.items()))


def draw(pool_paths, like_conditions, like_tests, *, n: int, seed: int,
         background=(), draw_column="orf", pool_table="null_pool.parquet",
         size=None, size_from=None, operation="union", log=print) -> tuple:
    """`(conditions, tests)` -- the null pool, in the two published schemas.

    `background` is the steps every drawn condition starts from, so a null construction
    is the same host plus a random perturbation rather than a random perturbation alone.
    Pass the observed arm's background steps; without them the null measures a graph
    with no host in it and every draw abstains.

    `operation` IS THE OBSERVED ARM'S OWN PERTURBATION, and it is not always `union`. A
    deletion cohort's observed condition SUBTRACTS a gene from the background; a null that
    unions random genes onto the same background is the null for a different experiment,
    and on a deletion screen it is a null that cannot move at all -- the drawn genes are
    already in the host, so every draw reproduces the baseline and the pool has zero
    spread. Match the observed arm.
    """
    if operation not in cond_mod.OPERATIONS:
        raise ValueError(f"operation must be one of {cond_mod.OPERATIONS}, "
                         f"got {operation!r}")
    pool = load_gpr(pool_paths)
    values = _pool_values(pool, draw_column)
    if values.size == 0:
        raise ValueError(f"the pool has no values in {draw_column!r} to draw from")

    by_id = {c.condition_id: c for c in like_conditions}
    strata: dict = {}
    # WHICH SIZES EACH ARM ASKED FOR, kept apart so that a stratum only one arm declares
    # does not put the other arm's prototypes on constructions it never has to answer.
    # Where the two coincide -- which is the case this exists for -- they measure one
    # drawn condition.
    arm_strata: dict = {}
    for t in like_tests:
        if size_from:
            c = by_id.get(t.network)
            s = size_in(pool, c, size_from, pool_table) if c is not None else None
        else:
            s = t.meta.get("n_units")
            s = int(s) if s is not None and s == s else size
        if s is None:
            raise ValueError(
                f"test {t.test_id!r} carries no n_units and no --size was given; a "
                f"null that is not size-matched ranks big constructions high by "
                f"construction")
        if int(s) == 0:
            # A STRATUM OF SIZE ZERO IS THE BASELINE, and the pool already carries it once.
            # An observed tests table names its own baseline as a test -- it is measured
            # like any other -- so `--like-tests` hands one in, and drawing "nothing" per
            # stratum would emit n copies of the background under n different ids.
            continue
        strata.setdefault(int(s), []).append(t)
        arm_strata.setdefault(t.arm, set()).add(int(s))

    protos: dict = {}
    for t in like_tests:
        protos.setdefault(_settings_key(t), t)
    n_tests = sum(len(arm_strata.get(pr.arm, ())) for pr in protos.values()) * n
    log(f"[draw] pool {len(values):,} distinct {draw_column} | "
        f"{len(strata)} size stratum/strata {sorted(strata)} | "
        + " ".join(f"{a}:{len(arm_strata[a])} stratum/strata" for a in sorted(arm_strata))
        + f" | {len(protos)} measurement spec(s) -> {n_tests:,} tests")
    if not background and by_id:
        log("[draw] WARNING no background steps were given, so each null condition is "
            "the drawn units ALONE. Pass the observed arm's background or every draw "
            "abstains on a graph with no host in it.")

    # THE PROTOTYPE INDEX IS A POSITION IN THIS LIST, so it is stable only for a fixed set
    # of prototypes. Adding a second arm's tests to a draw renumbers the first arm's -- the
    # drawn CONDITIONS are unchanged (they do not depend on the prototypes at all) but
    # every test id moves, and a results table measured before the second arm was added no
    # longer joins to the tests table on disk. Re-emit both arms in one draw, then measure.
    ordered = sorted(protos.items(), key=lambda kv: str(kv[0]))
    conditions, tests = [], []

    # THE NULL POOL CARRIES ITS OWN BASELINE, the background with nothing drawn onto it.
    # Two things need it. A `read_against` copied from an observed prototype would name a
    # test that is not in this table, which `tests.read` refuses outright. And the flux
    # balance arm differences every measurement against its baseline, so one baseline for
    # the whole pool is one carve, one repair and one model for every draw -- the
    # difference between a half-hour array and a week.
    baseline_of: dict = {}
    if background:
        conditions.append(cond_mod.Condition(condition_id="null:baseline",
                                             steps=tuple(background)))
        for ti, (_key, proto) in enumerate(ordered):
            bid = f"null:t{ti}:baseline"
            baseline_of[ti] = bid
            tests.append(test_mod.Test(
                test_id=bid, network="null:baseline", arm=proto.arm,
                settings=dict(proto.settings),
                meta=dict(stratum=0, n_units=0, draw_id=None, is_control=True,
                          cohort=None)))

    for s in sorted(strata):
        if s > values.size:
            raise ValueError(f"stratum size {s} exceeds the pool's {values.size} units")
        rng = np.random.default_rng([seed, s])
        picks = [tuple(sorted(rng.choice(values, size=s, replace=False).tolist()))
                 for _ in range(n)]
        for j, pick in enumerate(picks):
            cid = f"null:s{s}:{j:06d}"
            conditions.append(cond_mod.Condition(
                condition_id=cid,
                steps=tuple(background) + (cond_mod.Step(
                    table=pool_table, type=draw_column, set=pick,
                    operation=operation),)))
            # ONE CONDITION, EVERY MEASUREMENT SPEC. The draw is what is random; reading
            # it two ways is two tests on one construction, which is exactly the split
            # the tests table exists for.
            for ti, (_key, proto) in enumerate(ordered):
                if s not in arm_strata.get(proto.arm, ()):
                    continue
                tests.append(test_mod.Test(
                    test_id=f"null:t{ti}:s{s}:{j:06d}", network=cid, arm=proto.arm,
                    read_against=baseline_of.get(ti, ""),
                    settings=dict(proto.settings),
                    # `draw_id` IS WHAT MARKS A NULL ROW, and it is the only thing that
                    # can: `arm` is the SCHEMA arm and `tests.write` sets it from the
                    # schema, so a `meta["arm"]` of "null" never reaches the file. A row
                    # with a draw_id came from a draw and a row without one did not.
                    meta=dict(stratum=s, n_units=s, draw_id=j,
                              is_control=False, cohort=None)))
    return conditions, tests

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


def _settings_key(t: test_mod.Test) -> tuple:
    """What makes two tests the same MEASUREMENT, ignoring which network they run on.

    A null has to be drawn once per distinct measurement, not once per test: fifty
    perturbations read the same way need one null, and fifty nulls would be the same
    numbers fifty times over.
    """
    return tuple(sorted((k, str(v)) for k, v in t.settings.items()))


def draw(pool_paths, like_conditions, like_tests, *, n: int, seed: int,
         background=(), draw_column="orf", pool_table="null_pool.parquet",
         size=None, log=print) -> tuple:
    """`(conditions, tests)` -- the null pool, in the two published schemas.

    `background` is the steps every drawn condition starts from, so a null construction
    is the same host plus a random perturbation rather than a random perturbation alone.
    Pass the observed arm's background steps; without them the null measures a graph
    with no host in it and every draw abstains.
    """
    pool = load_gpr(pool_paths)
    values = _pool_values(pool, draw_column)
    if values.size == 0:
        raise ValueError(f"the pool has no values in {draw_column!r} to draw from")

    by_id = {c.condition_id: c for c in like_conditions}
    strata: dict = {}
    for t in like_tests:
        s = t.meta.get("n_units")
        s = int(s) if s is not None and s == s else size
        if s is None:
            raise ValueError(
                f"test {t.test_id!r} carries no n_units and no --size was given; a "
                f"null that is not size-matched ranks big constructions high by "
                f"construction")
        strata.setdefault(int(s), []).append(t)

    protos: dict = {}
    for t in like_tests:
        protos.setdefault(_settings_key(t), t)
    log(f"[draw] pool {len(values):,} distinct {draw_column} | "
        f"{len(strata)} size stratum/strata {sorted(strata)} | "
        f"{len(protos)} measurement spec(s) -> "
        f"{n * len(strata) * len(protos):,} tests")
    if not background and by_id:
        log("[draw] WARNING no background steps were given, so each null condition is "
            "the drawn units ALONE. Pass the observed arm's background or every draw "
            "abstains on a graph with no host in it.")

    conditions, tests = [], []
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
                    operation="union"),)))
            # ONE CONDITION, EVERY MEASUREMENT SPEC. The draw is what is random; reading
            # it two ways is two tests on one construction, which is exactly the split
            # the tests table exists for.
            for ti, (_key, proto) in enumerate(
                    sorted(protos.items(), key=lambda kv: str(kv[0]))):
                tests.append(test_mod.Test(
                    test_id=f"null:t{ti}:s{s}:{j:06d}", network=cid, arm="ecspr",
                    settings=dict(proto.settings),
                    meta=dict(arm="null", stratum=s, n_units=s, draw_id=j,
                              is_control=False, cohort=None)))
    return conditions, tests

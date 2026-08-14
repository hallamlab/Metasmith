"""The null: draw units from a pool, and emit them AS a conditions table.

WHY A CONDITIONS TABLE AND NOT A SECOND CODE PATH
--------------------------------------------------
A null arm that goes through its own solver call is a second thing to keep true,
and the whole claim a z-score rests on is that the observed and null values came
out of the identical operation. Emitting the pool as a conditions table makes that
structural: the null arm is the same probe command with a different
``--conditions`` file. It also gives "seed the pool once and share it across every
arm" its literal meaning -- the arms share a file.

SIZE MATCHING IS THE POINT
--------------------------
A unit contributing thirty-two reactions receives more added conductance than one
contributing a single reaction, so an unmatched null ranks big units high by
construction. Draws are therefore made per SIZE STRATUM, taken from the observed
conditions' own ``n_units`` column, and the same drawn sets are reused across every
observed condition in that stratum -- one pool, not one per arm.

DRAWS THAT REACH NOTHING STAY IN THE POOL
-----------------------------------------
A drawn unit whose reactions are all outside the atom-mapped universe contributes a
genuine zero. Dropping it leaves a null made only of draws that COULD move, which
is the opposite of the comparison being made, and it is also what makes the control
spread measurable at all.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from . import conditions as cond_mod
from .gpr import load_gpr


def _pool_values(pool: pd.DataFrame, draw_column: str) -> np.ndarray:
    if draw_column not in pool.columns:
        raise ValueError(f"draw column {draw_column!r} is not in the pool table")
    v = pool[draw_column].dropna().astype(str).unique()
    return np.array(sorted(v), dtype=object)


def _terminal_key(c: cond_mod.Condition) -> tuple:
    return (c.element, c.source_hub, c.sinks, c.readouts, c.media,
            c.background_column, c.background_values)


def draw(pool_paths, like, *, n: int, seed: int, draw_column="feature_id",
         size=None, log=print) -> list:
    """``[Condition, ...]`` -- ``n`` draws per size stratum, per distinct terminal
    spec in ``like``. Deterministic in ``seed``: the same seed writes the same file,
    which is the only reason two arms can be said to share a pool."""
    pool = load_gpr(pool_paths)
    values = _pool_values(pool, draw_column)
    if values.size == 0:
        raise ValueError(f"the pool has no values in {draw_column!r} to draw from")

    strata = {}
    for c in like:
        s = c.meta.get("n_units")
        s = int(s) if s is not None and s == s else size
        if s is None:
            raise ValueError(
                f"condition {c.condition_id!r} carries no n_units and no --size was "
                f"given; a null that is not size-matched ranks big units high by "
                f"construction")
        strata.setdefault(int(s), []).append(c)

    terminals = {}
    for c in like:
        terminals.setdefault(_terminal_key(c), c)
    log(f"[draw] pool {len(values):,} distinct {draw_column} | "
        f"{len(strata)} size stratum/strata {sorted(strata)} | "
        f"{len(terminals)} terminal spec(s) -> {n * len(strata) * len(terminals):,} rows")

    out = []
    for s in sorted(strata):
        if s > values.size:
            raise ValueError(f"stratum size {s} exceeds the pool's {values.size} units")
        rng = np.random.default_rng([seed, s])
        picks = [tuple(sorted(rng.choice(values, size=s, replace=False).tolist()))
                 for _ in range(n)]
        for ti, (key, proto) in enumerate(sorted(terminals.items(), key=lambda kv: str(kv[0]))):
            for j, pick in enumerate(picks):
                out.append(cond_mod.Condition(
                    condition_id=f"null:t{ti}:s{s}:{j:06d}",
                    element=proto.element, source_hub=proto.source_hub,
                    sinks=proto.sinks, readouts=proto.readouts,
                    media=proto.media,
                    background_column=proto.background_column,
                    background_values=proto.background_values,
                    mask_column=draw_column, mask_values=pick,
                    meta=dict(arm="null", stratum=s, n_units=s, draw_id=j,
                              is_control=False, cohort=None),
                ))
    return out

from __future__ import annotations

from pathlib import Path

from ..model import conditions as cond_mod
from ..model import nulls
from ..model import tests as test_mod


def draw(args, log):
    like_tests = test_mod.read(args.like_tests, "ecspr")
    like_conds = (cond_mod.read(args.like_conditions)
                  if args.like_conditions else [])

    # THE BACKGROUND IS THE OBSERVED ARM'S OWN STEPS, taken from a named condition rather
    # than re-authored here. A null built on a different host is not a null for this
    # experiment, and re-declaring the host in the draw command is exactly how the two
    # come to disagree.
    background = ()
    if args.background:
        by_id = {c.condition_id: c for c in like_conds}
        if args.background not in by_id:
            raise SystemExit(
                f"--background {args.background!r} is not a condition_id in "
                f"{args.like_conditions} (have {sorted(by_id)[:5]})")
        background = by_id[args.background].steps

    pool_table = args.pool_table
    if pool_table is None:
        if len(args.gpr) != 1:
            raise SystemExit(
                f"{len(args.gpr)} pool tables were given, so the null conditions cannot "
                f"tell which one they draw from. Pass --pool-table with the name they "
                f"should use")
        pool_table = Path(args.gpr[0]).name

    conds, ts = nulls.draw(args.gpr, like_conds, like_tests, n=args.n, seed=args.seed,
                           background=background, draw_column=args.draw_column,
                           pool_table=pool_table, size=args.size, log=log)
    out_c = cond_mod.write(conds, args.out)
    out_t = test_mod.write(ts, args.out_tests, "ecspr")
    log(f"[draw] seed={args.seed} n={args.n} -> {len(conds):,} conditions, "
        f"{len(ts):,} tests -> {out_c}, {out_t}")
    log("[draw] the null arm is now the same probe command with "
        f"--conditions {out_c} --tests {out_t}")
    return 0

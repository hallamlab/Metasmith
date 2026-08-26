from __future__ import annotations

from pathlib import Path

from ..model import conditions as cond_mod
from ..model import nulls
from ..model import tests as test_mod


def draw(args, log):
    # ONE DRAW, EITHER ARM, AND USUALLY BOTH. The drawn conditions are arm-neutral -- a
    # random construction is a random construction -- so a pool given both arms'
    # prototypes emits two tests tables over ONE set of constructions. That shared pool is
    # what makes the two arms' error bars comparable rather than merely adjacent.
    like_tests = []
    for path, arm in ((args.like_tests, "ecspr"), (args.like_gem_tests, "gem")):
        if path:
            like_tests += test_mod.read(path, arm)
    if not like_tests:
        raise SystemExit("give --like-tests, --like-gem-tests, or both: the draw takes "
                         "its measurement specs and its size strata from an observed "
                         "tests table")
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
                           pool_table=pool_table, size=args.size,
                           operation=args.draw_operation, log=log)
    out_c = cond_mod.write(conds, args.out)
    log(f"[draw] seed={args.seed} n={args.n} -> {len(conds):,} conditions -> {out_c}")
    for arm, path, verb in (("ecspr", args.out_tests, "ecspr ground/two-point"),
                            ("gem", args.out_gem_tests, "ecspr fba")):
        rows = [t for t in ts if t.arm == arm]
        if not rows:
            continue
        if not path:
            raise SystemExit(f"{len(rows)} {arm} test(s) were drawn but no "
                             f"--out-{'tests' if arm == 'ecspr' else 'gem-tests'} "
                             f"was given")
        out_t = test_mod.write(rows, path, arm)
        log(f"[draw] {len(rows):,} {arm} tests -> {out_t}")
        log(f"[draw] the {arm} null arm is now `{verb} --conditions {out_c} "
            f"--tests {out_t}`")
    return 0

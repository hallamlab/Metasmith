"""``ecspr draw`` -- the null pool, written once, as a conditions table."""
from __future__ import annotations

from ..model import conditions as cond_mod
from ..model import nulls


def draw(args, log):
    like = cond_mod.read(args.like, element=args.element, source=args.source,
                         sinks=args.sinks, readouts=args.readout_hub)
    pool = nulls.draw(args.gpr, like, n=args.n, seed=args.seed,
                      draw_column=args.draw_column, size=args.size, log=log)
    out = cond_mod.write(pool, args.out)
    log(f"[draw] seed={args.seed} n={args.n} -> {len(pool):,} conditions -> {out}")
    log("[draw] the null arm is now the same probe command with "
        f"--conditions {out}")
    return 0

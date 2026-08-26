from __future__ import annotations

from pathlib import Path

from ..model import conditions as cond_mod
from ..model import probes
from ..model import tests as test_mod


def _plan(args, probe):
    """`(conditions, tests)` from the two tables, or from the one-shot flags.

    THE TWO FORMS ARE EXCLUSIVE and the refusal says why: a tests table carries its own
    terminals, so `--source` beside it would be two answers to one question with no rule
    for which wins.
    """
    if args.tests and args.source:
        raise SystemExit("--tests and --source are two ways to say the same thing; "
                         "pass one. A tests table carries its own terminals.")
    if args.conditions and not args.tests:
        raise SystemExit("--conditions says what the networks ARE and nothing about "
                         "what to measure on them. Pass --tests as well.")
    if args.tests:
        if not args.conditions:
            raise SystemExit("--tests names networks; pass --conditions so they resolve")
        conds = cond_mod.read(args.conditions)
        ts = test_mod.read(args.tests, "ecspr", defaults=dict(
            element=args.element, source_hub=args.source,
            sink_hub=probes.SEP.join(args.sinks) if args.sinks else None,
            readout_hub=(probes.SEP.join(args.readout_hub)
                         if args.readout_hub else None)))
        for t in ts:
            if not t.source_hub:
                raise SystemExit(
                    f"test {t.test_id!r} names no source_hub and no --source default "
                    f"was given")
        return conds, ts

    if not args.source:
        raise SystemExit("pass --conditions with --tests, or --source with the sinks")
    if probe == "two-point" and not args.sinks:
        raise SystemExit("two-point needs --sinks: there is nothing to merge into a "
                         "ground, and no terminal to measure against")
    if probe == "ground" and not (args.sinks or args.readout_hub
                                  or args.readouts == "all"):
        raise SystemExit("ground needs somewhere to read: --precursors, "
                         "--readout-hub, or --readouts all")
    # THE ONE-SHOT FORM IS ONE CONDITION PER STAGED TABLE, unioned. That is what "measure
    # the whole GPR table" meant before the split, stated in the algebra rather than as a
    # special case inside the probe.
    steps = tuple(cond_mod.Step(table=Path(p).name, type="", set=(), operation="union")
                  for p in args.gpr)
    conds = [cond_mod.Condition(condition_id="whole_table", steps=steps)]
    ts = [test_mod.single("whole_table", "whole_table", element=args.element,
                          source_hub=args.source,
                          sink_hub=probes.SEP.join(args.sinks),
                          readout_hub=probes.SEP.join(args.readout_hub))]
    return conds, ts


def _run(args, log, probe, **kw):
    conds, ts = _plan(args, probe)
    shard = (None if args.no_resume
             else Path(args.shard_dir or f"{args.out}.shards"))
    basis = probes.Basis(args.atom_pairs, args.direction, element=args.element,
                         orientation=args.orientation.replace("-", "_"))
    log(f"[{probe}] {len(ts)} test(s) on {len(conds)} network(s) | "
        f"element={args.element} orientation={basis.orientation} "
        f"weighting={args.weighting} "
        f"| pairs={len(basis.pairs):,} rows, ratios={len(basis.ratios):,}")
    df = probes.run(basis, args.gpr, conds, ts, probe=probe,
                    weighting=args.weighting, shard_dir=shard, log=log, **kw)
    out = probes.write_results(df, args.out)
    log(f"[{probe}] {len(df):,} rows -> {out}")
    return 0


def two_point(args, log):
    return _run(args, log, "two-point")


def ground(args, log):
    return _run(args, log, "ground", leak=args.leak, port=args.port,
                readouts=args.readouts)

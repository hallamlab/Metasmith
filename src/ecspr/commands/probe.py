from __future__ import annotations

from pathlib import Path

from ..model import conditions as cond_mod
from ..model import probes


def _conditions(args, probe):
    if args.conditions and args.source:
        raise SystemExit("--conditions and --source are two ways to say the same "
                         "thing; pass one. A conditions table carries its own "
                         "terminals, and the explicit form takes no mask.")
    if args.conditions:
        return cond_mod.read(args.conditions, element=args.element,
                             source=args.source, sinks=args.sinks,
                             readouts=args.readout_hub, media=args.media)
    if not args.source:
        raise SystemExit("pass --conditions <table>, or --source with the sinks")
    if probe == "two-point" and not args.sinks:
        raise SystemExit("two-point needs --sinks: there is nothing to merge into a "
                         "ground, and no terminal to measure against")
    if probe == "ground" and not (args.sinks or args.readout_hub
                                  or args.readouts == "all"):
        raise SystemExit("ground needs somewhere to read: --precursors, "
                         "--readout-hub, or --readouts all")
    return [cond_mod.single("whole_table", element=args.element, source=args.source,
                            sinks=args.sinks, readouts=args.readout_hub,
                            media=args.media)]


def _run(args, log, probe, **kw):
    conds = _conditions(args, probe)
    shard = (None if args.no_resume
             else Path(args.shard_dir or f"{args.out}.shards"))
    basis = probes.Basis(args.atom_pairs, args.direction, element=args.element,
                         orientation=args.orientation.replace("-", "_"))
    log(f"[{probe}] {len(conds)} condition(s) | element={args.element} "
        f"orientation={basis.orientation} weighting={args.weighting} "
        f"| pairs={len(basis.pairs):,} rows, ratios={len(basis.ratios):,}")
    df = probes.run(basis, args.gpr, conds, probe=probe, weighting=args.weighting,
                    shard_dir=shard, log=log, **kw)
    out = probes.write_results(df, args.out)
    log(f"[{probe}] {len(df):,} rows -> {out}")
    return 0


def two_point(args, log):
    return _run(args, log, "two-point")


def ground(args, log):
    return _run(args, log, "ground", leak=args.leak, port=args.port,
                readouts=args.readouts)

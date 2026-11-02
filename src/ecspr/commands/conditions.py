from __future__ import annotations

from pathlib import Path

from ..model import conditions as cond_mod
from ..model import probes, recipes
from ..model import tests as test_mod


def conditions(args, log):
    """One sweep's two tables, from a background and a perturbation column.

    THE BACKGROUND IS A SELECTION, not a file. `--background TABLE:COLUMN:VALUES` reads
    the same way a step does, so what the builder writes and what a person would write by
    hand are the same thing -- and the background can be checked against the corpus before
    a single measurement runs.
    """
    corpus = probes.load_corpus(args.gpr)

    table, column, values = _parse_selection(args.background, "--background")
    if table not in corpus:
        raise SystemExit(f"--background reads {table!r}, which is not among the staged "
                         f"tables {sorted(corpus)}")
    background = (cond_mod.Step(table=table, type=column, set=values,
                                operation="union"),)

    template = test_mod.Test(
        test_id="", network="", arm="ecspr",
        settings=dict(element=args.element, source_hub=args.source,
                      sink_hub=test_mod.SEP.join(args.sinks),
                      readout_hub=test_mod.SEP.join(args.readout_hub),
                      probe=args.probe))

    conds, ts = recipes.sweep(
        corpus, args.table, args.column, background=background, template=template,
        family=args.family, baseline_id=args.baseline_id,
        values=tuple(args.values) if args.values else None,
        prefix=args.prefix, factor=args.factor)

    out_c = cond_mod.write(conds, args.out)
    out_t = test_mod.write(ts, args.out_tests, "ecspr")
    n_ctl = sum(1 for t in ts if t.is_control)
    log(f"[conditions] {args.family} over {args.table}:{args.column} -> "
        f"{len(conds):,} conditions, {len(ts):,} tests ({n_ctl} control(s) derived "
        f"from {recipes.UNIVERSE_COL})")
    log(f"[conditions] {out_c}\n[conditions] {out_t}")
    return 0


def _parse_selection(spec: str, flag: str):
    parts = str(spec).split(":")
    if len(parts) != 3:
        raise SystemExit(f"{flag} expects TABLE:COLUMN:VALUES (values separated by "
                         f"'{cond_mod.SEP}'); got [{spec}]")
    table, column, values = parts
    return table, column, tuple(v for v in values.split(cond_mod.SEP) if v)

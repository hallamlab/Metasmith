from __future__ import annotations

from ..model import conditions as cond_mod
from ..model import tests as test_mod


def fba(args, log):
    # Imported here rather than at module scope: `ecspr.model` must stay importable in an
    # environment with no cobra, which is every environment the conductance arm runs in.
    from ..model import fba as fba_mod
    from ..model import mnx

    conds = cond_mod.read(args.conditions)
    ts = test_mod.read(args.tests, "gem", defaults=dict(
        frame=args.frame_default, media=args.media_default,
        objective=args.objective, target_hub=args.target,
        growth_pin=args.growth_pin, min_lanes=args.min_lanes))
    media = fba_mod.load_media(args.media) if args.media else {}

    release = None
    if args.mnx:
        release = mnx.Release(args.mnx, cache=args.mnx_cache)
    else:
        log("[fba] no --mnx: an MNXR the frame never named cannot be inserted, so a "
            "de-novo condition is measured on the carve alone")

    log(f"[fba] {len(ts)} test(s) on {len(conds)} network(s) | "
        f"frames={len(args.frame)} media={sorted(media)} "
        f"weighting={args.weighting} gapfill={not args.no_gapfill} "
        f"delta={not args.no_delta}")

    from pathlib import Path
    shard = (None if args.no_resume
             else Path(args.shard_dir or f"{args.out}.shards"))
    if (args.part is None) != (args.of is None):
        log("[fba] --part and --of go together; ignoring the one given alone")
        args.part = args.of = None

    df = fba_mod.run(args.frame, args.gpr, conds, ts, media, release=release,
                     weighting=args.weighting, emit=args.emit,
                     do_gapfill=not args.no_gapfill, gapfill_media=args.gapfill_media,
                     delta=not args.no_delta, shard_dir=shard,
                     resume=not args.no_resume, part=args.part, of=args.of,
                     census_out=args.census or f"{args.out}.census.json", log=log)
    out = fba_mod.write_results(df, args.out)
    log(f"[fba] {len(df):,} rows -> {out}")
    return 0



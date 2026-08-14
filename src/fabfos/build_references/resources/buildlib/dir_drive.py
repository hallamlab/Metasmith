"""The direction lane's two driver steps: build the universe, evaluate one member.

WHY THIS IS A MODULE AND NOT TWO STRINGS INSIDE THE TRANSFORM
--------------------------------------------------------------
Both of these lived as `r'''...'''` constants inside the direction transform, written to
disk with a heredoc and run. That works for the transform and for nothing else -- nothing
else could reach them, so anyone re-running the lane by hand while the method was being
shaped was running different code. Same rule the five lookups follow: one piece of code,
one CLI, several callers.

THE UNIVERSE IS EVERY MNXR IN reac_prop, deliberately wider than the deployed run (which
scored only the base graph). The bake REFUSES if a reaction has atom pairs but no
direction row, and scoring a subset is exactly how that gap appears. A reaction the
ensemble is silent on lands at ratio 1.0 -- a real physical statement, not an absence.

A MEMBER THAT CANNOT IMPORT IS A MISSING VOTE, NOT A FAILED BUILD. dGbyG needs torch 2.8
+ torch-geometric, which cannot coexist with the AAM stack's torch 2.2.1 or with
equilibrator-cache's numpy>=2, so the direction image deliberately does not carry it.
`eval` writes an EMPTY member table in that case, so the combiner's argument stays a real
file and the absence stays a recorded fact rather than a missing path. `dir_combine`
already treats an absent member as silence; this is what makes that true in practice.

`eval` SHARDS, and `merge` PROVES THE SHARDS COVERED THE UNIVERSE. dGbyG is a per-reaction
forward pass with no shared state, measured at 0.249 s alone and 0.286 s under sixteen-way
contention on a 2.15 GB-per-process footprint that does not grow with concurrency -- so it
parallelises almost linearly and there is no reason to pay for it serially. The partition
is `aam_shard`'s, the same crc32 the mapper lanes use, so a shard means the same thing
everywhere. Unlike those lanes this one keeps no sidecar: Indigo needs one because it can
hang inside a compiled search having written nothing, whereas this is a bounded forward
pass. What replaces it is stronger -- `merge` is given the universe and refuses unless the
concatenated shards are exactly it, so a short member is a named failure rather than a
coverage hole nobody looks for.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import pandas as pd

from aam_shard import parse_spec, shard_of
from dir_refdata import load_mnxr_stoich, load_mnxm_props

MEMBER_COLS = ("mnxr", "dg", "sigma", "flag", "reason")


def cmd_universe(args):
    mnxrs = []
    with open(args.reac_prop) as fh:
        for line in fh:
            if line.startswith("#"):
                continue
            p = line.rstrip("\n").split("\t")
            if p and p[0].startswith("MNXR"):
                mnxrs.append(p[0])
    uniq = sorted(set(mnxrs))
    Path(args.out).write_text(json.dumps(uniq))
    print(f"[direction] universe: {len(uniq):,} reactions -> {args.out}", flush=True)
    return 0


def cmd_eval(args):
    mnxrs = json.load(open(args.universe))
    shard = parse_spec(args.shard)
    if shard:
        i, n = shard
        mnxrs = [m for m in mnxrs if shard_of(m, n) == i]
        print(f"[eval:{args.member}] shard {i}/{n}: {len(mnxrs):,} reactions", flush=True)
    # SyntaxError, not only ImportError. dGbyG installs cleanly under python 3.11 and
    # then fails to PARSE (a same-quote nested f-string, legal from 3.12), so the
    # unavailable-member path was never reached and the step died instead of degrading.
    # "The member is not usable in this environment" is one fact however it presents.
    try:
        if args.member == "eq":
            from dir_thermo_eq import EquilibratorMember as M
        else:
            from dir_thermo_dgbyg import DgbygMember as M
    except (ImportError, SyntaxError) as e:
        # --require is for a DEDICATED LANE, whose only product is this table. There, an
        # unusable member is a failed step; writing an empty table would publish silence
        # as if it were an answer. Without the flag the tolerant behaviour stands, which
        # is what a combined step wants: the combiner is defined over whoever spoke.
        if args.require:
            print(f"[eval:{args.member}] UNAVAILABLE in this environment: "
                  f"{type(e).__name__}: {e}", file=sys.stderr, flush=True)
            print(f"   --require was given, so this is a failure rather than an empty "
                  f"table. This lane's only product IS the member, and an empty one "
                  f"would read downstream as 'the member abstained on everything'.",
                  file=sys.stderr, flush=True)
            return 1
        pd.DataFrame(columns=list(MEMBER_COLS)).to_parquet(args.out, index=False)
        print(f"[eval:{args.member}] UNAVAILABLE in this image ({e}); wrote an empty "
              f"member table -- the ensemble loses a vote, not the build", flush=True)
        return 0

    stoich = load_mnxr_stoich(args.reac_prop)
    props = load_mnxm_props(args.chem_prop)
    member = M()

    rows = []
    for i, mnxr in enumerate(mnxrs, 1):
        s = stoich.get(mnxr)
        if s is None:
            rows.append(dict(mnxr=mnxr, dg=None, sigma=None, flag=None,
                             reason="no_stoich"))
            continue
        st, _is_bal, _is_tr = s
        dg, sig, flag, reason = member.dgr(st, props)
        rows.append(dict(mnxr=mnxr, dg=dg, sigma=sig, flag=flag, reason=reason))
        if i % 2000 == 0:
            print(f"[eval:{args.member}] {i:,}/{len(mnxrs):,}", flush=True)

    df = pd.DataFrame(rows, columns=list(MEMBER_COLS))
    df.to_parquet(args.out, index=False)
    ok = int(df["dg"].notna().sum())
    print(f"[eval:{args.member}] answered {ok:,}/{len(df):,} ({ok/max(1,len(df)):.1%})"
          f" -> {args.out}", flush=True)
    print(df["reason"].value_counts().to_string(), flush=True)
    return 0


def cmd_merge(args):
    """Concatenate shard tables and REFUSE unless they are exactly the universe.

    This is the shards' completeness proof, and it is why this lane needs no sidecar. The
    two ways a fan-out goes wrong are a shard that died and a shard count that changed
    under a resume, and both present the same way here: the union of what came back is
    not the set that was asked about. Naming the difference is the whole job.
    """
    files = sorted(args.shard_file)
    if len(files) != args.expect:
        print(f"[merge:{args.member}] expected {args.expect} shard tables, found "
              f"{len(files)}: {files}", file=sys.stderr, flush=True)
        return 1

    want = set(json.load(open(args.universe)))
    frames = []
    for f in files:
        d = pd.read_parquet(f)
        if d.empty:
            print(f"[merge:{args.member}] {f} is empty. A shard covers ~1/{args.expect} "
                  f"of the universe, so an empty one is a shard that did not run, not a "
                  f"shard with nothing to say.", file=sys.stderr, flush=True)
            return 1
        frames.append(d)

    df = pd.concat(frames, ignore_index=True)
    dup = df["mnxr"].duplicated()
    if dup.any():
        print(f"[merge:{args.member}] {int(dup.sum()):,} reactions appear in more than "
              f"one shard -- the partition is not disjoint, which means the shard count "
              f"used to write these tables is not the one used to merge them.",
              file=sys.stderr, flush=True)
        return 1

    got = set(df["mnxr"])
    if got != want:
        missing, extra = want - got, got - want
        print(f"[merge:{args.member}] the shards do not cover the universe: "
              f"{len(missing):,} missing, {len(extra):,} not asked for. First missing: "
              f"{sorted(missing)[:5]}", file=sys.stderr, flush=True)
        return 1

    df = df.sort_values("mnxr").reset_index(drop=True)
    df = df[list(MEMBER_COLS)]
    df.to_parquet(args.out, index=False)
    ok = int(df["dg"].notna().sum())
    print(f"[merge:{args.member}] {len(files)} shards -> {len(df):,} rows, answered "
          f"{ok:,} ({ok/max(1,len(df)):.1%}) -> {args.out}", flush=True)
    print(df["reason"].value_counts().to_string(), flush=True)
    return 0


def parse_args():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("universe"); p.set_defaults(fn=cmd_universe)
    p.add_argument("--reac-prop", required=True)
    p.add_argument("--out", required=True)

    p = sub.add_parser("eval"); p.set_defaults(fn=cmd_eval)
    p.add_argument("--member", required=True, choices=["eq", "dgbyg"])
    p.add_argument("--universe", required=True)
    p.add_argument("--reac-prop", required=True)
    p.add_argument("--chem-prop", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--require", action="store_true",
                   help="fail if the member cannot be imported, instead of writing an "
                        "empty table. For a dedicated member lane, where an empty "
                        "product is indistinguishable from total abstention.")
    p.add_argument("--shard", default=None, metavar="i/n",
                   help="evaluate only the reactions this shard owns, under the same "
                        "crc32 partition the mapper lanes use. Merge with `merge`, which "
                        "checks the shards add back up to the universe.")

    p = sub.add_parser("merge"); p.set_defaults(fn=cmd_merge)
    p.add_argument("--member", required=True, choices=["eq", "dgbyg"])
    p.add_argument("--shard-file", required=True, nargs="+")
    p.add_argument("--expect", required=True, type=int,
                   help="the shard count the tables were written under. A mismatch here "
                        "is the difference between a merge and a guess.")
    p.add_argument("--universe", required=True,
                   help="the list the shards were drawn from; the merge refuses unless "
                        "they reconstitute it exactly.")
    p.add_argument("--out", required=True)
    return ap.parse_args()


if __name__ == "__main__":
    a = parse_args()
    sys.exit(a.fn(a))

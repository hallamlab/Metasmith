"""The Indigo member -- a deterministic structural mapper, and the ensemble's third vote.

WHY A THIRD MEMBER AT ALL, GIVEN TWO NEURAL ONES ALREADY EXIST
--------------------------------------------------------------
RXNMapper and LocalMapper are both transformers over reaction SMILES. When they agree,
part of the agreement is shared architectural bias rather than independent
confirmation, so a two-member consensus is one-and-a-bit votes, not two. Indigo is not
a model: it is a maximum-common-substructure search in compiled C++, so it fails and
succeeds for entirely different reasons. It is also the ARBITER of the curation sweep,
where the question is not "which atom went where" but "can this completed reaction be
mapped at all" -- a question a deterministic mapper answers and a learned one only
guesses at.

THE MACHINERY THE OTHER TWO DO NOT NEED, AND WHY
------------------------------------------------
`automap` can hang inside the compiled search on a large symmetric molecule. It hangs
below the Python layer, so `signal.alarm` does not interrupt it and its own
`aam-timeout` option does not bound it. A plain loop therefore does not slow down --
it STOPS, having written nothing, with no record of which reaction it stopped on.

The answer is a SIDECAR, and the ordering is the entire trick: the reaction id is
written and flushed BEFORE the mapping is attempted, never after. A run that dies
mid-reaction leaves that id as the last line of the sidecar, so the next run knows
exactly which reaction to skip and loses nothing but the one it was on. Combined with
a watchdog that restarts past the last sidecar line, an unbounded hang becomes a
bounded cost.

A TIMEOUT IS A RECORDED OUTCOME, NOT A CRASH. Reactions that exceed the soft budget are
written with an empty map and `status=timeout`, which keeps "Indigo could not" distinct
from "Indigo never got there" -- the sidecar answers the second.

`retry` RE-ATTEMPTS THOSE AT A LONGER BUDGET AND NO LANE CALLS IT ANY MORE. It is kept
because it is the only way to measure whether the longer budget is worth anything, and
that has not been done here: the inherited claim is that a previous generation's second
pass recovered a measurable number, but no such number survives in this project's
outputs, and the direct evidence points the other way -- a rescue-lane retry ran twenty-
two minutes over ~30 reactions a shard with cpu time equal to wall time, meaning every
attempt was timing out again. Wiring it back in should follow that measurement rather
than precede it. See `bake/indigo.py` for what a timed-out reaction costs instead
(nothing: it falls to the LocalMapper gap).

SHARDING IS THE RUNTIME'S JOB. `--shard i/n` splits the work deterministically by a
stable hash of the reaction id. The mechanism -- the crc32 partition, the sidecar and
its shard-spec header -- now lives in `aam_shard`, because RXNMapper needs exactly the
same discipline for a different reason (it is OOM-killed rather than hung, and a resume
cannot tell the difference), and a fix that exists twice is a fix that gets corrected
once.
"""
from __future__ import annotations

import argparse
import signal
import sys
import time
from pathlib import Path

import pandas as pd

from . import shard as aam_shard
from . import worklist
from .shard import shard_of

COLUMNS = ("mnxr", "rxn_smiles", "mapped_rxn_smiles", "confidence", "status")

# Indigo does not score a mapping. A structural map is asserted the same way a curated
# one is, so it carries the same constant -- and the combiner's disagreement branch
# divides by a sum of confidences, which a NaN would poison.
INDIGO_CONFIDENCE = 1.0

# Soft budget per reaction, enforced from PYTHON around the call. It does not stop a
# hang inside the compiled search -- nothing in-process does -- which is exactly why the
# sidecar exists. It does bound the merely-slow ones.
DEFAULT_TIMEOUT_S = 20


class _Timeout(Exception):
    pass


def _alarm(_sig, _frm):
    raise _Timeout()


def map_one(ind, smi: str, timeout_s: int):
    """(mapped_smiles, status). `status` is one of ok / empty / timeout / error."""
    old = signal.signal(signal.SIGALRM, _alarm)
    signal.alarm(timeout_s)
    try:
        rxn = ind.loadReaction(smi)
        # "discard" throws away whatever map the input carried and computes a fresh one,
        # which is what we want: the universe SMILES are unmapped, and any map that did
        # survive would be the previous member's opinion rather than Indigo's.
        rxn.automap("discard")
        out = rxn.smiles()
        return (out, "ok") if out else ("", "empty")
    except _Timeout:
        return "", "timeout"
    except Exception:
        return "", "error"
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old)


def load_universe(worklist_parquet: Path, exclude=None, shard=None):
    """The reactions `aam_worklist` admits to THIS member, for this shard.

    THE `verdict` COLUMN IS REQUIRED. Every member reads the SAME adjudicated list, so
    that "the three saw the same reactions" is a property of the graph rather than three
    filters that happen to agree; pointing this at `lookup::reactions` now fails loudly
    instead of quietly restoring the per-member universe.

    AND THIS MEMBER SEES MORE OF IT, which is the one place the three legitimately
    differ. The atom cap bounds the neural members' cost -- a 512-token transformer and
    a lane that was OOM-killed twice. Indigo is a compiled substructure search with a
    recorded timeout, so it takes the oversized tail as well and `worklist.ATOM_LIMIT`
    says why. A reaction only Indigo reaches lands as `indigo_only` at half weight
    through the ordinary fusion; nothing here needs a special case for it.
    """
    d = pd.read_parquet(worklist_parquet)
    if "verdict" not in d.columns:
        raise SystemExit(
            f"[indigo] {worklist_parquet} has no `verdict` column, so it is not a "
            f"worklist. Members read `interm::aam_worklist` (or the rescued universe, "
            f"which carries the same column), never `lookup::reactions` directly.")
    n_all = len(d)
    d = d[d["verdict"].isin(worklist.INDIGO_ADMITS) & d["rxn_smiles"].notna()]
    n_over = int((d["verdict"] == "oversize").sum())
    out = {r.mnxr: r.rxn_smiles for r in d.itertuples(index=False)}
    n_map = len(out)
    if exclude:
        out = {m: s for m, s in out.items() if m not in exclude}
    out = aam_shard.select(out, shard)
    print(f"[indigo] worklist {n_all:,} adjudicated, {n_map:,} admitted "
          f"({n_over:,} over the atom cap, which only this member takes) "
          f"-> {len(out):,} to map", flush=True)
    return out


def cmd_map(args):
    from indigo import Indigo
    ind = Indigo()
    # Indigo's own timeout knob, set as well as the alarm. It bounds some searches and
    # not the pathological ones; belt and braces, with the sidecar as the actual answer.
    ind.setOption("aam-timeout", args.timeout * 1000)

    exclude = None
    if args.exclude and Path(args.exclude).exists():
        exclude = set(pd.read_parquet(args.exclude, columns=["mnxr"])["mnxr"])
    shard = aam_shard.parse_spec(args.shard)
    universe = load_universe(Path(args.worklist), exclude, shard)

    out = Path(args.out)
    side = Path(args.sidecar) if args.sidecar else out.with_suffix(".attempted")
    out.parent.mkdir(parents=True, exist_ok=True)

    # RESUME OFF THE SIDECAR, NOT OFF THE OUTPUT. The output only holds reactions that
    # finished; the sidecar holds every reaction that was STARTED. The difference is
    # precisely the reaction a hang died on, and resuming off the output would attempt
    # it again, hang again, and never make progress. `aam_shard.read_sidecar` is what
    # refuses a cache written under a different shard count.
    attempted = aam_shard.read_sidecar(side, shard, who="indigo")
    if attempted:
        print(f"[indigo] sidecar: {len(attempted):,} reactions already attempted "
              f"under {aam_shard.spec_header(shard)}", flush=True)
    todo = [(m, s) for m, s in universe.items() if m not in attempted]
    if args.limit:
        todo = todo[:args.limit]
    print(f"[indigo] todo: {len(todo):,}", flush=True)
    if not todo:
        if not out.exists() or out.stat().st_size == 0:
            out.write_text("\t".join(COLUMNS) + "\n")
        return 0

    write_header = not out.exists() or out.stat().st_size == 0
    fo = open(out, "a", buffering=1)
    fs = aam_shard.Sidecar(side, shard)
    if write_header:
        fo.write("\t".join(COLUMNS) + "\n")
    t0 = time.time()
    tally = {}
    try:
        for i, (mnxr, smi) in enumerate(todo, 1):
            # BEFORE the attempt, and flushed. This one ordering is what makes an
            # uninterruptible hang cost one reaction instead of the whole run.
            fs.mark(mnxr)
            mapped, status = map_one(ind, smi, args.timeout)
            tally[status] = tally.get(status, 0) + 1
            fo.write("\t".join([mnxr, smi.replace("\t", " "),
                                mapped.replace("\t", " "),
                                str(INDIGO_CONFIDENCE), status]) + "\n")
            if i % 500 == 0 or i == len(todo):
                dt = time.time() - t0
                rate = i / dt if dt else 0
                print(f"      {i:,}/{len(todo):,}  {tally}  "
                      f"({dt:.0f}s, {rate:.1f} rxn/s, "
                      f"eta {(len(todo)-i)/rate if rate else 0:.0f}s)", flush=True)
    finally:
        fo.close()
        fs.close()
    print(f"[indigo] {tally} -> {out}", flush=True)
    return 0


def cmd_retry(args):
    """Second pass at a longer budget over the reactions the first pass timed out on.

    Not a rerun: it reads the cache, takes only `status == timeout`, and rewrites those
    rows. A timeout is a statement about a budget, not about the chemistry, so retrying
    it is the honest move and dropping it silently is not.
    """
    from indigo import Indigo
    ind = Indigo()
    ind.setOption("aam-timeout", args.timeout * 1000)
    d = pd.read_csv(args.out, sep="\t")
    todo = d[d["status"] == "timeout"]
    print(f"[indigo] retrying {len(todo):,} timeouts at {args.timeout}s", flush=True)
    fixed = 0
    for idx, r in todo.iterrows():
        mapped, status = map_one(ind, r["rxn_smiles"], args.timeout)
        d.at[idx, "mapped_rxn_smiles"] = mapped
        d.at[idx, "status"] = status
        fixed += status == "ok"
    d.to_csv(args.out, sep="\t", index=False)
    print(f"[indigo] recovered {fixed:,} of {len(todo):,}", flush=True)
    return 0


def cmd_merge(args):
    """Shards -> one member cache, in the schema the extractor reads."""
    # A GLOB THAT MATCHED FEWER FILES IS A SHORT MEMBER, and a short member is
    # indistinguishable from a member the chemistry defeated once it reaches the fusion.
    # The lane already fails on a shard that exits non-zero; this catches the shard that
    # never started.
    if args.expect and len(args.shard_file) != args.expect:
        raise SystemExit(
            f"[indigo] merging {len(args.shard_file)} shard caches, expected "
            f"{args.expect}. The missing shard's reactions would silently leave the "
            f"member rather than fail it.")
    parts = [pd.read_csv(p, sep="\t") for p in args.shard_file]
    d = pd.concat(parts, ignore_index=True).drop_duplicates("mnxr", keep="first")
    d = d[d["mapped_rxn_smiles"].notna() & (d["mapped_rxn_smiles"].astype(str) != "")]
    d[["mnxr", "rxn_smiles", "mapped_rxn_smiles", "confidence"]].to_csv(
        args.out, sep="\t", index=False)
    print(f"[indigo] merged {len(parts)} shards -> {len(d):,} mapped reactions "
          f"-> {args.out}", flush=True)
    return 0


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("map"); p.set_defaults(fn=cmd_map)
    p.add_argument("--worklist", required=True,
                   help="interm::aam_worklist -- the adjudicated todo list every member "
                        "shares; the rescued universe carries the same schema")
    p.add_argument("--exclude", default=None,
                   help="parquet with an mnxr column a lower layer already claimed")
    p.add_argument("--shard", default=None, help="i/n -- deterministic by crc32 of mnxr")
    p.add_argument("--sidecar", default=None,
                   help="the attempted-ids log. Written BEFORE each attempt, so an "
                        "uninterruptible hang costs one reaction, not the run")
    p.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT_S)
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--out", required=True)

    p = sub.add_parser("retry"); p.set_defaults(fn=cmd_retry)
    p.add_argument("--out", required=True, help="the cache to retry timeouts in, in place")
    p.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT_S * 6)

    p = sub.add_parser("merge"); p.set_defaults(fn=cmd_merge)
    p.add_argument("--shard-file", nargs="+", required=True)
    p.add_argument("--expect", type=int, default=None,
                   help="how many shard caches there must be; a glob that matched fewer "
                        "is a short member, not a small one")
    p.add_argument("--out", required=True)
    return ap.parse_args(argv)


if __name__ == "__main__":
    a = parse_args()
    sys.exit(a.fn(a))

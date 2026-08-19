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

THE MAPPER RUNS IN A CHILD PROCESS, WHICH IS WHAT MAKES THE HANG KILLABLE. Nothing
in-process can interrupt compiled code that does not check for signals; another process
can send it SIGKILL. So the parent owns the budget: it sends one submission, waits, and
if nothing comes back it kills the child, records the reaction as `killed`, and respawns.
An unbounded hang becomes one reaction's budget plus a process spawn.

That containment is what lets `INDIGO_ADMITS` keep the oversized tail. The size sweep
measures a 36% hang rate up there, which under the old arrangement meant routing those
reactions into this lane traded them for a stalled member; with the child process a hang
costs twenty seconds and a fork. One line reverts the admission if it proves inadequate.

THE SIDECAR STAYS, and its job narrows to the one it always really had: the reaction id
is written and flushed BEFORE the attempt, so a run that is OOM-killed or cancelled --
which the child process does nothing about -- leaves that id as the last line and the
next run skips it. Combined with the durable cache it is what makes a killed lane resume
rather than restart.

TWO KINDS OF TOO-SLOW, KEPT APART. `timeout` is Indigo's own budget expiring somewhere a
signal reached, and the row is written with an empty map. `killed` is the parent having
to kill the child. The difference between those two counts IS the measurement of how
often the compiled search runs away where nothing in-process can follow it, which is the
number that decides whether the oversized tail stays affordable.

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
import multiprocessing as mp
import signal
import sys
import time
from pathlib import Path

import pandas as pd

from . import shard as aam_shard
from . import worklist

COLUMNS = ("mnxr", "rxn_smiles", "mapped_rxn_smiles", "confidence", "status")

INDIGO_CONFIDENCE = 1.0

DEFAULT_TIMEOUT_S = 20

KILL_GRACE_S = 5


class _Timeout(Exception):
    pass


def _alarm(_sig, _frm):
    raise _Timeout()


def map_one(ind, smi: str, timeout_s: int):
    old = signal.signal(signal.SIGALRM, _alarm)
    signal.alarm(timeout_s)
    try:
        rxn = ind.loadReaction(smi)
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


def _serve(conn, timeout_s: int):
    from indigo import Indigo
    ind = Indigo()
    ind.setOption("aam-timeout", timeout_s * 1000)
    try:
        while True:
            smi = conn.recv()
            if smi is None:
                return
            conn.send(map_one(ind, smi, timeout_s))
    except (EOFError, KeyboardInterrupt):
        return


class Mapper:
    def __init__(self, timeout_s: int, serve=None):
        self.timeout_s = timeout_s
        self._serve = serve or _serve
        self.n_killed = 0
        self.n_respawned = 0
        self.proc = None
        self.conn = None
        self._spawn()

    def _spawn(self):
        ctx = mp.get_context("fork")
        parent, child = ctx.Pipe(duplex=True)
        self.proc = ctx.Process(target=self._serve, args=(child, self.timeout_s),
                                daemon=True)
        self.proc.start()
        child.close()
        self.conn = parent

    def _kill(self):
        try:
            self.conn.close()
        except Exception:
            pass
        if self.proc is not None and self.proc.is_alive():
            self.proc.kill()
        if self.proc is not None:
            self.proc.join(timeout=10)
        self.n_respawned += 1
        self._spawn()

    def map_one(self, smi: str):
        try:
            self.conn.send(smi)
        except (BrokenPipeError, OSError, ValueError):
            self._kill()
            return "", "error"
        if not self.conn.poll(self.timeout_s + KILL_GRACE_S):
            self.n_killed += 1
            self._kill()
            return "", "killed"
        try:
            return self.conn.recv()
        except (EOFError, OSError):
            self._kill()
            return "", "error"

    def close(self):
        try:
            self.conn.send(None)
            self.conn.close()
        except Exception:
            pass
        if self.proc is not None:
            self.proc.join(timeout=10)
            if self.proc.is_alive():
                self.proc.kill()


def load_universe(universe_parquet: Path, exclude=None, shard=None):
    d = pd.read_parquet(universe_parquet)
    if "verdict" not in d.columns:
        raise SystemExit(
            f"[indigo] {universe_parquet} has no `verdict` column, so it is not a "
            f"submission table. Members read `interm::aam_universe`, never "
            f"`lookup::reactions` directly.")
    n_all = len(d)
    d = d[d["verdict"].isin(worklist.INDIGO_ADMITS) & d["rxn_smiles"].notna()]
    n_over = int((d["verdict"] == "oversize").sum())
    out = {r.mnxr: r.rxn_smiles for r in d.itertuples(index=False)}
    n_map = len(out)
    if exclude:
        out = {m: s for m, s in out.items() if m not in exclude}
    out = aam_shard.select(out, shard)
    by_class = ""
    if "submission_class" in d.columns:
        vc = d["submission_class"].value_counts().to_dict()
        by_class = f" {vc}"
    print(f"[indigo] universe {n_all:,} submissions, {n_map:,} admitted{by_class} "
          f"({n_over:,} over the atom cap, which only this member takes) "
          f"-> {len(out):,} to map", flush=True)
    return out


def cmd_map(args):
    exclude = None
    if args.exclude and Path(args.exclude).exists():
        exclude = set(pd.read_parquet(args.exclude, columns=["mnxr"])["mnxr"])
    shard = aam_shard.parse_spec(args.shard)
    universe = load_universe(Path(args.universe), exclude, shard)

    out = Path(args.out)
    side = Path(args.sidecar) if args.sidecar else out.with_suffix(".attempted")
    out.parent.mkdir(parents=True, exist_ok=True)

    prior_caches, prior_sides = aam_shard.cache_files(args.cache_dir)
    carried, _stale, _foreign = aam_shard.read_cache(prior_caches + [out], universe,
                                                     who="indigo")

    attempted = aam_shard.read_sidecars(prior_sides + [side], shard, who="indigo")

    done = {r[0] for r in carried} | attempted
    todo = [(m, s) for m, s in universe.items() if m not in done]
    if args.limit:
        todo = todo[:args.limit]
    print(f"[indigo] {len(carried):,} carried from cache, {len(attempted):,} already "
          f"attempted -> todo: {len(todo):,}", flush=True)

    write_header = not out.exists() or out.stat().st_size == 0
    fo = open(out, "a", buffering=1)
    if write_header:
        fo.write("\t".join(COLUMNS) + "\n")
    seen_here = set()
    if out.exists() and out.stat().st_size > 0:
        with open(out) as fh:
            fh.readline()
            seen_here = {l.split("\t", 1)[0] for l in fh if l.strip()}
    for row in carried:
        if row[0] in seen_here:
            continue
        fo.write("\t".join((row + [""] * len(COLUMNS))[:len(COLUMNS)]) + "\n")

    if not todo:
        fo.close()
        print("[indigo] nothing to map", flush=True)
        return 0

    mapper = Mapper(args.timeout)
    fs = aam_shard.Sidecar(side, shard)
    t0 = time.time()
    tally = {}
    try:
        for i, (mnxr, smi) in enumerate(todo, 1):
            fs.mark(mnxr)
            mapped, status = mapper.map_one(smi)
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
        mapper.close()
    print(f"[indigo] {tally} -> {out}", flush=True)
    if mapper.n_killed:
        print(f"[indigo] {mapper.n_killed:,} submission(s) had to be KILLED: the "
              f"compiled search did not return and no signal reaches it. Each cost "
              f"{args.timeout + KILL_GRACE_S}s and a respawn, not the lane.", flush=True)
    return 0


def cmd_retry(args):
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
    p.add_argument("--universe", required=True,
                   help="interm::aam_universe -- the one submission table every member "
                        "shares, carrying all three submission classes")
    p.add_argument("--exclude", default=None,
                   help="parquet with an mnxr column a lower layer already claimed")
    p.add_argument("--shard", default=None, help="i/n -- deterministic by crc32 of mnxr")
    p.add_argument("--sidecar", default=None,
                   help="the attempted-ids log. Written BEFORE each attempt, so a kill "
                        "outside this process costs one submission, not the run")
    p.add_argument("--cache-dir", default=None,
                   help="the STAGED durable cache for this member: prior runs' cache.tsv "
                        "and *.attempted. A row is reused only when its submission "
                        "STRING matches the one this run would send, and the sidecars "
                        "are re-partitioned onto this run's shard spec rather than "
                        "refused across the change")
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

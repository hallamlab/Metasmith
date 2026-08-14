#!/usr/bin/env python3
"""Drive the 994 shards through `cyanoverse_gpr.py` in batches, resumably.

    python examples/run_campaign.py --batch-size 50 [--start 0] [--end 994]
                                    [--max-batches N] [--dry-run]

WHY BATCHES AT ALL. One workflow of 994 shards would submit ~994 GPU tasks in a
single ask, and -- worse -- one poisoned shard would put the whole corpus's
cache in question. A batch is the unit of blast radius: each has its own task
key, caches independently, and is retried on its own.

WHY THIS IS A SCRIPT AND NOT A LOOP IN A SHELL. The campaign runs for well over
a day, so the thing that matters is that losing the process costs nothing. State
lives in `campaign_state.tsv`, one line per batch, appended and flushed as each
finishes; a rerun skips what is recorded done and picks up mid-corpus. Nothing
is held in memory that a crash would lose.

CONSECUTIVE FAILURES STOP THE CAMPAIGN. A single batch failing is ordinary --
a node dies, a walltime is clipped -- and gets retried on the next pass. Three
in a row is not ordinary: it means something systemic changed (a reference
vanished, the queue is rejecting the account, the engine overlay went stale) and
every further batch would burn queue time reproducing the same failure.

`--batch-size` AND `--start` ARE PINS, NOT TUNING KNOBS. The task key is derived
from the shard set, so a batch's identity IS its `A:B` spec: changing either
after the campaign has begun re-runs everything from scratch AND leaves two runs
covering the same shards, which the per-assembly split then refuses to merge
because it cannot tell which library commit each came from. Pick them once.
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
DRIVER = REPO / "examples" / "cyanoverse_gpr.py"
STATE = REPO / "data" / "scratch" / "campaign_state.tsv"
LOGS = REPO / "data" / "scratch" / "campaign_logs"
N_SHARDS = 994
MAX_CONSECUTIVE_FAILURES = 3


def read_state() -> dict[str, str]:
    """batch spec -> status, last write wins so a retry supersedes a failure."""
    out: dict[str, str] = {}
    if not STATE.exists():
        return out
    for line in STATE.read_text().splitlines():
        if not line or line.startswith("#"):
            continue
        parts = line.split("\t")
        if len(parts) < 2:
            # A crash mid-append leaves a partial line. Skipping it costs one
            # batch a re-run; raising here would brick every later invocation
            # of the campaign on a file that is otherwise entirely good.
            print(f"skipping malformed state line: {line!r}", file=sys.stderr)
            continue
        out[parts[0]] = parts[1]
    return out


def record(spec: str, status: str, seconds: float, key: str = "") -> None:
    STATE.parent.mkdir(parents=True, exist_ok=True)
    with STATE.open("a") as fh:
        fh.write(f"{spec}\t{status}\t{seconds:.0f}\t{key}\n")
        fh.flush()
        os.fsync(fh.fileno())


def run_batch(spec: str, env: dict) -> tuple[int, str]:
    LOGS.mkdir(parents=True, exist_ok=True)
    started = time.time()
    # Append, never truncate: the STOPPING message tells the operator to read
    # these logs, and opening "w" on a retry destroys the failure that was the
    # reason to look.
    log = LOGS / f"batch_{spec.replace(':', '_')}.log"
    with log.open("a") as fh:
        proc = subprocess.run(
            [sys.executable, str(DRIVER), "--shards", spec, "--run"],
            cwd=REPO, env=env, stdout=fh, stderr=subprocess.STDOUT)
    # RUN_KEY is written by the driver AFTER the plan resolves. If this attempt
    # died before that, the file on disk belongs to a PREVIOUS attempt, and
    # recording it would attribute this failure to the wrong run directory --
    # so it is only read when the file is newer than the attempt's start.
    key = ""
    work = REPO / "data" / "scratch" / f"cyanoverse_gpr_{spec.replace(':', '_')}"
    rk = work / "RUN_KEY"
    if rk.exists() and rk.stat().st_mtime >= started:
        key = rk.read_text().strip()
    return proc.returncode, key


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--batch-size", type=int, default=50)
    ap.add_argument("--start", type=int, default=0)
    ap.add_argument("--end", type=int, default=N_SHARDS)
    ap.add_argument("--max-batches", type=int, default=None,
                    help="stop after this many batches THIS invocation; the "
                         "campaign resumes where it left off. 0 means 0, not "
                         "'unlimited' -- pass no flag for unlimited")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()

    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(
        [str(REPO / "src"), str(REPO / "src" / "metasmith" / "src"),
         env.get("PYTHONPATH", "")]).rstrip(os.pathsep)

    specs = [f"{i}:{min(i + a.batch_size, a.end)}"
             for i in range(a.start, a.end, a.batch_size)]
    state = read_state()
    todo = [s for s in specs if state.get(s) != "ok"]
    print(f"{len(specs)} batch(es) of {a.batch_size}; "
          f"{len(specs) - len(todo)} already ok, {len(todo)} to run")
    if a.dry_run:
        for s in todo:
            print(f"  would run {s} (was {state.get(s, 'new')})")
        return 0

    consecutive = 0
    stopped_early = False
    for n, spec in enumerate(todo, 1):
        if a.max_batches is not None and n > a.max_batches:
            stopped_early = True
            print(f"stopping after {a.max_batches} batch(es) this invocation; "
                  f"{len(todo) - a.max_batches} remain")
            break
        print(f"\n=== batch {n}/{len(todo)}: shards {spec} ===", flush=True)
        t0 = time.time()
        rc, key = run_batch(spec, env)
        # ONE automatic retry, because the failures this campaign actually sees
        # are per-attempt rather than per-batch: a GPU task that landed on a bad
        # node, and a join that dropped a single shard's group (batch 102:152
        # produced 49 gpr tables from 50 complete lane sets). Nextflow runs with
        # `-resume`, so the second attempt recomputes only what is missing --
        # minutes against the ~2 h a first pass costs -- and without it a run
        # that is 49/50 done burns the same queue time on the next invocation
        # AND spends one of the three consecutive failures that stop the
        # campaign outright.
        if rc != 0:
            print(f"    {spec} returned rc{rc}; retrying once (-resume "
                  f"recomputes only what is missing)", flush=True)
            rc, key2 = run_batch(spec, env)
            key = key2 or key
        dt = time.time() - t0
        status = "ok" if rc == 0 else f"rc{rc}"
        record(spec, status, dt, key)
        print(f"    {spec}: {status} in {dt / 3600:.2f} h (key {key or '?'})",
              flush=True)
        if rc == 0:
            consecutive = 0
            continue
        consecutive += 1
        print(f"    log: {LOGS / ('batch_' + spec.replace(':', '_') + '.log')}",
              file=sys.stderr)
        if consecutive >= MAX_CONSECUTIVE_FAILURES:
            print(f"\nSTOPPING: {consecutive} batches failed in a row. That is "
                  f"systemic, not a bad node -- a reference went missing, the "
                  f"account stopped accepting the ask, or the engine overlay "
                  f"went stale. Every further batch would spend queue time "
                  f"reproducing it. Read the logs in {LOGS}.", file=sys.stderr)
            return 1

    final = read_state()
    done = sum(1 for s in specs if final.get(s) == "ok")
    print(f"\n{done}/{len(specs)} batches ok")
    if done == len(specs):
        return 0
    # 3 distinguishes "I stopped because you told me to" from "batches failed",
    # which the caller otherwise cannot tell apart from the exit code.
    return 3 if stopped_early else 2


if __name__ == "__main__":
    sys.exit(main())

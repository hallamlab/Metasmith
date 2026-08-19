#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
DRIVER = REPO / "research" / "fabfos" / "examples" / "cyanoverse_gpr.py"
STATE = REPO / "data" / "fabfos" / "scratch" / "campaign_state.tsv"
LOGS = REPO / "data" / "fabfos" / "scratch" / "campaign_logs"
N_SHARDS = 994
MAX_CONSECUTIVE_FAILURES = 3


def read_state() -> dict[str, str]:
    out: dict[str, str] = {}
    if not STATE.exists():
        return out
    for line in STATE.read_text().splitlines():
        if not line or line.startswith("#"):
            continue
        parts = line.split("\t")
        if len(parts) < 2:
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
    log = LOGS / f"batch_{spec.replace(':', '_')}.log"
    with log.open("a") as fh:
        proc = subprocess.run(
            [sys.executable, str(DRIVER), "--shards", spec, "--run"],
            cwd=REPO, env=env, stdout=fh, stderr=subprocess.STDOUT)
    key = ""
    work = REPO / "data" / "fabfos" / "scratch" / f"cyanoverse_gpr_{spec.replace(':', '_')}"
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
        [str(REPO / "src"),
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
    return 3 if stopped_early else 2


if __name__ == "__main__":
    sys.exit(main())

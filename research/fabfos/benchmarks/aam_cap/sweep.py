"""Run Indigo over the sets at a given budget, one subprocess per reaction.

    mamba run -n rdkit-scratch python research/fabfos/benchmarks/aam_cap/sweep.py \
        --sets <dir>/sets.jsonl --budget 20 --out <dir>/results_20s.jsonl

THE BUDGET IS THE PRODUCTION ONE. `indigo_member.DEFAULT_TIMEOUT_S` is 20 seconds, so a
sweep at 20 answers the question the bake would actually ask. Running it again at a
larger budget answers the second question -- whether a longer one is worth anything --
which the lane's own `retry` verb has never had a number for.

THREE OUTCOMES, and keeping them apart is the point:

  * `ok`     -- Indigo returned a mapping inside the budget.
  * `error`  -- Indigo raised, which is how it reports its own `aam-timeout` expiring.
  * `hung`   -- neither Indigo's timeout nor the process's own bound stopped it and the
                parent had to SIGKILL. This is the failure mode the member lane's sidecar
                exists for, and a sweep that could not name it would report it as `error`
                and understate what the tail costs.

Appends, and skips keys already in the output, so an interrupted sweep resumes.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

HERE = Path(__file__).resolve().parent

# How long past its own budget a reaction is given before the parent kills it. Indigo's
# `aam-timeout` either fires or does not; a run still alive well past it is not going to
# finish, and waiting longer only makes the sweep cost what the tail costs rather than
# measuring it.
KILL_MARGIN_S = 20


def _attempt(row, budget):
    t0 = time.time()
    try:
        p = subprocess.run(
            [sys.executable, str(HERE / "map_one.py"),
             json.dumps({"smiles": row["smiles"]}), str(budget)],
            capture_output=True, text=True, timeout=budget + KILL_MARGIN_S)
        res = (json.loads(p.stdout) if p.stdout.strip()
               else dict(status="crash", secs=round(time.time() - t0, 2), mapped=""))
    except subprocess.TimeoutExpired:
        res = dict(status="hung", secs=round(time.time() - t0, 2), mapped="")
    return dict(key=row["key"], pop=row["pop"], mnxr=row["mnxr"],
                element=row["element"], atoms=row["atoms"], budget=budget, **res)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--sets", required=True, type=Path)
    ap.add_argument("--budget", type=int, default=20,
                    help="seconds, per reaction; the lane's default is 20")
    ap.add_argument("--out", required=True, type=Path)
    ap.add_argument("--workers", type=int, default=6,
                    help="concurrent mappings. `secs` stays honest under concurrency -- "
                         "Indigo is single-threaded and each attempt has its own process "
                         "-- but keep this well under the core count or the numbers "
                         "start measuring the sweep instead of the mapper.")
    a = ap.parse_args(argv)

    rows = [json.loads(l) for l in a.sets.read_text().splitlines()]
    done = set()
    if a.out.exists():
        done = {json.loads(l)["key"] for l in a.out.read_text().splitlines()}
    todo = [r for r in rows if r["key"] not in done]
    print(f"[cap] {len(rows):,} reactions at a {a.budget}s budget, "
          f"{len(done):,} already recorded, {len(todo):,} to run", flush=True)

    a.out.parent.mkdir(parents=True, exist_ok=True)
    with a.out.open("a") as f, ThreadPoolExecutor(max_workers=a.workers) as pool:
        for i, rec in enumerate(pool.map(lambda r: _attempt(r, a.budget), todo)):
            f.write(json.dumps(rec) + "\n")
            f.flush()
            if (i + 1) % 20 == 0:
                print(f"[cap]   {i + 1}/{len(todo)}", flush=True)
    print("[cap] done", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())

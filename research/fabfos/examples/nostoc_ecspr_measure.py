#!/usr/bin/env python3
"""Measure the composed community networks by calling `ecspr ground` directly.

    python research/fabfos/examples/nostoc_ecspr_measure.py --out data/scratch/... [-j 4]
    python research/fabfos/examples/nostoc_ecspr_measure.py --out ... --only NOS ERY

Reads the networks `nostoc_ecspr.py --compose` wrote and writes one parquet per unit,
four elements concatenated, under `<out>/<network>/ecspr-ground_results/` -- the layout
the pinned `nostoc/ecspr` chunk carries, so the products drop in beside the recorded ones
and `nostoc_ecspr_set4.py` reads them unchanged.

WHY THIS EXISTS BESIDE THE DAG DRIVER, AND WHAT IT GIVES UP
-----------------------------------------------------------
`nostoc_ecspr.py --run` is the route of record and stays so: it is the one that carries
lineage. Two things block it on a workstation today, and neither is about this experiment.

1. `env::ecspr.env` names `quay.io/hallamlab/ecspr:2026.07.14`, and that image carries the
   env but not the package -- its own first line says the tag is not yet republished. The
   step dies `127` and nextflow's `errorStrategy 'ignore'` turns that into a run that
   completes with zero products.
2. `transforms/fabfos/ecspr_measure.py` hardcodes `ELEMENT = "C"`. The pinned products
   carry all four elements per unit, from the generation when the probes were a transform
   each, so the DAG as it now stands cannot reproduce the comparison at all.

What this gives up is exactly what the DAG buys: the plan, the lineage pin from the GPR
table back through the recovery chain, and the cache. It runs THE SAME COMMAND LINE the
transform builds -- that is the point of `ecspr` being a package with a CLI rather than a
staged script, and it is what keeps this honest rather than a second implementation.

The composition is already outside the DAG for a related reason; see `nostoc_ecspr.py`.
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "research/fabfos/examples"))
from nostoc_ecspr import ARMS, ELEMENTS, NETS, UNITS, net_id  # noqa: E402

LEAK = "1e-6"
ECSPR_ENV = "ecspr"
PROBES = {"ground": "conditions_{src}.parquet",
          "two-point": "conditions_2t_{src}.parquet"}


def units():
    out, seen = [], set()
    for members, src in UNITS:
        for arm in ARMS:
            nid = net_id(members, arm)
            if (nid, src) not in seen:
                seen.add((nid, src))
                out.append((nid, src))
            if len(members) == 1:
                break
    return out


def shimmed_gpr(nid, work) -> Path:
    dest = work / "gpr_shimmed.parquet"
    if dest.exists():
        return dest
    g = pd.read_parquet(NETS / nid / "gpr.parquet")
    g.assign(unit_id=g.orf, feature_id=g.orf, feature_name=g.orf,
             evidence_id=g.intermediate_id).to_parquet(dest, index=False)
    return dest


def measure(nid, src, element, out_root, probe="ground", threads=1):
    d = NETS / nid
    work = out_root / nid / "_work"
    work.mkdir(parents=True, exist_ok=True)
    gpr = shimmed_gpr(nid, work)
    stem = f"{src}_{element}" if probe == "ground" else f"2t_{src}_{element}"
    shard = work / f"{stem}.shards"
    dest = work / f"{stem}.parquet"
    if dest.exists() and dest.stat().st_size > 0:
        return dest, 0.0, "cached"
    cmd = ["mamba", "run", "-n", ECSPR_ENV, "ecspr", probe,
           "--gpr", str(gpr),
           "--conditions", str(d / PROBES[probe].format(src=src)),
           "--atom-pairs", str(d / "atom_pairs.parquet"),
           "--direction", str(d / "direction.parquet"),
           "--element", element,
           *(("--leak", LEAK) if probe == "ground" else ()),
           "--shard-dir", str(shard),
           "--log", str(work / f"{stem}.log"),
           "--out", str(dest)]
    # The container sets both to 1: the rectified-diode Newton solve is single-threaded and
    # letting BLAS fan out just contends with the other units in flight.
    env = dict(os.environ, OMP_NUM_THREADS=str(threads), OPENBLAS_NUM_THREADS=str(threads))
    t0 = time.time()
    r = subprocess.run(cmd, cwd=REPO, env=env, capture_output=True, text=True)
    dt = time.time() - t0
    if r.returncode != 0 or not dest.exists() or dest.stat().st_size == 0:
        tail = (r.stderr or r.stdout or "").strip().splitlines()[-6:]
        return None, dt, f"FAILED rc={r.returncode}: " + " | ".join(tail)

    df = pd.read_parquet(dest)
    keep = df[df.element == element]
    assert len(keep), f"{nid}__{src} {element}: basis element absent from the conditions"
    if len(keep) != len(df):
        keep.to_parquet(dest, index=False)
    return dest, dt, "ok"


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--out", required=True, help="results root")
    p.add_argument("--only", nargs="*", default=None, help="restrict to these network ids")
    p.add_argument("--elements", nargs="*", default=list(ELEMENTS))
    p.add_argument("--probes", nargs="*", default=list(PROBES), choices=list(PROBES))
    p.add_argument("-j", "--jobs", type=int, default=4)
    a = p.parse_args(argv)

    out_root = Path(a.out).resolve()
    todo = [(nid, src) for nid, src in units() if not a.only or nid in a.only]
    if not todo:
        raise SystemExit(f"--only matched nothing; networks are "
                         f"{sorted({n for n, _ in units()})}")
    jobs = [(nid, src, el, pr) for nid, src in
            sorted(todo, key=lambda t: (t[0].count("-"), t[0]))
            for el in a.elements for pr in a.probes]
    print(f"{len(todo)} units x {len(a.elements)} elements x {len(a.probes)} probes "
          f"= {len(jobs)} solves, {a.jobs} at a time", flush=True)

    failed = []
    from concurrent.futures import as_completed
    with ThreadPoolExecutor(max_workers=a.jobs) as pool:
        futs = {pool.submit(measure, n, s, e, out_root, probe=pr): (n, s, e, pr)
                for n, s, e, pr in jobs}
        for i, f in enumerate(as_completed(futs), 1):
            nid, src, el, pr = futs[f]
            path, dt, status = f.result()
            print(f"[{i:3d}/{len(jobs)}] {nid}__{src} {el} {pr}: {status} "
                  f"({dt/60:.1f} min)", flush=True)
            if path is None:
                failed.append((nid, src, el, pr, status))

    for nid, src in todo:
        for pr in a.probes:
            pre = "" if pr == "ground" else "2t_"
            parts = sorted((out_root / nid / "_work").glob(f"{pre}{src}_?.parquet"))
            if len(parts) != len(a.elements):
                print(f"  {nid}__{src} {pr}: {len(parts)}/{len(a.elements)} elements, "
                      f"not published", file=sys.stderr)
                continue
            dest = out_root / nid / f"ecspr-{pr.replace('-', '_')}_results"
            dest.mkdir(parents=True, exist_ok=True)
            df = pd.concat([pd.read_parquet(x) for x in parts], ignore_index=True)
            df.to_parquet(dest / f"{nid}__{src}.parquet", index=False)
            print(f"  published {nid}__{src} {pr}: {len(df):,} rows, "
                  f"{df.element.nunique()} elements")

    if failed:
        print(f"\n{len(failed)} solves FAILED:", file=sys.stderr)
        for row in failed:
            print("  " + " ".join(map(str, row)), file=sys.stderr)
        return 1
    print("\nall solves ok")
    return 0


if __name__ == "__main__":
    sys.exit(main())

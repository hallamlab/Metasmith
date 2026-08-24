#!/usr/bin/env python3
"""The SCADC ECSPr null, redrawn over the 4-lane metagenome pool, run locally.

    python examples/scadc_metag_ecspr_null.py --fir-run       # SLURM array, 13 shards
    python examples/scadc_metag_ecspr_null.py --fir-status
    python examples/scadc_metag_ecspr_null.py --fir-retrieve
    python examples/scadc_metag_ecspr_null.py --score

    python examples/scadc_metag_ecspr_null.py --draws --workers 12   # or solve here

    data/fabfos/runs/scadc_metagenome/nulls/draws.parquet     the null distribution
    data/fabfos/runs/scadc_metagenome/ecspr/results.parquet   the observed run scored against it

Two chunks, because they have different lifetimes: the draws cost a cluster job to
regenerate, the scoring costs seconds and is re-run whenever a default moves. Note
that `ecspr_dir()` below resolves a THIRD directory also called `ecspr` -- that one
is the observed run, read never written.

WHAT CHANGED, AND WHY IT MATTERS
--------------------------------
The first null (`examples/scadc_ecspr_null.py`, fir sbatch 52238675) drew from a
**3-lane** metagenome pool -- clean/kofam/uniref50 -- while the observed fosmid
units it scores are **4-lane**. The bases did not match, and the mismatch runs
one way: the null pool was denied the `pbert` lane's nominations, so each draw
carried less evidence than a same-sized observed unit by construction. That is
not a conservative direction, it is simply a different measurement on each side
of the comparison. `examples/scadc_metag_gpr_4lane.py` closes it; this redraws
against the result.

Everything else is deliberately unchanged. The sampler, the N-bucketing, the
weight resolution and the solve are IMPORTED from `scadc_ecspr_null_draw.py`
rather than restated, so "same rule as the run this replaces" is a property of
the code and not a claim in a docstring.

WHERE IT RUNS
-------------
Either. The draws are independent, which is the fact both paths exploit and the
July run did not: it went to fir as ONE serial job and spent ~6 h wall.

`--fir-run` submits a **SLURM array over the 13 N buckets**, so the wall clock is
the slowest bucket rather than the sum -- ~30 min. Prefer it; a bucket is also
the natural shard, since each array task loads the GPR table once and then never
touches the others' work.

`--draws` solves here across a process pool instead, for when the cluster is not
worth the round trip. One build+solve is ~2.1 s on an idle dev host, so 26,000
draws is ~1.5 h across a dozen cores -- but only on an IDLE host: measured at
1.4 draws/s (a ~5 h ETA) with the box at 15.7/16 cores, because the workers
then get ~78% of a core each.

The split of labour is what keeps it in memory: the parent owns the ORF pool and
the per-ORF weight index (the large structures) and resolves each draw to the
few hundred `{mnxr: E}` it ADDS to the host baseline; the workers own the atom
pair table and do the build+solve. Neither side holds the other's. The pool is
created before the parent loads the GPR table, so no worker ever inherits it.

One consequence of that split is worth stating: the serial version summed each
drawn ORF's contribution onto the host's 1.0, this one sums the draw's
contributions together and then onto the host's 1.0. Same value, different
association order, so weights can differ in the last mantissa bit -- these draws
are reproducible against each other but not bit-identical to a serial re-run.

RESUMABLE. Draw rows are appended to `nulls/draws.csv` as they land and a
restart skips any (style, n_rep, iter) already there, so a kill costs the
in-flight draws rather than the pass. Seeds come from `draw_seed`, so a resumed
draw is the same draw.
"""
from __future__ import annotations

import argparse
import csv
import multiprocessing as mp
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(ROOT / "src"))

from scadc_ecspr_null_draw import (  # noqa: E402  -- the rule, not a copy of it
    ELEMENT, clr, draw_contiguous, draw_seed, draw_uniform, load_orf_pool,
    n_buckets, resolve_weights,
)

METAG = ROOT / "data" / "fabfos" / "runs" / "scadc_metagenome"
GPR4 = METAG / "gpr" / "gpr_4lane.parquet"
ORFS_CSV = METAG / "sequences" / "metag.orfs.csv"
NULLS = METAG / "nulls"
SCORED = METAG / "ecspr"
HOST_GEM = ROOT / "data" / "fabfos" / "runs" / "e_coli_epi300" / "gpr" / "gpr_gem.parquet"

FULL_K = 1000
SEED = 20260731
STYLES = ("A", "D")
Q_THRESHOLD = 0.05
CSV_COLS = ["style", "n_rep", "iter", "n_drawn", "condition_id", "metric", "delta_null"]


def ecspr_dir() -> Path:
    for c in (ROOT / "data" / "fabfos" / "runs" / "scadc_ecspr", ROOT / "data" / "scadc" / "ecspr"):
        if (c / "results.parquet").exists():
            return c
    raise SystemExit(
        "[null] no observed ECSPr run found at data/fabfos/runs/scadc_ecspr/ or "
        "data/scadc/ecspr/ -- this null has nothing to be the null FOR")


_W: dict = {}


def _init(conditions_path, ratios_path, pairs_path, host_mnxr, baseline):
    from ecspr.model.build import graph_from_pairs, load_direction_ratios, load_pairs
    from ecspr.model.graph import Terminal, solve
    cond = pd.read_parquet(conditions_path)
    _W.update(
        pairs=load_pairs(pairs_path, element=ELEMENT),
        ratios=load_direction_ratios(ratios_path),
        sink_hubs=cond.sink_hub.tolist(),
        sink_label=dict(zip(cond.sink_hub, cond.condition_id)),
        source_hub=cond.source_hub.iloc[0],
        host_weights={m: 1.0 for m in host_mnxr},
        host_total=baseline[0], host_clr=np.asarray(baseline[1]),
        _g=graph_from_pairs, _T=Terminal, _s=solve,
    )


def _build_and_solve(weights):
    g = _W["_g"](_W["pairs"], ELEMENT, weights, _W["ratios"])
    src = _W["_T"].metabolite(g, _W["source_hub"], label="glucose")
    snk = _W["_T"].merge(g, _W["sink_hubs"], label="ground")
    sol = _W["_s"](g, src, snk)
    return sol.total, {m: sol.delivered(m) for m in _W["sink_hubs"]}


def _solve_draw(task):
    style, n_rep, it, n_drawn, delta = task
    weights = dict(_W["host_weights"])
    for mnxr, e in delta.items():
        weights[mnxr] = weights.get(mnxr, 0.0) + e
    total_c, delivered = _build_and_solve(weights)
    s = sum(delivered.values())
    share = np.array([delivered[m] / s if s > 0 else 0.0 for m in _W["sink_hubs"]])
    c = clr(share)
    dt = total_c - _W["host_total"]
    rows = []
    for i, mnxm in enumerate(_W["sink_hubs"]):
        cid = _W["sink_label"][mnxm]
        rows.append((style, n_rep, it, n_drawn, cid, "delta_total", dt))
        rows.append((style, n_rep, it, n_drawn, cid, "delta_clr",
                     float(c[i] - _W["host_clr"][i])))
    return rows


def already_done(path: Path) -> set:
    done = set()
    if not path.exists():
        return done
    with path.open() as fh:
        for row in csv.DictReader(fh):
            done.add((row["style"], int(row["n_rep"]), int(row["iter"])))
    return done


def run_draws(k: int, workers: int) -> int:
    refs = ecspr_dir() / "refs"
    conditions = ecspr_dir() / "conditions.parquet"
    pairs_path = str(refs / "atom_pairs.parquet")
    ratios_path = str(refs / "direction_ratios.parquet")
    if not GPR4.exists():
        raise SystemExit(f"[null] {GPR4} is missing -- run "
                         f"examples/scadc_metag_gpr_4lane.py first")

    NULLS.mkdir(parents=True, exist_ok=True)
    out = NULLS / "draws.csv"

    from ecspr.model.build import load_direction_ratios, load_pairs  # noqa: F401
    host_mnxr = sorted(pd.read_parquet(HOST_GEM).mnxr.dropna().unique().tolist())
    _init(conditions, ratios_path, pairs_path, host_mnxr, (0.0, [0.0]))
    t0 = time.time()
    host_total, host_delivered = _build_and_solve(_W["host_weights"])
    hs = sum(host_delivered.values())
    host_clr = clr(np.array([host_delivered[m] / hs if hs > 0 else 0.0
                             for m in _W["sink_hubs"]]))
    print(f"[null] host baseline total={host_total:.6g} ({time.time()-t0:.1f}s)", flush=True)
    _W.clear()

    ctx = mp.get_context("spawn")
    pool = ctx.Pool(workers, initializer=_init,
                    initargs=(conditions, ratios_path, pairs_path, host_mnxr,
                              (host_total, host_clr.tolist())))

    print(f"[null] loading {GPR4.relative_to(ROOT)}", flush=True)
    gpr = pd.read_parquet(GPR4)
    lanes = sorted(gpr["channel"].unique().tolist())
    from ecspr.model.evidence import per_unit_weights
    per_orf = per_unit_weights(gpr, "orf")
    del gpr
    print(f"[null] lanes={lanes}  {len(per_orf):,} ORFs carry >=1 nominated MNXR",
          flush=True)
    all_ids, contig_spans = load_orf_pool(str(ORFS_CSV))
    print(f"[null] pool: {len(all_ids):,} ORFs across {len(contig_spans):,} contigs",
          flush=True)

    reps = n_buckets(str(refs / "observed_n_orfs.txt"))
    done = already_done(out)
    total = len(reps) * len(STYLES) * k
    print(f"[null] {len(reps)} N buckets x {len(STYLES)} styles x {k} draws = "
          f"{total:,} solves. buckets={reps}", flush=True)
    if done:
        print(f"[null] resuming: {len(done):,} draws already in {out.name}", flush=True)

    def tasks():
        for style in STYLES:
            for n_rep in reps:
                for it in range(k):
                    if (style, n_rep, it) in done:
                        continue
                    rng = np.random.default_rng(draw_seed(SEED, style, n_rep, it))
                    if style == "A":
                        drawn = draw_uniform(rng, all_ids, n_rep)
                    else:
                        drawn = draw_contiguous(rng, all_ids, contig_spans, n_rep)
                        if drawn is None:
                            print(f"[null] SKIP D n={n_rep} iter={it}: no contig has "
                                  f">= {n_rep} ORFs", flush=True)
                            continue
                    yield (style, n_rep, it, len(drawn),
                           resolve_weights(drawn, per_orf, {}))

    write_header = not (out.exists() and out.stat().st_size > 0)
    n_done, t0 = len(done), time.time()
    with out.open("a", newline="") as fh:
        w = csv.writer(fh)
        if write_header:
            w.writerow(CSV_COLS)
        for rows in pool.imap_unordered(_solve_draw, tasks(), chunksize=1):
            w.writerows(rows)
            fh.flush()
            n_done += 1
            if n_done % 100 == 0 or n_done == total:
                el = time.time() - t0
                rate = (n_done - len(done)) / max(el, 1e-9)
                print(f"[null] {n_done:,}/{total:,} draws  {rate:.1f}/s  "
                      f"eta {(total-n_done)/max(rate,1e-9)/60:.0f} min", flush=True)
    pool.close()
    pool.join()

    import pyarrow.csv as pcsv
    import pyarrow.parquet as pq
    tbl = pcsv.read_csv(out)
    pq.write_table(tbl, NULLS / "draws.parquet")
    print(f"[null] {tbl.num_rows:,} draw-rows -> "
          f"{(NULLS / 'draws.parquet').relative_to(ROOT)}", flush=True)
    return 0


REMOTE_WORK = "/scratch/phyberos/fabfos_metagenome"
REMOTE_GPR4 = f"{REMOTE_WORK}/results/metag_gpr_4lane.parquet"
REMOTE_ORFS = f"{REMOTE_WORK}/raw/metag.orfs.csv"
REMOTE_REFS = f"{REMOTE_WORK}/ecspr_refs"
REMOTE_LIB = f"{REMOTE_WORK}/lib/ecspr"
REMOTE_OUT = f"{REMOTE_WORK}/results/null4_draws_b%a.csv"
SIF = ("/scratch/phyberos/cache/apptainer/"
       "docker..quay.io_hallamlab_python_for_data_science..1.2.5.sif")
N_BUCKETS = 13
JOB_NAME = "scadc_null4"


def _sbatch(k: int) -> str:
    return f"""#!/bin/bash
#SBATCH --job-name={JOB_NAME}
#SBATCH --account=rrg-shallam-ab
#SBATCH --array=0-{N_BUCKETS - 1}
#SBATCH --cpus-per-task=2
#SBATCH --mem=32G
#SBATCH --time=04:00:00
#SBATCH --output={REMOTE_WORK}/logs/null4_%A_%a.log

set -euo pipefail
apptainer exec --bind /scratch/phyberos:/scratch/phyberos {SIF} \\
  python3 {REMOTE_WORK}/scadc_ecspr_null_draw.py \\
    --metag-gpr  {REMOTE_GPR4} \\
    --metag-orfs {REMOTE_ORFS} \\
    --pairs      {REMOTE_REFS}/atom_pairs.parquet \\
    --ratios     {REMOTE_REFS}/direction_ratios.parquet \\
    --host-gem   {REMOTE_REFS}/gpr_gem.parquet \\
    --conditions {REMOTE_REFS}/conditions.parquet \\
    --observed-n {REMOTE_REFS}/observed_n_orfs.txt \\
    --lib-dir    {REMOTE_LIB} \\
    --bucket-index $SLURM_ARRAY_TASK_ID \\
    --k {k} --seed {SEED} \\
    --out {REMOTE_WORK}/results/null4_draws_b$SLURM_ARRAY_TASK_ID.csv \\
    --resume
echo "SBATCH_DONE rc=$?"
"""


def fir_run(k: int, host: str) -> int:
    import subprocess
    from _driver import ssh_once
    if not GPR4.exists():
        raise SystemExit(f"[fir] {GPR4} is missing -- build it first")
    draw_script = Path(__file__).resolve().parent / "scadc_ecspr_null_draw.py"

    print(f"=== staging the 4-lane table + draw script on {host} ===")
    ssh_once(host, f"mkdir -p {REMOTE_WORK}/logs {REMOTE_WORK}/results "
                   f"{Path(REMOTE_LIB).parent}; rm -rf {REMOTE_LIB}")
    for src, dst in ((GPR4, REMOTE_GPR4), (draw_script, f"{REMOTE_WORK}/")):
        print(f"  {Path(src).name} -> {dst}")
        subprocess.run(["scp", "-q", str(src), f"{host}:{dst}"], check=True)
    subprocess.run(["scp", "-qr", str(ROOT / "src" / "ecspr"),
                    f"{host}:{Path(REMOTE_LIB).parent}/"], check=True)

    missing = ssh_once(host, "; ".join(
        f'[ -e "{p}" ] || echo "MISSING {p}"'
        for p in (REMOTE_GPR4, REMOTE_ORFS, SIF,
                  f"{REMOTE_REFS}/atom_pairs.parquet",
                  f"{REMOTE_REFS}/observed_n_orfs.txt"))).strip()
    if missing:
        print(missing, file=sys.stderr)
        return 1

    sb = f"{REMOTE_WORK}/scadc_null4.sbatch"
    out = ssh_once(host, f"cat > {sb} <<'EOF'\n{_sbatch(k)}EOF\n"
                         f"cd {REMOTE_WORK} && sbatch {sb}")
    print(out.strip())
    print(f"\n  status:   python {Path(__file__).name} --fir-status"
          f"\n  retrieve: python {Path(__file__).name} --fir-retrieve")
    return 0


def fir_status(host: str) -> int:
    from _driver import ssh_once
    print(ssh_once(host, f"squeue -u phyberos -n {JOB_NAME} -o '%.14i %.9T %.10M %.20R'; "
                         f"echo '--- shard rows (16,001 = complete) ---'; "
                         f"wc -l {REMOTE_WORK}/results/null4_draws_b*.csv 2>/dev/null "
                         f"| tail -{N_BUCKETS + 1}"))
    return 0


def fir_retrieve(host: str) -> int:
    import subprocess
    NULLS.mkdir(parents=True, exist_ok=True)
    shard_dir = NULLS / "shards"
    shard_dir.mkdir(exist_ok=True)
    subprocess.run(["scp", "-q", f"{host}:{REMOTE_WORK}/results/null4_draws_b*.csv",
                    str(shard_dir)], check=True)
    shards = sorted(shard_dir.glob("null4_draws_b*.csv"))
    if len(shards) != N_BUCKETS:
        raise SystemExit(
            f"[fir] {len(shards)} shards retrieved, {N_BUCKETS} expected. Scoring a "
            f"partial null silently drops whole N buckets -- every observed unit "
            f"nearest to a missing one would be matched to the wrong size")

    frames = [pd.read_csv(s) for s in shards]
    draws = pd.concat(frames, ignore_index=True)
    per_bucket = draws.groupby("n_rep")["iter"].nunique()
    print(f"[fir] {len(shards)} shards -> {len(draws):,} draw-rows over "
          f"{draws['n_rep'].nunique()} buckets")
    print(f"[fir] draws per bucket: min {per_bucket.min()}, max {per_bucket.max()}")
    draws.to_csv(NULLS / "draws.csv", index=False)
    draws.to_parquet(NULLS / "draws.parquet", index=False)
    print(f"[fir] -> {(NULLS / 'draws.parquet').relative_to(ROOT)}")
    return 0


def nearest_bucket(n_orfs: int, buckets: list) -> int:
    return min(buckets, key=lambda b: abs(b - n_orfs))


def empirical_p(delta_obs: float, null_vals: np.ndarray, two_sided: bool) -> float:
    n = len(null_vals)
    exceed = (np.sum(np.abs(null_vals) >= abs(delta_obs)) if two_sided
              else np.sum(null_vals >= delta_obs))
    return (exceed + 1) / (n + 1)


def bh_fdr(pvals: np.ndarray) -> np.ndarray:
    n = len(pvals)
    order = np.argsort(pvals)
    q = pvals[order] * n / (np.arange(n) + 1)
    q = np.clip(np.minimum.accumulate(q[::-1])[::-1], 0, 1)
    out = np.empty(n)
    out[order] = q
    return out


def score() -> int:
    draws = NULLS / "draws.parquet"
    if not draws.exists():
        raise SystemExit(f"[score] {draws} missing -- run --draws first")
    obs = ecspr_dir() / "results.parquet"
    results = pd.read_parquet(obs)
    null = pd.read_parquet(draws)
    buckets = sorted(null["n_rep"].unique().tolist())
    print(f"[score] observed {obs.relative_to(ROOT)} | {len(buckets)} null "
          f"N-buckets: {buckets}")

    host = results["unit"] == "epi300_host"
    results["n_bucket"] = pd.array([pd.NA] * len(results), dtype="Int64")
    results.loc[~host, "n_bucket"] = results.loc[~host, "n_orfs"].apply(
        lambda n: nearest_bucket(n, buckets))

    groups = {key: g["delta_null"].to_numpy()
              for key, g in null.groupby(["n_rep", "condition_id", "metric"])}

    p = np.full(len(results), np.nan)
    for i, row in results.loc[~host].iterrows():
        key = (int(row["n_bucket"]), row["condition_id"], row["metric"])
        vals = groups.get(key)
        if vals is None:
            raise SystemExit(f"[score] no null draws for bucket {key}")
        p[i] = empirical_p(row["delta_obs"], vals, row["direction"] == "two_sided")
    results["p"] = p

    results["q"] = np.nan
    for _, idx in results.loc[~host].groupby("metric").groups.items():
        results.loc[idx, "q"] = bh_fdr(results.loc[idx, "p"].to_numpy())
    results["survives"] = pd.array([pd.NA] * len(results), dtype="boolean")
    results.loc[~host, "survives"] = results.loc[~host, "q"] < Q_THRESHOLD
    results["null_basis"] = "metag_gpr_4lane"

    n_s, n_t = int(results.loc[~host, "survives"].sum()), int((~host).sum())
    print(f"[score] {n_s}/{n_t} non-host rows survive q < {Q_THRESHOLD}")
    print(results.loc[~host].groupby("metric")["survives"].sum())
    for m, g in results.loc[~host].groupby("metric"):
        print(f"[score]   {m}: obs mean {g['delta_obs'].mean():+.5g} | "
              f"null mean {null[null.metric == m]['delta_null'].mean():+.5g} | "
              f"min p {g['p'].min():.4g}")

    SCORED.mkdir(parents=True, exist_ok=True)
    out = SCORED / "results.parquet"
    results.to_parquet(out, index=False)
    print(f"[score] wrote {out.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--draws", action="store_true", help="solve the draws locally")
    ap.add_argument("--fir-run", action="store_true",
                    help="stage the 4-lane table and submit the SLURM array")
    ap.add_argument("--fir-status", action="store_true")
    ap.add_argument("--fir-retrieve", action="store_true")
    ap.add_argument("--score", action="store_true")
    ap.add_argument("--k", type=int, default=FULL_K, help="draws per (style, bucket)")
    ap.add_argument("--workers", type=int, default=12, help="--draws only")
    ap.add_argument("--host", default="fir")
    a = ap.parse_args()
    if not (a.draws or a.score or a.fir_run or a.fir_status or a.fir_retrieve):
        ap.error("pick one of --draws / --fir-run / --fir-status / --fir-retrieve / --score")
    rc = 0
    if a.fir_run:
        rc = fir_run(a.k, a.host)
    if a.fir_status and rc == 0:
        rc = fir_status(a.host)
    if a.fir_retrieve and rc == 0:
        rc = fir_retrieve(a.host)
    if a.draws and rc == 0:
        rc = run_draws(a.k, a.workers)
    if a.score and rc == 0:
        rc = score()
    sys.exit(rc)

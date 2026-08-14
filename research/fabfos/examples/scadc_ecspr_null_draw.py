"""Null-draw sampler + solver for the SCADC ECSPr significance test (plan T2).

Draws ORFs from the metagenome pool, resolves them to {mnxr: E} via the SAME
per-ORF conservation weights (`ecspr.evidence.per_unit_weights`) and solves on
the SAME atom-resolved engine (`ecspr.build`/`ecspr.graph`) that produced
`data/fabfos/runs/scadc_ecspr/results.parquet` -- never the retired SMW star solver. Host
GEM weights are added on top of every draw exactly as `scadc_ecspr.py` adds
them on top of each observed unit's weights.

Two sampler styles:
  A -- uniform: N ORF ids drawn uniformly at random from the full metag ORF
       inventory (1,442,614 ids), independent of contig.
  D -- contiguous: an N-ORF contiguous window on one contig, chosen uniformly
       among contigs holding >= N ORFs, at a uniform starting offset. The
       operon-like/insert-like null -- ORFs physically adjacent, the way a
       real fosmid insert's ORFs are, vs style A's scattered draw.

N buckets are read off the OBSERVED run's own n_orfs distribution
(`--observed-n`, one int per line, host row excluded) and grouped so no
bucket's max/min ratio exceeds 1.15 -- so matching an observed unit to its
nearest bucket in T3 loses at most ~7% of N. Computed from the actual local
distribution at call time, never copied from a prior deployment's fixed
N-buckets (those were sized for a different, 199-insert run).

RESUMABLE: draws are appended to --out (CSV, one row per condition x metric)
and flushed after each draw solves. --resume skips any (style, n_rep, iter)
already present in an existing --out, so a walltime timeout or crash loses at
most the one in-flight draw, never the whole job.

Frozen / reproducible: each draw's RNG is seeded from (--seed, style, n_rep,
iter) through `draw_seed`, not a single running stream -- so resuming (or
re-running the exact same call) reproduces the identical draws. That seed is a
blake2b digest rather than `hash()`, which is salted per interpreter for str
and made this claim false across processes; see `draw_seed`.

Usage (inside the python_for_data_science apptainer image, same env as
compile_metag_gpr.py):
  python3 scadc_ecspr_null_draw.py \
    --metag-gpr   /scratch/phyberos/fabfos_metagenome/results/metag_gpr_3lane.parquet \
    --metag-orfs  /scratch/phyberos/fabfos_metagenome/raw/metag.orfs.csv \
    --pairs       /scratch/phyberos/fabfos_refs_ecspr/atom_pairs.parquet \
    --ratios      /scratch/phyberos/fabfos_refs_ecspr/direction_ratios.parquet \
    --host-gem    /scratch/phyberos/fabfos_refs_ecspr/gpr_gem.parquet \
    --conditions  /scratch/phyberos/fabfos_refs_ecspr/conditions.parquet \
    --observed-n  /scratch/phyberos/fabfos_refs_ecspr/observed_n_orfs.txt \
    --lib-dir     /scratch/phyberos/fabfos_metagenome/lib \
    --k 150 --seed 20260731 \
    --out /scratch/phyberos/fabfos_metagenome/results/null_draws.csv \
    --resume
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import os
import sys

import numpy as np
import pandas as pd

ELEMENT = "C"


def n_buckets(observed_n_path, max_ratio=1.15):
    ns = sorted({int(x) for x in open(observed_n_path) if x.strip()})
    if not ns:
        raise SystemExit("[null] --observed-n has no values")
    buckets, cur = [], [ns[0]]
    for n in ns[1:]:
        if n / cur[0] <= max_ratio:
            cur.append(n)
        else:
            buckets.append(cur)
            cur = [n]
    buckets.append(cur)
    return [round(sum(b) / len(b)) for b in buckets]


def load_orf_pool(metag_orfs_path):
    df = pd.read_csv(metag_orfs_path, usecols=["orf"])
    ids = df["orf"].to_numpy()
    contig = df["orf"].str.rsplit("_", n=1).str[0].to_numpy()
    ordinal = df["orf"].str.rsplit("_", n=1).str[1].astype(int).to_numpy()
    order = np.lexsort((ordinal, contig))
    ids, contig, ordinal = ids[order], contig[order], ordinal[order]
    boundaries = np.flatnonzero(np.r_[True, contig[1:] != contig[:-1], True])
    contig_spans = {}  # contig -> (start_idx, end_idx) into the sorted `ids` array
    names = contig[boundaries[:-1]]
    for name, s, e in zip(names, boundaries[:-1], boundaries[1:]):
        contig_spans[name] = (s, e)
    return ids, contig_spans


def draw_seed(seed, style, n_rep, it) -> int:
    """A per-draw seed that survives leaving the process.

    This was `abs(hash((seed, style, n_rep, it))) % 2**32`, and Python salts
    `hash()` of a *str* per interpreter unless PYTHONHASHSEED is set -- so the
    "resuming reproduces the identical draws" contract in this module's
    docstring held only within one process. A resumed job silently drew a
    different null than the one it was continuing, and re-running the same call
    reproduced nothing. blake2b of the same four values has no such salt.
    """
    h = hashlib.blake2b(f"{seed}|{style}|{n_rep}|{it}".encode(), digest_size=4)
    return int.from_bytes(h.digest(), "big")


def draw_uniform(rng, all_ids, n):
    return rng.choice(all_ids, size=n, replace=False)


def draw_contiguous(rng, ids, contig_spans, n):
    eligible = [c for c, (s, e) in contig_spans.items() if e - s >= n]
    if not eligible:
        return None
    contig = eligible[rng.integers(len(eligible))]
    s, e = contig_spans[contig]
    start = s + rng.integers(0, (e - s) - n + 1)
    return ids[start:start + n]


def resolve_weights(orf_ids, per_orf_weights, host_weights):
    combined = dict(host_weights)
    for oid in orf_ids:
        w = per_orf_weights.get(oid)
        if not w:
            continue
        for mnxr, e in w.items():
            combined[mnxr] = combined.get(mnxr, 0.0) + e
    return combined


def clr(shares: np.ndarray) -> np.ndarray:
    eps = 1e-12
    x = np.log(shares + eps)
    return x - x.mean()


def already_done(out_path):
    done = set()
    if not os.path.exists(out_path):
        return done
    with open(out_path) as fh:
        r = csv.DictReader(fh)
        for row in r:
            done.add((row["style"], int(row["n_rep"]), int(row["iter"])))
    return done


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--metag-gpr", required=True)
    ap.add_argument("--metag-orfs", required=True)
    ap.add_argument("--pairs", required=True)
    ap.add_argument("--ratios", required=True)
    ap.add_argument("--host-gem", required=True)
    ap.add_argument("--conditions", required=True)
    ap.add_argument("--observed-n", required=True)
    ap.add_argument("--lib-dir", default=None,
                     help="a staged copy of src/ecspr, when the remote env has no "
                          "installed `ecspr` package")
    ap.add_argument("--styles", default="A,D")
    ap.add_argument("--k", type=int, default=150, help="draws per (style, n bucket)")
    ap.add_argument("--seed", type=int, default=20260731)
    ap.add_argument("--out", required=True)
    ap.add_argument("--resume", action="store_true")
    ap.add_argument("--bucket-index", type=int, default=None,
                    help="run ONLY the i'th N bucket, for a SLURM array shard")
    a = ap.parse_args()

    if a.lib_dir:
        # The SIF this runs under carries numpy/scipy/pandas but not `ecspr`, so the
        # package directory is staged beside the driver and put on the path. Its
        # PARENT is what goes on sys.path -- `ecspr` is a package now, not four
        # loose modules.
        sys.path.insert(0, str(Path(a.lib_dir).resolve().parent))
    from ecspr.build import load_pairs, load_direction_ratios, graph_from_pairs
    from ecspr.evidence import per_unit_weights
    from ecspr.graph import Terminal, solve

    pairs = load_pairs(a.pairs, element=ELEMENT)
    ratios = load_direction_ratios(a.ratios)
    conditions = pd.read_parquet(a.conditions)
    assert conditions.element.eq(ELEMENT).all()
    source_hub = conditions.source_hub.iloc[0]
    assert conditions.source_hub.nunique() == 1
    sink_hubs = conditions.sink_hub.tolist()
    sink_label = dict(zip(conditions.sink_hub, conditions.condition_id))

    host = pd.read_parquet(a.host_gem)
    host_weights = {m: 1.0 for m in host.mnxr.dropna().unique()}

    print(f"[null] loading metag GPR from {a.metag_gpr}", flush=True)
    metag_gpr = pd.read_parquet(a.metag_gpr)
    per_orf_weights = per_unit_weights(metag_gpr, "orf")
    print(f"[null] {len(per_orf_weights):,} ORFs carry >=1 nominated MNXR", flush=True)

    all_ids, contig_spans = load_orf_pool(a.metag_orfs)
    print(f"[null] pool: {len(all_ids):,} ORFs across {len(contig_spans):,} contigs",
          flush=True)

    def build_and_solve(weights):
        g = graph_from_pairs(pairs, ELEMENT, weights, ratios)
        src = Terminal.metabolite(g, source_hub, label="glucose")
        snk = Terminal.merge(g, sink_hubs, label="ground")
        sol = solve(g, src, snk)
        delivered = {m: sol.delivered(m) for m in sink_hubs}
        return sol.total, delivered

    print("[null] solving host baseline", flush=True)
    host_total, host_delivered = build_and_solve(host_weights)
    host_sum = sum(host_delivered.values())
    host_share = np.array([host_delivered[m] / host_sum if host_sum > 0 else 0.0
                            for m in sink_hubs])
    host_clr = clr(host_share)
    print(f"[null] host total={host_total:.6g}", flush=True)

    reps = n_buckets(a.observed_n)
    # The bucket list is computed from the WHOLE observed distribution and only
    # then narrowed, so shard i means the same N in every array task and in a
    # serial re-run. Bucketing the shard's own slice instead would make the
    # bucket identity depend on how the work was divided.
    if a.bucket_index is not None:
        if not 0 <= a.bucket_index < len(reps):
            raise SystemExit(f"[null] --bucket-index {a.bucket_index} is outside "
                             f"the {len(reps)} buckets {reps}")
        reps = [reps[a.bucket_index]]
    styles = a.styles.split(",")
    print(f"[null] {len(reps)} N buckets x {len(styles)} styles x {a.k} draws "
          f"= {len(reps) * len(styles) * a.k:,} solves. buckets={reps}", flush=True)

    done = already_done(a.out) if a.resume else set()
    if done:
        print(f"[null] resuming: {len(done):,} draws already in {a.out}", flush=True)

    write_header = not (a.resume and os.path.exists(a.out) and os.path.getsize(a.out) > 0)
    fh = open(a.out, "a", newline="")
    w = csv.writer(fh)
    if write_header:
        w.writerow(["style", "n_rep", "iter", "n_drawn", "condition_id", "metric",
                    "delta_null"])
        fh.flush()

    total = len(reps) * len(styles) * a.k
    done_count = 0
    for style in styles:
        for n_rep in reps:
            for it in range(a.k):
                key = (style, n_rep, it)
                if key in done:
                    done_count += 1
                    continue
                rng = np.random.default_rng(draw_seed(a.seed, style, n_rep, it))
                if style == "A":
                    drawn = draw_uniform(rng, all_ids, n_rep)
                elif style == "D":
                    drawn = draw_contiguous(rng, all_ids, contig_spans, n_rep)
                    if drawn is None:
                        print(f"[null] SKIP {style} n={n_rep} iter={it}: "
                              f"no contig has >= {n_rep} ORFs", flush=True)
                        done_count += 1
                        continue
                else:
                    raise SystemExit(f"[null] unknown style {style!r}")

                weights = resolve_weights(drawn, per_orf_weights, host_weights)
                total_c, delivered = build_and_solve(weights)
                s = sum(delivered.values())
                share = np.array([delivered[m] / s if s > 0 else 0.0 for m in sink_hubs])
                c = clr(share)
                delta_total = total_c - host_total
                for i, mnxm in enumerate(sink_hubs):
                    cid = sink_label[mnxm]
                    w.writerow([style, n_rep, it, len(drawn), cid, "delta_total",
                                delta_total])
                    w.writerow([style, n_rep, it, len(drawn), cid, "delta_clr",
                                float(c[i] - host_clr[i])])
                fh.flush()
                done_count += 1
                if done_count % 25 == 0 or done_count == total:
                    print(f"[null] {done_count:,}/{total:,} draws", flush=True)

    fh.close()
    print(f"[null] done -> {a.out}", flush=True)


if __name__ == "__main__":
    sys.exit(main())

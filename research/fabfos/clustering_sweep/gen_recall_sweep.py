"""Clustering silhouette against clone recall, swept over the day-6 pool's pieces.

THE QUESTION
  The shipped insert set is cut at the silhouette optimum -- `--select-k silhouette`
  picked between-cluster identity 0.579 on the 669-piece run. Nothing had ever asked
  what that costs in CLONES. Base-level retention is the wrong instrument for that
  question and says so by staying above 99% almost everywhere: the loss is not bases
  trimmed off the ends, it is whole fosmids merged into a centroid they share 58%
  identity with. This script measures the loss the way it actually happens and draws
  the trade-off.

SCOPE
  The three `pool01_*` barcodes -- day 6 in `pool_lineage.csv`, and one library
  sequenced three times, so the subset is self-contained rather than an arbitrary
  slice. It carries 409 of the 669 pieces the full dedup clusters, and its silhouette
  optimum lands on the same identity the full run selected, which is what earns it
  standing as a test bed (the TSV records it, so that is evidenced and not assumed).

INPUT
  main/figures/inserts/{pieces,identity}.py -- a dedup work directory
  (`piece_meta.json`, `pooled.fna`, `ava_hits.tsv`) rebuilt from the shipped run
  under ./data. The all-vs-all is over all 669 pieces, so the day-6 submatrix is
  already in it and the subset is taken on the qualified piece id. `--rebuild`
  forces that directory to be re-derived rather than reused.

METHOD
  The sweep walks the unique merge heights of the complete-linkage tree, exactly as
  `_cluster_by_silhouette` does: every partition the clustering can actually produce,
  once each, with its between-cluster identity read off as `1 - height`. Forcing k is
  the wrong sweep -- the distance matrix is full of exact ties, so most k are
  unrealisable.

  Representatives are chosen by calling the pipeline's own `_representatives` --
  closure band, closed-ends preference, then the containment absorb pass. Restating
  those rules here would measure something the pipeline would never ship.

  CLONE RECALL fixes a reference cut at identity R (post-absorb) and calls each of its
  surviving representatives a clone. For a candidate partition, recall is the number of
  DISTINCT surviving centroids those clones resolve to, over the number of clones: two
  clones landing on one centroid is one clone lost. R is swept over 0.98 / 0.99 / 0.995
  because the reference is a cut of the same tree it judges, and that dependence has to
  be visible rather than buried in a default.

ENV   mamba run -n figure-net python main/clustering_sweep/gen_recall_sweep.py
OUT   main/clustering_sweep/cache/recall_sweep.tsv     the full curve
      main/clustering_sweep/cache/day6_pieces.fna      the 409-piece reference for T2
      main/clustering_sweep/cache/day6_pieces.tsv      C-key -> qualified piece id
      main/clustering_sweep/cache/day6_partitions.json centroid sets, per partition

`cache/` is gitignored repo-wide; the script is the artifact under version control.
"""
import argparse
import json
import sys
from pathlib import Path

import numpy as np
from scipy.cluster.hierarchy import fcluster, linkage
from scipy.spatial.distance import squareform
from sklearn.cluster import AgglomerativeClustering
from sklearn.metrics import silhouette_score

HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent
CACHE = HERE / "cache"
sys.path.insert(0, str(REPO / "src"))

from fabfos.algorithm.fabfos_recovery import (  # noqa: E402
    AVA_HSP_FMT, _representatives, _similarity, blast_hsps, dedup_prep,
    read_blast_tsv, read_fasta, write_fasta,
)

sys.path.insert(0, str(REPO / "main" / "figures" / "inserts"))
import identity as identity_lib   # noqa: E402  -- the shared all-vs-all
import pieces as piece_lib        # noqa: E402  -- the shared piece rebuild

SUBSET = "pool01"
REF_IDS = (0.98, 0.99, 0.995)
CONTAINMENT = 0.99
FRAGMENT_CONTAINMENT = 0.90
CLOSURE_MARGIN = 0.95
LIB_DEFAULT = 0.99


def load_work(work):
    meta = json.loads((Path(work) / "piece_meta.json").read_text())
    seqs = {n: s for n, _d, s in read_fasta(Path(work) / "pooled.fna")}
    hsps = read_blast_tsv(Path(work) / "ava_hits.tsv", outfmt=AVA_HSP_FMT)
    return meta, seqs, hsps


def canonical_work(force=False):
    work = piece_lib.build(force=force)
    identity_lib.hits(force=force)
    return work


def groups_from(assignment, labels):
    g = {}
    for lab, c in zip(assignment, labels):
        g.setdefault(int(lab), []).append(c)
    return g


def resolve(member2centroid, reps):
    alive = set(reps)
    return {m: c for m, c in member2centroid.items() if c in alive}


def read_recall_tables(labels):
    d = CACHE / "fir_hitsets"
    if not d.is_dir():
        return None, None
    bit = {c: 1 << i for i, c in enumerate(labels)}
    tables, stats = {}, {}
    for f in sorted(d.glob("*.hitsets.tsv")):
        pool = f.name.split(".")[0]
        rows = []
        for line in f.read_text().splitlines():
            sig, cnt = line.rsplit("\t", 1)
            m = 0
            for k in sig.split(","):
                m |= bit.get(k, 0)
            if m:
                rows.append((m, int(cnt)))
        tables[pool] = rows
    for f in sorted(d.glob("*.stats.tsv")):
        pool = f.name.split(".")[0]
        stats[pool] = dict(
            (ln.split("\t")[0], int(ln.split("\t")[1]))
            for ln in f.read_text().splitlines() if "\t" in ln)
    return (tables, stats) if tables else (None, None)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--rebuild", action="store_true",
                    help="re-derive the piece set and the all-vs-all instead of "
                         "reusing the cached ones")
    ap.add_argument("--subset", default=SUBSET, help="piece-id pool prefix (default pool01)")
    a = ap.parse_args()

    CACHE.mkdir(parents=True, exist_ok=True)
    work = canonical_work(force=a.rebuild)
    meta, seqs, hsps = load_work(work)

    labels = sorted(k for k, v in meta.items() if v["piece"].startswith(a.subset + "_"))
    if not labels:
        raise SystemExit(f"no pieces with id prefix {a.subset!r} in {work}")
    index = {c: i for i, c in enumerate(labels)}
    n = len(labels)
    total_bp = sum(meta[c]["length"] for c in labels)
    print(f"{n:,} pieces from {a.subset} ({total_bp/1e6:.2f} Mbp) of {len(meta):,} total")

    sim = _similarity(hsps, index, "symmetric")
    cont = _similarity(hsps, index, "containment")

    def represent(groups):
        return _representatives(groups, meta, sim, cont, index,
                                closure_margin=CLOSURE_MARGIN,
                                containment=CONTAINMENT,
                                fragment_containment=FRAGMENT_CONTAINMENT,
                                absorb=True, verbose=False)

    refs = {}
    for R in REF_IDS:
        m = AgglomerativeClustering(metric="precomputed", linkage="complete",
                                    n_clusters=None, distance_threshold=1 - R)
        m.fit(1 - sim)
        m2c, reps, _abs = represent(groups_from(m.labels_, labels))
        refs[R] = sorted(reps)
        print(f"  reference R={R}: {len(reps)} clones "
              f"({sum(meta[c]['length'] for c in reps)/1e6:.2f} Mbp)")

    dist = 1.0 - sim
    np.fill_diagonal(dist, 0.0)
    Z = linkage(squareform(dist, checks=False), method="complete")

    reads, rstats = read_recall_tables(labels)
    bit = {c: 1 << i for i, c in enumerate(labels)}
    if reads:
        denom = sum(c for rows_ in reads.values() for _m, c in rows_)
        print(f"  read recall: {len(reads)} barcodes, {denom:,} pairs with a hit, "
              f"{sum(len(v) for v in reads.values()):,} distinct hit-sets")
        for p, s in sorted(rstats.items()):
            print(f"      {p}: max hit-set {s.get('max_hitset_size','?')}, "
                  f"pairs at the -N cap {s.get('pairs_at_N_cap','?')}")
    else:
        denom = 0
        print("  read recall: no cache/fir_hitsets/ -- run "
              "tests/recall_map_on_fir.sh submit|fetch, then re-run this script")

    rows, partitions = [], {}
    for h in np.unique(Z[:, 2]):
        assignment = fcluster(Z, h, criterion="distance")
        k = len(set(assignment))
        if k < 2 or k >= n:
            continue
        m2c, reps, _abs = represent(groups_from(assignment, labels))
        survivors = set(reps)
        row = dict(k=k, identity=float(1.0 - h),
                   silhouette=float(silhouette_score(dist, assignment,
                                                     metric="precomputed")),
                   n_inserts=len(reps),
                   bases=sum(meta[c]["length"] for c in reps))
        row["base_frac"] = row["bases"] / total_bp
        for R in REF_IDS:
            hit = {m2c[c] for c in refs[R] if c in m2c and m2c[c] in survivors}
            row[f"recall_{R:g}"] = len(hit) / len(refs[R])
        if reads:
            mask = 0
            for c in reps:
                mask |= bit[c]
            kept = sum(cnt for rows_ in reads.values() for m, cnt in rows_ if m & mask)
            row["read_recall"] = kept / denom
        rows.append(row)
        partitions[f"{k}"] = sorted(reps)

    rows.sort(key=lambda r: r["k"])
    cols = (["k", "identity", "silhouette", "n_inserts", "bases", "base_frac"]
            + [f"recall_{R:g}" for R in REF_IDS]
            + (["read_recall"] if reads else []))
    tsv = CACHE / "recall_sweep.tsv"
    with open(tsv, "w") as fh:
        fh.write("\t".join(cols) + "\n")
        for r in rows:
            fh.write("\t".join(
                f"{r[c]:.6f}" if isinstance(r[c], float) else str(r[c]) for c in cols) + "\n")
    print(f"  {len(rows)} partitions -> {tsv.relative_to(REPO)}")

    write_fasta(CACHE / "day6_pieces.fna",
                [(c, f"piece={meta[c]['piece']} length={meta[c]['length']}", seqs[c])
                 for c in labels])
    with open(CACHE / "day6_pieces.tsv", "w") as fh:
        fh.write("key\tpiece\tlength\tclosed_ends\n")
        for c in labels:
            fh.write(f"{c}\t{meta[c]['piece']}\t{meta[c]['length']}\t"
                     f"{meta[c]['closed_ends']}\n")
    (CACHE / "day6_partitions.json").write_text(json.dumps(
        dict(subset=a.subset, labels=labels, refs={f"{R:g}": v for R, v in refs.items()},
             partitions=partitions)))
    print(f"  wrote day6_pieces.fna ({n} pieces), day6_pieces.tsv, day6_partitions.json")

    peak = max(rows, key=lambda r: r["silhouette"])
    print(f"\n  silhouette peak: N={peak['k']} identity {peak['identity']:.4f} "
          f"silhouette {peak['silhouette']:.4f} -> {peak['n_inserts']} inserts")
    for R in REF_IDS:
        print(f"      clone recall at that peak, R={R:g}: {peak[f'recall_{R:g}']:.4f}")
    if peak["k"] in (rows[0]["k"], rows[-1]["k"]):
        print("      WARNING: the optimum is at the edge of the partition range")

    if reads:
        print(f"      read recall at that peak: {peak['read_recall']:.4f}")

    criteria = [(f"recall_{R:g}", f"clone R={R:g}") for R in REF_IDS]
    if reads:
        criteria.append(("read_recall", "read"))
    for col, name in criteria:
        ok = [r for r in rows if r[col] >= 0.99]
        if not ok:
            print(f"  no cut reaches 0.99 under {name} recall")
            continue
        best = max(ok, key=lambda r: r["silhouette"])
        print(f"  best cut with {name} recall >= 0.99: N={best['k']} "
              f"identity {best['identity']:.4f} silhouette {best['silhouette']:.4f} "
              f"-> {best['n_inserts']} inserts "
              f"(silhouette cost {peak['silhouette'] - best['silhouette']:.4f})")

    near = min(rows, key=lambda r: abs(r["identity"] - LIB_DEFAULT))
    print(f"  library default identity {LIB_DEFAULT:g} lands at N={near['k']} "
          f"(identity {near['identity']:.4f}) silhouette {near['silhouette']:.4f}, "
          + ", ".join(f"{name} {near[col]:.4f}" for col, name in criteria))


if __name__ == "__main__":
    sys.exit(main())

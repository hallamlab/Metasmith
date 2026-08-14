"""Compare the layouts one sweep's channels produce, against each other and against KEGG.

Two numbers, both about the neighbour graph UMAP actually consumed rather than a matrix
re-derived here -- if two channels disagree, they disagree in the k nearest neighbours or
nowhere, and that is the object the layout is a projection of:

* **scatter index**, read from each layout's ``--metrics-out``: mean pairwise 2D distance
  within a KEGG pathway over the same among random equal-size sets. 1.0 is randomly placed.
  This is the verdict.
* **top-k neighbour overlap** between every pair of channels, from the ``--knn-out`` files:
  the mean over reactions of ``|N_a(r) & N_b(r)| / k``. This is what says how much of a
  scatter-index difference is a genuinely different neighbourhood rather than the same
  neighbourhood embedded differently.

    python ieff_channel_compare.py --knn cache/channels/knn_*.npz \
        --metrics cache/channels/metrics_*.json --out cache/channels/compare.json
"""
import argparse
import json
from pathlib import Path

import numpy as np


def overlap(a, b):
    """Mean fraction of shared neighbours per row, over two (n, k) neighbour tables."""
    if a.shape != b.shape:
        k = min(a.shape[1], b.shape[1])
        a, b = a[:, :k], b[:, :k]
    n, k = a.shape
    return float(np.mean([len(set(a[i]) & set(b[i])) / k for i in range(n)]))


def tag(path):
    """``knn_power_leak1e-2.npz`` -> ``power_leak1e-2``."""
    s = Path(path).stem
    for p in ("knn_", "metrics_"):
        if s.startswith(p):
            s = s[len(p):]
    return s


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--knn", type=Path, nargs="+", required=True)
    ap.add_argument("--metrics", type=Path, nargs="*", default=[])
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()

    knn = {}
    for p in args.knn:
        z = np.load(p, allow_pickle=False)
        knn[tag(p)] = np.asarray(z["idx"])
    names = sorted(knn)
    print(f"{len(names)} neighbour graphs, {knn[names[0]].shape}")

    ov = {}
    for i, a in enumerate(names):
        for b in names[i + 1:]:
            ov[f"{a} | {b}"] = overlap(knn[a], knn[b])
    print("\ntop-k neighbour overlap (1.000 = the same neighbourhoods):")
    for kk, v in sorted(ov.items(), key=lambda x: -x[1]):
        print(f"  {v:.3f}  {kk}")

    scat = {}
    for p in args.metrics:
        d = json.load(open(p))
        scat[tag(p)] = dict(mean=d["mean_scatter"], per_pathway=d["scatter"])
    if scat:
        print("\nmean KEGG-pathway scatter index (lower = pathways hold together):")
        for kk, v in sorted(scat.items(), key=lambda x: x[1]["mean"]):
            print(f"  {v['mean']:.3f}  {kk}")

    args.out.parent.mkdir(parents=True, exist_ok=True)
    json.dump(dict(overlap=ov, scatter=scat), open(args.out, "w"), indent=1)
    print(f"\nwrote {args.out}")


if __name__ == "__main__":
    main()

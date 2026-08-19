import argparse
import json
from pathlib import Path

import numpy as np


def overlap(a, b):
    if a.shape != b.shape:
        k = min(a.shape[1], b.shape[1])
        a, b = a[:, :k], b[:, :k]
    n, k = a.shape
    return float(np.mean([len(set(a[i]) & set(b[i])) / k for i in range(n)]))


def tag(path):
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

"""Cluster genes by how they move together across a count table's conditions.

    expression_clusters.py <counts.csv> <params.json> <out.tsv>

Genes that move together are the operon and the regulon, which per-gene testing
reports as unrelated hits. The distance is 1 - Pearson correlation across the
columns, so two genes are close when their profiles have the same shape, not the
same magnitude.

`params.json` is the method knob: `{"method": "hierarchical"|"kmeans", "k": <int|null>}`.
KBase ships four apps here -- hierarchical, k-means, WGCNA and an estimate of k --
which is one transform with a knob rather than four transforms. `k: null` estimates
it by silhouette over 2..12. WGCNA is not ported: it is R, and its output is a
module assignment this file's two methods already produce.
"""
import json
import sys

import numpy as np
import pandas as pd
from scipy.cluster.hierarchy import fcluster
from sklearn.metrics import silhouette_score

from hierarchical_clustering import HierarchicalCluster

K_RANGE = range(2, 13)


def correlation_distance(mat: np.ndarray) -> np.ndarray:
    # A gene that never moves has no correlation with anything; nan_to_num sends
    # it to distance 1 from every other gene rather than dropping the row, so the
    # output still carries one line per input gene.
    with np.errstate(invalid="ignore", divide="ignore"):
        corr = np.corrcoef(mat)
    dist = np.nan_to_num(1.0 - corr, nan=1.0)
    # `squareform` rejects a matrix that is asymmetric by any amount at all, and
    # np.corrcoef is symmetric only up to floating-point rounding. Averaging with
    # the transpose is the standard repair; zeroing the diagonal is the other half
    # of what it checks.
    dist = (dist + dist.T) / 2.0
    np.fill_diagonal(dist, 0.0)
    return dist


def choose_k(dist: np.ndarray, assign) -> int:
    best_k, best_score = 2, -1.0
    for k in K_RANGE:
        if k >= dist.shape[0]:
            break
        labels = assign(k)
        if len(set(labels)) < 2:
            continue
        score = silhouette_score(dist, labels, metric="precomputed")
        if score > best_score:
            best_k, best_score = k, score
    return best_k


def main(argv: list[str]) -> int:
    counts_path, params_path, out_path = argv
    with open(params_path) as fh:
        params = json.load(fh)
    method = params.get("method", "hierarchical")
    k = params.get("k")

    counts = pd.read_csv(counts_path, index_col=0)
    genes = list(counts.index)
    mat = counts.to_numpy(dtype=float)
    assert mat.shape[0] >= 2, f"[{counts_path}] has {mat.shape[0]} genes; nothing to cluster"
    dist = correlation_distance(mat)

    if method == "hierarchical":
        result = HierarchicalCluster(dist, labels=genes, method="average", metric="precomputed")
        assign = lambda n: list(fcluster(result.linkage, t=n, criterion="maxclust"))
    elif method == "kmeans":
        from sklearn.cluster import KMeans
        assign = lambda n: list(KMeans(n_clusters=n, n_init=10, random_state=42).fit_predict(mat))
    else:
        raise ValueError(f"unknown method [{method}]; this ports hierarchical and kmeans")

    if k is None:
        k = choose_k(dist, assign)
        print(f"k not given; silhouette over {K_RANGE.start}..{K_RANGE.stop - 1} chose k={k}")
    labels = assign(int(k))

    frame = pd.DataFrame({"gene": genes, "cluster": labels})
    frame.to_csv(out_path, sep="\t", index=False)
    sizes = frame["cluster"].value_counts().sort_index().to_dict()
    print(f"{len(genes)} genes, method={method}, k={k}, cluster sizes {sizes} -> {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

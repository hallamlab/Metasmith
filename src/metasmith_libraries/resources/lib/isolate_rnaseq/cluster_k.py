"""Choose the number of dose-response clusters from evidence, and score each gene's membership.

    cluster_k.py <trend.csv> <out_dir> [--drop-dose <label>] [--iterations 500] [--nulls 200]

Reads the `normalised_count_<dose>` columns of a dose-trend table. Takes the 500 most variable
expressed genes, z-scored across the doses, so clusters group response shapes, not levels.

For k = 2..12, under both average-linkage hierarchical clustering on correlation distance and
k-means:
  * consensus clustering (Monti et al. 2003): re-cluster a random 80% of genes per iteration,
    record how often each pair lands together, and report PAC, the share of gene pairs whose
    co-clustering rate is ambiguous, between 0.1 and 0.9 (Senbabaoglu et al. 2014);
  * gap statistic (Tibshirani et al. 2001), with k = 1 included, against null data made by
    shuffling each gene's values across the doses;
  * mean silhouette on correlation distance.

The rule, fixed before any result: k is the smallest k whose gap is within one standard error
of the largest gap. PAC is reported beside it, and a disagreement is reported, not resolved.
At the chosen k each gene gets a confidence: its mean co-clustering rate with the rest of its
cluster. Below 0.8 it is flagged unstable.
"""
import argparse
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy.cluster.hierarchy import fcluster, linkage
from scipy.spatial.distance import pdist, squareform
from sklearn.cluster import KMeans
from sklearn.metrics import adjusted_rand_score, silhouette_score

N_GENES = 500
MIN_MEAN = 20
K_RANGE = range(2, 13)
SUBSAMPLE = 0.8
STABLE = 0.8
SEED = 20261029


def profiles(trend: pd.DataFrame, drop: list[str]) -> pd.DataFrame:
    cols = [c for c in trend.columns if c.startswith("normalised_count_")
            and c.removeprefix("normalised_count_") not in drop]
    normed = trend[cols]
    normed.columns = [c.removeprefix("normalised_count_") for c in cols]
    logn = np.log2(normed + 1)
    expressed = logn[normed.mean(axis=1) >= MIN_MEAN]
    top = expressed.loc[expressed.var(axis=1).sort_values(ascending=False).index[:N_GENES]]
    return top.sub(top.mean(axis=1), axis=0).div(top.std(axis=1), axis=0)


# Every row is z-scored, so squared Euclidean distance is 2(n-1)(1 - r): k-means on these rows
# clusters on the same correlation distance the hierarchical method uses.
def cluster(z: np.ndarray, k: int, method: str, rng, n_init: int) -> np.ndarray:
    if k == 1:
        return np.ones(len(z), dtype=int)
    if method == "hierarchical":
        return fcluster(linkage(z, method="average", metric="correlation"), k, criterion="maxclust")
    return KMeans(n_clusters=k, n_init=n_init, random_state=int(rng.integers(1 << 31))).fit_predict(z) + 1


def within_ss(z: np.ndarray, labels: np.ndarray) -> float:
    return sum(((z[labels == c] - z[labels == c].mean(axis=0)) ** 2).sum() for c in np.unique(labels))


def consensus(z: np.ndarray, k: int, method: str, iterations: int, rng) -> np.ndarray:
    n = len(z)
    together = np.zeros((n, n))
    sampled = np.zeros((n, n))
    for _ in range(iterations):
        idx = rng.choice(n, int(SUBSAMPLE * n), replace=False)
        labels = cluster(z[idx], k, method, rng, n_init=10)
        same = (labels[:, None] == labels[None, :]).astype(float)
        together[np.ix_(idx, idx)] += same
        sampled[np.ix_(idx, idx)] += 1
    return np.divide(together, sampled, out=np.zeros_like(together), where=sampled > 0)


def gap(z: np.ndarray, method: str, nulls: int, rng) -> pd.DataFrame:
    ks = [1, *K_RANGE]
    observed = [np.log(within_ss(z, cluster(z, k, method, rng, n_init=100))) for k in ks]
    reference = np.empty((nulls, len(ks)))
    for b in range(nulls):
        null = rng.permuted(z, axis=1)
        reference[b] = [np.log(within_ss(null, cluster(null, k, method, rng, n_init=20))) for k in ks]
    return pd.DataFrame({
        "k": ks,
        "gap": reference.mean(axis=0) - observed,
        "gap_se": reference.std(axis=0) * np.sqrt(1 + 1 / nulls),
    }).set_index("k")


def choose(g: pd.DataFrame) -> tuple[int, int]:
    best = g.gap.idxmax()
    global_se = int(g.index[g.gap >= g.gap[best] - g.gap_se[best]].min())
    ks = list(g.index)
    tibs = next((k for k, nxt in zip(ks, ks[1:]) if g.gap[k] >= g.gap[nxt] - g.gap_se[nxt]), ks[-1])
    return global_se, int(tibs)


def pac(m: np.ndarray) -> float:
    upper = m[np.triu_indices_from(m, k=1)]
    return float(((upper > 0.1) & (upper < 0.9)).mean())


def select(z: pd.DataFrame, iterations: int, nulls: int, rng) -> tuple[pd.DataFrame, dict, dict]:
    dist = squareform(pdist(z.values, metric="correlation"))
    rows, matrices, choices = [], {}, {}
    for method in ("hierarchical", "kmeans"):
        g = gap(z.values, method, nulls, rng)
        choices[method] = choose(g)
        for k in [1, *K_RANGE]:
            row = {"method": method, "k": k, "gap": g.gap[k], "gap_se": g.gap_se[k]}
            if k > 1:
                labels = cluster(z.values, k, method, rng, n_init=100)
                m = consensus(z.values, k, method, iterations, rng)
                matrices[(method, k)] = m
                row |= {"silhouette": silhouette_score(dist, labels, metric="precomputed"), "PAC": pac(m)}
            rows.append(row)
            print(f"  {method} k={k}: " + ", ".join(f"{a}={b:.3f}" for a, b in row.items() if a not in ("method", "k")),
                  flush=True)
    return pd.DataFrame(rows), matrices, choices


def members(z: pd.DataFrame, m: np.ndarray, k: int) -> pd.DataFrame:
    # Final clusters come from the consensus matrix itself, as in ConsensusClusterPlus, so a
    # gene sits with the genes it joined most often across the iterations.
    labels = fcluster(linkage(squareform(1 - m, checks=False), method="average"), k, criterion="maxclust")
    confidence = np.array([m[i, (labels == labels[i]) & (np.arange(len(m)) != i)].mean()
                           if (labels == labels[i]).sum() > 1 else np.nan for i in range(len(m))])
    return pd.DataFrame({"cluster": labels, "membership_confidence": confidence.round(3),
                         "stable": confidence >= STABLE}, index=z.index)


def plot_selection(table: pd.DataFrame, matrices: dict, choices: dict, title: str, out: Path) -> None:
    fig, axes = plt.subplots(1, 4, figsize=(17, 4))
    colours = {"hierarchical": "#2e5fb0", "kmeans": "#c0392b"}
    for method, t in table.groupby("method"):
        c = colours[method]
        axes[0].errorbar(t.k, t.gap, yerr=t.gap_se, marker="o", ms=4, c=c, capsize=2,
                         label=f"{method} (chosen k={choices[method][0]})")
        t2 = t[t.k > 1]
        axes[1].plot(t2.k, t2.PAC, marker="o", ms=4, c=c, label=method)
        axes[2].plot(t2.k, t2.silhouette, marker="o", ms=4, c=c, label=method)
    for k in K_RANGE:
        upper = matrices[("hierarchical", k)][np.triu_indices(len(matrices[("hierarchical", k)]), k=1)]
        xs = np.sort(upper)
        axes[3].plot(xs, np.arange(1, len(xs) + 1) / len(xs), lw=1, c=plt.cm.viridis((k - 2) / 10), label=f"k={k}")
    axes[0].set(title="Gap statistic (+/- 1 SE)", xlabel="k", ylabel="gap")
    axes[1].set(title="PAC, lower = more stable", xlabel="k", ylabel="PAC")
    axes[2].set(title="Mean silhouette", xlabel="k", ylabel="silhouette")
    axes[3].set(title="Consensus CDF (hierarchical)", xlabel="co-clustering rate", ylabel="CDF")
    for ax in axes[:3]:
        ax.legend(fontsize=8)
        ax.set_xticks([1, *K_RANGE])
    axes[3].legend(fontsize=6, ncol=2)
    fig.suptitle(title)
    fig.tight_layout()
    fig.savefig(out, dpi=200)
    plt.close(fig)


def plot_profiles(z: pd.DataFrame, assigned: pd.DataFrame, title: str, out: Path) -> None:
    k = assigned.cluster.max()
    cols = int(np.ceil(np.sqrt(k)))
    rows = int(np.ceil(k / cols))
    fig, axes = plt.subplots(rows, cols, figsize=(3.6 * cols, 2.8 * rows), sharex=True, sharey=True, squeeze=False)
    x = np.arange(z.shape[1])
    for c, ax in zip(range(1, k + 1), axes.flat):
        group = assigned[assigned.cluster == c]
        unstable = z.loc[group.index[~group.stable]]
        stable = z.loc[group.index[group.stable]]
        ax.plot(x, unstable.T.values, c="#d9dce3", lw=0.4)
        ax.plot(x, stable.T.values, c="#8792a8", lw=0.4, alpha=0.6)
        ax.plot(x, z.loc[group.index].mean().values, c="#c0392b", lw=2)
        ax.set_title(f"cluster {c}: {len(group)} genes, {int(group.stable.sum())} stable", fontsize=9)
        ax.set_xticks(x, [c.removesuffix("ppm") for c in z.columns])
    for ax in axes.flat[k:]:
        ax.axis("off")
    for ax in axes[-1]:
        ax.set_xlabel("CuSO4 (ppm)")
    for ax in axes[:, 0]:
        ax.set_ylabel("z-score")
    fig.suptitle(title)
    fig.tight_layout()
    fig.savefig(out, dpi=200)
    plt.close(fig)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("trend")
    ap.add_argument("out")
    ap.add_argument("--drop-dose", action="append", default=[])
    ap.add_argument("--iterations", type=int, default=500)
    ap.add_argument("--nulls", type=int, default=200)
    args = ap.parse_args()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    tag = "_without_" + "_".join(args.drop_dose) if args.drop_dose else ""
    rng = np.random.default_rng(SEED)

    trend = pd.read_csv(args.trend, index_col=0)
    z = profiles(trend, args.drop_dose)
    print(f"{len(z)} genes x {z.shape[1]} doses{tag.replace('_', ' ')}", flush=True)
    table, matrices, choices = select(z, args.iterations, args.nulls, rng)
    table.round(4).to_csv(out / f"k_selection{tag}.csv", index=False)

    k, tibs = choices["hierarchical"]
    km_k, km_tibs = choices["kmeans"]
    hier = table[(table.method == "hierarchical") & (table.k > 1)].set_index("k")
    pac_best = int(hier.PAC.idxmin())
    lines = [
        f"genes: {len(z)}; doses: {', '.join(z.columns)}; iterations per k: {args.iterations}; null datasets: {args.nulls}; seed {SEED}",
        f"hierarchical: gap rule (smallest k within 1 SE of the max gap) -> k={k}; Tibshirani 2001 first-SE rule -> k={tibs}",
        f"k-means:      gap rule -> k={km_k}; Tibshirani 2001 first-SE rule -> k={km_tibs}",
        f"hierarchical PAC is lowest at k={pac_best} ({hier.PAC[pac_best]:.3f}); at the chosen k it is "
        + (f"{hier.PAC[k]:.3f}" if k > 1 else "n/a"),
    ]
    if k == 1:
        lines.append("the gap statistic finds no cluster structure beyond the null: no clusters are reported")
    else:
        agreement = adjusted_rand_score(cluster(z.values, k, "hierarchical", rng, 100),
                                        cluster(z.values, k, "kmeans", rng, 100))
        lines.append(f"agreement of hierarchical and k-means at k={k}: adjusted Rand index {agreement:.2f}")
        if pac_best != k or km_k != k:
            lines.append("THE METRICS DISAGREE on k; read the selection figure before trusting the clusters")
        assigned = members(z, matrices[("hierarchical", k)], k)
        assigned.insert(0, "gene_name", trend.gene_name.reindex(assigned.index))
        assigned.to_csv(out / f"clusters{tag}.csv")
        sizes = assigned.groupby("cluster").agg(genes=("stable", "size"), stable=("stable", "sum"),
                                                median_confidence=("membership_confidence", "median"))
        lines.append("clusters (genes, stable at confidence >= 0.8, median confidence):")
        lines += [f"  {c}: {r.genes}, {r.stable}, {r.median_confidence:.2f}" for c, r in sizes.iterrows()]
        plot_profiles(z, assigned, f"Dose-response clusters at k={k} (grey: confidence < {STABLE})",
                      out / f"cluster_profiles{tag}.png")
    plot_selection(table, matrices, choices, f"Choosing k for {len(z)} dose-response profiles{tag.replace('_', ' ')}",
                   out / f"k_selection{tag}.png")
    (out / f"k_selection_summary{tag}.txt").write_text("\n".join(lines) + "\n")
    print("\n".join(lines))


if __name__ == "__main__":
    main()

"""One-to-one vOTU pairing and precomputed violin summaries, shared by the E3 and E5 scorers."""
import numpy as np
import pandas as pd
from scipy.sparse import coo_matrix
from scipy.sparse.csgraph import min_weight_full_bipartite_matching


def pair(edges, n, m):
    """One-to-one assignment maximising summed identity, so no metasmith vOTU stands in for two published ones
    (sparse Jonker-Volgenant, https://doi.org/10.1007/BF02278710). Each published vOTU gets a private dummy
    column at a weight below any real pair, which makes a full matching of the rows exist and lets a vOTU stay unpaired."""
    rows = np.concatenate([edges.i.to_numpy(), np.arange(n)])
    cols = np.concatenate([edges.j.to_numpy(), m + np.arange(n)])
    vals = np.concatenate([edges.identity.to_numpy() / 100, np.full(n, 1e-6)])
    graph = coo_matrix((vals, (rows, cols)), shape=(n, m + n)).tocsr()
    _, col_of = min_weight_full_bipartite_matching(graph, maximize=True)
    paired_rows = np.flatnonzero(col_of < m)
    pairs = pd.DataFrame({"i": paired_rows, "j": col_of[paired_rows]})
    paired = pairs.merge(edges[["i", "j", "ani", "af_pub", "af_ours", "identity"]], on=["i", "j"]).set_index("i")
    assert len(paired) == len(pairs)
    return paired


def violin(values, grid, bandwidth):
    values = np.asarray(values, dtype=float)
    step = grid[1] - grid[0]
    hist, _ = np.histogram(np.clip(values, grid[0], grid[-1]), bins=np.append(grid - step / 2, grid[-1] + step / 2))
    half = int(4 * bandwidth)
    kernel = np.exp(-0.5 * (np.arange(-half, half + 1) / bandwidth) ** 2)
    dens = np.convolve(hist, kernel / kernel.sum(), mode="same")
    q1, med, q3 = np.percentile(values, [25, 50, 75])
    # Whiskers reach the most extreme value within 3 SD of the mean.
    inside = np.abs(values - values.mean()) <= 3 * values.std()
    return {"n": int(len(values)), "density": np.round(dens / dens.max(), 4).tolist(),
            "q1": round(q1, 2), "median": round(med, 2), "q3": round(q3, 2),
            "lo": round(values[inside].min(), 2), "hi": round(values[inside].max(), 2),
            "below": int((values < grid[0]).sum())}

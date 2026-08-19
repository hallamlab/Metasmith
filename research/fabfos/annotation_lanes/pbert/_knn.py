"""kNN retrieval over the Swiss-Prot label pool, with the retrieval metric as the knob.

WHAT VARIES AND WHAT DOES NOT. The lane does two distinct things with distance:
it *retrieves* K neighbours, and it *weights* their votes. Only the first is the
question here, so the panel swaps the retrieval metric and holds the weighting at
the shipped rule -- cosine, negatives clipped to zero, normalised within the top-K
(gpr_4lane.py's `w = vals / vals.sum()`). With cosine retrieval this reduces
exactly to the deployed lane, so the incumbent is a row in its own panel rather
than a separate code path. It also keeps the weighting defined for metrics whose
similarity is unbounded (dot) or always negative (euclidean), where "clip at zero"
has no meaning.

THE GATE is the retrieval metric's own top-1 value -- `nn_similarity` in the lane's
terms. Its scale is metric-specific, which is why the threshold sweep drives it off
an observed quantile grid rather than a fixed ladder.

Metrics. All but L1 are one matmul on a transformed pool, fitted on the pool alone:
  cosine       L2-normalise both sides                     (the incumbent)
  dot          raw inner product; magnitude-sensitive
  euclidean    -||q-r||, via ||q||^2 + ||r||^2 - 2q.r
  correlation  mean-centre each vector, then cosine
  zscore       per-dimension standardise on pool stats, then cosine
  whiten       PCA-whiten on pool covariance, then cosine
  l1           -||q-r||_1; no BLAS form, query-subsampled
"""
from __future__ import annotations

import numpy as np

K = 30
KWIDE = 200   # retrieved before exclusions, so a condition can drop rows and still fill K
CHUNK = 256


def _norm(x):
    n = np.linalg.norm(x, axis=1, keepdims=True)
    return x / np.clip(n, 1e-9, None)


class Metric:
    """A retrieval metric as a pair of pool-fitted transforms plus a similarity kind."""

    def __init__(self, name, q, r, kind="dot", sq_r=None):
        self.name, self.q, self.r, self.kind, self.sq_r = name, q, r, kind, sq_r

    def sim(self, qb):
        s = qb @ self.r.T
        if self.kind == "euclidean":
            # -(||q||^2 + ||r||^2 - 2q.r); the per-query ||q||^2 is dropped, it is
            # constant within a row and this value is only ever compared row-wise.
            s = 2.0 * s - self.sq_r
        return s


def build_metrics(pool: np.ndarray, query: np.ndarray, names=None) -> dict:
    """Return {name: Metric} with every transform fitted on the pool only."""
    out = {}
    want = set(names) if names else None

    def add(n, qf, rf, kind="dot", sq=None):
        if want is None or n in want:
            out[n] = Metric(n, qf, rf, kind, sq)

    add("cosine", _norm(query), _norm(pool))
    add("dot", query, pool)
    add("euclidean", query, pool, "euclidean", (pool ** 2).sum(1).astype(np.float32))
    mu = pool.mean(1, keepdims=True)
    add("correlation", _norm(query - query.mean(1, keepdims=True)), _norm(pool - mu))
    m, s = pool.mean(0), pool.std(0)
    s = np.clip(s, 1e-9, None)
    add("zscore", _norm((query - m) / s), _norm((pool - m) / s))
    if want is None or "whiten" in want:
        c = pool - m
        cov = (c.T @ c) / (len(c) - 1)
        w, v = np.linalg.eigh(cov.astype(np.float64))
        W = (v / np.sqrt(np.clip(w, 1e-6, None))).astype(np.float32)
        add("whiten", _norm((query - m) @ W), _norm(c @ W))
    return out


def topk(metric: Metric, k=KWIDE, mask_cols=None):
    """Top-k over the pool per query, by this metric's similarity.

    Wide by default: exclusions are applied afterwards by `refine`, so one
    retrieval serves every leakage condition.
    Returns (idx (N,k) int32, vals (N,k) float32) sorted by descending similarity.
    """
    n = len(metric.q)
    I = np.empty((n, k), np.int32)
    V = np.empty((n, k), np.float32)
    for s in range(0, n, CHUNK):
        e = min(s + CHUNK, n)
        sim = metric.sim(metric.q[s:e]).astype(np.float32)
        if mask_cols is not None:
            for i in range(s, e):
                c = mask_cols[i]
                if c >= 0:
                    sim[i - s, c] = -np.inf
        part = np.argpartition(-sim, k, axis=1)[:, :k]
        pv = np.take_along_axis(sim, part, 1)
        order = np.argsort(-pv, axis=1)
        I[s:e] = np.take_along_axis(part, order, 1)
        V[s:e] = np.take_along_axis(pv, order, 1)
    return I, V


def topk_l1(pool, query, k=KWIDE, qchunk=4):
    n = len(query)
    I = np.empty((n, k), np.int32)
    V = np.empty((n, k), np.float32)
    for s in range(0, n, qchunk):
        e = min(s + qchunk, n)
        d = np.abs(query[s:e, None, :] - pool[None, :, :]).sum(-1)
        sim = (-d).astype(np.float32)
        part = np.argpartition(-sim, k, axis=1)[:, :k]
        pv = np.take_along_axis(sim, part, 1)
        order = np.argsort(-pv, axis=1)
        I[s:e] = np.take_along_axis(part, order, 1)
        V[s:e] = np.take_along_axis(pv, order, 1)
    return I, V


def refine(idx, cos, k=K, drop_col=None, twin_cut=None):
    """Apply a leakage condition to a wide retrieval and return the surviving top-k.

    drop_col[i] is a pool row to hide for query i (its own Swiss-Prot accession);
    twin_cut hides every neighbour at or above that cosine (an exact-twin cut, the
    cheap stand-in for the source study's DIAMOND cluster removal).
    Returns (idx (N,k), cos (N,k)) padded with -1 / -inf where fewer than k survive.
    """
    n = len(idx)
    I = np.full((n, k), -1, np.int32)
    C = np.full((n, k), -np.inf, np.float32)
    for i in range(n):
        keep = np.ones(idx.shape[1], bool)
        if drop_col is not None and drop_col[i] >= 0:
            keep &= idx[i] != drop_col[i]
        if twin_cut is not None:
            keep &= cos[i] < twin_cut
        ii, cc = idx[i][keep][:k], cos[i][keep][:k]
        I[i, :len(ii)] = ii
        C[i, :len(cc)] = cc
    return I, C


def vote(idx, cos_of_neighbours, label_lists, floor):
    """The shipped vote: clip cosine at 0, normalise within top-K, keep labels >= floor.

    `cos_of_neighbours` is the cosine similarity of each retrieved neighbour -- the
    weighting stays cosine whatever metric did the retrieving.
    Returns a list of {label: vote} dicts, one per query.
    """
    out = []
    for i in range(len(idx)):
        live = idx[i] >= 0
        vals = np.clip(np.where(live, cos_of_neighbours[i], 0.0), 0, None)
        tot = vals.sum()
        if tot <= 0:
            out.append({})
            continue
        w = vals / tot
        acc: dict[str, float] = {}
        for wi, ri in zip(w, idx[i]):
            if wi <= 0 or ri < 0:
                continue
            for lab in label_lists[ri]:
                acc[lab] = acc.get(lab, 0.0) + wi
        out.append({k_: min(v, 1.0) for k_, v in acc.items() if v >= floor})
    return out

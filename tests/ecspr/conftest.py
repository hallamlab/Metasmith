import numpy as np
import pytest

from ecspr.model.graph import AtomGraph

SELFTEST_TOL = 1e-9


def reff_dense(G, s, t, wk=None) -> float:
    nodes = list(G.nodes); idx = {n: i for i, n in enumerate(nodes)}; n = len(nodes)
    L = np.zeros((n, n))
    for u, v, d in G.edges(data=True):
        w = 1.0 if wk is None else float(d.get(wk, 0.0))
        if w <= 0:
            continue
        i, j = idx[u], idx[v]
        L[i, i] += w; L[j, j] += w; L[i, j] -= w; L[j, i] -= w
    b = np.zeros(n - 1)
    if idx[s] < n - 1:
        b[idx[s]] += 1.0
    if idx[t] < n - 1:
        b[idx[t]] -= 1.0
    phi = np.zeros(n)
    phi[:-1] = np.linalg.solve(L[:-1, :-1], b)
    return float(phi[idx[s]] - phi[idx[t]])


def toy_graph(ratio=1.0) -> AtomGraph:
    rec = []

    def e(a, b, g):
        rec.append((a, b, g, ratio * g))

    e(("m0", 0), ("m1", 0), 1.0)
    e(("m0", 1), ("m1", 1), 1.0)
    e(("m1", 0), ("m1", 1), 0.3)
    e(("m1", 0), ("p1", 0), 2.0)
    e(("m1", 1), ("p2", 0), 0.5)
    e(("m1", 1), ("p2", 1), 0.25)
    e(("m1", 0), ("d0", 0), 0.7)
    e(("z0", 0), ("z0", 1), 1.0)
    return AtomGraph.from_edge_records(rec, dict(kind="toy"))


@pytest.fixture
def toy():
    return toy_graph(1.0)

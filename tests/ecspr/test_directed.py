"""The rectified-diode Newton solve, against independent referents.

Lifted out of ``ecspr_directed.py``'s ``_selftest_*`` block. Two gates matter here:
the Newton answer against a dense L-BFGS-B minimisation of the same convex energy
(shares no code path), and the symmetric limit against the plain undirected solve
(the property that says a directed model degrades to the undirected one exactly
where direction is unknown).
"""
import networkx as nx
import numpy as np
import pytest

from ecspr.model.directed import _directed_ceff_dense, build_incidence, directed_ceff

from conftest import SELFTEST_TOL, reff_dense


def toy_diode_nets():
    """Small (edges, n, gp, gm, s, t) diode networks. Orientation and ratios are
    varied so the active set is nontrivial."""
    nets = []
    # 4-node diamond: two parallel routes, one throttled backward.
    nets.append(([(0, 1), (0, 2), (1, 3), (2, 3)], 4,
                 np.array([1.0, 1.0, 1.0, 1.0]), np.array([1.0, 0.02, 0.5, 0.02]), 0, 3))
    # 5-node chain with a back-edge, mixed ratios.
    nets.append(([(0, 1), (1, 2), (2, 3), (3, 4), (4, 1)], 5,
                 np.array([2.0, 1.0, 1.5, 1.0, 0.7]),
                 np.array([0.02, 1.0, 0.03, 0.5, 0.01]), 0, 4))
    # 6-node grid-ish, several irreversible.
    nets.append(([(0, 1), (1, 2), (0, 3), (3, 4), (4, 2), (2, 5), (1, 4)], 6,
                 np.array([1.0, 1.2, 0.8, 1.0, 1.0, 0.6, 0.9]),
                 np.array([0.01, 0.01, 1.0, 0.02, 1.0, 0.02, 0.5]), 0, 5))
    return nets


@pytest.mark.parametrize("k", range(3))
def test_newton_matches_dense_referent(k):
    edges, n, gp, gm, s, t = toy_diode_nets()[k]
    B = build_incidence(edges, n)
    c_newt = directed_ceff(B, gp, gm, s, t)
    c_dense = _directed_ceff_dense(B, gp, gm, s, t)
    assert abs(c_newt - c_dense) < 1e-8


def test_symmetric_limit_matches_undirected():
    """At g- = g+ the directed solve must reproduce the undirected R_eff."""
    G = nx.Graph()
    G.add_nodes_from([("met", f"m{i}") for i in range(6)])
    G.add_nodes_from([("rxn", f"r{i}") for i in range(5)])
    edges = [(0, 0), (0, 1), (1, 1), (1, 2), (2, 2), (2, 3),
             (3, 3), (3, 4), (4, 4), (4, 5), (4, 0)]
    nx_edges = [(("rxn", f"r{r % 5}"), ("met", f"m{m}")) for r, m in edges]
    for u, v in nx_edges:
        G.add_edge(u, v, w=1.0)

    idx = {nd: i for i, nd in enumerate(G.nodes)}
    B = build_incidence([(idx[u], idx[v]) for u, v in nx_edges], len(idx))
    w = np.ones(len(nx_edges))
    for s_nd, t_nd in [(("met", "m0"), ("met", "m5")), (("met", "m1"), ("met", "m4")),
                       (("met", "m2"), ("met", "m5"))]:
        c_undir = 1.0 / reff_dense(G, s_nd, t_nd, wk="w")
        c_dir = directed_ceff(B, w, w.copy(), idx[s_nd], idx[t_nd])
        assert abs(c_dir - c_undir) < SELFTEST_TOL


def test_direction_actually_matters():
    """An irreversible chain conducts much better forward than backward -- and is
    direction-blind once the ratio is 1.0."""
    B = build_incidence([(0, 1), (1, 2)], 3)
    gp, gm = np.array([1.0, 1.0]), np.array([0.01, 0.01])
    c_fwd, c_rev = directed_ceff(B, gp, gm, 0, 2), directed_ceff(B, gp, gm, 2, 0)
    assert c_fwd > 10.0 * c_rev
    c_fs = directed_ceff(B, gp, gp.copy(), 0, 2)
    c_rs = directed_ceff(B, gp, gp.copy(), 2, 0)
    assert abs(c_fs - c_rs) < 1e-9


@pytest.mark.parametrize("k", range(3))
def test_warm_start_changes_only_the_iteration_count(k):
    """The smoothed energy is strictly convex with a unique grounded minimiser, so a
    warm start may move the iteration count and nothing else."""
    edges, n, gp, gm, s, t = toy_diode_nets()[k]
    B = build_incidence(edges, n)
    c_cold = directed_ceff(B, gp, gm, s, t)
    phi0 = np.random.default_rng(3 + k).standard_normal(n)
    phi0[0] = 0.0
    c_warm = directed_ceff(B, gp, gm, s, t, phi0=phi0)
    assert abs(c_cold - c_warm) < 1e-9

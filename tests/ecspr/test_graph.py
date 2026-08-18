"""The atom network, its terminals, the solve, and the universal-leakage ground.

Lifted out of ``ecspr_graph.py``'s ``_selftest_*`` block. The symmetric-limit test
against ``ecspr_atom_graph.supernode_ieff`` did not come across: that module is
retired and absent from this tree, and the same property is covered by
``test_directed.test_symmetric_limit_matches_undirected`` against a referent that
still exists.
"""
import networkx as nx
import numpy as np
import pytest

from ecspr.model.graph import AtomGraph, Terminal, measure_leak, solve, sweep_leak

from conftest import SELFTEST_TOL, reff_dense, toy_graph


@pytest.mark.parametrize("ratio", [1.0, 0.01])
def test_conservation(ratio):
    """Per-precursor delivered currents sum to the injected current."""
    g = toy_graph(ratio)
    src = Terminal.metabolite(g, "m0")
    sol = solve(g, src, Terminal.merge(g, ["p1", "p2"]))
    d1, d2 = sol.delivered("p1"), sol.delivered("p2")
    assert abs(d1 + d2 - sol.injected) < 1e-8
    assert abs(sol._net_inflow(src.nodes) + sol.injected) < 1e-8
    # The dead end's net current IS the KCL residual at an interior node, so it is
    # bounded by the Newton gradient tolerance rather than by machine epsilon.
    assert abs(sol.delivered("d0")) < 1e-6
    assert d1 > d2 > 0, "the asymmetric split must actually split asymmetrically"


@pytest.mark.parametrize("snk_met", ["p1", "p2"])
def test_single_sink_matches_dense_two_terminal(snk_met, toy):
    """A sink terminal of ONE metabolite reproduces an independent dense solve on
    the contracted graph."""
    src = Terminal.metabolite(toy, "m0")
    snk = Terminal.metabolite(toy, snk_met)
    sol = solve(toy, src, snk)
    G = nx.Graph()
    for e, (a, b) in enumerate(toy.edges):
        ka, kb = toy.nodes[a], toy.nodes[b]
        ka = "S*" if ka in src.nodes else ("T*" if ka in snk.nodes else ka)
        kb = "S*" if kb in src.nodes else ("T*" if kb in snk.nodes else kb)
        if ka == kb:
            continue
        if G.has_edge(ka, kb):
            G[ka][kb]["w"] += float(toy.gp[e])
        else:
            G.add_edge(ka, kb, w=float(toy.gp[e]))
    comp = nx.node_connected_component(G, "S*")
    r = reff_dense(G.subgraph(comp), "S*", "T*", wk="w")
    assert abs(sol.total - 1.0 / r) < SELFTEST_TOL


def test_atom_mask(toy):
    """A full mask is a no-op; a strict subset can only lower the total."""
    gnd = Terminal.merge(toy, ["p1", "p2"])
    full = solve(toy, Terminal.metabolite(toy, "m0"), gnd).total
    same = solve(toy, Terminal.metabolite(toy, "m0", mask=[0, 1]), gnd).total
    part = solve(toy, Terminal.metabolite(toy, "m0", mask=[0]), gnd).total
    assert abs(full - same) < SELFTEST_TOL
    assert 0.0 < part <= full + SELFTEST_TOL


def test_disconnected_is_a_definite_zero(toy):
    """A terminal with no path to the other returns zero, not a raise -- the atom
    graph is disconnected in general and an unreachable precursor is a coverage
    fact the caller has to be able to report."""
    sol = solve(toy, Terminal.metabolite(toy, "m0"), Terminal.metabolite(toy, "z0"))
    assert sol.total == 0.0 and sol.converged
    assert solve(toy, Terminal.metabolite(toy, "m0"),
                 Terminal.metabolite(toy, "nope")).total == 0.0


def test_voltage_gauge_and_interior(toy):
    """The sink terminal is the gauge (0), the source sits at 1/total, and an
    interior metabolite's atoms sit strictly between."""
    src = Terminal.metabolite(toy, "m0")
    sol = solve(toy, src, Terminal.merge(toy, ["p1", "p2"]))
    vs, vt = sol.voltage(("m0", 0)), sol.voltage(("p1", 0))
    assert abs(vt) < 1e-12
    assert abs(vs - 1.0 / sol.total) < SELFTEST_TOL
    assert abs(sol.drop(("m0", 0), ("p1", 0)) - vs) < SELFTEST_TOL
    for v in sol.voltage_metabolite("m1")["per_atom"].values():
        assert vt < v < vs
    # every atom of a merged terminal sits at one potential -- that is what
    # shorting means
    assert abs(sol.voltage(("p2", 0)) - sol.voltage(("p1", 0))) < 1e-12


def test_throughput_at_an_interior_metabolite(toy):
    """A pass-through metabolite carries the whole injected current; its NET current
    is 0. At a terminal, throughput is the full draw -- not the half-sum, which
    undercounts by 2x."""
    sol = solve(toy, Terminal.metabolite(toy, "m0"), Terminal.merge(toy, ["p1", "p2"]))
    assert abs(sol.delivered("m1")) < 1e-8
    assert abs(sol.throughput_metabolite("m1") - sol.injected) < 1e-6
    for m in ("p1", "p2"):
        assert abs(sol.throughput_metabolite(m) - sol.delivered(m)) < 1e-8
    assert abs(sol.throughput_metabolite("m0") - sol.injected) < 1e-8


def test_a_loss_of_function_redistributes(toy):
    """The instrument must see a throttled route: current shifts to the other one."""
    sym = solve(toy, Terminal.metabolite(toy, "m0"), Terminal.merge(toy, ["p1", "p2"]))
    lof = AtomGraph(list(toy.nodes), list(toy.edges), toy.gp.copy(), toy.gm.copy(),
                    dict(toy.meta))
    for e, (a, b) in enumerate(lof.edges):
        if lof.nodes[b][0] == "p1":
            lof.gp[e] *= 1e-4                 # a loss-of-function edge, caller-side
            lof.gm[e] = lof.gp[e]
    ko = solve(lof, Terminal.metabolite(lof, "m0"), Terminal.merge(lof, ["p1", "p2"]))
    assert ko.share("p1") < sym.share("p1") - 0.1
    assert ko.share("p2") > sym.share("p2") + 0.1
    assert abs(ko.share("p1") + ko.share("p2") - 1.0) < 1e-8


def test_leak_breaks_the_measurement_horizon(toy):
    """The claim the leak mode exists for: a NON-PRECURSOR metabolite has a nonzero
    draw. Under the two-terminal probe its draw is identically zero by KCL, so a leak
    run whose interior draws are all zero has changed nothing."""
    src = Terminal.metabolite(toy, "m0")
    prec = ["p1", "p2"]
    hard = solve(toy, src, Terminal.merge(toy, prec))
    interior = [m for m in toy.metabolites() if m not in prec and m != "m0"]
    assert interior
    for m in interior:
        for a in toy.atoms_of(m):
            assert abs(hard.current(a)) < 1e-9

    r = measure_leak(toy, src, prec, leak=1e-3)
    assert r["converged"]
    assert any(abs(r["draw"][m]) > 0 for m in interior)
    assert set(r["draw"]) == set(toy.metabolites())
    # A metabolite in a disconnected component draws NOTHING: its leak resistor
    # exists but no current can reach it, so a nonzero value would be the leak
    # PARAMETER showing up as signal.
    assert r["draw"]["z0"] == 0.0


def test_the_leak_is_per_metabolite_not_per_atom():
    """A leak attached per ATOM makes the readout a proxy for molecule size. Two
    metabolites at the same potential with different atom counts must draw the
    same current."""
    recs = [(("src", 0), ("a", 0), 1.0, 1.0)]
    recs += [(("src", 0), ("b", r), 0.25, 0.25) for r in range(4)]
    g = AtomGraph.from_edge_records(recs)
    res = measure_leak(g, Terminal.metabolite(g, "src"), [], leak=1e-3)
    da, db = res["draw"]["a"], res["draw"]["b"]
    assert len(g.atoms_of("a")) == 1 and len(g.atoms_of("b")) == 4
    assert abs(da - db) / max(da, db) < 1e-6


def test_leak_sweep_reports_ranking(toy):
    """The sweep must watch the RANKING: totals are nearly invariant to the leak, so
    a sweep that watches them reports stability that is not there."""
    runs = sweep_leak(toy, Terminal.metabolite(toy, "m0"), ["p1", "p2"],
                      [1e-8, 1e-6, 1e-4, 1e-2], top=10)
    assert len(runs) == 4
    assert all("rho_vs_prev" in r for r in runs)
    assert np.isnan(runs[0]["rho_vs_prev"])

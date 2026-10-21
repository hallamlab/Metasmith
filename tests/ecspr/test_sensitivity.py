import numpy as np
import pandas as pd
import pytest

import ecspr.model.build as EB
from ecspr.model.graph import AtomGraph, Terminal, solve
from ecspr.model.sensitivity import check_against_power, edge_sensitivity


def chain_pairs(n=10):
    rows = [dict(mnxr=f"R{i}", element="C", substrate=f"M{i}", product=f"M{i + 1}",
                 sub_idx=0, prod_idx=0, pair_w=1.0) for i in range(n)]
    rows.append(dict(mnxr="RX", element="C", substrate="M0", product="M5",
                     sub_idx=0, prod_idx=0, pair_w=1.0))
    rows.append(dict(mnxr="RY", element="C", substrate="M3", product="M8",
                     sub_idx=0, prod_idx=0, pair_w=1.0))
    return pd.DataFrame(rows)


def built(ratios):
    pairs = chain_pairs()
    w = {m: 1.0 for m in pairs.mnxr.unique()}
    g = EB.graph_from_pairs(pairs, "C", w, ratios, with_provenance=True)
    return g, Terminal.metabolite(g, "M0"), Terminal.metabolite(g, "M10")


def directed_ratios(seed=0):
    rng = np.random.default_rng(seed)
    r = {f"R{i}": float(10 ** rng.uniform(-2.5, 0.0)) for i in range(10)}
    r["RX"], r["RY"] = 0.05, 0.3
    return r


def test_symmetric_network_matches_edge_power():
    # Tellegen makes the adjoint and the dissipated-power share the same quantity here, so
    # this pins the adjoint's right-hand side without any finite difference.
    g, s, t = built({})
    assert check_against_power(g, solve(g, s, t)) < 1e-10


def test_symmetric_elasticities_sum_to_one():
    g, s, t = built({})
    sol = solve(g, s, t)
    sens = edge_sensitivity(sol)
    gp = g.gp[sens.oedge]
    total = float((gp * (sens.d_logceff_dgp + sens.d_logceff_dgm)).sum())
    assert total == pytest.approx(1.0, abs=1e-9)


@pytest.mark.parametrize("seed", [0, 1, 2])
def test_directed_gradient_matches_central_difference(seed):
    g, s, t = built(directed_ratios(seed))
    sol = solve(g, s, t)
    sens = edge_sensitivity(sol)

    def reff(gp, gm):
        g2 = AtomGraph(g.nodes, g.edges, gp, gm, dict(g.meta), dict(g.idx))
        return 1.0 / solve(g2, s, t).total

    for k, e in enumerate(sens.oedge):
        for arr, analytic in ((g.gp, sens.d_reff_dgp), (g.gm, sens.d_reff_dgm)):
            base = float(arr[e])
            h = max(abs(base) * 1e-6, 1e-14)
            up, dn = (g.gp.copy(), g.gm.copy()), (g.gp.copy(), g.gm.copy())
            which = 0 if arr is g.gp else 1
            up[which][e] = base + h
            dn[which][e] = base - h
            fd = (reff(*up) - reff(*dn)) / (2 * h)
            assert abs(fd - analytic[k]) <= max(1e-5 * abs(fd), 1e-9)


def test_disconnected_terminals_give_zero_sensitivity():
    pairs = chain_pairs()
    w = {m: 1.0 for m in pairs.mnxr.unique() if m not in {"R5", "RX", "RY"}}
    g = EB.graph_from_pairs(pairs, "C", w, {}, with_provenance=True)
    sol = solve(g, Terminal.metabolite(g, "M0"), Terminal.metabolite(g, "M10"))
    assert sol.total == 0.0
    sens = edge_sensitivity(sol)
    assert not np.any(sens.d_reff_dgp) and not np.any(sens.d_reff_dgm)


def test_power_identity_refuses_a_directed_network():
    g, s, t = built(directed_ratios())
    with pytest.raises(ValueError):
        check_against_power(g, solve(g, s, t))

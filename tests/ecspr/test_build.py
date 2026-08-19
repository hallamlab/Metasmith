import numpy as np
import pandas as pd
import pytest

from ecspr.model.build import (graph_from_pairs, read_graph_dir, write_graph_dir,
                               reaction_elasticities, _elements_in)
from ecspr.model.graph import Terminal, solve


def toy_pairs() -> pd.DataFrame:
    return pd.DataFrame([
        dict(mnxr="R1", element="C", substrate="A", product="B", sub_idx=0, prod_idx=0,
             pair_w=1.0, confidence=0.9),
        dict(mnxr="R1", element="C", substrate="A", product="B", sub_idx=1, prod_idx=1,
             pair_w=0.5, confidence=0.9),
        dict(mnxr="R2", element="C", substrate="B", product="P", sub_idx=0, prod_idx=0,
             pair_w=1.0, confidence=1.0),
        dict(mnxr="R2", element="N", substrate="B", product="Q", sub_idx=0, prod_idx=0,
             pair_w=1.0, confidence=1.0),
        dict(mnxr="R3", element="C", substrate="A", product="B", sub_idx=0, prod_idx=0,
             pair_w=1.0, confidence=1.0),
    ])


def _edges(g):
    return {(g.nodes[a], g.nodes[b]): (float(g.gp[i]), float(g.gm[i]))
            for i, (a, b) in enumerate(g.edges)}


def test_weights_ratios_element_filter_and_parallel_merge():
    g = graph_from_pairs(toy_pairs(), "C", {"R1": 2.0, "R2": 1.0, "R3": 3.0},
                         {"R2": 0.01})
    e = _edges(g)
    assert abs(e[(("A", 0), ("B", 0))][0] - 5.0) < 1e-12, "parallel edges must sum"
    assert abs(e[(("A", 1), ("B", 1))][0] - 1.0) < 1e-12, "pair_w must scale the edge"
    assert abs(e[(("B", 0), ("P", 0))][1] - 0.01) < 1e-12, "ratio must set gm"
    assert (("B", 0), ("Q", 0)) not in e, "the N transfer must not enter the C graph"
    assert g.meta["n_aam_gap"] == 0 and g.meta["n_reactions_used"] == 3


def test_the_aam_gap_is_counted_not_silent():
    g = graph_from_pairs(toy_pairs(), "C", {"R1": 1.0, "RX": 1.0})
    assert g.meta["n_aam_gap"] == 1
    assert g.meta["n_reactions_requested"] == 2


def test_both_atom_pair_schemas_agree():
    ints = pd.DataFrame([
        dict(mnxr="R1", element="C", substrate="A", product="B", sub_idx=0, prod_idx=2,
             pair_w=1.0),
        dict(mnxr="R1", element="C", substrate="A", product="B", sub_idx=1, prod_idx=3,
             pair_w=0.5),
    ])
    strs = pd.DataFrame([
        dict(mnxr="R1", element="C", substrate="A", product="B", sub_idx="0,1",
             prod_idx="2,3", pair_w="1.0,0.5"),
    ])
    a = graph_from_pairs(ints, "C", {"R1": 2.0})
    b = graph_from_pairs(strs, "C", {"R1": 2.0})
    assert {k: v[0] for k, v in _edges(a).items()} == \
           {k: v[0] for k, v in _edges(b).items()}


def test_graph_dir_round_trips(tmp_path):
    g = graph_from_pairs(toy_pairs(), "C", {"R1": 2.0, "R2": 1.0, "R3": 3.0},
                         {"R2": 0.01})
    write_graph_dir(tmp_path, {"C": g}, dict(source="pairs"))
    assert _elements_in(tmp_path) == ["C"]
    h = read_graph_dir(tmp_path, "C")
    assert h.nodes == g.nodes and h.edges == g.edges
    assert np.allclose(h.gp, g.gp) and np.allclose(h.gm, g.gm)
    assert h.meta["n_aam_gap"] == g.meta["n_aam_gap"]


def test_build_then_solve_then_caller_side_lof():
    p = pd.DataFrame([
        dict(mnxr="R1", element="C", substrate="S", product="M", sub_idx=0, prod_idx=0,
             pair_w=1.0),
        dict(mnxr="R2", element="C", substrate="M", product="P1", sub_idx=0, prod_idx=0,
             pair_w=1.0),
        dict(mnxr="R3", element="C", substrate="M", product="P2", sub_idx=0, prod_idx=0,
             pair_w=1.0),
    ])
    w = {"R1": 1.0, "R2": 2.0, "R3": 1.0}
    g = graph_from_pairs(p, "C", w)
    sol = solve(g, Terminal.metabolite(g, "S"), Terminal.merge(g, ["P1", "P2"]))
    gk = graph_from_pairs(p, "C", {k: v for k, v in w.items() if k != "R2"})
    sk = solve(gk, Terminal.metabolite(gk, "S"), Terminal.merge(gk, ["P1", "P2"]))
    assert abs(sol.share("P1") - 2.0 / 3.0) < 1e-9
    assert sk.share("P2") == 1.0 and sk.share("P1") == 0.0
    assert sk.total < sol.total


def test_gpr_polarity():
    cobra = pytest.importorskip("cobra")
    m = cobra.Model("toy")
    a, b, c = (cobra.Metabolite(x) for x in ("A", "B", "C"))
    r1 = cobra.Reaction("R1"); r1.add_metabolites({a: -1, b: 1})
    r2 = cobra.Reaction("R2"); r2.add_metabolites({b: -1, c: 1})
    r3 = cobra.Reaction("R3"); r3.add_metabolites({a: -1, c: 1})
    m.add_reactions([r1, r2, r3])
    r1.gene_reaction_rule = "g1 or g2"
    r2.gene_reaction_rule = "g3 and g4"
    r3.gene_reaction_rule = ""
    genes = sorted(g.id for g in m.genes)

    def live(active):
        ko = {g.id for g in m.genes} - set(active)
        return {r.id for r in m.reactions
                if not (r.gene_reaction_rule or "").strip() or bool(r.gpr.eval(ko))}

    wt = live(genes)
    assert wt == {"R1", "R2", "R3"}
    assert live([g for g in genes if g != "g1"]) == {"R1", "R2", "R3"}
    assert live([g for g in genes if g != "g3"]) == {"R1", "R3"}
    assert live([]) == {"R3"}
    for s in (live([g for g in genes if g != "g3"]), live([])):
        assert s <= wt, "a knockout may only ever REMOVE reactions"


def test_reversed_orientation_is_a_no_op_on_a_symmetric_reference():
    p, w = toy_pairs(), {"R1": 2.0, "R2": 1.0, "R3": 3.0}
    ratios = {"R1": 1.0, "R2": 1.0, "R3": 1.0}
    fwd = graph_from_pairs(p, "C", w, ratios, orientation="as_written")
    rev = graph_from_pairs(p, "C", w, ratios, orientation="reversed")
    assert fwd.nodes == rev.nodes and fwd.edges == rev.edges
    assert np.allclose(fwd.gp, rev.gp) and np.allclose(fwd.gm, rev.gm)


def test_reversed_orientation_inverts_the_ratio():
    p = pd.DataFrame([dict(mnxr="R1", element="C", substrate="A", product="B",
                           sub_idx=0, prod_idx=0, pair_w=1.0)])
    fwd = graph_from_pairs(p, "C", {"R1": 1.0}, {"R1": 0.01})
    rev = graph_from_pairs(p, "C", {"R1": 1.0}, {"R1": 0.01}, orientation="reversed")
    (fe, fv), = _edges(fwd).items()
    (re, rv), = _edges(rev).items()
    assert fe == (("A", 0), ("B", 0)) and fv == pytest.approx((1.0, 0.01))
    assert re == (("B", 0), ("A", 0)), "reversed must flip the favoured direction"
    assert rv == pytest.approx((1.0, 0.01)), "and must not amplify the conductance"
    assert fwd.meta["orientation"] == "as_written"
    assert rev.meta["orientation"] == "reversed"


def test_an_unknown_orientation_is_refused():
    with pytest.raises(ValueError):
        graph_from_pairs(toy_pairs(), "C", {"R1": 1.0}, orientation="backwards")


def _chain_pairs(steps) -> pd.DataFrame:
    """A series chain ``M0 -> M1 -> ... -> Mn``, one reaction and one carbon per step."""
    return pd.DataFrame([
        dict(mnxr=f"R{i}", element="C", substrate=f"M{i}", product=f"M{i + 1}",
             sub_idx=0, prod_idx=0, pair_w=1.0)
        for i in range(steps)])


def test_elasticities_partition_the_measurement():
    """A series chain of k equal steps puts exactly 1/k on each, and they sum to 1.

    This is what makes the spread readable as "how many levers": the shares are a
    partition, not a ranking, so 1/sum(eps^2) counts the steps.
    """
    p = _chain_pairs(4)
    w = {f"R{i}": 1.0 for i in range(4)}
    g = graph_from_pairs(p, "C", w, with_provenance=True)
    sol = solve(g, Terminal.metabolite(g, "M0"), Terminal.metabolite(g, "M4"))
    eps = reaction_elasticities(g, sol)
    assert eps.sum() == pytest.approx(1.0, abs=1e-12)
    assert set(eps.index) == set(w)
    assert eps.to_numpy() == pytest.approx(np.full(4, 0.25), abs=1e-9)
    assert 1.0 / float((eps ** 2).sum()) == pytest.approx(4.0, abs=1e-6)


def test_elasticity_is_the_derivative_a_sweep_would_measure():
    """The closed form against a numerical fold, on an unequal chain plus a parallel arm."""
    p = pd.concat([_chain_pairs(3),
                   pd.DataFrame([dict(mnxr="RB", element="C", substrate="M0",
                                      product="M2", sub_idx=0, prod_idx=0, pair_w=1.0)])])
    w = {"R0": 2.0, "R1": 0.5, "R2": 3.0, "RB": 0.25}
    g = graph_from_pairs(p, "C", w, with_provenance=True)
    term = (Terminal.metabolite(g, "M0"), Terminal.metabolite(g, "M3"))
    base = solve(g, *term).total
    eps = reaction_elasticities(g, solve(g, *term))
    fold = 1.001
    for r in w:
        gp = graph_from_pairs(p, "C", dict(w, **{r: w[r] * fold}))
        up = solve(gp, Terminal.metabolite(gp, "M0"), Terminal.metabolite(gp, "M3")).total
        assert np.log(up / base) / np.log(fold) == pytest.approx(eps[r], abs=2e-3)
    assert eps.sum() == pytest.approx(1.0, abs=1e-12)


def test_elasticities_need_provenance():
    g = graph_from_pairs(_chain_pairs(2), "C", {"R0": 1.0, "R1": 1.0})
    sol = solve(g, Terminal.metabolite(g, "M0"), Terminal.metabolite(g, "M2"))
    with pytest.raises(ValueError):
        reaction_elasticities(g, sol)


def test_the_two_seams_bound_the_ratio_at_the_same_width():
    """`build.DIRECTION_DECADE_CAP` and the bake's `DIR_DG_CLAMP` are one statement.

    The bake bounds |dG'| in kJ/mol and the model bounds |log10 ratio| in decades, and
    `ratio = exp(dG'/RT)` is what relates them. If they drift apart, a table baked under one
    bound gets read under another and the graph silently stops describing the annotation.
    """
    from ecspr.bake.direction import canon
    from ecspr.model.build import DIRECTION_DECADE_CAP

    assert canon.DIR_DG_CLAMP / canon.DIR_DECADE == pytest.approx(DIRECTION_DECADE_CAP)


def test_capping_is_symmetric_and_leaves_the_undirected_limit_alone():
    from ecspr.model.build import cap_direction_ratios

    caught = cap_direction_ratios({"a": 1e17, "b": 1e-17, "c": 1.0, "d": 10.0}, 3.0)
    assert caught == {"a": 1e3, "b": 1e-3, "c": 1.0, "d": 10.0}
    assert cap_direction_ratios({"a": 1e17}, None) == {"a": 1e17}
    assert cap_direction_ratios({"a": 1e17}, float("inf")) == {"a": 1e17}

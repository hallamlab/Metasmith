"""The builders: atom pairs x weights x direction ratios -> a network.

Lifted out of ``ecspr_build.py``'s ``_selftest_*`` block, plus the orientation flag
that arrived with the package.
"""
import numpy as np
import pandas as pd
import pytest

from ecspr.model.build import (graph_from_pairs, read_graph_dir, write_graph_dir,
                         _elements_in)
from ecspr.model.graph import Terminal, solve


def toy_pairs() -> pd.DataFrame:
    """Two reactions over a shared metabolite, one with two carbon transfers."""
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
             pair_w=1.0, confidence=1.0),          # parallel with R1's first transfer
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
    """The frozen reference is at pair granularity; the incumbent extract and the
    fixtures pack an atom list into one row as comma-joined strings."""
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
    """A built graph must measure, and dropping a reaction must move the share."""
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
    """cobra's ``GPR.eval`` takes KNOCKOUTS, not active genes. Handing it the active
    set inverts the question and the symptom is not a crash: a first run reported
    532 of 2742 reactions live for the WILD TYPE and MORE live after a knockout."""
    cobra = pytest.importorskip("cobra")
    m = cobra.Model("toy")
    a, b, c = (cobra.Metabolite(x) for x in ("A", "B", "C"))
    r1 = cobra.Reaction("R1"); r1.add_metabolites({a: -1, b: 1})
    r2 = cobra.Reaction("R2"); r2.add_metabolites({b: -1, c: 1})
    r3 = cobra.Reaction("R3"); r3.add_metabolites({a: -1, c: 1})
    m.add_reactions([r1, r2, r3])
    r1.gene_reaction_rule = "g1 or g2"      # isozymes: one knockout is not enough
    r2.gene_reaction_rule = "g3 and g4"     # a complex: either knockout kills it
    r3.gene_reaction_rule = ""              # ruleless: no gene to knock out
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


# ---------------------------------------------------------------------------
# orientation
# ---------------------------------------------------------------------------

def test_reversed_orientation_is_a_no_op_on_a_symmetric_reference():
    """THE test that ``--orientation`` flips the reference and touches nothing else:
    a reference whose ratios are all 1.0 is its own reverse."""
    p, w = toy_pairs(), {"R1": 2.0, "R2": 1.0, "R3": 3.0}
    ratios = {"R1": 1.0, "R2": 1.0, "R3": 1.0}
    fwd = graph_from_pairs(p, "C", w, ratios, orientation="as_written")
    rev = graph_from_pairs(p, "C", w, ratios, orientation="reversed")
    assert fwd.nodes == rev.nodes and fwd.edges == rev.edges
    assert np.allclose(fwd.gp, rev.gp) and np.allclose(fwd.gm, rev.gm)


def test_reversed_orientation_inverts_the_ratio():
    """A throttled edge as written is a favoured one reversed. The ratio-above-1
    rule then flips the edge, so the reversed graph carries the same conductance in
    the opposite direction rather than a manufactured 1/ratio amplification."""
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

"""The encoding, against a real bake rather than a fixture.

The tier that carries the most weight, and the one that most needed a budget
decision. The packing, the vocabulary join and the ratio flip are arithmetic
over 2.4M rows; a ten-row fixture would exercise the code without exercising the
arithmetic, and carbon would exercise the arithmetic at a hundred times the
cost. Sulfur is the same coverage for 24,198 pair rows instead of ~1.5M -- so
this whole file runs in about a second against the real deployed trio.

The counts below are the r7 bake at vocab 0ffd4c8c6231696e. They are a
REGRESSION pin over a fixed artifact, not a claim about what a rebake should
produce: when the bake is rebuilt they move, and the honest response is to
re-derive them and say in the commit which bake they now describe.

The sulfur counts run through `ratio_by_code`, so they are DIRECTION-SENSITIVE
as well as topology-sensitive: a re-bake that only changed the direction table
would still move them.
"""
from __future__ import annotations

import pytest

from ecspr.bake import encoding as refs

# The deployed trio, and the sulfur slice of it.
BAKE = "0ffd4c8c6231696e"
S_NODES, S_EDGES, S_PAIR_ROWS = 7833, 11078, 26352
S_REACTIONS_USED, S_METABOLITES = 18142, 6613


@pytest.fixture(scope="module")
def ident(deployed_bake):
    return refs.assert_same_bake(deployed_bake["vocab"], deployed_bake["atom_pairs"],
                                 deployed_bake["direction"])


def test_the_trio_carries_one_identity(ident):
    """Reading atom_pairs against another bake's vocab decodes every node to the
    wrong metabolite SILENTLY -- a plausible graph rather than a broken one. So
    the three files are one artifact, and `assert_same_bake` is what says so.
    """
    assert ident["vocab_sha256"].startswith(BAKE)


def test_the_recomputed_vocabulary_hash_matches_the_stored_one(deployed_bake, ident):
    """The identity block is a claim about the vocabulary; recompute it.

    A stored hash nobody recomputes is a hash that cannot detect the rewrite it
    exists to detect.
    """
    V = refs.load_vocab(deployed_bake["vocab"])
    assert refs.vocab_sha256(V.df) == ident["vocab_sha256"]


def test_the_bit_fields_are_wide_enough_for_what_was_packed(ident):
    """The packing is unsigned and silent on overflow, so the widths are checked
    rather than trusted: a rank that does not fit wraps into a DIFFERENT atom."""
    assert ident["max_atom_rank"] < (1 << ident["rank_bits"])
    assert ident["n_met"] <= (1 << ident["met_bits"])
    assert 2 * (ident["met_bits"] + ident["rank_bits"]) <= refs.NODE_KEY_BUDGET


def test_node_packing_round_trips(ident):
    """pack/unpack over the real width, including the boundary values.

    The round trip is what makes a packed table decodable at all, and the widths
    it round-trips under are this bake's, not a fixture's.
    """
    import numpy as np

    rb = ident["rank_bits"]
    met = np.array([0, 1, ident["n_met"] - 1], dtype=np.int64)
    rank = np.array([0, (1 << rb) - 1, 7], dtype=np.int64)
    m2, r2 = refs.unpack_node(refs.pack_node(met, rank, rb), rb)
    assert np.array_equal(m2, met) and np.array_equal(r2, rank)


def test_sulfur_compiles_to_the_same_graph_it_compiles_to_today(deployed_bake, ident):
    """The join, the flip and the edge factorisation, end to end on real rows.

    Uniform weights over the whole vocabulary: what is under test is the
    arithmetic, none of which depends on the weights being interesting. The
    element is sulfur because carbon is the version of this test that would blow
    the suite's budget, and dropping to sulfur is the first cut to reach for.
    """
    pytest.importorskip("scipy", reason="compile_atom_graph builds an AtomGraph")

    V = refs.load_vocab(deployed_bake["vocab"])
    D = refs.load_direction(deployed_bake["direction"])
    lut = refs.ratio_by_code(V, D)
    pairs = refs.load_atom_pairs(deployed_bake["atom_pairs"])
    weights = {r: 1.0 for r in V.codes("rxn")}

    g = refs.compile_atom_graph("S", weights, ident=ident, vocab=V, pairs=pairs,
                                ratio_lut=lut)
    assert (g.n, g.m) == (S_NODES, S_EDGES)
    assert g.meta["n_pair_rows"] == S_PAIR_ROWS
    assert g.meta["n_reactions_used"] == S_REACTIONS_USED
    assert g.meta["n_metabolites"] == S_METABOLITES
    assert g.meta["n_weights_off_vocab"] == 0, (
        "a weight keyed by a reaction outside the vocabulary is the AAM gap and "
        "must be counted, never dropped silently")


def test_a_reaction_with_no_atom_pairs_is_counted_as_a_gap_not_dropped(deployed_bake, ident):
    """Evidence without coverage is a fact the caller has to be able to see.

    A reaction that has weight but no pairs cannot contribute an edge -- but
    "we had no map for it" and "you gave us nothing" are different, and the
    second must not be able to masquerade as the first.
    """
    pytest.importorskip("scipy")

    V = refs.load_vocab(deployed_bake["vocab"])
    pairs = refs.load_atom_pairs(deployed_bake["atom_pairs"])
    g = refs.compile_atom_graph("S", {"MNXR_NOT_IN_THIS_BAKE": 1.0}, ident=ident,
                                vocab=V, pairs=pairs)
    assert g.meta["n_weights_off_vocab"] == 1
    assert g.meta["n_reactions_used"] == 0
    assert (g.n, g.m) == (0, 0)


def test_ratios_stay_float64(deployed_bake):
    """The consumer's flip test is a threshold at exactly 1.0.

    Four reactions in the real table sit within float32 epsilon of it (e.g.
    MNXR112716 at 1.0000000000016507), so narrowing `ratio` would silently
    reorient those four edges. A re-encoding may not change topology.
    """
    D = refs.load_direction(deployed_bake["direction"])
    assert str(D["ratio"].dtype) == "float64"
    near = D[(D["ratio"] > 1.0) & (D["ratio"] < 1.0 + 1e-7)]
    assert len(near) > 0, (
        "no ratio sits within float32 epsilon of 1.0 in this bake -- the pin "
        "above describes a hazard this artifact no longer carries, so re-derive "
        "it rather than deleting the guard")

# The encoding, against a real bake rather than a fixture.
#
# The tier that carries the most weight, and the one that most needed a budget
# decision. The packing, the vocabulary join and the ratio flip are arithmetic
# over 2.4M rows; a ten-row fixture would exercise the code without exercising the
# arithmetic, and carbon would exercise the arithmetic at a hundred times the
# cost. Sulfur is the same coverage for 24,198 pair rows instead of ~1.5M -- so
# this whole file runs in about a second against the real deployed trio.
#
# The counts below are the r10 bake. They are a REGRESSION pin over a fixed
# artifact, not a claim about what a rebake should produce: when the bake is
# rebuilt they move, and the honest response is to re-derive them and say in the
# commit which bake they now describe.
#
# `BAKE` CANNOT TELL YOU WHICH BAKE THAT IS. The identity is a fact about the node
# space, so r10 -- a direction-only re-bake, like r9 before it -- inherits r7's
# 0ffd4c8c6231696e byte for byte. The field that separates them is
# `direction.parquet`'s per-file `src_direction_sha256`, 554b4d8f for r10 against
# e8f72b8b for r9, 96cc532c for r8 and d3acf218 for r7, and it lives under a
# different footer key so that `assert_same_bake` does not compare it across the
# trio.
#
# The sulfur counts run through `ratio_by_code`, so they are DIRECTION-SENSITIVE
# as well as topology-sensitive: a re-bake that only changed the direction table
# would still move them.
from __future__ import annotations

import pytest

from ecspr.bake import encoding as refs

BAKE = "0ffd4c8c6231696e"
# S_EDGES is the only one r10 moved, 11,076 -> 11,092. A direction-only re-bake
# routes through `ratio_by_code`, so the edge count is what it can touch and the
# node, pair-row, reaction and metabolite counts are what it cannot.
S_NODES, S_EDGES, S_PAIR_ROWS = 7833, 11092, 26352
S_REACTIONS_USED, S_METABOLITES = 18142, 6613


@pytest.fixture(scope="module")
def ident(deployed_bake):
    return refs.assert_same_bake(deployed_bake["vocab"], deployed_bake["atom_pairs"],
                                 deployed_bake["direction"])


def test_the_trio_carries_one_identity(ident):
    assert ident["vocab_sha256"].startswith(BAKE)


def test_the_recomputed_vocabulary_hash_matches_the_stored_one(deployed_bake, ident):
    # The identity block is a claim about the vocabulary; recompute it.
    #
    # A stored hash nobody recomputes is a hash that cannot detect the rewrite it
    # exists to detect.
    V = refs.load_vocab(deployed_bake["vocab"])
    assert refs.vocab_sha256(V.df) == ident["vocab_sha256"]


def test_the_bit_fields_are_wide_enough_for_what_was_packed(ident):
    # The packing is unsigned and silent on overflow, so the widths are checked
    # rather than trusted: a rank that does not fit wraps into a DIFFERENT atom.
    assert ident["max_atom_rank"] < (1 << ident["rank_bits"])
    assert ident["n_met"] <= (1 << ident["met_bits"])
    assert 2 * (ident["met_bits"] + ident["rank_bits"]) <= refs.NODE_KEY_BUDGET


def test_node_packing_round_trips(ident):
    import numpy as np

    rb = ident["rank_bits"]
    met = np.array([0, 1, ident["n_met"] - 1], dtype=np.int64)
    rank = np.array([0, (1 << rb) - 1, 7], dtype=np.int64)
    m2, r2 = refs.unpack_node(refs.pack_node(met, rank, rb), rb)
    assert np.array_equal(m2, met) and np.array_equal(r2, rank)


def test_sulfur_compiles_to_the_same_graph_it_compiles_to_today(deployed_bake, ident):
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
    pytest.importorskip("scipy")

    V = refs.load_vocab(deployed_bake["vocab"])
    pairs = refs.load_atom_pairs(deployed_bake["atom_pairs"])
    g = refs.compile_atom_graph("S", {"MNXR_NOT_IN_THIS_BAKE": 1.0}, ident=ident,
                                vocab=V, pairs=pairs)
    assert g.meta["n_weights_off_vocab"] == 1
    assert g.meta["n_reactions_used"] == 0
    assert (g.n, g.m) == (0, 0)


def test_ratios_stay_float64(deployed_bake):
    # The consumer's flip test is a threshold at exactly 1.0.
    #
    # Ratios land a couple of float32 ULPs off that threshold -- r10's closest
    # approaches are 1.0000001793 above and 0.9999996794 below, six reactions inside
    # 1e-6 on each side -- so which side of it an edge falls on is decided by the
    # width of the type as much as by the chemistry. r7 carried four that crossed
    # outright under float32 (MNXR112716 at 1.0000000000016507); r8, r9 and r10 carry
    # none, and the window below was widened from 1e-7 to 1e-6 to keep describing the
    # population that is actually at risk. That no reaction crosses TODAY is a
    # property of one artifact, not a reason to stop checking the type. A re-encoding
    # may not change topology.
    D = refs.load_direction(deployed_bake["direction"])
    assert str(D["ratio"].dtype) == "float64"
    near = D[(D["ratio"] > 1.0) & (D["ratio"] < 1.0 + 1e-6)]
    assert len(near) > 0, (
        "no ratio sits within ten float32 ULPs of 1.0 in this bake -- the window "
        "above describes a hazard this artifact no longer carries, so re-derive "
        "it rather than deleting the guard")

from __future__ import annotations

import pytest

from ecspr.bake import encoding as refs

BAKE = "675826f1bdae1f20"
S_NODES, S_EDGES, S_PAIR_ROWS = 7241, 10165, 24198
S_REACTIONS_USED, S_METABOLITES = 16757, 6062


@pytest.fixture(scope="module")
def ident(deployed_bake):
    return refs.assert_same_bake(deployed_bake["vocab"], deployed_bake["atom_pairs"],
                                 deployed_bake["direction"])


def test_the_trio_carries_one_identity(ident):
    assert ident["vocab_sha256"].startswith(BAKE)


def test_the_recomputed_vocabulary_hash_matches_the_stored_one(deployed_bake, ident):
    V = refs.load_vocab(deployed_bake["vocab"])
    assert refs.vocab_sha256(V.df) == ident["vocab_sha256"]


def test_the_bit_fields_are_wide_enough_for_what_was_packed(ident):
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
    D = refs.load_direction(deployed_bake["direction"])
    assert str(D["ratio"].dtype) == "float64"
    near = D[(D["ratio"] > 1.0) & (D["ratio"] < 1.0 + 1e-7)]
    assert len(near) > 0, (
        "no ratio sits within float32 epsilon of 1.0 in this bake -- the pin "
        "above describes a hazard this artifact no longer carries, so re-derive "
        "it rather than deleting the guard")

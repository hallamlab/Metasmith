"""Atom-resolved ECSPr graph for the concept figure, built from the metabolism bake.

A node is ``(metabolite, canonical atom rank)`` and an edge IS an atom transfer, so a path
cannot enter and leave a reaction through two substrates -- the defect that retired the
bipartite "star" topology (a zero-carbon channel scored 21x a real one on MNXR106432).
Reactions are what gets laid out; metabolites are the medium the current flows through.

Direction evidence may only *throttle*: a reaction whose ``ratio = g_rev/g_fwd`` exceeds 1
runs against the way its equation is written, so its edge is FLIPPED and the ratio inverted
rather than the reverse branch being handed conductance it has no evidence for. That is the
rule stated in ``ecspr.model.build.graph_from_pairs`` and it is applied at build time here, which
is why per-reaction terminals below are read off the graph's own post-flip orientation
instead of being re-derived from the pair table.

Bake inputs (``data/fabfos/processed/metabolism_bake``) were produced on ``capellaz`` from
MetaNetX 4.5; ``vocab.parquet`` maps integer codes to MNX symbols, ``atom_pairs.parquet``
carries one row per transferred atom, ``direction.parquet`` the directionality ensemble.
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import scipy.sparse as sp

# ECSPr is imported from this repo's own `src/` rather than installed, so the figures track
# the package with no rebuild. ECSPR_SRC / ECSPR_BAKE override the defaults so the same
# scripts run unmodified off-box (fir), where the two trees are staged side by side.
import os                                                            # noqa: E402
SRC = os.environ.get("ECSPR_SRC", str(Path(__file__).resolve().parents[4] / "src"))
if SRC not in sys.path:
    sys.path.insert(0, SRC)
from ecspr.model.graph import AtomGraph                                    # noqa: E402

BAKE = Path(os.environ.get(
    "ECSPR_BAKE",
    str(Path(__file__).resolve().parents[4] / "data/fabfos/processed/metabolism_bake")))
TINY = np.finfo(float).tiny


def load_vocab():
    v = pd.read_parquet(BAKE / "vocab.parquet")
    out = {}
    for kind, sub in v.groupby("kind"):
        out[kind] = (dict(zip(sub.code, sub.symbol)), dict(zip(sub.symbol, sub.code)))
    return out


def build_atom_graph(element="C", medium_rxn_symbols=None, weights=None):
    """AtomGraph for one element over a chosen reaction medium.

    ``weights`` is ``{mnxr: E_r}``; ``None`` means unweighted (every reaction present at
    conductance 1), which is what a universe-scale concept figure wants -- an evidence lane
    would make this a claim about one organism.

    Returns ``(graph, terminals, met_symbols)`` where ``terminals[mnxr]`` is
    ``(substrate_atom_nodes, product_atom_nodes)`` as frozensets of node indices.
    """
    vocab = load_vocab()
    el_code = vocab["element"][1][element]
    met_sym = vocab["met"][0]
    rxn_sym, rxn_code = vocab["rxn"][0], vocab["rxn"][1]

    p = pd.read_parquet(BAKE / "atom_pairs.parquet")
    p = p[p.element == el_code]
    if medium_rxn_symbols is not None:
        keep_codes = {rxn_code[s] for s in medium_rxn_symbols if s in rxn_code}
        p = p[p.rxn.isin(keep_codes)]
    p = p.reset_index(drop=True)

    d = pd.read_parquet(BAKE / "direction.parquet")
    ratio_map = dict(zip(d.rxn, d.ratio))

    gp = p.pair_w.to_numpy(float)
    if weights is not None:
        er = p.rxn.map(lambda c: weights.get(rxn_sym.get(c), 0.0)).to_numpy(float)
        gp = gp * er
    ratio = p.rxn.map(ratio_map).fillna(1.0).to_numpy(float)

    flip = ratio > 1.0
    tm = np.where(flip, p.head_met, p.tail_met)
    tr = np.where(flip, p.head_rank, p.tail_rank)
    hm = np.where(flip, p.tail_met, p.head_met)
    hr = np.where(flip, p.tail_rank, p.head_rank)
    ratio = np.where(flip, 1.0 / np.maximum(ratio, TINY), ratio)
    gm = ratio * gp

    keep = (gp > 0) & ~((tm == hm) & (tr == hr))
    tm, tr, hm, hr, gp, gm = tm[keep], tr[keep], hm[keep], hr[keep], gp[keep], gm[keep]
    rxn_of_row = p.rxn.to_numpy()[keep]

    key = pd.MultiIndex.from_arrays([tm, tr, hm, hr])
    codes, uniq = pd.factorize(key, sort=False)
    ne = len(uniq)
    gp_e = np.bincount(codes, gp, minlength=ne)
    gm_e = np.bincount(codes, gm, minlength=ne)

    nodes, idx, edges = [], {}, []

    def _i(k):
        j = idx.get(k)
        if j is None:
            j = idx[k] = len(nodes)
            nodes.append(k)
        return j

    for a, b, c, e in uniq:
        edges.append((_i((int(a), int(b))), _i((int(c), int(e)))))

    # edge -> reaction provenance. Two consumers: reaction terminals below, and
    # ``ecspr.model.build.reaction_currents``, which needs it under this exact key to attribute a
    # solution's edge currents back to reactions.
    mnxr_of_row = np.array([rxn_sym[c] for c in rxn_of_row], dtype=object)
    prov = pd.DataFrame(dict(edge=codes, mnxr=mnxr_of_row, gp=gp, rxn=rxn_of_row))

    meta = dict(element=element, n_nodes=len(nodes), n_edges=len(edges),
                n_reactions_used=int(pd.unique(rxn_of_row).size),
                n_reversed_rows=int(flip.sum()),
                edge_reactions=prov[["edge", "mnxr", "gp"]])
    g = AtomGraph(nodes, edges, gp_e, gm_e, meta, idx)

    tails = np.fromiter((e[0] for e in edges), int, len(edges))
    heads = np.fromiter((e[1] for e in edges), int, len(edges))
    terminals = {}
    for rc, sub in prov.groupby("rxn"):
        e = np.unique(sub.edge.to_numpy())
        terminals[rxn_sym[rc]] = (frozenset(tails[e].tolist()), frozenset(heads[e].tolist()))
    return g, terminals, met_sym


def restrict_to_giant(g, terminals):
    """Keep the largest weakly-connected component.

    The universe carbon graph has ~44k tiny components that cannot carry current anywhere
    and only inflate the system the factorization has to carry.
    """
    from scipy.sparse.csgraph import connected_components
    e = np.asarray(g.edges, dtype=np.int64)
    A = sp.coo_matrix((np.ones(len(e)), (e[:, 0], e[:, 1])), shape=(g.n, g.n))
    _nc, lab = connected_components(A, directed=False)
    giant = np.bincount(lab).argmax()
    keepn = np.flatnonzero(lab == giant)
    remap = np.full(g.n, -1, np.int64)
    remap[keepn] = np.arange(len(keepn))
    keepe = np.flatnonzero((lab[e[:, 0]] == giant) & (lab[e[:, 1]] == giant))
    nodes = [g.nodes[i] for i in keepn]
    edges = [(int(remap[a]), int(remap[b])) for a, b in e[keepe]]
    eremap = np.full(g.m, -1, np.int64)
    eremap[keepe] = np.arange(len(keepe))

    prov = g.meta["edge_reactions"]
    pe = prov.edge.to_numpy()
    ok = eremap[pe] >= 0
    prov2 = pd.DataFrame(dict(edge=eremap[pe[ok]], mnxr=prov.mnxr.to_numpy()[ok],
                              gp=prov.gp.to_numpy()[ok]))
    meta = dict(g.meta)
    meta.update(edge_reactions=prov2, n_nodes=len(nodes), n_edges=len(edges))
    g2 = AtomGraph(nodes, edges, g.gp[keepe], g.gm[keepe], meta)

    term2 = {}
    for r, (subs, prods) in terminals.items():
        s = frozenset(int(remap[i]) for i in subs if remap[i] >= 0)
        p = frozenset(int(remap[i]) for i in prods if remap[i] >= 0)
        if s and p:
            term2[r] = (s, p)
    return g2, term2


def incidence(edges, n):
    """Signed incidence (+1 tail, -1 head), vectorized.

    ``ecspr.model.directed.build_incidence`` builds the same matrix with a Python loop over m,
    which is minutes at 1.6M edges.
    """
    e = np.asarray(edges, dtype=np.int64)
    m = len(e)
    rows = np.repeat(np.arange(m), 2)
    cols = e.ravel()
    vals = np.tile(np.array([1.0, -1.0]), m)
    return sp.csr_matrix((vals, (rows, cols)), shape=(m, n))

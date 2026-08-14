"""
Concept figure: draw a reaction/metabolite network using UMAP purely as a
force-projection layout engine (not a validated embedding).

Loads a facet graph pickle (bipartite networkx graph, nodes keyed
('rxn', MNXR...) / ('met', MNXM...), edges carrying ``w_C`` = ECSPr's
atom-mapped carbon-conductance weight: an edge only exists, and is only
heavy, where carbon atoms actually transit that reaction<->metabolite step).
Distance in this figure is meant to reflect that conductance, not raw
shared-metabolite jaccard (which a ubiquitous cofactor like ATP or water can
dominate regardless of whether any carbon actually moves through it).

Getting there without an O(n^2)/O(n) pairwise effective-conductance solve:
a Laplacian eigenembedding of the conductance-weighted graph is the standard
low-rank approximation of pairwise effective-resistance distance (one sparse
eigensolve, not billions of pairwise probes). That embedding is handed to
UMAP (euclidean metric) purely for a 2D force-projection layout to render.
"""
import argparse
import pickle
from pathlib import Path

import numpy as np
import networkx as nx
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import scipy.sparse as sp
from scipy.sparse.linalg import eigsh
from scipy.sparse.csgraph import dijkstra
import umap


def load_graph(facet_dir: Path, weight_attr: str = "w_C", min_edge_weight: float = None) -> nx.Graph:
    p = facet_dir if facet_dir.suffix == ".pkl" else facet_dir / "base_C.pkl"
    assert p.exists(), f"missing facet graph {p}"
    with open(p, "rb") as fh:
        g = pickle.load(fh)

    if min_edge_weight is not None:
        weak = [(u, v) for u, v, d in g.edges(data=True)
                if float(d.get(weight_attr, 1.0)) < min_edge_weight]
        if weak:
            print(f"dropping {len(weak)} edges below {weight_attr} < {min_edge_weight}")
            g = g.copy()
            g.remove_edges_from(weak)

    n_components = nx.number_connected_components(g)
    giant = max(nx.connected_components(g), key=len)
    dropped = g.number_of_nodes() - len(giant)
    if dropped:
        print(f"dropping {dropped} nodes across {n_components - 1} disconnected side-components (keeping giant component, {len(giant)} nodes)")
        g = g.subgraph(giant).copy()

    return g


def conductance_spectral_embedding(g: nx.Graph, weight_attr: str, k: int):
    """Low-rank Euclidean stand-in for pairwise effective-resistance distance.

    Effective resistance R(i,j) = L+_ii + L+_jj - 2 L+_ij (L+ = Laplacian
    pseudoinverse) is what "pairwise conductance" means precisely, but
    computing it exactly is O(n) sparse solves (or O(n^2) if materialized) --
    fine at facet-graph scale, needlessly slow at universe scale. Truncating
    to the k smallest-nonzero-eigenvalue Laplacian eigenvectors, each scaled
    by 1/sqrt(eigenvalue), gives coordinates whose Euclidean distance
    approximates that same resistance distance (this is the standard
    commute-time / diffusion-map embedding), computed with a single sparse
    ARPACK shift-invert eigensolve instead of n solves.
    """
    nodes = list(g.nodes())
    idx = {n: i for i, n in enumerate(nodes)}
    n = len(nodes)

    rows, cols, vals = [], [], []
    for u, v, data in g.edges(data=True):
        w = float(data.get(weight_attr, 1.0))
        i, j = idx[u], idx[v]
        rows += [i, j]
        cols += [j, i]
        vals += [w, w]
    W = sp.coo_matrix((vals, (rows, cols)), shape=(n, n)).tocsr()
    deg = np.asarray(W.sum(axis=1)).flatten()
    L = sp.diags(deg) - W

    k = min(k, n - 2)
    eigvals, eigvecs = eigsh(L, k=k + 1, sigma=1e-8, which="LM")
    order = np.argsort(eigvals)
    eigvals, eigvecs = eigvals[order][1:], eigvecs[:, order][:, 1:]
    embedding = eigvecs / np.sqrt(np.maximum(eigvals, 1e-12))

    return nodes, embedding


def shortest_path_distance_matrix(g: nx.Graph, weight_attr: str):
    """All-pairs min-path distance through the conductance-weighted graph.

    Edge length = 1/w (heavy conductance = short hop, per the facet graphs'
    own convention). Unlike effective resistance/commute-time, this doesn't
    account for parallel paths -- it's the simpler, cheaper "min path"
    metric, and it doesn't exhibit the horseshoe/arch degeneracy that the
    spectral commute-time embedding does for this graph topology. Computed
    with scipy's compiled Dijkstra (not networkx's pure-Python one) so it
    stays fast (seconds, not forever) even at universe scale.
    """
    nodes = list(g.nodes())
    idx = {n: i for i, n in enumerate(nodes)}
    n = len(nodes)

    rows, cols, vals = [], [], []
    for u, v, data in g.edges(data=True):
        w = float(data.get(weight_attr, 1.0))
        length = 1.0 / w if w > 0 else np.inf
        i, j = idx[u], idx[v]
        rows += [i, j]
        cols += [j, i]
        vals += [length, length]
    graph_csr = sp.coo_matrix((vals, (rows, cols)), shape=(n, n)).tocsr()

    dist = dijkstra(graph_csr, directed=False)
    return nodes, dist.astype(np.float32)


def compress_radial_outliers(xy: np.ndarray, gamma: float) -> np.ndarray:
    """Pull far-out points back toward the layout's median radius.

    UMAP's own repulsion has no notion of a global scale, so a single
    weakly-attached cluster can end up many times farther from the bulk than
    any other inter-cluster gap, stretching the whole canvas around it. This
    rescales radius (measured from the centroid) as r' = pivot * (r/pivot)^gamma
    with pivot = median radius: at gamma=1 it's a no-op, at gamma<1 distances
    far past the pivot shrink proportionally more than distances near it, so
    the outlier's excess distance shrinks while nearby clusters stay
    distinguishable (they don't collapse into each other).
    """
    if gamma >= 1.0:
        return xy
    centroid = xy.mean(axis=0)
    delta = xy - centroid
    r = np.linalg.norm(delta, axis=1)
    pivot = np.median(r[r > 0]) if np.any(r > 0) else 1.0
    r_safe = np.where(r > 0, r, 1.0)
    scale = pivot * (r_safe / pivot) ** gamma / r_safe
    return centroid + delta * scale[:, None]


def layout(
    g: nx.Graph,
    distance_method: str,
    weight_attr: str,
    spectral_dims: int,
    seed: int,
    n_neighbors: int,
    min_dist: float,
    spread: float,
    init: str,
    repulsion_strength: float,
    radial_gamma: float,
):
    if distance_method == "shortest-path":
        nodes, embedding = shortest_path_distance_matrix(g, weight_attr)
        metric = "precomputed"
    else:
        nodes, embedding = conductance_spectral_embedding(g, weight_attr, spectral_dims)
        metric = "euclidean"

    reducer = umap.UMAP(
        n_components=2,
        metric=metric,
        n_neighbors=n_neighbors,
        min_dist=min_dist,
        spread=spread,
        init=init,
        repulsion_strength=repulsion_strength,
        random_state=seed,
    )
    xy = reducer.fit_transform(embedding)
    xy = compress_radial_outliers(xy, radial_gamma)

    return {n: xy[i] for i, n in enumerate(nodes)}


def load_pathway_membership(reac_prop_path: Path, pathways_path: Path, pathway_ids: list):
    """MNXR -> KEGG pathway id, via MetaNetX's own keggR: cross-reference.

    reac_prop.tsv's ``reference`` column carries ``keggR:R#####`` for
    KEGG-sourced reactions; the ModelSEED KEGG.pathways table carries
    pathway id -> pipe-separated R##### membership. Chaining the two gives
    MNXR -> pathway without needing a KEGG API call. A reaction in more than
    one requested pathway gets whichever was listed first in ``pathway_ids``
    (fine for a sanity-check overlay, not a claim of primary pathway).
    """
    pathway_reactions = {}
    pathway_names = {}
    with open(pathways_path) as fh:
        next(fh)
        for line in fh:
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 5:
                continue
            _source, pid, name, _aliases, reactions = parts[:5]
            if pid in pathway_ids:
                pathway_reactions[pid] = set(reactions.split("|"))
                pathway_names[pid] = name

    mnxr_to_kegg = {}
    with open(reac_prop_path) as fh:
        for line in fh:
            if line.startswith("#"):
                continue
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 3:
                continue
            mnxr, _eq, ref = parts[0], parts[1], parts[2]
            if ref.startswith("keggR:"):
                mnxr_to_kegg[mnxr] = ref[len("keggR:"):]

    mnxr_to_pathway = {}
    for mnxr, keggr in mnxr_to_kegg.items():
        for pid in pathway_ids:
            if pid in pathway_reactions and keggr in pathway_reactions[pid]:
                mnxr_to_pathway[mnxr] = pid
                break

    return mnxr_to_pathway, pathway_names


PATHWAY_PALETTE = ["#d62728", "#2ca02c", "#9467bd", "#17becf", "#e377c2", "#bcbd22"]


def render(g, rxn_nodes, met_nodes, pos, title: str, out: Path, mnxr_to_pathway=None, pathway_names=None):
    fig, ax = plt.subplots(figsize=(10, 10))
    nx.draw_networkx_edges(g, pos, ax=ax, width=0.15, alpha=0.2, edge_color="grey")

    base_alpha = 0.35 if mnxr_to_pathway else 1.0
    nx.draw_networkx_nodes(
        g, pos, nodelist=met_nodes, ax=ax, node_size=3, node_color="#3a7bd5",
        alpha=base_alpha, label="metabolite",
    )

    if not mnxr_to_pathway:
        nx.draw_networkx_nodes(
            g, pos, nodelist=rxn_nodes, ax=ax, node_size=3, node_color="#e67e22", label="reaction"
        )
    else:
        unhighlighted = [n for n in rxn_nodes if n[1] not in mnxr_to_pathway]
        nx.draw_networkx_nodes(
            g, pos, nodelist=unhighlighted, ax=ax, node_size=3, node_color="#e67e22",
            alpha=base_alpha, label="reaction (other)",
        )
        pathway_ids = list(pathway_names)
        for i, pid in enumerate(pathway_ids):
            nodes = [n for n in rxn_nodes if mnxr_to_pathway.get(n[1]) == pid]
            if not nodes:
                continue
            nx.draw_networkx_nodes(
                g, pos, nodelist=nodes, ax=ax, node_size=14,
                node_color=PATHWAY_PALETTE[i % len(PATHWAY_PALETTE)],
                edgecolors="black", linewidths=0.2,
                label=f"{pathway_names[pid]} ({pid})",
            )

    ax.set_title(
        f"{title}\n(node position = UMAP force-projection layout, not an embedding)",
        fontsize=11,
    )
    ax.legend(loc="upper right", fontsize=7, markerscale=2)
    ax.set_xticks([])
    ax.set_yticks([])
    for spine in ax.spines.values():
        spine.set_visible(False)
    fig.tight_layout()

    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out.with_suffix(".png"), dpi=300)
    fig.savefig(out.with_suffix(".svg"))
    plt.close(fig)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--facet-dir", required=True, type=Path)
    ap.add_argument("--out", required=True, type=Path)
    ap.add_argument("--title", default=None)
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--weight-attr", default="w_C",
                     help="edge attribute to treat as conductance (default w_C, carbon-atom-transit weight)")
    ap.add_argument("--spectral-dims", type=int, default=30,
                     help="rank of the Laplacian eigenembedding approximating pairwise conductance distance, before UMAP")
    ap.add_argument("--distance-method", default="spectral",
                     choices=["spectral", "shortest-path"],
                     help="how to turn the conductance-weighted graph into a distance UMAP can consume")
    ap.add_argument("--n-neighbors", type=int, default=30)
    ap.add_argument("--min-dist", type=float, default=0.05,
                     help="minimum embedded separation UMAP will allow; >1 forbids clusters from forming at all")
    ap.add_argument("--spread", type=float, default=1.0)
    ap.add_argument("--init", default="random", choices=["random", "spectral", "pca"])
    ap.add_argument("--repulsion-strength", type=float, default=1.0)
    ap.add_argument("--radial-gamma", type=float, default=1.0,
                     help="<1 compresses points beyond the median radius back toward it (1.0 = off). "
                          "Prefer --min-edge-weight to cut a weakly-tethered appendage at source: this "
                          "squashes every radius toward one value, homogenising real structure too.")
    ap.add_argument("--min-edge-weight", type=float, default=None,
                     help="drop edges whose conductance is below this before laying out. A near-open-circuit "
                          "edge tethers a whole appendage at a distance ~100x the bulk, which is what the "
                          "radial compression was covering for.")
    ap.add_argument("--kegg-reac-prop", type=Path, default=None,
                     help="MetaNetX reac_prop.tsv, for MNXR -> keggR cross-reference")
    ap.add_argument("--kegg-pathways", type=Path, default=None,
                     help="ModelSEED-style KEGG.pathways tsv (pathway id -> pipe-separated R##### list)")
    ap.add_argument("--highlight-pathway", action="append", default=[],
                     help="KEGG pathway id (e.g. map00010) to highlight; repeatable")
    args = ap.parse_args()

    g = load_graph(args.facet_dir, args.weight_attr, args.min_edge_weight)
    rxn_nodes = sorted(n for n in g.nodes if n[0] == "rxn")
    met_nodes = sorted(n for n in g.nodes if n[0] == "met")
    # Keyword-only: these arguments are all scalars of compatible types, so passing them
    # positionally shifts silently rather than raising if the signature ever changes.
    pos = layout(
        g,
        distance_method=args.distance_method,
        weight_attr=args.weight_attr,
        spectral_dims=args.spectral_dims,
        seed=args.seed,
        n_neighbors=args.n_neighbors,
        min_dist=args.min_dist,
        spread=args.spread,
        init=args.init,
        repulsion_strength=args.repulsion_strength,
        radial_gamma=args.radial_gamma,
    )
    title = args.title or args.facet_dir.name

    mnxr_to_pathway, pathway_names = None, None
    if args.highlight_pathway:
        assert args.kegg_reac_prop and args.kegg_pathways, \
            "--highlight-pathway needs --kegg-reac-prop and --kegg-pathways"
        mnxr_to_pathway, pathway_names = load_pathway_membership(
            args.kegg_reac_prop, args.kegg_pathways, args.highlight_pathway
        )
        for pid in args.highlight_pathway:
            n = sum(1 for p in mnxr_to_pathway.values() if p == pid)
            print(f"{pid} ({pathway_names.get(pid, '?')}): {n} reactions matched in this graph")

    render(g, rxn_nodes, met_nodes, pos, title, args.out, mnxr_to_pathway, pathway_names)
    print(f"reactions={len(rxn_nodes)} metabolites={len(met_nodes)} edges={g.number_of_edges()}")
    print(f"wrote {args.out.with_suffix('.png')} and .svg")


if __name__ == "__main__":
    main()

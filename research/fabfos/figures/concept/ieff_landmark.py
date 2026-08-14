"""Lay the reaction network out from an 8-dimensional elemental landmark profile.

One universal-ground solve per (element, direction). Four landmarks, one per biomass
element -- oxaloacetate for C, L-glutamate for N, phosphate for P, L-cysteine for S -- each
solved on *its own element's* atom graph, in both directions: current injected at the
landmark (what it feeds) and current drained to it (what feeds it). Eight solves total,
eight numbers per reaction, cosine into UMAP.

**This is a behavioural profile, not a pairwise metric, and that is a known trade.** Two
reactions sit together here because they respond alike to the same eight probes, not
because current flows between them. The 296-column version of this construction was measured
against the exact pairwise distance on 3,327 reactions at top-10 neighbour overlap
0.001-0.002 against a random baseline of 0.003, robust from 16 to 3,327 columns -- see the
README's layout-input section. It is here because the exact sweep does not fit the universe
(332-1,175 h) and this costs eight solves. Read the resulting figure as "reactions with
similar elemental draw", never as "reactions that exchange mass".

**The biomass ratio is a weight on the blocks.** C:N:P:S at 100:10:1:1 (the rounded
287:17:1:0.8) is an elemental *composition*; applying it to routing current is a prior for
how much each element's flow should count, not a derivation from stoichiometry. Features are
log10 currents, and each element's block is scaled by sqrt(w) so that its contribution to the
cosine inner product -- which is quadratic in the feature scale -- is proportional to w.

The layout universe is the carbon giant component: a reaction with no atom of some element
scores zero in that block rather than being dropped.

**Keep all eight columns.** Two facts about them point opposite ways and the measurement
settles it. The reverse solve looks redundant -- in/out correlate 0.99 (N), 0.99 (P), 1.00
(S), and only carbon's 0.77 -- and P and S together are 0.8% of the cosine inner product.
But magnitude share is not information share: P and S correlate with carbon at -0.01 to 0.08,
so they are the near-orthogonal axes, and the whole 8-column matrix has a participation ratio
of only 2.14. Mean KEGG scatter index on the EPI300 GEM lane, one layout regime throughout:
all 8 = 0.707; drop the P/S reverse = 0.762; drop N/P/S reverse = 0.758; forward only = 0.776;
C_out+C_in+N_out = 0.888, with TCA worse than random. A duplicated column is not information
but it does double that element's block magnitude, which is a weight correction toward the
intended ratio -- that is why the "redundant" columns still earn their place.

    python ieff_landmark.py --scale kegg --out cache/lm_kegg
    python ieff_landmark.py --scale gpr --gpr-table .../gpr_gem.parquet --out cache/lm_gem
"""
import argparse
import json
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
import umap

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from atom_graph import build_atom_graph, restrict_to_giant             # noqa: E402
from ieff_ground import GroundSystem, edge_current, newton_rhs         # noqa: E402
from ieff_sweep import gpr_medium, kegg_annotated, star_medium         # noqa: E402
from ieff_layout import (PANEL, incidence, metabolite_positions,       # noqa: E402
                         pathway_members, render, scatter_index)

# element -> (landmark metabolite, biomass weight). MetaNetX 4.5 ids, from chem_prop.tsv.
LANDMARKS = {
    "C": ("MNXM46", 100.0),        # oxaloacetate
    "N": ("MNXM741173", 10.0),     # L-glutamate
    "P": ("MNXM9", 1.0),           # phosphate
    "S": ("MNXM738068", 1.0),      # L-cysteine
}
# Currents span ~14 decades and reach 0 for unreachable reactions; the floor is what makes
# log10 finite and sets the dynamic range the cosine sees.
FLOOR = 1e-15


def medium_for(scale, gpr_table):
    if scale == "medium":
        return star_medium()
    if scale == "kegg":
        return star_medium() | kegg_annotated()
    if scale == "gpr":
        if gpr_table is None:
            raise SystemExit("--scale gpr needs --gpr-table")
        return gpr_medium(gpr_table)
    return None                                    # universe: every reaction in the bake


def solve_landmark(element, met, medium, leak, t0):
    """Two per-reaction current vectors for one element: fed-by-landmark, and feeding-it.

    The rectified network is directional, so negating the injection is a genuinely different
    solve rather than a sign flip of the same field -- which is the point of taking both.
    """
    g, term, met_sym = build_atom_graph(element, medium)
    g, term = restrict_to_giant(g, term)
    S = GroundSystem(g, term, leak=leak)
    code_of = {s: c for c, s in met_sym.items()}
    if met not in code_of:
        raise SystemExit(f"{met} is not in the bake vocabulary")
    code = code_of[met]
    src = [i for i in range(g.n) if g.nodes[i][0] == code]
    print(f"[{time.time()-t0:.1f}s] {element}: giant n={g.n} m={g.m} rxns={len(term)}; "
          f"{met} has {len(src)} {element} atom nodes", flush=True)
    if not src:
        # A landmark carrying no atom of its own element in this medium would make the whole
        # block zero, which reads as "nothing draws" rather than as a missing measurement.
        raise SystemExit(f"{met} has no {element} atom node in this medium")

    I, _ = S.injection_at(src)
    reuse = S.full_reuse()
    out = {}
    for sign, tag in ((+1.0, "out"), (-1.0, "in")):
        phi, nit, why = newton_rhs(S.B, S.gp, S.gm, sign * I, S.keep, reuse)
        ie = np.abs(edge_current(S.B, S.gp, S.gm, phi))
        into_ground = float(-(S.B.T @ ie)[S.ground])
        print(f"[{time.time()-t0:.1f}s]   {element}.{tag}: {nit} Newton iterations "
              f"({why}), {into_ground:.9f} A at ground", flush=True)
        out[f"{element}_{tag}"] = pd.Series(S.attribute(ie), index=S.rxn_names)
    return out


def features(cols, order, weights):
    """The 8 columns as a weighted log-current matrix over ``order``."""
    X = np.zeros((len(order), len(cols)), float)
    for j, (name, s) in enumerate(cols.items()):
        X[:, j] = s.reindex(order).fillna(0.0).to_numpy()
    X = np.log10(np.maximum(X, FLOOR)) - np.log10(FLOOR)      # >= 0, unreachable -> 0
    w = np.array([weights[n.split("_")[0]] for n in cols], float)
    return X * np.sqrt(w / w.sum())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--scale", choices=["medium", "kegg", "gpr", "universe"], default="kegg")
    ap.add_argument("--gpr-table", type=Path, default=None)
    ap.add_argument("--out", type=Path, required=True, help="output stem (no extension)")
    ap.add_argument("--leak", type=float, default=1e-6)
    ap.add_argument("--columns", default="C_out,C_in,N_out,N_in,P_out,P_in,S_out,S_in",
                    help="which (element, direction) columns enter the layout. All eight is "
                         "the measured best; see the module docstring for the subsets tried.")
    ap.add_argument("--features", type=Path, default=None,
                    help="re-lay-out from a saved .features.parquet instead of re-solving")
    ap.add_argument("--n-neighbors", type=int, default=30)
    ap.add_argument("--min-dist", type=float, default=1.2)
    ap.add_argument("--spread", type=float, default=2.5)
    ap.add_argument("--radial-gamma", type=float, default=0.4)
    ap.add_argument("--seed", type=int, default=42, help="0 drops random_state, which is "
                    "what lets UMAP use every core")
    ap.add_argument("--n-jobs", type=int, default=-1)
    ap.add_argument("--clip", type=float, default=1.5)
    ap.add_argument("--title", default="MetaNetX reaction network, CNPS landmark profile "
                                       "(8 ECSPr universal-ground solves, cosine)")
    args = ap.parse_args()
    if args.min_dist > args.spread:
        raise SystemExit("UMAP requires --min-dist <= --spread")

    t0 = time.time()
    args.out.parent.mkdir(parents=True, exist_ok=True)
    reach = {}
    if args.features:
        F = pd.read_parquet(args.features)
        print(f"[{time.time()-t0:.1f}s] features {F.shape} from {args.features}", flush=True)
    else:
        medium = medium_for(args.scale, args.gpr_table)
        cols = {}
        for el, (met, _w) in LANDMARKS.items():
            cols.update(solve_landmark(el, met, medium, args.leak, t0))
        # The carbon giant component defines the points; the other elements score onto it.
        order = list(cols["C_out"].index)
        weights = {el: w for el, (_m, w) in LANDMARKS.items()}
        F = pd.DataFrame(features(cols, order, weights), index=order, columns=list(cols))
        reach = {n: float((cols[n].reindex(order).fillna(0.0) > 0).mean()) for n in cols}
        print(f"[{time.time()-t0:.1f}s] features {F.shape}; fraction of reactions drawing "
              f"current per column:", flush=True)
        for n, f in reach.items():
            print(f"  {n:<7} {f:.4f}", flush=True)
        # All eight are saved whatever the layout uses, so a column choice is re-decidable
        # from the parquet without paying for the solves again.
        F.to_parquet(args.out.with_name(args.out.name + ".features.parquet"))

    keep = [c for c in args.columns.split(",") if c]
    missing = [c for c in keep if c not in F.columns]
    if missing:
        raise SystemExit(f"no such column(s): {missing}; have {list(F.columns)}")
    # A row that is zero across every kept column has no direction on the unit sphere, so
    # cosine is undefined for it -- dropping it is the honest handling, and it is named.
    live = F[keep].to_numpy().sum(1) > 0
    if not live.all():
        print(f"dropping {int((~live).sum())} reactions that draw no current in any of "
              f"{keep}", flush=True)
    F = F[live]
    order = list(F.index)
    X = F[keep].to_numpy()
    print(f"[{time.time()-t0:.1f}s] layout on {keep}: {X.shape}", flush=True)

    kw = dict(n_components=2, n_neighbors=args.n_neighbors, min_dist=args.min_dist,
              spread=args.spread, metric="cosine", init="random")
    if args.seed:
        # random_state pins the layout and forces UMAP single-threaded; --seed 0 trades
        # reproducibility for the cores.
        kw["random_state"] = args.seed
    else:
        kw["n_jobs"] = args.n_jobs
    xy = umap.UMAP(**kw).fit_transform(X)
    bad = ~np.isfinite(xy).all(1)
    if bad.any():
        raise RuntimeError(f"{int(bad.sum())} points have non-finite coordinates")
    if args.radial_gamma < 1.0:
        from network_concept import compress_radial_outliers
        xy = compress_radial_outliers(xy, args.radial_gamma)
    print(f"[{time.time()-t0:.1f}s] umap {xy.shape}", flush=True)

    members, names = pathway_members()
    lab = {}
    for pid in PANEL:
        for r in members.get(pid, ()):
            lab.setdefault(r, pid)
    labels = np.array([lab.get(r, "") for r in order])
    rng = np.random.default_rng(7)
    metrics = {}
    print("scatter index (1.0 = randomly placed, lower = pathway holds together):")
    for pid in PANEL:
        ii = np.where(labels == pid)[0]
        if len(ii) > 2:
            metrics[pid] = float(scatter_index(xy, ii, rng))
            print(f"  {pid} {names[pid][:34]:<36} n={len(ii):<4} {metrics[pid]:.3f}",
                  flush=True)
    if metrics:
        print(f"  mean {np.mean(list(metrics.values())):.3f}", flush=True)

    net, sub = None, "reactions only; metabolites are the medium, not points"
    if args.gpr_table:
        met, pairs, missing = incidence(args.gpr_table, order)
        net = (metabolite_positions(xy, pairs, len(met)), pairs)
        sub = "metabolites drawn at the mean of their reactions, and do not enter the layout"
    render(xy, labels, names, f"{args.title}\n({sub}. Position = UMAP force projection)",
           args.out, clip=args.clip)
    np.save(args.out.with_name(args.out.name + ".xy.npy"), xy)
    json.dump(dict(scale=args.scale, leak=args.leak, landmarks=LANDMARKS, n=len(order),
                   columns=keep, reach=reach, scatter=metrics, argv=sys.argv,
                   regime=dict(n_neighbors=args.n_neighbors, min_dist=args.min_dist,
                               spread=args.spread, radial_gamma=args.radial_gamma,
                               seed=args.seed, metric="cosine")),
              open(args.out.with_name(args.out.name + ".metrics.json"), "w"), indent=1)
    print(f"[{time.time()-t0:.1f}s] done", flush=True)


if __name__ == "__main__":
    main()

"""Lay the figure out on the *genome* instead of on a fitted embedding.

The ring in ``ring_layout`` is a UMAP coordinate: an angle fitted to the pairwise I_eff
distance, which makes the circle a layout and nothing else. Here the circle is the **host
chromosome** -- angle is genomic position, read off the DH10B annotation -- and metabolism is
carried entirely by the edges. Nothing about the drawing is fitted, so two figures drawn from
different networks are directly comparable point for point.

Four concentric rings, outermost first::

    host genes           angle = CDS midpoint / chromosome length
    host reactions       angle = circular mean of the genes that encode it (subunits collapse)
    insert reactions     angle = circular mean of the insert ORFs that encode it
    insert genes         angle = ORF midpoint / insert length

The two gene rings are two different coordinates sharing one circle: 4.69 Mb of chromosome
outside, 44.5 kb of cloned DNA inside, each wrapped once. That is the point of the figure --
an insert gene's chemistry reaches into the host at whatever host angle its metabolites live,
so a radial-looking bundle of edges is an insert region plugging into one region of host
metabolism and a fan is one plugging in everywhere.

A reaction the host and the insert both encode is drawn **twice**, once in each reaction ring,
because it has two genomic positions and the figure's coordinate is genomic position.

Metabolites keep the rule the other figures use: the mean of the positions of the reactions
they move an atom through, so a cofactor shared right around the circle averages to the centre.

**Edges are arcs, not chords.** Every edge is a circular arc subtending the same angle at its
own centre (60 degrees by default), bowed so that travelling from source to target runs
*clockwise* along the curve. Direction is the chemistry's: substrate -> reaction -> product.
A constant subtended angle means the bow is proportional to the chord, so the arcs stay a
consistent visual family from the shortest edge to the one crossing the circle.

    python chromosome_ring.py --sparse cache/_old3/pairwise_Ieff_epi300_clone2.npz \
        --gpr-table ../../../data/fabfos/e_coli_epi300/gpr/gpr_epi300_clone2.parquet \
        --gem-table ../../../data/fabfos/e_coli_epi300/gpr/gpr_gem.parquet \
        --gbk ../../../data/originals/genomes/e_coli_dh10b/genome/NC_010473.1.gbk \
        --fosmid-gff ../../../data/fabfos/scadc_fosmids/annotations/fosmids.gff \
        --fosmid-table ../../../data/fabfos/scadc_fosmids/gpr/gpr_4lane.parquet \
        --insert pool33_TTGTCGGT:... --out cache/chrom/clone2
"""
import argparse
import json
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt                                      # noqa: E402
from matplotlib.collections import LineCollection                    # noqa: E402

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from ieff_layout import (EDGE_ALPHA, EDGE_BUCKETS, EDGE_COLOR,       # noqa: E402
                         EDGE_WIDTH, MET_SIZE, load_sparse)

# The alpha an edge gets at the cutoff degree and at degree 1. The floor is not zero: an edge
# that survives the cutoff is one the drawing is asserting exists, and a fade to invisible
# would assert it and hide it at once.
ALPHA_LO, ALPHA_HI = 0.05, 0.75

TAU = 2.0 * np.pi

# Radial bands, outermost first. Each is (inner, outer) and the beeswarm packs within it; the
# gaps are what make the four rings read as four rings without any of them being drawn.
BAND = {"host_gene": (1.02, 1.10),
        "host_rxn": (0.84, 0.96),
        "clone_rxn": (0.44, 0.62),
        "clone_gene": (0.30, 0.36)}
# Radial spacing of the beeswarm's slots. A band holds (width / SLOT) of them, and the ring
# only needs to be as thick as its worst pile-up: one insert ORF can encode dozens of
# reactions, which all land at exactly the same angle and can only be separated radially.
SLOT = 0.006
LAYER_COLOR = {"host_gene": "#6f6f6f",
               "host_rxn": "#909090",
               "clone_rxn_shared": "#ff9d3f",
               "clone_rxn_new": "#d62728",
               "clone_gene": "#7a3a3a"}
GUIDE = "#d8d8d8"


# ---------------------------------------------------------------- genomic coordinates

def gbk_genes(path):
    """``old_locus_tag`` -> CDS midpoint, plus the replicon length.

    The GEM's gene ids are DH10B's *old* locus tags (``ECDH10B_0002``); RefSeq's current ones
    (``ECDH10B_RS00005``) are a different series, so the old tag is the join key and reading
    ``/locus_tag`` instead silently matches nothing.
    """
    txt = Path(path).read_text()
    length = int(re.search(r"^LOCUS\s+\S+\s+(\d+) bp", txt, re.M).group(1))
    mid = {}
    # One CDS block at a time: its location line, then its qualifiers up to the next feature.
    for m in re.finditer(r"^     CDS             (\S[^\n]*(?:\n {21}[^ /][^\n]*)*)\n"
                         r"((?: {21}/[^\n]*\n(?: {22,}[^/\n][^\n]*\n)*)*)", txt, re.M):
        loc, quals = m.group(1).replace("\n", "").replace(" ", ""), m.group(2)
        tag = re.search(r'/old_locus_tag="([^"]+)"', quals)
        if not tag:
            continue
        pos = [int(x) for x in re.findall(r"\d+", loc)]
        if not pos:
            continue
        # join(...) wraps the origin for a handful of genes; the mean of its ends is then
        # wrong by half a genome, so take the first and last coordinate of the span given.
        mid[tag.group(1)] = 0.5 * (pos[0] + pos[-1])
    return mid, length


def gff_orfs(path, insert):
    """Prodigal GFF -> ORF name (``<insert>_<n>``) -> midpoint, plus the insert length."""
    mid, length = {}, None
    for line in open(path):
        if line.startswith("# Sequence Data:") and f'seqhdr="{insert} ' in line:
            length = int(re.search(r"seqlen=(\d+)", line).group(1))
            continue
        if line.startswith("#") or not line.startswith(insert + "\t"):
            continue
        p = line.split("\t")
        n = re.search(r"ID=\d+_(\d+)", p[8]).group(1)
        mid[f"{insert}_{n}"] = 0.5 * (int(p[3]) + int(p[4]))
    if length is None:
        raise SystemExit(f"{insert} has no sequence record in {path}")
    return mid, length


def beeswarm(theta, band, arc, slot=SLOT):
    """Radii within ``band``: the innermost slot free of a point already within ``arc``.

    ``ring_layout.beeswarm`` with the slot pitch exposed, because the pile-ups here are not
    the layout's mild crowding but exact ties -- every reaction one gene encodes shares that
    gene's angle exactly, so the ring has to be thick enough to stack them.
    """
    lo, hi = band
    slots = max(int(np.ceil((hi - lo) / slot)), 1)
    radii = np.linspace(lo, hi, slots)
    last = np.full(slots, -np.inf)
    out = np.empty(len(theta))
    for i in np.argsort(theta):
        t = theta[i]
        s = next((s for s in range(slots) if t - last[s] > arc), int(np.argmin(last)))
        out[i] = radii[s]
        last[s] = t
    return out


def circmean(angles):
    return float(np.arctan2(np.sin(angles).mean(), np.cos(angles).mean()) % TAU)


def reaction_angles(gene_angle, pairs):
    """Reaction -> circular mean of the angles of the genes that encode it.

    ``pairs`` is (reaction, gene). The mean is circular because the coordinate is: two
    subunits either side of the origin average to the origin, not to the far side. A multi-
    subunit complex spread right round the chromosome has no meaningful mean angle, and the
    resultant length is returned alongside so that can be seen rather than assumed.
    """
    by = {}
    for r, g in pairs:
        a = gene_angle.get(g)
        if a is not None:
            by.setdefault(r, []).append(a)
    ang, R = {}, {}
    for r, aa in by.items():
        aa = np.asarray(aa)
        ang[r] = circmean(aa)
        R[r] = float(np.hypot(np.cos(aa).mean(), np.sin(aa).mean()))
    return ang, R


# ---------------------------------------------------------------- incidence, with direction

def directed_incidence(gpr_table, src, element="C"):
    """(reaction row, metabolite, is_product) for every atom-carrying incidence.

    The same two calls the sweep made, so the drawn network is the solved one -- but keeping
    the substrate/product split ``ieff_layout.incidence`` collapses, because an arc that is
    drawn with a direction needs one. A metabolite on both sides yields both edges.
    """
    from atom_graph import build_atom_graph, restrict_to_giant
    from ieff_sweep import gpr_medium

    g, term, met_sym = build_atom_graph(element, gpr_medium(gpr_table))
    g, term = restrict_to_giant(g, term)
    row = {r: i for i, r in enumerate(src)}
    mets, trip, missing = {}, [], 0
    for r, (subs, prods) in term.items():
        i = row.get(r)
        if i is None:
            missing += 1
            continue
        for side, atoms in ((0, subs), (1, prods)):
            for a in atoms:
                m = met_sym[g.nodes[a][0]]
                trip.append((i, mets.setdefault(m, len(mets)), side))
    trip = np.unique(np.asarray(trip, np.int64), axis=0)
    return list(mets), trip, missing


# ---------------------------------------------------------------- arcs

def arc_segments(p0, p1, degrees, steps=16):
    """Polylines from ``p0`` to ``p1`` along a circular arc subtending ``degrees``.

    The arc is bowed so that travel from ``p0`` to ``p1`` is **clockwise** about the arc's own
    centre, which puts the centre 90 degrees clockwise of the direction of travel. A constant
    subtended angle fixes the radius at ``chord / (2 sin(a/2))``, so every edge in the figure
    is a segment of the same shape scaled to its own chord -- the bow reads as one visual
    family rather than as a quantity.
    """
    a = np.radians(degrees)
    d = p1 - p0
    c = np.hypot(d[:, 0], d[:, 1])
    c = np.where(c < 1e-12, 1e-12, c)
    R = c / (2.0 * np.sin(a / 2.0))
    # Perpendicular to travel, rotated -90 degrees: the side the centre of a clockwise arc is on.
    n = np.stack([d[:, 1], -d[:, 0]], axis=1) / c[:, None]
    centre = 0.5 * (p0 + p1) + n * (R * np.cos(a / 2.0))[:, None]
    t0 = np.arctan2(p0[:, 1] - centre[:, 1], p0[:, 0] - centre[:, 0])
    t = t0[:, None] - np.linspace(0.0, a, steps)[None, :]
    return np.stack([centre[:, 0:1] + R[:, None] * np.cos(t),
                     centre[:, 1:2] + R[:, None] * np.sin(t)], axis=2)


def edge_alpha(deg, tone, cut, gamma):
    """Per-edge alpha from its metabolite's degree, and which edges are drawn at all.

    Two tones, because they answer differently and the ring figures already chose:

    ``bucket`` is ``ieff_layout.draw_network``'s exactly -- ``1/degree`` split into five
    quantile buckets, the densest undrawn and the rest at 0.85 / 0.2 / 0.05 / 0.01. It is a
    step, not a ramp: the degree<=2 bucket carries the drawing and everything above it is
    barely-there ink. That is what `ring_clone2` is drawn with and why it reads sparse.

    ``log`` is a continuous ``(log(cut/degree)/log(cut))**gamma`` with a hard cutoff. It shows
    more of the mid-degree structure, at the cost of the thousand faint curves the step tone
    suppresses.
    """
    if tone == "bucket":
        w = 1.0 / deg
        cuts = np.quantile(w, np.linspace(0, 1, EDGE_BUCKETS + 1)[1:-1])
        b = np.searchsorted(cuts, w, side="right")
        a = np.take([0.0] + EDGE_ALPHA[::-1], b)
        return a, b > 0, f"1/degree in {EDGE_BUCKETS} quantile buckets at {EDGE_ALPHA}"
    t = np.clip(np.log(cut / np.maximum(deg, 1)) / np.log(cut), 0.0, 1.0) ** gamma
    return (ALPHA_LO + (ALPHA_HI - ALPHA_LO) * t, deg <= cut,
            f"log(1/degree)^{gamma} in {ALPHA_LO}-{ALPHA_HI}, cutoff at degree {cut:g}")


def draw_arcs(ax, xy, mxy, trip, degrees, tone="bucket", cut=15, gamma=1.6, hot=None,
              host=True):
    """The reaction-metabolite drawing as arcs, toned by metabolite degree.

    Whichever tone, the reason is one: a currency metabolite incident to hundreds of reactions
    draws hundreds of curves, and their *sum* is what makes a drawing like this a hairball.
    """
    deg = np.bincount(trip[:, 1], minlength=len(mxy))[trip[:, 1]]
    # Source and target: substrate -> reaction, reaction -> product.
    prod = trip[:, 2] == 1
    src_xy = np.where(prod[:, None], xy[trip[:, 0]], mxy[trip[:, 1]])
    dst_xy = np.where(prod[:, None], mxy[trip[:, 1]], xy[trip[:, 0]])
    hot = np.zeros(len(trip), bool) if hot is None else hot
    t, keep, how = edge_alpha(deg, tone, cut, gamma)
    host = np.full(len(trip), bool(host))
    print(f"edges: {len(trip)} total; drew {int((keep & ~hot & host).sum())} host arcs at "
          f"{degrees:.0f} degrees clockwise, alpha by {how}; "
          f"{int((~keep).sum())} left undrawn", flush=True)
    m = keep & ~hot & host
    if m.any():
        rgba = np.tile(matplotlib.colors.to_rgba(EDGE_COLOR), (int(m.sum()), 1))
        rgba[:, 3] = t[m]
        ax.add_collection(LineCollection(arc_segments(src_xy[m], dst_xy[m], degrees),
                                         colors=rgba, linewidths=EDGE_WIDTH, zorder=0))
    # The insert's own incidences, on top and on the same tone rule -- they are the subject,
    # and they are 4% of the edges, so they are worth their own colour but not their own scale.
    m = keep & hot
    if m.any():
        print(f"{int(m.sum())} of those touch an insert reaction and are drawn coloured, "
              f"above the host's ({int((hot & ~keep).sum())} more were left undrawn)",
              flush=True)
        # Lifted off the host's tone rather than given their own: a hub edge stays faint when
        # it is the insert's too, because it is still saying nothing about where the insert is.
        rgba = np.tile(matplotlib.colors.to_rgba("#d62728"), (int(m.sum()), 1))
        rgba[:, 3] = np.clip(0.15 + 0.85 * t[m], 0.0, 1.0)
        ax.add_collection(LineCollection(arc_segments(src_xy[m], dst_xy[m], degrees),
                                         colors=rgba, linewidths=0.6, zorder=1))


# ---------------------------------------------------------------- main

def to_xy(theta, radius):
    """Genomic angle -> canvas. Position zero is at the top and coordinate runs clockwise,
    which is how a circular genome map is conventionally read."""
    a = np.pi / 2.0 - theta
    return np.stack([radius * np.cos(a), radius * np.sin(a)], axis=1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sparse", type=Path, required=True,
                    help="the solved network; only its reaction list is used")
    ap.add_argument("--gpr-table", type=Path, required=True,
                    help="union table: the reaction set's origin, and the incidence source")
    ap.add_argument("--gem-table", type=Path, required=True, help="host reaction <- gene")
    ap.add_argument("--gbk", type=Path, required=True, help="host genome, for CDS positions")
    ap.add_argument("--fosmid-gff", type=Path, required=True)
    ap.add_argument("--fosmid-table", type=Path, required=True, help="insert reaction <- ORF")
    ap.add_argument("--insert", required=True)
    ap.add_argument("--arc-degrees", type=float, default=60.0)
    ap.add_argument("--pack-arc", type=float, default=TAU / 3000,
                    help="radians a point occupies for the anti-overlap beeswarm; roughly a "
                         "marker's own angular width at radius 1 on the default canvas")
    ap.add_argument("--no-network", action="store_true", help="rings only, no edges")
    ap.add_argument("--plain-edges", action="store_true",
                    help="draw every edge in the host's grey instead of colouring the "
                         "insert's own incidences")
    ap.add_argument("--no-host-edges", action="store_true",
                    help="draw only the insert's incidences; the host keeps its rings but "
                         "loses its metabolism, which is the only way to make this coordinate "
                         "sparse -- see the edge-length note in README")
    ap.add_argument("--edge-tone", choices=("bucket", "log"), default="bucket",
                    help="'bucket' is the ring figures' own weighting, a steep step that "
                         "leaves only the low-degree edges visible; 'log' is a continuous "
                         "ramp with a hard --max-degree cutoff")
    ap.add_argument("--max-degree", type=float, default=15,
                    help="drop every edge whose metabolite touches more reactions than this; "
                         "it is the currency metabolites' summed ink, not any one curve, that "
                         "makes the hairball")
    ap.add_argument("--edge-gamma", type=float, default=1.6,
                    help="exponent on the log-degree tone; above 1 the mid degrees recede")
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--metrics-out", type=Path, default=None)
    ap.add_argument("--title", default="EPI300 chromosome and one SCADC fosmid insert: "
                                       "angle is genomic position, edges are metabolism")
    args = ap.parse_args()

    st = load_sparse(args.sparse)
    src = st["meta"]["src"]
    S = set(src)
    print(f"network: {len(src)} reactions", flush=True)

    u = pd.read_parquet(args.gpr_table, columns=["mnxr", "origin"])
    host_rxn = set(u.loc[u.origin == "host", "mnxr"]) & S
    clone_rxn = set(u.loc[u.origin == args.insert, "mnxr"]) & S

    gmid, glen = gbk_genes(args.gbk)
    gene_angle = {g: TAU * p / glen for g, p in gmid.items()}
    gem = pd.read_parquet(args.gem_table, columns=["feature_id", "feature_kind", "mnxr"])
    gem = gem[(gem.feature_kind == "gem_gene") & gem.mnxr.isin(host_rxn)]
    hr_angle, hr_R = reaction_angles(gene_angle, gem[["mnxr", "feature_id"]].values)
    host_genes = sorted(set(gem.feature_id) & set(gene_angle))
    print(f"host: {glen/1e6:.2f} Mb, {len(gmid)} CDS with an old locus tag; "
          f"{len(host_genes)} of them encode a network reaction; "
          f"{len(hr_angle)}/{len(host_rxn)} host reactions placed "
          f"({len(host_rxn) - len(hr_angle)} have no gene in the GEM lane and are dropped)",
          flush=True)

    omid, olen = gff_orfs(args.fosmid_gff, args.insert)
    orf_angle = {o: TAU * p / olen for o, p in omid.items()}
    fos = pd.read_parquet(args.fosmid_table, columns=["orf", "mnxr"])
    fos = fos[fos.orf.isin(orf_angle) & fos.mnxr.isin(clone_rxn)]
    cr_angle, cr_R = reaction_angles(orf_angle, fos[["mnxr", "orf"]].values)
    clone_genes = sorted(set(fos.orf))
    print(f"insert: {olen/1e3:.1f} kb, {len(omid)} ORFs, {len(clone_genes)} of them encode a "
          f"network reaction; {len(cr_angle)}/{len(clone_rxn)} insert reactions placed",
          flush=True)
    shared = sorted(set(cr_angle) & host_rxn)
    print(f"{len(shared)} of the insert's placed reactions are also encoded by the host "
          f"chromosome and so appear in both reaction rings", flush=True)

    # One record per drawn point: which ring, which angle, and which network row it is (genes
    # have none). A reaction encoded by both genomes is two records, which is the whole reason
    # this is a list rather than a per-reaction array.
    rows, layer, theta, net_row = [], [], [], []
    for g in host_genes:
        rows.append(g); layer.append("host_gene"); theta.append(gene_angle[g]); net_row.append(-1)
    idx = {r: i for i, r in enumerate(src)}
    for r, a in sorted(hr_angle.items()):
        rows.append(r); layer.append("host_rxn"); theta.append(a); net_row.append(idx[r])
    for r, a in sorted(cr_angle.items()):
        rows.append(r); layer.append("clone_rxn_shared" if r in host_rxn else "clone_rxn_new")
        theta.append(a); net_row.append(idx[r])
    for o in clone_genes:
        rows.append(o); layer.append("clone_gene"); theta.append(orf_angle[o]); net_row.append(-1)
    layer = np.array(layer); theta = np.array(theta); net_row = np.array(net_row)

    # The two clone_rxn classes are one ring: they are packed together, or a shared reaction
    # and a new one at the same angle would sit on top of each other.
    ring = np.array(["clone_rxn" if l.startswith("clone_rxn") else l for l in layer])
    radius = np.empty(len(theta))
    for k in set(ring):
        m = ring == k
        radius[m] = beeswarm(theta[m], BAND[k], args.pack_arc)
    xy = to_xy(theta, radius)

    met = mxy = trip = None
    if not args.no_network:
        mets, tri, missing = directed_incidence(args.gpr_table, src)
        print(f"incidence: {len(src)} reactions, {len(mets)} metabolites, {len(tri)} directed "
              f"edges; {missing} graph reactions with no row in the table", flush=True)
        # Re-key onto drawn points: a reaction in both rings carries its edges twice, once
        # from each of its genomic positions, which is what "the host also encodes this" looks
        # like when the coordinate is genomic.
        pt = {}
        for i, (l, nr) in enumerate(zip(layer, net_row)):
            if nr >= 0:
                pt.setdefault(nr, []).append(i)
        trip = np.array([(i, m, s) for r, m, s in tri for i in pt.get(r, ())], np.int64)
        # A metabolite whose only reactions were dropped for want of a gene has no position;
        # it is removed rather than drawn at the centre, where it would read as a cofactor.
        cnt = np.bincount(trip[:, 1], minlength=len(mets)).astype(float)
        keep = np.flatnonzero(cnt > 0)
        remap = np.full(len(mets), -1, np.int64)
        remap[keep] = np.arange(len(keep))
        mxy = np.stack([np.bincount(trip[:, 1], xy[trip[:, 0], d], minlength=len(mets))
                        for d in (0, 1)], axis=1)[keep] / cnt[keep, None]
        trip[:, 1] = remap[trip[:, 1]]
        met = [mets[i] for i in keep]
        if len(keep) < len(mets):
            print(f"{len(mets) - len(keep)} metabolites dropped: every reaction they touch "
                  f"was dropped for want of a gene", flush=True)

    # ---- render
    fig, ax = plt.subplots(figsize=(12, 12))
    for k, band in BAND.items():
        t = np.linspace(0, TAU, 721)
        r = 0.5 * (band[0] + band[1])
        ax.plot(r * np.cos(t), r * np.sin(t), color=GUIDE, lw=0.6, zorder=-1)
    if trip is not None:
        hot = None if args.plain_edges else np.isin(
            trip[:, 0], np.flatnonzero(np.char.startswith(layer.astype(str), "clone_rxn")))
        draw_arcs(ax, xy, mxy, trip, args.arc_degrees, args.edge_tone,
                  args.max_degree, args.edge_gamma, hot, not args.no_host_edges)
        ax.scatter(mxy[:, 0], mxy[:, 1], s=MET_SIZE, c="#4d79b5", alpha=0.65, linewidths=0,
                   zorder=2, label=f"metabolite, at the mean of its reactions (n={len(met)})")
    legend = {"host_gene": f"host gene ({len(host_genes)}, {glen/1e6:.2f} Mb chromosome)",
              "host_rxn": f"host reaction ({len(hr_angle)})",
              "clone_rxn_new": "insert reaction the host lacks",
              "clone_rxn_shared": "insert reaction the host also encodes",
              "clone_gene": f"insert ORF ({len(clone_genes)}, {olen/1e3:.1f} kb insert)"}
    for k, lab in legend.items():
        m = layer == k
        if not m.any():
            continue
        n = int(m.sum())
        # The insert's points are the subject and sit in the busiest part of the drawing, so
        # they get a white keyline: without it a red marker on a bundle of arcs is not a point.
        ins = k.startswith("clone")
        ax.scatter(xy[m, 0], xy[m, 1], s=11 if "rxn" in k else 6, c=LAYER_COLOR[k],
                   edgecolors="white" if ins else "black",
                   linewidths=0.45 if ins else (0.15 if "rxn" in k else 0.0), zorder=3,
                   label=lab if lab.endswith(")") else f"{lab} (n={n})")
    # The origin of each coordinate, marked once: without it the two rings are two circles of
    # points and nothing says where either genome starts.
    for k in ("host_gene", "clone_gene"):
        lo, hi = BAND[k]
        ax.plot([0, 0], [lo - 0.03, hi + 0.03], color="#333333", lw=0.9, zorder=4)
    ax.text(0, BAND["host_gene"][1] + 0.05, "0 / 4.69 Mb", ha="center", fontsize=7)
    ax.text(0, BAND["clone_gene"][1] + 0.02, f"0 / {olen/1e3:.1f} kb", ha="center", fontsize=7)

    ax.set_aspect("equal")
    ax.set_xlim(-1.25, 1.25); ax.set_ylim(-1.25, 1.25)
    import textwrap
    sub = (f"angle = genomic position (clockwise from the top); radius = ring + anti-overlap "
           f"only. Edges are {args.arc_degrees:.0f}-degree circular arcs running clockwise "
           f"from substrate to reaction to product.")
    ax.set_title("\n".join(textwrap.fill(ln, 104)
                           for ln in f"{args.title}\n({sub})".split("\n")), fontsize=10)
    ax.legend(loc="upper right", fontsize=7.5, markerscale=1.6, framealpha=0.92)
    ax.set_xticks([]); ax.set_yticks([])
    for s in ax.spines.values():
        s.set_visible(False)
    fig.tight_layout()
    args.out.parent.mkdir(parents=True, exist_ok=True)
    png, svg = args.out.with_name(args.out.name + ".png"), args.out.with_name(args.out.name + ".svg")
    fig.savefig(png, dpi=250); fig.savefig(svg)
    plt.close(fig)
    print(f"wrote {png} and .svg", flush=True)

    if args.metrics_out:
        R = np.array(list(hr_R.values()))
        m = dict(network=str(args.sparse), n_network_reactions=len(src),
                 genome_bp=glen, insert_bp=olen,
                 host_genes_drawn=len(host_genes), host_reactions_drawn=len(hr_angle),
                 host_reactions_dropped_no_gene=len(host_rxn) - len(hr_angle),
                 insert_orfs_drawn=len(clone_genes), insert_reactions_drawn=len(cr_angle),
                 insert_reactions_dropped=len(clone_rxn) - len(cr_angle),
                 shared_with_host=len(shared),
                 host_reaction_resultant_length=dict(
                     median=float(np.median(R)), p05=float(np.quantile(R, 0.05)),
                     frac_below_0_5=float((R < 0.5).mean())),
                 metabolites=len(met) if met else 0,
                 argv=sys.argv)
        args.metrics_out.parent.mkdir(parents=True, exist_ok=True)
        json.dump(m, open(args.metrics_out, "w"), indent=1)
        print(f"wrote {args.metrics_out}", flush=True)


if __name__ == "__main__":
    main()

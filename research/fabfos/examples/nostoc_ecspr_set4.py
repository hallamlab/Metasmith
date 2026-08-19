#!/usr/bin/env python3
"""set4 over the community measurement: what each member carries FOR the others.

    mamba run -n figure-net python research/fabfos/examples/nostoc_ecspr_set4.py table
    mamba run -n figure-net python research/fabfos/examples/nostoc_ecspr_set4.py chord
    mamba run -n figure-net python research/fabfos/examples/nostoc_ecspr_set4.py chord --member NOS

Every ECSPr product is an ABSOLUTE measurement of one network, so every comparison
is a subtraction the caller performs. This performs the community-versus-alone one:
for member X, the three-member ``bl-on`` network with X injecting, against X's own
singleton, both read at X's OWN metabolite copy. Positive means X's metabolism
carries more current there in company than alone -- what it contributes.

WHAT THE NUMBER IS, since it is not what the sibling `figure` scope's set4 chord
plots. That panel shades each axis by a directed effective CONDUCTANCE solved per
(source, sink) pair. The ground products carry no such column, and re-solving 79
pairs x 4 elements x 6 networks is a fresh measurement, not a read. So the value
here is a NODE-level change in ``throughput`` -- current handled at the axis's SINK
metabolite -- mapped onto the set4 axes. It is a weaker claim: an axis is shaded by
what happens at its endpoint, not by what happens along it, so two axes sharing a
sink necessarily get the same number. Measuring the 79 axes two-terminal on these
networks would fix that and remains available.

THREE CORRECTIONS, each of which changes the answer:

* Global dilution. Most of the injected current LEAVES X's copy across the bridges,
  so the raw difference is negative almost everywhere and says only "there are now
  neighbours". The headline number is therefore a CENTRED LOG RATIO: the floored
  log2 ratio minus the MEAN log2 ratio over the LIVE nodes of X's copy in that
  element graph. That is Aitchison's clr difference written out -- subtracting the
  mean of the logs is dividing each side by its own geometric mean, and since both
  sides are read over the same node set the two closures collapse into one
  subtraction. This correction is load-bearing, not cosmetic: the average live
  carbon node loses 0.6-1.0 log2 purely because there are now two other copies to
  spread into, and uncentred that reads as almost every carbon axis being
  "relieved". Nitrogen moves the same way and phosphorus and sulfur the other, so
  the correction is per element, not global. `--centre median` swaps in the median,
  which is what v1 shipped; it is the softer correction here, because the live-node
  log-ratio distribution is right-skewed in every member and element.
* Near-zero denominators. Without a floor, ``AcCoA -> lipid IV-A`` and friends
  manufacture log2 fold changes of 18-26 out of ~0/~0 and dominate the figure. Both
  sides are floored at ``--floor``; an axis whose sink is below it on both sides is
  ``below-floor`` and is not drawn.
* The port confound. X's own biomass precursors carry a port to ground when X grows
  alone and carry NONE in the community run, because the community's ports live in
  the RECEIVING copies. Their alone throughput is therefore inflated by the port
  drain, which biases their log-ratio DOWN. Such axes are marked ``port-confounded``
  rather than dropped: it makes a contribution conservative and a relief suspect,
  and a reader needs to see which is which. The clean band is Media -> Central.

``throughput`` is current HANDLED (max of in- and outflow), not net, so a rise at
X's node has two readings: X's own pathway carrying more, or current arriving from a
neighbour and passing through. The table separates them with ``dst_M_rank`` -- X's
gene coverage for that sink, ranked against the other two members. A rise where X
ranks 1st is X's machinery doing the work; where X ranks 3rd it is the opposite, and
``L-cysteine -> biotin`` in Allorhizobium is exactly that case: its coverage is the
lowest of the three, so the node lights up because biotin is reaching it, not because
it is making any. Filter on ``dst_M_rank`` before reading a rise as a contribution.

Chord geometry, encoding and constants are ported from the sibling scope's panel at
`scadc/lipidomics feat/fig-model:main/ecspr/method/chord_set4_serc.py` (+ `_style.py`),
which is not importable from here. The panels ship POSITIVE-ONLY (`--sign pos`): the
question is what a member carries FOR the others, the reliefs are where the port
confound bites hardest, and dropping them frees two channels. Opacity then runs
LINEARLY in the clr rather than on the reference's symlog ramp, whose 1e-4 linear
threshold is effectively a log scale and put the median response at alpha 0.8 -- with
seventy-odd chords that reads as texture, with eight it wastes the channel. And the
linestyle, freed from carrying sign, carries `dst_M_rank == 3` instead, so an axis
that rises where the member is the WORST of the three at making the sink cannot be
mistaken for a contribution.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import shutil
import sys
from pathlib import Path

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
CHUNK = REPO / "data/fabfos/nostoc/ecspr"
RESULTS = CHUNK / "results"
FIGS = CHUNK / "figures"

AXES_SRC = Path("/home/tony/agentic_workspace/projects/fabfos/anaerobic-digester"
                "/data/reference/ECSPr_axes/biomass.json")
AXES_SHA = "f63c6c8c7636b099aa872634f0a2a327222df6165057407e01fbf9c283d8c44c"

MEMBERS = ("NOS", "ERY", "RHI")
TRIPLE = "NOS-ERY-RHI_bl-on"
ELEMENTS = ("C", "N", "P", "S")

TABLE = FIGS / "delta_ieff_set4"
AXES_VENDORED = FIGS / "set4_axes.json"

VALUE_COL = {"clr": "clr", "median": "log2fc_med"}


ARCS = ("input", "central", "biomass")
ARC_TITLE = {"input": "Media", "central": "Central", "biomass": "Biomass"}

ELEMENT_COLORS = {"C": "#2a2a2a", "N": "#636efa", "P": "#ef553b", "S": "#ffa15a"}

COLLAPSE = {"input": False, "central": True, "biomass": False}

CATEGORY_ORDER = ("protein", "nucleotide", "phospholipid", "lps", "murein",
                  "cofactor", "met_salvage", "catabolic")

INPUT_EXTRA = {("P", "MNXM9")}

LABEL_ABBREV = {
    "UDP-N-acetyl-alpha-D-muramoyl-L-alanyl-gamma-D-glutamyl-meso-2,6-"
    "diaminopimeloyl-D-alanyl-D-alanine": "UDP-MurNAc-pentapeptide",
    "UDP-N-acetyl-alpha-D-glucosamine": "UDP-GlcNAc",
    "D-glyceraldehyde 3-phosphate": "G3P",
    "alpha-D-glucose 6-phosphate": "G6P",
    "beta-D-fructose 6-phosphate": "F6P",
    "alpha-D-ribofuranose 5-phosphate": "ribose 5-phosphate",
    "1-deoxy-D-xylulose 5-phosphate": "DXP",
    "D-erythrose 4-phosphate": "E4P",
    "phosphoenolpyruvate": "PEP",
    "(2R)-3-phosphoglycerate": "3-phosphoglycerate",
    "S-methyl-5'-thioadenosine": "MTA",
    "meso-2,6-diaminopimelate": "meso-DAP",
}

INK, INK_SOFT, INK_FAINT = "#1c1c1c", "#6b7280", "#c2c7ce"
ALPHA_MAX, LIN_THRESH, LW = 0.95, 1e-4, 1.4
ALPHA_MIN_SYMLOG, ALPHA_MIN_LINEAR = 0.05, 0.16
DPI_REVIEW = 220


def _cat_rank(c):
    return CATEGORY_ORDER.index(c) if c in CATEGORY_ORDER else len(CATEGORY_ORDER)


def _sha256(p: Path) -> str:
    h = hashlib.sha256()
    h.update(p.read_bytes())
    return h.hexdigest()


def _unlink(p: Path) -> None:
    if p.exists() or p.is_symlink():
        p.unlink()


def load_axes(vendored: Path = AXES_VENDORED) -> pd.DataFrame:
    if not vendored.exists():
        assert AXES_SRC.exists(), f"missing set4 source {AXES_SRC}"
        got = _sha256(AXES_SRC)
        assert got == AXES_SHA, f"set4 source sha changed: {got}"
        vendored.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(AXES_SRC, vendored)
    got = _sha256(vendored)
    assert got == AXES_SHA, f"vendored set4 sha changed: {got}"

    spec = json.loads(vendored.read_text())
    rows = []
    for axis_id, r in spec.items():
        assert len(r["source"]) == 1 and len(r["sink"]) == 1, f"{axis_id} not 1->1"
        rows.append({"axis_id": axis_id, "element": r["element"],
                     "src_mnxm": r["source"][0], "dst_mnxm": r["sink"][0],
                     "category": r["category"], "route": r["route"],
                     "module_name": r["module_name"],
                     "description": r["description"]})
    df = pd.DataFrame(rows)
    assert len(df) == 79, f"expected 79 axes, got {len(df)}"
    return df


def gene_coverage() -> pd.DataFrame:
    M: dict[str, pd.Series] = {}
    for pair in ("NOS-ERY_bl-on", "NOS-RHI_bl-on", "ERY-RHI_bl-on"):
        b = pd.read_parquet(CHUNK / "networks" / pair / "bridges.parquet")
        for org, col in ((b.org_a.iloc[0], "M_a"), (b.org_b.iloc[0], "M_b")):
            s = b.set_index(["element", "metabolite"])[col]
            s = s[~s.index.duplicated()]
            if org in M:
                x, y = M[org].align(s, join="inner")
                assert np.allclose(x, y), f"{org} M disagrees between pair tables"
                s = M[org].combine_first(s)
            M[org] = s
    df = pd.DataFrame(M)
    return df.join(df.rank(axis=1, ascending=False, method="min")
                     .add_suffix("_rank"))


def _read_ground(network: str) -> dict[str, pd.DataFrame]:
    d = RESULTS / network / "ecspr-ground_results"
    assert d.is_dir(), f"missing {d}"
    seen: dict[str, pd.DataFrame] = {}
    for f in sorted(d.glob("*.parquet")):
        df = pd.read_parquet(f)
        for cid, g in df.groupby("condition_id", observed=True):
            g = g.sort_values("metabolite").reset_index(drop=True)
            if cid in seen:
                prev = seen[cid]
                assert (len(prev) == len(g)
                        and (prev.metabolite.values == g.metabolite.values).all()), \
                    f"{cid} duplicated with a different node set"
                diff = np.nanmax(np.abs(prev.throughput.values - g.throughput.values))
                assert diff < 1e-9, f"{cid} duplicates disagree by {diff:g}"
                continue
            seen[str(cid)] = g
    assert seen, f"no ground products under {d}"
    return seen


def _direction(cid: str) -> str:
    return cid.split("|")[1]


def _own_copy(g: pd.DataFrame, member: str) -> pd.DataFrame:
    pref = f"{member}:"
    own = g[g.metabolite.astype(str).str.startswith(pref)].copy()
    own["mnxm"] = own.metabolite.astype(str).str.slice(len(pref))
    return own


def _member_nodes(member: str, triple: str, floor: float) -> pd.DataFrame:
    alone = _read_ground(member)
    comm = _read_ground(triple)

    a_cids = {c: f for c, f in alone.items() if _direction(c) == f"{member}->{member}"}
    c_cids = {c: f for c, f in comm.items()
              if _direction(c).startswith(f"{member}->")}
    assert len(a_cids) == 4, f"{member} alone: expected 4 elements, got {len(a_cids)}"
    assert len(c_cids) == 4, f"{member} in {triple}: got {len(c_cids)}"

    out = []
    for el in ELEMENTS:
        ac = next(c for c in a_cids if c.split("|")[2] == el)
        cc = next(c for c in c_cids if c.split("|")[2] == el)
        al = _own_copy(a_cids[ac], member)
        co = _own_copy(c_cids[cc], member)

        al = al.set_index("mnxm")[["throughput", "role", "in_graph", "port"]]
        co = co.set_index("mnxm")[["throughput", "role", "in_graph"]]
        al.columns = ["th_al", "role_al", "in_graph_al", "port_al"]
        co.columns = ["th_co", "role_co", "in_graph_co"]
        j = al.join(co, how="inner")

        l2 = np.log2(np.maximum(j.th_co, floor) / np.maximum(j.th_al, floor))
        live = (j.th_al >= floor) | (j.th_co >= floor)
        j["log2fc_raw"] = l2
        j["element"] = el
        j["centre_mean"] = float(np.mean(l2[live]))
        j["centre_median"] = float(np.median(l2[live]))
        j["n_live"] = int(live.sum())
        j["clr"] = l2 - j["centre_mean"]
        j["log2fc_med"] = l2 - j["centre_median"]
        out.append(j.reset_index())

    df = pd.concat(out, ignore_index=True)
    df["member"] = member
    return df


def build_table(floor: float, triple: str) -> pd.DataFrame:
    axes = load_axes()
    names = pd.read_parquet(CHUNK / "metabolite_names.parquet")
    label = names.drop_duplicates("mnxm").set_index("mnxm")["name"]

    axes = axes.assign(src_label=axes.src_mnxm.map(label).fillna(axes.src_mnxm),
                       dst_label=axes.dst_mnxm.map(label).fillna(axes.dst_mnxm))
    node_arc, _members, central_mets, input_nodes = assign_arcs(axes)

    def arc_of(element, mnxm):
        if mnxm in central_mets:
            return "central"
        return "input" if (element, mnxm) in input_nodes else "biomass"

    cov = gene_coverage()

    rows = []
    centres = {}
    for member in MEMBERS:
        nodes = _member_nodes(member, triple, floor)
        for el, g in nodes.groupby("element"):
            centres[(member, f"{el} mean")] = float(g.centre_mean.iloc[0])
            centres[(member, f"{el} med")] = float(g.centre_median.iloc[0])
            centres[(member, f"{el} live")] = int(g.n_live.iloc[0])
        idx = nodes.set_index(["element", "mnxm"])

        for a in axes.itertuples():
            rec = {"member": member, "element": a.element, "axis_id": a.axis_id,
                   "module_name": a.module_name, "category": a.category,
                   "route": a.route, "description": a.description,
                   "src_mnxm": a.src_mnxm, "dst_mnxm": a.dst_mnxm,
                   "src_name": label.get(a.src_mnxm, a.src_mnxm),
                   "dst_name": label.get(a.dst_mnxm, a.dst_mnxm),
                   "src_arc": arc_of(a.element, a.src_mnxm),
                   "dst_arc": arc_of(a.element, a.dst_mnxm)}

            if (a.element, a.dst_mnxm) in cov.index:
                c = cov.loc[(a.element, a.dst_mnxm)]
                rec.update(dst_bridged=True,
                           dst_M=float(c[member]),
                           dst_M_rank=int(c[f"{member}_rank"]),
                           dst_M_best=str(cov.loc[(a.element, a.dst_mnxm),
                                                  list(MEMBERS)].idxmax()))
            else:
                rec.update(dst_bridged=False, dst_M=np.nan,
                           dst_M_rank=0, dst_M_best="")

            missing = [m for m in (a.src_mnxm, a.dst_mnxm)
                       if (a.element, m) not in idx.index]
            if missing:
                rec.update(state="off-network",
                           reason=f"absent from the {a.element} graph: "
                                  + ",".join(missing))
                rows.append(rec)
                continue

            s = idx.loc[(a.element, a.src_mnxm)]
            d = idx.loc[(a.element, a.dst_mnxm)]
            rec.update(th_al_src=float(s.th_al), th_co_src=float(s.th_co),
                       th_al_dst=float(d.th_al), th_co_dst=float(d.th_co),
                       delta_dst=float(d.th_co - d.th_al),
                       log2fc_raw=float(d.log2fc_raw),
                       clr=float(d.clr),
                       log2fc_med=float(d.log2fc_med),
                       centre_mean=float(d.centre_mean),
                       centre_median=float(d.centre_median),
                       role_al_dst=str(d.role_al), role_co_dst=str(d.role_co))

            if not (int(s.in_graph_al) and int(s.in_graph_co)
                    and int(d.in_graph_al) and int(d.in_graph_co)):
                rec.update(state="off-network",
                           reason="endpoint off the largest connected component "
                                  "in at least one of the two graphs")
            elif d.th_al < floor and d.th_co < floor:
                rec.update(state="below-floor",
                           reason=f"sink below the {floor:g} mass floor on both "
                                  "sides; the ratio would be manufactured")
            elif str(d.role_al) == "precursor":
                rec.update(state="port-confounded",
                           reason="sink is a biomass precursor with a port to "
                                  "ground when alone and none in community; its "
                                  "alone throughput is inflated, so the log-ratio "
                                  "is biased DOWN (contribution conservative, "
                                  "relief suspect)")
            else:
                rec.update(state="ok", reason="")
            rows.append(rec)

    return pd.DataFrame(rows), centres


def table(args) -> int:
    FIGS.mkdir(parents=True, exist_ok=True)
    df, centres = build_table(args.floor, args.triple)

    for ext, writer in (("parquet", lambda p: df.to_parquet(p, index=False)),
                        ("tsv", lambda p: df.to_csv(p, sep="\t", index=False,
                                                    float_format="%.6g"))):
        p = Path(f"{TABLE}.{ext}")
        _unlink(p)
        writer(p)
        print(f"wrote {p.relative_to(REPO)}  ({len(df)} rows)")

    print("\n=== GATE ===")
    print(f"rows              : {len(df)}  (expect {79 * len(MEMBERS)})")
    assert len(df) == 79 * len(MEMBERS)
    print(f"floor             : {args.floor:g}")
    print("state             :")
    print(df.groupby(["member", "state"]).size().unstack(fill_value=0).to_string())
    print("\ncentring constants (log2, per member x element; `mean` = the clr):")
    c = pd.Series(centres).unstack()
    print(c[[f"{e} {w}" for e in ELEMENTS for w in ("mean", "med", "live")]]
          .to_string(float_format=lambda x: f"{x:7.3f}"))

    drawn = df[df.state.isin(DRAWN_STATES)]
    print(f"\nmax |clr| drawn   : {drawn.clr.abs().max():.3f}"
          f"   max +clr: {drawn.clr.max():.3f}")
    print("\npositive axes, by centring and by rank of the member's coverage of "
          "the sink:")
    print(pd.DataFrame({
        "drawn": drawn.groupby("member").size(),
        "pos (clr)": drawn.groupby("member").clr.apply(lambda s: (s > 0).sum()),
        "pos (median)": drawn.groupby("member").log2fc_med.apply(
            lambda s: (s > 0).sum()),
        **{f"pos rank {k}": drawn[drawn.clr > 0].groupby("member").dst_M_rank.apply(
            lambda s, k=k: (s == k).sum()) for k in (1, 2, 3)},
    }).fillna(0).astype(int).to_string())

    cols = ["member", "element", "src_name", "dst_name", "th_al_dst",
            "th_co_dst", "clr", "dst_M_rank", "state"]
    show = drawn.assign(dst_name=drawn.dst_name.map(lambda s: LABEL_ABBREV.get(s, s)),
                        src_name=drawn.src_name.map(lambda s: LABEL_ABBREV.get(s, s)))
    print("\n--- every rise (member carries MORE in community), the panel's content ---")
    print(show[show.clr > 0].sort_values("clr", ascending=False)[cols]
              .to_string(index=False))
    print("\n--- strongest reliefs (member carries LESS in community; not drawn) ---")
    print(show.nsmallest(10, "clr")[cols].to_string(index=False))
    return 0


def assign_arcs(axes: pd.DataFrame):
    cat = axes[axes["category"] == "catabolic"]
    central_mets = set(cat["dst_mnxm"])
    input_nodes = {(r.element, r.src_mnxm) for r in cat.itertuples()} | INPUT_EXTRA
    assert not {m for _e, m in INPUT_EXTRA} & central_mets, \
        "INPUT_EXTRA node is also a catabolic destination"

    node_arc, members = {}, {a: [] for a in ARCS}
    node_cat = {}

    def key(arc, element, mnxm):
        return mnxm if COLLAPSE[arc] else (element, mnxm)

    def add(arc, element, mnxm, lab, category):
        k = key(arc, element, mnxm)
        if k not in node_arc:
            node_arc[k] = arc
            members[arc].append((k, element, mnxm, lab))
        prev = node_cat.get(k)
        if prev is None or _cat_rank(category) < _cat_rank(prev):
            node_cat[k] = category
        return k

    for r in axes.itertuples():
        for mnxm, lab in ((r.src_mnxm, r.src_label), (r.dst_mnxm, r.dst_label)):
            if mnxm in central_mets:
                add("central", r.element, mnxm, lab, r.category)
            elif (r.element, mnxm) in input_nodes:
                add("input", r.element, mnxm, lab, r.category)
            else:
                add("biomass", r.element, mnxm, lab, r.category)

    members["biomass"].sort(key=lambda m: (_cat_rank(node_cat[m[0]]), m[3]))
    return node_arc, members, central_mets, input_nodes


def node_key_for(arc, element, mnxm):
    return mnxm if COLLAPSE[arc] else (element, mnxm)


def layout(members):
    ARC_GAP = np.deg2rad(11)
    ELEM_GAP = {arc: np.deg2rad(0.0 if arc == "biomass" else 3.5) for arc in ARCS}

    grouped = {}
    for arc in ARCS:
        if COLLAPSE[arc]:
            grouped[arc] = [("*", members[arc])]
        else:
            by = {}
            for m in members[arc]:
                by.setdefault(m[1], []).append(m)
            grouped[arc] = [(e, by[e]) for e in ELEMENTS if e in by]

    total = sum(len(m) for m in members.values())
    elem_gap_total = sum(max(0, len(g) - 1) * ELEM_GAP[arc]
                         for arc, g in grouped.items())
    usable = 2 * np.pi - len(ARCS) * ARC_GAP - elem_gap_total

    angle, arc_span, elem_span = {}, {}, {}
    cursor = np.pi / 2
    for arc in ARCS:
        arc_start = cursor
        for gi, (e, ms) in enumerate(grouped[arc]):
            span = usable * (len(ms) / total)
            for i, (k, _el, _m, _lab) in enumerate(ms):
                angle[k] = cursor - (i + 0.5) / len(ms) * span
            elem_span[(arc, e)] = (cursor, cursor - span)
            cursor -= span
            if gi < len(grouped[arc]) - 1:
                cursor -= ELEM_GAP[arc]
        arc_span[arc] = (arc_start, cursor)
        cursor -= ARC_GAP
    return angle, arc_span, elem_span


def alpha_for(lfc, vmax, ramp="linear"):
    if not np.isfinite(lfc):
        return ALPHA_MAX
    a = abs(lfc)
    if ramp == "linear":
        t = a / vmax if vmax > 0 else 0.0
        lo = ALPHA_MIN_LINEAR
    else:
        denom = math.log10(1 + vmax / LIN_THRESH)
        t = math.log10(1 + a / LIN_THRESH) / denom if denom > 0 else 0.0
        lo = ALPHA_MIN_SYMLOG
    return lo + (ALPHA_MAX - lo) * min(1.0, max(0.0, t))


def _bezier(p0, p1, bow):
    from matplotlib.path import Path as MplPath
    c0, c1 = np.asarray(p0) * bow, np.asarray(p1) * bow
    return MplPath([p0, c0, c1, p1],
                   [MplPath.MOVETO, MplPath.CURVE4, MplPath.CURVE4, MplPath.CURVE4])


def _apply_rc(plt):
    plt.rcParams.update({
        "font.family": "DejaVu Sans", "font.size": 11,
        "text.color": INK, "axes.labelcolor": INK,
        "figure.facecolor": "white", "axes.facecolor": "white",
        "savefig.facecolor": "white",
        "svg.fonttype": "none",
    })


DRAWN_STATES = ("ok", "port-confounded")


def _axes_with_labels(df: pd.DataFrame) -> pd.DataFrame:
    a = (df.drop_duplicates("axis_id")
           [["axis_id", "element", "src_mnxm", "dst_mnxm", "category",
             "src_name", "dst_name"]]
           .rename(columns={"src_name": "src_label", "dst_name": "dst_label"}))
    assert len(a) == 79, f"expected 79 axes in the table, got {len(a)}"
    for c in ("src_label", "dst_label"):
        a[c] = a[c].astype(str).map(lambda s: LABEL_ABBREV.get(s, s))
    return a


def draw_member(df_all: pd.DataFrame, member: str, vmax: float, args) -> dict:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.patches as mpatches
    from matplotlib.lines import Line2D

    _apply_rc(plt)
    axes = _axes_with_labels(df_all)
    node_arc, members, central_mets, input_nodes = assign_arcs(axes)
    angle, arc_span, elem_span = layout(members)

    def key_of(element, mnxm):
        if mnxm in central_mets:
            return node_key_for("central", element, mnxm)
        arc = "input" if (element, mnxm) in input_nodes else "biomass"
        return node_key_for(arc, element, mnxm)

    df = df_all[df_all.member == member].assign(value=df_all[VALUE_COL[args.centre]])
    measurable = df[df.state.isin(DRAWN_STATES) & np.isfinite(df.value)]
    shown = measurable
    if args.sign == "pos":
        shown = shown[shown.value > 0]
    elif args.sign == "neg":
        shown = shown[shown.value < 0]
    if args.rank != "all":
        shown = shown[shown.dst_M_rank == int(args.rank)]

    hot = {key_of(r.element, r.dst_mnxm) for r in
           shown.reindex(shown.value.abs().sort_values(ascending=False).index)
                .head(args.bold).itertuples()}
    touched = ({key_of(r.element, r.src_mnxm) for r in shown.itertuples()}
               | {key_of(r.element, r.dst_mnxm) for r in shown.itertuples()})

    fig, ax = plt.subplots(figsize=(11.5, 11.5))
    R = 1.0

    for arc in ARCS:
        a0, a1 = arc_span[arc]
        mid = (a0 + a1) / 2
        ax.text(1.44 * np.cos(mid), 1.44 * np.sin(mid), ARC_TITLE[arc],
                ha="center", va="center", fontsize=13, color=INK_SOFT, zorder=6,
                rotation=np.rad2deg(mid) + (180 if np.cos(mid) < 0 else 0),
                rotation_mode="anchor")
        for (a, e), (s0, s1) in elem_span.items():
            if a != arc or e == "*" or arc == "biomass":
                continue
            m2 = (s0 + s1) / 2
            ax.text(1.27 * np.cos(m2), 1.27 * np.sin(m2), e, ha="center",
                    va="center", fontsize=9, fontweight="bold",
                    color=ELEMENT_COLORS[e], zorder=6)

    for arc in ARCS:
        for k, e, _m, lab in members[arc]:
            th = angle[k]
            x, y = R * np.cos(th), R * np.sin(th)
            live = k in touched
            if COLLAPSE[arc]:
                ax.scatter([x], [y], s=88 if live else 40, facecolors="white",
                           edgecolors=INK if live else INK_FAINT,
                           linewidths=1.1 if live else 0.8, zorder=7)
            else:
                ax.scatter([x], [y], s=88 if live else 40, zorder=7,
                           color=ELEMENT_COLORS[e] if live else INK_FAINT,
                           edgecolors="white", linewidths=0.5)
            bold = k in hot
            ha = "left" if np.cos(th) >= 0 else "right"
            rot = np.rad2deg(th) + (180 if np.cos(th) < 0 else 0)
            ax.text(1.03 * x, 1.03 * y, lab, rotation=rot, rotation_mode="anchor",
                    ha=ha, va="center",
                    fontsize=6.6 if bold else (5.8 if live else 5.0),
                    fontweight="bold" if bold else "normal",
                    color=(INK if bold else INK_SOFT) if live else INK_FAINT,
                    zorder=6)

    d = shown.assign(_clear=(shown.dst_M_rank != 3).astype(int),
                     _ord=shown.value.abs())
    n_shown = n_pos = n_neg = n_r3 = 0
    for r in d.sort_values(["_clear", "_ord"]).itertuples():
        ka, kb = key_of(r.element, r.src_mnxm), key_of(r.element, r.dst_mnxm)
        if ka not in angle or kb not in angle or ka == kb:
            continue
        a, b = angle[ka], angle[kb]
        p0 = (R * np.cos(a), R * np.sin(a))
        p1 = (R * np.cos(b), R * np.sin(b))
        sep = abs(a - b)
        sep = min(sep, 2 * np.pi - sep)
        bow = 0.15 + 0.55 * (1 - sep / np.pi)

        pos = r.value > 0
        n_shown += 1
        n_pos, n_neg = n_pos + int(pos), n_neg + int(not pos)
        al = alpha_for(r.value, vmax, ramp=args.ramp)
        if args.sign == "both":
            dashed = not pos
        else:
            dashed = int(r.dst_M_rank) == 3
        n_r3 += int(int(r.dst_M_rank) == 3)
        ax.add_patch(mpatches.PathPatch(
            _bezier(p0, p1, bow), fill=False,
            edgecolor=ELEMENT_COLORS[r.element], lw=LW, alpha=al,
            linestyle=(0, (2.6, 2.0)) if dashed else "solid",
            zorder=(3 if dashed else 4) + al, capstyle="round"))

    if not args.bare:
        ax.text(-1.47, 1.40, member, ha="left", va="top", fontsize=28,
                fontweight="bold", color=INK, zorder=8)
        subtitle = {"pos": "set4 axes carrying MORE current in community than alone",
                    "neg": "set4 axes carrying LESS current in community than alone",
                    "both": "set4 axes: in community  vs  alone"}[args.sign]
        ax.text(-1.47, 1.22, subtitle, ha="left", va="top", fontsize=10,
                color=INK_SOFT, zorder=8)
        quantity = ("clr" if args.centre == "clr" else "median-centred log2")
        if args.sign == "both":
            style = [Line2D([], [], color=INK, lw=1.6, ls="solid",
                            label="carries more current in community"),
                     Line2D([], [], color=INK, lw=1.6, ls=(0, (2.6, 2.0)),
                            label="carries less current in community")]
        else:
            style = [Line2D([], [], color=INK, lw=1.6, ls="solid",
                            label="this member is best- or mid-covered for the sink "
                                  "(rank 1-2 of 3)"),
                     Line2D([], [], color=INK, lw=1.6, ls=(0, (2.6, 2.0)),
                            label="least-covered of the three (rank 3): current "
                                  "arriving here, not made here")]
        ramp_note = ("linear" if args.ramp == "linear" else "symlog")
        rank_note = {"all": f"{n_shown} of {len(measurable)} measurable axes drawn; "
                            "a rise alone is not evidence this member MAKES the "
                            "sink -- hence the linestyle",
                     "1": "only axes whose sink this member is best placed to "
                          "make (dst_M_rank 1)",
                     "3": "only axes whose sink this member is least placed to "
                          "make (dst_M_rank 3)"}[args.rank]
        handles = style + [
            Line2D([], [], color=INK, lw=1.6, alpha=0.25,
                   label=f"opacity: {quantity}, {ramp_note} ramp, "
                         f"0 to {vmax:.1f} (shared by all three panels)"),
            Line2D([], [], color="none", label=rank_note),
        ]
        leg = ax.legend(handles=handles, loc="lower left",
                        bbox_to_anchor=(-0.06, -0.02), frameon=False,
                        fontsize=8.5, labelcolor=INK_SOFT, handlelength=2.8)
        leg.set_zorder(9)

    ax.set_xlim(-1.5, 1.5)
    ax.set_ylim(-1.5, 1.5)
    ax.set_aspect("equal")
    ax.axis("off")

    stem = f"chord_delta_set4_{member}"
    png = FIGS / f"{stem}.v{args.version}.png"
    svg = FIGS / f"{stem}.v{args.version}.svg"
    for p in (png, svg):
        _unlink(p)
    fig.savefig(png, dpi=DPI_REVIEW, bbox_inches="tight")
    fig.savefig(svg, bbox_inches="tight")
    plt.close(fig)
    return {"png": png, "svg": svg, "arcs": {a: len(members[a]) for a in ARCS},
            "drawn": n_shown, "pos": n_pos, "neg": n_neg, "rank3": n_r3,
            "measurable": len(measurable), "nodes lit": len(touched),
            "unmeasurable": int(len(df) - len(measurable))}


def chord(args) -> int:
    p = Path(f"{TABLE}.parquet")
    if not p.exists():
        print(f"missing {p.relative_to(REPO)} -- run the `table` subcommand first",
              file=sys.stderr)
        return 2
    df = pd.read_parquet(p)
    val = df[VALUE_COL[args.centre]]

    m = df.state.isin(DRAWN_STATES)
    if args.sign == "pos":
        vmax = float(val[m & (val > 0)].max())
    elif args.sign == "neg":
        vmax = float(-val[m & (val < 0)].min())
    else:
        vmax = float(val[m].abs().max())
    targets = MEMBERS if args.member == "all" else (args.member,)

    print("=== GATE ===")
    print(f"quantity   : {VALUE_COL[args.centre]}  (--centre {args.centre})")
    print(f"sign       : {args.sign}   ramp: {args.ramp}")
    print(f"ramp max   : {vmax:.4f}  (shared across members)")
    for mem in targets:
        g = draw_member(df, mem, vmax, args)
        print(f"\n{mem}: arcs {g['arcs']}  drawn {g['drawn']} of {g['measurable']} "
              f"measurable (+{g['pos']} / -{g['neg']}, {g['rank3']} at rank 3)  "
              f"nodes lit {g['nodes lit']}  unmeasurable {g['unmeasurable']}")
        print(f"  {g['png'].relative_to(REPO)}")
        print(f"  {g['svg'].relative_to(REPO)}")
    return 0


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)

    t = sub.add_parser("table", help="ΔIeff over set4, 79 axes x 3 members")
    t.add_argument("--floor", type=float, default=1e-4,
                   help="mass floor on both sides of the log-ratio (default 1e-4); "
                        "below it a ratio is manufactured out of numerical dust")
    t.add_argument("--triple", default=TRIPLE,
                   help="composed network supplying the community arm")

    c = sub.add_parser("chord", help="one chord panel per member")
    c.add_argument("--member", default="all", choices=("all",) + MEMBERS)
    c.add_argument("--version", type=int, default=2)
    c.add_argument("--centre", default="clr", choices=tuple(VALUE_COL),
                   help="which centring the panel plots: `clr` subtracts the mean "
                        "log-ratio over live nodes, `median` the median (v1)")
    c.add_argument("--sign", default="pos", choices=("both", "pos", "neg"),
                   help="pos (default, shipped): only axes carrying MORE current "
                        "in community -- the contribution the panel is about")
    c.add_argument("--ramp", default="linear", choices=("linear", "symlog"),
                   help="opacity ramp; symlog is the reference panel's")
    c.add_argument("--rank", default="all", choices=("all", "1", "3"),
                   help="restrict to axes whose sink this member is best (1) or "
                        "least (3) placed to make; see gene_coverage()")
    c.add_argument("--bold", type=int, default=8,
                   help="bold the endpoints of the N strongest responses")
    c.add_argument("--bare", action="store_true",
                   help="drop the centre label and key, as the reference ships")

    a = p.parse_args(argv)
    return {"table": table, "chord": chord}[a.cmd](a)


if __name__ == "__main__":
    sys.exit(main())

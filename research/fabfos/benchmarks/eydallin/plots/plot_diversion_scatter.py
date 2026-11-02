#!/usr/bin/env python3
"""Every metabolite's response to the positive case against its response to the diverted one.

Universal ground (`measure_leak` with no precursor list): every metabolite leaks equally to
OMEGA, so every metabolite carries a draw and can be a point. That is the whole reason for
this grounding -- under a port list only the ports have a current to read.

x = dIeff under glgC x2 (453.3% of WT glycogen), y = dIeff under talA x2 (49.7%). A point in
the fourth quadrant is a metabolite the positive case feeds and the diverted case starves.

Two envs, because no single one holds both halves: `ecspr` solves, `figure-net` draws. The
solve caches to a TSV, so the second run reads it rather than needing the engine at all --
which is why the engine imports sit inside `compute` instead of at module scope.

    mamba run -n ecspr      python research/fabfos/benchmarks/eydallin/plot_diversion_scatter.py
    mamba run -n figure-net python research/fabfos/benchmarks/eydallin/plot_diversion_scatter.py
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[5]

RUNS = ROOT / "data/fabfos/runs"
CHEM_PROP = ROOT / "data/fabfos/originals/metanetx/4.5/chem_prop.tsv"
OUT_DIR = ROOT / "data/fabfos/runs/eydallin_clones/ecspr"
CACHE = Path(__file__).resolve().parents[1] / "cache"

SOURCE_MNXM = "MNXM1364061"
GLYCOGEN_MNXM = "MNXM738130"
CASES = {"glgC": ["MNXR145050"], "talA": ["MNXR146501"]}

# Annotated by request. MetaNetX 4.5 carries R5P as the ring form, which is the species the
# rpiA/rpiB rows actually name -- the three straight-chain `*ribose 5-phosphate` entries are
# in chem_prop but in none of AG1's reactions. Resolved at runtime rather than guessed.
R5P_CANDIDATES = ["MNXM1363910", "MNXM1363912", "MNXM1560737", "MNXM1363911"]

SURFACE, INK, INK2 = "#fcfcfb", "#0b0b0b", "#52514e"
CLOUD, ACCENT, GRID = "#2a78d6", "#eb6834", "#dedcd6"


def clip_ratios(ratios: dict, clip: float) -> dict:
    # Cap every reaction's directional asymmetry at `clip`:1, both ways.
    #
    # `graph_from_pairs` treats a raw ratio as g_rev/g_fwd against the equation as MetaNetX
    # wrote it, then normalises to the favoured branch (see build.py) -- so clamping the raw
    # value into [1/clip, clip] before that step caps the favoured:disfavoured conductance
    # ratio at `clip`:1 regardless of which way the equation happens to be written. ratio==1.0
    # (zero ensemble votes, e.g. glgA/glgP) is unaffected either way -- there is nothing to
    # clip.
    lo, hi = 1.0 / clip, clip
    return {r: min(max(v, lo), hi) for r, v in ratios.items()}


def compute(a) -> pd.DataFrame:
    sys.path.insert(0, str(ROOT / "src"))
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from ecspr.model.build import load_pairs, load_direction_ratios, graph_from_pairs
    from ecspr.model.graph import Terminal, measure_leak
    import bake_pairs

    def draws(pairs, element, weights, ratios, leak) -> dict:
        g = graph_from_pairs(pairs, element, weights, ratios)
        src = Terminal.metabolite(g, SOURCE_MNXM, label="glucose")
        return measure_leak(g, src, None, leak=leak)["draw"]

    pairs = load_pairs(bake_pairs.atom_pairs(), element=a.element)
    ratios = load_direction_ratios(bake_pairs.direction_ratios())
    if a.clip_ratio:
        n_clipped = sum(1 for v in ratios.values()
                         if v > a.clip_ratio or v < 1.0 / a.clip_ratio)
        ratios = clip_ratios(ratios, a.clip_ratio)
        print(f"[div] clipped {n_clipped}/{len(ratios)} reaction ratios to "
              f"{a.clip_ratio:g}:1", file=sys.stderr)
    host = pd.read_parquet(RUNS / a.host / "gpr" / "gpr_gem.parquet")
    base_w = {m: 1.0 for m in host.mnxr.dropna().astype(str).unique()}

    base = draws(pairs, a.element, base_w, ratios, a.leak)
    print(f"[div] {len(base)} metabolites, universal leak={a.leak:g}", file=sys.stderr)
    df = pd.DataFrame({"mnxm": sorted(base), "base": [base[m] for m in sorted(base)]})
    for gene, rxns in CASES.items():
        d = draws(pairs, a.element,
                  dict(base_w, **{r: base_w[r] * a.fold for r in rxns}), ratios, a.leak)
        df[f"d_{gene}"] = [d.get(m, 0.0) - base[m] for m in df.mnxm]
        print(f"[div] {gene} x{a.fold:g} done", file=sys.stderr)

    c = pd.read_csv(CHEM_PROP, sep="\t", comment="#", header=None,
                    names=["id", "name", "ref", "formula", "charge", "mass",
                           "InChI", "InChIKey", "SMILES"], dtype=str, low_memory=False)
    return df.merge(c[["id", "name"]].rename(columns={"id": "mnxm"}), on="mnxm", how="left")


def symlog_bins(v, thr, n=48):
    lo, hi = float(np.min(v)), float(np.max(v))
    edges = [-thr, thr]
    if lo < -thr:
        edges = list(-np.logspace(np.log10(thr), np.log10(-lo), n // 2)) + edges
    if hi > thr:
        edges = edges + list(np.logspace(np.log10(thr), np.log10(hi), n // 2))
    return np.unique(np.asarray(edges, float))


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--host", default="e_coli_ag1")
    p.add_argument("--element", default="C")
    p.add_argument("--fold", type=float, default=2.0)
    p.add_argument("--leak", type=float, default=1e-3)
    p.add_argument("--scale", default="symlog", choices=("symlog", "linear"))
    p.add_argument("--clip-ratio", type=float, default=0.0,
                   help="cap every reaction's direction ratio at this many:1 (e.g. 2 or 10); "
                        "0 (default) leaves the baked ratios, however extreme, untouched")
    p.add_argument("--recompute", action="store_true")
    a = p.parse_args()
    if a.clip_ratio and a.clip_ratio < 1.0:
        raise SystemExit("--clip-ratio must be >= 1 (it is a ratio, not a fraction)")

    CACHE.mkdir(exist_ok=True)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    tag_clip = f"_clip{a.clip_ratio:g}" if a.clip_ratio else ""
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    import bake_pairs
    tag_clip += bake_pairs.tag()
    tsv = OUT_DIR / f"diversion_scatter_{a.host}_{a.element}_leak{a.leak:g}{tag_clip}.tsv"
    if tsv.exists() and not a.recompute:
        df = pd.read_csv(tsv, sep="\t")
        print(f"[div] reusing {tsv}", file=sys.stderr)
    else:
        df = compute(a)
        df.to_csv(tsv, sep="\t", index=False)
        print(f"[div] wrote {tsv}", file=sys.stderr)

    x, y = df.d_glgC.to_numpy(), df.d_talA.to_numpy()
    thr = 1e-12

    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.gridspec import GridSpec

    plt.rcParams.update({"font.size": 9, "text.color": INK,
                         "axes.labelcolor": INK, "xtick.color": INK2,
                         "ytick.color": INK2, "axes.edgecolor": GRID})
    fig = plt.figure(figsize=(7.6, 7.0), facecolor=SURFACE)
    gs = GridSpec(2, 2, figure=fig, width_ratios=(1, 3.2), height_ratios=(1, 3.2),
                  wspace=0.06, hspace=0.06, left=0.05, right=0.855,
                  bottom=0.09, top=0.97)
    ax = fig.add_subplot(gs[1, 1])
    axx = fig.add_subplot(gs[0, 1], sharex=ax)
    axy = fig.add_subplot(gs[1, 0], sharey=ax)

    for b in (ax, axx, axy):
        b.set_facecolor(SURFACE)
        for s in ("top", "right"):
            b.spines[s].set_visible(False)
    if a.scale == "symlog":
        ax.set_xscale("symlog", linthresh=thr)
        ax.set_yscale("symlog", linthresh=thr)

    ax.axhline(0, color=GRID, lw=1, zorder=0)
    ax.axvline(0, color=GRID, lw=1, zorder=0)
    ax.scatter(x, y, s=13, c=CLOUD, alpha=0.30, linewidths=0, zorder=2)

    from matplotlib.patheffects import withStroke
    marks = [("glycogen", GLYCOGEN_MNXM, 4)]
    r5p = next((m for m in R5P_CANDIDATES if (df.mnxm == m).any()), None)
    if r5p:
        marks.append(("ribose 5-phosphate", r5p, 6))
    for label, mnxm, dy in marks:
        row = df[df.mnxm == mnxm]
        if row.empty:
            print(f"[div] {label} absent from the graph", file=sys.stderr)
            continue
        px, py = float(row.d_glgC.iloc[0]), float(row.d_talA.iloc[0])
        ax.scatter([px], [py], s=64, c=ACCENT, edgecolors=SURFACE, linewidths=2, zorder=4)
        frac = ax.transLimits.transform((px, py))[0]
        ha = "right" if frac > 0.5 else "left"
        ax.annotate(label, (px, py), textcoords="offset points",
                    xytext=(-13 if ha == "right" else 13, dy), ha=ha,
                    color=INK, fontsize=9.5, zorder=5,
                    path_effects=[withStroke(linewidth=3.5, foreground=SURFACE)])
        print(f"[div] {label:22} dIeff glgC {px:+.4e}   talA {py:+.4e}", file=sys.stderr)

    xb = symlog_bins(x, thr) if a.scale == "symlog" else 64
    yb = symlog_bins(y, thr) if a.scale == "symlog" else 64
    axx.hist(x, bins=xb, color=CLOUD, alpha=0.75, linewidth=0)
    axy.hist(y, bins=yb, color=CLOUD, alpha=0.75, linewidth=0,
             orientation="horizontal")
    axy.invert_xaxis()
    for b in (axx, axy):
        b.tick_params(left=False, right=False, top=False, bottom=False,
                      labelleft=False, labelright=False, labeltop=False, labelbottom=False)
        b.spines["left" if b is axx else "bottom"].set_visible(False)

    if a.scale == "symlog":
        dec = [1e-5, 1e-7, 1e-9, 1e-11]
        ticks = [-t for t in dec] + [0.0] + dec[::-1]
        for axis in (ax.xaxis, ax.yaxis):
            axis.set_ticks(ticks)
            axis.set_ticks([], minor=True)
    else:
        ax.ticklabel_format(axis="both", style="sci", scilimits=(0, 0))
        ax.xaxis.get_offset_text().set_color(INK2)
        ax.yaxis.get_offset_text().set_color(INK2)
        ax.yaxis.get_offset_text().set_position((1.0, 1.0))
    ax.tick_params(labelleft=False, labelright=True, left=False, right=True, length=3)
    ax.yaxis.set_label_position("right")
    notes = ([f"direction ratio clipped {a.clip_ratio:g}:1"] if a.clip_ratio else []) \
        + ([bake_pairs.BAKE.name] if bake_pairs.tag() else [])
    clip_note = ("  (" + "; ".join(notes) + ")") if notes else ""
    ax.set_xlabel(r"$\Delta I_{\rm eff}$   glgC $\times$2" + clip_note)
    ax.set_ylabel(r"$\Delta I_{\rm eff}$   talA $\times$2")

    tag = "" if a.scale == "symlog" else f"_{a.scale}"
    png = CACHE / f"diversion_scatter_{a.host}_{a.element}_leak{a.leak:g}{tag_clip}{tag}.png"
    fig.savefig(png, dpi=200, facecolor=SURFACE)
    print(f"[div] wrote {png}", file=sys.stderr)
    q = pd.Series(dict(
        Q1_both_up=int(((x > thr) & (y > thr)).sum()),
        Q2_glgC_down_talA_up=int(((x < -thr) & (y > thr)).sum()),
        Q3_both_down=int(((x < -thr) & (y < -thr)).sum()),
        Q4_glgC_up_talA_down=int(((x > thr) & (y < -thr)).sum()),
        dead=int(((np.abs(x) <= thr) & (np.abs(y) <= thr)).sum()), n=len(x)))
    print(q.to_string(), file=sys.stderr)


if __name__ == "__main__":
    main()

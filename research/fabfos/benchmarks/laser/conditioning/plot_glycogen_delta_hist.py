#!/usr/bin/env python3
"""Histogram of the eydallin cohort's glycogen delta under universal ground, on r9.

The conditioning study's claim is that the ECSPr response is not badly scaled, it is read
over the wrong set -- the members carrying it span one to two decades, and what spans
fifteen is the tail reported as if every member of it were a measurement. That claim was
made on the ASKA library, where the labels are a classifier's. This asks it of the actual
signal: the 23 eydallin conditions with a measured Fig. 1 phenotype.

Two panels, one distribution:

  left    the delta as it is -- signed, symlog, split by whether the clone measured as an
          accumulator or as glycogen-deficient. This is what a regression would see.
  right   |delta| in decades with the `scoring.responders` cut drawn on it, so the claim is
          visible rather than asserted: how many members carry 90% of the response, and how
          wide they are.

Universal ground (`measure_leak` with no port list) is `cohort_delta_panel.py`'s grounding,
which is what the committed +0.21 was taken under. Curated GEM arm; no pbert.

    mamba run -n figure-net python \
        research/fabfos/benchmarks/laser/conditioning/plot_glycogen_delta_hist.py \
        --panel research/fabfos/benchmarks/eydallin/cache/cohort_delta_panel_gem_fold2.0_C.parquet
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy.stats import spearmanr

ROOT = Path(__file__).resolve().parents[5]
sys.path.insert(0, str(ROOT / "src"))

from ecspr.model.scoring import responders  # noqa: E402

BLUE = "#2a78d6"
RED = "#e34948"
GREY = "#9a9892"
SURFACE = "#fcfcfb"
TEXT_PRIMARY = "#0b0b0b"
TEXT_SECONDARY = "#52514e"
GRID = "#e3e2dc"

CALLOUTS = {"glgC", "glgA", "glgB", "glgP", "talA", "malP", "ddg"}


def style(ax):
    ax.set_facecolor(SURFACE)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    for s in ("left", "bottom"):
        ax.spines[s].set_color(GRID)
    ax.tick_params(colors=TEXT_SECONDARY, labelsize=9)
    ax.grid(True, color=GRID, lw=0.6, alpha=0.7)
    ax.set_axisbelow(True)


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--panel", type=Path, required=True)
    p.add_argument("--coverage", type=float, default=0.90)
    p.add_argument("--arms", type=Path, default=None,
                   help="universal_ground_cohort.py table; adds the control panel")
    p.add_argument("--out", type=Path,
                   default=Path(__file__).resolve().parent / "cache/glycogen_delta_hist.png")
    a = p.parse_args()

    df = pd.read_parquet(a.panel).copy()
    scored = df.dropna(subset=["pct_wt"]).copy()
    d = scored.delta.to_numpy(float)
    mag = np.abs(d)
    keep = responders(mag, a.coverage)
    rho, pv = spearmanr(scored.delta, scored.pct_wt)

    live = mag[mag > 0]
    lo = float(np.floor(np.log10(live.min()))) if live.size else -20.0
    hi = float(np.ceil(np.log10(live.max()))) if live.size else 0.0
    lin = float(np.median(live)) if live.size else 1e-12

    n_ax = 3 if a.arms else 2
    fig, axes = plt.subplots(1, n_ax, figsize=(6.75 * n_ax, 5.4), facecolor=SURFACE)
    axL, axR = axes[0], axes[1]

    style(axL)
    up = scored.pct_wt.to_numpy(float) >= 100.0
    floor_exp = lo - 1.0
    depth = np.where(mag > 0, np.log10(np.maximum(mag, 10.0 ** floor_exp)) - floor_exp, 0.0)
    x = np.sign(d) * depth
    edges = np.linspace(-(hi - floor_exp), hi - floor_exp, 33)
    axL.hist([x[up], x[~up]], bins=edges, stacked=True, color=[BLUE, RED],
             edgecolor=SURFACE, lw=0.6,
             label=[f"accumulator, ≥100% WT (n={int(up.sum())})",
                    f"deficient, <100% WT (n={int((~up).sum())})"])
    axL.axvline(0.0, color=TEXT_SECONDARY, lw=1.0, ls="--", alpha=0.7)
    step = 2.0
    tk = np.arange(0, hi - floor_exp + step, step)
    axL.set_xticks(np.concatenate([-tk[::-1][:-1], [0.0], tk[1:]]))
    axL.set_xticklabels([f"−1e{int(t + floor_exp)}" for t in tk[::-1][:-1]] + ["0"] +
                        [f"+1e{int(t + floor_exp)}" for t in tk[1:]], fontsize=8)
    axL.set_xlabel("Δ draw at glycogen, ×2 fold, universal ground  "
                   "(signed, decades from floor)",
                   color=TEXT_SECONDARY, fontsize=10)
    axL.set_ylabel("conditions", color=TEXT_SECONDARY, fontsize=10)
    axL.set_title("the signal, signed", color=TEXT_PRIMARY, fontsize=12, loc="left")
    axL.legend(frameon=False, fontsize=9, labelcolor=TEXT_SECONDARY)
    agree = int(((d > 0) == up)[d != 0].sum())
    live_n = int((d != 0).sum())
    axL.text(0.98, 0.96, f"Spearman(Δ, %WT) = {rho:+.3f}  p = {pv:.2f}   n = {len(scored)}\n"
                         f"sign agrees with phenotype: {agree}/{live_n}\n"
                         f"Δ < 0: {int((d < 0).sum())}/{len(scored)}",
             transform=axL.transAxes, ha="right", va="top", fontsize=9,
             color=TEXT_SECONDARY, linespacing=1.5)

    style(axR)
    bins = np.arange(lo, hi + 0.5, 0.5)
    lg = np.log10(np.maximum(mag, 10.0 ** lo))
    axR.hist([lg[keep], lg[~keep]], bins=bins, stacked=True, color=[BLUE, GREY],
             edgecolor=SURFACE, lw=0.6,
             label=[f"carries {a.coverage:.0%} of the response (n={int(keep.sum())})",
                    f"the remaining {1 - a.coverage:.0%} (n={int((~keep).sum())})"])
    if keep.any():
        span = float(np.ptp(lg[keep]))
        axR.axvspan(lg[keep].min(), lg[keep].max(), color=BLUE, alpha=0.07, zorder=0)
        axR.annotate(f"{span:.2f} decades", xy=((lg[keep].min() + lg[keep].max()) / 2,
                                                axR.get_ylim()[1] * 0.92),
                     ha="center", fontsize=10, color=BLUE, weight="bold")
    axR.set_xlabel("log₁₀ |Δ draw at glycogen|", color=TEXT_SECONDARY, fontsize=10)
    axR.set_ylabel("conditions", color=TEXT_SECONDARY, fontsize=10)
    axR.set_title("the same numbers, by magnitude", color=TEXT_PRIMARY, fontsize=12,
                  loc="left")
    axR.legend(frameon=False, fontsize=9, labelcolor=TEXT_SECONDARY, loc="upper left")
    p5, p95 = np.percentile(lg, [5, 95])
    axR.text(0.98, 0.60, f"whole cohort 5–95: {p95 - p5:.2f} decades",
             transform=axR.transAxes, ha="right", va="top", fontsize=9,
             color=TEXT_SECONDARY)

    named = scored.loc[keep, ["gene", "delta"]].sort_values(
        "delta", key=np.abs, ascending=False)
    axR.text(0.98, 0.52, "carrying it: " + ", ".join(named.gene.astype(str)[:8]),
             transform=axR.transAxes, ha="right", va="top", fontsize=8.5,
             color=TEXT_SECONDARY, wrap=True)

    # ---- optional third: the control that says whose fault the shape is ---------------
    # If the all-negative pattern survives with EVERY ratio pinned to 1.0, it is the
    # grounding geometry and not the direction bake. Plotted against the phenotype so the
    # absence of a trend is visible rather than summarised as one rho.
    if a.arms:
        axA = axes[2]
        style(axA)
        arms = pd.read_csv(a.arms, sep="\t").dropna(subset=["pct_wt"])
        marks = {"baked": ("o", BLUE), "capped": ("s", GREY), "symmetric": ("^", RED)}
        for name, sub in arms.groupby("arm"):
            m, c = marks.get(name, ("o", GREY))
            dd = sub.delta.to_numpy(float)
            dep = np.where(np.abs(dd) > 0,
                           np.log10(np.maximum(np.abs(dd), 10.0 ** floor_exp)) - floor_exp,
                           0.0)
            r_, p_ = spearmanr(sub.delta, sub.pct_wt)
            axA.scatter(np.sign(dd) * dep, sub.pct_wt, marker=m, s=42, alpha=0.75,
                        color=c, edgecolor=SURFACE, lw=0.7,
                        label=f"{name}  ρ={r_:+.3f} (p={p_:.2f})")
        axA.axvline(0.0, color=TEXT_SECONDARY, lw=1.0, ls="--", alpha=0.7)
        axA.axhline(100.0, color=TEXT_SECONDARY, lw=1.0, ls=":", alpha=0.7)
        axA.set_yscale("log")
        axA.set_xticks(np.concatenate([-tk[::-1][:-1], [0.0], tk[1:]]))
        axA.set_xticklabels([f"−1e{int(t + floor_exp)}" for t in tk[::-1][:-1]] + ["0"] +
                            [f"+1e{int(t + floor_exp)}" for t in tk[1:]], fontsize=8)
        axA.set_xlabel("Δ draw at glycogen (signed, decades from floor)",
                       color=TEXT_SECONDARY, fontsize=10)
        axA.set_ylabel("measured glycogen, % of wild type", color=TEXT_SECONDARY,
                       fontsize=10)
        axA.set_title("the control: same probe, no direction evidence",
                      color=TEXT_PRIMARY, fontsize=12, loc="left")
        axA.legend(frameon=False, fontsize=9, labelcolor=TEXT_SECONDARY, loc="lower left")

    fig.suptitle("Eydallin cohort glycogen response on the r9 direction bake — "
                 "universal ground, curated GEM arm",
                 color=TEXT_PRIMARY, fontsize=13, x=0.01, ha="left")
    fig.tight_layout(rect=(0, 0, 1, 0.94))
    a.out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(a.out, dpi=170, facecolor=SURFACE)
    print(f"[hist] n={len(scored)} scored, {int(keep.sum())} carry {a.coverage:.0%}, "
          f"spanning {float(np.ptp(lg[keep])):.2f} decades; whole cohort 5-95 "
          f"{p95 - p5:.2f}; rho={rho:+.4f} p={pv:.3g}", file=sys.stderr)
    print(f"[hist] wrote {a.out}", file=sys.stderr)


if __name__ == "__main__":
    main()

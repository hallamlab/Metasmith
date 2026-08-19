#!/usr/bin/env python3
"""Histogram of the eydallin cohort's glycogen delta under universal ground, linear axis.

`plot_glycogen_delta_hist.py` reads the same delta on signed decades because the magnitudes
span nine of them. This asks the plainer question instead: on the axis a reader looks at
first -- linear, signed, no transform -- where do the 25 conditions actually sit, and which
of them are the `scoring.responders` cut carrying 90% of the response.

Two panels sharing one x-axis:

  top      the delta as measured, linear and signed, split by accumulator vs
           glycogen-deficient phenotype.
  bottom   a barcode -- one tick per condition at its exact delta, so the histogram's binning
           does not hide how few points are doing the work. Responder-cut hits drawn tall and
           solid; the rest short and faint.

Universal ground at leak=1e-3, baked arm (`universal_ground_cohort.py`'s r9 direction table,
curated GEM, no cap).

    mamba run -n figure-net python \
        research/fabfos/benchmarks/laser/conditioning/plot_universal_ground_hist.py
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

HERE = Path(__file__).resolve().parent
DEFAULT_PANEL = HERE / "cache/universal_ground_cohort_leak0.001.tsv"


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
    p.add_argument("--panel", type=Path, default=DEFAULT_PANEL,
                   help="universal_ground_cohort.py table (long form, one row per arm)")
    p.add_argument("--arm", default="baked")
    p.add_argument("--coverage", type=float, default=0.90)
    p.add_argument("--bins", type=int, default=28)
    p.add_argument("--out", type=Path, default=HERE / "cache/universal_ground_hist.png")
    a = p.parse_args()

    df = pd.read_csv(a.panel, sep="\t")
    sub = df[df.arm == a.arm].dropna(subset=["pct_wt"]).copy()
    d = sub.delta.to_numpy(float)
    mag = np.abs(d)
    keep = responders(mag, a.coverage)
    up = sub.pct_wt.to_numpy(float) >= 100.0

    span = float(np.max(mag)) if mag.size else 1.0
    pad = 0.08 * span
    xlim = (-span - pad, span + pad)

    fig, (axH, axB) = plt.subplots(
        2, 1, figsize=(8.4, 6.2), facecolor=SURFACE, sharex=True,
        gridspec_kw={"height_ratios": [3.4, 1.0], "hspace": 0.06})

    # ---- top: linear, signed histogram --------------------------------------------
    style(axH)
    edges = np.linspace(xlim[0], xlim[1], a.bins + 1)
    axH.hist([d[up], d[~up]], bins=edges, stacked=True, color=[BLUE, RED],
              edgecolor=SURFACE, lw=0.6,
              label=[f"accumulator, ≥100% WT (n={int(up.sum())})",
                     f"deficient, <100% WT (n={int((~up).sum())})"])
    axH.axvline(0.0, color=TEXT_SECONDARY, lw=1.0, ls="--", alpha=0.7)
    axH.set_ylabel("conditions", color=TEXT_SECONDARY, fontsize=10)
    axH.set_title(f"Eydallin cohort glycogen Δ, universal ground (leak=1e-3, {a.arm} arm) "
                  "— linear, signed",
                  color=TEXT_PRIMARY, fontsize=12, loc="left")
    axH.legend(frameon=False, fontsize=9, labelcolor=TEXT_SECONDARY)
    agree = int(((d > 0) == up)[d != 0].sum())
    live_n = int((d != 0).sum())
    axH.text(0.98, 0.96,
             f"n = {len(sub)}   sign agrees with phenotype: {agree}/{live_n}\n"
             f"carries {a.coverage:.0%} of the response: {int(keep.sum())} of {len(sub)}",
             transform=axH.transAxes, ha="right", va="top", fontsize=9,
             color=TEXT_SECONDARY, linespacing=1.5)

    # ---- bottom: barcode, one tick per condition at its exact delta ---------------
    style(axB)
    axB.set_yticks([])
    for s in ("top", "right", "left"):
        axB.spines[s].set_visible(False)
    axB.grid(False)
    for xi, hit, acc in zip(d, keep, up):
        color = BLUE if acc else RED
        if hit:
            axB.axvline(xi, color=color, lw=1.4, alpha=0.9, ymin=0.05, ymax=0.95)
        else:
            axB.axvline(xi, color=GREY, lw=0.8, alpha=0.55, ymin=0.25, ymax=0.75)
    axB.axvline(0.0, color=TEXT_SECONDARY, lw=1.0, ls="--", alpha=0.7)
    axB.set_xlim(xlim)
    axB.set_xlabel("Δ draw at glycogen, ×2 fold, universal ground (linear, signed)",
                  color=TEXT_SECONDARY, fontsize=10)
    axB.text(0.01, 0.5, "hits", transform=axB.transAxes, ha="left", va="center",
              fontsize=8.5, color=TEXT_SECONDARY, style="italic")

    named = sub.loc[keep, ["gene", "delta"]].sort_values("delta", key=np.abs, ascending=False)
    axH.text(0.98, 0.86, "carrying it: " + ", ".join(named.gene.astype(str)[:8]),
             transform=axH.transAxes, ha="right", va="top", fontsize=8.5,
             color=TEXT_SECONDARY, wrap=True)

    fig.subplots_adjust(left=0.09, right=0.98, top=0.90, bottom=0.10)
    a.out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(a.out, dpi=170, facecolor=SURFACE)
    print(f"[hist] n={len(sub)}, {int(keep.sum())} carry {a.coverage:.0%}, "
          f"span={span:.3g}; wrote {a.out}", file=sys.stderr)


if __name__ == "__main__":
    main()

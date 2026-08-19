#!/usr/bin/env python3
"""ASKA-wide sweep vs. the eydallin cohort, universal ground leak=1e-3 -- the in-silico
analogue of Eydallin's own screen.

Eydallin's paper is a wet-lab ASKA overexpression screen: every clone in the library gets
made, grown, and its glycogen content read against wild type; 25 of ~4,100 clones turned up
as hits. `sweep_aska.py --channel gem --probe share --ground leak` is the same experiment run
on the model instead of the bench -- every ASKA clone gets its named gene's reactions folded
x2 under universal ground and the delta at glycogen read off. This plot puts the two side by
side: the full library's delta distribution against the 25-gene cohort's, and where each
cohort's `scoring.responders` hits sit within it.

Two panels sharing one x-axis, both on the ASKA-solved (`n_rxn > 0`) support:

  top      KDE of delta, ASKA library vs. eydallin cohort, each normalised to its own density
           so the ~35x difference in n doesn't just draw the bigger cohort's curve on top of
           the smaller one.
  bottom   a barcode -- every ASKA clone's delta as a short tick, every eydallin condition's
           delta as a short tick, both cohorts' responder-cut hits drawn tall and solid.

    mamba run -n figure-net python \
        research/fabfos/benchmarks/laser/conditioning/plot_aska_eydallin_kde.py
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
from scipy.stats import gaussian_kde

ROOT = Path(__file__).resolve().parents[5]
sys.path.insert(0, str(ROOT / "src"))

from ecspr.model.scoring import responders  # noqa: E402

ASKA = "#2a9d6f"
EYDALLIN = "#2a78d6"
GREY = "#9a9892"
SURFACE = "#fcfcfb"
TEXT_PRIMARY = "#0b0b0b"
TEXT_SECONDARY = "#52514e"
GRID = "#e3e2dc"

HERE = Path(__file__).resolve().parent
DEFAULT_ASKA = (ROOT / "data/fabfos/runs/eydallin_clones/ecspr/"
                "aska_sweep_gem_e_coli_ag1_fold2.0_C_shareleak0.001.tsv")
DEFAULT_EYDALLIN = HERE / "cache/universal_ground_cohort_leak0.001.tsv"


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
    p.add_argument("--aska", type=Path, default=DEFAULT_ASKA)
    p.add_argument("--eydallin", type=Path, default=DEFAULT_EYDALLIN)
    p.add_argument("--arm", default="baked", help="eydallin direction arm")
    p.add_argument("--coverage", type=float, default=0.90)
    p.add_argument("--out", type=Path, default=HERE / "cache/aska_eydallin_kde.png")
    a = p.parse_args()

    aska = pd.read_csv(a.aska, sep="\t")
    aska = aska[aska.n_rxn > 0].copy()
    d_aska = aska.delta.to_numpy(float)
    keep_aska = responders(np.abs(d_aska), a.coverage)

    eyd = pd.read_csv(a.eydallin, sep="\t")
    eyd = eyd[eyd.arm == a.arm].dropna(subset=["pct_wt"]).copy()
    d_eyd = eyd.delta.to_numpy(float)
    keep_eyd = responders(np.abs(d_eyd), a.coverage)

    span = float(max(np.max(np.abs(d_aska)), np.max(np.abs(d_eyd))))
    pad = 0.08 * span
    xlim = (-span - pad, span + pad)

    fig, (axK, axB) = plt.subplots(
        2, 1, figsize=(9.0, 6.4), facecolor=SURFACE, sharex=True,
        gridspec_kw={"height_ratios": [3.2, 1.2], "hspace": 0.06})

    style(axK)
    xs = np.linspace(xlim[0], xlim[1], 800)
    for d, color, label, n in (
            (d_aska, ASKA, "ASKA library (all clones)", len(d_aska)),
            (d_eyd, EYDALLIN, "eydallin cohort", len(d_eyd))):
        if np.ptp(d) == 0 or len(d) < 2:
            continue
        bw = max(span / 40.0, 1e-12) / (np.std(d) or 1.0)
        kde = gaussian_kde(d, bw_method=bw)
        ys = kde(xs)
        axK.plot(xs, ys, color=color, lw=1.8, label=f"{label} (n={n})")
        axK.fill_between(xs, ys, color=color, alpha=0.12)
    axK.axvline(0.0, color=TEXT_SECONDARY, lw=1.0, ls="--", alpha=0.7)
    axK.set_ylabel("density", color=TEXT_SECONDARY, fontsize=10)
    axK.set_title("ASKA library vs. eydallin cohort, glycogen Δ under universal ground "
                  "(leak=1e-3) -- our in-silico ASKA screen",
                  color=TEXT_PRIMARY, fontsize=12, loc="left")
    axK.legend(frameon=False, fontsize=9, labelcolor=TEXT_SECONDARY)
    axK.text(0.98, 0.90,
             f"ASKA carries {a.coverage:.0%} of the response: {int(keep_aska.sum())} of "
             f"{len(aska)} atom-mapped clones\n"
             f"eydallin carries {a.coverage:.0%}: {int(keep_eyd.sum())} of {len(eyd)}",
             transform=axK.transAxes, ha="right", va="top", fontsize=9,
             color=TEXT_SECONDARY, linespacing=1.5)

    style(axB)
    axB.set_yticks([])
    for s in ("top", "right", "left"):
        axB.spines[s].set_visible(False)
    axB.grid(False)

    for xi, hit in zip(d_aska, keep_aska):
        if hit:
            axB.axvline(xi, color=ASKA, lw=1.2, alpha=0.9, ymin=0.55, ymax=0.95)
        else:
            axB.axvline(xi, color=ASKA, lw=0.4, alpha=0.12, ymin=0.55, ymax=0.80)
    for xi, hit in zip(d_eyd, keep_eyd):
        if hit:
            axB.axvline(xi, color=EYDALLIN, lw=1.4, alpha=0.9, ymin=0.05, ymax=0.45)
        else:
            axB.axvline(xi, color=EYDALLIN, lw=0.7, alpha=0.35, ymin=0.20, ymax=0.30)

    axB.axvline(0.0, color=TEXT_SECONDARY, lw=1.0, ls="--", alpha=0.7)
    axB.set_xlim(xlim)
    axB.set_xlabel("Δ draw at glycogen, ×2 fold, universal ground (linear, signed)",
                  color=TEXT_SECONDARY, fontsize=10)
    axB.text(0.01, 0.75, "ASKA", transform=axB.transAxes, ha="left", va="center",
              fontsize=8.5, color=ASKA, style="italic")
    axB.text(0.01, 0.25, "eydallin", transform=axB.transAxes, ha="left", va="center",
              fontsize=8.5, color=EYDALLIN, style="italic")

    named_aska = aska.loc[keep_aska, ["gene", "delta"]].sort_values(
        "delta", key=np.abs, ascending=False)
    named_eyd = eyd.loc[keep_eyd, ["gene", "delta"]].sort_values(
        "delta", key=np.abs, ascending=False)
    axK.text(0.98, 0.72, "ASKA carrying it: " + ", ".join(named_aska.gene.astype(str)[:8]),
             transform=axK.transAxes, ha="right", va="top", fontsize=8.5,
             color=ASKA, wrap=True)
    axK.text(0.98, 0.64, "eydallin carrying it: " + ", ".join(named_eyd.gene.astype(str)[:8]),
             transform=axK.transAxes, ha="right", va="top", fontsize=8.5,
             color=EYDALLIN, wrap=True)

    fig.subplots_adjust(left=0.08, right=0.98, top=0.90, bottom=0.10)
    a.out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(a.out, dpi=170, facecolor=SURFACE)
    print(f"[kde] aska n={len(aska)} ({int(keep_aska.sum())} hits), "
          f"eydallin n={len(eyd)} ({int(keep_eyd.sum())} hits); wrote {a.out}",
          file=sys.stderr)


if __name__ == "__main__":
    main()

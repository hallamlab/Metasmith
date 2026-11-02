#!/usr/bin/env python3
"""Scatter: |log2FC| measured glycogen vs log2FC glucose->glycogen conductance.

Two panels of the same relationship rather than one, because the readout is sparse:
18 of 25 conditions sit at or below a 1e-6 floor, so a single linear panel would show
one cluster on the axis and nothing else. The left panel keeps every condition on a log
axis with the floor drawn explicitly; the right panel is the seven that clear it, linear,
where the within-set trend is visible and runs the wrong way.

Direction is an ENCODING here, not an axis: the two-point probe is monotone (Rayleigh),
so y cannot go negative under a fold > 1. Colouring excess vs deficient is what shows
that the model's biggest responders are the genes that LOWER glycogen.

    mamba run -n figure-net python research/fabfos/benchmarks/eydallin/plot_twopoint_cohort.py
"""
from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy.stats import spearmanr

HERE = Path(__file__).resolve().parents[1]
ROOT = HERE.parents[3]
DEFAULT_IN = (ROOT / "data/fabfos/runs/eydallin_clones/ecspr"
              / "twopoint_cohort_e_coli_ag1_fold2.0_C.tsv")

BLUE = "#2a78d6"
RED = "#e34948"
SURFACE = "#fcfcfb"
TEXT_PRIMARY = "#0b0b0b"
TEXT_SECONDARY = "#52514e"
TEXT_MUTED = "#8a8981"
GRID = "#e3e2dc"

FLOOR = 1e-6
ON_PATH = 1e-4


def _style(ax):
    ax.set_facecolor(SURFACE)
    ax.grid(True, color=GRID, linewidth=0.8, zorder=0)
    ax.set_axisbelow(True)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(GRID)
        ax.spines[side].set_linewidth(0.8)
    ax.tick_params(colors=TEXT_SECONDARY, labelsize=9, length=0)


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--table", type=Path, default=DEFAULT_IN)
    p.add_argument("--out", type=Path, default=HERE / "cache" / "twopoint_cohort_scatter.png")
    args = p.parse_args()
    args.out.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(args.table, sep="\t").dropna(subset=["pct_wt"]).copy()
    df["color"] = np.where(df.direction == "excess", BLUE, RED)
    df["y"] = df.log2fc_ieff.clip(lower=FLOOR)
    df["at_floor"] = df.log2fc_ieff <= FLOOR
    on = df[df.log2fc_ieff > ON_PATH].copy()

    rho_all, p_all = spearmanr(df.abs_log2fc_meas, df.log2fc_ieff)
    rho_on, p_on = spearmanr(on.abs_log2fc_meas, on.log2fc_ieff)

    fig, (axL, axR) = plt.subplots(1, 2, figsize=(13.0, 6.2), facecolor=SURFACE,
                                   gridspec_kw=dict(width_ratios=[1.25, 1.0], wspace=0.24))
    for ax in (axL, axR):
        _style(ax)

    axL.set_yscale("log")
    axL.axhspan(FLOOR / 2.4, FLOOR, color=GRID, alpha=0.55, zorder=1, linewidth=0)
    axL.axhline(ON_PATH, color=TEXT_MUTED, linewidth=1.0, zorder=2)

    for at_floor, mk in ((False, dict(alpha=0.9)), (True, dict(alpha=0.55))):
        sub = df[df.at_floor == at_floor]
        axL.scatter(sub.abs_log2fc_meas, sub.y, s=78, c=sub.color, zorder=4,
                    edgecolors=SURFACE, linewidths=1.8, **mk)

    for _, r in on.iterrows():
        axL.annotate(r.gene, xy=(r.abs_log2fc_meas, r.y), xytext=(8, -3),
                     textcoords="offset points", fontsize=10, color=TEXT_PRIMARY,
                     zorder=6)

    n_floor = int(df.at_floor.sum())
    axL.annotate(f"at or below floor — no measurable path  (n={n_floor})",
                 xy=(0.5, FLOOR), xytext=(0.52, FLOOR * 1.9), fontsize=9,
                 color=TEXT_MUTED, zorder=6)
    axL.annotate(f"responders (n={len(on)})", xy=(0.5, ON_PATH), xytext=(2.02, ON_PATH * 1.5),
                 fontsize=9, color=TEXT_MUTED, ha="right", zorder=6)

    axL.set_ylim(FLOOR / 2.6, 1.4)
    axL.set_xlim(0.35, 2.42)
    axL.set_xlabel("|log2FC| measured glycogen  (vs wild-type)", fontsize=10.5,
                   color=TEXT_SECONDARY, labelpad=9)
    axL.set_ylabel("log2FC glucose→glycogen conductance  (gene ×2)", fontsize=10.5,
                   color=TEXT_SECONDARY, labelpad=9)
    axL.set_title(f"All 25 in-universe conditions      ρ = {rho_all:+.3f}  (p = {p_all:.2f})",
                  fontsize=11.5, color=TEXT_PRIMARY, loc="left", pad=12)

    axR.scatter(on.abs_log2fc_meas, on.log2fc_ieff, s=96, c=on.color, zorder=4,
                edgecolors=SURFACE, linewidths=1.8, alpha=0.9)
    OFFSETS = {"glgB": (-12, -6, "right"), "glgC": (10, -6, "left")}
    for _, r in on.iterrows():
        dx, dy, ha = OFFSETS.get(r.gene, (0, 11, "center"))
        axR.annotate(f"{r.gene}\n{r.pct_wt:.0f}%", xy=(r.abs_log2fc_meas, r.log2fc_ieff),
                     xytext=(dx, dy), textcoords="offset points", ha=ha,
                     fontsize=9.5, color=TEXT_PRIMARY, linespacing=1.35, zorder=6)

    fit = np.polyfit(on.abs_log2fc_meas, on.log2fc_ieff, 1)
    xs = np.linspace(on.abs_log2fc_meas.min() - 0.08, on.abs_log2fc_meas.max() + 0.08, 50)
    axR.plot(xs, np.polyval(fit, xs), color=TEXT_MUTED, linewidth=1.6, zorder=3)

    axR.set_ylim(-0.06, 0.60)
    axR.set_xlim(0.55, 2.35)
    axR.set_xlabel("|log2FC| measured glycogen  (vs wild-type)", fontsize=10.5,
                   color=TEXT_SECONDARY, labelpad=9)
    axR.set_ylabel("log2FC glucose→glycogen conductance", fontsize=10.5,
                   color=TEXT_SECONDARY, labelpad=9)
    axR.set_title(f"The {len(on)} responders only      ρ = {rho_on:+.3f}  (p = {p_on:.2f})",
                  fontsize=11.5, color=TEXT_PRIMARY, loc="left", pad=12)

    handles = [plt.Line2D([], [], marker="o", linestyle="", markersize=9, color=BLUE,
                          markeredgecolor=SURFACE, markeredgewidth=1.6,
                          label="glycogen excess (>WT)"),
               plt.Line2D([], [], marker="o", linestyle="", markersize=9, color=RED,
                          markeredgecolor=SURFACE, markeredgewidth=1.6,
                          label="glycogen deficient (<WT)")]
    leg = fig.legend(handles=handles, loc="upper right", ncol=2, frameon=False,
                     fontsize=9.5, handletextpad=0.5,
                     bbox_to_anchor=(0.988, 0.998), columnspacing=1.6)
    for t in leg.get_texts():
        t.set_color(TEXT_SECONDARY)

    fig.suptitle("Two-point ECSPr conductance does not track glycogen phenotype magnitude",
                 fontsize=14, color=TEXT_PRIMARY, x=0.008, ha="left", y=0.985)
    fig.text(0.008, 0.925,
             "eydallin cohort on host e_coli_ag1, r7 bake, element C, conductance fold ×2. "
             "The probe is monotone, so y cannot go negative — direction is colour, not sign.",
             fontsize=9.5, color=TEXT_SECONDARY, ha="left")

    fig.subplots_adjust(top=0.845, bottom=0.105, left=0.062, right=0.985)
    fig.savefig(args.out, dpi=170, facecolor=SURFACE)
    print(f"wrote {args.out}")
    print(f"  all 25   rho={rho_all:+.4f} p={p_all:.3g}")
    print(f"  {len(on)} responders  rho={rho_on:+.4f} p={p_on:.3g}")
    print(f"  {n_floor} at/below floor {FLOOR:g}")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Scatter: predicted glycogen delta vs. measured phenotype, across the cohort_delta_panel.py run.

    mamba run -n figure-net python main/benchmarks/eydallin/plot_cohort_delta_panel.py \
        --panel cache/cohort_delta_panel_gem_fold2.0_C.parquet
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

BLUE = "#2a78d6"
RED = "#e34948"
SURFACE = "#fcfcfb"
TEXT_PRIMARY = "#0b0b0b"
TEXT_SECONDARY = "#52514e"
GRID = "#e3e2dc"

CALLOUTS = {"glgC", "glgA", "glgB", "glgP", "ddg", "malP"}


def _arcsinh_scale(x, lin_thresh):
    return np.arcsinh(x / lin_thresh)


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--panel", type=Path, required=True)
    p.add_argument("--out", type=Path, default=None)
    args = p.parse_args()

    df = pd.read_parquet(args.panel).dropna(subset=["pct_wt"]).copy()
    rho, pval = spearmanr(df.delta, df.pct_wt)

    nonzero = np.abs(df.delta.to_numpy())
    nonzero = nonzero[nonzero > 0]
    lin_thresh = float(np.median(nonzero)) if nonzero.size else 1e-12
    df["x"] = _arcsinh_scale(df.delta.to_numpy(), lin_thresh)

    fig, ax = plt.subplots(figsize=(9.5, 6.5), facecolor=SURFACE)
    ax.set_facecolor(SURFACE)

    ax.scatter(df.x, df.pct_wt, s=48, color=BLUE, alpha=0.75, zorder=3,
              edgecolors=SURFACE, linewidths=0.8)
    ax.axhline(100, color=TEXT_SECONDARY, linewidth=1, linestyle="--", alpha=0.6, zorder=1)
    ax.axvline(0, color=TEXT_SECONDARY, linewidth=1, linestyle="--", alpha=0.6, zorder=1)

    for _, r in df[df.gene.isin(CALLOUTS)].iterrows():
        ax.annotate(r.gene, xy=(r.x, r.pct_wt), xytext=(7, 4), textcoords="offset points",
                   color=RED, fontsize=9.5, fontweight="bold", zorder=4)
        ax.scatter([r.x], [r.pct_wt], s=70, facecolors="none", edgecolors=RED,
                  linewidths=1.4, zorder=4)

    finite = df.x[np.isfinite(df.x)]
    span = max(abs(finite.min()), abs(finite.max())) if len(finite) else 1
    tick_x = np.linspace(-span, span, 7)
    tick_val = np.sinh(tick_x) * lin_thresh
    ax.set_xticks(tick_x)
    ax.set_xticklabels([f"{v:.1e}" for v in tick_val], rotation=30, ha="right")

    ax.set_xlabel("predicted Δ draw at glycogen (base → gene ×2, signed arcsinh scale)",
                  color=TEXT_SECONDARY)
    ax.set_ylabel("measured glycogen, % of WT (Eydallin 2010 Fig. 1)", color=TEXT_SECONDARY)

    ax.grid(axis="both", color=GRID, linewidth=0.7, zorder=0)
    ax.set_axisbelow(True)
    for spine in ("top", "right"):
        ax.spines[spine].set_visible(False)
    for spine in ("left", "bottom"):
        ax.spines[spine].set_color(GRID)
    ax.tick_params(colors=TEXT_SECONDARY)

    fig.suptitle("Predicted vs. measured glycogen response — eydallin GOF cohort, GEM channel",
                x=0.02, y=0.985, ha="left", color=TEXT_PRIMARY, fontsize=14, fontweight="bold")
    fig.text(0.02, 0.945,
            f"n={len(df)} conditions  ·  Spearman ρ={rho:+.3f} (p={pval:.2g})  ·  "
            f"fold ×2, source D-glucose, universal leak 1e-6, host e_coli_ag1/DH1",
            color=TEXT_SECONDARY, fontsize=9.5, ha="left", va="top")

    fig.tight_layout(rect=(0, 0, 1, 0.9))
    out = args.out or (HERE / "cache" / (args.panel.stem + "_scatter.png"))
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=180, facecolor=SURFACE)
    print(f"[plot] wrote {out}")


if __name__ == "__main__":
    main()

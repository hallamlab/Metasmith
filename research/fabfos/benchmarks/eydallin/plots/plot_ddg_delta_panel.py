#!/usr/bin/env python3
"""Histogram of delta draw across every metabolite, ddg overexpressed, glycogen marked.

Reads the cache `ddg_delta_panel.py` writes. Deltas span orders of magnitude and both
signs, concentrated near the solver's numerical floor (~1e-13) with a long tail out to
the metabolites structurally close to ddg's own reaction -- a linear axis would show one
spike at zero and nothing else, so x is signed-arcsinh-scaled (behaves like log far from
zero, linear near it, and unlike log it accepts negative deltas without folding them onto
|delta|).

    mamba run -n figure-net python main/benchmarks/eydallin/plot_ddg_delta_panel.py \
        --panel cache/ddg_delta_panel_fold2.0_as_written_C.parquet
"""
from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parents[1]
GLYCOGEN_MNXM = "MNXM738130"

BLUE = "#2a78d6"
RED = "#e34948"
SURFACE = "#fcfcfb"
TEXT_PRIMARY = "#0b0b0b"
TEXT_SECONDARY = "#52514e"
GRID = "#e3e2dc"


def _arcsinh_scale(x, lin_thresh):
    return np.arcsinh(x / lin_thresh)


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--panel", type=Path, required=True,
                   help="parquet written by ddg_delta_panel.py")
    p.add_argument("--out", type=Path, default=None)
    p.add_argument("--bins", type=int, default=60)
    args = p.parse_args()

    df = pd.read_parquet(args.panel)
    if GLYCOGEN_MNXM not in set(df.mnxm):
        raise SystemExit(f"[plot] {GLYCOGEN_MNXM} not in {args.panel} -- wrong panel file?")

    gly_delta = float(df.loc[df.mnxm == GLYCOGEN_MNXM, "delta"].iloc[0])
    delta = df["delta"].to_numpy()

    nonzero = np.abs(delta[delta != 0])
    lin_thresh = float(np.median(nonzero)) if nonzero.size else 1e-12
    x = _arcsinh_scale(delta, lin_thresh)
    gly_x = _arcsinh_scale(np.array([gly_delta]), lin_thresh)[0]

    fig, ax = plt.subplots(figsize=(9, 6), facecolor=SURFACE)
    ax.set_facecolor(SURFACE)

    counts, edges, _ = ax.hist(x, bins=args.bins, color=BLUE, edgecolor=SURFACE,
                               linewidth=0.5, zorder=2)
    ax.set_ylim(0, counts.max() * 1.22)

    ax.axvline(gly_x, color=RED, linewidth=2, zorder=3)
    ax.annotate(
        f"glycogen  Δ={gly_delta:.2e}",
        xy=(gly_x, counts.max() * 1.02), xytext=(0, 6), textcoords="offset points",
        ha="center", va="bottom", color=RED, fontsize=10, fontweight="bold",
    )

    finite = x[np.isfinite(x)]
    lo, hi = (finite.min(), finite.max()) if finite.size else (-1, 1)
    span = max(abs(lo), abs(hi), abs(gly_x))
    tick_x = np.linspace(-span, span, 7)
    tick_val = np.sinh(tick_x) * lin_thresh
    ax.set_xticks(tick_x)
    ax.set_xticklabels([f"{v:.1e}" for v in tick_val], rotation=30, ha="right")

    ax.set_xlabel("Δ draw (glucose leak, base → ddg ×2 pert)  —  signed arcsinh scale",
                  color=TEXT_SECONDARY)
    ax.set_ylabel("metabolites", color=TEXT_SECONDARY)

    ax.grid(axis="y", color=GRID, linewidth=0.7, zorder=0)
    ax.set_axisbelow(True)
    for spine in ("top", "right"):
        ax.spines[spine].set_visible(False)
    for spine in ("left", "bottom"):
        ax.spines[spine].set_color(GRID)
    ax.tick_params(colors=TEXT_SECONDARY)

    n = len(df)
    rank = int((df.delta.abs() >= abs(gly_delta)).sum())
    fig.suptitle("Whole-metabolome $\\Delta I_{eff}$ under ddg (lpxP) overexpression",
                x=0.02, y=0.985, ha="left", color=TEXT_PRIMARY, fontsize=14, fontweight="bold")
    fig.text(0.02, 0.945,
            f"n={n} metabolites  ·  glycogen ranks {rank}/{n} by |Δ|  ·  "
            f"source D-glucose, universal leak 1e-6, host e_coli_k12",
            color=TEXT_SECONDARY, fontsize=9.5, ha="left", va="top")

    fig.tight_layout(rect=(0, 0, 1, 0.9))
    out = args.out or (HERE / "cache" / (args.panel.stem + "_hist.png"))
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=180, facecolor=SURFACE)
    print(f"[plot] wrote {out}")


if __name__ == "__main__":
    main()

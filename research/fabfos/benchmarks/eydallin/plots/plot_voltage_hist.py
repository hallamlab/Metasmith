#!/usr/bin/env python3
"""Histogram of per-metabolite voltage, base host GEM, glucose source, glycogen marked.

Reads the cache `voltage_vs_minpath.py` writes.

    mamba run -n figure-net python main/benchmarks/eydallin/plot_voltage_hist.py
"""
from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

HERE = Path(__file__).resolve().parents[1]
GLYCOGEN_MNXM = "MNXM738130"

BLUE = "#2a78d6"
RED = "#e34948"
SURFACE = "#fcfcfb"
TEXT_PRIMARY = "#0b0b0b"
TEXT_SECONDARY = "#52514e"
GRID = "#e3e2dc"


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--panel", type=Path,
                   default=HERE / "cache" / "voltage_vs_minpath_base.parquet")
    p.add_argument("--out", type=Path, default=None)
    p.add_argument("--bins", type=int, default=60)
    p.add_argument("--min-voltage", type=float, default=None,
                   help="drop metabolites at or below this voltage before plotting")
    args = p.parse_args()

    df = pd.read_parquet(args.panel)
    gly_v = float(df.loc[df.mnxm == GLYCOGEN_MNXM, "voltage"].iloc[0])
    n_total = len(df)
    if args.min_voltage is not None:
        df = df[df.voltage > args.min_voltage]

    fig, ax = plt.subplots(figsize=(9, 6), facecolor=SURFACE)
    ax.set_facecolor(SURFACE)

    counts, edges, _ = ax.hist(df.voltage, bins=args.bins, color=BLUE, edgecolor=SURFACE,
                               linewidth=0.5, zorder=2)
    ax.set_yscale("log")
    ax.set_ylim(bottom=0.5)

    ax.axvline(gly_v, color=RED, linewidth=2, zorder=3)
    ax.annotate(f"glycogen  {gly_v:.1f}", xy=(gly_v, ax.get_ylim()[1]),
               xytext=(-10, -14), textcoords="offset points",
               ha="right", va="top", color=RED, fontsize=10, fontweight="bold")

    ax.set_xlabel("voltage — potential relative to ground, arbitrary model units "
                  "(injected current = 1)", color=TEXT_SECONDARY)
    ax.set_ylabel("metabolites (log scale)", color=TEXT_SECONDARY)

    ax.grid(axis="y", color=GRID, linewidth=0.7, zorder=0, which="both")
    ax.set_axisbelow(True)
    for spine in ("top", "right"):
        ax.spines[spine].set_visible(False)
    for spine in ("left", "bottom"):
        ax.spines[spine].set_color(GRID)
    ax.tick_params(colors=TEXT_SECONDARY)

    n = len(df)
    title = "Distribution of metabolite voltage"
    if args.min_voltage is not None:
        title += f" (v > {args.min_voltage:g})"
    fig.suptitle(title, x=0.02, y=0.985, ha="left", color=TEXT_PRIMARY, fontsize=14,
                fontweight="bold")
    base = "base host GEM, glucose source, universal leak 1e-6, host e_coli_k12"
    subtitle = f"n={n}/{n_total} metabolites shown  ·  {base}"
    if args.min_voltage is None:
        near_ceiling = int((df.voltage > df.voltage.max() * 0.9).sum())
        subtitle = (f"n={n} metabolites  ·  {near_ceiling} within 10% of the ceiling "
                   f"({df.voltage.max():.1f})  ·  {base}")
    fig.text(0.02, 0.945, subtitle, color=TEXT_SECONDARY, fontsize=9.5, ha="left", va="top")

    fig.tight_layout(rect=(0, 0, 1, 0.9))
    out = args.out or (HERE / "cache" / "voltage_hist_base.png")
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=180, facecolor=SURFACE)
    print(f"[plot] wrote {out}")


if __name__ == "__main__":
    main()

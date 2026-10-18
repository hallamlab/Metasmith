#!/usr/bin/env python3
"""Histogram of potential-drop-from-source (V(source) - V(m)) at a given leak.

Reads a panel `leak_sweep.py` writes (cache/drop_vs_minpath_leak{leak:.0e}.parquet).

    mamba run -n figure-net python main/benchmarks/eydallin/plot_drop_hist.py --leak 0.01
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

LANDMARKS = {
    "MNXM23": "pyruvate",
    "MNXM46": "OAA",
    "MNXM1104385": "XMP",
    "MNXM73": "PEP",
    "MNXM1107752": "citrate",
    "MNXM1368744": "2-oxoglutarate",
    "MNXM1364111": "G6P",
}

BLUE = "#2a78d6"
RED = "#e34948"
SURFACE = "#fcfcfb"
TEXT_PRIMARY = "#0b0b0b"
TEXT_SECONDARY = "#52514e"
GRID = "#e3e2dc"


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--leak", type=float, required=True)
    p.add_argument("--panel", type=Path, default=None)
    p.add_argument("--out", type=Path, default=None)
    p.add_argument("--bins", type=int, default=60)
    args = p.parse_args()

    panel = args.panel or (HERE / "cache" / f"drop_vs_minpath_leak{args.leak:.0e}.parquet")
    df = pd.read_parquet(panel)
    gly_drop = float(df.loc[df.mnxm == GLYCOGEN_MNXM, "drop"].iloc[0])

    fig, ax = plt.subplots(figsize=(9, 6), facecolor=SURFACE)
    ax.set_facecolor(SURFACE)

    ax.hist(df["drop"], bins=args.bins, color=BLUE, edgecolor=SURFACE, linewidth=0.5, zorder=2)
    ax.set_yscale("log")
    ax.set_ylim(bottom=0.5)

    ax.axvline(gly_drop, color=RED, linewidth=2, zorder=3)
    ax.annotate(f"glycogen  {gly_drop:.4g}", xy=(gly_drop, ax.get_ylim()[1]),
               xytext=(10, -14), textcoords="offset points",
               ha="left", va="top", color=RED, fontsize=10, fontweight="bold")

    top = ax.get_ylim()[1]
    stagger = [-14, -34, -54, -74, -94, -114, -134, -154]
    resolved = [(m, lbl) for m, lbl in LANDMARKS.items() if m in set(df.mnxm)]
    missing = [lbl for m, lbl in LANDMARKS.items() if m not in set(df.mnxm)]
    if missing:
        print(f"[plot] landmarks not in panel (disconnected from source/sink component): "
              f"{missing}")
    for i, (m, lbl) in enumerate(resolved):
        x = float(df.loc[df.mnxm == m, "drop"].iloc[0])
        ax.axvline(x, color=TEXT_SECONDARY, linewidth=1, linestyle="--", alpha=0.6, zorder=3)
        ax.annotate(f"{lbl}  {x:.3g}", xy=(x, top),
                   xytext=(6, stagger[i % len(stagger)]), textcoords="offset points",
                   ha="left", va="top", color=TEXT_PRIMARY, fontsize=8.5,
                   arrowprops=dict(arrowstyle="-", color=TEXT_SECONDARY, alpha=0.6,
                                    shrinkA=0, shrinkB=0, linewidth=0.7))

    ax.set_xlabel("drop = V(source) - V(metabolite), arbitrary model units",
                  color=TEXT_SECONDARY)
    ax.set_ylabel("metabolites (log scale)", color=TEXT_SECONDARY)

    ax.grid(axis="y", color=GRID, linewidth=0.7, zorder=0, which="both")
    ax.set_axisbelow(True)
    for spine in ("top", "right"):
        ax.spines[spine].set_visible(False)
    for spine in ("left", "bottom"):
        ax.spines[spine].set_color(GRID)
    ax.tick_params(colors=TEXT_SECONDARY)

    near0 = int((df["drop"] < df["drop"].max() * 0.01).sum())
    near_max = int((df["drop"] > df["drop"].max() * 0.99).sum())
    fig.suptitle(f"Distribution of potential drop from source (leak={args.leak:g})",
                x=0.02, y=0.985, ha="left", color=TEXT_PRIMARY, fontsize=14,
                fontweight="bold")
    subtitle = (f"n={len(df)} metabolites  ·  {near0} within 1% of zero  ·  "
               f"{near_max} within 1% of max  ·  base host GEM, glucose source")
    fig.text(0.02, 0.945, subtitle, color=TEXT_SECONDARY, fontsize=9.5, ha="left", va="top")

    fig.tight_layout(rect=(0, 0, 1, 0.9))
    out = args.out or (HERE / "cache" / f"drop_hist_leak{args.leak:.0e}.png")
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=180, facecolor=SURFACE)
    print(f"[plot] wrote {out}")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Scatter: per-metabolite voltage (drop to ground) vs. reaction-hop minpath from glucose.

Reads the cache `voltage_vs_minpath.py` writes (base host GEM, no perturbation).

    mamba run -n figure-net python main/benchmarks/eydallin/plot_voltage_vs_minpath.py
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
    args = p.parse_args()

    df = pd.read_parquet(args.panel).dropna(subset=["hop_dist"])
    rho, pval = spearmanr(df.hop_dist, df.voltage)

    rng = np.random.default_rng(0)
    jitter = rng.uniform(-0.12, 0.12, size=len(df))

    fig, ax = plt.subplots(figsize=(9, 6), facecolor=SURFACE)
    ax.set_facecolor(SURFACE)

    ax.scatter(df.hop_dist + jitter, df.voltage, s=16, color=BLUE, alpha=0.35,
              linewidths=0, zorder=2)

    if GLYCOGEN_MNXM in set(df.mnxm):
        gly = df[df.mnxm == GLYCOGEN_MNXM].iloc[0]
        ax.scatter([gly.hop_dist], [gly.voltage], s=90, color=RED, zorder=4,
                  edgecolors=SURFACE, linewidths=1.2)
        ax.annotate("glycogen", xy=(gly.hop_dist, gly.voltage), xytext=(10, -14),
                   textcoords="offset points", color=RED, fontsize=10, fontweight="bold")

    med = df.groupby("hop_dist")["voltage"].median()
    ax.plot(med.index, med.values, color=TEXT_PRIMARY, linewidth=1.5, marker="o",
           markersize=5, zorder=3, label="median per hop distance")

    ax.set_xlabel("reaction-hop minpath from D-glucose (AAM-compiled, host-restricted, "
                  "cofactors barred)", color=TEXT_SECONDARY)
    ax.set_ylabel("voltage — potential relative to ground (universal leak sink)",
                  color=TEXT_SECONDARY)
    ax.set_xticks(sorted(df.hop_dist.unique()))

    ax.grid(axis="y", color=GRID, linewidth=0.7, zorder=0)
    ax.set_axisbelow(True)
    for spine in ("top", "right"):
        ax.spines[spine].set_visible(False)
    for spine in ("left", "bottom"):
        ax.spines[spine].set_color(GRID)
    ax.tick_params(colors=TEXT_SECONDARY)
    ax.legend(frameon=False, loc="lower left", labelcolor=TEXT_SECONDARY, fontsize=9)

    fig.suptitle("Voltage vs. topological distance — base host GEM, glucose source",
                x=0.02, y=0.985, ha="left", color=TEXT_PRIMARY, fontsize=14, fontweight="bold")
    fig.text(0.02, 0.945,
            f"n={len(df)} metabolites  ·  Spearman ρ={rho:.3f} (p={pval:.2g})  ·  "
            f"universal leak 1e-6, host e_coli_k12",
            color=TEXT_SECONDARY, fontsize=9.5, ha="left", va="top")

    fig.tight_layout(rect=(0, 0, 1, 0.9))
    out = args.out or (HERE / "cache" / "voltage_vs_minpath_base.png")
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=180, facecolor=SURFACE)
    print(f"[plot] wrote {out}")


if __name__ == "__main__":
    main()

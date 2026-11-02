#!/usr/bin/env python3
"""Draw indicator species results: one chart per label, and one aligned across labels.

Replaces upstream's plot_indicspecies.py and plot_indicspecies_aligned.py, which drew
exactly two groupings against each other with one study's palettes. Per label, the
significant indicators are drawn by IndVal statistic and coloured by the level set they
indicate. With two or more labels, the aligned chart places every indicator of any label
on one axis, one column per label.
"""

import argparse
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402
import seaborn as sns  # noqa: E402

SUFFIX = "_indicator_species_summary.tsv"


def indicated(row, level_cols):
    return "+".join(c[2:] for c in level_cols if row[c] == 1)


def load(results_dir):
    tables = {}
    for path in sorted(results_dir.glob(f"*{SUFFIX}")):
        df = pd.read_csv(path, sep="\t")
        level_cols = [c for c in df.columns if c.startswith("s.")]
        if "significant" not in df.columns or df.empty:
            continue
        df = df.loc[df["significant"] == True].copy()  # noqa: E712
        df["group"] = df.apply(indicated, axis=1, level_cols=level_cols)
        tables[path.name[: -len(SUFFIX)]] = df
    return tables


def per_label(tables, out, top):
    out.mkdir(parents=True, exist_ok=True)
    for label, df in tables.items():
        if df.empty:
            (out / f"{label}_no_indicators.txt").write_text(f"no significant indicator for {label}\n")
            continue
        df = df.sort_values("stat", ascending=False).head(top)
        fig, ax = plt.subplots(figsize=(8, max(3, 0.28 * len(df))))
        sns.barplot(data=df, x="stat", y="ASV", hue="group", dodge=False, ax=ax)
        ax.set_xlabel("IndVal statistic")
        ax.set_title(f"Indicators of {label}")
        fig.tight_layout()
        for ext in ("svg", "png"):
            fig.savefig(out / f"{label}_indicators.{ext}", dpi=150)
        plt.close(fig)


def aligned(tables, out):
    out.mkdir(parents=True, exist_ok=True)
    if len(tables) < 2:
        (out / "SKIPPED.txt").write_text(
            f"aligned plots need two or more labels with results; found {len(tables)}\n")
        return
    wide = pd.concat({label: df.set_index("ASV")["stat"] for label, df in tables.items()}, axis=1)
    wide = wide.fillna(0).sort_values(list(wide.columns), ascending=False)
    fig, ax = plt.subplots(figsize=(2 + 1.2 * wide.shape[1], max(3, 0.22 * len(wide))))
    sns.heatmap(wide, cmap="viridis", ax=ax, cbar_kws={"label": "IndVal statistic"})
    fig.tight_layout()
    for ext in ("svg", "png"):
        fig.savefig(out / f"indicators_aligned.{ext}", dpi=150)
    plt.close(fig)
    wide.to_csv(out / "indicators_aligned.tsv", sep="\t")


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--results", type=Path, required=True)
    ap.add_argument("--plots", type=Path, required=True)
    ap.add_argument("--aligned", type=Path, required=True)
    ap.add_argument("--top", type=int, default=40)
    args = ap.parse_args()
    tables = load(args.results)
    per_label(tables, args.plots, args.top)
    aligned(tables, args.aligned)


if __name__ == "__main__":
    main()

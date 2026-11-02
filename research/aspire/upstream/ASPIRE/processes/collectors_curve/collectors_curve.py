#!/usr/bin/env python3
"""
collectors_curves.py
Build species-accumulation (collector’s) curves from an ASV count table.

Inputs
------
- Counts table (TSV/TSV.GZ): rows = ASVs, columns = samples, integer counts (>=0).
- Metadata (TSV/TSV.GZ): includes a sample-id column and a grouping column.

What the bounds mean
--------------------
At each step k (1..#samples in a group), we randomize sample order many times
and record the number of unique ASVs observed after k samples. The low/high
curves are the 2.5th and 97.5th percentiles across those permutations — a 95%
Monte Carlo envelope over ordering uncertainty (not a parametric CI).

Features
--------
- Faceted and overlay plots by group (same hues).
- Dashed vertical line at the TRUE sample count per group (same hue).
- Upper-only x-axis padding so the dashed line at the maximum is visible.
- Optional explicit color mapping per group (validated, with warnings).
- Optional explicit group order for both facets (left→right, top→bottom)
  and legend order.
- Multiple output formats with --formats (e.g., pdf,png,svg).
- Presence threshold control (default >0).

CLI
---
python collectors_curves.py \
  --counts asv_counts.tsv \
  --meta sample_metadata.tsv \
  --group-col sample_type \
  --permutations 512 \
  --palette tab10 \
  --xpad 0.75 \
  --title "Collector's curves" \
  --formats pdf,png \
  --out_prefix collectors

Outputs
-------
- collectors_overlay.<ext> (per requested format)
- collectors_faceted.<ext>
- collectors_stats/<group>_collector_stats.tsv
- collectors_summary.tsv  (one-row summary per group)
"""

import math
import argparse
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.colors import to_rgba

plt.switch_backend("Agg")

SAMPLE_ID_COL = 'sampleID'


# ----------------------------- IO & Prep ------------------------------------
def _read_table(path: str) -> pd.DataFrame:
    path = str(path)
    sep = "\t"
    if path.endswith(".gz"):
        return pd.read_csv(path, sep=sep, compression="infer")
    return pd.read_csv(path, sep=sep)


def read_inputs(counts_path, meta_path, sample_id_col, group_col):
    counts = _read_table(counts_path)
    if counts.shape[1] < 2:
        raise ValueError("Counts table must have ASVs in rows and samples in columns. "
                         "Ensure the first column is an index/ASV ID and the rest are samples.")
    counts = counts.set_index(counts.columns[0])

    meta = _read_table(meta_path)

    # Validate required columns
    for c in (sample_id_col, group_col):
        if c not in meta.columns:
            raise ValueError(
                f"Metadata missing required column: '{c}'. "
                f"Available: {list(meta.columns)}"
            )

    # Sanitize IDs/groups to avoid hidden dupes
    meta[sample_id_col] = meta[sample_id_col].astype(str).str.strip()
    meta[group_col] = meta[group_col].astype(str).str.strip()

    # Keep only intersecting samples, preserve column order in counts
    keep = [c for c in counts.columns if c in set(meta[sample_id_col])]
    counts = counts.loc[:, keep]
    meta = meta[meta[sample_id_col].isin(keep)].copy()

    if counts.empty or meta.empty:
        raise ValueError("No overlapping samples between counts and metadata with provided column names.")
    return counts, meta


# ----------------------------- Colors & Ordering ----------------------------
def parse_group_colors(arg_string):
    """
    Parse "GroupA=#1f77b4,GroupB:orange" into dict {group: color_str}
    """
    mapping = {}
    if not arg_string:
        return mapping
    parts = [p for p in arg_string.split(",") if p.strip()]
    for p in parts:
        if "=" in p:
            k, v = p.split("=", 1)
        elif ":" in p:
            k, v = p.split(":", 1)
        else:
            warnings.warn(f"Ignoring malformed group-color pair '{p}'. Use Group=Color.")
            continue
        mapping[k.strip()] = v.strip()
    return mapping


def parse_list_csv(arg: str):
    if not arg:
        return []
    return [x.strip() for x in str(arg).split(",") if x and x.strip()]


def validate_color(color_str, group_name):
    try:
        to_rgba(color_str)  # raises if invalid
        return True
    except ValueError:
        warnings.warn(f"Ignoring invalid color '{color_str}' for group '{group_name}'. Falling back to palette.")
        return False


def resolve_group_order(meta_series, order):
    uniq = list(pd.unique(meta_series.fillna("NA")))
    if not order:
        return uniq
    specified = order
    unknown = [g for g in specified if g not in uniq]
    if unknown:
        warnings.warn(f"--group-order contains unknown group(s) {unknown}; they will be ignored.")
    final = [g for g in specified if g in uniq]
    # Append any groups not specified, preserving original appearance order
    final += [g for g in uniq if g not in final]
    return final


def color_map_for_groups(groups, palette_name="tab10", user_map=None):
    """
    Build a color map for groups (in the supplied order).
    - user_map overrides palette per group (validated)
    - unknown groups in user_map => warn
    - groups missing in user_map => palette fallback
    """
    groups = list(groups)
    cmap = plt.get_cmap(palette_name)
    base_colors = [cmap(i % cmap.N) for i in range(len(groups))]

    colors = {}
    user_map = user_map or {}

    # Warn on unknown groups supplied by user
    unknown = [g for g in user_map if g not in groups]
    if unknown:
        warnings.warn(f"--group-colors provided for unknown group(s): {unknown}. They will be ignored.")

    for i, g in enumerate(groups):
        if g in user_map and validate_color(user_map[g], g):
            colors[g] = user_map[g]
        else:
            colors[g] = base_colors[i]

    # Warn on groups missing explicit colors if user_map was given
    if user_map:
        missing = [g for g in groups if g not in user_map]
        if missing:
            warnings.warn(f"No color provided for group(s) {missing}; using palette '{palette_name}' for them.")
    return colors


# ----------------------------- Core Logic -----------------------------------
def species_accumulation(counts_bool, sample_ids, n_perm=256, seed=0):
    """
    counts_bool: boolean DataFrame (ASVs x S) presence/absence
    sample_ids: list of sample columns (subset/order independent)
    returns dict with 'mean','lo','hi','all','n_samples'
    """
    rng = np.random.default_rng(seed)
    col_idx = np.array([counts_bool.columns.get_loc(s) for s in sample_ids])
    B = counts_bool.values  # (n_asv, n_samples_total)

    n_samp = len(sample_ids)
    n_asv = B.shape[0]
    acc = np.zeros((n_perm, n_samp), dtype=np.int32)

    for p in range(n_perm):
        order = rng.permutation(col_idx)
        seen = np.zeros(n_asv, dtype=bool)
        for i, j in enumerate(order):
            seen |= B[:, j]
            acc[p, i] = int(seen.sum())

    mean = acc.mean(axis=0)
    lo = np.percentile(acc, 2.5, axis=0)
    hi = np.percentile(acc, 97.5, axis=0)
    return {"mean": mean, "lo": lo, "hi": hi, "all": acc, "n_samples": n_samp}


def build_group_curves(counts, meta, sample_id_col, group_col, groups_order, n_perm=256, seed=0):
    counts_bool = counts.gt(0)
    curves = {}
    for g in groups_order:
        # metadata may contain duplicates; de-duplicate while preserving counts-column order
        sids_raw = meta.loc[meta[group_col] == g, sample_id_col].tolist()
        wanted = set(sids_raw)
        sids = [c for c in counts.columns if c in wanted]
        if len(sids) == 0:
            continue
        curv = species_accumulation(counts_bool, sids, n_perm=n_perm, seed=seed)
        curv["n_samples_meta_rows"] = len(sids_raw)
        if curv["n_samples_meta_rows"] != curv["n_samples"]:
            warnings.warn(
                f"{group_col}='{g}': {curv['n_samples_meta_rows']} metadata rows -> "
                f"{curv['n_samples']} unique samples after de-dup."
            )
        curves[g] = curv
    # Filter the order to groups that actually produced curves
    realized_order = [g for g in groups_order if g in curves]
    return curves, realized_order


# ----------------------------- Plotting -------------------------------------
def _x_limits_with_padding(xmax, xpad):
    # Upper-only padding: left bound fixed at 1.0, right bound = xmax + xpad
    pad = max(xpad, 0.0)
    left = 1.0
    right = xmax + pad
    return left, right


def plot_overlay(curves, colors, group_col, out_base, xpad, groups_order, title, formats, dpi, ylim_pad):
    plt.figure(figsize=(8, 6), dpi=dpi)
    ymax = max(curves[g]["hi"].max() for g in groups_order)
    xmax = max(len(curves[g]["mean"]) for g in groups_order)
    handles, labels = [], []

    for g in groups_order:
        curv = curves[g]
        x = np.arange(1, len(curv["mean"]) + 1)
        plt.fill_between(x, curv["lo"], curv["hi"], alpha=0.2, color=colors[g])
        line, = plt.plot(x, curv["mean"], linewidth=2, color=colors[g])
        plt.axvline(curv["n_samples"], linestyle="--", linewidth=1.5, color=colors[g], alpha=0.9)
        handles.append(line); labels.append(g)

    plt.xlabel("# samples accumulated")
    plt.ylabel("# unique ASVs observed")
    if title:
        plt.title(title)
    xlo, xhi = _x_limits_with_padding(xmax, xpad)
    plt.xlim(xlo, xhi)
    plt.ylim(0, ymax * ylim_pad)
    plt.legend(handles, labels, title=group_col, 
            bbox_to_anchor=(1.05, 1), loc='upper left',
            frameon=True, fontsize=10, title_fontsize=12)
    plt.tight_layout()
    for ext in formats:
        plt.savefig(f"{out_base}_overlay.{ext}")
    plt.close()


def plot_faceted(curves, colors, group_col, out_base, xpad, groups_order, title, formats, dpi, max_cols, show_perms, ylim_pad):
    groups = list(groups_order)
    n = len(groups)
    ncols = min(max_cols, n) if n else 1
    nrows = math.ceil(n / ncols) if n else 1
    fig, axes = plt.subplots(nrows, ncols, figsize=(4*ncols, 3.5*nrows), dpi=dpi)
    if nrows * ncols == 1:
        axes = np.array([[axes]])
    axes = np.array(axes).reshape(nrows, ncols)

    xmax = max(len(curves[g]["mean"]) for g in groups)
    ymax = max(curves[g]["hi"].max() for g in groups)
    xlo, xhi = _x_limits_with_padding(xmax, xpad)

    for i, g in enumerate(groups):
        r, c = divmod(i, ncols)
        ax = axes[r, c]
        curv = curves[g]
        x = np.arange(1, len(curv["mean"]) + 1)
        sp = min(show_perms, curv["all"].shape[0])
        for k in range(sp):
            ax.plot(x, curv["all"][k, :], alpha=0.08, linewidth=1, color=colors[g])
        ax.fill_between(x, curv["lo"], curv["hi"], alpha=0.2, color=colors[g])
        ax.plot(x, curv["mean"], linewidth=2, color=colors[g])
        ax.axvline(curv["n_samples"], linestyle="--", linewidth=1.2, color=colors[g], alpha=0.95)

        ax.set_title(f"{group_col} = {g}")
        ax.set_xlim(xlo, xhi)
        ax.set_ylim(0, ymax * ylim_pad)
        ax.set_xlabel("# samples")
        ax.set_ylabel("# ASVs")
        ax.grid(alpha=0.2, linestyle="--", linewidth=0.5)

    # Turn off unused panels
    for j in range(i + 1, nrows * ncols):
        r, c = divmod(j, ncols)
        fig.delaxes(axes[r, c])

    if title:
        fig.suptitle(title, y=0.99)
    fig.tight_layout()
    fig.subplots_adjust(top=0.90 if title else 0.95)
    for ext in formats:
        fig.savefig(f"{out_base}_faceted.{ext}")
    plt.close(fig)


# ----------------------------- CLI ------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="Build collector's curves (species accumulation) from an ASV table.")
    ap.add_argument("--counts", required=True,
                    help="TSV (optionally .gz) with ASVs as rows and samples as columns.")
    ap.add_argument("--meta", required=True,
                    help="TSV (optionally .gz) metadata file.")
    ap.add_argument("--sample-col", default=SAMPLE_ID_COL,
                    help="Metadata sample ID column.")
    ap.add_argument("--group-col", default="sample_type",
                    help="Metadata column for hue/facets (e.g., sample_type).")
    ap.add_argument("--permutations", type=int, default=256,
                    help="Random permutations per group (higher = smoother bounds).")
    ap.add_argument("--seed", type=int, default=0,
                    help="Random seed.")
    ap.add_argument("--out_prefix", default="collectors",
                    help="Output prefix for figures and stats.")

    # Colors & appearance
    ap.add_argument("--color-col", default="Color",
                    help="Color column")
    ap.add_argument("--group-colors", default="",
                    help="Explicit group color mapping, e.g. GroupA=#1f77b4,GroupB=#ff7f0e. Overrides --color-col values.")
    ap.add_argument("--group-order", default="",
                    help="Comma-separated explicit group order for facets/legend.")
    ap.add_argument("--title", default="",
                    help="Optional plot title (overlay and faceted).")
    ap.add_argument("--formats", default="pdf,svg",
                    help="Comma-separated image formats to write (e.g., pdf,png,svg).")
    ap.add_argument("--dpi", type=int, default=150, help="Figure DPI for raster outputs (PNG).")
    ap.add_argument("--xpad", type=float, default=0.5,
                    help="Upper x-axis padding only (default 0.5) so max dashed vlines are visible.")
    ap.add_argument("--max-cols", type=int, default=3,
                    help="Max columns for faceted layout.")
    ap.add_argument("--show-perms", type=int, default=10,
                    help="Max individual permutation traces to overlay in each facet.")
    ap.add_argument("--ylim-pad", type=float, default=1.1,
                    help="Multiply ymax by this factor for headroom on y-axis.")

    # Filtering
    ap.add_argument("--presence-threshold", type=float, default=0.0,
                    help="Count threshold > T to consider an ASV present (default 0).")

    args = ap.parse_args()

    counts, meta = read_inputs(args.counts, args.meta, args.sample_col, args.group_col)

    # Keep group labels as strings to support values like BAL / Bronchial Brush.
    meta[args.group_col] = meta[args.group_col].astype(str).str.strip()

    # Build color mapping from metadata when available, fallback to palette.
    user_palette = {}
    if args.color_col in meta.columns:
        pal_df = meta[[args.group_col, args.color_col]].dropna(subset=[args.group_col]).drop_duplicates(
            subset=[args.group_col], keep="first"
        )
        user_palette = {
            str(row[args.group_col]).strip(): row[args.color_col]
            for _, row in pal_df.iterrows()
        }
    else:
        warnings.warn(
            f"Color column '{args.color_col}' not found in metadata; using fallback palette."
        )
    explicit_palette = parse_group_colors(args.group_colors)
    if explicit_palette:
        user_palette.update(explicit_palette)

    requested_order = parse_list_csv(args.group_order)
    fallback_order = requested_order if requested_order else (list(user_palette.keys()) if user_palette else None)
    groups_order = resolve_group_order(meta[args.group_col], fallback_order)
    colors = color_map_for_groups(groups_order, palette_name="tab10", user_map=user_palette)

    # Presence/absence threshold
    if args.presence_threshold > 0:
        counts = (counts > args.presence_threshold).astype(int)
    else:
        # keep numeric and treat >0 as present in core function
        counts = counts.apply(pd.to_numeric, errors="coerce").fillna(0)

    all_groups_order = resolve_group_order(meta[args.group_col], groups_order)
    curves, groups_order = build_group_curves(
        counts, meta,
        sample_id_col=args.sample_col,
        group_col=args.group_col,
        groups_order=all_groups_order,
        n_perm=args.permutations,
        seed=args.seed,
    )
    if not curves:
        raise RuntimeError("No groups found to plot (check --group-col values and overlaps with counts).")


    formats = [f.strip().lstrip(".").lower() for f in args.formats.split(",") if f.strip()]
    out_base = args.out_prefix

    # Figures
    plot_overlay(curves, colors, args.group_col, out_base, xpad=args.xpad,
                 groups_order=groups_order, title=args.title, formats=formats, dpi=args.dpi, ylim_pad=args.ylim_pad)
    plot_faceted(curves, colors, args.group_col, out_base, xpad=args.xpad, groups_order=groups_order,
                 title=args.title, formats=formats, dpi=args.dpi, max_cols=args.max_cols,
                 show_perms=args.show_perms, ylim_pad=args.ylim_pad)

    # Write per-group summary stats + combined summary
    out_dir = Path(f"{out_base}_stats")
    out_dir.mkdir(exist_ok=True)
    summary_rows = []
    for g in groups_order:
        curv = curves[g]
        df = pd.DataFrame({
            "n_samples": np.arange(1, len(curv["mean"]) + 1),
            "mean_asvs": curv["mean"],
            "lo_2p5": curv["lo"],
            "hi_97p5": curv["hi"],
        })
        safe_g = "".join(ch if ch.isalnum() or ch in "-._" else "_" for ch in str(g))
        df.to_csv(out_dir / f"{safe_g}_collector_stats.tsv", sep="\t", index=False)

        # one-line summary per group (final step)
        summary_rows.append({
            "group": g,
            "n_samples": int(curv["n_samples"]),
            "mean_asvs_final": float(curv["mean"][-1]),
            "lo_2p5_final": float(curv["lo"][-1]),
            "hi_97p5_final": float(curv["hi"][-1]),
        })

    pd.DataFrame(summary_rows).to_csv(f"{out_base}_summary.tsv", sep="\t", index=False)

    # Optional: console summary
    for g in groups_order:
        curv = curves[g]
        print(f"[{args.group_col}={g}] samples={curv['n_samples']} "
              f"(metadata rows={curv['n_samples_meta_rows']})")

if __name__ == "__main__":
    main()

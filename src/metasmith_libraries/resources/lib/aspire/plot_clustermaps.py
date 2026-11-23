#!/usr/bin/env python3
"""
clustermaps_cli.py
Build log-scaled abundance clustermaps (rows = taxa/ranks or ASVs, cols = samples)
with configurable column color bars (group1, group2, optional group3).

This script is intentionally data-agnostic:
- configurable sample / feature / grouping column names
- configurable palette and ordering for grouping bars
- optional ISA significance gate with auto-detection of significance/stat columns
- optional mitochondrial ASV clustermaps with configurable sample-name harmonization

Backward-compatible aliases are provided for project-specific flags:
- `--type-*` maps to `--group1-*`
- `--status-palette` maps to `--group2-palette`
- `--kit-*` maps to `--group3-*`
"""

from __future__ import annotations

import argparse
import warnings
from pathlib import Path

import matplotlib as mpl
import matplotlib.colors as mcolors
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from matplotlib.colors import LinearSegmentedColormap
from matplotlib.patches import Patch

# ----------------------- Matplotlib/Seaborn defaults ------------------------
mpl.rcParams["pdf.fonttype"] = 42      # Keep text as text in PDF
mpl.rcParams["svg.fonttype"] = "none"  # Keep text as text in SVG
mpl.rcParams["savefig.dpi"] = 600
plt.rcParams.update({"font.size": 12})
plt.rcParams["font.family"] = "Source Sans Pro"
sns.set_theme()
sns.set_style("white")


# ------------------------------- Utilities ----------------------------------
def parse_kv_csv(s: str) -> dict[str, str]:
    """Parse `A=#fff,B:#123` into a dict. Returns {} for empty input."""
    if not s:
        return {}
    out: dict[str, str] = {}
    for item in s.split(","):
        item = item.strip()
        if not item:
            continue
        if "=" in item:
            k, v = item.split("=", 1)
        elif ":" in item:
            k, v = item.split(":", 1)
        else:
            warnings.warn(f"Ignoring malformed mapping: {item!r}")
            continue
        out[k.strip()] = v.strip()
    return out


def parse_csv_list(s: str) -> list[str]:
    if not s:
        return []
    return [x.strip() for x in s.split(",") if x.strip()]


def unique_preserve(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        if not item or item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def ensure_cols(df: pd.DataFrame, required: list[str], where: str):
    missing = [c for c in required if c and c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns in {where}: {missing}")


def normalize_label(v) -> str:
    if pd.isna(v):
        return "Other"
    text = str(v).strip()
    if not text:
        return "Other"
    lowered = text.lower()
    ambiguous_tokens = (
        "unclassified",
        "uncultured",
        "unknown",
        "unidentified",
        "unassigned",
        "ambiguous",
        "incertae",
        "metagenome",
        "other",
        "none",
        "nan",
        "na",
    )
    if any(tok in lowered for tok in ambiguous_tokens):
        return "Other"
    return text


def dynamic_height(
    n_rows: int,
    row_labels: list[str] | None = None,
    per_row: float = 0.4,
    min_h: float = 8.0,
    max_h: float = 1200.0,
) -> float:
    max_row_label_len = max((len(str(x)) for x in (row_labels or [])), default=0)
    label_term = min(max_row_label_len * 0.02, 4.0)
    height = (per_row * max(n_rows, 1)) + label_term + 2.0
    return float(np.clip(height, min_h, max_h))


def dynamic_width(
    n_cols: int,
    col_labels: list[str],
    row_labels: list[str] | None = None,
    base_w: float = 32.0,
    per_col: float = 0.22,
    min_w: float = 18.0,
    max_w: float = 1200.0,
) -> float:
    max_col_label_len = max((len(str(x)) for x in col_labels), default=0)
    max_row_label_len = max((len(str(x)) for x in (row_labels or [])), default=0)
    col_label_term = min(max_col_label_len * 0.18, 10.0)
    row_label_term = min(max_row_label_len * 0.12, 16.0)
    width = max(base_w, min_w, (per_col * max(n_cols, 1)) + col_label_term + row_label_term + 6.0)
    return float(np.clip(width, min_w, max_w))


def auto_palette(values: list[str]) -> dict[str, str]:
    vals = [normalize_label(v) for v in values if normalize_label(v) not in {"", "Other"}]
    vals = list(dict.fromkeys(vals))
    if not vals:
        return {}
    colors = sns.color_palette("tab20", n_colors=max(len(vals), 3))
    return {v: mcolors.to_hex(colors[i % len(colors)]) for i, v in enumerate(vals)}


def bool_series(series: pd.Series) -> pd.Series:
    if series.dtype == bool:
        return series.fillna(False)
    if pd.api.types.is_numeric_dtype(series):
        return series.fillna(0) != 0
    s = series.astype(str).str.strip().str.lower()
    true_set = {"true", "t", "1", "yes", "y"}
    return s.isin(true_set)


def to_numeric_series(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(0.0)


def read_isa_sig_asvs(
    isa_path: Path | None,
    min_stat: float,
    asv_id_col: str,
    significance_cols: list[str] | None = None,
    stat_cols: list[str] | None = None,
) -> set[str]:
    """
    Return ASV IDs that pass:
      ANY(significance cols is true) AND ANY(stat cols >= min_stat)
    """
    if not isa_path:
        return set()
    df = pd.read_csv(isa_path, sep="\t", header=0)
    if asv_id_col not in df.columns:
        df.rename(columns={df.columns[0]: asv_id_col}, inplace=True)
    if asv_id_col not in df.columns:
        warnings.warn(f"ISA table missing ASV identifier column {asv_id_col!r}; skipping ISA gate.")
        return set()

    sig_cols = [c for c in (significance_cols or []) if c in df.columns]
    stat_cols_found = [c for c in (stat_cols or []) if c in df.columns]

    if not sig_cols:
        sig_cols = [c for c in df.columns if c.lower().endswith("_significance") or c.lower() in {"significant"}]
    if not stat_cols_found:
        stat_cols_found = [c for c in df.columns if c.lower().endswith("_stat") or c.lower() in {"stat"}]

    if not sig_cols or not stat_cols_found:
        warnings.warn("ISA table lacks usable significance/stat columns; falling back to top-N selection only.")
        return set()

    sig_any = pd.Series(False, index=df.index)
    for c in sig_cols:
        sig_any |= bool_series(df[c])

    stat_any = pd.Series(False, index=df.index)
    for c in stat_cols_found:
        stat_any |= to_numeric_series(df[c]) >= float(min_stat)

    mask = sig_any & stat_any
    return set(df.loc[mask, asv_id_col].astype(str))


def build_rank_universe(
    asv_meta: pd.DataFrame,
    ranks: list[str],
    group1_col: str,
    group1_order: list[str],
    exclude_group1: set[str],
    sig_asvs: set[str],
    topn_map: dict[str, int],
    count_col: str,
    asv_id_col: str,
) -> dict[str, list[str]]:
    """
    For each rank, compute union across group1:
      top N taxa within each group1 + taxa from ISA-significant ASVs.
    """
    keep = asv_meta.copy()
    keep[group1_col] = keep[group1_col].astype(str)
    keep = keep[~keep[group1_col].isin(exclude_group1)]

    groups = group1_order[:] if group1_order else keep[group1_col].dropna().astype(str).unique().tolist()
    universe: dict[str, list[str]] = {}

    for rank in ranks:
        if rank not in keep.columns:
            warnings.warn(f"Rank column {rank!r} not found in ASV table; skipping.")
            continue
        grp = keep.groupby([group1_col, rank], dropna=False)[count_col].sum().reset_index()
        labels: set[str] = set()
        for g in groups:
            gdf = grp[grp[group1_col].astype(str) == str(g)]
            if gdf.empty:
                continue
            topn = int(topn_map.get(rank, 30))
            top_labels = gdf.sort_values(count_col, ascending=False)[rank].head(topn).tolist()
            labels.update(normalize_label(x) for x in top_labels)

        if sig_asvs and asv_id_col in keep.columns:
            sig_rank = keep[keep[asv_id_col].astype(str).isin(sig_asvs)][rank].tolist()
            labels.update(normalize_label(x) for x in sig_rank)

        labels.discard("")
        if not labels:
            labels = {"Other"}
        universe[rank] = sorted(labels)
    return universe


def assign_plot_labels(series: pd.Series, allowed: list[str]) -> pd.Series:
    allowed_set = set(allowed)
    return series.map(lambda x: normalize_label(x) if normalize_label(x) in allowed_set else "Other")


def prepare_sample_meta(
    meta: pd.DataFrame,
    asv_meta: pd.DataFrame,
    sample_col: str,
    sample_code_col: str,
    group1_col: str,
    group2_col: str,
    group3_col: str,
) -> pd.DataFrame:
    """
    Build a deduplicated sample metadata table that includes sample_code + grouping columns.
    Missing grouping columns in metadata are filled from ASV meta when available.
    """
    ensure_cols(meta, [sample_col], "metadata")
    ensure_cols(asv_meta, [sample_col, sample_code_col, group1_col, group2_col], "ASV meta")

    sample_cols = unique_preserve([sample_col, sample_code_col, group1_col, group2_col])
    if group3_col:
        sample_cols = unique_preserve(sample_cols + [group3_col])

    base = meta.copy()
    for col in sample_cols:
        if col not in base.columns and col in asv_meta.columns:
            fill = asv_meta[[sample_col, col]].dropna().drop_duplicates(subset=[sample_col])
            base = base.merge(fill, on=sample_col, how="left")

    ensure_cols(base, [sample_col, sample_code_col, group1_col, group2_col], "merged sample metadata")
    keep_cols = [c for c in sample_cols if c in base.columns]
    out = base[keep_cols].drop_duplicates(subset=[sample_col]).copy()
    out[sample_col] = out[sample_col].astype(str)
    out[sample_code_col] = out[sample_code_col].astype(str)
    return out


def col_colors_from_meta(
    samples: list[str],
    sample_meta_by_code: pd.DataFrame,
    color_specs: list[tuple[str, dict[str, str]]],
) -> pd.DataFrame:
    """
    Build clustermap color bars from metadata.
    `color_specs` = [(column_name, palette_dict), ...]
    """
    sub = sample_meta_by_code.reindex(samples).copy()
    bars: dict[str, pd.Series] = {}
    for col, palette in color_specs:
        if col not in sub.columns:
            continue
        bars[col] = sub[col].map(
            lambda v: palette.get(str(v), palette.get(normalize_label(v), "#D3D3D3"))
        ).fillna("#D3D3D3")
    if not bars:
        return pd.DataFrame(index=samples)
    return pd.DataFrame(bars, index=samples)


def choose_mito_sample_names(raw_cols: list[str], valid_samples: set[str], mode: str) -> tuple[list[str], str]:
    raw = [str(x) for x in raw_cols]
    stripped = [c.rsplit("_", 1)[0] if "_" in c else c for c in raw]

    if mode == "none":
        return raw, "none"
    if mode == "strip_last_token":
        return stripped, "strip_last_token"

    # auto
    raw_hits = len(set(raw) & valid_samples)
    stripped_hits = len(set(stripped) & valid_samples)
    if stripped_hits > raw_hits:
        return stripped, "strip_last_token"
    return raw, "none"


def draw_clustermap(
    pivot: pd.DataFrame,
    col_colors_df: pd.DataFrame,
    color_specs: list[tuple[str, dict[str, str]]],
    outfile_prefix: Path,
    tick_vals_orig: list[int],
    vmax_display: int,
    figsize_w: float,
    row_height: float,
    min_fig_h: float,
    max_fig_h: float,
    method: str = "ward",
    metric: str = "euclidean",
    dendrogram_ratio=(0.05, 0.2),
    colors_ratio=0.02,
    cbar_pos=(1.02, 0.2, 0.03, 0.4),
    alpha: float = 0.75,
    formats: list[str] | None = None,
):
    """Write two plots: `_code` (fixed columns) and `_clustered` (clustered columns)."""
    if pivot.empty:
        warnings.warn(f"Skipping empty pivot: {outfile_prefix}")
        return

    row_cluster = pivot.shape[0] > 1
    col_cluster = pivot.shape[1] > 1
    if not row_cluster or not col_cluster:
        warnings.warn(
            f"Pivot for {outfile_prefix} is degenerate "
            f"(rows={pivot.shape[0]}, cols={pivot.shape[1]}). "
            "Disabling unsupported dendrogram(s)."
        )

    # Order colors by heatmap columns
    if not col_colors_df.empty:
        col_colors_df = col_colors_df.loc[pivot.columns]

    pivot_log = np.log10(pivot + 1.0)
    cmap = LinearSegmentedColormap.from_list("light_greyscale", ["#ffffff", "#d9d9d9", "#000000"], N=256)
    tick_vals_log = [np.log10(v + 1) for v in tick_vals_orig]
    vmax_log = np.log10(vmax_display + 1)
    height = dynamic_height(
        pivot.shape[0],
        row_labels=pivot.index.tolist(),
        per_row=row_height,
        min_h=min_fig_h,
        max_h=max_fig_h,
    )
    width = dynamic_width(
        pivot.shape[1],
        col_labels=pivot.columns.tolist(),
        row_labels=pivot.index.tolist(),
        base_w=figsize_w,
    )

    def add_annotation_legends(grid) -> None:
        """Add legends for metadata color strips without covering the heatmap."""
        y_anchor = 1.0
        observed_colors = {
            col: set(col_colors_df[col].dropna().astype(str))
            for col in col_colors_df.columns
        }
        for col, palette in color_specs:
            if col not in observed_colors:
                continue
            handles = [
                Patch(facecolor=color, edgecolor="none", label=str(label))
                for label, color in palette.items()
                if str(color) in observed_colors[col]
            ]
            if not handles:
                continue
            legend = grid.fig.legend(
                handles=handles,
                title=col,
                loc="upper left",
                bbox_to_anchor=(1.01, y_anchor),
                frameon=False,
                borderaxespad=0,
            )
            grid.fig.add_artist(legend)
            y_anchor -= min(0.08 + 0.045 * len(handles), 0.42)

    # 1) fixed sample-code order
    g = sns.clustermap(
        pivot_log,
        method=method,
        metric=metric,
        col_colors=col_colors_df if not col_colors_df.empty else None,
        cmap=cmap,
        vmin=0,
        vmax=vmax_log,
        linewidths=0.5,
        xticklabels=True,
        yticklabels=True,
        dendrogram_ratio=dendrogram_ratio,
        colors_ratio=colors_ratio,
        figsize=(width, height),
        cbar_pos=cbar_pos,
        alpha=alpha,
        row_cluster=row_cluster,
        col_cluster=False,
    )
    cbar = g.ax_heatmap.collections[0].colorbar
    cbar.set_ticks(tick_vals_log)
    cbar.set_ticklabels([f"{v:,}" for v in tick_vals_orig])
    cbar.set_label("ASV Count", rotation=270, labelpad=15)
    g.ax_heatmap.set_xticks(g.ax_heatmap.get_xticks())
    g.ax_heatmap.set_xticklabels(pivot_log.columns, rotation=90, ha="center")
    g.ax_heatmap.tick_params(axis="x", bottom=True, labelbottom=True, length=5)
    add_annotation_legends(g)

    formats = formats or ["pdf", "png"]
    for fmt in formats:
        plt.savefig(outfile_prefix.with_suffix(f".{fmt}"), bbox_inches="tight")
    plt.close()

    # 2) clustered columns
    g = sns.clustermap(
        pivot_log,
        method=method,
        metric=metric,
        col_colors=col_colors_df if not col_colors_df.empty else None,
        cmap=cmap,
        vmin=0,
        vmax=vmax_log,
        linewidths=0.5,
        xticklabels=True,
        yticklabels=True,
        dendrogram_ratio=dendrogram_ratio,
        colors_ratio=colors_ratio,
        figsize=(width, height),
        cbar_pos=cbar_pos,
        alpha=alpha,
        row_cluster=row_cluster,
        col_cluster=col_cluster,
    )
    cbar = g.ax_heatmap.collections[0].colorbar
    cbar.set_ticks(tick_vals_log)
    cbar.set_ticklabels([f"{v:,}" for v in tick_vals_orig])
    cbar.set_label("ASV Count", rotation=270, labelpad=15)
    g.ax_heatmap.tick_params(axis="x", bottom=True, labelbottom=True, length=5)
    add_annotation_legends(g)

    clustered_prefix = outfile_prefix.with_name(outfile_prefix.stem.replace("_code", "_clustered"))
    for fmt in formats:
        plt.savefig(clustered_prefix.with_suffix(f".{fmt}"), bbox_inches="tight")
    plt.close()


# ------------------------------- Main ---------------------------------------
def main():
    ap = argparse.ArgumentParser(
        description="Generalized clustermap pipeline (ranks/ASV + optional mitochondrial mode)."
    )

    # Core inputs
    ap.add_argument("--asv-meta", type=Path, required=True, help="ASV metadata/count table (TSV).")
    ap.add_argument("--metadata", type=Path, required=True, help="Sample metadata table (TSV).")
    ap.add_argument("--isa", type=Path, default=None, help="Optional ISA results table (TSV).")
    ap.add_argument("--outdir", type=Path, required=True, help="Output directory.")

    # Column mapping
    ap.add_argument("--asv-id-col", type=str, default="ASV_ID", help="ASV identifier column.")
    ap.add_argument("--sample-col", type=str, default="sample", help="Sample ID column.")
    ap.add_argument("--sample-code-col", type=str, default="sample_code", help="Sample code column used for heatmap x-axis.")
    ap.add_argument("--group1-col", type=str, default="type_group", help="Primary grouping column for top-N selection + bar 1.")
    ap.add_argument("--group2-col", type=str, default="status", help="Secondary grouping column for bar 2.")
    ap.add_argument("--group3-col", type=str, default="kit", help="Optional third grouping column for bar 3 (empty to disable).")

    # Group ordering/exclusion + palettes (with backward-compatible aliases)
    ap.add_argument("--group1-order", "--type-order", dest="group1_order", type=str, default="",
                    help="Comma-separated order for group1 categories.")
    ap.add_argument("--exclude-group1", "--exclude-types", dest="exclude_group1", type=str, default="",
                    help="Comma-separated group1 categories to exclude.")
    ap.add_argument("--group1-palette", "--type-palette", dest="group1_palette", type=str, default="",
                    help='Palette mapping for group1, e.g. "A=#111,B=#222". Empty => auto.')
    ap.add_argument("--group2-palette", "--status-palette", dest="group2_palette", type=str, default="",
                    help='Palette mapping for group2, e.g. "X=#111,Y=#222". Empty => auto.')
    ap.add_argument("--group3-palette", "--kit-palette", dest="group3_palette", type=str, default="",
                    help='Palette mapping for group3, e.g. "K1=#111,K2=#222". Empty => auto if group3 exists.')

    # Rank selection
    ap.add_argument("--ranks", type=str, default="Phylum,Class,Order,Family,Genus,Species,ASV_ID",
                    help="Comma-separated rank columns to plot.")
    ap.add_argument("--topN", type=str, default="Phylum=30,Class=30,Order=30,Family=30,Genus=30,Species=30,ASV_ID=6000",
                    help="Per-rank top-N mapping, e.g. 'Phylum=30,...,ASV_ID=6000'.")
    ap.add_argument("--count-col", type=str, default="corr_count", help="Abundance/count column in ASV meta.")

    # ISA gate
    ap.add_argument("--isa-min-stat", type=float, default=0.6, help="Minimum ISA stat threshold.")
    ap.add_argument("--isa-significance-cols", type=str, default="",
                    help="Comma-separated ISA significance columns. Empty => auto-detect.")
    ap.add_argument("--isa-stat-cols", type=str, default="",
                    help="Comma-separated ISA stat columns. Empty => auto-detect.")

    # Heatmap appearance
    ap.add_argument("--tick-values", type=str, default="5,50,500,5000,50000",
                    help="Colorbar tick values in original count scale.")
    ap.add_argument("--vmax", type=int, default=50000, help="Colorbar vmax in original count scale.")
    ap.add_argument("--figwidth", type=float, default=32.0, help="Figure width (in).")
    ap.add_argument("--row-height", type=float, default=0.4, help="Row height scale (in/row).")
    ap.add_argument("--min-height", type=float, default=8.0, help="Minimum figure height (in).")
    ap.add_argument("--max-height", type=float, default=6000.0, help="Maximum figure height (in).")
    ap.add_argument("--formats", type=str, default="pdf,png,svg",
                    help="Comma-separated output formats, e.g. 'pdf,png' or 'pdf,svg'.")

    # Mitochondrial (optional)
    ap.add_argument("--mito-asv", type=Path, default=None, help="Optional mitochondrial ASV table (TSV).")
    ap.add_argument("--mito-outdir", type=Path, default=None, help="Output directory for mito clustermaps.")
    ap.add_argument("--mito-count-col", type=str, default="count", help="Mito abundance field after stacking.")
    ap.add_argument("--mito-sample-mode", choices=["auto", "none", "strip_last_token"], default="auto",
                    help="How to harmonize mito sample names to metadata sample IDs.")

    args = ap.parse_args()

    outdir = args.outdir
    outdir.mkdir(parents=True, exist_ok=True)

    ranks = parse_csv_list(args.ranks)
    group1_order = parse_csv_list(args.group1_order)
    exclude_group1 = set(parse_csv_list(args.exclude_group1))
    topn_raw = parse_kv_csv(args.topN)
    topn_map = {k: int(v) for k, v in topn_raw.items()}
    tick_vals_orig = [int(x.strip()) for x in args.tick_values.split(",") if x.strip()]
    vmax_display = int(args.vmax)
    formats = [x.strip().lower() for x in args.formats.split(",") if x.strip()]
    formats = [x for x in formats if x in {"pdf", "png", "svg"}]
    if not formats:
        formats = ["pdf", "png"]

    meta = pd.read_csv(args.metadata, sep="\t", header=0)
    asv_meta = pd.read_csv(args.asv_meta, sep="\t", header=0)
    ensure_cols(asv_meta, [args.asv_id_col, args.sample_col, args.sample_code_col, args.group1_col, args.group2_col, args.count_col], "ASV meta table")
    ensure_cols(meta, [args.sample_col], "metadata table")

    sample_meta = prepare_sample_meta(
        meta=meta,
        asv_meta=asv_meta,
        sample_col=args.sample_col,
        sample_code_col=args.sample_code_col,
        group1_col=args.group1_col,
        group2_col=args.group2_col,
        group3_col=args.group3_col,
    )

    # Palettes (auto if omitted)
    if not group1_order:
        group1_order = asv_meta[args.group1_col].dropna().astype(str).unique().tolist()

    group1_palette = parse_kv_csv(args.group1_palette)
    if not group1_palette:
        group1_palette = auto_palette(group1_order)

    group2_palette = parse_kv_csv(args.group2_palette)
    if not group2_palette:
        g2_vals = sample_meta[args.group2_col].dropna().astype(str).unique().tolist()
        group2_palette = auto_palette(g2_vals)

    group3_col = args.group3_col.strip()
    group3_palette = parse_kv_csv(args.group3_palette) if group3_col else {}
    if group3_col and not group3_palette and group3_col in sample_meta.columns:
        g3_vals = sample_meta[group3_col].dropna().astype(str).unique().tolist()
        group3_palette = auto_palette(g3_vals)

    sig_cols = parse_csv_list(args.isa_significance_cols) or None
    stat_cols = parse_csv_list(args.isa_stat_cols) or None
    sig_asvs = read_isa_sig_asvs(
        isa_path=args.isa,
        min_stat=args.isa_min_stat,
        asv_id_col=args.asv_id_col,
        significance_cols=sig_cols,
        stat_cols=stat_cols,
    ) if args.isa else set()

    rank_universe = build_rank_universe(
        asv_meta=asv_meta,
        ranks=ranks,
        group1_col=args.group1_col,
        group1_order=group1_order,
        exclude_group1=exclude_group1,
        sig_asvs=sig_asvs,
        topn_map=topn_map,
        count_col=args.count_col,
        asv_id_col=args.asv_id_col,
    )

    working = asv_meta.copy()
    working[args.group1_col] = working[args.group1_col].astype(str)
    working = working[~working[args.group1_col].isin(exclude_group1)]

    sample_meta_by_code = sample_meta.set_index(args.sample_code_col)
    color_specs: list[tuple[str, dict[str, str]]] = [
        (args.group1_col, group1_palette),
        (args.group2_col, group2_palette),
    ]
    if group3_col and group3_col in sample_meta_by_code.columns and group3_palette:
        color_specs.append((group3_col, group3_palette))

    for rank in ranks:
        if rank not in working.columns:
            warnings.warn(f"Skipping rank {rank!r}; column not present.")
            continue
        allowed = rank_universe.get(rank, [])
        if not allowed:
            warnings.warn(f"Skipping rank {rank!r}; no labels selected.")
            continue

        colname = f"{rank}_plot"
        working[colname] = assign_plot_labels(working[rank], allowed)

        pivot = (
            working.groupby([args.sample_code_col, colname])[args.count_col]
            .sum()
            .reset_index()
            .pivot(index=colname, columns=args.sample_code_col, values=args.count_col)
            .fillna(0)
        )
        if pivot.empty:
            warnings.warn(f"Skipping rank {rank!r}; pivot is empty after filtering.")
            continue

        col_colors_df = col_colors_from_meta(
            samples=pivot.columns.tolist(),
            sample_meta_by_code=sample_meta_by_code,
            color_specs=color_specs,
        )

        prefix = outdir / f"clustermap_{colname}_code"
        draw_clustermap(
            pivot=pivot,
            col_colors_df=col_colors_df,
            color_specs=color_specs,
            outfile_prefix=prefix,
            tick_vals_orig=tick_vals_orig,
            vmax_display=vmax_display,
            figsize_w=args.figwidth,
            row_height=args.row_height,
            min_fig_h=args.min_height,
            max_fig_h=args.max_height,
            formats=formats,
        )
        pivot.to_csv(outdir / f"clustermap_{colname}.tsv", sep="\t")

    # ------------------------- Mitochondrial (optional) -------------------------
    if args.mito_asv:
        mito_outdir = args.mito_outdir or (outdir / "mito")
        mito_outdir.mkdir(parents=True, exist_ok=True)

        mito_df = pd.read_csv(args.mito_asv, sep="\t", header=0, index_col=0)
        valid_samples = set(sample_meta[args.sample_col].astype(str))
        mito_cols, mode_used = choose_mito_sample_names(mito_df.columns.tolist(), valid_samples, args.mito_sample_mode)
        if mode_used != "none":
            print(f"[i] Mito sample harmonization mode: {mode_used}")
        mito_df.columns = mito_cols

        mito_stack = mito_df.stack().reset_index()
        mito_stack.columns = [args.asv_id_col, args.sample_col, args.mito_count_col]
        mito_stack = mito_stack[mito_stack[args.mito_count_col] > 0]
        mito_stack[args.sample_col] = mito_stack[args.sample_col].astype(str)

        mito_join_cols = unique_preserve(
            [args.sample_col, args.sample_code_col, args.group1_col, args.group2_col]
            + ([group3_col] if group3_col and group3_col in sample_meta.columns else [])
        )
        mito_meta = pd.merge(
            mito_stack,
            sample_meta[mito_join_cols],
            on=args.sample_col,
            how="left",
        )
        mito_meta[args.group1_col] = mito_meta[args.group1_col].astype(str)
        mito_meta = mito_meta[~mito_meta[args.group1_col].isin(exclude_group1)]

        pivot = (
            mito_meta.groupby([args.sample_code_col, args.asv_id_col])[args.mito_count_col]
            .sum()
            .reset_index()
            .pivot(index=args.asv_id_col, columns=args.sample_code_col, values=args.mito_count_col)
            .fillna(0)
        )
        if not pivot.empty:
            col_colors_df = col_colors_from_meta(
                samples=pivot.columns.tolist(),
                sample_meta_by_code=sample_meta_by_code,
                color_specs=color_specs,
            )
            prefix = mito_outdir / "clustermap_ASV_code_mito"
            draw_clustermap(
                pivot=pivot,
                col_colors_df=col_colors_df,
                color_specs=color_specs,
                outfile_prefix=prefix,
                tick_vals_orig=tick_vals_orig,
                vmax_display=vmax_display,
                figsize_w=args.figwidth,
                row_height=args.row_height,
                min_fig_h=args.min_height,
                max_fig_h=args.max_height,
                formats=formats,
            )
            pivot.to_csv(mito_outdir / "clustermap_ASV_mito.tsv", sep="\t")
        else:
            warnings.warn("Mito pivot is empty after join/filter; skipping mito clustermap.")

    print(f"Done. Outputs in: {outdir}")


if __name__ == "__main__":
    main()

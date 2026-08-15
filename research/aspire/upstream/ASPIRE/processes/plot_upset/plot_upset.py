#!/usr/bin/env python3
"""
asv_overlap.py
-------------
Unified CLI to generate UpSet & Venn-style set intersections for ASV data.
Supports:
- Microbial and/or mitochondrial runs
- Unique-membership UpSet (counts of ASVs)
- Abundance-weighted UpSet with stacked bars by category
- Optional Venn plots (can be disabled with --skip-venn)
- Presence tables (exclusive membership lists) and exclusive-sum tables
- Fully generalized grouping via metadata column
- Color mapping from metadata Color column

Examples
--------
# Basic run with depth groups
python asv_overlap.py \
  --data-dir /home/ryan/SeqData \
  --domain micro \
  --group-col Depth \
  --color-col Color \
  --formats svg,pdf

# Skip venn plots for many groups
python asv_overlap.py \
  --data-dir /data/run1 \
  --domain both \
  --group-col SampleType \
  --color-col Color \
  --skip-venn \
  --formats png

# Run with subset of groups
python asv_overlap.py \
  --data-dir /data/run1 \
  --domain micro \
  --group-col Depth \
  --color-col Color \
  --subset-groups "10,40,60" \
  --formats pdf
"""
from __future__ import annotations
import argparse
import os
from pathlib import Path
from itertools import combinations
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple
import numpy as np
import pandas as pd
import matplotlib as mpl
mpl.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from matplotlib.patches import Patch
import seaborn as sns
from upsetplot import from_contents, UpSet
import upsetplot.plotting as upsetplot_plotting

# upsetplot<=0.9.0 uses pandas chained inplace fillna in UpSet.plot_matrix.
# Under pandas>=3 this is a no-op and leaves NaN edgecolors, which then crashes
# matplotlib with "Invalid RGBA argument: nan". Monkeypatch with assignment-based
# fillna to be pandas-3 safe.
def _patch_upsetplot_plot_matrix() -> None:
    if getattr(upsetplot_plotting.UpSet.plot_matrix, "__name__", "") == "_plot_matrix_pandas_safe":
        return

    def _plot_matrix_pandas_safe(self, ax):
        ax = self._reorient(ax)
        data = self.intersections
        n_cats = data.index.nlevels

        inclusion = data.index.to_frame().values

        styles = [
            [
                self.subset_styles[i]
                if inclusion[i, j]
                else {"facecolor": self._other_dots_color, "linewidth": 0}
                for j in range(n_cats)
            ]
            for i in range(len(data))
        ]
        styles = sum(styles, [])
        style_columns = {
            "facecolor": "facecolors",
            "edgecolor": "edgecolors",
            "linewidth": "linewidths",
            "linestyle": "linestyles",
            "hatch": "hatch",
        }
        styles = (
            pd.DataFrame(styles)
            .reindex(columns=style_columns.keys())
            .astype(
                {
                    "facecolor": "O",
                    "edgecolor": "O",
                    "linewidth": float,
                    "linestyle": "O",
                    "hatch": "O",
                }
            )
        )
        styles["linewidth"] = styles["linewidth"].fillna(1)
        styles["facecolor"] = styles["facecolor"].fillna(self._facecolor)
        styles["edgecolor"] = styles["edgecolor"].fillna(styles["facecolor"])
        styles["linestyle"] = styles["linestyle"].fillna("solid")
        del styles["hatch"]

        x = np.repeat(np.arange(len(data)), n_cats)
        y = np.tile(np.arange(n_cats), len(data))

        if self._element_size is not None:
            s = (self._element_size * 0.35) ** 2
        else:
            s = 200

        ax.scatter(
            *self._swapaxes(x, y),
            s=s,
            zorder=10,
            **styles.rename(columns=style_columns),
        )

        if self._with_lines:
            idx = np.flatnonzero(inclusion)
            line_data = (
                pd.Series(y[idx], index=x[idx])
                .groupby(level=0)
                .aggregate(["min", "max"])
            )
            colors = pd.Series(
                [
                    style.get("edgecolor", style.get("facecolor", self._facecolor))
                    for style in self.subset_styles
                ],
                name="color",
            )
            line_data = line_data.join(colors)
            ax.vlines(
                line_data.index.values,
                line_data["min"],
                line_data["max"],
                lw=2,
                colors=line_data["color"],
                zorder=5,
            )

        tick_axis = ax.yaxis
        tick_axis.set_ticks(np.arange(n_cats))
        tick_axis.set_ticklabels(
            data.index.names, rotation=0 if self._horizontal else -90
        )
        ax.xaxis.set_visible(False)
        ax.tick_params(axis="both", which="both", length=0)
        if not self._horizontal:
            ax.yaxis.set_ticks_position("top")
        ax.set_frame_on(False)
        ax.set_xlim(-0.5, x[-1] + 0.5, auto=False)
        ax.grid(False)

    upsetplot_plotting.UpSet.plot_matrix = _plot_matrix_pandas_safe


_patch_upsetplot_plot_matrix()


# upsetplot<=0.9.0 uses np.diff(...) directly for text offsets in _label_sizes,
# yielding shape-(1,) arrays (not scalars). Newer matplotlib rejects this in
# text coordinates ("only 0-dimensional arrays can be converted to Python scalars").
def _patch_upsetplot_label_sizes() -> None:
    if getattr(upsetplot_plotting.UpSet._label_sizes, "__name__", "") == "_label_sizes_scalar_margin":
        return

    def _label_sizes_scalar_margin(self, ax, rects, where):
        if not self._show_counts and not self._show_percentages:
            return
        if self._show_counts is True:
            count_fmt = "{:.0f}"
        else:
            count_fmt = self._show_counts
            if "{" not in count_fmt:
                count_fmt = upsetplot_plotting.util.to_new_pos_format(count_fmt)

        pct_fmt = "{:.1%}" if self._show_percentages is True else self._show_percentages

        if count_fmt and pct_fmt:
            if where == "top":
                fmt = f"{count_fmt}\n({pct_fmt})"
            else:
                fmt = f"{count_fmt} ({pct_fmt})"

            def make_args(val):
                return val, val / self.total
        elif count_fmt:
            fmt = count_fmt

            def make_args(val):
                return (val,)
        else:
            fmt = pct_fmt

            def make_args(val):
                return (val / self.total,)

        if where == "right":
            margin = float(0.01 * abs(np.diff(ax.get_xlim())).item())
            for rect in rects:
                width = float(rect.get_width() + rect.get_x())
                ax.text(
                    width + margin,
                    float(rect.get_y() + rect.get_height() * 0.5),
                    fmt.format(*make_args(width)),
                    ha="left",
                    va="center",
                )
        elif where == "left":
            margin = float(0.01 * abs(np.diff(ax.get_xlim())).item())
            for rect in rects:
                width = float(rect.get_width() + rect.get_x())
                ax.text(
                    width + margin,
                    float(rect.get_y() + rect.get_height() * 0.5),
                    fmt.format(*make_args(width)),
                    ha="right",
                    va="center",
                )
        elif where == "top":
            margin = float(0.01 * abs(np.diff(ax.get_ylim())).item())
            for rect in rects:
                height = float(rect.get_height() + rect.get_y())
                ax.text(
                    float(rect.get_x() + rect.get_width() * 0.5),
                    height + margin,
                    fmt.format(*make_args(height)),
                    ha="center",
                    va="bottom",
                )
        else:
            raise NotImplementedError("unhandled where: %r" % where)

    upsetplot_plotting.UpSet._label_sizes = _label_sizes_scalar_margin


_patch_upsetplot_label_sizes()

# Optional venn backends
_HAVE_MPL_VENN = False
_HAVE_VENN = False
try:
    from matplotlib_venn import venn2, venn3  # 2–3 sets
    _HAVE_MPL_VENN = True
except Exception:
    pass
try:
    from venn import venn
    _HAVE_VENN = True
except Exception:
    pass

# ---------- Matplotlib / seaborn defaults ----------
mpl.rcParams['pdf.fonttype'] = 42
mpl.rcParams['svg.fonttype'] = 'none'
mpl.rcParams['savefig.dpi'] = 600
plt.rcParams.update({'font.size': 12, 'font.family': 'Source Sans Pro'})
sns.set_theme()
sns.set_style("white")

# ---------- Utility ----------
def split_taxa_string(taxa_str: str, delimiter: str = ';') -> Dict[str, Optional[str]]:
    levels = ["Domain", "Phylum", "Class", "Order", "Family", "Genus", "Species"]
    if pd.isna(taxa_str) or taxa_str == 'Unassigned':
        parts = ['Unassigned']
    else:
        parts = [part.strip().split('__', 1)[1] for part in str(taxa_str).split(delimiter)]
    out = {}
    for i, lvl in enumerate(levels):
        out[lvl] = parts[i] if i < len(parts) else None
    return out

def ensure_dir(path: str | Path) -> None:
    Path(path).mkdir(parents=True, exist_ok=True)

def to_bool_series(members: Mapping[str, set], elements: Iterable[str]) -> pd.DataFrame:
    """
    Build an indicator matrix (rows=element, columns=groups) telling membership.
    """
    elements = list(elements)
    cols = list(members.keys())
    data = np.zeros((len(elements), len(cols)), dtype=bool)
    for j, g in enumerate(cols):
        s = members[g]
        data[:, j] = [e in s for e in elements]
    df = pd.DataFrame(data, index=elements, columns=cols)
    return df

def exclusive_sets(name_to_set: Mapping[str, set]) -> Dict[Tuple[str, ...], List[str]]:
    """
    For each non-empty combination of groups, compute exactly-there elements:
    intersection(included) minus union(excluded).
    Returns dict with tuple of names -> sorted list of element IDs.
    """
    groups = list(name_to_set.keys())
    out: Dict[Tuple[str, ...], List[str]] = {}
    for r in range(1, len(groups) + 1):
        for combo in combinations(groups, r):
            inc = set.intersection(*(name_to_set[n] for n in combo))
            exc_names = [n for n in groups if n not in combo]
            if exc_names:
                exc = set.union(*(name_to_set[n] for n in exc_names))
                members = inc - exc
            else:
                members = inc
            if members:
                out[tuple(combo)] = sorted(members)
    return out

def sort_groups(groups: Sequence[str]) -> List[str]:
    """Sort groups - numerically if possible, otherwise alphabetically."""
    try:
        # Try to sort as integers
        return sorted(groups, key=lambda x: int(x))
    except (ValueError, TypeError):
        try:
            # Try to sort as floats
            return sorted(groups, key=lambda x: float(x))
        except (ValueError, TypeError):
            # Fall back to string sorting
            return sorted(groups, key=str)


def sanitize_palette(
    palette: Mapping[str, str],
    groups: Sequence[str],
) -> Dict[str, str]:
    """Ensure every requested group has a valid matplotlib color."""
    if not groups:
        return {}
    fallback = sns.color_palette("husl", n_colors=len(groups)).as_hex()
    out: Dict[str, str] = {}
    for i, g in enumerate(groups):
        raw = palette.get(g)
        color = str(raw).strip() if raw is not None else ""
        if not color or color.lower() == "nan" or not mcolors.is_color_like(color):
            color = fallback[i]
        out[g] = color
    return out

# ---------- IO layer ----------
class Inputs:
    def __init__(
        self,
        data_dir: Path,
        subdir: str,
        domain: str,  # "micro" or "mito"
        taxonomy_path: Optional[Path] = None,
        metadata_path: Optional[Path] = None,
        asv_raw_path: Optional[Path] = None,
        asv_final_path: Optional[Path] = None,
    ):
        self.data_dir = Path(data_dir)
        self.subdir = subdir
        self.domain = domain
        if domain == "micro":
            self.asv_raw = asv_raw_path or (self.data_dir / subdir / "ASVs" / "ASV_target.micro.tsv")
            self.asv_final = asv_final_path or (self.data_dir / subdir / "ASVs" / "ASV_final.micro.tsv")
            self.meta = metadata_path or (self.data_dir / subdir / "metadata" / "metadata_updated_micro.tsv")
            # taxonomy shared across domains by default
            self.tax = taxonomy_path or (self.data_dir / subdir / "taxonomy" / "ASV_SILVA_tax.full-length.vsearch.tsv")
            self.out_base = self.data_dir / subdir / "metadata"
        elif domain == "mito":
            self.asv_raw = asv_raw_path or (self.data_dir / subdir / "mito" / "ASVs" / "ASV_target.mito.tsv")
            self.asv_final = asv_final_path or (self.data_dir / subdir / "mito" / "ASVs" / "ASV_final.mito.tsv")
            self.meta = metadata_path or (self.data_dir / subdir / "mito" / "metadata" / "metadata_updated_mito.tsv")
            self.tax = taxonomy_path or (self.data_dir / subdir / "taxonomy" / "ASV_SILVA_tax.full-length.vsearch.tsv")
            self.out_base = self.data_dir / subdir / "mito" / "metadata"
        else:
            raise ValueError("domain must be 'micro' or 'mito'")

    def __repr__(self) -> str:
        return f"Inputs(domain={self.domain}, raw={self.asv_raw}, final={self.asv_final}, meta={self.meta}, tax={self.tax})"

def read_asv_table(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, sep="\t", index_col=0)
    # Normalize sample names (drop paths and read pair endings)
    return df

def read_metadata(path: Path, group_col: str, color_col: str) -> pd.DataFrame:
    md = pd.read_csv(path, sep="\t", dtype=str)
    # Validate required columns
    if group_col not in md.columns:
        raise ValueError(f"Metadata missing required column: '{group_col}'. Available: {list(md.columns)}")
    if color_col and color_col not in md.columns:
        print(f"[WARN] Metadata color column '{color_col}' not found; using explicit/fallback palette.")
    return md

def parse_group_palette(arg: str | None) -> Dict[str, str]:
    palette: Dict[str, str] = {}
    if not arg:
        return palette
    for part in str(arg).split(","):
        part = part.strip()
        if not part:
            continue
        if "=" in part:
            key, val = part.split("=", 1)
        elif ":" in part:
            key, val = part.split(":", 1)
        else:
            print(f"[WARN] Ignoring malformed palette entry: {part}")
            continue
        key = key.strip()
        val = val.strip()
        if key and val:
            palette[key] = val
    return palette

def read_taxonomy(path: Path) -> pd.DataFrame:
    tx = pd.read_csv(path, sep="\t")
    if "ASV_ID" in tx.columns:
        tx["ASV_ID"] = tx["ASV_ID"].astype(str).str.split(';', 1).str[0]
        tx = tx.set_index("ASV_ID")
    elif "Feature ID" in tx.columns:
        tx['Feature ID'] = tx['Feature ID'].astype(str).str.partition(';')[0]
        tx = tx.set_index("Feature ID")
    else:
        raise ValueError("taxonomy file must have ASV_ID or Feature ID column")
    return tx

def attach_taxonomy(long_df: pd.DataFrame, tax_df: pd.DataFrame) -> pd.DataFrame:
    merged = long_df.merge(tax_df, how="left", left_on="ASV_ID", right_index=True)
    # expand taxon string if present
    if "Taxon" in merged.columns:
        pieces = [split_taxa_string(x) for x in merged["Taxon"]]
        tax_exp = pd.DataFrame(pieces, index=merged.index)
        merged = pd.concat([merged, tax_exp], axis=1)
    return merged

# ---------- UpSet builders ----------
def build_upset_unique(group_sets: Mapping[str, set]) -> pd.Series:
    """Series with MultiIndex booleans -> counts (unique elements)."""
    return from_contents(group_sets)  # direct counts

def build_upset_weighted_rows(
    group_sets: Mapping[str, set],
    per_group_values: Mapping[Tuple[str, str], float],
    group_order: Sequence[str]
) -> pd.DataFrame:
    """
    Build a DataFrame suitable for UpSet(..., sum_over='count') and
    .add_stacked_bars(by='group', sum_over='count').
    Rows are ASV×group (only where ASV in that group's set).
    Columns are membership indicators for ALL groups (booleans),
    and two extra columns: 'group' and 'count'.
    """
    # union of all ASVs
    all_asvs = sorted(set().union(*group_sets.values())) if group_sets else []
    indicators = to_bool_series(group_sets, all_asvs)  # rows=ASV, cols=groups
    rows = []
    for g in group_order:
        members = group_sets.get(g, set())
        for asv in members:
            row = indicators.loc[asv].to_dict()
            row['ASV_ID'] = asv
            row['group'] = g
            row['count'] = float(per_group_values.get((g, asv), 0.0))
            rows.append(row)
    df = pd.DataFrame(rows)
    # Ensure all indicator columns present in the chosen order
    df = df[[*group_order, 'ASV_ID', 'group', 'count']]
    return df

# ---------- Plotters ----------
def savefig_multi(fig: plt.Figure, out_path_base: Path, name: str, formats: Sequence[str]) -> None:
    fig.canvas.draw()
    for ext in formats:
        fig.savefig(
            out_path_base.with_name(f"{out_path_base.stem}_{name}.{ext}"),
            bbox_inches="tight",
            pad_inches=0.8,
        )
    plt.close(fig)


def _tune_upset_layout(fig: plt.Figure, labels: Sequence[str]) -> None:
    """
    Reserve enough margin for long category labels (e.g., 'Bronchial Brush')
    so they are not clipped in saved output.
    """
    for ax in fig.axes:
        for txt in ax.get_xticklabels() + ax.get_yticklabels():
            txt.set_clip_on(False)
        for txt in ax.texts:
            txt.set_clip_on(False)

def plot_upset_unique(
    group_sets: Mapping[str, set],
    colors: Mapping[str, str],
    title: str,
    out_base: Path,
    formats: Sequence[str],
    group_col: str,
    font_size: float,
) -> None:
    rev_grp_sets = {g: s for g, s in reversed(list(group_sets.items()))}
    data = build_upset_unique(rev_grp_sets)
    upset = UpSet(
        data,
        subset_size='count',
        element_size=None,
        show_counts=True,
        sort_categories_by='input',
        min_subset_size=0
    )
    for g, c in colors.items():
        upset.style_categories([g], bar_facecolor=c, bar_edgecolor="black")
    max_len = max((len(str(g)) for g in group_sets.keys()), default=0)
    fig_w = max(14, 12 + 0.28 * max_len)
    fig = plt.figure(figsize=(fig_w, 9))
    with mpl.rc_context({"font.size": float(font_size)}):
        upset.plot(fig=fig)
    _tune_upset_layout(fig, list(group_sets.keys()))
    fig.suptitle(title, y=0.98, fontsize=float(font_size) + 1)
    # legend
    handles = [Patch(facecolor=colors[g], edgecolor="black", label=g) for g in group_sets.keys()]
    fig.legend(
        handles=handles,
        title=group_col,
        bbox_to_anchor=(1.02, 1),
        loc='upper left',
        borderaxespad=0,
        fontsize=float(font_size),
        title_fontsize=float(font_size),
    )
    savefig_multi(fig, out_base, "upset", formats)

def plot_upset_weighted(
    group_sets: Mapping[str, set],
    per_group_values: Mapping[Tuple[str, str], float],
    group_order: Sequence[str],
    colors: Mapping[str, str],
    title: str,
    out_base: Path,
    formats: Sequence[str],
    group_col: str,
    font_size: float,
) -> None:
    rev_grp_sets = {g: s for g, s in reversed(list(group_sets.items()))}
    df = build_upset_weighted_rows(rev_grp_sets, per_group_values, group_order)
    contents = {c: set(df.loc[df[c].astype(bool), 'ASV_ID']) for c in group_order[::-1]}
    weights = df.groupby(['ASV_ID', 'group'])['count'].sum().reset_index().set_index('ASV_ID')
    # Force custom stacking order here
    weights['group'] = pd.Categorical(weights['group'],
                                      categories=list(group_order),
                                      ordered=True)
    ser = from_contents(contents, data=weights)
    upset = UpSet(
        ser, sum_over='count', subset_size='sum',
        element_size=None, show_counts=True,
        sort_categories_by='input', min_subset_size=0,
        intersection_plot_elements=0
    )
    for g, c in colors.items():
        upset.style_categories([g], bar_facecolor=c, bar_edgecolor="black")
    upset.add_stacked_bars(by="group", sum_over="count", colors=colors, title=f"Abundance by {group_col}", elements=10)
    max_len = max((len(str(g)) for g in group_order), default=0)
    fig_w = max(14, 12 + 0.28 * max_len)
    fig = plt.figure(figsize=(fig_w, 9))
    with mpl.rc_context({"font.size": float(font_size)}):
        axes = upset.plot(fig=fig)
    _tune_upset_layout(fig, list(group_order))
    # fix legend ordering
    ax_extra = axes.get('extra0', None)
    if ax_extra is not None:
        handles, labels = ax_extra.get_legend_handles_labels()
        order = list(group_order)
        handles = [handles[labels.index(o)] for o in order if o in labels]
        labels = [o for o in order if o in labels]
        ax_extra.legend(
            handles,
            labels,
            title=group_col,
            bbox_to_anchor=(1.05, 1),
            loc='upper left',
            borderaxespad=0,
            fontsize=float(font_size),
            title_fontsize=float(font_size),
        )
    fig.suptitle(title, y=1.02, fontsize=float(font_size) + 1)
    savefig_multi(fig, out_base, "upset_weighted", formats)

def plot_venn(
    group_sets: Mapping[str, set],
    colors: Mapping[str, str],
    title: str,
    out_base: Path,
    formats: Sequence[str],
    weighted_labels: Optional[Mapping[Tuple[str, ...], float]] = None
) -> None:
    """
    3 sets => matplotlib-venn (if available)
    4–6 sets => venn.draw_venn (if available)
    """
    names = list(group_sets.keys())
    sets = [group_sets[n] for n in names]
    
    if len(names) == 2 and _HAVE_MPL_VENN:
        fig = plt.figure(figsize=(6, 6))
        venn2([sets[0], sets[1]], tuple(names),
              set_colors=(colors[names[0]], colors[names[1]]), alpha=0.6)
        plt.title(title)
        savefig_multi(fig, out_base, "venn", formats)
        return
    
    if len(names) == 3 and _HAVE_MPL_VENN:
        fig = plt.figure(figsize=(6, 6))
        if weighted_labels is None:
            venn3([sets[0], sets[1], sets[2]], tuple(names),
                  set_colors=(colors[names[0]], colors[names[1]], colors[names[2]]), alpha=0.6)
        else:
            # matplotlib-venn supports dict 'subsets' with binary keys like '100','010',...
            label_map = {
                (names[0],): '100',
                (names[1],): '010',
                (names[2],): '001',
                (names[0], names[1]): '110',
                (names[0], names[2]): '101',
                (names[1], names[2]): '011',
                (names[0], names[1], names[2]): '111',
            }
            subsets = {}
            for combo, val in weighted_labels.items():
                key = tuple(sorted(combo))
                if key in label_map:
                    subsets[label_map[key]] = float(val)
            venn3(subsets=subsets, set_labels=tuple(names),
                  set_colors=(colors[names[0]], colors[names[1]], colors[names[2]]), alpha=0.6)
        plt.title(title)
        savefig_multi(fig, out_base, "venn", formats)
        return
    
    if 4 <= len(names) <= 6 and _HAVE_VENN:
        labels2sets = {n: set(s) for n, s in zip(names, sets)}
        if not any(len(s) for s in labels2sets.values()):
            print("All Venn sets are empty; skipping Venn plot.")
        else:
            fig, ax = plt.subplots(figsize=(6, 6), constrained_layout=True)
            ax = venn(
                labels2sets,
                ax=ax,
                cmap=[colors[n] for n in names],
                fontsize=8,
                alpha=0.45,
            )
            ax.set_title(title)
            savefig_multi(fig, out_base, "venn", formats)
            plt.close(fig)
            return
    
    # Fallback: skip venn if no backend or too many groups
    print(f"[WARN] Venn plotting not available for {len(names)} sets (matplotlib-venn or venn not installed, or >6 sets). Skipping.")

def write_presence_and_sums(
    name_to_set: Mapping[str, set],
    per_asv_total: Mapping[str, float],
    out_base: Path,
    fname_prefix: str
) -> None:
    """
    Write two TSVs:
      - <prefix>_venn_presence_table.tsv : rows = (grouping label, ASV_ID)
      - <prefix>_venn_sum_table.tsv      : rows = (grouping label, Sum_count)
    """
    exc = exclusive_sets(name_to_set)
    # Presence
    rows = [[(" + ".join(k) if len(k) > 1 else k[0]), asv] for k, ids in exc.items() for asv in ids]
    pres = pd.DataFrame(rows, columns=["grouping", "ASV_ID"])
    pres.to_csv(out_base.with_name(f"{out_base.stem}_{fname_prefix}_venn_presence_table.tsv"), sep="\t", index=False)
    # Sums
    rows2 = []
    for k, ids in exc.items():
        s = float(sum(per_asv_total.get(a, 0.0) for a in ids))
        rows2.append([(" + ".join(k) if len(k) > 1 else k[0]), s])
    sums = pd.DataFrame(rows2, columns=["grouping", "Sum_count"])
    sums.to_csv(out_base.with_name(f"{out_base.stem}_{fname_prefix}_venn_sum_table.tsv"), sep="\t", index=False)

# ---------- Pipeline per domain ----------
def run_domain(
    inp: Inputs,
    id_col: str,
    group_col: str,
    color_col: str,
    group_order: Optional[Sequence[str]],
    subset_groups: Optional[Sequence[str]],
    use_raw: bool,
    use_final: bool,
    skip_venn: bool,
    formats: Sequence[str],
    font_size: float,
    group_palette: Optional[Mapping[str, str]] = None,
    output_tag: str = "",
) -> None:
    ensure_dir(inp.out_base)
    
    # Read
    raw_df = read_asv_table(inp.asv_raw) if use_raw else None
    final_df = read_asv_table(inp.asv_final) if use_final else None
    md = read_metadata(inp.meta, group_col, color_col)
    tx = read_taxonomy(inp.tax)
    
    # Build palette from explicit config first, then metadata (group -> color mapping).
    palette = {str(k): v for k, v in (group_palette or {}).items()}
    if color_col and color_col in md.columns:
        palette_df = md[[group_col, color_col]].dropna(subset=[group_col]).drop_duplicates()
        metadata_palette = dict(zip(palette_df[group_col].astype(str), palette_df[color_col]))
        metadata_palette.update(palette)
        palette = metadata_palette
    if not palette:
        palette = {str(g): "" for g in md[group_col].dropna().astype(str).unique()}
    
    # Sort groups (or apply explicit order when provided)
    if group_order:
        requested = [str(g).strip() for g in group_order if str(g).strip()]
        auto_groups = sort_groups([g for g in palette.keys() if g not in requested])
        all_groups = requested + auto_groups
    else:
        all_groups = sort_groups(list(palette.keys()))
    
    # Filter to subset if specified
    if subset_groups:
        all_groups = [g for g in all_groups if g in subset_groups]
        palette = {g: palette[g] for g in all_groups if g in palette}

    palette = sanitize_palette(palette, all_groups)
    
    print(f"[{inp.domain}] Groups (in order): {all_groups}")
    print(f"[{inp.domain}] Palette: {palette}")

    tag_suffix = f"_{output_tag.strip()}" if output_tag and output_tag.strip() else ""
    
    # Long format helper
    def melt_counts(count_df: pd.DataFrame) -> pd.DataFrame:
        st = count_df.stack().reset_index()
        st.columns = ['ASV_ID', id_col, 'count']
        return st
    
    # Process raw data
    if use_raw and raw_df is not None:
        raw_long = melt_counts(raw_df)
        raw_long = raw_long.merge(md, how="left", on=id_col)
        raw_pos = raw_long[raw_long['count'] > 0].copy()
        raw_tx = attach_taxonomy(raw_pos, tx)
        
        # Per-ASV totals and per-(group, ASV) totals
        raw_asv_total = raw_tx.groupby("ASV_ID")["count"].sum().to_dict()
        raw_group_asv_total = raw_tx.groupby([group_col, "ASV_ID"])["count"].sum().to_dict()
        
        # Build sets
        groups_present = [g for g in all_groups if g in set(raw_tx[group_col].unique())]
        group_sets = {g: set(raw_tx.loc[raw_tx[group_col] == g, 'ASV_ID']) for g in groups_present}
        
        if len(group_sets) >= 2:
            base = inp.out_base / f"raw_{inp.domain}{tag_suffix}"
            
            # UpSet unique
            plot_upset_unique(group_sets, {g: palette[g] for g in groups_present},
                            f"ASV Membership by {group_col} (Raw)", base, formats, group_col, font_size)
            
            # UpSet weighted
            plot_upset_weighted(group_sets, raw_group_asv_total, groups_present,
                              {g: palette[g] for g in groups_present},
                              f"ASV Abundance by {group_col} (Raw)", base, formats, group_col, font_size)
            
            # Venn (optional)
            if not skip_venn:
                plot_venn(group_sets, {g: palette[g] for g in groups_present},
                         f"Venn: ASV Membership (Raw)", base, formats)
            
            # Tables
            write_presence_and_sums(group_sets, raw_asv_total, base, "raw")
        else:
            print(f"[{inp.domain}] Skipping raw suite (need ≥2 present groups).")
    
    # Process final data
    if use_final and final_df is not None:
        final_long = melt_counts(final_df)
        final_long = final_long.merge(md, how="left", on=id_col)
        final_pos = final_long[final_long['count'] > 0].copy()
        fin_tx = attach_taxonomy(final_pos, tx)
        
        # Per-ASV totals and per-(group, ASV) totals
        fin_asv_total = fin_tx.groupby("ASV_ID")["count"].sum().to_dict()
        fin_group_asv_total = fin_tx.groupby([group_col, "ASV_ID"])["count"].sum().to_dict()
        
        # Build sets
        groups_present = [g for g in all_groups if g in set(fin_tx[group_col].unique())]
        group_sets = {g: set(fin_tx.loc[fin_tx[group_col] == g, 'ASV_ID']) for g in groups_present}
        
        if len(group_sets) >= 2:
            base = inp.out_base / f"final_{inp.domain}{tag_suffix}"
            
            # UpSet unique
            plot_upset_unique(group_sets, {g: palette[g] for g in groups_present},
                            f"ASV Membership by {group_col} (Final)", base, formats, group_col, font_size)
            
            # UpSet weighted
            plot_upset_weighted(group_sets, fin_group_asv_total, groups_present,
                              {g: palette[g] for g in groups_present},
                              f"ASV Abundance by {group_col} (Final)", base, formats, group_col, font_size)
            
            # Venn (optional)
            if not skip_venn:
                plot_venn(group_sets, {g: palette[g] for g in groups_present},
                         f"Venn: ASV Membership (Final)", base, formats)
            
            # Tables
            write_presence_and_sums(group_sets, fin_asv_total, base, "final")
        else:
            print(f"[{inp.domain}] Skipping final suite (need ≥2 present groups).")

# ---------- CLI ----------
def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="ASV overlap (UpSet & Venn) plotter")
    ap.add_argument("--data-dir", required=True, help="Project root directory")
    ap.add_argument("--subdir", default="spark_output", help="Root subdir with outputs")
    ap.add_argument("--domain", choices=["micro", "mito", "both"], default="micro",
                    help="Which domain(s) to run")
    ap.add_argument("--taxonomy-path", default=None,
                    help="Optional path to taxonomy, should point to <subdir>/taxonomy/ASV_SILVA_tax.full-length.vsearch.tsv")
    ap.add_argument("--metadata-path", default=None,
                    help="Optional metadata TSV override (defaults to metadata_updated_<domain>.tsv)")
    ap.add_argument("--asv-raw-path", default=None,
                    help="Optional raw ASV table override")
    ap.add_argument("--asv-final-path", default=None,
                    help="Optional final ASV table override")
    
    # Grouping and colors
    ap.add_argument("--sample-id-col", default="sampleID", help="Metadata column with sample IDs")
    ap.add_argument("--group-col", required=True, help="Metadata column for grouping (e.g., Depth, SampleType)")
    ap.add_argument("--color-col", default="Color", help="Metadata column with color values (hex codes)")
    ap.add_argument("--group-palette", default=None,
                    help="Explicit group color mapping, e.g. GroupA=#1f77b4,GroupB=#ff7f0e. Overrides --color-col values.")
    ap.add_argument("--subset-groups", default=None,
                    help="Optional comma-separated list of groups to include (subset of all groups)")
    ap.add_argument("--group-order", default=None,
                    help="Optional comma-separated explicit group order (e.g., 'Oral Rinse,BAL,Bronchial Brush')")
    
    # Data selection
    ap.add_argument("--use-raw", action="store_true", default=True, help="Process raw ASV data")
    ap.add_argument("--use-final", action="store_true", default=True, help="Process final ASV data")
    ap.add_argument("--raw-only", action="store_true", help="Only process raw data (shortcut)")
    ap.add_argument("--final-only", action="store_true", help="Only process final data (shortcut)")
    
    # Plotting options
    ap.add_argument("--skip-venn", action="store_true",
                    help="Skip Venn diagram generation (useful for >3-5 groups)")
    ap.add_argument("--formats", default="svg,pdf",
                    help="Comma-separated figure formats: e.g., svg,pdf,png")
    ap.add_argument("--font-size", type=float, default=12.0,
                    help="Base font size used in UpSet plots")
    ap.add_argument("--output-tag", default="",
                    help="Optional suffix tag added to output stems, e.g. 'raw' -> raw_micro_raw_*.svg")
    
    return ap.parse_args()

def main() -> None:
    args = parse_args()
    data_dir = Path(args.data_dir)
    taxonomy_path = Path(args.taxonomy_path) if args.taxonomy_path else None
    metadata_path = Path(args.metadata_path) if args.metadata_path else None
    asv_raw_path = Path(args.asv_raw_path) if args.asv_raw_path else None
    asv_final_path = Path(args.asv_final_path) if args.asv_final_path else None
    formats = [f.strip().lstrip(".") for f in args.formats.split(",") if f.strip()]
    
    # Parse subset groups if provided
    subset_groups = None
    if args.subset_groups:
        subset_groups = [g.strip() for g in args.subset_groups.split(",") if g.strip()]
    group_order = None
    if args.group_order:
        group_order = [g.strip() for g in args.group_order.split(",") if g.strip()]
    group_palette = parse_group_palette(args.group_palette)
    
    # Determine which data to process
    use_raw = args.use_raw
    use_final = args.use_final
    if args.raw_only:
        use_raw = True
        use_final = False
    elif args.final_only:
        use_raw = False
        use_final = True
    
    domains = ["micro", "mito"] if args.domain == "both" else [args.domain]
    
    for dom in domains:
        inp = Inputs(
            data_dir=data_dir,
            subdir=args.subdir,
            domain=dom,
            taxonomy_path=taxonomy_path,
            metadata_path=metadata_path,
            asv_raw_path=asv_raw_path,
            asv_final_path=asv_final_path,
        )
        print(f"[INFO] Running {inp}")
        run_domain(
            inp=inp,
            id_col=args.sample_id_col,
            group_col=args.group_col,
            color_col=args.color_col,
            group_order=group_order,
            subset_groups=subset_groups,
            use_raw=use_raw,
            use_final=use_final,
            skip_venn=args.skip_venn,
            formats=formats,
            font_size=args.font_size,
            group_palette=group_palette,
            output_tag=args.output_tag,
        )
        print(f"[OK] Finished {dom}")

if __name__ == "__main__":
    main()

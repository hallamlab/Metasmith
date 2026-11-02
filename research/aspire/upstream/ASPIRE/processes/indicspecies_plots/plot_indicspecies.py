#!/usr/bin/env python3
"""
isa_plots_cli.py
Build significance tables and publication-grade scatter plots from
indicspecies outputs (multipatt sign tables), optionally overlaying
Venn membership and coloring by Phylum.

Inputs (typical)
---------------
--group1-results   path/to/group1_indicator_species_results.tsv
--group2-results   path/to/group2_indicator_species_results.tsv
--venn           path/to/Three_types_venn_presence_table.tsv   (optional)
--taxonomy       path/to/taxonomy_updated.tsv                   (optional)

All key values (paths, thresholds, palettes, mappings) are configurable
via command line options. Outputs (SVG+PDF) and enriched TSVs are written
under --outdir.

Examples
--------
python isa_plots_cli.py \
  --group1-results  /.../indicspecies/group1_indicator_species_results.tsv \
  --group2-results /.../indicspecies/group2_indicator_species_results.tsv \
  --venn          /.../metadata/Three_types_venn_presence_table.tsv \
  --taxonomy      /.../metadata/taxonomy_updated.tsv \
  --outdir        /.../indicspecies \
  --q-thresh 0.05 --stat-thresh 0.0 \
  --group1-index "1=BAL,2=Bronchial Brush,3=Oral Rinse" \
  --group1-palette "Oral Rinse=#6A3D9A,BAL=#0072B2,Bronchial Brush=#009E73" \
  --group2-index "1=Cancer,2=Non-Cancer" \
  --group2-palette "Cancer=#A50026,Non-Cancer=#FFFFFF"

Palettes (and index-to-label lookups) can also be sourced from a metadata TSV by
passing --metadata along with the relevant column names, avoiding long
comma-separated CLI mappings.
"""

import argparse
from pathlib import Path
from typing import Optional
import warnings
import re

import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib as mpl
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import matplotlib.lines as mlines

# ----------------------- Matplotlib/Seaborn defaults ------------------------
mpl.rcParams['pdf.fonttype'] = 42      # Keep text as text in PDF
mpl.rcParams['svg.fonttype'] = 'none'  # Keep text as text in SVG
mpl.rcParams['savefig.dpi'] = 600
plt.rcParams.update({'font.size': 12})
plt.rcParams['font.family'] = 'Source Sans Pro'
sns.set_theme()
sns.set_style("white")

EXPLICIT_GROUP_TYPE_PALETTE = {
    "Oral Rinse": "#6A3D9A",
    "BAL": "#0072B2",
    "Bronchial Brush": "#009E73",
    "Lung Brush": "#009E73",
    "BAL+Oral Rinse": "#E78AC3",
    "BAL+Bronchial Brush": "#5CC8C8",
    "BAL+Lung Brush": "#5CC8C8",
    "Bronchial Brush+Oral Rinse": "#B8E186",
    "Lung Brush+Oral Rinse": "#B8E186",
    "BAL+Bronchial Brush+Oral Rinse": "#CBB6E9",
    "BAL+Lung Brush+Oral Rinse": "#CBB6E9",
    "not_indicator": "#D3D3D3",
}

CANONICAL_GROUP_TYPE_ORDER = [
    "Oral Rinse",
    "BAL+Oral Rinse",
    "BAL",
    "BAL+Bronchial Brush",
    "Bronchial Brush",
    "Bronchial Brush+Oral Rinse",
    "BAL+Bronchial Brush+Oral Rinse",
    "not_indicator",
]


# ------------------------------- Utilities ----------------------------------


def parse_mapping(s: str) -> dict:
    """
    Parse "A=#fff,B:#123,C=steelblue" or "1=BAL,2=Bronchial Brush" into dict.
    Returns {} if s is falsy/empty.
    """
    if not s:
        return {}
    out = {}
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


def parse_order(s: str) -> list[str]:
    if not s:
        return []
    vals: list[str] = []
    for x in str(s).split(","):
        x = x.strip()
        if not x:
            continue
        vals.append(normalize_combo(x))
    out: list[str] = []
    for v in vals:
        if v not in out:
            out.append(v)
    return out


def read_metadata_table(path: Optional[Path]) -> Optional[pd.DataFrame]:
    if path is None:
        return None
    df = pd.read_csv(path, sep="\t", header=0)
    return df


def _normalize_index_key(val) -> str:
    text = str(val).strip()
    if not text:
        return ""
    if text.isdigit():
        return text
    try:
        return str(int(float(text)))
    except (ValueError, TypeError):
        return text


def _coerce_scalar(val):
    if isinstance(val, pd.Series):
        if val.empty:
            return np.nan
        return val.iloc[0]
    return val


def build_meta_mapping(df: pd.DataFrame, key_col: str, value_col: str,
                       *, key_formatter=None, value_formatter=None) -> dict:
    ensure_cols(df, [key_col, value_col], "metadata palette table")
    sub = df[[key_col, value_col]].copy()
    sub = sub.dropna(how="all")
    mapping = {}
    for _, row in sub.iterrows():
        raw_key = _coerce_scalar(row[key_col])
        raw_val = _coerce_scalar(row[value_col])
        if pd.isna(raw_key) or pd.isna(raw_val):
            continue
        key = key_formatter(raw_key) if key_formatter else str(raw_key).strip()
        val = value_formatter(raw_val) if value_formatter else str(raw_val).strip()
        if key == "" or val == "":
            continue
        mapping.setdefault(key, val)
    return mapping


def extend_digit_keys(mapping: dict) -> dict:
    out = dict(mapping)
    for key, val in list(mapping.items()):
        if isinstance(key, str) and key.isdigit():
            out[int(key)] = val
    return out


def build_index_map_from_table(df: pd.DataFrame, idx_col: str, label_col: str) -> dict:
    ensure_cols(df, [idx_col, label_col], "indicator results table")
    mapping: dict = {}
    for idx_val, label in zip(df[idx_col], df[label_col]):
        idx_val = _coerce_scalar(idx_val)
        label = _coerce_scalar(label)
        if pd.isna(idx_val) or pd.isna(label):
            continue
        key = _normalize_index_key(idx_val)
        text = str(label).strip()
        if key and text:
            mapping.setdefault(key, text)
    return mapping


def build_palette_from_table(df: pd.DataFrame, label_col: str, color_col: str) -> dict:
    ensure_cols(df, [label_col, color_col], "indicator results table")
    mapping: dict = {}
    for label, color in zip(df[label_col], df[color_col]):
        label = _coerce_scalar(label)
        color = _coerce_scalar(color)
        if pd.isna(label) or pd.isna(color):
            continue
        text = str(label).strip()
        col = str(color).strip()
        if text and col:
            mapping.setdefault(text, col)
    return mapping


def infer_index_map_from_sign_table(df: pd.DataFrame, idx_col: str, p_col: str, stat_col: str) -> dict:
    """Infer index -> label mapping from group membership columns in a sign table.

    For indicspecies-style tables with columns `s.<group>` and a numeric `index`,
    decode `index` as a bitmask over the `s.<group>` column order, yielding labels
    like `0`, `1`, `0+1`, `0+2+4`, etc.
    """
    reserved = {
        "asv", "asv_id", "feature", "otu",
        "index", "stat", "p.value", "p_value", "q.value", "significant",
        str(idx_col).strip().lower(), str(p_col).strip().lower(), str(stat_col).strip().lower(),
    }
    s_cols = [str(c).strip() for c in df.columns if str(c).strip().startswith("s.")]
    if s_cols:
        groups = [c.split("s.", 1)[1].strip() for c in s_cols]
        groups = [g for g in groups if g]
        if groups:
            mapping: dict = {}
            n = len(groups)
            for i in range(1, (1 << n)):
                members = [groups[b] for b in range(n) if (i >> b) & 1]
                if members:
                    label = "+".join(members) if len(members) > 1 else members[0]
                    mapping[str(i)] = label
                    mapping[i] = label
            return mapping

    membership_cols = []
    for col in df.columns:
        name = str(col).strip()
        if not name:
            continue
        if name.lower() in reserved:
            continue
        membership_cols.append(name)
    return {str(i + 1): label for i, label in enumerate(membership_cols)}


def auto_palette_for_labels(labels: list[str]) -> dict:
    uniq = [str(x).strip() for x in labels if str(x).strip() and str(x).strip() != "not_indicator"]
    uniq = sorted(set(uniq))
    if not uniq:
        return {}
    colors = sns.color_palette("tab20", n_colors=max(len(uniq), 3))
    return {label: mcolors.to_hex(colors[i % len(colors)]) for i, label in enumerate(uniq)}


def auto_markers_for_labels(labels: list[str]) -> dict:
    uniq = [str(x).strip() for x in labels if str(x).strip() and str(x).strip() != "not_indicator"]
    uniq = sorted(set(uniq))
    if not uniq:
        return {}
    default_markers = ["o", "s", "D", "X", "^", "v", "P", "*", "h", "H", "8", "p", "<", ">"]
    return {label: default_markers[i % len(default_markers)] for i, label in enumerate(uniq)}


def sanitize_stub(name: str, fallback: str) -> str:
    if not name:
        return fallback
    slug = re.sub(r"[^0-9A-Za-z]+", "_", str(name).strip()).strip("_").lower()
    return slug or fallback


def normalize_combo(label: str) -> str:
    """
    Normalize combo strings like "BAL + Bronchial Brush" -> "BAL+Bronchial Brush"
    while preserving component order.
    """
    if not isinstance(label, str):
        return str(label)
    parts = [p.strip() for p in label.split("+")]
    parts = [p for p in parts if p]
    uniq: list[str] = []
    for p in parts:
        if p not in uniq:
            uniq.append(p)
    if not uniq:
        return label
    return "+".join(uniq)


def infer_all_combo_label(labels: list[str]) -> Optional[str]:
    """Infer the all-group combo as the label with the most components."""
    norm = [normalize_combo(str(x).strip()) for x in labels if str(x).strip()]
    norm = [x for x in norm if x and x != "not_indicator"]
    if not norm:
        return None
    combos = []
    for x in norm:
        n_parts = len([p for p in x.split("+") if p.strip()])
        if n_parts > 1:
            combos.append((n_parts, x))
    if not combos:
        return None
    combos.sort(key=lambda t: (-t[0], t[1]))
    return combos[0][1]


def combo_contains_component(label: object, component: object) -> bool:
    if label is None or component is None:
        return False
    lbl = normalize_combo(str(label).strip())
    comp = normalize_combo(str(component).strip())
    if not lbl or not comp:
        return False
    lbl_parts = {p.strip() for p in lbl.split("+") if p.strip()}
    comp_parts = {p.strip() for p in comp.split("+") if p.strip()}
    if not lbl_parts or not comp_parts:
        return False
    return comp_parts.issubset(lbl_parts)


def drop_all_group_rows(df: pd.DataFrame, label_col: str, all_label: Optional[str]) -> pd.DataFrame:
    if not all_label or label_col not in df.columns:
        return df
    out = df.copy()
    labels = out[label_col].map(lambda x: normalize_combo(str(x)) if pd.notna(x) else x)
    return out.loc[labels != all_label].copy()


def normalize_label_mapping_values(mapping: dict) -> dict:
    out = {}
    for k, v in mapping.items():
        out[k] = normalize_combo(str(v).strip())
    return out


def normalize_palette_keys(mapping: dict) -> dict:
    out = {}
    for k, v in mapping.items():
        kk = normalize_combo(str(k).strip())
        if kk and kk not in out:
            out[kk] = v
    return out


def normalize_order_list(vals: list[str]) -> list[str]:
    out: list[str] = []
    for v in vals:
        vv = normalize_combo(str(v).strip())
        if vv and vv not in out:
            out.append(vv)
    return out


def canonicalize_group_type_order(vals: list[str]) -> list[str]:
    norm = normalize_order_list(vals)
    if not norm:
        return []
    known = [lbl for lbl in CANONICAL_GROUP_TYPE_ORDER if lbl in norm]
    extra = [lbl for lbl in norm if lbl not in known]
    return known + extra


def augment_combo_palette(palette: dict, labels: list[str]) -> dict:
    out = dict(palette)
    for raw in labels:
        label = str(raw).strip()
        if not label or label in out or "+" not in label:
            continue
        explicit_color = EXPLICIT_GROUP_TYPE_PALETTE.get(label)
        if explicit_color is None:
            explicit_color = EXPLICIT_GROUP_TYPE_PALETTE.get(normalize_combo(label))
        if explicit_color is not None:
            out[label] = explicit_color
            out.setdefault(normalize_combo(label), explicit_color)
            continue
        parts = [p.strip() for p in re.split(r"\s*\+\s*", label) if p.strip()]
        if len(parts) < 2:
            continue
        colors = []
        ok = True
        for p in parts:
            if p in out:
                colors.append(out[p])
            else:
                ok = False
                break
        if not ok:
            continue
        try:
            rgb = np.mean([mcolors.to_rgb(c) for c in colors], axis=0)
            blend = mcolors.to_hex(rgb)
            out[label] = blend
            out.setdefault(normalize_combo(label), blend)
        except Exception:
            continue
    return out


def ensure_palette_has_order_entries(palette: dict, order: list[str]) -> dict:
    out = dict(palette)
    for raw in order:
        label = normalize_combo(str(raw).strip())
        if not label or label in out:
            continue
        if label in EXPLICIT_GROUP_TYPE_PALETTE:
            out[label] = EXPLICIT_GROUP_TYPE_PALETTE[label]
    return out


def read_taxonomy_table(taxonomy_path: Path) -> pd.DataFrame:
    """
    Read taxonomy TSV produced earlier; create Domain..Species columns robustly.
    Expects columns: ASV_ID, Taxon (e.g., 'k__Bacteria; p__Firmicutes; ...').
    """
    tax_df = pd.read_csv(taxonomy_path, sep="\t", header=0)
    if "Feature ID" in tax_df.columns:
        tax_df['ASV_ID'] = tax_df['Feature ID'].astype(str).str.partition(';')[0]
    if "ASV_ID" not in tax_df.columns:
        raise ValueError("Taxonomy file must contain column 'ASV_ID'.")
    if "Taxon" not in tax_df.columns:
        raise ValueError("Taxonomy file must contain column 'Taxon'.")

    # Some pipelines append a trailing ";<something>" to ASV_ID – drop last ';...' safely
    tax_df["ASV_ID"] = tax_df["ASV_ID"].astype(str).apply(lambda s: s.split(";", 1)[0])

    def split_taxa_string(taxa_str: str, delimiter=';'):
        tax_levels = ["Domain", "Phylum", "Class", "Order", "Family", "Genus", "Species"]
        taxa_str = str(taxa_str)
        if taxa_str and taxa_str != 'Unassigned' and taxa_str.lower() != 'nan':
            parts = [part.strip().split('__', 1)[-1] for part in taxa_str.split(delimiter)]
        else:
            parts = ['Unassigned']
        return {lvl: (parts[i] if i < len(parts) else None) for i, lvl in enumerate(tax_levels)}

    split_cols = tax_df["Taxon"].apply(split_taxa_string).apply(pd.Series)
    tax_df = pd.concat([tax_df, split_cols], axis=1).set_index("ASV_ID")
    return tax_df


def ensure_cols(df: pd.DataFrame, required: list, where: str):
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns in {where}: {missing}")


def compute_sig_table(
    sign_df: pd.DataFrame,
    index_map: dict,
    palette: dict,
    p_col: str = "p.value",
    q_col: str = "q.value",
    stat_col: str = "stat",
    idx_col: str = "index",
    p_thresh: float = 0.05,
    stat_thresh: float = 0.0,
    force_all_sig: bool = False,
    prefix: str = "type",
) -> pd.DataFrame:
    """
    From indicspecies summary table -> tidy table with log q, significance, label, color.
    - index_map: numeric string/int -> label (e.g., "1"->"BAL")
    - palette: label -> color
    """
    df = sign_df.copy()

    # ISA plotting must use the corrected q-values computed by indicspecies.
    ensure_cols(df, [p_col, q_col, stat_col, idx_col], "indicspecies summary table")
    df[p_col] = pd.to_numeric(df[p_col], errors="coerce")
    df[q_col] = pd.to_numeric(df[q_col], errors="coerce")
    df[stat_col] = pd.to_numeric(df[stat_col], errors="coerce")
    df[idx_col] = pd.to_numeric(df[idx_col], errors="coerce").astype("Int64")

    # keep minimal columns, but preserve any group-membership columns too
    base_cols = ["ASV_ID"] if "ASV_ID" in df.columns else []
    keep_cols = base_cols + [c for c in df.columns if c not in base_cols]
    df = df[keep_cols]

    # compute -log10 corrected q-value
    with np.errstate(divide="ignore", invalid="ignore"):
        df[f"{prefix}_log_q"] = (-np.log10(df[q_col])).replace([np.inf, -np.inf], np.nan).round(3)

    # significance
    if force_all_sig:
        df[f"{prefix}_significance"] = True
    else:
        df[f"{prefix}_significance"] = (df[q_col] < p_thresh) & (df[stat_col] > stat_thresh)

    # label from index_map
    def idx_to_label(x):
        if pd.isna(x):
            return "not_indicator"
        key = str(int(x))
        raw = index_map.get(key, index_map.get(int(x), f"index_{int(x)}"))
        return normalize_combo(raw)

    labels = df[idx_col].apply(idx_to_label).astype(str)
    df[f"{prefix}_label"] = labels.where(df[f"{prefix}_significance"], "not_indicator")

    # colors
    pal = dict(palette)
    pal["not_indicator"] = "lightgray"
    df[f"{prefix}_color"] = df[f"{prefix}_label"].map(lambda k: pal.get(k, "lightgray"))

    # rename canonical columns for consistency
    df.rename(columns={
        p_col:  f"{prefix}_p_value",
        q_col:  f"{prefix}_q_value",
        stat_col: f"{prefix}_stat",
        idx_col:  f"{prefix}_index"
    }, inplace=True)

    return df


def compute_plot_layout(
    df: pd.DataFrame,
    *,
    x_col: str,
    y_col: str,
    min_dist_x=0.02,
    min_dist_y=0.03,
    step_x=0.35,
    step_y=0.35,
    anchor=0.05,
    iters=200,
    add_random_eps=(0.0, 0.0),
    key_col: str = "ASV_ID",
) -> pd.DataFrame:
    dd = df.copy()
    if x_col not in dd.columns or y_col not in dd.columns:
        raise ValueError(f"x_col={x_col!r} or y_col={y_col!r} not present in DataFrame")

    dd[x_col] = pd.to_numeric(dd[x_col], errors="coerce")
    dd[y_col] = pd.to_numeric(dd[y_col], errors="coerce")
    dd = dd.replace([np.inf, -np.inf], np.nan).dropna(subset=[x_col, y_col])
    if dd.empty:
        return dd

    x = dd[x_col].to_numpy()
    y = dd[y_col].to_numpy()
    xmin, xmax = float(np.nanmin(x)), float(np.nanmax(x))
    ymin, ymax = float(np.nanmin(y)), float(np.nanmax(y))
    if xmin == xmax:
        xmin -= 0.05
        xmax += 0.05
    if ymin == ymax:
        ymin -= 0.05
        ymax += 0.05

    nxv = (x - xmin) / (xmax - xmin)
    nyv = (y - ymin) / (ymax - ymin)
    pos = np.stack([nxv, nyv], axis=1).astype(float)
    orig = pos.copy()

    n = len(pos)
    eye_mask = ~np.eye(n, dtype=bool)
    rng = np.random.default_rng(0)
    for _ in range(iters):
        dx = pos[:, None, 0] - pos[None, :, 0]
        dy = pos[:, None, 1] - pos[None, :, 1]
        mask = eye_mask & (np.abs(dx) < min_dist_x) & (np.abs(dy) < min_dist_y)
        if not mask.any():
            break
        sign_x = np.sign(dx)
        sign_y = np.sign(dy)
        sign_x[sign_x == 0] = rng.choice([-1.0, 1.0], size=(sign_x == 0).sum())
        sign_y[sign_y == 0] = rng.choice([-1.0, 1.0], size=(sign_y == 0).sum())
        force_x = np.zeros_like(dx)
        force_y = np.zeros_like(dy)
        force_x[mask] = (min_dist_x - np.abs(dx[mask])) * sign_x[mask]
        force_y[mask] = (min_dist_y - np.abs(dy[mask])) * sign_y[mask]
        pos[:, 0] += step_x * force_x.sum(axis=1) - anchor * (pos[:, 0] - orig[:, 0])
        pos[:, 1] += step_y * force_y.sum(axis=1) - anchor * (pos[:, 1] - orig[:, 1])
        np.clip(pos, 0.0, 1.0, out=pos)

    if add_random_eps != (0.0, 0.0):
        pos[:, 0] = np.clip(pos[:, 0] + rng.normal(0, add_random_eps[0], n), 0, 1)
        pos[:, 1] = np.clip(pos[:, 1] + rng.normal(0, add_random_eps[1], n), 0, 1)

    dd["_x_"] = pos[:, 0] * (xmax - xmin) + xmin
    dd["_y_"] = pos[:, 1] * (ymax - ymin) + ymin

    keep = [c for c in [key_col, x_col, y_col, "_x_", "_y_"] if c in dd.columns]
    return dd[keep].drop_duplicates(subset=[key_col] if key_col in dd.columns else None)


def plot_p_vs_stat_no_overlap(
    df: pd.DataFrame,
    output_file: Path,
    *,
    x_col: str,
    y_col: str,
    hue_col: str | None = None,
    style_col: str | None = None,
    hue_order: list[str] | None = None,
    style_order: list[str] | None = None,
    color_palette: dict | None = None,
    marker_dict: dict | None = None,
    # Jitter controls (normalized units)
    min_dist_x=0.02,
    min_dist_y=0.03,
    step_x=0.35,
    step_y=0.35,
    anchor=0.05,
    iters=200,
    add_random_eps=(0.0, 0.0),
    # Visuals
    invert_y=False,
    point_size=50,
    alpha=0.85,
    show_legend=True,
    legend_color_title="Type",
    legend_marker_title="Status",
    # Layout
    plot_size_in=(8.0, 6.0),
    axes_pad_in=(0.8, 0.6, 0.3, 0.2),
    figure_edge_pad_in=0.25,
    legend_pad_in=0.45,
    legend_vgap_in=0.25,
    legend_fontsize=10,
    label_col: str | None = None,
    label_mask_col: str | None = None,
    label_fontsize: int = 7,
    label_max: int = 300,
    xlabel: str | None = None,
    ylabel: str | None = None,
    layout_df: pd.DataFrame | None = None,
    layout_key_col: str = "ASV_ID",
):
    """Scatter x_col vs y_col with axis-wise repulsive jitter and fixed data area."""
    dd = df.copy()

    if x_col not in dd.columns or y_col not in dd.columns:
        raise ValueError(f"x_col={x_col!r} or y_col={y_col!r} not present in DataFrame")

    dd[x_col] = pd.to_numeric(dd[x_col], errors="coerce")
    dd[y_col] = pd.to_numeric(dd[y_col], errors="coerce")
    dd = dd.replace([np.inf, -np.inf], np.nan).dropna(subset=[x_col, y_col])
    if dd.empty:
        warnings.warn(f"No data to plot for {output_file.name} after dropping NaN/inf.")
        return

    x = dd[x_col].to_numpy()
    y = dd[y_col].to_numpy()
    xmin, xmax = float(np.nanmin(x)), float(np.nanmax(x))
    ymin, ymax = float(np.nanmin(y)), float(np.nanmax(y))
    if xmin == xmax:
        xmin -= 0.05; xmax += 0.05
    if ymin == ymax:
        ymin -= 0.05; ymax += 0.05

    if layout_df is not None:
        if layout_key_col not in dd.columns or layout_key_col not in layout_df.columns:
            raise ValueError(f"layout reuse requires '{layout_key_col}' in both plot data and layout_df")
        dd = dd.drop(columns=[c for c in ["_x_", "_y_"] if c in dd.columns])
        dd = dd.merge(
            layout_df[[layout_key_col, "_x_", "_y_"]].drop_duplicates(subset=[layout_key_col]),
            on=layout_key_col,
            how="left",
        )
        if dd[["_x_", "_y_"]].isna().any().any():
            raise ValueError(f"Missing reused layout coordinates for {output_file.name}")
    else:
        layout_dd = compute_plot_layout(
            dd,
            x_col=x_col,
            y_col=y_col,
            min_dist_x=min_dist_x,
            min_dist_y=min_dist_y,
            step_x=step_x,
            step_y=step_y,
            anchor=anchor,
            iters=iters,
            add_random_eps=add_random_eps,
            key_col=layout_key_col,
        )
        dd = dd.drop(columns=[c for c in ["_x_", "_y_"] if c in dd.columns])
        merge_cols = [c for c in [layout_key_col, x_col, y_col, "_x_", "_y_"] if c in layout_dd.columns]
        dd = dd.merge(layout_dd[merge_cols], on=[c for c in [layout_key_col, x_col, y_col] if c in merge_cols], how="left")

    # Normalize category columns to string so palette/marker dict keys are stable.
    if hue_col is not None and hue_col in dd.columns:
        dd[hue_col] = dd[hue_col].astype(str)
    if style_col is not None and style_col in dd.columns:
        dd[style_col] = dd[style_col].astype(str)

    # Resolve hue palette
    palette = None
    hue_order_eff = None
    if hue_col is not None:
        present = [str(v) for v in dd[hue_col].dropna().unique().tolist()]
        if hue_order:
            pref = [str(v) for v in hue_order]
            rem = [v for v in present if v not in pref]
            hue_order_eff = pref + rem
        else:
            hue_order_eff = present
        if color_palette:
            # Use given mapping, but only for categories present. Match keys as strings.
            palette_src = {str(k): v for k, v in color_palette.items()}
            palette = {}
            missing = []
            for k in hue_order_eff:
                k_str = str(k)
                if k_str in palette_src:
                    palette[k] = palette_src[k_str]
                else:
                    palette[k] = palette_src.get("not_indicator", "lightgray")
                    if k_str != "not_indicator":
                        missing.append(k_str)
            if missing:
                warnings.warn(
                    f"Palette missing {len(missing)} key(s) ({', '.join(missing[:8])}"
                    f"{'...' if len(missing) > 8 else ''}), using fallback color."
                )
        else:
            palette = None  # default seaborn

    # Resolve style markers
    markers = None
    style_order_eff = None
    if style_col is not None:
        cats = [str(v) for v in dd[style_col].dropna().unique().tolist()]
        if style_order:
            pref = [str(v) for v in style_order]
            rem = [v for v in cats if v not in pref]
            style_order_eff = pref + rem
        else:
            style_order_eff = cats
        default_markers = ["o", "s", "D", "X", "^", "v", "P", "*", "h", "H", "8", "p", "<", ">"]
        if marker_dict:
            markers = {str(k): v for k, v in marker_dict.items()}
            # Seaborn requires marker values for all present style levels.
            missing_cats = [str(c) for c in style_order_eff if str(c) not in markers]
            if missing_cats:
                for i, c in enumerate(missing_cats):
                    markers[c] = default_markers[i % len(default_markers)]
                warnings.warn(
                    f"Marker map missing {len(missing_cats)} level(s) "
                    f"({', '.join(missing_cats[:8])}{'...' if len(missing_cats) > 8 else ''}); "
                    "auto-filled with default markers."
                )
        else:
            markers = {c: default_markers[i % len(default_markers)] for i, c in enumerate(style_order_eff)}

    # Legend handles
    color_handles = []
    legend_palette = palette if palette is not None else color_palette
    if legend_palette:
        ordered_color_names = list(legend_palette.keys())
        if hue_order_eff:
            ordered_color_names = [n for n in hue_order_eff if n in legend_palette]
        for name in ordered_color_names:
            col = legend_palette[name]
            color_handles.append(
                mlines.Line2D([], [], marker="o", linestyle="None",
                              markerfacecolor=col, markeredgecolor="black",
                              markeredgewidth=0.5, markersize=8, label=str(name))
            )

    marker_handles = []
    if show_legend and style_col is not None and markers:
        ordered_marker_names = list(markers.keys())
        if style_order_eff:
            ordered_marker_names = [n for n in style_order_eff if n in markers]
        for name in ordered_marker_names:
            mk = markers[name]
            marker_handles.append(
                mlines.Line2D([], [], color="gray", marker=mk, linestyle="None",
                              markeredgewidth=0.5, markersize=8, label=str(name))
            )

    # Measure legend sizes
    def _legend_size_in(handles, title, fontsize):
        if not handles:
            return (0.0, 0.0)
        ftmp, axtmp = plt.subplots(figsize=(2, 2), dpi=100)
        leg = axtmp.legend(handles=handles, title=title, frameon=True, loc="upper left",
                           fontsize=fontsize, title_fontsize=fontsize)
        ftmp.canvas.draw()
        bbox = leg.get_window_extent(ftmp.canvas.get_renderer())
        w_in = bbox.width / ftmp.dpi
        h_in = bbox.height / ftmp.dpi
        plt.close(ftmp)
        return (w_in, h_in)

    color_w, color_h = _legend_size_in(color_handles, legend_color_title, legend_fontsize)
    marker_w, marker_h = _legend_size_in(marker_handles, legend_marker_title, legend_fontsize)

    legend_w_in = max(color_w, marker_w) if show_legend else 0.0
    legend_h_in = (color_h if color_h else 0.0) + (marker_h if marker_h else 0.0)
    if show_legend and color_h and marker_h:
        legend_h_in += legend_vgap_in

    # Fixed data rectangle layout
    plot_w_in, plot_h_in = plot_size_in
    padL, padB, padR, padT = axes_pad_in
    pane_w_in = plot_w_in + padL + padR
    pane_h_in = plot_h_in + padT + padB

    fig_w_in = (figure_edge_pad_in + pane_w_in +
                ((legend_pad_in + legend_w_in) if (show_legend and legend_w_in > 0) else 0.0) +
                figure_edge_pad_in)
    fig_h_in = figure_edge_pad_in + max(pane_h_in, legend_h_in if show_legend else pane_h_in) + figure_edge_pad_in

    fig = plt.figure(figsize=(fig_w_in, fig_h_in), dpi=100)

    ax_left = (figure_edge_pad_in + padL) / fig_w_in
    ax_bottom = (figure_edge_pad_in + padB) / fig_h_in
    ax_w = plot_w_in / fig_w_in
    ax_h = plot_h_in / fig_h_in
    ax = fig.add_axes([ax_left, ax_bottom, ax_w, ax_h])

    leg_ax = None
    if show_legend and legend_w_in > 0:
        leg_left = (figure_edge_pad_in + pane_w_in + legend_pad_in) / fig_w_in
        leg_w = legend_w_in / fig_w_in
        leg_ax = fig.add_axes([leg_left, figure_edge_pad_in / fig_h_in, leg_w,
                               1.0 - 2 * figure_edge_pad_in / fig_h_in])
        leg_ax.axis("off")

    # Plot
    sns.scatterplot(
        data=dd, x="_x_", y="_y_",
        hue=hue_col if hue_col else None,
        style=style_col if style_col else None,
        hue_order=hue_order_eff if hue_col else None,
        style_order=style_order_eff if style_col else None,
        palette=palette, markers=markers,
        s=point_size, alpha=alpha,
        linewidth=0.5, edgecolor="black",
        legend=False, ax=ax,
    )

    # In labeled plots, label every plotted colored point. Gray/background
    # "not_indicator" points are not labeled.
    if label_col and label_col in dd.columns:
        lbl_df = dd.copy()
        if hue_col and hue_col in lbl_df.columns:
            lbl_df = lbl_df.loc[
                lbl_df[hue_col].fillna("not_indicator").astype(str) != "not_indicator"
            ].copy()
        lbl_df = lbl_df.dropna(subset=[label_col, "_x_", "_y_"])
        if not lbl_df.empty:
            for _, row in lbl_df.iterrows():
                txt = str(row[label_col]).strip()
                if not txt:
                    continue
                ax.text(float(row["_x_"]), float(row["_y_"]), txt,
                        fontsize=label_fontsize, color="black",
                        ha="left", va="bottom")
    if xlabel is None:
        if x_col.endswith("_stat"):
            xlabel = "Indicator statistic"
        else:
            xlabel = x_col
    if ylabel is None:
        if y_col.endswith("_log_q"):
            ylabel = "-log10(q-value)"
        elif y_col.endswith("_log_p"):
            ylabel = "-log10(p-value)"
        else:
            ylabel = y_col
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.set_xlim(xmin - 0.05, xmax + 0.05)
    ax.set_ylim(ymin - 0.05, ymax + 0.05)
    if invert_y:
        ax.invert_yaxis()
    ax.grid(True, linewidth=0.3, alpha=0.3)
    ax.tick_params(axis="both", which="both", length=4, width=1)

    # Legend pane
    if show_legend and leg_ax is not None:
        y_cursor = 1.0
        if color_handles:
            leg1 = leg_ax.legend(handles=color_handles, title=legend_color_title,
                                 loc="upper left", bbox_to_anchor=(0.0, y_cursor),
                                 frameon=True, fontsize=legend_fontsize, title_fontsize=legend_fontsize)
            leg_ax.add_artist(leg1)
            y_cursor -= ((color_h if color_h else 0.0) + legend_vgap_in) / fig_h_in
        if marker_handles:
            leg_ax.legend(handles=marker_handles, title=legend_marker_title,
                          loc="upper left", bbox_to_anchor=(0.0, y_cursor),
                          frameon=True, fontsize=legend_fontsize, title_fontsize=legend_fontsize)

    # Save (SVG + PDF)
    output_file = Path(output_file)
    fig.savefig(output_file)
    if output_file.suffix.lower() == ".svg":
        fig.savefig(output_file.with_suffix(".pdf"))
    plt.close(fig)


# ------------------------------- Main ---------------------------------------
def main():
    ap = argparse.ArgumentParser(
        description="Refactored ISA plotting pipeline (indicspecies -> tidy tables + figures)."
    )
    ap.add_argument("--group1-results", type=Path, required=True,
                    help="indicspecies summary table for the first grouping (TSV; must include q.value).")
    ap.add_argument("--group2-results", type=Path, required=True,
                    help="indicspecies summary table for the second grouping (TSV; must include q.value).")
    ap.add_argument("--venn", type=Path, default=None,
                    help="Optional Venn presence table (cols: grouping, ASV_ID).")
    ap.add_argument("--taxonomy", type=Path, default=None,
                    help="Optional taxonomy table (taxonomy_updated.tsv).")
    ap.add_argument("--outdir", type=Path, required=True,
                    help="Output directory for enriched TSVs and figures.")

    # Thresholds
    ap.add_argument("--q-thresh", type=float, default=0.05, help="q-value significance threshold (default: 0.05).")
    ap.add_argument("--stat-thresh", type=float, default=0.0, help="stat threshold (default: 0.0).")
    ap.add_argument("--group1-name", default="Group1", help="Friendly name for the first grouping (used in legends/output names).")
    ap.add_argument("--group2-name", default="Group2", help="Friendly name for the second grouping (used in legends/output names).")

    # Index maps
    ap.add_argument("--group1-index", type=str, default="",
                    help='Mapping of indicspecies "index" to labels for group1, e.g. '
                         '"1=BAL,2=Bronchial Brush,3=Oral Rinse" (optional if metadata columns provided).')
    ap.add_argument("--group2-index", type=str, default="",
                    help='Mapping for group2 "index" values, e.g. "1=Cancer,2=Non-Cancer" '
                         '(optional if metadata columns provided).')
    ap.add_argument("--group1-label-col", default=None,
                    help="Column in the group1 results TSV containing the human-readable label for each index.")
    ap.add_argument("--group1-color-col", default=None,
                    help="Column in the group1 results TSV containing colors for each label.")
    ap.add_argument("--group2-label-col", default=None,
                    help="Column in the group2 results TSV containing the human-readable label for each index.")
    ap.add_argument("--group2-color-col", default=None,
                    help="Column in the group2 results TSV containing colors for each label.")
    ap.add_argument("--group2-marker-col", default=None,
                    help="Column in the group2 results TSV containing marker codes for each label.")

    # Palettes
    ap.add_argument("--group1-palette", type=str, default="",
                    help='Color map for group1 labels, e.g. '
                         '"Oral Rinse=#6A3D9A,BAL=#0072B2,Bronchial Brush=#009E73" '
                         '(optional if metadata columns provided).')
    ap.add_argument("--group2-palette", type=str, default="",
                    help='Color map for group2 labels, e.g. "Cancer=#A50026,Non-Cancer=#FFFFFF" '
                         '(optional if metadata columns provided).')
    ap.add_argument("--group2-markers", type=str, default="",
                    help='Marker styles for group2, e.g. "Cancer=X,Non-Cancer=D" '
                         '(optional if metadata columns provided).')
    ap.add_argument("--group1-order", type=str, default="",
                    help="Optional comma-separated order for group1 labels (legend + plotting order).")
    ap.add_argument("--group2-order", type=str, default="",
                    help="Optional comma-separated order for group2 labels (legend + plotting order).")
    ap.add_argument("--focus-group1-label", type=str, default="",
                    help="Optional group1 component label to keep colored (for example: Bronchial Brush). Mixed labels containing this component stay colored; other labels become not_indicator.")
    ap.add_argument("--focus-group2-label", type=str, default="",
                    help="Optional group2 component label to keep colored; mixed labels containing this component stay colored; other labels become not_indicator.")
    ap.add_argument("--label-focused-asvs", action="store_true",
                    help="Label ASV_ID text for focused points in ISA figures.")

    meta_opts = ap.add_argument_group("Metadata-derived mappings")
    meta_opts.add_argument("--metadata", type=Path, default=None,
                           help="Optional TSV containing group label/index/color/marker columns.")
    meta_opts.add_argument("--group1-meta-index-col", default=None,
                           help="Column in metadata mapping to group1 indices (used instead of --group1-index).")
    meta_opts.add_argument("--group1-meta-label-col", default=None,
                           help="Column in metadata providing group1 labels for palettes.")
    meta_opts.add_argument("--group1-meta-color-col", default=None,
                           help="Column in metadata providing hex/RGB colors for group1 labels.")
    meta_opts.add_argument("--group2-meta-index-col", default=None,
                           help="Column in metadata mapping to group2 indices (used instead of --group2-index).")
    meta_opts.add_argument("--group2-meta-label-col", default=None,
                           help="Column in metadata providing group2 labels.")
    meta_opts.add_argument("--group2-meta-color-col", default=None,
                           help="Column in metadata providing group2 colors.")
    meta_opts.add_argument("--group2-meta-marker-col", default=None,
                           help="Column in metadata providing matplotlib marker codes per group2 label.")

    # Column names in sign tables (robustness for variants)
    ap.add_argument("--p-col", default="p.value", help="Column name for raw p-values in indicspecies summary tables (default: p.value).")
    ap.add_argument("--q-col", default="q.value", help="Column name for corrected q-values in indicspecies summary tables (default: q.value).")
    ap.add_argument("--stat-col", default="stat", help="Column name for stat values in sign tables (default: stat).")
    ap.add_argument("--idx-col", default="index", help="Column name for index in sign tables (default: index).")

    # Plot layout
    ap.add_argument("--plot-width", type=float, default=8.0)
    ap.add_argument("--plot-height", type=float, default=6.0)

    args = ap.parse_args()

    group1_title = args.group1_name
    group2_title = args.group2_name
    group1_stub = sanitize_stub(group1_title, "group1")
    group2_stub = sanitize_stub(group2_title, "group2")
    group1_order = canonicalize_group_type_order(parse_order(args.group1_order))
    group2_order = parse_order(args.group2_order)

    outdir = args.outdir
    outdir.mkdir(parents=True, exist_ok=True)

    # ---- Read inputs ----
    g1_df = pd.read_csv(args.group1_results, sep="\t", header=0)
    if g1_df.columns[0].lower() not in ("asv_id", "asv", "feature", "otu"):
        # indicspecies export often lacks ASV column name; make it explicit
        g1_df.rename(columns={g1_df.columns[0]: "ASV_ID"}, inplace=True)
    else:
        g1_df.rename(columns={g1_df.columns[0]: "ASV_ID"}, inplace=True)

    g2_df = pd.read_csv(args.group2_results, sep="\t", header=0)
    if g2_df.columns[0].lower() not in ("asv_id", "asv", "feature", "otu"):
        g2_df.rename(columns={g2_df.columns[0]: "ASV_ID"}, inplace=True)
    else:
        g2_df.rename(columns={g2_df.columns[0]: "ASV_ID"}, inplace=True)

    venn_df = None
    if args.venn and args.venn.exists():
        venn_df = pd.read_csv(args.venn, sep="\t", header=0)
        ensure_cols(venn_df, ["ASV_ID", "grouping"], "Venn table")
        venn_df["grouping"] = venn_df["grouping"].astype(str).map(normalize_combo)

    tax_df = None
    if args.taxonomy:
        tax_df = read_taxonomy_table(args.taxonomy)

    palette_meta = read_metadata_table(args.metadata)

    # ---- Parse mappings/palettes ----
    group1_index_map: dict = {}
    if palette_meta is not None and args.group1_meta_index_col and args.group1_meta_label_col:
        group1_index_map.update(
            build_meta_mapping(
                palette_meta,
                key_col=args.group1_meta_index_col,
                value_col=args.group1_meta_label_col,
                key_formatter=_normalize_index_key,
                value_formatter=lambda x: str(x).strip(),
            )
        )
    group1_index_map.update(parse_mapping(args.group1_index))
    if not group1_index_map and args.group1_label_col:
        group1_index_map.update(
            build_index_map_from_table(g1_df, args.idx_col, args.group1_label_col)
        )
    if not group1_index_map:
        group1_index_map.update(
            infer_index_map_from_sign_table(g1_df, args.idx_col, args.p_col, args.stat_col)
        )
    if not group1_index_map:
        raise ValueError("Provide group1 index mapping via --group1-index or metadata columns.")
    group1_index_map = normalize_label_mapping_values(group1_index_map)
    group1_index_map = extend_digit_keys(group1_index_map)

    group2_index_map: dict = {}
    if palette_meta is not None and args.group2_meta_index_col and args.group2_meta_label_col:
        group2_index_map.update(
            build_meta_mapping(
                palette_meta,
                key_col=args.group2_meta_index_col,
                value_col=args.group2_meta_label_col,
                key_formatter=_normalize_index_key,
                value_formatter=lambda x: str(x).strip(),
            )
        )
    group2_index_map.update(parse_mapping(args.group2_index))
    if not group2_index_map and args.group2_label_col:
        group2_index_map.update(
            build_index_map_from_table(g2_df, args.idx_col, args.group2_label_col)
        )
    if not group2_index_map:
        group2_index_map.update(
            infer_index_map_from_sign_table(g2_df, args.idx_col, args.p_col, args.stat_col)
        )
    if not group2_index_map:
        raise ValueError("Provide group2 index mapping via --group2-index or metadata columns.")
    group2_index_map = normalize_label_mapping_values(group2_index_map)
    group2_index_map = extend_digit_keys(group2_index_map)

    group1_palette: dict = {}
    if palette_meta is not None and args.group1_meta_label_col and args.group1_meta_color_col:
        group1_palette.update(
            build_meta_mapping(
                palette_meta,
                key_col=args.group1_meta_label_col,
                value_col=args.group1_meta_color_col,
                value_formatter=lambda x: str(x).strip(),
            )
        )
    group1_palette.update(parse_mapping(args.group1_palette))
    if not group1_palette and args.group1_label_col and args.group1_color_col:
        group1_palette.update(
            build_palette_from_table(g1_df, args.group1_label_col, args.group1_color_col)
        )
    if not group1_palette:
        group1_palette.update(auto_palette_for_labels(list(group1_index_map.values())))
    group1_palette = normalize_palette_keys(group1_palette)
    group1_palette = augment_combo_palette(group1_palette, [str(v) for v in group1_index_map.values()])
    group1_palette = ensure_palette_has_order_entries(group1_palette, group1_order)
    group1_palette = normalize_palette_keys(group1_palette)

    group2_palette: dict = {}
    if palette_meta is not None and args.group2_meta_label_col and args.group2_meta_color_col:
        group2_palette.update(
            build_meta_mapping(
                palette_meta,
                key_col=args.group2_meta_label_col,
                value_col=args.group2_meta_color_col,
                value_formatter=lambda x: str(x).strip(),
            )
        )
    group2_palette.update(parse_mapping(args.group2_palette))
    if not group2_palette and args.group2_label_col and args.group2_color_col:
        group2_palette.update(
            build_palette_from_table(g2_df, args.group2_label_col, args.group2_color_col)
        )
    if not group2_palette:
        group2_palette.update(auto_palette_for_labels(list(group2_index_map.values())))
    group2_palette = normalize_palette_keys(group2_palette)
    group2_palette = augment_combo_palette(group2_palette, [str(v) for v in group2_index_map.values()])
    group2_palette = ensure_palette_has_order_entries(group2_palette, group2_order)
    group2_palette = normalize_palette_keys(group2_palette)

    group2_markers: dict = {}
    if palette_meta is not None and args.group2_meta_label_col and args.group2_meta_marker_col:
        group2_markers.update(
            build_meta_mapping(
                palette_meta,
                key_col=args.group2_meta_label_col,
                value_col=args.group2_meta_marker_col,
                value_formatter=lambda x: str(x).strip(),
            )
        )
    manual_markers = parse_mapping(args.group2_markers)
    if manual_markers:
        group2_markers.update(manual_markers)
    if not group2_markers and args.group2_label_col and args.group2_marker_col:
        group2_markers.update(
            build_palette_from_table(g2_df, args.group2_label_col, args.group2_marker_col)
        )
    if not group2_markers:
        group2_markers.update(auto_markers_for_labels(list(group2_palette.keys())))
    group2_markers = {normalize_combo(str(k)): v for k, v in group2_markers.items()}

    focus_group1_label = normalize_combo(args.focus_group1_label) if args.focus_group1_label else ""
    focus_group2_label = normalize_combo(args.focus_group2_label) if args.focus_group2_label else ""

    # Keep the full configured category set in legends, including all mixed groups.
    group1_all_label = infer_all_combo_label(
        group1_order if group1_order else list(group1_palette.keys()) + [str(v) for v in group1_index_map.values()]
    )
    group2_all_label = infer_all_combo_label(
        group2_order if group2_order else list(group2_palette.keys()) + [str(v) for v in group2_index_map.values()]
    )
    group1_order_plot = [normalize_combo(x) for x in group1_order]
    group2_order_plot = [normalize_combo(x) for x in group2_order]
    group1_palette_plot = {
        normalize_combo(str(k)): v
        for k, v in group1_palette.items()
    }
    group2_palette_plot = {
        normalize_combo(str(k)): v
        for k, v in group2_palette.items()
    }
    group2_markers_plot = {
        normalize_combo(str(k)): v
        for k, v in group2_markers.items()
    }

    def apply_focus_label(df_in: pd.DataFrame, label_col: str, focus_label: str, all_label: Optional[str], flag_col: str) -> pd.DataFrame:
        df = df_in.copy()
        if not focus_label or label_col not in df.columns:
            df[flag_col] = False
            return df
        lab_norm = df[label_col].map(lambda x: normalize_combo(str(x)) if pd.notna(x) else "")
        mask = lab_norm.map(lambda x: combo_contains_component(x, focus_label))
        df[flag_col] = mask.fillna(False)
        df.loc[~df[flag_col], label_col] = "not_indicator"
        return df

    # ---- Build significance tables ----
    group1_sig = compute_sig_table(
        g1_df, index_map=group1_index_map, palette=group1_palette,
        p_col=args.p_col, q_col=args.q_col, stat_col=args.stat_col, idx_col=args.idx_col,
        p_thresh=args.q_thresh, stat_thresh=args.stat_thresh,
        force_all_sig=False, prefix="group1"
    )
    group1_sig.to_csv(outdir / f"{group1_stub}_ISA_enriched.tsv", sep="\t", index=False)

    group2_sig = compute_sig_table(
        g2_df, index_map=group2_index_map, palette=group2_palette,
        p_col=args.p_col, q_col=args.q_col, stat_col=args.stat_col, idx_col=args.idx_col,
        p_thresh=args.q_thresh, stat_thresh=args.stat_thresh,
        force_all_sig=False, prefix="group2"
    )
    group2_sig.to_csv(outdir / f"{group2_stub}_ISA_enriched.tsv", sep="\t", index=False)
    group1_sig_plot = drop_all_group_rows(group1_sig, "group1_label", group1_all_label)
    group2_sig_plot = drop_all_group_rows(group2_sig, "group2_label", group2_all_label)

    # Keep all-sig and focus-only variants separately.
    group1_sig_plot_all = group1_sig_plot.copy()
    group2_sig_plot_all = group2_sig_plot.copy()
    group1_sig_plot_focus = apply_focus_label(group1_sig_plot, "group1_label", focus_group1_label, group1_all_label, "__focus_group1__")
    group2_sig_plot_focus = apply_focus_label(group2_sig_plot, "group2_label", focus_group2_label, group2_all_label, "__focus_group2__")
    group1_layout_all = compute_plot_layout(group1_sig_plot_all, x_col="group1_stat", y_col="group1_log_q")
    group2_layout_all = compute_plot_layout(group2_sig_plot_all, x_col="group2_stat", y_col="group2_log_q")

    group1_order_all = list(group1_order_plot) if group1_order_plot else []
    group2_order_all = list(group2_order_plot) if group2_order_plot else []
    group1_palette_all = dict(group1_palette_plot)
    group2_palette_all = dict(group2_palette_plot)
    group1_palette_all["not_indicator"] = "lightgray"
    group2_palette_all["not_indicator"] = "lightgray"

    group1_order_focus = list(group1_order_all)
    group2_order_focus = list(group2_order_all)
    group1_palette_focus = dict(group1_palette_all)
    group2_palette_focus = dict(group2_palette_all)
    group2_markers_focus = dict(group2_markers_plot)

    if focus_group1_label:
        base_labels = normalize_order_list(
            (group1_order_all if group1_order_all else []) +
            list(group1_palette_all.keys()) +
            group1_sig_plot_focus["group1_label"].dropna().astype(str).tolist()
        )
        keep_group1 = [
            lbl for lbl in base_labels
            if combo_contains_component(lbl, focus_group1_label)
        ]
        if not keep_group1:
            keep_group1 = [focus_group1_label]
        group1_order_focus = [lbl for lbl in group1_order_all if lbl in keep_group1] if group1_order_all else keep_group1
        if "not_indicator" not in group1_order_focus:
            group1_order_focus.append("not_indicator")
        group1_palette_focus = {
            lbl: group1_palette_all.get(lbl, group1_palette.get(lbl, "lightgray"))
            for lbl in keep_group1
        }
        group1_palette_focus["not_indicator"] = "lightgray"

    if focus_group2_label:
        base_labels = normalize_order_list(
            (group2_order_all if group2_order_all else []) +
            list(group2_palette_all.keys()) +
            group2_sig_plot_focus["group2_label"].dropna().astype(str).tolist()
        )
        keep_group2 = [
            lbl for lbl in base_labels
            if combo_contains_component(lbl, focus_group2_label)
        ]
        if not keep_group2:
            keep_group2 = [focus_group2_label]
        group2_order_focus = [lbl for lbl in group2_order_all if lbl in keep_group2] if group2_order_all else keep_group2
        group2_palette_focus = {
            lbl: group2_palette_all.get(lbl, group2_palette.get(lbl, "lightgray"))
            for lbl in keep_group2
        }
        group2_palette_focus["not_indicator"] = "lightgray"
        group2_markers_focus = {lbl: group2_markers_plot.get(lbl, "o") for lbl in keep_group2}

    # ---- Type plot (ISA) ----
    plot_p_vs_stat_no_overlap(
        group1_sig_plot_all,
        outdir / f"{group1_stub}_ISA_plot.svg",
        x_col="group1_stat", y_col="group1_log_q",
        hue_col="group1_label",
        hue_order=group1_order_all if group1_order_all else None,
        color_palette=group1_palette_all,
        plot_size_in=(args.plot_width, args.plot_height),
        legend_color_title=group1_title,
        layout_df=group1_layout_all,
    )
    plot_p_vs_stat_no_overlap(
        group1_sig_plot_all,
        outdir / f"{group1_stub}_ISA_plot_LABELED.svg",
        x_col="group1_stat", y_col="group1_log_q",
        hue_col="group1_label",
        hue_order=group1_order_all if group1_order_all else None,
        color_palette=group1_palette_all,
        plot_size_in=(args.plot_width, args.plot_height),
        legend_color_title=group1_title,
        label_col="ASV_ID",
        label_mask_col="group1_significance",
        layout_df=group1_layout_all,
    )
    if focus_group1_label:
        plot_p_vs_stat_no_overlap(
            group1_sig_plot_focus,
            outdir / f"{group1_stub}_ISA_plot_FOCUS.svg",
            x_col="group1_stat", y_col="group1_log_q",
            hue_col="group1_label",
            hue_order=group1_order_focus if group1_order_focus else None,
            color_palette=group1_palette_focus,
            plot_size_in=(args.plot_width, args.plot_height),
            legend_color_title=f"{group1_title} ({focus_group1_label})",
            layout_df=group1_layout_all,
        )
        plot_p_vs_stat_no_overlap(
            group1_sig_plot_focus,
            outdir / f"{group1_stub}_ISA_plot_FOCUS_LABELED.svg",
            x_col="group1_stat", y_col="group1_log_q",
            hue_col="group1_label",
            hue_order=group1_order_focus if group1_order_focus else None,
            color_palette=group1_palette_focus,
            plot_size_in=(args.plot_width, args.plot_height),
            legend_color_title=f"{group1_title} ({focus_group1_label})",
            label_col="ASV_ID" if args.label_focused_asvs else None,
            label_mask_col="__focus_group1__" if args.label_focused_asvs else None,
            layout_df=group1_layout_all,
        )

    # ---- Type plot using Venn membership (optional; force all sig for color only) ----
    if venn_df is not None:
        vmap = dict(zip(venn_df["ASV_ID"], venn_df["grouping"]))
        # Map Venn label -> normalized, then to index_map labels
        # Convert to the index label set used in palette (normalize for consistent keys)
        # If a Venn label isn't present in your palette, it'll fall back to lightgray.
        v_sub = g1_df.copy()
        if "ASV_ID" not in v_sub.columns:
            v_sub.rename(columns={v_sub.columns[0]: "ASV_ID"}, inplace=True)
        v_sub["ASV_ID"] = v_sub["ASV_ID"].astype(str)
        v_sub["__venn_label__"] = v_sub["ASV_ID"].map(vmap).dropna()

        venn_sig = compute_sig_table(
            v_sub, index_map={}, palette={},  # labels come from Venn below
            p_col=args.p_col, q_col=args.q_col, stat_col=args.stat_col, idx_col=args.idx_col,
            p_thresh=args.q_thresh, stat_thresh=args.stat_thresh,
            force_all_sig=True, prefix="group1"
        )
        venn_sig["group1_label"] = venn_sig["ASV_ID"].map(vmap).map(normalize_combo).fillna("not_indicator")
        # Convert Venn labels to palette keys if you used a different wording
        # Example mapping often needed:
        # "Oral Rinse + BAL" -> "BAL+Oral Rinse"
        venn_label_to_palette_key = {normalize_combo(k): k for k in group1_palette.keys()}  # identity by default
        venn_sig["group1_label"] = venn_sig["group1_label"].map(lambda s: venn_label_to_palette_key.get(s, s))
        venn_sig["group1_color"] = venn_sig["group1_label"].map(lambda k: group1_palette.get(k, "lightgray"))

        venn_sig.to_csv(outdir / f"{group1_stub}_venn_enriched.tsv", sep="\t", index=False)
        venn_sig_plot = drop_all_group_rows(venn_sig, "group1_label", group1_all_label)
        venn_sig_plot = apply_focus_label(venn_sig_plot, "group1_label", focus_group1_label, group1_all_label, "__focus_group1__")
        plot_p_vs_stat_no_overlap(
            venn_sig_plot,
            outdir / f"{group1_stub}_Venn_plot.svg",
            x_col="group1_stat", y_col="group1_log_q",
            hue_col="group1_label",
            hue_order=group1_order_all if group1_order_all else None,
            color_palette=group1_palette_all,
            plot_size_in=(args.plot_width, args.plot_height),
            legend_color_title=group1_title,
            label_col="ASV_ID" if args.label_focused_asvs and focus_group1_label else None,
            label_mask_col="__focus_group1__" if args.label_focused_asvs and focus_group1_label else None,
            layout_df=group1_layout_all,
        )

    # ---- Status plot (ISA) ----
    plot_p_vs_stat_no_overlap(
        group2_sig_plot_all,
        outdir / f"{group2_stub}_ISA_plot.svg",
        x_col="group2_stat", y_col="group2_log_q",
        hue_col="group2_label",
        hue_order=group2_order_all if group2_order_all else None,
        color_palette=group2_palette_all,
        plot_size_in=(args.plot_width, args.plot_height),
        legend_color_title=group2_title,
        layout_df=group2_layout_all,
    )
    if focus_group2_label:
        plot_p_vs_stat_no_overlap(
            group2_sig_plot_focus,
            outdir / f"{group2_stub}_ISA_plot_FOCUS.svg",
            x_col="group2_stat", y_col="group2_log_q",
            hue_col="group2_label",
            hue_order=group2_order_focus if group2_order_focus else None,
            color_palette=group2_palette_focus,
            plot_size_in=(args.plot_width, args.plot_height),
            legend_color_title=f"{group2_title} ({focus_group2_label})",
            layout_df=group2_layout_all,
        )
        plot_p_vs_stat_no_overlap(
            group2_sig_plot_focus,
            outdir / f"{group2_stub}_ISA_plot_FOCUS_LABELED.svg",
            x_col="group2_stat", y_col="group2_log_q",
            hue_col="group2_label",
            hue_order=group2_order_focus if group2_order_focus else None,
            color_palette=group2_palette_focus,
            plot_size_in=(args.plot_width, args.plot_height),
            legend_color_title=f"{group2_title} ({focus_group2_label})",
            label_col="ASV_ID" if args.label_focused_asvs else None,
            label_mask_col="__focus_group2__" if args.label_focused_asvs else None,
            layout_df=group2_layout_all,
        )
    # ---- Combined tables/plots: join group1 + group2 on ASV ----
    combined = pd.merge(group1_sig[["ASV_ID", "group1_stat", "group1_p_value", "group1_q_value", "group1_log_q",
                                    "group1_significance", "group1_label", "group1_color"]],
                        group2_sig[["ASV_ID", "group2_stat", "group2_p_value", "group2_q_value", "group2_log_q",
                                    "group2_significance", "group2_label"]],
                        on="ASV_ID", how="outer")
    combined.to_csv(outdir / f"{group1_stub}_{group2_stub}_ISA_results.tsv", sep="\t", index=False)
    combined_plot = drop_all_group_rows(combined, "group1_label", group1_all_label)
    combined_plot = drop_all_group_rows(combined_plot, "group2_label", group2_all_label)
    combined_layout_all = compute_plot_layout(combined_plot, x_col="group2_stat", y_col="group2_log_q")

    plot_p_vs_stat_no_overlap(
        combined_plot,
        outdir / "Combined_ISA_plot.svg",
        x_col="group2_stat", y_col="group2_log_q",
        hue_col="group1_label", style_col="group2_label",
        hue_order=group1_order_all if group1_order_all else None,
        style_order=group2_order_all if group2_order_all else None,
        color_palette=group1_palette_all, marker_dict=group2_markers_plot,
        legend_color_title=group1_title, legend_marker_title=group2_title,
        plot_size_in=(args.plot_width, args.plot_height),
        layout_df=combined_layout_all,
    )

    # ---- Phylum-colored variants (if taxonomy provided) ----
    if tax_df is not None:
        # For Type ISA
        group1_tax = group1_sig.merge(tax_df, left_on="ASV_ID", right_index=True, how="left")
        group1_tax_plot = drop_all_group_rows(group1_tax, "group1_label", group1_all_label)
        group1_tax_plot["Phylum_plot"] = np.where(
            group1_tax_plot["group1_significance"].fillna(False).astype(bool),
            group1_tax_plot["Phylum"].fillna("not_indicator").astype(str),
            "not_indicator"
        )
        phyla = sorted([p for p in group1_tax_plot["Phylum_plot"].dropna().astype(str).unique().tolist() if p != "not_indicator"])
        phyl_pal = {p: c for p, c in zip(phyla, sns.color_palette('tab20', len(phyla)).as_hex())}
        phyl_pal["not_indicator"] = "lightgray"
        group1_tax.to_csv(outdir / f"{group1_stub}_ISA_with_taxonomy.tsv", sep="\t", index=False)
        plot_p_vs_stat_no_overlap(
            group1_tax_plot,
            outdir / f"{group1_stub}_ISA_plot_Phylum.svg",
            x_col="group1_stat", y_col="group1_log_q",
            hue_col="Phylum_plot", color_palette=phyl_pal,
            legend_color_title="Phylum",
            plot_size_in=(args.plot_width, args.plot_height),
            layout_df=group1_layout_all,
        )
        plot_p_vs_stat_no_overlap(
            group1_tax_plot,
            outdir / f"{group1_stub}_ISA_plot_Phylum_LABELED.svg",
            x_col="group1_stat", y_col="group1_log_q",
            hue_col="Phylum_plot", color_palette=phyl_pal,
            legend_color_title="Phylum",
            plot_size_in=(args.plot_width, args.plot_height),
            label_col="ASV_ID",
            label_mask_col="group1_significance",
            layout_df=group1_layout_all,
        )

        # Combined + taxonomy
        comb_tax = combined.merge(tax_df, left_on="ASV_ID", right_index=True, how="left")
        comb_tax_plot = drop_all_group_rows(comb_tax, "group1_label", group1_all_label)
        comb_tax_plot = drop_all_group_rows(comb_tax_plot, "group2_label", group2_all_label)
        comb_tax.to_csv(outdir / "Combined_ISA_with_taxonomy.tsv", sep="\t", index=False)
        plot_p_vs_stat_no_overlap(
            comb_tax_plot,
            outdir / "Combined_ISA_plot_Phylum.svg",
            x_col="group2_stat", y_col="group2_log_q",
            hue_col="Phylum", style_col="group2_label",
            style_order=group2_order_all if group2_order_all else None,
            color_palette=phyl_pal, marker_dict=group2_markers_plot,
            legend_color_title="Phylum", legend_marker_title=group2_title,
            plot_size_in=(args.plot_width, args.plot_height),
            layout_df=combined_layout_all,
        )

    print(f"Done. Outputs in: {outdir}")

if __name__ == "__main__":
    main()

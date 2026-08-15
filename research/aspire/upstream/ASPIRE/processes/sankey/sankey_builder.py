#!/usr/bin/env python3
"""
data_loss_sankey.py
Build Sankey diagrams for read/ASV flow with flexible I/O, grouping, and colors.

Two modes:
A) COMPUTE from pipeline TSVs (default)
B) MANUAL via --steps/--lmp-in/--lmp-out

Examples
--------
# A) Compute from files (defaults mirror your script paths/columns)
python data_loss_sankey.py \
  --data-dir /path/to/project \
  --sub-dir spark_combined_output \
  --metadata /path/to/project/ref_db/spark_metadata.tsv \
  --group1-col type_group \
  --samp-col lmp_id \
  --keep-types "Oral Rinse,Lung Brush,BAL,Skin Brush,Scope Flush" \
  --fastq-stats stats/fastq_stats.tsv \
  --filtered-stats stats/filtered_fastqs.tsv \
  --asv-raw ASVs/ASV_counts.tsv \
  --asv-decon ASVs/ASV_target.decon.tsv \
  --asv-micro ASVs/ASV_target.micro.tsv \
  --palette "Scope Flush:#E69F00,Skin Brush:#CC79A7,Lung Brush:#009E73,BAL:#0072B2,Oral Rinse:#6A3D9A,Failed-QC:lightgray" \
  --title "Data Loss Flow" \
  --output-prefix metadata/data_loss_sankey --make-labeled --make-unlabeled

# B) Manual counts
python data_loss_sankey.py \
  --steps "Quality Control:123456,Error Correction:110000,Decontamination:98000,Off-Target Filtering:82000,Finished Data:76000" \
  --lmp-in "Oral Rinse:40000,Lung Brush:35000,BAL:28000,Skin Brush:12000,Scope Flush:8400" \
  --lmp-out "Oral Rinse:18000,Lung Brush:22000,BAL:24000,Skin Brush:9000,Scope Flush:5100" \
  --palette "Oral Rinse:#6A3D9A,Lung Brush:#009E73,BAL:#0072B2,Skin Brush:#CC79A7,Scope Flush:#E69F00" \
  --output-prefix out/sankey --make-labeled --make-unlabeled
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path
from typing import Dict, List, Tuple, Sequence, Optional
from xml.sax.saxutils import escape

import pandas as pd
import plotly.graph_objects as go

# Optional aesthetics (kept simple; plots are Plotly HTML)
import matplotlib as mpl
import seaborn as sns
import matplotlib.pyplot as plt

# ---------- Global aesthetics (safe no-ops if MPL not used) ----------
mpl.rcParams['pdf.fonttype'] = 42
mpl.rcParams['svg.fonttype'] = 'none'
mpl.rcParams['savefig.dpi'] = 600
plt.rcParams.update({'font.size': 12})
plt.rcParams['font.family'] = 'Source Sans Pro'
sns.set_theme()
sns.set_style("white")


# =========================
# Utility parsers / helpers
# =========================
def parse_kv_csv(s: str, val_cast=int) -> Dict[str, object]:
    """
    Parse 'A:1,B:2' into dict. Whitespace tolerated. Empty string -> {}.
    """
    out: Dict[str, object] = {}
    if not s:
        return out
    for item in s.split(','):
        item = item.strip()
        if not item:
            continue
        if ':' not in item:
            raise ValueError(f"Expected key:value pair, got '{item}'")
        k, v = item.split(':', 1)
        k = k.strip()
        v = v.strip()
        out[k] = val_cast(v) if val_cast is not None else v
    return out


def parse_steps_csv(s: str) -> Tuple[List[str], List[int]]:
    """
    Parse 'StepA:100,StepB:90,...' -> (['StepA','StepB',...],[100,90,...])
    """
    d = parse_kv_csv(s, val_cast=int)
    return list(d.keys()), list(d.values())


def extract_sample_id_from_path(path_str: str) -> str:
    """
    Extract a sample id from a file path.
    Returns the basename without common sequencing extensions.
    """
    base = os.path.basename(path_str)
    # Fallback: strip extensions
    stem = base
    for ext in ('.fastq.gz', '.fq.gz', '.fastq', '.fq',
                '.fasta.gz', '.fasta', '.fa.gz', '.fa',
                '.gz', '.tsv', '.csv', '.txt'):
        if stem.endswith(ext):
            stem = stem[: -len(ext)]
    if (('.filtered' in path_str) or
        ('.merged' in path_str) or
        ('.trimmed' in path_str)
        ):
        stem = re.sub(r'(\.filtered|\.merged|\.trimmed)$', '', stem)
    else:
        stem = re.sub(r'(_R[12]|_[12])?(_001)?$', '', stem)
    stem = re.sub(r'(-)$', '_', stem)

    return stem


def safe_int(x) -> int:
    try:
        return int(x)
    except Exception:
        return 0


# =========================
# I/O readers (compute mode)
# =========================
def read_metadata(path: Path, samp_col: str, group_col: str,
                  keep_groups: Optional[Sequence[str]]) -> pd.DataFrame:
    df = pd.read_csv(path, sep='\t', header=0)
    if keep_groups:
        df = df[df[group_col].isin(keep_groups)].copy()
    # Make sure sample ids are strings
    df[samp_col] = df[samp_col].astype(str)
    return df


def load_sample_manifest(path: Path) -> Dict[str, str]:
    """
    Build a lookup from FASTQ file path (or basename) to sample ID.
    Manifest columns: sample_id, fastq_r1, fastq_r2 (no header).
    """
    df = pd.read_csv(path, sep='\t', header=None, names=['sample_id', 'r1', 'r2'])
    mapping: Dict[str, str] = {}
    for _, row in df.iterrows():
        sample_id = str(row['sample_id']).strip()
        if not sample_id:
            continue
        for col in ('r1', 'r2'):
            fastq_path = str(row[col]).strip()
            if not fastq_path or fastq_path.lower() == 'nan':
                continue
            candidates = {
                fastq_path,
                os.path.basename(fastq_path),
            }
            try:
                candidates.add(str(Path(fastq_path).resolve()))
            except Exception:
                pass
            for cand in candidates:
                if cand in mapping and mapping[cand] != sample_id:
                    raise ValueError(
                        f"FASTQ '{cand}' maps to multiple sample IDs ({mapping[cand]} vs {sample_id})"
                    )
                mapping[cand] = sample_id
    if not mapping:
        raise ValueError(f"No FASTQ entries were parsed from manifest: {path}")
    return mapping


def read_fastq_stats(path: Path, samp_col: str,
                     manifest_map: Optional[Dict[str, str]] = None) -> pd.DataFrame:
    """
    Expects columns: file, num_seqs
    Collapses replicates by sample ID via groupby+sum.
    """
    df = pd.read_csv(path, sep='\t', header=0)
    if 'file' not in df or 'num_seqs' not in df:
        raise ValueError(f"{path} must contain columns: file, num_seqs")
    stats_dir = path.parent

    def lookup_sample(file_path: str) -> str:
        if manifest_map:
            candidates = [
                file_path,
                os.path.basename(file_path),
                extract_sample_id_from_path(file_path),
            ]
            rel_path = (stats_dir / file_path)
            candidates.append(str(rel_path))
            candidates.append(os.path.basename(rel_path))
            try:
                candidates.append(str(rel_path.resolve()))
            except Exception:
                pass
            for cand in candidates:
                if cand in manifest_map:
                    return manifest_map[cand]
            print(candidates)
            print(manifest_map)
            raise ValueError(f"File '{file_path}' not found in manifest")
        return extract_sample_id_from_path(file_path)

    df[samp_col] = df['file'].apply(lookup_sample)
    out = df.groupby(samp_col, as_index=False)['num_seqs'].sum()
    return out


def read_asv_matrix(path: Path, samp_col: str) -> pd.DataFrame:
    """
    Input: wide matrix (rows=ASVs, columns=samples), counts.
    Returns long: [ASV_ID, samp_col, count] with count>0
    """
    df = pd.read_csv(path, sep='\t', header=0, index_col=0)
    long_df = df.stack().reset_index()
    long_df.columns = ['ASV_ID', 'sample_raw', 'count']
    long_df = long_df[long_df['count'] > 0].copy()
    long_df[samp_col] = long_df['sample_raw'].astype(str)
    long_df.drop(columns=['sample_raw'], inplace=True)
    return long_df


def group_counts_by_group(long_counts: pd.DataFrame, metadata: pd.DataFrame,
                          samp_col: str, group_col: str) -> pd.DataFrame:
    """
    Merge counts with metadata and sum by the chosen grouping column.
    Replicates with the same sample ID and group are naturally summed.
    """
    merged = long_counts.merge(metadata[[samp_col, group_col]], on=samp_col, how='inner')
    merged[group_col] = merged[group_col].astype(str)
    grp = merged.groupby(group_col, as_index=False)['count'].sum()
    grp.rename(columns={'count': 'num_reads'}, inplace=True)
    return grp


# =========================
# Sankey construction
# =========================
def write_fallback_svg(output_svg: Path, title: str, steps: List[str], counts: List[int],
                       lmp_in: Dict[str, int], lmp_out: Dict[str, int],
                       palette: Dict[str, str], labeled: bool) -> None:
    """
    Write a compact static SVG summary when Plotly/Kaleido export is unavailable.
    This keeps publication contracts and reports complete on servers without Chrome.
    """
    width, height = 1200, 760
    margin_x = 80
    top = 110
    row_h = 34
    text_color = "#222222"

    def color_for(key: str, fallback: str = "#555555") -> str:
        value = palette.get(key, fallback) or fallback
        return value if value.startswith("#") or value.isalpha() else fallback

    elements = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="white"/>',
        f'<text x="{margin_x}" y="48" font-family="Arial, sans-serif" font-size="26" font-weight="700" fill="{text_color}">{escape(title)}</text>',
        f'<text x="{margin_x}" y="78" font-family="Arial, sans-serif" font-size="14" fill="#666666">Static fallback export; interactive Sankey is available in the matching HTML file.</text>',
    ]

    columns = [
        ("Input groups", lmp_in, margin_x),
        ("Processing steps", dict(zip(steps, counts)), 440),
        ("Output groups", lmp_out, 820),
    ]
    max_value = max([1] + [int(v) for _, values, _ in columns for v in values.values()])

    for heading, values, x in columns:
        elements.append(
            f'<text x="{x}" y="{top - 28}" font-family="Arial, sans-serif" font-size="17" font-weight="700" fill="{text_color}">{escape(heading)}</text>'
        )
        for idx, (name, value) in enumerate(values.items()):
            y = top + idx * row_h
            bar_w = max(8, int(260 * (int(value) / max_value)))
            fill = color_for(name, "#444444" if heading == "Processing steps" else "#999999")
            label = f"{name}: {int(value):,}" if labeled else f"{int(value):,}"
            elements.extend([
                f'<rect x="{x}" y="{y - 18}" width="{bar_w}" height="22" rx="2" fill="{fill}" opacity="0.9"/>',
                f'<text x="{x}" y="{y + 20}" font-family="Arial, sans-serif" font-size="12" fill="{text_color}">{escape(label)}</text>',
            ])

    # Simple flow guide so the fallback still reads as a left-to-right process.
    for x1, x2 in ((350, 420), (730, 800)):
        elements.extend([
            f'<line x1="{x1}" y1="360" x2="{x2}" y2="360" stroke="#777777" stroke-width="3"/>',
            f'<polygon points="{x2},360 {x2 - 12},353 {x2 - 12},367" fill="#777777"/>',
        ])

    elements.append("</svg>")
    output_svg.write_text("\n".join(elements) + "\n", encoding="utf-8")


def build_sankey(steps: List[str], counts: List[int],
                 lmp_in: Dict[str, int], lmp_out: Dict[str, int],
                 palette: Dict[str, str], title: str,
                 output_html: Path, labeled: bool,
                 arrangement: str = "snap") -> None:
    """
    Build and save a Plotly HTML sankey.
    """
    # Helper to safely get color with black as fallback
    def get_color(key: str) -> str:
        color = palette.get(key, "black")
        return color if color else "black"

    nodes: List[Dict[str, str]] = []
    links: List[Dict[str, int]] = []
    node_idx: Dict[Tuple[str, str], int] = {}
    link_colors: List[str] = []
    node_x: List[float] = []
    node_y: List[float] = []

    def even_positions(n: int, low: float = 0.06, high: float = 0.94) -> List[float]:
        """
        Evenly space node centers on [low, high].
        Using inner margins prevents edge crowding and makes snap layout look balanced.
        """
        if n <= 0:
            return []
        if n == 1:
            return [0.5]
        span = high - low
        return [low + (i * span / (n - 1)) for i in range(n)]

    in_y = even_positions(len(lmp_in))
    out_y = even_positions(len(lmp_out))

    # Input-type nodes (left side, x=0.01)
    n_inputs = len(lmp_in)
    for i, (k, v) in enumerate(lmp_in.items()):
        nodes.append({"label": f"{k} ({v})" if labeled else "", "color": get_color(k)})
        node_idx[(k, "in")] = len(nodes) - 1
        node_x.append(0.01)
        node_y.append(in_y[i])

    # Process nodes (middle, evenly spaced)
    n_steps = len(steps)
    for i, (step, cnt) in enumerate(zip(steps, counts)):
        nodes.append({"label": f"{step}<br>({cnt})" if labeled else "", "color": "black"})
        node_idx[(step, "proc")] = len(nodes) - 1
        node_x.append(0.2 + (i / max(n_steps - 1, 1)) * 0.6)  # Spread from 0.2 to 0.8
        node_y.append(0.5)  # Center vertically

    # Output-type nodes (right side, x=0.99)
    n_outputs = len(lmp_out)
    for i, (k, v) in enumerate(lmp_out.items()):
        nodes.append({"label": f"{k} ({v})" if labeled else "", "color": get_color(k)})
        node_idx[(k, "out")] = len(nodes) - 1
        node_x.append(0.99)
        node_y.append(out_y[i])

    # Links: input -> first step
    first_step = steps[0]
    for k, v in lmp_in.items():
        links.append({
            "source": node_idx[(k, "in")],
            "target": node_idx[(first_step, "proc")],
            "value": v
        })
        link_colors.append("grey")

    # Links: step -> next step (+ loss nodes)
    for i in range(len(steps) - 1):
        s, t = steps[i], steps[i + 1]
        # main flow to next step
        links.append({
            "source": node_idx[(s, "proc")],
            "target": node_idx[(t, "proc")],
            "value": counts[i + 1]
        })
        link_colors.append("grey")

        # loss from this step
        if counts[i] > counts[i + 1]:
            loss_val = counts[i] - counts[i + 1]
            loss_label = f"{loss_val} removed" if labeled else ""
            nodes.append({"label": loss_label, "color": "lightgrey"})
            loss_idx = len(nodes) - 1
            node_x.append(0.2 + (i / max(n_steps - 1, 1)) * 0.6 + 0.05)  # Slightly offset
            node_y.append(0.9)  # Position loss nodes at bottom
            links.append({
                "source": node_idx[(s, "proc")],
                "target": loss_idx,
                "value": loss_val
            })
            link_colors.append("lightgrey")

    # Links: last step -> outputs
    last_step = steps[-1]
    for k, v in lmp_out.items():
        links.append({
            "source": node_idx[(last_step, "proc")],
            "target": node_idx[(k, "out")],
            "value": v
        })
        link_colors.append("grey")

    fig = go.Figure(data=[go.Sankey(
        arrangement=arrangement,
        node=dict(
            pad=10,
            thickness=20,
            line=dict(color="black", width=0.5),
            label=[n["label"] for n in nodes],
            color=[n["color"] for n in nodes],
            x=node_x,  # Explicit x positions
            y=node_y,  # Explicit y positions
        ),
        link=dict(
            source=[l["source"] for l in links],
            target=[l["target"] for l in links],
            value=[l["value"] for l in links],
            color=link_colors,
        ),
    )])
    fig.update_layout(title_text=title, font_size=12)
    output_html.parent.mkdir(parents=True, exist_ok=True)
    fig.write_html(str(output_html))
    print(f"✔ Sankey saved: {output_html}")
    try:
        output_svg = output_html.with_suffix(".svg")
        fig.write_image(str(output_svg))
        print(f"✔ Sankey saved: {output_svg}")
    except Exception as exc:
        write_fallback_svg(output_svg, title, steps, counts, lmp_in, lmp_out, palette, labeled)
        print(
            f"[WARN] Could not export Sankey SVG for {output_html}: {exc}. "
            f"Wrote static fallback SVG instead: {output_svg}",
            file=sys.stderr,
        )


# =========================
# CLI
# =========================
def get_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Generate Sankey diagrams for data loss/flow.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # --- Compute mode inputs
    io = p.add_argument_group("Compute Mode Inputs")
    io.add_argument("--data-dir", type=Path, help="Project root (used to resolve defaults)")
    io.add_argument("--sub-dir", default="spark_combined_output", help="Subdir under data-dir for outputs/stats")
    io.add_argument("--metadata", type=Path, help="TSV with sample metadata")
    io.add_argument("--sample-manifest", type=Path,
                    help="TSV with columns: sample_id, fastq_r1, fastq_r2")
    io.add_argument("--samp-col", default="lmp_id", help="Sample column name in metadata")
    io.add_argument("--group1-col", default="group1", help="Grouping column in metadata")
    io.add_argument("--color-col", default="Color", help="Color column in metadata")
    io.add_argument("--keep-types", default="",
                    help="Comma-separated list; if empty, keep all types")
    io.add_argument("--all-samples", action="store_true",
                    help="Use all metadata samples as the sample universe instead of only samples present in the microbial ASV table.")

    io.add_argument("--fastq-stats", default="stats/fastq_stats.tsv",
                    help="Path (relative to sub-dir or absolute) to raw fastq stats TSV")

    io.add_argument("--filtered-stats", default="stats/filtered_fastqs.tsv",
                    help="Path to filtered fastq stats TSV")

    io.add_argument("--asv-raw", default="ASVs/ASV_counts.tsv", help="Wide ASV counts matrix")

    io.add_argument("--asv-decon", default="ASVs/ASV_target.decon.tsv", help="Wide ASV after decontamination")
    io.add_argument("--asv-micro", default="ASVs/ASV_target.micro.tsv", help="Wide ASV microbial (finished)")

    # --- Appearance / output
    out = p.add_argument_group("Output")
    out.add_argument("--title", default="Data Loss Flow", help="Plot title")
    out.add_argument("--output-prefix", default="metadata/data_loss_sankey",
                     help="Output prefix ('.html' appended automatically)")
    out.add_argument("--make-labeled", action="store_true", help="Create labeled-node HTML")
    out.add_argument("--make-unlabeled", action="store_true", help="Create unlabeled-node HTML")
    out.add_argument(
        "--arrangement",
        default="snap",
        choices=["snap", "perpendicular", "freeform", "fixed"],
        help="Plotly sankey node arrangement mode (use 'freeform' for draggable nodes).",
    )
    out.add_argument(
        "--vertical-order",
        default="",
        help="Comma-separated group order from top to bottom for input/output Sankey nodes. Unlisted groups are appended.",
    )

    # --- Misc
    p.add_argument("--verbose", action="store_true", help="Verbose logs")

    return p


def main():
    args = get_parser().parse_args()

    # ---- Compute mode ----
    if not args.data_dir:
        raise SystemExit("--data-dir is required in compute mode")
    data_dir: Path = args.data_dir

    # Resolve default paths if relative
    def resolve(rel_or_abs: str) -> Path:
        p = Path(rel_or_abs)
        if p.is_absolute():
            return p
        return data_dir / args.sub_dir / rel_or_abs

    metadata_path = args.metadata or (data_dir / "ref_db" / "spark_metadata.tsv")
    manifest_path = args.sample_manifest or (data_dir / "ref_db" / "sample_manifest.tsv")
    fastq_stats_path = resolve(args.fastq_stats)
    filtered_stats_path = resolve(args.filtered_stats)
    asv_raw_path = resolve(args.asv_raw)
    asv_decon_path = resolve(args.asv_decon)
    asv_micro_path = resolve(args.asv_micro)

    keep_types = [t.strip() for t in args.keep_types.split(',')] if args.keep_types.strip() else None

    if args.verbose:
        print(f"[i] Metadata: {metadata_path}")
        print(f"[i] Sample manifest: {manifest_path}")
        print(f"[i] Raw fastq stats: {fastq_stats_path}")
        print(f"[i] Filtered stats: {filtered_stats_path}")
        print(f"[i] ASV raw: {asv_raw_path}")
        print(f"[i] ASV decon: {asv_decon_path}")
        print(f"[i] ASV micro: {asv_micro_path}")

    meta = read_metadata(metadata_path, args.samp_col, args.group1_col, keep_types)
    meta[args.group1_col] = meta[args.group1_col].astype(str)

    # ASV matrices -> long -> merge -> sum
    # Use consistent sample ID parsing across all ASV matrices
    asv_micro_long = read_asv_matrix(
        asv_micro_path,
        args.samp_col,
    )

    if args.all_samples:
        sample_list = meta[args.samp_col].astype(str).unique().tolist()
    else:
        sample_list = asv_micro_long[args.samp_col].unique().tolist()

    asv_raw_long = read_asv_matrix(
        asv_raw_path,
        args.samp_col,
    )
    asv_raw_long = asv_raw_long[asv_raw_long[args.samp_col].isin(sample_list)].copy()

    asv_decon_long = read_asv_matrix(
        asv_decon_path,
        args.samp_col,
    )
    asv_decon_long = asv_decon_long[asv_decon_long[args.samp_col].isin(sample_list)].copy()

    # Restrict metadata to the active sample universe
    meta = meta[meta[args.samp_col].isin(sample_list)].copy()

    manifest_map = load_sample_manifest(manifest_path)
    filter_map = {v: v for k, v in manifest_map.items()}

    # Raw reads (pairs): sum num_seqs across files, then /2, with replicates collapsed
    raw_df = read_fastq_stats(
        fastq_stats_path,
        args.samp_col,
        manifest_map,
    )
    raw_df = raw_df[raw_df[args.samp_col].isin(sample_list)].copy()

    # Filtered reads (already single-end counts in your script), replicates collapsed
    filt_df = read_fastq_stats(
        filtered_stats_path,
        args.samp_col,
        filter_map,
    )
    filt_df = filt_df[filt_df[args.samp_col].isin(sample_list)].copy()

    # Build palette from metadata: grouping column -> color, with string keys
    palette = {str(t): str(c) for t, c in zip(meta[args.group1_col], meta[args.color_col])}

    # Sort palette deterministically (numeric if possible, else lexical)
    try:
        palette = dict(sorted(palette.items(), key=lambda x: float(x[0])))
    except (ValueError, TypeError):
        palette = dict(sorted(palette.items()))

    # Sum by group — this implicitly respects keep_types and drops samples without metadata
    raw_by_type = raw_df.merge(meta[[args.samp_col, args.group1_col]],
                               on=args.samp_col, how='inner') \
                        .groupby(args.group1_col, as_index=False)['num_seqs'].sum()
    raw_by_type['num_reads'] = (raw_by_type['num_seqs'] // 2).astype(int)
    
    filt_by_type = filt_df.merge(meta[[args.samp_col, args.group1_col]],
                                 on=args.samp_col, how='inner') \
                          .groupby(args.group1_col, as_index=False)['num_seqs'].sum()
    filt_by_type['num_reads'] = filt_by_type['num_seqs'].astype(int)
    
    # Override step totals so node labels use exactly the subset represented in the ribbons
    raw_reads_total = int(raw_by_type['num_reads'].sum())
    filt_reads_total = int(filt_by_type['num_reads'].sum())

    asv_raw_by_type = group_counts_by_group(asv_raw_long, meta, args.samp_col, args.group1_col)
    asv_decon_by_type = group_counts_by_group(asv_decon_long, meta, args.samp_col, args.group1_col)
    asv_micro_by_type = group_counts_by_group(asv_micro_long, meta, args.samp_col, args.group1_col)

    # Totals for remaining steps
    asv_raw_reads = int(asv_raw_by_type['num_reads'].sum())
    asv_decon_reads = int(asv_decon_by_type['num_reads'].sum())
    asv_micro_reads = int(asv_micro_by_type['num_reads'].sum())

    # Steps & counts (used for node labels and loss computation)
    steps = [
        'Quality Control',
        'Error Correction',
        'Decontamination',
        'Off-Target Filtering',
        'Finished Data'
    ]
    counts = [
        raw_reads_total,
        filt_reads_total,
        asv_raw_reads,
        asv_decon_reads,
        asv_micro_reads
    ]

    vertical_order = [t.strip() for t in args.vertical_order.split(',') if t.strip()]
    if keep_types:
        types = keep_types
    else:
        unique_types = [str(t) for t in meta[args.group1_col].dropna().unique()]
        # Sort numerically if possible, otherwise alphabetically
        try:
            types = sorted(unique_types, key=lambda x: float(x))
        except (ValueError, TypeError):
            types = sorted(unique_types)
    if vertical_order:
        present = {str(t) for t in types}
        ordered = [t for t in vertical_order if t in present]
        ordered.extend([str(t) for t in types if str(t) not in set(ordered)])
        types = ordered

    # Input and output dicts for sankey ends (string keys to match palette)
    lmp_in = {
        str(t): int(raw_by_type.loc[raw_by_type[args.group1_col] == t, 'num_reads'].sum())
        for t in types
    }
    lmp_out = {
        str(t): int(asv_micro_by_type.loc[asv_micro_by_type[args.group1_col] == t, 'num_reads'].sum())
        for t in types
    }

    # Preserve explicit top-to-bottom order when requested; otherwise sort deterministically.
    if not vertical_order:
        try:
            lmp_in = dict(sorted(lmp_in.items(), key=lambda x: float(x[0])))
            lmp_out = dict(sorted(lmp_out.items(), key=lambda x: float(x[0])))
        except (ValueError, TypeError):
            lmp_in = dict(sorted(lmp_in.items()))
            lmp_out = dict(sorted(lmp_out.items()))

    if args.verbose:
        print("[i] Steps:")
        for s, c in zip(steps, counts):
            print(f"  - {s}: {c}")
        print("[i] Inputs by type:", lmp_in)
        print("[i] Outputs by type:", lmp_out)

    # Outputs
    out_pref = (args.data_dir / args.sub_dir / args.output_prefix) if args.data_dir else Path(args.output_prefix)
    # default: generate both if none chosen
    if not args.make_labeled and not args.make_unlabeled:
        args.make_labeled = True
        args.make_unlabeled = True

    if args.make_labeled:
        build_sankey(
            steps, counts, lmp_in, lmp_out, palette,
            args.title, out_pref.with_suffix(".label.html"), True,
            arrangement=args.arrangement
        )
    if args.make_unlabeled:
        build_sankey(
            steps, counts, lmp_in, lmp_out, palette,
            args.title, out_pref.with_suffix(".html"), False,
            arrangement=args.arrangement
        )


if __name__ == "__main__":
    main()

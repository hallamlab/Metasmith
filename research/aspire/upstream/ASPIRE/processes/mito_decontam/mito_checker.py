#!/usr/bin/env python3
"""
asv_nontarget_summary.py
Refactor of the ASV non-target/host summary & plots with a robust CLI.

Inputs (all TSVs may be .tsv or .tsv.gz):
  --mitomaster-file : Combined MITOMASTER output (first two columns: Sequence_ID, haplo).
  --mito-blast      : BLAST6 of ASVs vs mito DB.
  --silva-tax       : Full-length taxonomy table with Feature ID / Taxon / Consensus.
  --biof-file       : BLAST6 of ASVs vs “BioFactorial” (host) contaminants DB.

Outputs (to --output-dir):
  <prefix>.master.tsv
  <prefix>.summary_cumulative.tsv
  <prefix>.summary_host.tsv
  <prefix>.summary_counts.tsv
  <prefix>_non_target_cumulative.<ext>
  <prefix>_microbial_vs_host_cumulative.<ext>
  <prefix>_non_target_bar.<ext>

Example:
  python asv_nontarget_summary.py \
    --mitomaster-file mitomaster_combined.tsv \
    --mito-blast mito_ncbi.blast6.tsv \
    --silva-tax ASV_SILVA_tax.full-length.vsearch.tsv \
    --biof-file ssu_pipeline_contaminants.blast6.tsv \
    --output-dir out --prefix nontarget \
    --min-pident 97 --min-percov 51 --formats svg,pdf
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import List, Tuple, Sequence, Dict

import pandas as pd


BLAST6_COLUMNS = [
    'qseqid', 'sseqid', 'pident', 'length', 'qlen', 'mismatch',
    'gapopen', 'qstart', 'qend', 'sstart', 'send', 'evalue', 'bitscore'
]


def parse_figsize(s: str) -> Tuple[float, float]:
    """Parse '10x6' into (10.0, 6.0)."""
    try:
        w, h = s.lower().split('x')
        return float(w), float(h)
    except Exception:
        raise argparse.ArgumentTypeError("figsize must look like '10x6' (width x height inches)")


def read_possibly_gz(path: Path, **kwargs) -> pd.DataFrame:
    """Read CSV/TSV possibly gzipped based on extension."""
    if str(path).endswith(".gz"):
        return pd.read_csv(path, compression="gzip", **kwargs)
    return pd.read_csv(path, **kwargs)


def sanitize_seqid_series(s: pd.Series) -> pd.Series:
    """Trim anything after first ';' like 'id;size=...'."""
    return s.astype(str).str.split(';').str[0]


def read_mitomaster_table(path: Path) -> pd.DataFrame:
    """
    MITOMASTER combined TSV; we only need the first two columns (Sequence_ID, haplo).
    The raw file may have blank lines; original script skipped header via lines_list[1:].
    """
    rows: List[List[str]] = []
    with path.open('r') as fh:
        for line in fh:
            if line.strip():
                rows.append(line.rstrip('\n').split('\t')[:2])

    if not rows:
        return pd.DataFrame(columns=['Sequence_ID', 'haplo'])

    # If first row looks like a header, drop it
    first = [x.lower() for x in rows[0]]
    if any(tok in first[0] for tok in ['sequence', 'seq']) or 'haplo' in first[-1]:
        data = rows[1:]
    else:
        data = rows

    df = pd.DataFrame(data, columns=['Sequence_ID', 'haplo'])
    df['Sequence_ID'] = sanitize_seqid_series(df['Sequence_ID'])
    df.drop_duplicates(subset='Sequence_ID', inplace=True)
    return df


def read_blast6_filtered(path: Path, min_pident: float, min_percov: float) -> pd.DataFrame:
    df = read_possibly_gz(path, sep='\t', header=None, names=BLAST6_COLUMNS)
    # Guard division; qlen can be zero in malformed inputs
    df = df[df['qlen'] > 0].copy()
    df['percov'] = (df['length'] / df['qlen']) * 100.0
    df = df[(df['pident'] >= min_pident) & (df['percov'] >= min_percov)].copy()
    df['qseqid'] = sanitize_seqid_series(df['qseqid'])
    df.drop_duplicates(subset='qseqid', inplace=True)
    return df


def read_silva_taxonomy(
    path: Path,
    feature_col: str,
    taxon_col: str,
    consensus_col: str,
) -> pd.DataFrame:
    df = read_possibly_gz(path, sep='\t', header=0)
    if feature_col not in df or taxon_col not in df or consensus_col not in df:
        missing = [c for c in (feature_col, taxon_col, consensus_col) if c not in df]
        raise ValueError(f"Missing required columns in SILVA taxonomy TSV: {missing}")
    out = pd.DataFrame({
        'Sequence_ID': sanitize_seqid_series(df[feature_col]),
        'FL_Taxonomy': df[taxon_col].astype(str),
        'FL_Confidence': df[consensus_col],
    })
    out.drop_duplicates(subset='Sequence_ID', inplace=True)
    return out


def build_master_table(
    silva_df: pd.DataFrame,
    mito_blast_df: pd.DataFrame,
    biof_blast_df: pd.DataFrame,
    mitomaster_df: pd.DataFrame,
    mitochondria_substring: str,
) -> pd.DataFrame:
    # Taxonomy classifiers can omit non-bacterial and otherwise unassigned ASVs.
    # Retain IDs detected by any non-target method so those hits are not lost
    # before the pass/fail flags are assembled.
    sequence_ids = pd.concat([
        silva_df['Sequence_ID'],
        mito_blast_df['qseqid'],
        biof_blast_df['qseqid'],
        mitomaster_df['Sequence_ID'],
    ], ignore_index=True).dropna().drop_duplicates()
    seqs = pd.DataFrame({'Sequence_ID': sequence_ids})
    # Fast membership via sets
    in_biof = set(biof_blast_df['qseqid'])
    in_mito_blast = set(mito_blast_df['qseqid'])
    in_mitomaster = set(mitomaster_df['Sequence_ID'])

    # Binary pass/fail flags (1 = microbial pass, 0 = flagged as non-target/host)
    # Keep parity with original script
    seqs['BioFactorial'] = [0 if sid in in_biof else 1 for sid in seqs['Sequence_ID']]
    # NB classifier substring check
    silva_map = silva_df.set_index('Sequence_ID')['FL_Taxonomy']
    seqs['Qiime_NB_FULL'] = [0 if mitochondria_substring.lower() in str(silva_map.get(sid, '')).lower() else 1
                             for sid in seqs['Sequence_ID']]
    seqs['MITOMASTER'] = [0 if sid in in_mitomaster else 1 for sid in seqs['Sequence_ID']]
    seqs['BLAST_mito'] = [0 if sid in in_mito_blast else 1 for sid in seqs['Sequence_ID']]

    # Annotations
    sub_biof = biof_blast_df[['qseqid', 'sseqid', 'pident']].copy()
    sub_biof.columns = ['Sequence_ID', 'BF_ID', 'BF_pid']
    sub_mibl = mito_blast_df[['qseqid', 'sseqid', 'pident']].copy()
    sub_mibl.columns = ['Sequence_ID', 'MI_Accession', 'MI_pid']
    silva_keep = silva_df[['Sequence_ID', 'FL_Taxonomy', 'FL_Confidence']].copy()

    master = seqs.merge(sub_biof, on='Sequence_ID', how='left') \
                 .merge(silva_keep, on='Sequence_ID', how='left') \
                 .merge(sub_mibl, on='Sequence_ID', how='left')
    master.drop_duplicates(subset='Sequence_ID', inplace=True)
    return master


def cumulative_microbe_vs_nontarget(master: pd.DataFrame, steps: Sequence[str]) -> pd.DataFrame:
    """Cumulative contaminant flags across steps."""
    contaminant_flags = pd.Series(False, index=master.index)
    rows = []
    for step in steps:
        new_contaminants = (master[step] == 0) & (~contaminant_flags)
        contaminant_flags |= new_contaminants
        non_target = int(contaminant_flags.sum())
        microbes = int(len(master) - non_target)
        rows.append({'Method': step, 'Non-Target': non_target, 'Microbes': microbes})
    return pd.DataFrame(rows)


def cumulative_microbial_vs_host(master: pd.DataFrame, steps: Sequence[str], host_first_step: str) -> pd.DataFrame:
    """
    Replicates original logic:
      - Use cumulative flags but subtract the host-only count at the first step
        so remaining 'Host' after that step counts incremental host-like calls.
      - Exclude the first host step from the final plot/table.
    """
    contaminant_flags = pd.Series(False, index=master.index)
    rows = []
    host_baseline = 0
    for step in steps:
        new_contaminants = (master[step] == 0) & (~contaminant_flags)
        contaminant_flags |= new_contaminants
        non_target = int(contaminant_flags.sum())
        if step == host_first_step:
            host_baseline = non_target
            # record baseline, but we will drop this step from final result
            rows.append({'Method': step, 'Host': host_baseline, 'Microbial': int(len(master) - host_baseline)})
        else:
            host_incr = int(non_target - host_baseline)
            microbial = int(len(master) - non_target)
            rows.append({'Method': step, 'Host': host_incr, 'Microbial': microbial})
    # drop the first host step row (to match original figure)
    return pd.DataFrame(rows)[lambda d: d['Method'] != host_first_step]


def per_method_counts(master: pd.DataFrame, steps: Sequence[str]) -> pd.DataFrame:
    """Non-cumulative per-step counts (Non-Target vs Microbes)."""
    rows = []
    for step in steps:
        contaminants = (master[step] == 0)
        non_target = int(contaminants.sum())
        microbes = int(len(master) - non_target)
        rows.append({'Method': step, 'Non-Target': non_target, 'Microbes': microbes})
    return pd.DataFrame(rows)


def save_df(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, sep='\t', index=False)


def save_plot(fig: plt.Figure, out_base: Path, exts: Sequence[str], dpi: int) -> None:
    import matplotlib.pyplot as plt

    out_base.parent.mkdir(parents=True, exist_ok=True)
    for ext in exts:
        fig.savefig(out_base.with_suffix(f".{ext}"), dpi=dpi, bbox_inches='tight')
    plt.close(fig)


def plot_cumulative_microbe_vs_nontarget(df: pd.DataFrame, figsize: Tuple[float, float], style: str, title: str) -> plt.Figure:
    import matplotlib.pyplot as plt
    import seaborn as sns

    sns.set(style=style) # type: ignore
    fig = plt.figure(figsize=figsize)
    ax = sns.lineplot(
        data=df.melt(id_vars='Method', value_vars=['Non-Target', 'Microbes'],
                     var_name='Classification', value_name='Count'),
        x='Method', y='Count', hue='Classification', marker='o', linewidth=2
    )
    # Label points
    melted = df.melt(id_vars='Method', value_vars=['Non-Target', 'Microbes'],
                     var_name='Classification', value_name='Count').reset_index(drop=True)
    steps = list(df['Method'])
    for i, row in melted.iterrows():
        ax.text(x=steps.index(row['Method']), y=row['Count'] + 0.01 * max(df[['Non-Target','Microbes']].to_numpy().ravel()),
                s=str(int(row['Count'])), ha='center', va='bottom', fontsize=9)
    ax.set_title(title)
    ax.set_ylabel("Number of ASVs")
    ax.set_xlabel("Method")
    plt.xticks(rotation=45, ha='right')
    plt.legend(title='Classification', bbox_to_anchor=(1.05, 1), loc='upper left', frameon=False)
    plt.tight_layout()
    return fig


def plot_cumulative_microbial_vs_host(df: pd.DataFrame, figsize: Tuple[float, float], style: str, title: str) -> plt.Figure:
    import matplotlib.pyplot as plt
    import seaborn as sns

    sns.set(style=style) # type: ignore
    fig = plt.figure(figsize=figsize)
    ax = sns.lineplot(
        data=df.melt(id_vars='Method', value_vars=['Host', 'Microbial'],
                     var_name='Classification', value_name='Count'),
        x='Method', y='Count', hue='Classification', marker='o', linewidth=2
    )
    melted = df.melt(id_vars='Method', value_vars=['Host', 'Microbial'],
                     var_name='Classification', value_name='Count').reset_index(drop=True)
    steps = list(df['Method'])
    for i, row in melted.iterrows():
        ax.text(x=steps.index(row['Method']), y=row['Count'] + 0.01 * max(df[['Host','Microbial']].to_numpy().ravel()),
                s=str(int(row['Count'])), ha='center', va='bottom', fontsize=9)
    ax.set_title(title)
    ax.set_ylabel("Number of ASVs")
    ax.set_xlabel("Method")
    plt.xticks(rotation=45, ha='right')
    plt.legend(title='Classification', bbox_to_anchor=(1.05, 1), loc='upper left', frameon=False)
    plt.tight_layout()
    return fig


def plot_non_target_bar(df_counts: pd.DataFrame, figsize: Tuple[float, float], style: str, title: str) -> plt.Figure:
    import matplotlib.pyplot as plt
    import seaborn as sns

    sns.set(style=style) # type: ignore
    fig = plt.figure(figsize=figsize)
    # Use seaborn catplot returns FacetGrid; instead, draw barplot on our fig/ax
    ax = plt.gca()
    sub = df_counts[['Method', 'Non-Target']].rename(columns={'Non-Target': 'Count'})
    sns.barplot(data=sub, x='Method', y='Count', ax=ax)
    ax.set_title(title)
    ax.set_ylabel("Number of ASVs")
    ax.set_xlabel("Method")
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    return fig


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Summarize ASV non-target/host classifications and generate plots.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    io = p.add_argument_group("Inputs/Outputs")
    io.add_argument("--mitomaster-file", type=Path, required=True, help="Combined MITOMASTER TSV")
    io.add_argument("--mito-blast", type=Path, required=True, help="BLAST6 vs mitochondrial DB")
    io.add_argument("--silva-tax", type=Path, required=True, help="Full-length SILVA taxonomy TSV")
    io.add_argument("--biof-file", type=Path, required=True, help="BLAST6 vs BioFactorial contaminants DB")
    io.add_argument("--output-dir", type=Path, required=True, help="Directory to write outputs")
    io.add_argument("--prefix", default="nontarget", help="Output file prefix")
    io.add_argument("--formats", default="svg,pdf", help="Comma-separated image formats (e.g., svg,pdf,png)")
    io.add_argument("--overwrite", action="store_true", help="Overwrite existing outputs")

    filt = p.add_argument_group("Filters")
    filt.add_argument("--min-pident", type=float, default=97.0, help="Minimum percent identity")
    filt.add_argument("--min-percov", type=float, default=51.0, help="Minimum percent coverage")
    filt.add_argument("--mitochondria-substring", default="mitochondria",
                      help="Substring to flag mitochondrial hits in taxonomy")

    tax = p.add_argument_group("Taxonomy Columns")
    tax.add_argument("--feature-col", default="Feature ID", help="Column name for feature IDs")
    tax.add_argument("--taxon-col", default="Taxon", help="Column name for taxonomy strings")
    tax.add_argument("--consensus-col", default="Consensus", help="Column for NB confidence")

    run = p.add_argument_group("Run/Plots")
    run.add_argument("--steps", default="BioFactorial,Qiime_NB_FULL,MITOMASTER,BLAST_mito",
                     help="Comma-separated step order (must match master columns)")
    run.add_argument("--host-first-step", default="BioFactorial", help="Which step is the initial host screen")
    run.add_argument("--figsize", type=parse_figsize, default="10x6", help="Figure size WxH in inches")
    run.add_argument("--style", default="whitegrid", help="Seaborn style (e.g., whitegrid, darkgrid)")
    run.add_argument("--dpi", type=int, default=300, help="Image DPI")
    run.add_argument("--no-plots", action="store_true", help="Skip plot generation")

    return p.parse_args()


def main():
    args = parse_args()
    outdir: Path = args.output_dir
    outdir.mkdir(parents=True, exist_ok=True)
    exts = [e.strip().lstrip('.').lower() for e in args.formats.split(',') if e.strip()]

    # Read inputs
    mmfl_df = read_mitomaster_table(args.mitomaster_file)
    mibl_df = read_blast6_filtered(args.mito_blaste if hasattr(args, 'mito_blaste') else args.mito_blast,
                                   args.min_pident, args.min_percov)
    biof_df = read_blast6_filtered(args.biof_file, args.min_pident, args.min_percov)
    ggfl_df = read_silva_taxonomy(args.silva_tax, args.feature_col, args.taxon_col, args.consensus_col)

    # Master table
    master_df = build_master_table(
        silva_df=ggfl_df,
        mito_blast_df=mibl_df,
        biof_blast_df=biof_df,
        mitomaster_df=mmfl_df,
        mitochondria_substring=args.mitochondria_substring,
    )

    master_path = outdir / f"{args.prefix}.master.tsv"
    if master_path.exists() and not args.overwrite:
        raise FileExistsError(f"Refusing to overwrite existing file without --overwrite: {master_path}")
    save_df(master_df, master_path)

    # Summaries
    steps = [s.strip() for s in args.steps.split(',') if s.strip()]
    # Validate steps exist
    missing_steps = [s for s in steps if s not in master_df.columns]
    if missing_steps:
        raise ValueError(f"Steps not found in master table: {missing_steps}")

    cum_micro_vs_non = cumulative_microbe_vs_nontarget(master_df, steps)
    cum_host = cumulative_microbial_vs_host(master_df, steps, args.host_first_step)
    per_counts = per_method_counts(master_df, steps)

    save_df(cum_micro_vs_non, outdir / f"{args.prefix}.summary_cumulative.tsv")
    save_df(cum_host, outdir / f"{args.prefix}.summary_host.tsv")
    save_df(per_counts, outdir / f"{args.prefix}.summary_counts.tsv")

    VALID_STYLES = {'white', 'dark', 'whitegrid', 'darkgrid', 'ticks'}

    style = args.style.strip().lower()
    if style not in VALID_STYLES:
        raise ValueError(f"Invalid style '{style}'. Must be one of: {', '.join(VALID_STYLES)}")

    if not args.no_plots:
        # Plot 1: cumulative microbes vs non-target
        fig1 = plot_cumulative_microbe_vs_nontarget(
            cum_micro_vs_non, args.figsize, style, "Cumulative ASV Classification: Microbes vs Non-Target"
        )
        save_plot(fig1, outdir / f"{args.prefix}_non_target_cumulative", exts, args.dpi)

        # Plot 2: cumulative microbial vs host (excluding first host step)
        fig2 = plot_cumulative_microbial_vs_host(
            cum_host, args.figsize, style, "Cumulative ASV Classification: Microbial vs Host"
        )
        save_plot(fig2, outdir / f"{args.prefix}_microbial_vs_host_cumulative", exts, args.dpi)

        # Plot 3: Non-Target per method (bar)
        fig3 = plot_non_target_bar(
            per_counts, args.figsize, style, "Non-Target"
        )
        save_plot(fig3, outdir / f"{args.prefix}_non_target_bar", exts, args.dpi)

    print(f"📝 Master: {master_path}")
    print(f"📈 Summaries & plots written in: {outdir}")


if __name__ == "__main__":
    main()

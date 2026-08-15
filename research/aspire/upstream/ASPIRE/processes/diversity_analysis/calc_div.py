#!/usr/bin/env python3
"""
diversity_metrics.py
Compute alpha (Shannon) and beta (Bray–Curtis, Jaccard) diversity from ASV tables.

Assumptions
-----------
- ASV tables are TSVs.
- You can specify whether samples are in rows or columns.
- Script outputs tidy TSV matrices for distances and a 2-col TSV for Shannon.

Examples
--------
# Typical usage (samples are columns; ASVs are rows)
python diversity_metrics.py \
  --micro-table spark_combined_output/ASVs/ASV_final.micro.tsv \
  --mito-table  spark_combined_output/mito/ASVs/ASV_final.mito.tsv \
  --samples-on columns \
  --outdir spark_combined_output/diversity \
  --mito-outdir spark_combined_output/mito/diversity

# Only micro, custom presence threshold for Jaccard, skip Bray
python diversity_metrics.py \
  --micro-table ASV_final.micro.tsv \
  --samples-on columns \
  --presence-threshold 1 \
  --skip-bray
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys
import numpy as np
import pandas as pd
from scipy.spatial.distance import pdist, squareform
from skbio.diversity.alpha import shannon as skbio_shannon


# ------------------------- I/O & utilities -------------------------
def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def load_counts(
    path: Path,
    samples_on: str = "columns",
    index_col: int | str | None = 0,
    drop_empty_samples: bool = True,
    drop_empty_features: bool = True,
) -> pd.DataFrame:
    """
    Load an ASV table and return a DataFrame with SAMPLES as ROWS and FEATURES as COLUMNS.
    """
    if not path.exists():
        raise FileNotFoundError(f"Counts table not found: {path}")
    df = pd.read_csv(path, sep="\t", index_col=index_col)
    # Normalize orientation → rows = samples
    if samples_on.lower() in ("columns", "cols"):
        df = df.T
    elif samples_on.lower() not in ("rows",):
        raise ValueError("--samples-on must be 'rows' or 'columns'")

    # Drop all-zero rows/cols if requested
    if drop_empty_features:
        df = df.loc[:, (df != 0).any(axis=0)]
    if drop_empty_samples:
        df = df.loc[(df != 0).any(axis=1)]
    if df.empty:
        raise ValueError(f"After filtering, table is empty: {path}")
    # Enforce numeric matrix
    df = df.apply(pd.to_numeric, errors="coerce").fillna(0)
    return df


def write_square_matrix(df: pd.DataFrame, out_path: Path) -> None:
    df.index.name = "sampleID"
    df.to_csv(out_path, sep="\t")


def shannon_per_sample(counts_df: pd.DataFrame) -> pd.DataFrame:
    vals = {s: float(skbio_shannon(counts_df.loc[s].values)) for s in counts_df.index}
    out = pd.DataFrame.from_dict(vals, orient="index", columns=["Shannon"])
    out.index.name = "sampleID"
    return out


def pairwise_distance(counts_df: pd.DataFrame, metric: str) -> pd.DataFrame:
    arr = counts_df.values
    d = pdist(arr, metric=metric)
    M = squareform(d)
    return pd.DataFrame(M, index=counts_df.index, columns=counts_df.index)


def jaccard_from_presence(counts_df: pd.DataFrame, threshold: float = 0.0) -> pd.DataFrame:
    presence = (counts_df > threshold).astype(int).values
    d = pdist(presence, metric="jaccard")
    M = squareform(d)
    return pd.DataFrame(M, index=counts_df.index, columns=counts_df.index)


# ------------------------------ CLI --------------------------------
def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        description="Compute alpha (Shannon) and beta (Bray–Curtis, Jaccard) diversity.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    ap.add_argument("--micro-table", type=Path, required=False, help="ASV table for microbiome.")
    ap.add_argument("--mito-table", type=Path, required=False, help="ASV table for mitochondrial subset.")
    ap.add_argument("--samples-on", choices=["rows", "columns"], default="columns",
                    help="Where samples live in the input tables.")
    ap.add_argument("--index-col", default=0,
                    help="Index column for TSVs (int or column name).")
    ap.add_argument("--outdir", type=Path, default=Path("diversity"),
                    help="Output directory for micro metrics.")
    ap.add_argument("--mito-outdir", type=Path, default=None,
                    help="Output directory for mito metrics (default: <outdir>/../mito/diversity if mito-table is given).")

    # Computation switches
    ap.add_argument("--skip-alpha", action="store_true", help="Skip Shannon alpha diversity.")
    ap.add_argument("--skip-bray", action="store_true", help="Skip Bray–Curtis distances.")
    ap.add_argument("--skip-jaccard", action="store_true", help="Skip Jaccard distances.")
    ap.add_argument("--presence-threshold", type=float, default=0.0,
                    help="Presence cutoff for Jaccard (values > threshold are present).")

    # Filtering
    ap.add_argument("--keep-empty-samples", action="store_true",
                    help="Do NOT drop all-zero samples.")
    ap.add_argument("--keep-empty-features", action="store_true",
                    help="Do NOT drop all-zero features (ASVs).")

    return ap


def resolve_dirs(outdir: Path, mito_outdir: Path | None, mito_table: Path | None) -> tuple[Path, Path | None]:
    micro_dir = outdir
    ensure_dir(micro_dir)
    mito_dir = None
    if mito_table is not None:
        if mito_outdir is not None:
            mito_dir = mito_outdir
        else:
            # default: sibling 'mito/diversity' next to micro outdir root if recognizable,
            # otherwise `<outdir>/mito`
            if "mito" in str(outdir).split(os.sep):
                mito_dir = outdir
            else:
                mito_dir = outdir.parent / "mito" / "diversity"
        ensure_dir(mito_dir)
    return micro_dir, mito_dir


def run_for_table(
    label: str,
    table_path: Path,
    out_dir: Path,
    samples_on: str,
    index_col,
    do_alpha: bool,
    do_bray: bool,
    do_jaccard: bool,
    j_thresh: float,
    drop_empty_samples: bool,
    drop_empty_features: bool,
) -> None:
    print(f"[{label}] Loading: {table_path}")
    counts = load_counts(
        table_path,
        samples_on=samples_on,
        index_col=index_col,
        drop_empty_samples=not drop_empty_samples is True,  # invert keep flag
        drop_empty_features=not drop_empty_features is True,
    )

    if do_alpha:
        shan = shannon_per_sample(counts)
        out = out_dir / ("shannon.tsv" if label == "micro" else "shannon.mito.tsv")
        shan.to_csv(out, sep="\t")
        print(f"[{label}] Wrote {out}")

    if do_bray:
        bray = pairwise_distance(counts, metric="braycurtis")
        out = out_dir / ("bray.tsv" if label == "micro" else "bray.mito.tsv")
        write_square_matrix(bray, out)
        print(f"[{label}] Wrote {out}")

    if do_jaccard:
        jacc = jaccard_from_presence(counts, threshold=j_thresh)
        out = out_dir / ("jaccard.tsv" if label == "micro" else "jaccard.mito.tsv")
        write_square_matrix(jacc, out)
        print(f"[{label}] Wrote {out}")


def main():
    args = build_parser().parse_args()

    if not args.micro_table and not args.mito_table:
        print("ERROR: Provide at least one of --micro-table or --mito-table", file=sys.stderr)
        sys.exit(2)

    # normalize index_col to int or str
    try:
        index_col = int(args.index_col) if str(args.index_col).isdigit() else args.index_col
    except Exception:
        index_col = args.index_col

    do_alpha = not args.skip_alpha
    do_bray = not args.skip_bray
    do_jacc = not args.skip_jaccard

    micro_dir, mito_dir = resolve_dirs(args.outdir, args.mito_outdir, args.mito_table)

    if args.micro_table:
        run_for_table(
            "micro",
            args.micro_table,
            micro_dir,
            args.samples_on,
            index_col,
            do_alpha,
            do_bray,
            do_jacc,
            args.presence_threshold,
            args.keep_empty_samples,
            args.keep_empty_features,
        )

    if args.mito_table:
        try:
            run_for_table(
                "mito",
                args.mito_table,
                mito_dir if mito_dir is not None else args.outdir,
                args.samples_on,
                index_col,
                do_alpha,
                do_bray,
                do_jacc,
                args.presence_threshold,
                args.keep_empty_samples,
                args.keep_empty_features,
            )
        except ValueError as exc:
            if "After filtering, table is empty" in str(exc):
                print(f"[mito] WARNING: {exc}. Skipping mito diversity.", file=sys.stderr)
            else:
                raise


if __name__ == "__main__":
    main()

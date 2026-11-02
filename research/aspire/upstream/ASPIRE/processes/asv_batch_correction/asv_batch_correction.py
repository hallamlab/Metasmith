#!/usr/bin/env python3
"""
asv_batch_correction.py

Batch effect correction for 16S amplicon data with before/after visualization.
Uses ConQuR count-table correction + CLR diagnostics with UMAP/HDBSCAN and swarm plots.

Now includes joint UMAP+HDBSCAN parameter optimization with cluster count constraints!

This script CORRECTS SAMPLES by removing technical batch effects from their microbial profiles
while preserving biological variation.

Quickstart:
  python asv_batch_correction.py \
    --data-dir /home/ryan/SeqData/SeqData/UBC/LMP_priority1 \
    --asv spark_combined_output/ASVs/ASV_final.micro.tsv \
    --metadata spark_combined_output/metadata/metadata_updated.tsv \
    --batch-col batch \
    --output-dir spark_combined_output/batch_correction \
    --biological-covariates type_group,status \
    --optimize-clustering \
    --target-clusters 3-8 \
    --verbose
"""
from __future__ import annotations
import argparse
import json
import os
import shutil
import subprocess
import tempfile
import textwrap
import warnings
from pathlib import Path
from typing import List, Optional, Tuple, Dict

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import seaborn as sns
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.metrics import silhouette_score, calinski_harabasz_score, davies_bouldin_score

# Compositional data transforms
from skbio.stats.composition import clr
try:
    # skbio <=0.6
    from skbio.stats.composition import multiplicative_replacement
except ImportError:
    # skbio >=0.7 renamed this helper
    from skbio.stats.composition import multi_replace as multiplicative_replacement

# Dimensionality reduction & clustering
import umap
import hdbscan

warnings.filterwarnings('ignore')

# Plotting aesthetics
plt.rcParams.update({
    'font.size': 11,
    'font.family': 'sans-serif',
    'axes.linewidth': 1.2,
    'figure.dpi': 150,
})
sns.set_style("white")

DEFAULT_SAMPLE_ID_COL = 'sampleID'


def _sorted_non_null_unique(series: pd.Series) -> List:
    """Return non-null unique values sorted robustly across mixed types."""
    vals = [v for v in pd.unique(series) if not pd.isna(v)]
    return sorted(vals, key=lambda x: str(x))


def _safe_color(value, fallback: str = '#808080') -> str:
    """Return a valid matplotlib color string, falling back when invalid/missing."""
    if pd.isna(value):
        return fallback
    color = str(value).strip()
    if not color or color.lower() == 'nan':
        return fallback
    return color if mcolors.is_color_like(color) else fallback


def _prepare_biological_covariates(cov: pd.DataFrame) -> pd.DataFrame:
    """
    Preserve numeric covariates as numeric gradients for ConQuR.

    - numeric or fully numeric-coercible columns stay numeric and are median-imputed
    - non-numeric columns are treated as categorical and receive a "MISSING" label
    - constant columns are dropped
    """
    prepared = pd.DataFrame(index=cov.index)

    for col in cov.columns:
        series = cov[col]
        non_null = series.dropna()
        if non_null.empty:
            continue

        numeric_cast = pd.to_numeric(series, errors="coerce")
        is_numeric = pd.api.types.is_numeric_dtype(series) or (
            len(non_null) > 0 and numeric_cast.notna().sum() == len(non_null)
        )

        if is_numeric:
            filled = numeric_cast.astype(float)
            median = filled.median()
            if pd.isna(median):
                continue
            prepared[col] = filled.fillna(median)
        else:
            prepared[col] = series.fillna("MISSING").astype(str)

    if prepared.shape[1] == 0:
        return prepared

    non_constant = prepared.nunique(dropna=False) > 1
    return prepared.loc[:, non_constant]


# ============================================================================
# Batch Correction Functions
# ============================================================================

def _correction_guard_stats(
    data_before: np.ndarray,
    data_after: np.ndarray,
) -> Tuple[float, float, float]:
    """Return (median_spread_ratio, max_abs_after, p95_abs_delta)."""
    before = np.asarray(data_before, dtype=float)
    after = np.asarray(data_after, dtype=float)
    spread_before = np.nanmax(before, axis=0) - np.nanmin(before, axis=0)
    spread_after = np.nanmax(after, axis=0) - np.nanmin(after, axis=0)
    ratio = np.nanmedian((spread_after + 1e-9) / (spread_before + 1e-9))
    max_abs_after = float(np.nanmax(np.abs(after)))
    p95_abs_delta = float(np.nanpercentile(np.abs(after - before), 95))
    return float(ratio), max_abs_after, p95_abs_delta


def _is_pathological_correction(
    data_before: np.ndarray,
    data_after: np.ndarray,
    spread_ratio_max: float = 4.0,
    max_abs_max: float = 40.0,
) -> bool:
    """Flag numerically pathological corrections in CLR space."""
    before = np.asarray(data_before, dtype=float)
    after = np.asarray(data_after, dtype=float)
    if before.shape != after.shape:
        return True
    if not np.isfinite(after).all():
        return True
    ratio, max_abs_after, _ = _correction_guard_stats(before, after)
    if not np.isfinite(ratio):
        return True
    return (ratio > spread_ratio_max) or (max_abs_after > max_abs_max)


def _run_conqur_r(
    counts: pd.DataFrame,
    metadata: pd.DataFrame,
    batch_col: str,
    covariate_cols: List[str],
    num_core: int = 2,
    use_libsize: bool = True,
    batch_ref: str = "",
    logistic_lasso: bool = False,
    quantile_type: str = "standard",
    simple_match: bool = False,
    lambda_quantile: str = "2p/n",
    interplt: bool = False,
    delta: float = 0.4999,
    auto_install: bool = False,
    random_state: int = 42,
) -> pd.DataFrame:
    """
    Run ConQuR in R and return corrected count table (samples x features).
    """
    rscript_bin = shutil.which("Rscript")
    if not rscript_bin:
        raise RuntimeError(
            "Rscript not found in PATH. Install R in the batch-correction environment."
        )

    with tempfile.TemporaryDirectory(prefix="conqur_") as td:
        td_path = Path(td)
        counts_path = td_path / "counts.tsv"
        meta_path = td_path / "metadata.tsv"
        out_path = td_path / "corrected.tsv"
        r_path = td_path / "run_conqur_tmp.R"

        counts.to_csv(counts_path, sep="\t")
        metadata.to_csv(meta_path, sep="\t")

        r_code = textwrap.dedent(
            """
            args <- commandArgs(trailingOnly = TRUE)
            if (length(args) < 16) stop("Expected 16 arguments")

            counts_path      <- args[[1]]
            meta_path        <- args[[2]]
            batch_col        <- args[[3]]
            cov_csv          <- args[[4]]
            out_path         <- args[[5]]
            num_core         <- as.integer(args[[6]])
            use_libsize      <- as.integer(args[[7]]) == 1
            batch_ref        <- args[[8]]
            logistic_lasso   <- as.integer(args[[9]]) == 1
            quantile_type    <- args[[10]]
            simple_match     <- as.integer(args[[11]]) == 1
            lambda_quantile  <- args[[12]]
            interplt         <- as.integer(args[[13]]) == 1
            delta            <- as.numeric(args[[14]])
            auto_install     <- as.integer(args[[15]]) == 1
            seed             <- as.integer(args[[16]])

            r_lib_user <- Sys.getenv("R_LIBS_USER", unset = "")
            if (nchar(r_lib_user) > 0) {
              dir.create(r_lib_user, recursive = TRUE, showWarnings = FALSE)
              .libPaths(c(r_lib_user, .libPaths()))
            }

            if (auto_install) {
              lib_primary <- .libPaths()[1]
              for (lock_name in c("00LOCK-ConQuR", "00LOCK-remotes", "00LOCK-foreach")) {
                lock_path <- file.path(lib_primary, lock_name)
                if (dir.exists(lock_path)) {
                  unlink(lock_path, recursive = TRUE, force = TRUE)
                }
              }
              repos <- "https://cloud.r-project.org"
              if (!requireNamespace("remotes", quietly = TRUE)) {
                install.packages("remotes", repos = repos, lib = lib_primary)
              }
              if (!requireNamespace("foreach", quietly = TRUE)) {
                install.packages("foreach", repos = repos, lib = lib_primary)
              }
              if (!requireNamespace("ConQuR", quietly = TRUE)) {
                remotes::install_github(
                  "wdl2459/ConQuR",
                  dependencies = TRUE,
                  upgrade = "never",
                  lib = lib_primary
                )
              }
            }

            if (!requireNamespace("foreach", quietly = TRUE)) {
              stop("R package 'foreach' is required for ConQuR (%do% operator missing).")
            }

            if (!requireNamespace("ConQuR", quietly = TRUE)) {
              stop("ConQuR package not installed. Install with: remotes::install_github('wdl2459/ConQuR')")
            }

            suppressPackageStartupMessages(library(foreach))
            suppressPackageStartupMessages(library(ConQuR))

            counts_df <- read.table(
              counts_path, sep = "\\t", header = TRUE, row.names = 1,
              check.names = FALSE, quote = "", comment.char = "", stringsAsFactors = FALSE
            )
            meta_df <- read.table(
              meta_path, sep = "\\t", header = TRUE, row.names = 1,
              check.names = FALSE, quote = "", comment.char = "", stringsAsFactors = FALSE
            )

            common <- intersect(rownames(counts_df), rownames(meta_df))
            if (length(common) == 0) stop("No overlapping samples between counts and metadata")
            counts_df <- counts_df[common, , drop = FALSE]
            meta_df <- meta_df[common, , drop = FALSE]

            if (!(batch_col %in% colnames(meta_df))) {
              stop(paste0("Batch column not found in metadata: ", batch_col))
            }

            counts_mat <- as.matrix(counts_df)
            storage.mode(counts_mat) <- "numeric"
            counts_mat[!is.finite(counts_mat)] <- 0
            counts_mat[counts_mat < 0] <- 0

            batchid <- as.factor(meta_df[[batch_col]])
            if (length(levels(batchid)) < 2) {
              write.table(counts_mat, file = out_path, sep = "\\t", quote = FALSE, col.names = NA)
              quit(status = 0)
            }

            covariates <- data.frame(row.names = rownames(meta_df))
            if (nchar(cov_csv) > 0) {
              cov_cols <- strsplit(cov_csv, ",", fixed = TRUE)[[1]]
              cov_cols <- cov_cols[cov_cols != ""]
              missing_cols <- setdiff(cov_cols, colnames(meta_df))
              if (length(missing_cols) > 0) {
                stop(paste0("Missing covariates in metadata: ", paste(missing_cols, collapse = ",")))
              }
              covariates <- meta_df[, cov_cols, drop = FALSE]
            }

            if (ncol(covariates) == 0) {
              covariates <- data.frame(dummy = rep("all", nrow(meta_df)), row.names = rownames(meta_df))
            }
            for (nm in colnames(covariates)) {
              if (is.character(covariates[[nm]])) covariates[[nm]] <- as.factor(covariates[[nm]])
            }

            if (batch_ref == "") {
              tb <- sort(table(batchid), decreasing = TRUE)
              batch_ref <- names(tb)[1]
            }
            if (!(batch_ref %in% levels(batchid))) {
              batch_ref <- levels(batchid)[1]
            }

            if (is.na(num_core) || num_core < 1) num_core <- 1
            set.seed(seed)

            corrected <- if (use_libsize) {
              ConQuR::ConQuR_libsize(
                tax_tab = counts_mat,
                batchid = batchid,
                covariates = covariates,
                batch_ref = batch_ref,
                logistic_lasso = logistic_lasso,
                quantile_type = quantile_type,
                simple_match = simple_match,
                lambda_quantile = lambda_quantile,
                interplt = interplt,
                delta = delta,
                num_core = num_core
              )
            } else {
              ConQuR::ConQuR(
                tax_tab = counts_mat,
                batchid = batchid,
                covariates = covariates,
                batch_ref = batch_ref,
                logistic_lasso = logistic_lasso,
                quantile_type = quantile_type,
                simple_match = simple_match,
                lambda_quantile = lambda_quantile,
                interplt = interplt,
                delta = delta,
                num_core = num_core
              )
            }

            corr <- as.matrix(corrected)
            storage.mode(corr) <- "numeric"
            corr[!is.finite(corr)] <- 0
            corr[corr < 0] <- 0

            write.table(corr, file = out_path, sep = "\\t", quote = FALSE, col.names = NA)
            """
        ).strip() + "\n"
        r_path.write_text(r_code)

        cmd = [
            rscript_bin,
            str(r_path),
            str(counts_path),
            str(meta_path),
            batch_col,
            ",".join(covariate_cols),
            str(out_path),
            str(max(1, int(num_core))),
            "1" if use_libsize else "0",
            batch_ref or "",
            "1" if logistic_lasso else "0",
            quantile_type,
            "1" if simple_match else "0",
            lambda_quantile,
            "1" if interplt else "0",
            str(float(delta)),
            "1" if auto_install else "0",
            str(int(random_state)),
        ]
        run_env = os.environ.copy()
        r_lib_user = td_path / "r_libs_user"
        r_lib_user.mkdir(parents=True, exist_ok=True)
        run_env["R_LIBS_USER"] = str(r_lib_user)
        proc = subprocess.run(cmd, capture_output=True, text=True, env=run_env)
        if proc.returncode != 0:
            raise RuntimeError(
                "ConQuR correction failed.\n"
                f"STDOUT:\n{proc.stdout}\n\nSTDERR:\n{proc.stderr}"
            )

        corrected = pd.read_csv(out_path, sep="\t", index_col=0)
        corrected = corrected.apply(pd.to_numeric, errors="coerce").fillna(0.0).clip(lower=0.0)
        return corrected


def conqur_correction_wrapper(
    asv_counts: pd.DataFrame,
    metadata: pd.DataFrame,
    batch_col: str,
    biological_covariates: Optional[pd.DataFrame] = None,
    num_core: int = 2,
    use_libsize: bool = True,
    batch_ref: str = "",
    logistic_lasso: bool = False,
    quantile_type: str = "standard",
    simple_match: bool = False,
    lambda_quantile: str = "2p/n",
    interplt: bool = False,
    delta: float = 0.4999,
    auto_install: bool = False,
    random_state: int = 42,
) -> pd.DataFrame:
    """
    Correct raw ASV counts with ConQuR and return counts (samples x features).
    """
    if batch_col not in metadata.columns:
        raise ValueError(f"Batch column '{batch_col}' not found in metadata")

    counts = asv_counts.copy()
    counts = counts.apply(pd.to_numeric, errors="coerce").fillna(0.0).clip(lower=0.0)
    meta = metadata.copy()
    shared = counts.index.intersection(meta.index)
    if len(shared) == 0:
        raise ValueError("No shared samples between counts and metadata for ConQuR")
    counts = counts.loc[shared]
    meta = meta.loc[shared]

    cov_cols: List[str] = []
    if biological_covariates is not None and biological_covariates.shape[1] > 0:
        cov = biological_covariates.copy()
        cov = cov.reindex(shared)
        cov = cov.loc[:, cov.notna().any(axis=0)]
        if cov.shape[1] > 0:
            cov = _prepare_biological_covariates(cov)
            if cov.shape[1] > 0:
                for c in cov.columns:
                    meta[c] = cov[c].values
                cov_cols = list(cov.columns)

    corrected = _run_conqur_r(
        counts=counts,
        metadata=meta,
        batch_col=batch_col,
        covariate_cols=cov_cols,
        num_core=num_core,
        use_libsize=use_libsize,
        batch_ref=batch_ref,
        logistic_lasso=logistic_lasso,
        quantile_type=quantile_type,
        simple_match=simple_match,
        lambda_quantile=lambda_quantile,
        interplt=interplt,
        delta=delta,
        auto_install=auto_install,
        random_state=random_state,
    )

    corrected = corrected.reindex(index=counts.index, columns=counts.columns)
    corrected = corrected.apply(pd.to_numeric, errors="coerce").fillna(0.0).clip(lower=0.0)
    return corrected


# ============================================================================
# Data Loading & Processing
# ============================================================================

def load_asv_table(path: Path, orientation: str = "features_rows") -> pd.DataFrame:
    """
    Load ASV table and return as samples x features (rows = samples).
    
    Parameters
    ----------
    path : Path
        Path to ASV table file
    orientation : str
        "features_rows" = ASVs in rows, samples in columns (will be transposed)
        "samples_rows" = samples in rows, ASVs in columns (no transpose)
    
    Returns
    -------
    df : DataFrame (samples x features)
        Each row is a sample, each column is an ASV/feature
    """
    df = pd.read_csv(path, sep="\t", header=0, index_col=0)
    
    print(f"  [i] Loaded ASV table: {df.shape}")
    
    if orientation == "features_rows":
        print(f"  [i] Transposing: features in rows -> samples in rows")
        df = df.T  # Transpose so rows = samples, columns = features
        
    print(f"  [i] Final orientation: {df.shape} (samples x features)")
    
    return df.apply(pd.to_numeric, errors="coerce").fillna(0)


def load_metadata(path: Path, index_col: str) -> pd.DataFrame:
    """Load metadata and set index."""
    df = pd.read_csv(path, sep="\t", header=0)
    if index_col not in df.columns:
        raise ValueError(f"Index column '{index_col}' not in metadata")
    return df.drop_duplicates(subset=[index_col]).set_index(index_col)


def align_data(asv: pd.DataFrame, meta: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Align ASV table and metadata to shared samples.
    
    Parameters
    ----------
    asv : DataFrame (samples x features)
    meta : DataFrame (samples x metadata_cols)
    
    Returns
    -------
    asv_aligned : DataFrame (samples x features)
    meta_aligned : DataFrame (samples x metadata_cols)
    """
    shared = asv.index.intersection(meta.index)
    if len(shared) == 0:
        raise ValueError("No overlapping samples")
    
    print(f"  [i] Found {len(shared)} shared samples")
    
    asv = asv.loc[shared]
    meta = meta.loc[shared]
    
    # Filter zero-abundance features
    n_features_before = asv.shape[1]
    asv = asv.loc[:, (asv != 0).any(axis=0)]
    n_features_after = asv.shape[1]
    
    if n_features_after < n_features_before:
        print(f"  [i] Removed {n_features_before - n_features_after} all-zero features")
    
    # Filter zero-abundance samples
    n_samples_before = asv.shape[0]
    asv = asv.loc[(asv != 0).any(axis=1), :]
    n_samples_after = asv.shape[0]
    
    if n_samples_after < n_samples_before:
        print(f"  [i] Removed {n_samples_before - n_samples_after} all-zero samples")
    
    # Re-align metadata
    meta = meta.loc[asv.index]
    
    return asv, meta


def apply_clr_transform(asv_counts: pd.DataFrame) -> pd.DataFrame:
    """
    Apply CLR transformation with multiplicative replacement for zeros.
    
    CLR is applied to each SAMPLE (row), normalizing the compositional nature
    of sequencing data.
    
    Parameters
    ----------
    asv_counts : DataFrame (samples x features)
        Raw count matrix where each row is a sample
    
    Returns
    -------
    clr_data : DataFrame (samples x features)
        CLR-transformed data - each sample's composition is log-ratio transformed
    """
    print(f"  [i] Applying CLR transformation to {asv_counts.shape[0]} samples")
    
    # Multiplicative replacement handles zeros in compositional data
    # This is applied row-wise (per sample)
    arr = multiplicative_replacement(asv_counts.values)
    
    # CLR transformation (per sample)
    clr_arr = clr(arr)
    
    print(f"  [i] CLR range: [{clr_arr.min():.2f}, {clr_arr.max():.2f}]")
    
    return pd.DataFrame(
        clr_arr,
        index=asv_counts.index,
        columns=asv_counts.columns
    )


def _largest_remainder_integerize_rows(
    values: np.ndarray,
    target_totals: np.ndarray,
) -> np.ndarray:
    """
    Integerize each sample row while preserving sample totals via largest remainder.
    """
    n_rows, n_cols = values.shape
    out = np.zeros((n_rows, n_cols), dtype=np.int64)
    safe_targets = np.asarray(np.rint(target_totals), dtype=float).reshape(-1)
    safe_targets[~np.isfinite(safe_targets)] = 0.0
    int64_max = float(np.iinfo(np.int64).max)
    safe_targets = np.clip(safe_targets, 0.0, int64_max)
    target_totals = safe_targets.astype(np.int64)
    if target_totals.shape[0] != n_rows:
        raise ValueError("target_totals length does not match values rows")

    for i in range(n_rows):
        row = values[i]
        tgt = int(max(target_totals[i], 0))

        flo = np.floor(row).astype(np.int64)
        frac = row - flo
        cur = int(flo.sum())
        rem = tgt - cur

        if rem > 0:
            order = np.argsort(-frac, kind="mergesort")
            idx = order[:min(rem, n_cols)]
            flo[idx] += 1
        elif rem < 0:
            order = np.argsort(frac, kind="mergesort")
            to_remove = -rem
            for j in order:
                if to_remove <= 0:
                    break
                if flo[j] > 0:
                    flo[j] -= 1
                    to_remove -= 1

        out[i] = flo

    return out


def export_corrected_abundance_table(
    corrected_counts: pd.DataFrame,
    asv_raw_counts: pd.DataFrame,
    out_dir: Path,
) -> Tuple[Path, Path, Path, Path]:
    """
    Export ConQuR-corrected counts and integer pseudo-count companion tables.

    Writes:
      1) samples x features
      2) features x samples (drop-in orientation for calc_div.py default)
      3) integer pseudo-count samples x features (sum-preserving per sample totals)
      4) integer pseudo-count features x samples
    """
    if corrected_counts.shape != asv_raw_counts.shape:
        raise ValueError(
            "Corrected data and raw counts must have matching shape "
            f"(got {corrected_counts.shape} vs {asv_raw_counts.shape})"
        )

    corrected_samples = corrected_counts.copy()
    corrected_samples = corrected_samples.apply(pd.to_numeric, errors="coerce")
    corrected_samples = corrected_samples.replace([np.inf, -np.inf], np.nan).fillna(0.0).clip(lower=0.0)
    corrected = corrected_samples.to_numpy(dtype=float)

    # Integerize to count-like values while preserving corrected sample totals.
    lib_size = corrected.sum(axis=1, keepdims=True)
    raw_lib_size = asv_raw_counts.to_numpy(dtype=float).sum(axis=1, keepdims=True)
    int64_max = float(np.iinfo(np.int64).max)
    bad_lib = (~np.isfinite(lib_size)) | (lib_size <= 0) | (lib_size > int64_max)
    if np.any(bad_lib):
        n_bad = int(np.sum(bad_lib))
        print(
            f"  [w] {n_bad} sample totals were invalid/overflow-prone after correction; "
            "using raw library sizes for integer pseudo-count export."
        )
    lib_size = np.where(bad_lib, raw_lib_size, lib_size)
    lib_size[lib_size <= 0] = 1.0

    comp = corrected / lib_size
    comp_sum = comp.sum(axis=1, keepdims=True)
    bad_rows = comp_sum.ravel() <= 0
    if np.any(bad_rows):
        raw_arr = asv_raw_counts.to_numpy(dtype=float)
        raw_comp = raw_arr / np.maximum(raw_arr.sum(axis=1, keepdims=True), 1.0)
        comp[bad_rows, :] = raw_comp[bad_rows, :]
        comp_sum = comp.sum(axis=1, keepdims=True)
    comp = comp / comp_sum

    reconstructed = comp * lib_size
    corrected_pseudocount = _largest_remainder_integerize_rows(
        reconstructed,
        lib_size.reshape(-1),
    )
    corrected_pseudocount_df = pd.DataFrame(
        corrected_pseudocount,
        index=corrected_samples.index,
        columns=corrected_samples.columns,
    )

    samples_rows_path = out_dir / "asv_corrected_abundance.samples_rows.tsv"
    features_rows_path = out_dir / "asv_corrected_abundance.features_rows.tsv"
    pseudocount_samples_rows_path = out_dir / "asv_corrected_pseudocount.samples_rows.tsv"
    pseudocount_features_rows_path = out_dir / "asv_corrected_pseudocount.features_rows.tsv"
    corrected_samples.to_csv(samples_rows_path, sep="\t")
    corrected_samples.T.to_csv(features_rows_path, sep="\t")
    corrected_pseudocount_df.to_csv(pseudocount_samples_rows_path, sep="\t")
    corrected_pseudocount_df.T.to_csv(pseudocount_features_rows_path, sep="\t")
    return (
        samples_rows_path,
        features_rows_path,
        pseudocount_samples_rows_path,
        pseudocount_features_rows_path,
    )


# ============================================================================
# Dimensionality Reduction & Clustering with Optimization
# ============================================================================

def evaluate_clustering(
    embedding: np.ndarray, 
    labels: np.ndarray,
    target_min: Optional[int] = None,
    target_max: Optional[int] = None,
) -> Dict[str, float]:
    """
    Evaluate clustering quality using multiple metrics with optional cluster count penalty.
    
    Parameters
    ----------
    embedding : array (n_samples, n_dims)
        UMAP or other embedding
    labels : array (n_samples,)
        Cluster labels (-1 for noise)
    target_min : int, optional
        Minimum desired clusters (penalize if fewer)
    target_max : int, optional
        Maximum desired clusters (penalize if more)
    
    Returns
    -------
    metrics : dict
        Dictionary of metric names and scores
    """
    # Filter out noise points for sklearn metrics
    mask = labels != -1
    n_clusters = len(np.unique(labels[mask])) if mask.sum() > 0 else 0
    n_noise = (labels == -1).sum()
    noise_ratio = n_noise / len(labels)
    
    if mask.sum() < 2 or n_clusters < 2:
        # Not enough points or clusters to evaluate
        return {
            'n_clusters': n_clusters,
            'n_noise': n_noise,
            'silhouette': -1.0,
            'calinski_harabasz': 0.0,
            'davies_bouldin': float('inf'),
            'noise_ratio': noise_ratio,
            'cluster_penalty': 1.0,  # Maximum penalty
        }
    
    embedding_clean = embedding[mask]
    labels_clean = labels[mask]
    
    # Silhouette score: [-1, 1], higher is better
    sil = silhouette_score(embedding_clean, labels_clean)
    
    # Calinski-Harabasz: [0, inf], higher is better
    ch = calinski_harabasz_score(embedding_clean, labels_clean)
    
    # Davies-Bouldin: [0, inf], lower is better
    db = davies_bouldin_score(embedding_clean, labels_clean)
    
    # Cluster count penalty (0 = no penalty, 1 = maximum penalty)
    cluster_penalty = 0.0
    if target_min is not None and n_clusters < target_min:
        # Penalize for too few clusters
        cluster_penalty = (target_min - n_clusters) / target_min
    elif target_max is not None and n_clusters > target_max:
        # Penalize for too many clusters (stronger penalty)
        cluster_penalty = min(1.0, (n_clusters - target_max) / target_max)
    
    return {
        'n_clusters': n_clusters,
        'n_noise': n_noise,
        'silhouette': sil,
        'calinski_harabasz': ch,
        'davies_bouldin': db,
        'noise_ratio': noise_ratio,
        'cluster_penalty': cluster_penalty,
    }


def optimize_umap_hdbscan_joint(
    data: pd.DataFrame,
    n_neighbors_list: List[int] = [10, 15, 20, 30],
    min_dist_list: List[float] = [0.0, 0.1, 0.25],
    min_cluster_sizes: List[int] = [3, 5, 7, 10],
    min_samples_list: List[int] = [1, 3, 5],
    cluster_selection_methods: List[str] = ['eom', 'leaf'],
    target_clusters_min: Optional[int] = None,
    target_clusters_max: Optional[int] = None,
    metric: str = 'euclidean',
    random_state: int = 42,
) -> Tuple[Dict, Dict, pd.DataFrame]:
    """
    Joint optimization of UMAP and HDBSCAN parameters.
    
    Strategy: Test UMAP parameters, then for each UMAP embedding test HDBSCAN parameters.
    This is more efficient than full grid search.
    
    Parameters
    ----------
    data : DataFrame (samples x features)
        Data to embed and cluster
    target_clusters_min : int, optional
        Minimum desired number of clusters
    target_clusters_max : int, optional
        Maximum desired number of clusters
    
    Returns
    -------
    best_umap_params : dict
        Best UMAP parameters
    best_hdbscan_params : dict
        Best HDBSCAN parameters
    results_df : DataFrame
        All tried combinations with scores
    """
    print("  [i] Optimizing UMAP + HDBSCAN parameters jointly...")
    
    n_umap_combos = len(n_neighbors_list) * len(min_dist_list)
    n_hdbscan_combos = len(min_cluster_sizes) * len(min_samples_list) * len(cluster_selection_methods)
    total = n_umap_combos * n_hdbscan_combos
    
    print(f"      Testing {n_umap_combos} UMAP × {n_hdbscan_combos} HDBSCAN = {total} total combinations")
    if target_clusters_min or target_clusters_max:
        print(f"      Target cluster range: {target_clusters_min or 'any'}-{target_clusters_max or 'any'}")
    
    results = []
    embeddings_cache = {}
    
    # Stage 1: Generate UMAP embeddings
    for n_neighbors in n_neighbors_list:
        for min_dist in min_dist_list:
            umap_key = (n_neighbors, min_dist)
            
            reducer = umap.UMAP(
                n_neighbors=n_neighbors,
                min_dist=min_dist,
                metric=metric,
                n_components=2,
                random_state=random_state,
            )
            embedding = reducer.fit_transform(data.values)
            embeddings_cache[umap_key] = embedding
    
    # Stage 2: Test HDBSCAN on each embedding
    for umap_key, embedding in embeddings_cache.items():
        n_neighbors, min_dist = umap_key
        
        for min_cluster_size in min_cluster_sizes:
            for min_samples in min_samples_list:
                for selection_method in cluster_selection_methods:
                    # Fit HDBSCAN
                    clusterer = hdbscan.HDBSCAN(
                        min_cluster_size=min_cluster_size,
                        min_samples=min_samples,
                        cluster_selection_method=selection_method,
                        metric=metric,
                        gen_min_span_tree=True,
                    )
                    labels = clusterer.fit_predict(embedding)
                    
                    # Evaluate
                    metrics = evaluate_clustering(
                        embedding, labels,
                        target_min=target_clusters_min,
                        target_max=target_clusters_max
                    )
                    
                    # Store results
                    result = {
                        'umap_n_neighbors': n_neighbors,
                        'umap_min_dist': min_dist,
                        'hdbscan_min_cluster_size': min_cluster_size,
                        'hdbscan_min_samples': min_samples,
                        'hdbscan_selection_method': selection_method,
                        **metrics
                    }
                    results.append(result)
    
    results_df = pd.DataFrame(results)
    
    # Define composite score with cluster count consideration
    # Normalize Calinski-Harabasz to 0-1 range
    ch_norm = results_df['calinski_harabasz'] / results_df['calinski_harabasz'].max() if results_df['calinski_harabasz'].max() > 0 else 0
    
    # Composite score components:
    # - Silhouette (40%): cluster quality
    # - CH normalized (20%): cluster separation
    # - Low noise ratio (20%): fewer outliers
    # - Cluster penalty (20%): stay within target range
    results_df['composite_score'] = (
        results_df['silhouette'].clip(lower=0) * 0.4 +  # Quality
        ch_norm * 0.2 +  # Separation
        (1 - results_df['noise_ratio']) * 0.2 +  # Fewer noise points
        (1 - results_df['cluster_penalty']) * 0.2  # Within target range
    )
    
    # Get best parameters
    best_idx = results_df['composite_score'].idxmax()
    
    best_umap_params = {
        'n_neighbors': int(results_df.loc[best_idx, 'umap_n_neighbors']),
        'min_dist': float(results_df.loc[best_idx, 'umap_min_dist']),
        'metric': metric,
        'n_components': 2,
        'random_state': random_state,
    }
    
    best_hdbscan_params = {
        'min_cluster_size': int(results_df.loc[best_idx, 'hdbscan_min_cluster_size']),
        'min_samples': int(results_df.loc[best_idx, 'hdbscan_min_samples']),
        'cluster_selection_method': results_df.loc[best_idx, 'hdbscan_selection_method'],
        'metric': metric,
    }
    
    print(f"  [✓] Best UMAP: n_neighbors={best_umap_params['n_neighbors']}, "
          f"min_dist={best_umap_params['min_dist']}")
    print(f"  [✓] Best HDBSCAN: min_cluster_size={best_hdbscan_params['min_cluster_size']}, "
          f"min_samples={best_hdbscan_params['min_samples']}, "
          f"method={best_hdbscan_params['cluster_selection_method']}")
    print(f"      Results: {int(results_df.loc[best_idx, 'n_clusters'])} clusters, "
          f"silhouette={results_df.loc[best_idx, 'silhouette']:.3f}, "
          f"noise_ratio={results_df.loc[best_idx, 'noise_ratio']:.1%}")
    
    return best_umap_params, best_hdbscan_params, results_df


def compute_umap_hdbscan(
    data: pd.DataFrame,
    n_neighbors: int = 15,
    min_dist: float = 0.1,
    metric: str = "euclidean",
    min_cluster_size: int = 5,
    min_samples: Optional[int] = None,
    cluster_selection_method: str = 'eom',
    random_state: int = 42,
    optimize_params: bool = False,
    target_clusters_min: Optional[int] = None,
    target_clusters_max: Optional[int] = None,
) -> Tuple[np.ndarray, np.ndarray, Optional[pd.DataFrame]]:
    """
    Compute UMAP embedding and HDBSCAN clusters on SAMPLES.
    
    Parameters
    ----------
    data : DataFrame (samples x features)
        Each row is a sample to be embedded and clustered
    optimize_params : bool
        If True, perform joint parameter search for UMAP+HDBSCAN
    target_clusters_min/max : int, optional
        Desired cluster count range for optimization
    
    Returns
    -------
    embedding : array (n_samples, 2)
        2D UMAP coordinates for each sample
    labels : array (n_samples,)
        Cluster labels for each sample (-1 for noise)
    optimization_results : DataFrame or None
        Results from parameter optimization if performed
    """
    optimization_results = None
    
    if optimize_params:
        # Joint optimization
        best_umap_params, best_hdbscan_params, optimization_results = optimize_umap_hdbscan_joint(
            data,
            target_clusters_min=target_clusters_min,
            target_clusters_max=target_clusters_max,
            metric=metric,
            random_state=random_state,
        )
        
        # Use optimized parameters
        n_neighbors = best_umap_params['n_neighbors']
        min_dist = best_umap_params['min_dist']
        min_cluster_size = best_hdbscan_params['min_cluster_size']
        min_samples = best_hdbscan_params['min_samples']
        cluster_selection_method = best_hdbscan_params['cluster_selection_method']
    
    # UMAP embedding
    reducer = umap.UMAP(
        n_neighbors=n_neighbors,
        min_dist=min_dist,
        metric=metric,
        n_components=2,
        random_state=random_state,
    )
    embedding = reducer.fit_transform(data.values)
    
    # HDBSCAN clustering
    clusterer = hdbscan.HDBSCAN(
        min_cluster_size=min_cluster_size,
        min_samples=min_samples,
        cluster_selection_method=cluster_selection_method,
        metric=metric,
        gen_min_span_tree=True,
    )
    labels = clusterer.fit_predict(embedding)
    
    return embedding, labels, optimization_results


# ============================================================================
# Plotting Functions
# ============================================================================

def save_png_and_svg(output_path: Path | str, *, bbox_inches: str = 'tight', dpi: int = 300) -> None:
    """Save the current matplotlib figure as PNG plus matching SVG."""
    png_path = Path(output_path)
    if png_path.suffix.lower() != '.png':
        png_path = png_path.with_suffix('.png')
    plt.savefig(png_path, bbox_inches=bbox_inches, dpi=dpi)
    plt.savefig(png_path.with_suffix('.svg'), bbox_inches=bbox_inches)


def plot_optimization_results(results_df: pd.DataFrame, output_path: Path) -> None:
    """
    Visualize joint UMAP+HDBSCAN parameter optimization results.
    """
    fig = plt.figure(figsize=(18, 12))
    gs = fig.add_gridspec(3, 3, hspace=0.3, wspace=0.3)
    
    # Plot 1: Silhouette vs n_neighbors (grouped by min_dist)
    ax = fig.add_subplot(gs[0, 0])
    for min_dist in sorted(results_df['umap_min_dist'].unique()):
        subset = results_df[results_df['umap_min_dist'] == min_dist]
        grouped = subset.groupby('umap_n_neighbors')['silhouette'].mean()
        ax.plot(grouped.index, grouped.values, marker='o', label=f'min_dist={min_dist}', linewidth=2)
    ax.set_xlabel('UMAP n_neighbors')
    ax.set_ylabel('Mean Silhouette Score')
    ax.set_title('Silhouette vs UMAP n_neighbors')
    ax.legend(loc='upper left', bbox_to_anchor=(1.02, 1.0))
    ax.grid(alpha=0.3)
    
    # Plot 2: Cluster count vs min_cluster_size
    ax = fig.add_subplot(gs[0, 1])
    for method in results_df['hdbscan_selection_method'].unique():
        subset = results_df[results_df['hdbscan_selection_method'] == method]
        grouped = subset.groupby('hdbscan_min_cluster_size')['n_clusters'].mean()
        ax.plot(grouped.index, grouped.values, marker='o', label=method, linewidth=2)
    ax.set_xlabel('HDBSCAN min_cluster_size')
    ax.set_ylabel('Mean Number of Clusters')
    ax.set_title('Cluster Count vs min_cluster_size')
    ax.legend(loc='upper left', bbox_to_anchor=(1.02, 1.0))
    ax.grid(alpha=0.3)
    
    # Plot 3: Noise ratio heatmap (UMAP params)
    ax = fig.add_subplot(gs[0, 2])
    pivot = results_df.groupby(['umap_n_neighbors', 'umap_min_dist'])['noise_ratio'].mean().unstack()
    sns.heatmap(pivot, annot=True, fmt='.2f', cmap='RdYlGn_r', ax=ax, cbar_kws={'label': 'Noise Ratio'})
    ax.set_title('Noise Ratio by UMAP Params')
    ax.set_xlabel('min_dist')
    ax.set_ylabel('n_neighbors')
    
    # Plot 4: Composite score vs parameters (scatter)
    ax = fig.add_subplot(gs[1, 0])
    scatter = ax.scatter(
        results_df['umap_n_neighbors'],
        results_df['hdbscan_min_cluster_size'],
        c=results_df['composite_score'],
        s=100,
        cmap='viridis',
        alpha=0.6,
        edgecolors='black',
        linewidth=0.5
    )
    # Mark the best
    best_idx = results_df['composite_score'].idxmax()
    ax.scatter(
        results_df.loc[best_idx, 'umap_n_neighbors'],
        results_df.loc[best_idx, 'hdbscan_min_cluster_size'],
        color='red', s=400, marker='*', zorder=10,
        edgecolors='black', linewidth=2, label='Best'
    )
    ax.set_xlabel('UMAP n_neighbors')
    ax.set_ylabel('HDBSCAN min_cluster_size')
    ax.set_title('Composite Score (★ = Best)')
    plt.colorbar(scatter, ax=ax, label='Composite Score')
    ax.legend(loc='upper left', bbox_to_anchor=(1.02, 1.0))
    ax.grid(alpha=0.3)
    
    # Plot 5: Cluster count distribution
    ax = fig.add_subplot(gs[1, 1])
    results_df['n_clusters'].hist(bins=20, ax=ax, edgecolor='black', alpha=0.7)
    ax.axvline(results_df.loc[best_idx, 'n_clusters'], color='red', linestyle='--', linewidth=2, label='Best')
    ax.set_xlabel('Number of Clusters')
    ax.set_ylabel('Frequency')
    ax.set_title('Distribution of Cluster Counts')
    ax.legend(loc='upper left', bbox_to_anchor=(1.02, 1.0))
    ax.grid(alpha=0.3)
    
    # Plot 6: Silhouette vs cluster count
    ax = fig.add_subplot(gs[1, 2])
    ax.scatter(results_df['n_clusters'], results_df['silhouette'], alpha=0.5, s=50)
    ax.scatter(
        results_df.loc[best_idx, 'n_clusters'],
        results_df.loc[best_idx, 'silhouette'],
        color='red', s=300, marker='*', zorder=10,
        edgecolors='black', linewidth=2, label='Best'
    )
    ax.set_xlabel('Number of Clusters')
    ax.set_ylabel('Silhouette Score')
    ax.set_title('Silhouette vs Cluster Count')
    ax.legend(loc='upper left', bbox_to_anchor=(1.02, 1.0))
    ax.grid(alpha=0.3)
    
    # Plot 7: Top 10 parameter combinations
    ax = fig.add_subplot(gs[2, :])
    top10 = results_df.nlargest(10, 'composite_score').copy()
    top10['param_combo'] = top10.apply(
        lambda row: f"U:{int(row['umap_n_neighbors'])},{row['umap_min_dist']:.2f}\n"
                   f"H:{int(row['hdbscan_min_cluster_size'])},{int(row['hdbscan_min_samples'])},{row['hdbscan_selection_method'][:3]}",
        axis=1
    )
    
    x = range(len(top10))
    width = 0.2
    ax.bar([i - width*1.5 for i in x], top10['silhouette'], width, label='Silhouette', alpha=0.8)
    ax.bar([i - width*0.5 for i in x], top10['n_clusters']/10, width, label='Clusters/10', alpha=0.8)
    ax.bar([i + width*0.5 for i in x], 1-top10['noise_ratio'], width, label='1-Noise', alpha=0.8)
    ax.bar([i + width*1.5 for i in x], top10['composite_score'], width, label='Composite', alpha=0.8)
    
    ax.set_xlabel('Parameter Combination')
    ax.set_ylabel('Score')
    ax.set_title('Top 10 Parameter Combinations')
    ax.set_xticks(x)
    ax.set_xticklabels(top10['param_combo'], rotation=45, ha='right', fontsize=8)
    ax.legend(loc='upper left', bbox_to_anchor=(1.02, 1.0))
    ax.grid(axis='y', alpha=0.3)
    
    save_png_and_svg(output_path)
    plt.close()
    
    print(f"  [✓] Saved parameter optimization plots")


def plot_umap_comparison(
    embedding_before: np.ndarray,
    embedding_after: np.ndarray,
    labels_before: np.ndarray,
    labels_after: np.ndarray,
    batch: pd.Series,
    biological_data: List[Tuple[pd.Series, dict, str]],  # List of (series, palette, name)
    output_prefix: Path,
) -> None:
    """
    Create multi-row UMAP comparison plots showing before/after correction.
    Each point represents a SAMPLE.
    
    Rows:
    - Row 1: Colored by batch (shows if batch effects are removed)
    - Row 2: Colored by HDBSCAN clusters (shows if biological structure preserved)
    - Row 3+: Colored by biological metadata (one row per biological variable)
    
    Parameters
    ----------
    biological_data : List[Tuple[pd.Series, dict, str]]
        List of tuples, each containing (series, color_palette_dict, name)
    """
    # Calculate number of rows needed
    n_bio_rows = len(biological_data) if biological_data else 0
    n_rows = 2 + n_bio_rows  # Batch row + Cluster row + biological rows
    
    fig, axes = plt.subplots(n_rows, 2, figsize=(16, 7 * n_rows))
    
    # Ensure axes is always 2D
    if n_rows == 1:
        axes = axes.reshape(1, -1)
    
    # Get color palettes
    n_batches = len(batch.unique())
    batch_palette = dict(zip(
        sorted(batch.unique()),
        sns.color_palette("Set2", n_batches)
    ))
    
    # =========================================================================
    # ROW 0: Colored by BATCH
    # =========================================================================
    
    # Plot 1: Before - colored by batch
    ax = axes[0, 0]
    for b in sorted(batch.unique()):
        mask = batch == b
        ax.scatter(
            embedding_before[mask, 0],
            embedding_before[mask, 1],
            label=f"Batch {b}",
            c=[batch_palette[b]],
            alpha=0.7,
            s=60,
            edgecolors='black',
            linewidth=0.5,
        )
    ax.set_title("Before Correction - Colored by Batch", fontsize=13, fontweight='bold')
    ax.set_xlabel("UMAP 1")
    ax.set_ylabel("UMAP 2")
    ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', frameon=True)
    ax.grid(alpha=0.3)
    
    # Plot 2: After - colored by batch
    ax = axes[0, 1]
    for b in sorted(batch.unique()):
        mask = batch == b
        ax.scatter(
            embedding_after[mask, 0],
            embedding_after[mask, 1],
            label=f"Batch {b}",
            c=[batch_palette[b]],
            alpha=0.7,
            s=60,
            edgecolors='black',
            linewidth=0.5,
        )
    ax.set_title("After Correction - Colored by Batch", fontsize=13, fontweight='bold')
    ax.set_xlabel("UMAP 1")
    ax.set_ylabel("UMAP 2")
    ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', frameon=True)
    ax.grid(alpha=0.3)
    
    # =========================================================================
    # ROW 1: Colored by HDBSCAN CLUSTERS
    # =========================================================================
    
    # Plot 3: Before - colored by HDBSCAN clusters
    ax = axes[1, 0]
    n_clusters_before = len(set(labels_before)) - (1 if -1 in labels_before else 0)
    cluster_palette_before = dict(zip(
        sorted(set(labels_before)),
        sns.color_palette("tab20", len(set(labels_before)))
    ))
    for cluster in sorted(set(labels_before)):
        mask = labels_before == cluster
        label = f"Noise" if cluster == -1 else f"Cluster {cluster}"
        ax.scatter(
            embedding_before[mask, 0],
            embedding_before[mask, 1],
            label=label,
            c=[cluster_palette_before[cluster]],
            alpha=0.7,
            s=60,
            edgecolors='black',
            linewidth=0.5,
        )
    ax.set_title(f"Before Correction - HDBSCAN Clusters (n={n_clusters_before})", 
                 fontsize=13, fontweight='bold')
    ax.set_xlabel("UMAP 1")
    ax.set_ylabel("UMAP 2")
    ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', frameon=True)
    ax.grid(alpha=0.3)
    
    # Plot 4: After - colored by HDBSCAN clusters
    ax = axes[1, 1]
    n_clusters_after = len(set(labels_after)) - (1 if -1 in labels_after else 0)
    cluster_palette_after = dict(zip(
        sorted(set(labels_after)),
        sns.color_palette("tab20", len(set(labels_after)))
    ))
    for cluster in sorted(set(labels_after)):
        mask = labels_after == cluster
        label = f"Noise" if cluster == -1 else f"Cluster {cluster}"
        ax.scatter(
            embedding_after[mask, 0],
            embedding_after[mask, 1],
            label=label,
            c=[cluster_palette_after[cluster]],
            alpha=0.7,
            s=60,
            edgecolors='black',
            linewidth=0.5,
        )
    ax.set_title(f"After Correction - HDBSCAN Clusters (n={n_clusters_after})", 
                 fontsize=13, fontweight='bold')
    ax.set_xlabel("UMAP 1")
    ax.set_ylabel("UMAP 2")
    ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', frameon=True)
    ax.grid(alpha=0.3)
    
    # =========================================================================
    # ROWS 2+: Colored by BIOLOGICAL METADATA
    # =========================================================================
    
    if biological_data:
        for row_idx, (bio_series, bio_palette, bio_name) in enumerate(biological_data):
            current_row = 2 + row_idx
            bio_values_sorted = _sorted_non_null_unique(bio_series)
            if not bio_values_sorted:
                continue
            
            # Before - biological
            ax = axes[current_row, 0]
            for bio_val in bio_values_sorted:
                mask = bio_series == bio_val
                color = _safe_color(bio_palette.get(bio_val, '#808080'))
                ax.scatter(
                    embedding_before[mask, 0],
                    embedding_before[mask, 1],
                    label=str(bio_val),
                    c=[color],
                    alpha=0.7,
                    s=60,
                    edgecolors='black',
                    linewidth=0.5,
                )
            ax.set_title(f"Before Correction - Colored by {bio_name}", 
                         fontsize=13, fontweight='bold')
            ax.set_xlabel("UMAP 1")
            ax.set_ylabel("UMAP 2")
            ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', frameon=True, title=bio_name)
            ax.grid(alpha=0.3)
            
            # After - biological
            ax = axes[current_row, 1]
            for bio_val in bio_values_sorted:
                mask = bio_series == bio_val
                color = _safe_color(bio_palette.get(bio_val, '#808080'))
                ax.scatter(
                    embedding_after[mask, 0],
                    embedding_after[mask, 1],
                    label=str(bio_val),
                    c=[color],
                    alpha=0.7,
                    s=60,
                    edgecolors='black',
                    linewidth=0.5,
                )
            ax.set_title(f"After Correction - Colored by {bio_name}", 
                         fontsize=13, fontweight='bold')
            ax.set_xlabel("UMAP 1")
            ax.set_ylabel("UMAP 2")
            ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', frameon=True, title=bio_name)
            ax.grid(alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(f"{output_prefix}_umap_comparison.png", bbox_inches='tight', dpi=300)
    plt.savefig(f"{output_prefix}_umap_comparison.svg", bbox_inches='tight')
    plt.close()
    
    print(f"[✓] Saved UMAP comparison plots with {n_bio_rows} biological metadata variables")


def plot_feature_batch_effect_scatter(
    data_before: pd.DataFrame,
    data_after: pd.DataFrame,
    batch: pd.Series,
    biological: Optional[pd.Series],
    output_prefix: Path,
    min_eta_threshold: float = 0.01,
) -> pd.DataFrame:
    """
    Create comprehensive scatter plot showing batch effects across all features.
    
    Parameters
    ----------
    min_eta_threshold : float
        Minimum eta-squared to consider feature as having batch effect (default 0.01 = small effect)
    
    Returns
    -------
    DataFrame with all computed statistics for further analysis.
    """
    from scipy.stats import f_oneway, variation
    from adjustText import adjust_text  # pip install adjustText (for non-overlapping labels)
    
    print("  [i] Computing batch effect statistics for all features...")
    
    feature_stats = []
    
    for feature in data_before.columns:
        # Get data
        vals_before = data_before[feature].values
        vals_after = data_after[feature].values
        
        # --- BEFORE CORRECTION METRICS ---
        
        # 1. Eta-squared (proportion of variance explained by batch)
        groups_before = [data_before.loc[batch == b, feature].values for b in batch.unique()]
        
        try:
            f_stat_before, p_val_before = f_oneway(*groups_before)
        except:
            f_stat_before, p_val_before = 0.0, 1.0
        
        # Calculate sum of squares with numerical stability checks
        grand_mean_before = vals_before.mean()
        ss_total_before = np.sum((vals_before - grand_mean_before)**2)
        
        # Between-group sum of squares
        ss_between_before = 0
        for b in batch.unique():
            group = data_before.loc[batch == b, feature].values
            n_group = len(group)
            group_mean = np.mean(group)
            ss_between_before += n_group * (group_mean - grand_mean_before)**2
        
        # Eta-squared with safety check
        if ss_total_before > 1e-10:
            eta_sq_before = ss_between_before / ss_total_before
            eta_sq_before = np.clip(eta_sq_before, 0, 1)
        else:
            eta_sq_before = 0.0
        
        # 2. Coefficient of variation across batch means
        batch_means_before = [np.mean(g) for g in groups_before]
        if len(batch_means_before) > 1 and np.mean(batch_means_before) != 0:
            cv_batch_before = np.std(batch_means_before) / np.abs(np.mean(batch_means_before))
        else:
            cv_batch_before = 0.0
        
        # 3. Total variance
        total_var_before = np.var(vals_before)
        
        # 4. Mean absolute value
        mean_abs_before = np.mean(np.abs(vals_before))
        
        # 5. Prevalence
        prevalence = (data_before[feature] != 0).sum() / len(data_before)
        
        # --- AFTER CORRECTION METRICS ---
        
        groups_after = [data_after.loc[batch == b, feature].values for b in batch.unique()]
        
        try:
            f_stat_after, p_val_after = f_oneway(*groups_after)
        except:
            f_stat_after, p_val_after = 0.0, 1.0
        
        grand_mean_after = vals_after.mean()
        ss_total_after = np.sum((vals_after - grand_mean_after)**2)
        
        ss_between_after = 0
        for b in batch.unique():
            group = data_after.loc[batch == b, feature].values
            n_group = len(group)
            group_mean = np.mean(group)
            ss_between_after += n_group * (group_mean - grand_mean_after)**2
        
        if ss_total_after > 1e-10:
            eta_sq_after = ss_between_after / ss_total_after
            eta_sq_after = np.clip(eta_sq_after, 0, 1)
        else:
            eta_sq_after = 0.0
        
        batch_means_after = [np.mean(g) for g in groups_after]
        if len(batch_means_after) > 1 and np.mean(batch_means_after) != 0:
            cv_batch_after = np.std(batch_means_after) / np.abs(np.mean(batch_means_after))
        else:
            cv_batch_after = 0.0
        
        total_var_after = np.var(vals_after)
        mean_abs_after = np.mean(np.abs(vals_after))
        
        # --- CORRECTION QUALITY METRICS ---
        
        # Absolute change in batch effect
        delta_eta_sq = eta_sq_before - eta_sq_after
        
        # Percent reduction - only meaningful if there was a batch effect to begin with
        if eta_sq_before >= min_eta_threshold:
            pct_reduction = (delta_eta_sq / eta_sq_before * 100)
        else:
            pct_reduction = np.nan
        
        # Variance retained
        if total_var_before > 1e-10:
            var_retained = (total_var_after / total_var_before * 100)
        else:
            var_retained = 100.0
        
        # Effect size change
        delta_f_stat = f_stat_before - f_stat_after
        
        # Categorize batch effect strength
        if eta_sq_before < 0.01:
            effect_category_before = 'negligible'
        elif eta_sq_before < 0.06:
            effect_category_before = 'small'
        elif eta_sq_before < 0.14:
            effect_category_before = 'medium'
        else:
            effect_category_before = 'large'
        
        if eta_sq_after < 0.01:
            effect_category_after = 'negligible'
        elif eta_sq_after < 0.06:
            effect_category_after = 'small'
        elif eta_sq_after < 0.14:
            effect_category_after = 'medium'
        else:
            effect_category_after = 'large'
        
        feature_stats.append({
            'feature': feature,
            # Before metrics
            'eta_sq_before': eta_sq_before,
            'f_stat_before': f_stat_before,
            'p_val_before': p_val_before,
            'cv_batch_before': cv_batch_before,
            'total_var_before': total_var_before,
            'mean_abs_before': mean_abs_before,
            'effect_category_before': effect_category_before,
            # After metrics
            'eta_sq_after': eta_sq_after,
            'f_stat_after': f_stat_after,
            'p_val_after': p_val_after,
            'cv_batch_after': cv_batch_after,
            'total_var_after': total_var_after,
            'mean_abs_after': mean_abs_after,
            'effect_category_after': effect_category_after,
            # Quality metrics
            'delta_eta_sq': delta_eta_sq,
            'pct_reduction': pct_reduction,
            'var_retained': var_retained,
            'delta_f_stat': delta_f_stat,
            'prevalence': prevalence,
        })
    
    df_stats = pd.DataFrame(feature_stats)
    
    # Save full statistics
    df_stats.to_csv(f"{output_prefix}_all_feature_statistics.tsv", sep='\t', index=False)
    print(f"  [✓] Saved statistics for {len(df_stats)} features")
    
    # =================================================================
    # DETERMINE SHARED AXIS LIMITS AND COLOR RANGES
    # =================================================================
    
    # X-axis (eta-squared): Use max from before, add 10% padding
    x_max = max(df_stats['eta_sq_before'].max(), df_stats['eta_sq_after'].max())
    x_max = max(x_max * 1.1, 0.3)  # At least 0.3 to show reference lines
    x_lim = [-0.01, x_max]
    
    # Y-axis (variance): Use max from before/after, add 10% padding
    y_max = max(df_stats['total_var_before'].max(), df_stats['total_var_after'].max())
    y_max = y_max * 1.1
    y_lim = [-y_max * 0.02, y_max]
    
    # Color range for "before" plot (eta-squared)
    color_max_before = max(df_stats['eta_sq_before'].quantile(0.95), 0.3)
    
    # Color range for "after" plot (delta eta-squared)
    delta_max = df_stats['delta_eta_sq'].quantile(0.95)
    delta_min = df_stats['delta_eta_sq'].quantile(0.05)
    color_lim_after = [min(delta_min, -0.05), max(delta_max, 0.15)]
    
    # =================================================================
    # CREATE FIGURE WITH FIXED LAYOUT
    # =================================================================
    
    fig = plt.figure(figsize=(20, 8))
    
    # Use GridSpec for precise control
    from matplotlib.gridspec import GridSpec
    gs = GridSpec(1, 2, figure=fig, width_ratios=[1, 1], 
                  left=0.08, right=0.92, bottom=0.20, top=0.93, wspace=0.35)
    
    ax1 = fig.add_subplot(gs[0, 0])
    ax2 = fig.add_subplot(gs[0, 1])
    
    # =================================================================
    # PLOT 1: BEFORE CORRECTION
    # =================================================================
    
    scatter_before = ax1.scatter(
        df_stats['eta_sq_before'],
        df_stats['total_var_before'],
        c=df_stats['eta_sq_before'],
        s=df_stats['prevalence'] * 100 + 10,
        alpha=0.6,
        cmap='Greys',
        edgecolors='black',
        linewidth=0.5,
        vmin=0,
        vmax=color_max_before,
    )
    
    ax1.set_xlim(x_lim)
    ax1.set_ylim(y_lim)
    ax1.set_xlabel('Batch Effect Strength (η² - Variance Explained by Batch)', 
                   fontsize=12, fontweight='bold')
    ax1.set_ylabel('Total Variance (Biological + Technical Signal)', 
                   fontsize=12, fontweight='bold')
    ax1.set_title('Before Batch Correction', fontsize=14, fontweight='bold', pad=15)
    ax1.grid(alpha=0.3, linestyle='--')
    
    # Add reference lines
    ax1.axvline(0.01, color='gray', linestyle=':', alpha=0.6, linewidth=2, 
                label='Small (η²=0.01)', zorder=1)
    ax1.axvline(0.06, color='gray', linestyle='--', alpha=0.6, linewidth=2, 
                label='Medium (η²=0.06)', zorder=1)
    ax1.axvline(0.14, color='gray', linestyle='-', alpha=0.6, linewidth=2, 
                label='Large (η²=0.14)', zorder=1)
    ax1.legend(loc='upper left', bbox_to_anchor=(1.02, 1.0), fontsize=9, framealpha=0.95, edgecolor='black')
    
    # Colorbar for before - positioned to not overlap
    cbar_ax_before = fig.add_axes([0.08, 0.08, 0.35, 0.02])  # [left, bottom, width, height]
    cbar_before = fig.colorbar(scatter_before, cax=cbar_ax_before, orientation='horizontal')
    cbar_before.set_label('Batch Effect Strength (η²)', fontsize=10, fontweight='bold')
    
    '''
    # Annotate worst offenders with non-overlapping labels
    threshold_var = df_stats['total_var_before'].quantile(0.85)
    threshold_eta = 0.14
    problematic = df_stats[
        (df_stats['total_var_before'] > threshold_var) & 
        (df_stats['eta_sq_before'] > threshold_eta)
    ].nlargest(3, 'eta_sq_before')  # Only top 3 to avoid clutter
    
    texts_before = []
    for _, row in problematic.iterrows():
        feature_label = row['feature'][:25] + '...' if len(row['feature']) > 25 else row['feature']
        texts_before.append(
            ax1.text(
                row['eta_sq_before'], 
                row['total_var_before'],
                feature_label,
                fontsize=8,
                bbox=dict(boxstyle='round,pad=0.4', facecolor='yellow', 
                         alpha=0.8, edgecolor='black', linewidth=1),
                zorder=100
            )
        )
    
    # Use adjustText to prevent label overlaps (if available)
    try:
        from adjustText import adjust_text
        if texts_before:
            adjust_text(texts_before, ax=ax1, 
                       arrowprops=dict(arrowstyle='->', color='black', lw=0.5),
                       expand_points=(1.5, 1.5))
    except ImportError:
        pass  # adjustText not installed, labels may overlap
    '''
    # =================================================================
    # PLOT 2: AFTER CORRECTION
    # =================================================================
    
    scatter_after = ax2.scatter(
        df_stats['eta_sq_after'],
        df_stats['total_var_after'],
        c=df_stats['delta_eta_sq'],
        s=df_stats['prevalence'] * 100 + 10,
        alpha=0.6,
        cmap='Greys',
        edgecolors='black',
        linewidth=0.5,
        vmin=color_lim_after[0],
        vmax=color_lim_after[1],
    )
    
    ax2.set_xlim(x_lim)
    ax2.set_ylim(y_lim)
    ax2.set_xlabel('Batch Effect Strength (η² - Variance Explained by Batch)', 
                   fontsize=12, fontweight='bold')
    ax2.set_ylabel('Total Variance (Biological Signal Retained)', 
                   fontsize=12, fontweight='bold')
    ax2.set_title('After Batch Correction', fontsize=14, fontweight='bold', pad=15)
    ax2.grid(alpha=0.3, linestyle='--')
    
    # Add reference lines (same as before)
    ax2.axvline(0.01, color='gray', linestyle=':', alpha=0.6, linewidth=2, 
                label='Small (η²=0.01)', zorder=1)
    ax2.axvline(0.06, color='gray', linestyle='--', alpha=0.6, linewidth=2, 
                label='Medium (η²=0.06)', zorder=1)
    ax2.axvline(0.14, color='gray', linestyle='-', alpha=0.6, linewidth=2, 
                label='Large (η²=0.14)', zorder=1)
    ax2.legend(loc='upper left', bbox_to_anchor=(1.02, 1.0), fontsize=9, framealpha=0.95, edgecolor='black')
    
    # Colorbar for after - positioned to not overlap
    cbar_ax_after = fig.add_axes([0.575, 0.08, 0.35, 0.02])  # Aligned with right plot
    cbar_after = fig.colorbar(scatter_after, cax=cbar_ax_after, orientation='horizontal')
    cbar_after.set_label('Absolute Batch Effect Reduction (Δη²)', 
                         fontsize=10, fontweight='bold')
    '''
    # Annotate features that still have problems
    still_problematic = df_stats[
        df_stats['eta_sq_after'] > 0.1
    ].nlargest(3, 'eta_sq_after')
    
    texts_after = []
    for _, row in still_problematic.iterrows():
        feature_label = row['feature'][:25] + '...' if len(row['feature']) > 25 else row['feature']
        texts_after.append(
            ax2.text(
                row['eta_sq_after'], 
                row['total_var_after'],
                feature_label,
                fontsize=8,
                bbox=dict(boxstyle='round,pad=0.4', facecolor='orange', 
                         alpha=0.8, edgecolor='black', linewidth=1),
                zorder=100
            )
        )
    
    # Use adjustText to prevent label overlaps
    try:
        from adjustText import adjust_text
        if texts_after:
            adjust_text(texts_after, ax=ax2,
                       arrowprops=dict(arrowstyle='->', color='black', lw=0.5),
                       expand_points=(1.5, 1.5))
    except ImportError:
        pass
    '''
    # =================================================================
    # ADD SUMMARY STATISTICS BOX (positioned to not overlap)
    # =================================================================
    
    features_with_batch_effect = df_stats[df_stats['eta_sq_before'] >= min_eta_threshold]
    
    if len(features_with_batch_effect) > 0:
        mean_pct_reduction = features_with_batch_effect['pct_reduction'].mean()
        median_pct_reduction = features_with_batch_effect['pct_reduction'].median()
        n_with_effect = len(features_with_batch_effect)
    else:
        mean_pct_reduction = 0
        median_pct_reduction = 0
        n_with_effect = 0
    
    summary_text = (
        f"Total features: {len(df_stats)}\n"
        f"Features with batch effect (η²≥{min_eta_threshold}): {n_with_effect}\n"
        f"\n"
        f"All features:\n"
        f"  Mean η² before: {df_stats['eta_sq_before'].mean():.3f} "
        f"(median: {df_stats['eta_sq_before'].median():.3f})\n"
        f"  Mean η² after: {df_stats['eta_sq_after'].mean():.3f} "
        f"(median: {df_stats['eta_sq_after'].median():.3f})\n"
        f"  Mean Δη²: {df_stats['delta_eta_sq'].mean():.3f}\n"
        f"\n"
        f"Features with initial batch effect (η²≥{min_eta_threshold}):\n"
        f"  Mean % reduction: {mean_pct_reduction:.1f}%\n"
        f"  Median % reduction: {median_pct_reduction:.1f}%\n"
        f"  Improved: {(features_with_batch_effect['delta_eta_sq'] > 0).sum()}/{n_with_effect}"
    )
    
    # Position text box at bottom center, below colorbars
    fig.text(0.5, -0.25, summary_text, ha='center', va='bottom', fontsize=9, 
             family='monospace',
             bbox=dict(boxstyle='round,pad=0.8', facecolor='white', 
                      alpha=0.95, edgecolor='black', linewidth=1.5))
    
    # =================================================================
    # SAVE FIGURE
    # =================================================================
    
    plt.savefig(f"{output_prefix}_batch_effect_scatter.png", bbox_inches='tight', dpi=300)
    plt.savefig(f"{output_prefix}_batch_effect_scatter.svg", bbox_inches='tight')
    plt.close()
    
    print(f"[✓] Saved comprehensive batch effect scatter plot")
    
    # Print detailed summary
    print("\n" + "="*70)
    print("FEATURE-LEVEL BATCH EFFECT SUMMARY")
    print("="*70)
    print(f"Total features analyzed: {len(df_stats)}")
    print(f"\nFeatures categorized by initial batch effect:")
    print(f"  Negligible (η² < 0.01): {(df_stats['eta_sq_before'] < 0.01).sum()}")
    print(f"  Small (0.01 ≤ η² < 0.06): {((df_stats['eta_sq_before'] >= 0.01) & (df_stats['eta_sq_before'] < 0.06)).sum()}")
    print(f"  Medium (0.06 ≤ η² < 0.14): {((df_stats['eta_sq_before'] >= 0.06) & (df_stats['eta_sq_before'] < 0.14)).sum()}")
    print(f"  Large (η² ≥ 0.14): {(df_stats['eta_sq_before'] >= 0.14).sum()}")
    
    print(f"\nBatch effect strength (η²) - ALL features:")
    print(f"  Before: mean={df_stats['eta_sq_before'].mean():.4f}, median={df_stats['eta_sq_before'].median():.4f}, max={df_stats['eta_sq_before'].max():.4f}")
    print(f"  After:  mean={df_stats['eta_sq_after'].mean():.4f}, median={df_stats['eta_sq_after'].median():.4f}, max={df_stats['eta_sq_after'].max():.4f}")
    
    if n_with_effect > 0:
        print(f"\nCorrection performance - ONLY features with initial batch effect (η² ≥ {min_eta_threshold}):")
        print(f"  N = {n_with_effect} features")
        print(f"  Mean % reduction: {mean_pct_reduction:.1f}%")
        print(f"  Median % reduction: {median_pct_reduction:.1f}%")
        print(f"  Features improved: {(features_with_batch_effect['delta_eta_sq'] > 0).sum()} ({(features_with_batch_effect['delta_eta_sq'] > 0).sum()/n_with_effect*100:.1f}%)")
        print(f"  Features worsened: {(features_with_batch_effect['delta_eta_sq'] < 0).sum()}")
    
    print(f"\nAbsolute change (Δη²) - ALL features:")
    print(f"  Mean: {df_stats['delta_eta_sq'].mean():.4f}")
    print(f"  Median: {df_stats['delta_eta_sq'].median():.4f}")
    print(f"  Features improved (Δη² > 0): {(df_stats['delta_eta_sq'] > 0).sum()} ({(df_stats['delta_eta_sq'] > 0).sum()/len(df_stats)*100:.1f}%)")
    
    print(f"\nVariance retained: {df_stats['var_retained'].mean():.1f}% (median: {df_stats['var_retained'].median():.1f}%)")
    print("="*70 + "\n")
    
    return df_stats


def plot_batch_swarm_comparison(
    data_before: pd.DataFrame,
    data_after: pd.DataFrame,
    batch: pd.Series,
    biological_data: List[Tuple[pd.Series, dict, str]],  # Updated parameter
    output_prefix: Path,
    n_features_to_plot: int = 5,
) -> None:
    """
    Create swarmplot + boxplot showing batch effects before/after correction.
    Now supports multiple biological metadata columns (uses first for PC1 plots).
    """
    
    # Run diagnostics FIRST
    diagnose_batch_correction(
        data_before,
        data_after,
        batch,
        output_prefix,
    )
    
    # Extract first biological variable if available
    biological_series = None
    biological_colors = None
    biological_name = None
    
    if biological_data and len(biological_data) > 0:
        biological_series = biological_data[0][0]
        biological_colors = biological_data[0][1]
        biological_name = biological_data[0][2]
    
    # Create batch color palette (used for hue)
    n_batches = len(batch.unique())
    batch_palette = dict(zip(
        sorted(batch.unique()),
        sns.color_palette("Set2", n_batches)
    ))
    
    # Compute PC1 for overall batch effect visualization
    # PC1 captures the major axis of variation across SAMPLES
    pca_before = PCA(n_components=1)
    pc1_before = pca_before.fit_transform(data_before.values).flatten()
    
    pca_after = PCA(n_components=1)
    pc1_after = pca_after.fit_transform(data_after.values).flatten()
    
    # Create comparison dataframe (each row is a sample)
    df_plot = pd.DataFrame({
        'sample': data_before.index,
        'PC1_before': pc1_before,
        'PC1_after': pc1_after,
        'batch': batch.values,
    })
    
    if biological_series is not None:
        df_plot['biological'] = biological_series.values
    
    # Create figure with two panels
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))
    
    # Determine x-axis and hue
    if biological_series is not None:
        x_col = 'biological'
        hue_col = 'batch'  # Batch as hue (colors)
        hue_palette = batch_palette
        x_label = biological_name if biological_name else 'Biological Group'
    else:
        # Fallback to batch on x-axis if no biological data
        x_col = 'batch'
        hue_col = None
        hue_palette = None
        x_label = 'Batch'
    
    # Before correction
    ax = axes[0]
    sns.boxplot(
        data=df_plot,
        x=x_col,
        y='PC1_before',
        hue=hue_col,
        palette=hue_palette,
        ax=ax,
        linewidth=1.5,
        fliersize=0,
        legend=False,  # Don't show legend on boxplot
    )
    
    sns.swarmplot(
        data=df_plot,
        x=x_col,
        y='PC1_before',
        hue=hue_col,
        palette=hue_palette,
        ax=ax,
        alpha=0.7,
        size=6,
        edgecolor='black',
        linewidth=0.5,
        dodge=True,  # Important for alignment with boxplot when hue is used
    )
    
    ax.set_title("Before Batch Correction", fontsize=14, fontweight='bold')
    ax.set_xlabel(x_label, fontsize=12)
    ax.set_ylabel("PC1 Score (samples)", fontsize=12)
    ax.grid(axis='y', alpha=0.3, linestyle='--')
    
    # Fix legend - only show one
    if hue_col is not None:
        handles, labels = ax.get_legend_handles_labels()
        # Remove duplicate labels (seaborn creates duplicates for box+swarm)
        by_label = dict(zip(labels, handles))
        ax.legend(by_label.values(), by_label.keys(), 
                 title='Batch', bbox_to_anchor=(1.05, 1), loc='upper left', frameon=True)
    
    # After correction
    ax = axes[1]
    sns.boxplot(
        data=df_plot,
        x=x_col,
        y='PC1_after',
        hue=hue_col,
        palette=hue_palette,
        ax=ax,
        linewidth=1.5,
        fliersize=0,
        legend=False,  # Don't show legend on boxplot
    )
    
    sns.swarmplot(
        data=df_plot,
        x=x_col,
        y='PC1_after',
        hue=hue_col,
        palette=hue_palette,
        ax=ax,
        alpha=0.7,
        size=6,
        edgecolor='black',
        linewidth=0.5,
        dodge=True,
    )
    
    ax.set_title("After Batch Correction", fontsize=14, fontweight='bold')
    ax.set_xlabel(x_label, fontsize=12)
    ax.set_ylabel("PC1 Score (samples)", fontsize=12)
    ax.grid(axis='y', alpha=0.3, linestyle='--')
    
    # Fix legend
    if hue_col is not None:
        handles, labels = ax.get_legend_handles_labels()
        by_label = dict(zip(labels, handles))
        ax.legend(by_label.values(), by_label.keys(),
                 title='Batch', bbox_to_anchor=(1.05, 1), loc='upper left', frameon=True)
    
    plt.tight_layout()
    plt.savefig(f"{output_prefix}_batch_swarm_PC1.png", bbox_inches='tight', dpi=300)
    plt.savefig(f"{output_prefix}_batch_swarm_PC1.svg", bbox_inches='tight')
    plt.close()
    
    print(f"[✓] Saved batch swarmplot comparison (PC1)")
    
    # Create comprehensive scatter plot showing ALL features
    # Pass the first biological series (or None if not available)
    df_all_stats = plot_feature_batch_effect_scatter(
        data_before,
        data_after,
        batch,
        biological_series,  # Just the series (or None)
        output_prefix,
    )


def diagnose_batch_correction(
    data_before: pd.DataFrame,
    data_after: pd.DataFrame,
    batch: pd.Series,
    output_prefix: Path,
) -> None:
    """
    Diagnostic checks to see if batch correction is too aggressive.
    """
    print("\n" + "="*70)
    print("BATCH CORRECTION DIAGNOSTICS")
    print("="*70)
    
    # Check 1: Are batch means identical after correction?
    print("\n[1] Checking batch means after correction...")
    
    n_identical = 0
    n_features_checked = min(10, len(data_after.columns))
    
    for feature in data_after.columns[:n_features_checked]:
        batch_means = [data_after.loc[batch == b, feature].mean() for b in batch.unique()]
        batch_means_std = np.std(batch_means)
        
        if batch_means_std < 1e-10:  # Essentially zero
            n_identical += 1
    
    if n_identical == n_features_checked:
        print("    ⚠️  WARNING: All checked features have IDENTICAL batch means after correction!")
        print("    This means the correction is removing ALL between-batch variance.")
        print("    This will give η² = 0 by construction, but may remove biological signal.")
    else:
        print(f"    ✓ {n_identical}/{n_features_checked} features have identical batch means")
    
    # Check 2: Total variance retained
    print("\n[2] Checking variance retention...")
    
    var_before = data_before.var(axis=0).mean()
    var_after = data_after.var(axis=0).mean()
    var_retained_pct = (var_after / var_before * 100) if var_before > 0 else 0
    
    print(f"    Mean variance before: {var_before:.4f}")
    print(f"    Mean variance after:  {var_after:.4f}")
    print(f"    Variance retained:    {var_retained_pct:.1f}%")
    
    if var_retained_pct < 50:
        print("    ⚠️  WARNING: Less than 50% of variance retained!")
        print("    The correction may be removing too much biological signal.")
    elif var_retained_pct > 95:
        print("    ⚠️  WARNING: More than 95% of variance retained!")
        print("    The correction may not be removing enough batch effect.")
    else:
        print("    ✓ Reasonable variance retention")
    
    # Check 3: Within-batch variance vs between-batch variance
    print("\n[3] Checking variance decomposition...")
    
    # Calculate for a sample of features
    within_before = []
    between_before = []
    within_after = []
    between_after = []
    
    for feature in data_before.columns[:50]:  # Sample 50 features
        # Before
        grand_mean_before = data_before[feature].mean()
        
        # Within-batch variance (pooled)
        within_var_before = 0
        for b in batch.unique():
            group = data_before.loc[batch == b, feature]
            within_var_before += np.sum((group - group.mean())**2)
        within_var_before /= (len(data_before) - len(batch.unique()))
        
        # Between-batch variance
        between_var_before = 0
        for b in batch.unique():
            group = data_before.loc[batch == b, feature]
            between_var_before += len(group) * (group.mean() - grand_mean_before)**2
        between_var_before /= (len(batch.unique()) - 1)
        
        within_before.append(within_var_before)
        between_before.append(between_var_before)
        
        # After
        grand_mean_after = data_after[feature].mean()
        
        within_var_after = 0
        for b in batch.unique():
            group = data_after.loc[batch == b, feature]
            within_var_after += np.sum((group - group.mean())**2)
        within_var_after /= (len(data_after) - len(batch.unique()))
        
        between_var_after = 0
        for b in batch.unique():
            group = data_after.loc[batch == b, feature]
            between_var_after += len(group) * (group.mean() - grand_mean_after)**2
        between_var_after /= (len(batch.unique()) - 1)
        
        within_after.append(within_var_after)
        between_after.append(between_var_after)
    
    print(f"    Before correction:")
    print(f"      Mean within-batch variance:  {np.mean(within_before):.4f}")
    print(f"      Mean between-batch variance: {np.mean(between_before):.4f}")
    print(f"      Ratio (between/within):      {np.mean(between_before)/np.mean(within_before):.4f}")
    
    print(f"    After correction:")
    print(f"      Mean within-batch variance:  {np.mean(within_after):.4f}")
    print(f"      Mean between-batch variance: {np.mean(between_after):.4f}")
    print(f"      Ratio (between/within):      {np.mean(between_after)/np.mean(within_after) if np.mean(within_after) > 1e-10 else 0:.4f}")
    
    if np.mean(between_after) < 1e-10:
        print("    ⚠️  WARNING: Between-batch variance is essentially ZERO after correction!")
        print("    This indicates very strong correction and possible over-adjustment.")
        print("    Re-check batch/covariate specification and ConQuR settings.")
    
    # Check 4: Method note
    print("\n[4] Checking correction method...")
    print("    Correction method: ConQuR (count-space batch correction)")
    print("    Verify covariates are biologically meaningful and sufficiently complete.")
    
    # Recommendations
    print("\n" + "="*70)
    print("RECOMMENDATIONS:")
    print("="*70)
    
    if np.mean(between_after) < 1e-10:
        print("❌ Correction appears too aggressive for this dataset.")
        print("\nWhat's happening:")
        print("  - Between-batch variance is effectively collapsed to zero")
        print("  - But it also removes real biological differences between batches")
        print("\nSolutions:")
        print("  1. Use biological covariates with clear biological meaning")
        print("     Add --biological-covariates to your command")
        print("\n  2. Tune ConQuR settings:")
        print("     --conqur-mode, --conqur-quantile-type, --conqur-simple-match, --conqur-delta")
        print("\n  3. Accept the limitation:")
        print("     If batches are confounded with biology, no correction method can")
        print("     perfectly separate them. The η² = 0 is expected in this case.")
    else:
        print("✓ Correction appears to be working properly")
    
    print("="*70 + "\n")
    
    # Create visualization
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    
    # Plot 1: Variance decomposition before
    ax = axes[0, 0]
    ax.scatter(within_before, between_before, alpha=0.5, s=50)
    ax.set_xlabel('Within-Batch Variance')
    ax.set_ylabel('Between-Batch Variance')
    ax.set_title('Before Correction')
    ax.plot([0, max(within_before)], [0, max(within_before)], 'r--', alpha=0.5, label='Equal variance line')
    ax.legend(loc='upper left', bbox_to_anchor=(1.02, 1.0))
    ax.grid(alpha=0.3)
    
    # Plot 2: Variance decomposition after
    ax = axes[0, 1]
    ax.scatter(within_after, between_after, alpha=0.5, s=50)
    ax.set_xlabel('Within-Batch Variance')
    ax.set_ylabel('Between-Batch Variance')
    ax.set_title('After Correction')
    if max(within_after) > 1e-10:
        ax.plot([0, max(within_after)], [0, max(within_after)], 'r--', alpha=0.5, label='Equal variance line')
    ax.legend(loc='upper left', bbox_to_anchor=(1.02, 1.0))
    ax.grid(alpha=0.3)
    
    # Plot 3: Batch means distribution (sample feature)
    ax = axes[1, 0]
    sample_feature = data_before.columns[0]
    batch_means_before = [data_before.loc[batch == b, sample_feature].mean() for b in sorted(batch.unique())]
    batch_means_after = [data_after.loc[batch == b, sample_feature].mean() for b in sorted(batch.unique())]
    
    x = np.arange(len(batch_means_before))
    width = 0.35
    ax.bar(x - width/2, batch_means_before, width, label='Before', alpha=0.8)
    ax.bar(x + width/2, batch_means_after, width, label='After', alpha=0.8)
    ax.set_xlabel('Batch')
    ax.set_ylabel('Mean Value')
    ax.set_title(f'Batch Means: {sample_feature[:30]}...')
    ax.set_xticks(x)
    ax.set_xticklabels(sorted(batch.unique()))
    ax.legend(loc='upper left', bbox_to_anchor=(1.02, 1.0))
    ax.grid(axis='y', alpha=0.3)
    
    # Plot 4: Total variance retained per feature
    ax = axes[1, 1]
    var_retained = []
    for feature in data_before.columns:
        vb = data_before[feature].var()
        va = data_after[feature].var()
        if vb > 1e-10:
            var_retained.append(va / vb * 100)
    
    ax.hist(var_retained, bins=50, edgecolor='black', alpha=0.7)
    ax.axvline(np.mean(var_retained), color='red', linestyle='--', linewidth=2, label=f'Mean: {np.mean(var_retained):.1f}%')
    ax.set_xlabel('Variance Retained (%)')
    ax.set_ylabel('Number of Features')
    ax.set_title('Distribution of Variance Retention')
    ax.legend(loc='upper left', bbox_to_anchor=(1.02, 1.0))
    ax.grid(axis='y', alpha=0.3)
    
    plt.tight_layout()
    save_png_and_svg(f"{output_prefix}_correction_diagnostics.png")
    plt.close()
    
    print(f"[✓] Saved diagnostic plots to: {output_prefix}_correction_diagnostics.png\n")


def compute_batch_statistics(
    data_before: pd.DataFrame,
    data_after: pd.DataFrame,
    batch: pd.Series,
    output_file: Path,
) -> pd.DataFrame:
    """
    Compute statistics quantifying batch effects before/after correction.
    All metrics evaluate how SAMPLES cluster by batch vs biological signal.
    """
    from sklearn.metrics import silhouette_score
    from scipy.stats import f_oneway
    
    stats_list = []
    
    # PCA-based metrics on SAMPLES
    pca_before = PCA(n_components=2)
    pc_before = pca_before.fit_transform(data_before.values)
    
    pca_after = PCA(n_components=2)
    pc_after = pca_after.fit_transform(data_after.values)
    
    # Silhouette score (lower is better for batch correction)
    sil_before = silhouette_score(pc_before, batch)
    sil_after = silhouette_score(pc_after, batch)
    
    stats_list.append({
        'metric': 'Silhouette Score (Batch)',
        'before': sil_before,
        'after': sil_after,
        'change': sil_after - sil_before,
        'interpretation': 'Lower is better (less batch separation)'
    })
    
    # Variance explained by batch (PC1)
    _, p_before = f_oneway(*[pc_before[batch == b, 0] for b in batch.unique()])
    _, p_after = f_oneway(*[pc_after[batch == b, 0] for b in batch.unique()])
    
    stats_list.append({
        'metric': 'PC1 Batch F-test p-value',
        'before': p_before,
        'after': p_after,
        'change': p_after - p_before,
        'interpretation': 'Higher p-value is better (less batch effect)'
    })
    
    # Variance explained by first PC
    stats_list.append({
        'metric': 'PC1 Variance Explained (%)',
        'before': pca_before.explained_variance_ratio_[0] * 100,
        'after': pca_after.explained_variance_ratio_[0] * 100,
        'change': (pca_after.explained_variance_ratio_[0] - 
                   pca_before.explained_variance_ratio_[0]) * 100,
        'interpretation': 'Change indicates structure preservation'
    })
    
    df_stats = pd.DataFrame(stats_list)
    df_stats.to_csv(output_file, sep='\t', index=False)
    
    print("\n" + "="*70)
    print("BATCH CORRECTION STATISTICS")
    print("="*70)
    print(df_stats.to_string(index=False))
    print("="*70 + "\n")
    
    return df_stats


def _safe_spearman(x: np.ndarray, y: np.ndarray) -> float:
    """Spearman correlation with guardrails for constant/invalid vectors."""
    from scipy.stats import spearmanr

    x = np.asarray(x, dtype=float)
    y = np.asarray(y, dtype=float)
    mask = np.isfinite(x) & np.isfinite(y)
    if mask.sum() < 3:
        return np.nan
    x = x[mask]
    y = y[mask]
    if np.std(x) < 1e-12 or np.std(y) < 1e-12:
        return np.nan
    rho = spearmanr(x, y).correlation
    return float(rho) if np.isfinite(rho) else np.nan


def _eta_squared_1way(values: np.ndarray, groups: pd.Series) -> float:
    """One-way ANOVA effect size eta-squared."""
    vals = np.asarray(values, dtype=float).reshape(-1)
    grp = pd.Series(groups).astype(str).values
    mask = np.isfinite(vals)
    vals = vals[mask]
    grp = grp[mask]
    if len(vals) < 3:
        return np.nan
    uniq = pd.unique(grp)
    if len(uniq) < 2:
        return np.nan
    grand = np.mean(vals)
    ss_total = np.sum((vals - grand) ** 2)
    if ss_total <= 1e-12:
        return np.nan
    ss_between = 0.0
    for g in uniq:
        group_vals = vals[grp == g]
        if len(group_vals) == 0:
            continue
        ss_between += len(group_vals) * (np.mean(group_vals) - grand) ** 2
    return float(ss_between / ss_total)


def plot_countspace_preservation(
    raw_counts: pd.DataFrame,
    corrected_counts_float: pd.DataFrame,
    corrected_counts_pseudo: pd.DataFrame,
    batch: pd.Series,
    output_prefix: Path,
    biological_series: Optional[pd.Series] = None,
    biological_name: str = "Biological",
) -> pd.DataFrame:
    """
    Compare raw counts vs corrected tables to quantify relationship preservation.

    Outputs:
      - {output_prefix}_countspace_preservation.png/.svg
      - {output_prefix}_countspace_preservation_metrics.tsv
    """
    from scipy.spatial.distance import pdist

    common_samples = raw_counts.index.intersection(corrected_counts_float.index).intersection(corrected_counts_pseudo.index)
    common_features = raw_counts.columns.intersection(corrected_counts_float.columns).intersection(corrected_counts_pseudo.columns)
    if len(common_samples) == 0 or len(common_features) == 0:
        raise ValueError("No overlap across raw/float/pseudocount tables for preservation diagnostics.")

    raw = raw_counts.loc[common_samples, common_features].apply(pd.to_numeric, errors="coerce").fillna(0.0).clip(lower=0.0)
    cor_f = corrected_counts_float.loc[common_samples, common_features].apply(pd.to_numeric, errors="coerce").fillna(0.0).clip(lower=0.0)
    cor_p = corrected_counts_pseudo.loc[common_samples, common_features].apply(pd.to_numeric, errors="coerce").fillna(0.0).clip(lower=0.0)

    batch = batch.reindex(common_samples).astype(str)
    bio = None
    if biological_series is not None:
        bio = biological_series.reindex(common_samples).astype(str).fillna("MISSING")

    # Sample-wise and feature-wise rank concordance.
    sample_rho_float = np.array([_safe_spearman(raw.loc[s].values, cor_f.loc[s].values) for s in common_samples], dtype=float)
    sample_rho_pseudo = np.array([_safe_spearman(raw.loc[s].values, cor_p.loc[s].values) for s in common_samples], dtype=float)
    feature_rho_float = np.array([_safe_spearman(raw[c].values, cor_f[c].values) for c in common_features], dtype=float)
    feature_rho_pseudo = np.array([_safe_spearman(raw[c].values, cor_p[c].values) for c in common_features], dtype=float)

    # Bray-Curtis distance preservation (sample-to-sample geometry).
    raw_rel = raw.div(raw.sum(axis=1).replace(0, np.nan), axis=0).fillna(0.0)
    cor_f_rel = cor_f.div(cor_f.sum(axis=1).replace(0, np.nan), axis=0).fillna(0.0)
    cor_p_rel = cor_p.div(cor_p.sum(axis=1).replace(0, np.nan), axis=0).fillna(0.0)
    d_raw = pdist(raw_rel.values, metric="braycurtis")
    d_f = pdist(cor_f_rel.values, metric="braycurtis")
    d_p = pdist(cor_p_rel.values, metric="braycurtis")

    def _safe_pearson(a: np.ndarray, b: np.ndarray) -> float:
        a = np.asarray(a, dtype=float)
        b = np.asarray(b, dtype=float)
        mask = np.isfinite(a) & np.isfinite(b)
        if mask.sum() < 3:
            return np.nan
        a = a[mask]
        b = b[mask]
        if np.std(a) < 1e-12 or np.std(b) < 1e-12:
            return np.nan
        return float(np.corrcoef(a, b)[0, 1])

    bray_pearson_float = _safe_pearson(d_raw, d_f)
    bray_pearson_pseudo = _safe_pearson(d_raw, d_p)
    bray_spearman_float = _safe_spearman(d_raw, d_f)
    bray_spearman_pseudo = _safe_spearman(d_raw, d_p)

    # PC1 effect sizes in CLR space: batch should drop, biology should be retained.
    def _clr_pc1(df: pd.DataFrame) -> np.ndarray:
        arr = multiplicative_replacement(df.to_numpy(dtype=float))
        clr_arr = clr(arr)
        return PCA(n_components=1).fit_transform(clr_arr).ravel()

    pc1_raw = _clr_pc1(raw)
    pc1_f = _clr_pc1(cor_f)
    pc1_p = _clr_pc1(cor_p)

    eta_batch_raw = _eta_squared_1way(pc1_raw, batch)
    eta_batch_float = _eta_squared_1way(pc1_f, batch)
    eta_batch_pseudo = _eta_squared_1way(pc1_p, batch)

    eta_bio_raw = np.nan
    eta_bio_float = np.nan
    eta_bio_pseudo = np.nan
    if bio is not None:
        eta_bio_raw = _eta_squared_1way(pc1_raw, bio)
        eta_bio_float = _eta_squared_1way(pc1_f, bio)
        eta_bio_pseudo = _eta_squared_1way(pc1_p, bio)

    metrics = pd.DataFrame({
        "metric": [
            "sample_spearman_median_raw_vs_float",
            "sample_spearman_median_raw_vs_pseudo",
            "feature_spearman_median_raw_vs_float",
            "feature_spearman_median_raw_vs_pseudo",
            "bray_distance_pearson_raw_vs_float",
            "bray_distance_pearson_raw_vs_pseudo",
            "bray_distance_spearman_raw_vs_float",
            "bray_distance_spearman_raw_vs_pseudo",
            "eta_batch_pc1_raw",
            "eta_batch_pc1_corrected_float",
            "eta_batch_pc1_corrected_pseudo",
            f"eta_{biological_name}_pc1_raw",
            f"eta_{biological_name}_pc1_corrected_float",
            f"eta_{biological_name}_pc1_corrected_pseudo",
        ],
        "value": [
            float(np.nanmedian(sample_rho_float)),
            float(np.nanmedian(sample_rho_pseudo)),
            float(np.nanmedian(feature_rho_float)),
            float(np.nanmedian(feature_rho_pseudo)),
            bray_pearson_float,
            bray_pearson_pseudo,
            bray_spearman_float,
            bray_spearman_pseudo,
            eta_batch_raw,
            eta_batch_float,
            eta_batch_pseudo,
            eta_bio_raw,
            eta_bio_float,
            eta_bio_pseudo,
        ],
    })
    metrics.to_csv(f"{output_prefix}_countspace_preservation_metrics.tsv", sep="\t", index=False)

    # Figure panels.
    fig, axes = plt.subplots(2, 2, figsize=(15, 11))

    # (1) Sample-wise correlation distribution.
    sample_plot_df = pd.DataFrame({
        "comparison": (["raw vs corrected(float)"] * len(sample_rho_float)) + (["raw vs corrected(pseudo)"] * len(sample_rho_pseudo)),
        "rho": np.concatenate([sample_rho_float, sample_rho_pseudo]),
    }).dropna(subset=["rho"])
    sns.boxplot(data=sample_plot_df, x="comparison", y="rho", ax=axes[0, 0], color="#c6dbef")
    sns.stripplot(data=sample_plot_df, x="comparison", y="rho", ax=axes[0, 0], color="#08519c", alpha=0.35, size=3)
    axes[0, 0].set_title("Per-Sample Spearman (feature profile preservation)")
    axes[0, 0].set_ylabel("Spearman rho")
    axes[0, 0].set_xlabel("")
    axes[0, 0].grid(axis="y", alpha=0.3)

    # (2) Feature-wise correlation distribution.
    feature_plot_df = pd.DataFrame({
        "comparison": (["raw vs corrected(float)"] * len(feature_rho_float)) + (["raw vs corrected(pseudo)"] * len(feature_rho_pseudo)),
        "rho": np.concatenate([feature_rho_float, feature_rho_pseudo]),
    }).dropna(subset=["rho"])
    sns.boxplot(data=feature_plot_df, x="comparison", y="rho", ax=axes[0, 1], color="#fee0d2")
    sns.stripplot(data=feature_plot_df, x="comparison", y="rho", ax=axes[0, 1], color="#a50f15", alpha=0.25, size=2)
    axes[0, 1].set_title("Per-Feature Spearman (across-sample preservation)")
    axes[0, 1].set_ylabel("Spearman rho")
    axes[0, 1].set_xlabel("")
    axes[0, 1].grid(axis="y", alpha=0.3)

    # (3) Mean abundance relationship.
    mean_raw = np.log10(raw.mean(axis=0).to_numpy(dtype=float) + 1.0)
    mean_f = np.log10(cor_f.mean(axis=0).to_numpy(dtype=float) + 1.0)
    mean_p = np.log10(cor_p.mean(axis=0).to_numpy(dtype=float) + 1.0)
    axes[1, 0].scatter(mean_raw, mean_f, s=16, alpha=0.45, c="#3182bd", label="corrected(float)")
    axes[1, 0].scatter(mean_raw, mean_p, s=16, alpha=0.45, c="#e6550d", label="corrected(pseudo)")
    min_lim = float(np.nanmin([mean_raw.min(), mean_f.min(), mean_p.min()]))
    max_lim = float(np.nanmax([mean_raw.max(), mean_f.max(), mean_p.max()]))
    axes[1, 0].plot([min_lim, max_lim], [min_lim, max_lim], "k--", linewidth=1)
    axes[1, 0].set_title("Feature Mean Abundance: raw vs corrected")
    axes[1, 0].set_xlabel("log10(mean raw count + 1)")
    axes[1, 0].set_ylabel("log10(mean corrected count + 1)")
    axes[1, 0].legend(frameon=True, loc='upper left', bbox_to_anchor=(1.02, 1.0))
    axes[1, 0].grid(alpha=0.25)

    # (4) Effect sizes summary.
    states = ["Raw", "Corrected(float)", "Corrected(pseudo)"]
    x = np.arange(len(states))
    width = 0.35
    batch_vals = [eta_batch_raw, eta_batch_float, eta_batch_pseudo]
    if np.all(np.isfinite([eta_bio_raw, eta_bio_float, eta_bio_pseudo])):
        bio_vals = [eta_bio_raw, eta_bio_float, eta_bio_pseudo]
        axes[1, 1].bar(x - width / 2, batch_vals, width=width, color="#31a354", label="Batch eta^2 (lower better)")
        axes[1, 1].bar(x + width / 2, bio_vals, width=width, color="#756bb1", label=f"{biological_name} eta^2 (retain signal)")
    else:
        axes[1, 1].bar(x, batch_vals, width=0.6, color="#31a354", label="Batch eta^2 (lower better)")
    axes[1, 1].set_xticks(x)
    axes[1, 1].set_xticklabels(states)
    axes[1, 1].set_ylim(bottom=0)
    axes[1, 1].set_ylabel("Eta-squared (PC1, CLR space)")
    axes[1, 1].set_title("Batch-effect attenuation and biological retention")
    axes[1, 1].legend(frameon=True, loc='upper left', bbox_to_anchor=(1.02, 1.0))
    axes[1, 1].grid(axis="y", alpha=0.3)

    fig.suptitle("Count-Space Preservation Diagnostics", fontsize=15, fontweight="bold")
    plt.tight_layout()
    plt.savefig(f"{output_prefix}_countspace_preservation.png", bbox_inches="tight", dpi=300)
    plt.savefig(f"{output_prefix}_countspace_preservation.svg", bbox_inches="tight")
    plt.close()

    print(f"[✓] Saved count-space preservation plot: {output_prefix}_countspace_preservation.png")
    print(f"[✓] Saved count-space preservation metrics: {output_prefix}_countspace_preservation_metrics.tsv")
    return metrics


def _metric_value(metrics: pd.DataFrame, name: str) -> float:
    hit = metrics.loc[metrics["metric"] == name, "value"]
    if hit.empty:
        return np.nan
    return float(hit.iloc[0])


def choose_correction_policy(
    metrics: pd.DataFrame,
    policy: str,
    min_sample_rho: float,
    min_bray_rho: float,
    max_batch_eta_ratio: float,
    min_batch_eta_drop: float,
    min_bio_eta_ratio: float,
    biological_name: str,
) -> Tuple[str, List[str]]:
    """Return selected source ("corrected" or "raw") plus decision reasons."""
    if policy == "always":
        return "corrected", ["correction_policy=always"]
    if policy == "never":
        return "raw", ["correction_policy=never"]

    reasons: List[str] = []
    checks: List[bool] = []

    sample_rho = _metric_value(metrics, "sample_spearman_median_raw_vs_pseudo")
    bray_rho = _metric_value(metrics, "bray_distance_spearman_raw_vs_pseudo")
    eta_batch_raw = _metric_value(metrics, "eta_batch_pc1_raw")
    eta_batch_corrected = _metric_value(metrics, "eta_batch_pc1_corrected_pseudo")
    eta_bio_raw = _metric_value(metrics, f"eta_{biological_name}_pc1_raw")
    eta_bio_corrected = _metric_value(metrics, f"eta_{biological_name}_pc1_corrected_pseudo")

    ok_sample = np.isfinite(sample_rho) and sample_rho >= min_sample_rho
    checks.append(bool(ok_sample))
    reasons.append(f"sample_spearman_pseudo={sample_rho:.4g} >= {min_sample_rho}: {ok_sample}")

    ok_bray = np.isfinite(bray_rho) and bray_rho >= min_bray_rho
    checks.append(bool(ok_bray))
    reasons.append(f"bray_spearman_pseudo={bray_rho:.4g} >= {min_bray_rho}: {ok_bray}")

    if np.isfinite(eta_batch_raw) and np.isfinite(eta_batch_corrected):
        if eta_batch_raw < min_batch_eta_drop:
            ok_batch = False
            reasons.append(
                f"eta_batch_raw={eta_batch_raw:.4g} < min_batch_eta_drop={min_batch_eta_drop}; no correction needed"
            )
        else:
            ok_batch = (
                eta_batch_corrected <= eta_batch_raw * max_batch_eta_ratio
                or (eta_batch_raw - eta_batch_corrected) >= min_batch_eta_drop
            )
            reasons.append(
                "batch_eta_pseudo="
                f"{eta_batch_corrected:.4g}, raw={eta_batch_raw:.4g}, "
                f"ratio_limit={max_batch_eta_ratio}, min_drop={min_batch_eta_drop}: {ok_batch}"
            )
    else:
        ok_batch = False
        reasons.append("batch eta metrics unavailable: False")
    checks.append(bool(ok_batch))

    if np.isfinite(eta_bio_raw) and np.isfinite(eta_bio_corrected) and eta_bio_raw > 0:
        ok_bio = eta_bio_corrected >= eta_bio_raw * min_bio_eta_ratio
        checks.append(bool(ok_bio))
        reasons.append(
            f"bio_eta_pseudo={eta_bio_corrected:.4g} >= raw({eta_bio_raw:.4g})*{min_bio_eta_ratio}: {ok_bio}"
        )
    else:
        reasons.append("biological eta unavailable or zero; skipped")

    selected = "corrected" if all(checks) else "raw"
    return selected, reasons


def write_selected_batch_outputs(
    raw_counts: pd.DataFrame,
    corrected_counts_float: pd.DataFrame,
    corrected_counts_pseudo: pd.DataFrame,
    asv_clr_before: pd.DataFrame,
    asv_clr_after: pd.DataFrame,
    selected_source: str,
    reasons: List[str],
    out_dir: Path,
) -> None:
    """Write selected raw/corrected outputs consumed by downstream ASPIRE branches."""
    use_corrected = selected_source == "corrected"
    abundance_samples = corrected_counts_float if use_corrected else raw_counts
    pseudocount_samples = corrected_counts_pseudo if use_corrected else raw_counts.round().astype(int)
    clr_selected = asv_clr_after if use_corrected else asv_clr_before

    abundance_samples.to_csv(out_dir / "asv_selected_abundance.samples_rows.tsv", sep="\t")
    abundance_samples.T.to_csv(out_dir / "asv_selected_abundance.features_rows.tsv", sep="\t")
    pseudocount_samples.to_csv(out_dir / "asv_selected_pseudocount.samples_rows.tsv", sep="\t")
    pseudocount_samples.T.to_csv(out_dir / "asv_selected_pseudocount.features_rows.tsv", sep="\t")
    clr_selected.to_csv(out_dir / "asv_clr_selected.tsv", sep="\t")

    decision = pd.DataFrame({
        "selected_source": [selected_source],
        "reason": ["; ".join(reasons)],
    })
    decision.to_csv(out_dir / "batch_correction_decision.tsv", sep="\t", index=False)
    print(f"  [i] Batch correction selected source: {selected_source}")
    for reason in reasons:
        print(f"      - {reason}")


# ============================================================================
# Main Pipeline
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Batch effect correction for 16S amplicon data using ConQuR + visualization",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    
    # I/O
    parser.add_argument("--data-dir", type=Path, required=True,
                        help="Project root directory")
    parser.add_argument("--asv", type=str, required=True,
                        help="Path to ASV table (TSV, ASVs in rows, samples in columns)")
    parser.add_argument("--metadata", type=str, required=True,
                        help="Path to metadata TSV")
    parser.add_argument("--asv-meta", type=str, required=True,
                        help="Path to asv + metadata TSV")    
    parser.add_argument("--sample-id-col", type=str, default=DEFAULT_SAMPLE_ID_COL,
                        help="Sample ID column name in metadata")
    parser.add_argument("--batch-col", required=True,
                        help="Batch column name in metadata")
    parser.add_argument("--output-dir", type=str, required=True,
                        help="Output directory for results")
    
    # Data format
    parser.add_argument("--asv-orientation", choices=["features_rows", "samples_rows"],
                        default="features_rows",
                        help="ASV table orientation (features_rows = ASVs in rows, samples in columns)")
    
    # Biological covariates to preserve
    parser.add_argument("--biological-covariates", default="",
                        help="Comma-separated biological variables to preserve (e.g., 'type_group,status')")

    # ConQuR options
    parser.add_argument("--conqur-mode", choices=["libsize", "standard"], default="libsize",
                        help="ConQuR variant: libsize-aware (ConQuR_libsize) or standard (ConQuR)")
    parser.add_argument("--conqur-num-core", type=int, default=2,
                        help="Number of cores for ConQuR")
    parser.add_argument("--conqur-batch-ref", default="",
                        help="Reference batch label for ConQuR (default: largest batch)")
    parser.add_argument("--conqur-logistic-lasso", action="store_true",
                        help="Use logistic lasso in ConQuR")
    parser.add_argument("--conqur-quantile-type", choices=["standard", "lasso"], default="standard",
                        help="Quantile regression mode for ConQuR")
    parser.add_argument("--conqur-simple-match", action="store_true",
                        help="Use simple zero/non-zero matching in ConQuR")
    parser.add_argument("--conqur-lambda-quantile", choices=["2p/n", "2p/logn"], default="2p/n",
                        help="ConQuR penalty heuristic for quantile model")
    parser.add_argument("--conqur-interplt", action="store_true",
                        help="Enable ConQuR interpolation for the CDF")
    parser.add_argument("--conqur-delta", type=float, default=0.4999,
                        help="ConQuR delta parameter")
    parser.add_argument("--conqur-auto-install", action="store_true",
                        help="Auto-install ConQuR in R env if missing (requires internet)")

    # Correction acceptance policy
    parser.add_argument("--correction-policy", choices=["auto", "always", "never"], default="auto",
                        help="Whether downstream outputs should use corrected counts, raw counts, or auto-selected counts")
    parser.add_argument("--auto-min-sample-rho", type=float, default=0.85,
                        help="Auto policy: minimum median sample-wise Spearman raw vs corrected pseudo-counts")
    parser.add_argument("--auto-min-bray-rho", type=float, default=0.75,
                        help="Auto policy: minimum Spearman preservation of Bray-Curtis distances")
    parser.add_argument("--auto-max-batch-eta-ratio", type=float, default=0.95,
                        help="Auto policy: corrected batch eta must be no more than this fraction of raw eta")
    parser.add_argument("--auto-min-batch-eta-drop", type=float, default=0.01,
                        help="Auto policy: minimum absolute batch eta drop, also used as a no-batch-effect floor")
    parser.add_argument("--auto-min-bio-eta-ratio", type=float, default=0.70,
                        help="Auto policy: corrected biological eta must retain at least this fraction of raw eta")
    
    # UMAP parameters (manual)
    parser.add_argument("--umap-neighbors", type=int, default=15,
                        help="UMAP n_neighbors (lower = more local structure)")
    parser.add_argument("--umap-min-dist", type=float, default=0.1,
                        help="UMAP min_dist (lower = tighter clusters)")
    
    # HDBSCAN parameters (manual)
    parser.add_argument("--hdbscan-min-cluster-size", type=int, default=5,
                        help="HDBSCAN min_cluster_size (lower = more clusters)")
    parser.add_argument("--hdbscan-min-samples", type=int, default=None,
                        help="HDBSCAN min_samples (lower = more clusters)")
    parser.add_argument("--hdbscan-selection-method", choices=['eom', 'leaf'], default='eom',
                        help="HDBSCAN cluster selection method (leaf = more clusters)")
    
    # Optimization
    parser.add_argument("--optimize-clustering", action="store_true",
                        help="Automatically optimize UMAP+HDBSCAN parameters (ignores manual settings)")
    parser.add_argument("--target-clusters", default="3-10",
                        help="Target cluster range for optimization (e.g., '3-8' or '5-12')")
    
    # Plotting
    parser.add_argument("--n-features-plot", type=int, default=5,
                        help="Number of top variable features to plot in swarmplots")
    parser.add_argument("--biological-color-col", default="Depth",
                        help="Comma-separated metadata columns for biological grouping (e.g., 'Depth,Month,Type')")
    parser.add_argument("--color-palette-col", default="Color",
                        help="Comma-separated metadata columns containing color hex codes (e.g., 'Color1,Color2'). "
                             "If fewer than biological columns, auto-generates remaining palettes.")
    parser.add_argument(
        "--biological-palettes-json",
        default="{}",
        help="JSON object mapping biological columns to label=color palette strings or mappings.",
    )
    
    # Misc
    parser.add_argument("--random-state", type=int, default=42,
                        help="Random state for reproducibility")
    parser.add_argument("--verbose", action="store_true",
                        help="Verbose output")
    
    args = parser.parse_args()
    
    # Parse target cluster range
    target_clusters_min, target_clusters_max = None, None
    if args.target_clusters:
        try:
            parts = args.target_clusters.split('-')
            target_clusters_min = int(parts[0])
            target_clusters_max = int(parts[1]) if len(parts) > 1 else None
        except:
            print(f"[!] Warning: Could not parse --target-clusters '{args.target_clusters}', using defaults")
    
    # Setup paths
    root = args.data_dir
    asv_path = root / args.asv if not Path(args.asv).is_absolute() else Path(args.asv)
    meta_path = root / args.metadata if not Path(args.metadata).is_absolute() else Path(args.metadata)
    asv_meta_path = root / args.asv_meta if not Path(args.asv_meta).is_absolute() else Path(args.asv_meta)
    out_dir = root / args.output_dir if not Path(args.output_dir).is_absolute() else Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    
    if args.verbose:
        print("\n" + "="*70)
        print("BATCH EFFECT CORRECTION PIPELINE")
        print("Correcting SAMPLES to remove technical batch effects (ConQuR)")
        print("="*70)
        print(f"ASV table: {asv_path}")
        print(f"Metadata: {meta_path}")
        print(f"Batch column: {args.batch_col}")
        print(f"Output: {out_dir}")
        print(
            "ConQuR: "
            f"mode={args.conqur_mode}, "
            f"quantile_type={args.conqur_quantile_type}, "
            f"num_core={args.conqur_num_core}, "
            f"batch_ref={(args.conqur_batch_ref if args.conqur_batch_ref else 'largest batch')}"
        )
        if args.optimize_clustering:
            print(f"Clustering: AUTO-OPTIMIZING (target: {target_clusters_min}-{target_clusters_max} clusters)")
        else:
            print(f"UMAP: n_neighbors={args.umap_neighbors}, min_dist={args.umap_min_dist}")
            print(f"HDBSCAN: min_cluster_size={args.hdbscan_min_cluster_size}, "
                  f"min_samples={args.hdbscan_min_samples}, method={args.hdbscan_selection_method}")
        print("="*70 + "\n")
    
    # Load data
    print("[1/9] Loading data...")
    asv_raw = load_asv_table(asv_path, args.asv_orientation)
    meta_index_col = args.sample_id_col
    metadata = load_metadata(meta_path, meta_index_col)
    
    asv_meta = pd.read_csv(asv_meta_path, sep="\t", header=0)

    # Align
    print("[2/9] Aligning samples...")
    asv_raw, metadata = align_data(asv_raw, metadata)
    print(f"  Final aligned shape: {asv_raw.shape} (samples x features)")
    print(f"  This means we will correct {asv_raw.shape[0]} samples")
    
    # Check batch column
    if args.batch_col not in metadata.columns:
        raise ValueError(f"Batch column '{args.batch_col}' not found in metadata")
    
    batch_series = metadata[args.batch_col].astype(str)
    n_batches = len(batch_series.unique())
    print(f"  Found {n_batches} batches: {sorted(batch_series.unique())}")
    
    # Get biological covariates if specified
    bio_cov = None
    bio_color_data = []  # List of (series, palette_dict, name) tuples
    
    if args.biological_covariates:
        bio_cols = [c.strip() for c in args.biological_covariates.split(",")]
        bio_cov = metadata[bio_cols].copy()
        print(f"  Biological covariates to preserve: {bio_cols}")
    
    # Parse biological color columns
    if args.biological_color_col:
        bio_color_cols = [c.strip() for c in args.biological_color_col.split(",") if c.strip()]
        palette_cols = [c.strip() for c in args.color_palette_col.split(",") if c.strip()]
        try:
            explicit_bio_palettes = json.loads(args.biological_palettes_json)
        except json.JSONDecodeError:
            explicit_bio_palettes = {}
        if not isinstance(explicit_bio_palettes, dict):
            explicit_bio_palettes = {}
        
        print(f"\n  Biological metadata for visualization: {bio_color_cols}")
        
        # Process each biological column
        for idx, bio_col in enumerate(bio_color_cols):
            if bio_col not in metadata.columns:
                print(f"    [!] Warning: Column '{bio_col}' not found in metadata, skipping")
                continue
            
            bio_series = metadata[bio_col]
            bio_values_sorted = _sorted_non_null_unique(bio_series)
            if not bio_values_sorted:
                print(f"    [!] Warning: Column '{bio_col}' has no non-null values, skipping")
                continue
            
            explicit_palette = explicit_bio_palettes.get(bio_col)
            if explicit_palette:
                if isinstance(explicit_palette, str):
                    entries = [item.strip() for item in explicit_palette.split(",") if "=" in item]
                    bio_palette = {
                        label.strip(): _safe_color(color.strip())
                        for label, color in (item.split("=", 1) for item in entries)
                    }
                elif isinstance(explicit_palette, dict):
                    bio_palette = {
                        str(label): _safe_color(color)
                        for label, color in explicit_palette.items()
                    }
                else:
                    bio_palette = {}
                normalized_palette = {}
                missing_vals = []
                for value in bio_values_sorted:
                    if value in bio_palette:
                        normalized_palette[value] = bio_palette[value]
                    elif str(value) in bio_palette:
                        normalized_palette[value] = bio_palette[str(value)]
                    else:
                        missing_vals.append(value)
                fallback_colors = sns.color_palette("husl", len(missing_vals))
                for value, color in zip(missing_vals, fallback_colors):
                    normalized_palette[value] = plt.matplotlib.colors.rgb2hex(color)
                bio_palette = normalized_palette
                print(f"    ✓ {bio_col}: using explicit biological palette")
            # Check if we have a palette column for this biological column
            elif idx < len(palette_cols) and palette_cols[idx] in metadata.columns:
                # Use specified palette from metadata
                palette_col = palette_cols[idx]
                palette_df = metadata[[bio_col, palette_col]].dropna(subset=[bio_col]).drop_duplicates(subset=[bio_col], keep='first')
                bio_palette = {row[bio_col]: _safe_color(row[palette_col]) for _, row in palette_df.iterrows()}
                # Fill categories missing in palette column with generated colors
                missing_vals = [v for v in bio_values_sorted if v not in bio_palette]
                if missing_vals:
                    fallback_colors = sns.color_palette("husl", len(missing_vals))
                    for val, c in zip(missing_vals, fallback_colors):
                        bio_palette[val] = plt.matplotlib.colors.rgb2hex(c)
                print(f"    ✓ {bio_col}: using colors from '{palette_col}' column")
            else:
                # Auto-generate palette
                n_categories = len(bio_values_sorted)
                if n_categories <= 10:
                    palette = sns.color_palette("tab10", n_categories)
                elif n_categories <= 20:
                    palette = sns.color_palette("tab20", n_categories)
                else:
                    palette = sns.color_palette("husl", n_categories)
                
                bio_palette = dict(zip(bio_values_sorted,
                                     [plt.matplotlib.colors.rgb2hex(c) for c in palette]))
                print(f"    ✓ {bio_col}: auto-generated palette ({n_categories} categories)")
            
            bio_color_data.append((bio_series, bio_palette, bio_col))
    
    # CLR transformation
    print("[3/9] Applying CLR transformation to samples...")
    asv_clr_before = apply_clr_transform(asv_raw)
    
    # Batch correction (ConQuR on raw counts)
    print("[4/9] Performing batch correction on samples with ConQuR...")
    print("  [i] This corrects technical batch effects in raw count space.")
    corrected_counts = conqur_correction_wrapper(
        asv_counts=asv_raw,
        metadata=metadata,
        batch_col=args.batch_col,
        biological_covariates=bio_cov,
        num_core=args.conqur_num_core,
        use_libsize=(args.conqur_mode == "libsize"),
        batch_ref=args.conqur_batch_ref,
        logistic_lasso=args.conqur_logistic_lasso,
        quantile_type=args.conqur_quantile_type,
        simple_match=args.conqur_simple_match,
        lambda_quantile=args.conqur_lambda_quantile,
        interplt=args.conqur_interplt,
        delta=args.conqur_delta,
        auto_install=args.conqur_auto_install,
        random_state=args.random_state,
    )
    corrected_counts.to_csv(out_dir / "asv_corrected_counts.samples_rows.tsv", sep="\t")

    # CLR views for diagnostics/outlier steps.
    asv_clr_after = apply_clr_transform(corrected_counts)

    # Safety guard for clearly pathological corrected compositions.
    before_fxs = asv_clr_before.T.to_numpy(dtype=float)
    after_fxs = asv_clr_after.T.to_numpy(dtype=float)
    if _is_pathological_correction(before_fxs, after_fxs, spread_ratio_max=8.0, max_abs_max=80.0):
        ratio, max_abs_after, p95_abs_delta = _correction_guard_stats(before_fxs, after_fxs)
        raise RuntimeError(
            "ConQuR output looks pathological in CLR space "
            f"(spread_ratio={ratio:.2f}, max_abs={max_abs_after:.2f}, p95|delta|={p95_abs_delta:.2f}). "
            "Check batch/covariate specification."
        )
    
    print(f"  [✓] Correction complete: {asv_clr_after.shape} (samples x features)")
    
    # Save corrected data
    asv_clr_before.to_csv(out_dir / "asv_clr_before_correction.tsv", sep="\t")
    asv_clr_after.to_csv(out_dir / "asv_clr_after_correction.tsv", sep="\t")
    print(f"  [✓] Saved CLR-transformed data (before/after)")
    asv_clr_after_stack = asv_clr_after.stack().reset_index()
    asv_clr_after_stack.columns = [meta_index_col, 'ASV_ID', 'batch_corrected_clr']

    # Use configured sample-id column for merge, with legacy fallback for older ASV-meta files.
    join_sample_col = meta_index_col
    if join_sample_col not in asv_meta.columns:
        legacy_candidates = [DEFAULT_SAMPLE_ID_COL, "longID", "sample"]
        legacy_col = next((c for c in legacy_candidates if c in asv_meta.columns), None)
        if legacy_col is None:
            raise ValueError(
                f"ASV metadata is missing sample column '{join_sample_col}'. "
                f"Available columns: {list(asv_meta.columns)}"
            )
        print(
            f"  [w] ASV metadata missing '{join_sample_col}'; using legacy sample column '{legacy_col}'"
        )
        asv_meta = asv_meta.rename(columns={legacy_col: join_sample_col})

    if "ASV_ID" not in asv_meta.columns:
        raise ValueError(
            f"ASV metadata is missing required column 'ASV_ID'. "
            f"Available columns: {list(asv_meta.columns)}"
        )

    asv_meta[join_sample_col] = asv_meta[join_sample_col].astype(str)
    asv_meta["ASV_ID"] = asv_meta["ASV_ID"].astype(str)
    asv_clr_after_stack[join_sample_col] = asv_clr_after_stack[join_sample_col].astype(str)
    asv_clr_after_stack["ASV_ID"] = asv_clr_after_stack["ASV_ID"].astype(str)

    asv_clr_after_stack = asv_meta.merge(
        asv_clr_after_stack, on=["ASV_ID", join_sample_col], how="left"
    )
    asv_clr_after_stack.to_csv(out_dir / "asv_clr_after_correction_with_metadata.tsv", sep="\t", index=False)
    print(f"  [✓] Saved CLR-transformed data with ASV metadata")

    corrected_samples_path, corrected_features_path, corrected_int_samples_path, corrected_int_features_path = export_corrected_abundance_table(
        corrected_counts=corrected_counts,
        asv_raw_counts=asv_raw,
        out_dir=out_dir,
    )
    print(f"  [✓] Saved diversity-compatible corrected abundance tables")
    print(f"      - {corrected_samples_path}")
    print(f"      - {corrected_features_path}")
    print(f"      - {corrected_int_samples_path}")
    print(f"      - {corrected_int_features_path}")

    corrected_counts_float = pd.read_csv(corrected_samples_path, sep="\t", index_col=0)
    corrected_counts_pseudo = pd.read_csv(corrected_int_samples_path, sep="\t", index_col=0)
    primary_bio_series = bio_color_data[0][0] if bio_color_data else None
    primary_bio_name = bio_color_data[0][2] if bio_color_data else "Biological"
    print("[5/9] Generating count-space preservation diagnostics...")
    preservation_metrics = plot_countspace_preservation(
        raw_counts=asv_raw,
        corrected_counts_float=corrected_counts_float,
        corrected_counts_pseudo=corrected_counts_pseudo,
        batch=batch_series,
        output_prefix=out_dir / "batch_correction",
        biological_series=primary_bio_series,
        biological_name=primary_bio_name,
    )

    # UMAP + HDBSCAN
    print("[6/9] Computing UMAP embeddings and HDBSCAN clusters for samples...")
    
    # Before correction
    umap_before, clusters_before, opt_results_before = compute_umap_hdbscan(
        asv_clr_before,
        n_neighbors=args.umap_neighbors,
        min_dist=args.umap_min_dist,
        min_cluster_size=args.hdbscan_min_cluster_size,
        min_samples=args.hdbscan_min_samples,
        cluster_selection_method=args.hdbscan_selection_method,
        random_state=args.random_state,
        optimize_params=args.optimize_clustering,
        target_clusters_min=target_clusters_min,
        target_clusters_max=target_clusters_max,
    )
    
    # After correction
    umap_after, clusters_after, opt_results_after = compute_umap_hdbscan(
        asv_clr_after,
        n_neighbors=args.umap_neighbors,
        min_dist=args.umap_min_dist,
        min_cluster_size=args.hdbscan_min_cluster_size,
        min_samples=args.hdbscan_min_samples,
        cluster_selection_method=args.hdbscan_selection_method,
        random_state=args.random_state,
        optimize_params=args.optimize_clustering,
        target_clusters_min=target_clusters_min,
        target_clusters_max=target_clusters_max,
    )
    
    print(f"  Clusters before: {len(set(clusters_before)) - (1 if -1 in clusters_before else 0)}")
    print(f"  Clusters after: {len(set(clusters_after)) - (1 if -1 in clusters_after else 0)}")
    
    # Save optimization results if performed
    if args.optimize_clustering:
        if opt_results_before is not None:
            opt_results_before.to_csv(out_dir / "clustering_optimization_before.tsv", sep="\t", index=False)
            plot_optimization_results(opt_results_before, out_dir / "clustering_optimization_before.png")
        if opt_results_after is not None:
            opt_results_after.to_csv(out_dir / "clustering_optimization_after.tsv", sep="\t", index=False)
            plot_optimization_results(opt_results_after, out_dir / "clustering_optimization_after.png")
    
        # Save embeddings and clusters
    results_df = pd.DataFrame({
        'sample': asv_clr_before.index,
        'batch': batch_series.values,
        'umap1_before': umap_before[:, 0],
        'umap2_before': umap_before[:, 1],
        'cluster_before': clusters_before,
        'umap1_after': umap_after[:, 0],
        'umap2_after': umap_after[:, 1],
        'cluster_after': clusters_after,
    })
    
    # Add all biological metadata columns to results
    if bio_color_data:
        for bio_series, bio_palette, bio_name in bio_color_data:
            results_df[bio_name] = bio_series.values
    
    results_df.to_csv(out_dir / "umap_hdbscan_results.tsv", sep="\t", index=False)
    print(f"  [✓] Saved UMAP/HDBSCAN results")
    
    # Plot UMAP comparison
    print("[7/9] Generating UMAP comparison plots...")
    plot_umap_comparison(
        umap_before,
        umap_after,
        clusters_before,
        clusters_after,
        batch_series,
        bio_color_data,  # Changed from single to list
        out_dir / "batch_correction",
    )
    
    # Plot swarm comparison
    print("[8/9] Generating swarmplot comparisons...")
    plot_batch_swarm_comparison(
        asv_clr_before,
        asv_clr_after,
        batch_series,
        bio_color_data,  # Pass the list
        out_dir / "batch_correction",
        n_features_to_plot=args.n_features_plot,
    )
    
    # Compute statistics
    print("[9/9] Computing batch effect statistics...")
    stats_df = compute_batch_statistics(
        asv_clr_before,
        asv_clr_after,
        batch_series,
        out_dir / "batch_correction_statistics.tsv",
    )
    selected_source, decision_reasons = choose_correction_policy(
        metrics=preservation_metrics,
        policy=args.correction_policy,
        min_sample_rho=args.auto_min_sample_rho,
        min_bray_rho=args.auto_min_bray_rho,
        max_batch_eta_ratio=args.auto_max_batch_eta_ratio,
        min_batch_eta_drop=args.auto_min_batch_eta_drop,
        min_bio_eta_ratio=args.auto_min_bio_eta_ratio,
        biological_name=primary_bio_name,
    )
    write_selected_batch_outputs(
        raw_counts=asv_raw,
        corrected_counts_float=corrected_counts_float,
        corrected_counts_pseudo=corrected_counts_pseudo,
        asv_clr_before=asv_clr_before,
        asv_clr_after=asv_clr_after,
        selected_source=selected_source,
        reasons=decision_reasons,
        out_dir=out_dir,
    )
    
    print("\n" + "="*70)
    print("BATCH CORRECTION COMPLETE")
    print("="*70)
    print(f"Corrected {asv_clr_after.shape[0]} samples across {asv_clr_after.shape[1]} features")
    print(f"\nOutput directory: {out_dir}")
    print(f"\nKey files:")
    print(f"  - asv_clr_before_correction.tsv (samples x features)")
    print(f"  - asv_clr_after_correction.tsv (samples x features, batch-corrected)")
    print(f"  - asv_corrected_abundance.samples_rows.tsv (samples x features, non-negative)")
    print(f"  - asv_corrected_abundance.features_rows.tsv (features x samples, calc_div-compatible)")
    print(f"  - asv_corrected_pseudocount.samples_rows.tsv (samples x features, integer pseudo-counts)")
    print(f"  - asv_corrected_pseudocount.features_rows.tsv (features x samples, integer pseudo-counts)")
    print(f"  - batch_correction_countspace_preservation.png/svg")
    print(f"  - batch_correction_countspace_preservation_metrics.tsv")
    print(f"  - umap_hdbscan_results.tsv")
    print(f"  - batch_correction_umap_comparison.png/svg")
    print(f"  - batch_correction_batch_swarm_PC1.png/svg")
    print(f"  - batch_correction_batch_swarm_features.png/svg")
    print(f"  - batch_correction_statistics.tsv")
    if args.optimize_clustering:
        print(f"  - clustering_optimization_before.tsv/pdf")
        print(f"  - clustering_optimization_after.tsv/pdf")
    print("="*70 + "\n")


if __name__ == "__main__":
    main()

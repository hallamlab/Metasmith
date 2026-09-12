#!/usr/bin/env python3
"""
asv_outliers_ensemble.py
Ensemble outlier detection over ASV tables with CLR (optional), per-group training,
and consensus voting across IsolationForest, OneClassSVM, and HDBSCAN.

Quickstart (original workflow with raw counts):
  python asv_outliers_ensemble.py \
    --data-dir /home/ryan/SeqData/SeqData/UBC/LMP_priority1 \
    --asv spark_combined_output/ASVs/ASV_final.micro.tsv \
    --metadata spark_combined_output/metadata/metadata_updated.tsv \
    --group-cols none,type_group \
    --output-dir spark_combined_output/metadata \
    --transform clr --hdbscan-min-cluster-size 5 --svm-nu 0.1 \
    --vote-threshold 3 --verbose

Quickstart (with batch-corrected CLR data):
  python asv_outliers_ensemble.py \
    --data-dir /home/ryan/SeqData/SeqData/UBC/LMP_priority1 \
    --asv spark_combined_output/batch_correction/asv_clr_after_correction.tsv \
    --metadata spark_combined_output/metadata/metadata_updated.tsv \
    --group-cols none,type_group \
    --output-dir spark_combined_output/outlier_detection_corrected \
    --asv-orientation samples_rows \
    --pre-transformed \
    --hdbscan-min-cluster-size 5 --svm-nu 0.1 \
    --vote-threshold 3 --verbose

Notes
-----
- By default the ASV file is assumed to be features (ASVs) in rows and samples in columns;
  pass --asv-orientation samples_rows if yours is already samples x features.
- For compositional data, `--transform clr` applies multiplicative replacement then CLR.
- If using batch-corrected data from asv_batch_correction.py, use:
  --asv-orientation samples_rows --pre-transformed --transform none
- Grouping:
    --group-cols can include 'none' (treat all samples together) and/or metadata column names.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd
import matplotlib as mpl
import matplotlib.pyplot as plt
import seaborn as sns

from sklearn.ensemble import IsolationForest
from sklearn.svm import OneClassSVM
from sklearn.preprocessing import StandardScaler

import hdbscan
from hdbscan import approximate_predict

# Optional compositional transforms (CLR)
from skbio.stats.composition import clr
try:
    # skbio <=0.6
    from skbio.stats.composition import multiplicative_replacement
except ImportError:
    # skbio >=0.7 renamed this helper
    from skbio.stats.composition import multi_replace as multiplicative_replacement

DEFAULT_SAMPLE_ID_COL = 'sampleID'
FALLBACK_SAMPLE_ID_COLS = ("sampleID", "sample", "longID")

mpl.rcParams["pdf.fonttype"] = 42
mpl.rcParams["svg.fonttype"] = "none"


def write_outlier_summary_plot(preds: pd.DataFrame, tag: str, out_dir: Path) -> None:
    detector_cols = [col for col in ("IsolationForest", "OneClassSVM", "HDBSCAN") if col in preds]
    summary = pd.DataFrame({
        "detector": detector_cols + ["Consensus"],
        "outlier_count": [int((preds[col] == -1).sum()) for col in detector_cols]
        + [int(preds["is_outlier"].astype(bool).sum())],
    })
    summary["sample_count"] = len(preds)
    summary["outlier_fraction"] = summary["outlier_count"] / max(len(preds), 1)
    summary.to_csv(out_dir / f"outliers_{tag}_summary.tsv", sep="\t", index=False)

    fig, axes = plt.subplots(1, 2, figsize=(11, 4.5))
    sns.barplot(data=summary, x="detector", y="outlier_count", color="#D55E00", ax=axes[0])
    axes[0].set_title("Outliers by detector")
    axes[0].set_xlabel("")
    axes[0].set_ylabel("Samples flagged")
    axes[0].tick_params(axis="x", rotation=25)

    max_votes = int(preds["outlier_votes"].max()) if len(preds) else 0
    vote_counts = preds["outlier_votes"].value_counts().reindex(range(max_votes + 1), fill_value=0)
    axes[1].bar(vote_counts.index.astype(str), vote_counts.values, color="#0072B2")
    axes[1].set_title("Ensemble vote distribution")
    axes[1].set_xlabel("Outlier votes")
    axes[1].set_ylabel("Samples")
    sns.despine(fig=fig)
    fig.suptitle(f"Outlier detection summary: {tag}", fontweight="bold")
    fig.tight_layout()
    for ext in ("svg", "pdf", "png"):
        fig.savefig(out_dir / f"outliers_{tag}_summary.{ext}", bbox_inches="tight", dpi=300)
    plt.close(fig)


# -----------------------------
# Helpers
# -----------------------------
def resolve_path(root: Optional[Path], rel_or_abs: str) -> Path:
    p = Path(rel_or_abs)
    return p if p.is_absolute() or root is None else (root / p)


def load_metadata(path: Path, index_col: str, verbose: bool = False) -> pd.DataFrame:
    df = pd.read_csv(path, sep="\t", header=0)
    selected_col = index_col
    if selected_col not in df.columns:
        fallback_col = next((c for c in FALLBACK_SAMPLE_ID_COLS if c in df.columns), None)
        if fallback_col is None:
            raise ValueError(
                f"Metadata column '{index_col}' not found in columns: {df.columns.tolist()}"
            )
        selected_col = fallback_col
        if verbose:
            print(f"[w] Metadata column '{index_col}' not found; using '{selected_col}' instead.")

    df[selected_col] = df[selected_col].astype(str).str.strip()
    df = df.drop_duplicates(subset=[selected_col]).set_index(selected_col)
    return df


def load_asv_table(path: Path, orientation: str) -> pd.DataFrame:
    """
    Returns samples x features (rows=samples).
    
    Parameters
    ----------
    path : Path
        Path to ASV table
    orientation : str
        "features_rows" = ASVs in rows, samples in columns (will transpose)
        "samples_rows" = samples in rows, ASVs in columns (no transpose)
        
    Returns
    -------
    df : DataFrame (samples x features)
    """
    df = pd.read_csv(path, sep="\t", header=0, index_col=0)
    if orientation == "features_rows":
        df = df.T  # make rows=samples
    elif orientation != "samples_rows":
        raise ValueError("--asv-orientation must be 'features_rows' or 'samples_rows'")
    df.index = df.index.astype(str).str.strip()
    # cast to numeric if needed
    return df.apply(pd.to_numeric, errors="coerce").fillna(0)


def align_and_filter(asv: pd.DataFrame, meta: pd.DataFrame, pre_transformed: bool = False) -> pd.DataFrame:
    """
    Align samples between ASV table and metadata, filter zero entries.
    
    Parameters
    ----------
    asv : DataFrame (samples x features)
    meta : DataFrame
    pre_transformed : bool
        If True, skip filtering all-zero features/samples since CLR-transformed
        data may have negative values and zero filtering may not be appropriate
    """
    shared = asv.index.intersection(meta.index)
    if len(shared) == 0:
        raise ValueError("No overlapping samples between ASV table and metadata.")
    asv = asv.loc[shared]
    
    if not pre_transformed:
        # For raw count data, filter all-zero features and samples
        asv = asv.loc[(asv != 0).any(axis=1), (asv != 0).any(axis=0)]
        # drop samples that became all-zero after feature drop
        asv = asv.loc[(asv != 0).any(axis=1)]
    else:
        # For pre-transformed data, just check that we have variation
        # Remove constant features (no variation across samples)
        asv = asv.loc[:, asv.std(axis=0) > 0]
    
    return asv


def apply_transform(asv_samples_x_features: pd.DataFrame, transform: str, pre_transformed: bool = False) -> pd.DataFrame:
    """
    Apply transformation to ASV data.
    
    Parameters
    ----------
    asv_samples_x_features : DataFrame (samples x features)
    transform : str
        "none" or "clr"
    pre_transformed : bool
        If True, skip transformation (data already transformed)
    """
    if pre_transformed:
        if transform != "none":
            print("[!] Warning: --pre-transformed flag set but --transform is not 'none'.")
            print("[!] Skipping transformation since data is already transformed.")
        return asv_samples_x_features.copy()
    
    if transform == "none":
        return asv_samples_x_features.copy()
    
    if transform == "clr":
        # multiplicative replacement expects ndarray; works on samples x features
        arr = multiplicative_replacement(asv_samples_x_features.values)
        emb = clr(arr)
        return pd.DataFrame(emb, index=asv_samples_x_features.index, columns=asv_samples_x_features.columns)
    
    raise ValueError("--transform must be 'none' or 'clr'")


def maybe_scale(train: np.ndarray, test: np.ndarray, do_scale: bool) -> Tuple[np.ndarray, np.ndarray]:
    if not do_scale:
        return train, test
    scaler = StandardScaler(with_mean=True, with_std=True)
    train_s = scaler.fit_transform(train)
    test_s = scaler.transform(test)
    return train_s, test_s


def fit_predict_group(
    X_train: np.ndarray,
    X_test: np.ndarray,
    samples_test: List[str],
    use_iso: bool,
    use_svm: bool,
    use_hdb: bool,
    iso_kwargs: dict,
    svm_kwargs: dict,
    hdb_kwargs: dict,
) -> pd.DataFrame:
    cols = []
    preds = {}

    if use_iso:
        iso = IsolationForest(**iso_kwargs)
        iso.fit(X_train)
        preds["IsolationForest"] = iso.predict(X_test)  # 1 inliers, -1 outliers
        cols.append("IsolationForest")

    if use_svm:
        svm = OneClassSVM(**svm_kwargs)
        svm.fit(X_train)
        preds["OneClassSVM"] = svm.predict(X_test)  # 1 / -1
        cols.append("OneClassSVM")

    if use_hdb:
        # Train HDBSCAN; predict for test via approximate_predict
        # If fit fails due to insufficient points, mark all as inliers
        try:
            hdb = hdbscan.HDBSCAN(prediction_data=True, **hdb_kwargs)
            hdb.fit(X_train)
            labels, strengths = approximate_predict(hdb, X_test)
            preds["HDBSCAN"] = np.where(labels == -1, -1, 1)
            preds["HDBSCAN_strength"] = strengths
            cols.append("HDBSCAN")
        except Exception:
            # Fall back: mark as inliers
            preds["HDBSCAN"] = np.ones(len(samples_test), dtype=int)
            preds["HDBSCAN_strength"] = np.zeros(len(samples_test), dtype=float)
            cols.append("HDBSCAN")

    df = pd.DataFrame({"sample": samples_test})
    for k, v in preds.items():
        df[k] = v
    # consensus on only the binary predictors
    bin_cols = [c for c in ["IsolationForest", "OneClassSVM", "HDBSCAN"] if c in df.columns]
    df["outlier_votes"] = (df[bin_cols] == -1).sum(axis=1)
    return df.set_index("sample")


def run_for_group(
    group_name: str,
    asv_df: pd.DataFrame,      # transformed samples x features (only group samples will be taken)
    meta_df: pd.DataFrame,     # metadata indexed by sample
    scale: bool,
    model_flags: Tuple[bool, bool, bool],
    iso_kwargs: dict,
    svm_kwargs: dict,
    hdb_kwargs: dict,
    vote_threshold: int,
) -> pd.DataFrame:
    # select group samples
    group_samples = list(meta_df.index)
    if len(group_samples) < 2:
        # not enough to train; mark all as inliers with 0 votes
        base = pd.DataFrame(index=group_samples)
        base["group"] = group_name
        for c in ["IsolationForest", "OneClassSVM", "HDBSCAN"]:
            base[c] = 1
        base["outlier_votes"] = 0
        base["is_outlier"] = False
        return base

    train_samples = [a for a in asv_df.index if a in group_samples]
    test_samples = [a for a in asv_df.index if a in group_samples]

    X_train = asv_df.loc[train_samples].values
    X_test = asv_df.loc[test_samples].values

    X_train, X_test = maybe_scale(X_train, X_test, scale)

    use_iso, use_svm, use_hdb = model_flags
    preds = fit_predict_group(
        X_train, X_test, test_samples,
        use_iso, use_svm, use_hdb,
        iso_kwargs, svm_kwargs, hdb_kwargs
    )
    preds["group"] = group_name
    preds["is_outlier"] = preds["outlier_votes"] >= vote_threshold
    return preds


# -----------------------------
# CLI
# -----------------------------
def get_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Ensemble outlier detection over ASV tables with optional CLR and per-group training.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    io = p.add_argument_group("I/O")
    io.add_argument("--data-dir", type=Path, required=True, help="Project root to resolve relative paths")
    io.add_argument("--asv", type=str, required=True, 
                    help="Path to ASV table (TSV). Can be raw counts or batch-corrected CLR data.")
    io.add_argument("--metadata", type=str, required=True, help="Path to metadata TSV")
    io.add_argument("--sample-id-col", type=str, default=DEFAULT_SAMPLE_ID_COL,
                    help="Sample identifier column in metadata")
    io.add_argument("--output-dir", type=str, required=True, help="Directory to write outputs")

    fmt = p.add_argument_group("Data formatting")
    fmt.add_argument("--asv-orientation", choices=["features_rows", "samples_rows"], default="features_rows",
                     help="How the ASV file is laid out on disk. Use 'samples_rows' for batch-corrected data.")
    fmt.add_argument("--transform", choices=["none", "clr"], default="clr", 
                     help="Feature transform before modeling. Use 'none' with --pre-transformed.")
    fmt.add_argument("--pre-transformed", action="store_true",
                     help="Set this flag if input data is already CLR-transformed (e.g., from batch correction). "
                          "This will skip the transformation step.")
    fmt.add_argument("--scale", action="store_true", help="Z-score features (fit on train, apply to test)")

    grp = p.add_argument_group("Grouping & training")
    grp.add_argument("--group-cols", default="none",
                     help="Comma-sep list of grouping columns from metadata; include 'none' to pool all samples")

    mdl = p.add_argument_group("Models & voting")
    mdl.add_argument("--use-iso", action="store_true", help="Include IsolationForest")
    mdl.add_argument("--use-svm", action="store_true", help="Include OneClassSVM")
    mdl.add_argument("--use-hdb", action="store_true", help="Include HDBSCAN")
    mdl.add_argument("--vote-threshold", type=int, default=3, help="Votes ≥ threshold => outlier")

    iso = p.add_argument_group("IsolationForest")
    iso.add_argument("--iso-contamination", default="auto", help="Contamination for IsolationForest (e.g. 0.05 or 'auto')")
    iso.add_argument("--iso-estimators", type=int, default=100, help="Number of trees")
    iso.add_argument("--iso-random-state", type=int, default=42, help="Random state")

    svm = p.add_argument_group("OneClassSVM")
    svm.add_argument("--svm-kernel", default="rbf", help="Kernel")
    svm.add_argument("--svm-gamma", default="scale", help="Gamma")
    svm.add_argument("--svm-nu", type=float, default=0.1, help="Nu parameter (upper bound on outlier fraction)")

    hdb = p.add_argument_group("HDBSCAN")
    hdb.add_argument("--hdbscan-min-cluster-size", type=int, default=5, help="HDBSCAN min_cluster_size")
    hdb.add_argument("--hdbscan-min-samples", type=int, default=None, help="HDBSCAN min_samples (optional)")
    hdb.add_argument("--hdbscan-metric", default="euclidean", help="Distance metric")

    misc = p.add_argument_group("Misc")
    misc.add_argument("--verbose", action="store_true", help="Verbose logs")

    return p


def main():
    args = get_parser().parse_args()

    root = args.data_dir
    out_dir = resolve_path(root, args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # defaults: if no model flags given, enable all three
    if not (args.use_iso or args.use_svm or args.use_hdb):
        args.use_iso = args.use_svm = args.use_hdb = True

    # Validate arguments
    if args.pre_transformed and args.transform == "clr":
        print("[!] Warning: --pre-transformed flag is set, but --transform is 'clr'.")
        print("[!] Setting --transform to 'none' since data is already transformed.")
        args.transform = "none"

    # load data
    meta = load_metadata(resolve_path(root, args.metadata), args.sample_id_col, verbose=args.verbose)
    asv = load_asv_table(resolve_path(root, args.asv), args.asv_orientation)
    asv = align_and_filter(asv, meta, pre_transformed=args.pre_transformed)

    if args.verbose:
        print(f"[i] ASV matrix (samples x features) after filtering: {asv.shape}")
        print(f"[i] Metadata rows (unique samples): {meta.shape[0]}")
        if args.pre_transformed:
            print(f"[i] Using pre-transformed data (skipping CLR transformation)")
        else:
            print(f"[i] Will apply transformation: {args.transform}")

    # transform & (optionally) scale later per train/test
    feat = apply_transform(asv, args.transform, pre_transformed=args.pre_transformed)

    # group list
    group_cols = [g.strip() for g in args.group_cols.split(",") if g.strip()]
    outputs = []

    # model kwargs
    iso_kwargs = dict(
        contamination=args.iso_contamination,
        n_estimators=args.iso_estimators,
        random_state=args.iso_random_state,
        n_jobs=-1,
    )
    svm_kwargs = dict(kernel=args.svm_kernel, gamma=args.svm_gamma, nu=args.svm_nu)
    hdb_kwargs = dict(min_cluster_size=args.hdbscan_min_cluster_size, metric=args.hdbscan_metric)
    if args.hdbscan_min_samples is not None:
        hdb_kwargs["min_samples"] = args.hdbscan_min_samples

    for gcol in group_cols:
        if gcol.lower() == "none":
            # treat all samples as a single group
            preds = run_for_group(
                group_name="all",
                asv_df=feat,
                meta_df=meta,  # all rows
                scale=args.scale,
                model_flags=(args.use_iso, args.use_svm, args.use_hdb),
                iso_kwargs=iso_kwargs,
                svm_kwargs=svm_kwargs,
                hdb_kwargs=hdb_kwargs,
                vote_threshold=args.vote_threshold,
            )
            preds["group_col"] = "none"
        else:
            if gcol not in meta.columns:
                raise ValueError(f"Grouping column '{gcol}' not found in metadata.")
            # process each level separately, then concat
            per_levels = []
            for level, sub_meta in meta.groupby(gcol):
                sub_feat = feat.loc[sub_meta.index.intersection(feat.index)]
                if sub_feat.shape[0] < 2:
                    continue
                pred = run_for_group(
                    group_name=str(level),
                    asv_df=sub_feat,
                    meta_df=sub_meta,
                    scale=args.scale,
                    model_flags=(args.use_iso, args.use_svm, args.use_hdb),
                    iso_kwargs=iso_kwargs,
                    svm_kwargs=svm_kwargs,
                    hdb_kwargs=hdb_kwargs,
                    vote_threshold=args.vote_threshold,
                )
                per_levels.append(pred)
            if not per_levels:
                if args.verbose:
                    print(f"[!] No usable groups for '{gcol}'")
                continue
            preds = pd.concat(per_levels).sort_index()
            preds["group_col"] = gcol

        preds = preds.sort_index()
        outputs.append((gcol, preds))

        # write per-group-col file
        tag = "all" if gcol.lower() == "none" else gcol
        out_path = out_dir / f"outliers_{tag}.tsv"
        preds.reset_index().rename(columns={"index": "sample"}).to_csv(out_path, sep="\t", index=False)
        write_outlier_summary_plot(preds, tag, out_dir)
        if args.verbose:
            n_out = int(preds["is_outlier"].sum())
            print(f"[✓] Wrote {out_path}  (outliers={n_out}, n={len(preds)})")
    
    # convenience: also write a combined file if >1 grouping requested
    if len(outputs) > 1:
        combo = []
        for gcol, df in outputs:
            tag = "all" if gcol.lower() == "none" else gcol
            tmp = df.copy()
            tmp["grouping"] = tag
            combo.append(tmp)
        combo = pd.concat(combo).reset_index().rename(columns={"index": "sample"})
        combo.to_csv(out_dir / "outliers_all_groupings.tsv", sep="\t", index=False)
        if args.verbose:
            print(f"[✓] Wrote {out_dir / 'outliers_all_groupings.tsv'}")


if __name__ == "__main__":
    main()

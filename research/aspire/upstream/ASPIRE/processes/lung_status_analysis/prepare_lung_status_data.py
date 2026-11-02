#!/usr/bin/env python3
"""
Prepare data for cancerous vs. contralateral vs. healthy lung analysis.

Creates lung_status grouping:
- TumorSide: sample side matches Cancer_Site (cancer patients only)
- Contralateral: opposite side in cancer patients
- Healthy: control patients

For control patients, averages left/right samples to create one profile per patient.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd


TRUE_VALUES = {"true", "t", "1", "yes", "y"}
FALSE_VALUES = {"false", "f", "0", "no", "n", ""}


def canonicalize_sample_type(x: str) -> str:
    x_clean = str(x).strip().lower()
    if x_clean in {"lung brush", "bronchial brush", "brochial brush", "brush"}:
        return "Lung Brush"
    if x_clean in {"bal", "bronchoalveolar lavage"}:
        return "BAL"
    if x_clean in {"oral rinse", "oral_rinse", "oral"}:
        return "Oral Rinse"
    return str(x)


def as_bool(value) -> bool | None:
    if pd.isna(value):
        return None
    text = str(value).strip().lower()
    if text in TRUE_VALUES:
        return True
    if text in FALSE_VALUES:
        return False
    return None


def canonicalize_side(value: object) -> str | None:
    if pd.isna(value):
        return None
    text = str(value).strip().lower()
    if text.startswith("l"):
        return "L"
    if text.startswith("r"):
        return "R"
    return None


def assign_lung_status(
    row: pd.Series,
    case_col: str,
    cancer_site_col: str,
    lung_code_col: str,
    tumor_side_col: str | None,
    contralateral_col: str | None,
    healthy_col: str | None,
    explicit_status_col: str | None,
    status_a_value: str,
    status_b_value: str,
    reference_status_value: str,
) -> str:
    """
    Assign lung status using explicit metadata when available, otherwise derive it.

    Returns:
        - "TumorSide": cancer patient, sample matches tumor side
        - "Contralateral": cancer patient, sample opposite tumor side
        - "Healthy": control patient
    """
    if explicit_status_col and explicit_status_col in row.index:
        explicit = str(row.get(explicit_status_col, "")).strip()
        explicit_map = {
            status_a_value: "TumorSide",
            status_b_value: "Contralateral",
            reference_status_value: "Healthy",
        }
        if explicit in explicit_map:
            return explicit_map[explicit]

    if healthy_col and healthy_col in row.index:
        is_healthy = as_bool(row.get(healthy_col))
        if is_healthy is True:
            return "Healthy"

    if contralateral_col and contralateral_col in row.index:
        is_contralateral = as_bool(row.get(contralateral_col))
        if is_contralateral is True:
            return "Contralateral"

    tumor_side = None
    if tumor_side_col and tumor_side_col in row.index:
        tumor_side = canonicalize_side(row.get(tumor_side_col))
    if tumor_side is None and cancer_site_col in row.index:
        tumor_side = canonicalize_side(row.get(cancer_site_col))

    lung_code = canonicalize_side(row.get(lung_code_col, ""))
    case = str(row.get(case_col, "")).strip()

    if case == "Cancer":
        if tumor_side and lung_code and tumor_side == lung_code:
            return "TumorSide"
        elif tumor_side and lung_code and tumor_side != lung_code:
            return "Contralateral"
        else:
            return "Other"
    elif case in ["Control", "Non-Cancer"]:
        return "Healthy"
    else:
        return "Unknown"


def main() -> None:
    p = argparse.ArgumentParser(
        description="Prepare lung status groupings for cancer vs. contralateral vs. healthy analysis."
    )
    p.add_argument(
        "--input",
        type=Path,
        required=True,
        help="Input ASV table (long format TSV with metadata)",
    )
    p.add_argument(
        "--sample-type",
        type=str,
        default="Lung Brush",
        help="Sample type to filter for (default: 'Lung Brush')",
    )
    p.add_argument("--sample-col", type=str, default="sample", help="Sample ID column")
    p.add_argument("--type-col", type=str, default="type_group", help="Sample type column")
    p.add_argument("--case-col", type=str, default="Case", help="Case/control column")
    p.add_argument("--patient-col", type=str, default="Participant_ID", help="Patient ID column")
    p.add_argument("--cancer-site-col", type=str, default="Cancer_Site", help="Tumor-side metadata column")
    p.add_argument("--lung-code-col", type=str, default="lung_code", help="Sample-side code column")
    p.add_argument("--tumor-side-col", type=str, default="TumorSide", help="Optional explicit tumor-side column")
    p.add_argument("--contralateral-col", type=str, default="Contralateral", help="Optional boolean contralateral column")
    p.add_argument("--healthy-col", type=str, default="Healthy", help="Optional boolean healthy/control column")
    p.add_argument("--lung-status-col", type=str, default="lung_status", help="Optional precomputed lung-status column")
    p.add_argument("--status-a-value", default="TumorSide", help="Value representing the first paired status")
    p.add_argument("--status-b-value", default="Contralateral", help="Value representing the second paired status")
    p.add_argument("--reference-status-value", default="Healthy", help="Value representing the unpaired reference status")
    p.add_argument(
        "--outdir",
        type=Path,
        required=True,
        help="Output directory for prepared data",
    )
    args = p.parse_args()

    args.outdir.mkdir(parents=True, exist_ok=True)

    # Load data
    print(f"Loading data from {args.input}...")
    df = pd.read_csv(args.input, sep="\t", low_memory=False)
    if args.sample_col not in df.columns:
        print(f"ERROR: Sample column not found: {args.sample_col}")
        sys.exit(1)
    if args.type_col not in df.columns:
        print(f"ERROR: Sample type column not found: {args.type_col}")
        sys.exit(1)
    print(f"  Loaded {len(df):,} rows, {df[args.sample_col].nunique()} samples")

    df[".sample_type_canonical"] = df[args.type_col].map(canonicalize_sample_type)
    requested_sample_type = canonicalize_sample_type(args.sample_type)

    print(f"  Available sample types in '{args.type_col}': {sorted(df['.sample_type_canonical'].dropna().unique())}")

    df_filt = df[df[".sample_type_canonical"] == requested_sample_type].copy()
    print(f"  Filtered to {requested_sample_type}: {len(df_filt):,} rows, {df_filt[args.sample_col].nunique()} samples")

    if df_filt.empty:
        print(f"ERROR: No samples found for sample type '{requested_sample_type}'")
        print(f"  Available types: {sorted(df['.sample_type_canonical'].dropna().unique())}")
        sys.exit(1)

    # Assign lung status
    print("\nAssigning lung_status...")
    df_filt["lung_status"] = df_filt.apply(
        assign_lung_status,
        axis=1,
        case_col=args.case_col,
        cancer_site_col=args.cancer_site_col,
        lung_code_col=args.lung_code_col,
        tumor_side_col=args.tumor_side_col if args.tumor_side_col in df_filt.columns else None,
        contralateral_col=args.contralateral_col if args.contralateral_col in df_filt.columns else None,
        healthy_col=args.healthy_col if args.healthy_col in df_filt.columns else None,
        explicit_status_col=args.lung_status_col if args.lung_status_col in df_filt.columns else None,
        status_a_value=args.status_a_value,
        status_b_value=args.status_b_value,
        reference_status_value=args.reference_status_value,
    )

    # Remove "Other" and "Unknown" categories
    df_filt = df_filt[df_filt["lung_status"].isin(["TumorSide", "Contralateral", "Healthy"])].copy()
    print(f"  After assignment: {df_filt[args.sample_col].nunique()} samples")

    if df_filt.empty:
        print("ERROR: No samples remaining after lung_status assignment")
        print("  Check that your data has proper Case, Cancer_Site, and lung_code columns")
        sys.exit(1)

    # Summary by lung_status
    summary = df_filt.groupby("lung_status")[args.sample_col].nunique().to_dict()
    print("\nSample counts by lung_status:")
    for status, count in sorted(summary.items()):
        print(f"  {status}: {count}")

    # Summary by patient and lung_status
    patient_col = args.patient_col if args.patient_col in df_filt.columns else ("patient_code" if "patient_code" in df_filt.columns else None)
    if patient_col is None:
        print("ERROR: No patient ID column found")
        sys.exit(1)
    print(f"\nPatient counts by lung_status:")
    patient_summary = df_filt.groupby("lung_status")[patient_col].nunique().to_dict()
    for status, count in sorted(patient_summary.items()):
        print(f"  {status}: {count} patients")

    # Check paired structure for cancer patients
    cancer_df = df_filt[df_filt[args.case_col] == "Cancer"]
    if not cancer_df.empty:
        paired_counts = (
            cancer_df.groupby(patient_col)["lung_status"]
            .apply(lambda x: set(x))
            .value_counts()
        )
        print("\nPairing structure for cancer patients:")
        for pair_set, count in paired_counts.items():
            print(f"  {pair_set}: {count} patients")

    # Save full filtered data with lung_status
    out_prefix = requested_sample_type.replace(" ", "_")
    out_full = args.outdir / f"{out_prefix}_with_lung_status.tsv"
    df_filt.to_csv(out_full, sep="\t", index=False)
    print(f"\nSaved full data to: {out_full}")

    # Create wide-format ASV table for easier downstream analysis
    if "ASV_ID" in df_filt.columns and "count" in df_filt.columns:
        asv_wide = df_filt.pivot_table(
            index=args.sample_col,
            columns="ASV_ID",
            values="count",
            fill_value=0,
        )
        asv_wide.index.name = "sample"
        out_asv = args.outdir / f"{out_prefix}_ASV_table.tsv"
        asv_wide.to_csv(out_asv, sep="\t")
        print(f"Saved ASV table to: {out_asv}")

        # Save metadata
        meta_cols = [
            args.sample_col,
            patient_col,
            args.case_col,
            args.cancer_site_col,
            args.type_col,
            args.lung_code_col,
            "lung_status",
        ]
        extra_cols = [
            args.tumor_side_col,
            args.contralateral_col,
            args.healthy_col,
            args.lung_status_col,
        ]
        available_cols = [c for c in meta_cols if c in df_filt.columns]
        available_cols.extend([c for c in extra_cols if c in df_filt.columns and c not in available_cols])

        metadata = df_filt[available_cols].drop_duplicates(subset=[args.sample_col]).copy()
        if args.sample_col != "sample":
            metadata = metadata.rename(columns={args.sample_col: "sample"})
        out_meta = args.outdir / f"{out_prefix}_metadata.tsv"
        metadata.to_csv(out_meta, sep="\t", index=False)
        print(f"Saved metadata to: {out_meta}")

    print("\nData preparation complete!")
    print(f"\nNext steps:")
    print(f"1. Run paired analysis (TumorSide vs Contralateral)")
    print(f"2. Average control samples and run between-patient analyses")
    print(f"3. Visualize results")


if __name__ == "__main__":
    main()

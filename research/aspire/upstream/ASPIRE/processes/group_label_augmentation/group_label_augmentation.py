#!/usr/bin/env python3
"""Apply validated soft group labels to sample and long-form ASV metadata."""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


MISSING_LABELS = {"", "nan", "none", "na", "null"}


def parse_csv(value: str | None) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in value.replace("|", ",").split(",") if item.strip()]


def is_missing(value: object) -> bool:
    if pd.isna(value):
        return True
    return str(value).strip().lower() in MISSING_LABELS


def select_assignments(
    assignments: pd.DataFrame,
    validation: pd.DataFrame,
    target_col: str,
    excluded_labels: set[str],
    min_confidence: float,
    min_neighbor_agreement: float,
    min_cv_balanced_accuracy: float,
) -> pd.DataFrame:
    selected = assignments.loc[assignments.get("group_col", pd.Series(dtype=str)).eq(target_col)].copy()
    if selected.empty:
        return selected

    required = {"sample", "assigned_label", "confidence", "nearest_distance", "neighbor_agreement"}
    missing = required.difference(selected.columns)
    if missing:
        raise ValueError(f"Soft-assignment table is missing required columns: {sorted(missing)}")

    validation_row = validation.loc[validation.get("group_col", pd.Series(dtype=str)).eq(target_col)]
    cv_ok = False
    distance_threshold = np.nan
    cv_balanced_accuracy = np.nan
    if not validation_row.empty:
        row = validation_row.iloc[0]
        cv_balanced_accuracy = pd.to_numeric(row.get("balanced_accuracy"), errors="coerce")
        distance_threshold = pd.to_numeric(row.get("nearest_distance_threshold"), errors="coerce")
        cv_ok = bool(
            str(row.get("status", "")).lower() == "ok"
            and pd.notna(cv_balanced_accuracy)
            and float(cv_balanced_accuracy) >= min_cv_balanced_accuracy
        )

    selected["confidence"] = pd.to_numeric(selected["confidence"], errors="coerce")
    selected["neighbor_agreement"] = pd.to_numeric(selected["neighbor_agreement"], errors="coerce")
    selected["nearest_distance"] = pd.to_numeric(selected["nearest_distance"], errors="coerce")
    selected["cv_balanced_accuracy"] = cv_balanced_accuracy
    selected["distance_threshold"] = distance_threshold
    selected["accepted"] = True
    selected["rejection_reason"] = ""

    def reject(mask: pd.Series, reason: str) -> None:
        active = mask & selected["accepted"]
        selected.loc[active, "accepted"] = False
        selected.loc[active, "rejection_reason"] = reason

    reject(selected["assigned_label"].astype(str).isin(excluded_labels), "excluded_label")
    reject(selected["confidence"].lt(min_confidence) | selected["confidence"].isna(), "low_confidence")
    reject(
        selected["neighbor_agreement"].lt(min_neighbor_agreement)
        | selected["neighbor_agreement"].isna(),
        "low_neighbor_agreement",
    )
    if pd.notna(distance_threshold):
        reject(selected["nearest_distance"].gt(float(distance_threshold)), "out_of_distribution")
    else:
        reject(pd.Series(True, index=selected.index), "missing_distance_calibration")
    if not cv_ok:
        reject(pd.Series(True, index=selected.index), "insufficient_cross_validation")

    selected = selected.sort_values(
        ["sample", "accepted", "confidence", "neighbor_agreement"],
        ascending=[True, False, False, False],
    ).drop_duplicates("sample", keep="first")
    return selected


def augment_metadata(
    metadata: pd.DataFrame,
    selected: pd.DataFrame,
    sample_col: str,
    target_col: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if sample_col not in metadata.columns:
        raise ValueError(f"Metadata sample column not found: {sample_col}")
    if target_col not in metadata.columns:
        raise ValueError(f"Metadata target group column not found: {target_col}")

    output = metadata.copy()
    observed_col = f"{target_col}_observed"
    source_col = f"{target_col}_assignment_source"
    confidence_col = f"{target_col}_assignment_confidence"
    agreement_col = f"{target_col}_neighbor_agreement"
    output[observed_col] = output[target_col]
    output[source_col] = np.where(output[target_col].map(is_missing), "unassigned", "observed")
    output[confidence_col] = np.where(output[source_col].eq("observed"), 1.0, np.nan)
    output[agreement_col] = np.where(output[source_col].eq("observed"), 1.0, np.nan)

    accepted = selected.loc[selected.get("accepted", False).eq(True)].set_index("sample") if not selected.empty else pd.DataFrame()
    if not accepted.empty:
        sample_keys = output[sample_col].astype(str)
        missing = output[target_col].map(is_missing)
        assignable = missing & sample_keys.isin(accepted.index.astype(str))
        output.loc[assignable, target_col] = sample_keys[assignable].map(accepted["assigned_label"])
        output.loc[assignable, source_col] = "soft_assigned"
        output.loc[assignable, confidence_col] = sample_keys[assignable].map(accepted["confidence"])
        output.loc[assignable, agreement_col] = sample_keys[assignable].map(accepted["neighbor_agreement"])

    audit_cols = [sample_col, observed_col, target_col, source_col, confidence_col, agreement_col]
    audit = output[audit_cols].drop_duplicates(subset=[sample_col]).copy()
    return output, audit


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--metadata", required=True)
    parser.add_argument("--asv-meta", required=True)
    parser.add_argument("--assignments", required=True)
    parser.add_argument("--validation-summary", required=True)
    parser.add_argument("--sample-col", required=True)
    parser.add_argument("--target-col", required=True)
    parser.add_argument("--exclude-labels", default="outlier")
    parser.add_argument("--min-confidence", type=float, default=0.70)
    parser.add_argument("--min-neighbor-agreement", type=float, default=0.60)
    parser.add_argument("--min-cv-balanced-accuracy", type=float, default=0.60)
    parser.add_argument("--metadata-output", default="metadata_updated_micro.augmented.tsv")
    parser.add_argument("--asv-meta-output", default="ASV_meta_micro.augmented.tsv")
    parser.add_argument("--audit-output", default="group_label_augmentation_audit.tsv")
    args = parser.parse_args()

    metadata = pd.read_csv(args.metadata, sep="\t")
    asv_meta = pd.read_csv(args.asv_meta, sep="\t")
    assignments = pd.read_csv(args.assignments, sep="\t")
    validation = pd.read_csv(args.validation_summary, sep="\t")
    excluded = set(parse_csv(args.exclude_labels))

    selected = select_assignments(
        assignments,
        validation,
        args.target_col,
        excluded,
        args.min_confidence,
        args.min_neighbor_agreement,
        args.min_cv_balanced_accuracy,
    )
    metadata_augmented, audit = augment_metadata(metadata, selected, args.sample_col, args.target_col)
    asv_meta_augmented, _ = augment_metadata(asv_meta, selected, args.sample_col, args.target_col)

    metadata_augmented.to_csv(args.metadata_output, sep="\t", index=False)
    asv_meta_augmented.to_csv(args.asv_meta_output, sep="\t", index=False)
    if selected.empty:
        audit["soft_assignment_accepted"] = False
        audit["soft_assignment_rejection_reason"] = "no_candidate_assignment"
    else:
        selected_audit = selected[["sample", "accepted", "rejection_reason"]].rename(
            columns={
                "sample": args.sample_col,
                "accepted": "soft_assignment_accepted",
                "rejection_reason": "soft_assignment_rejection_reason",
            }
        )
        audit = audit.merge(selected_audit, on=args.sample_col, how="left")
        audit["soft_assignment_accepted"] = audit["soft_assignment_accepted"].fillna(False).astype(bool)
        audit["soft_assignment_rejection_reason"] = audit["soft_assignment_rejection_reason"].fillna(
            "no_candidate_assignment"
        )
    audit.to_csv(args.audit_output, sep="\t", index=False)

    accepted_count = int(audit["soft_assignment_accepted"].sum())
    print(f"[i] Applied {accepted_count} validated soft assignments for {args.target_col}.")


if __name__ == "__main__":
    main()

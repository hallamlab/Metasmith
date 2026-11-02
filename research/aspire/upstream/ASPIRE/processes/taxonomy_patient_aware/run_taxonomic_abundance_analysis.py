#!/usr/bin/env python3
"""
Actual-data taxonomic abundance analysis (Cancer vs Control) stratified by sample type.

Matches the taxonomic levels used in power analysis scripts:
- Phylum
- Family

For each tax level and sample type:
1) Aggregate ASV counts to taxon per sample
2) Aggregate to patient level (sum across samples per patient)
3) Convert to relative abundance
4) Test Cancer vs Control per taxon (Mann-Whitney U, two-sided)
5) BH-FDR correction within each tax level x sample type set
"""

from __future__ import annotations

import argparse
from pathlib import Path
import numpy as np
import pandas as pd
from scipy.stats import mannwhitneyu
from statsmodels.stats.multitest import multipletests


def canonicalize_sample_type(x: str) -> str:
    x_clean = str(x).strip().lower()
    if x_clean in {"oral", "oral rinse", "oral_rinse"}:
        return "Oral Rinse"
    if x_clean in {"bal", "bronchoalveolar lavage"}:
        return "BAL"
    if x_clean in {"lung brush", "bronchial brush", "brochial brush", "brush"}:
        return "Lung Brush"
    return str(x)


def filter_contralateral_cancer(df: pd.DataFrame, case_col: str, type_col: str, contralateral_sample_types: list[str], contralateral_col: str,
                                cancer_site_col: str, lung_side_col: str,
                                contralateral_value: str) -> pd.DataFrame:
    work = df.copy()
    contra = contralateral_col
    if contra not in work.columns and {cancer_site_col, lung_side_col}.issubset(work.columns):
        cancer_side = work[cancer_site_col].astype(str).str[:1].str.upper()
        lung_side = work[lung_side_col].astype(str).str[:1].str.upper()
        case_vals = work[case_col].astype(str)
        work[".derived_lung_status"] = np.where(
            case_vals.isin(["Control", "Non-Cancer"]),
            "Healthy",
            np.where(cancer_side == lung_side, "TumorSide", "Contralateral"),
        )
        contra = ".derived_lung_status"

    if contra not in work.columns:
        return work

    is_cancer = ~work[case_col].astype(str).isin(["Control", "Non-Cancer"])
    is_contra = work[contra].astype(str) == str(contralateral_value)
    in_target_type = work[type_col].map(canonicalize_sample_type).isin(set(contralateral_sample_types))
    return work.loc[~(is_cancer & is_contra & in_target_type)].copy()


def aggregate_to_taxonomy(long_df: pd.DataFrame, tax_level: str, sample_col: str, count_col: str) -> pd.DataFrame:
    agg = long_df.groupby([sample_col, tax_level], as_index=False)[count_col].sum()
    wide = agg.pivot(index=sample_col, columns=tax_level, values=count_col).fillna(0)
    return wide


def relative_abundance(counts: np.ndarray) -> np.ndarray:
    totals = counts.sum(axis=1, keepdims=True)
    totals[totals == 0] = 1
    return counts / totals


def rclr_transform(df: pd.DataFrame) -> pd.DataFrame:
    arr = df.values.astype(float)
    out = np.zeros_like(arr, dtype=float)
    for i in range(arr.shape[0]):
        row = arr[i, :]
        pos = row > 0
        if np.any(pos):
            lv = np.log(row[pos])
            out[i, pos] = lv - lv.mean()
    return pd.DataFrame(out, index=df.index, columns=df.columns)


def cohens_d(x: np.ndarray, y: np.ndarray) -> float:
    x = np.asarray(x, dtype=float)
    y = np.asarray(y, dtype=float)
    if len(x) < 2 or len(y) < 2:
        return np.nan
    vx = np.var(x, ddof=1)
    vy = np.var(y, ddof=1)
    pooled = ((len(x) - 1) * vx + (len(y) - 1) * vy) / (len(x) + len(y) - 2)
    if pooled <= 0:
        return 0.0
    return (np.mean(x) - np.mean(y)) / np.sqrt(pooled)


def run_one_group(
    long_df: pd.DataFrame,
    tax_level: str,
    sample_type: str,
    sample_col: str,
    patient_col: str,
    case_col: str,
    type_col: str,
    count_col: str,
    min_prevalence: float,
    transform: str,
    case_groups: list[str],
) -> pd.DataFrame:
    st = long_df[long_df[type_col] == sample_type].copy()
    if st.empty:
        return pd.DataFrame()

    tax_wide = aggregate_to_taxonomy(st, tax_level, sample_col, count_col)

    meta = st[[sample_col, patient_col, case_col]].drop_duplicates().set_index(sample_col)
    sample_ids = meta.index.intersection(tax_wide.index)
    if len(sample_ids) == 0:
        return pd.DataFrame()

    meta = meta.loc[sample_ids].copy()
    tax_wide = tax_wide.loc[sample_ids].copy()

    sample_rel = pd.DataFrame(
        relative_abundance(tax_wide.values),
        index=tax_wide.index,
        columns=tax_wide.columns,
    )

    # Collapse to one equal-weight profile per patient
    patient_rel = sample_rel.join(meta[[patient_col]]).groupby(patient_col).mean()

    prevalence = (patient_rel > 0).mean(axis=0)
    keep_taxa = prevalence[prevalence >= min_prevalence].index.tolist()
    if len(keep_taxa) == 0:
        return pd.DataFrame()

    patient_rel = patient_rel[keep_taxa]
    if len(case_groups) == 2:
        group_a, group_b = case_groups
        case_meta = meta[[patient_col, case_col]].drop_duplicates()
        case_meta = case_meta[case_meta[case_col].astype(str).isin(case_groups)].copy()
        case_meta["case_status"] = case_meta[case_col].astype(str)
    else:
        group_a, group_b = "Cancer", "Control"
        case_meta = (
            meta[[patient_col, case_col]]
            .drop_duplicates()
            .assign(case_status=lambda d: np.where(d[case_col].isin(["Control", "Non-Cancer"]), "Control", "Cancer"))
        )
    patient_case = case_meta.groupby(patient_col, as_index=False)["case_status"].first().set_index(patient_col)

    common_patients = patient_rel.index.intersection(patient_case.index)
    patient_rel = patient_rel.loc[common_patients]
    patient_case = patient_case.loc[common_patients]

    if transform == "rclr":
        features = rclr_transform(patient_rel)
    else:
        features = patient_rel.copy()

    labels = patient_case["case_status"]
    n_group_a = int((labels == group_a).sum())
    n_group_b = int((labels == group_b).sum())

    rows = []
    for taxon in features.columns:
        x = features.loc[labels == group_a, taxon].values
        y = features.loc[labels == group_b, taxon].values

        if len(x) >= 2 and len(y) >= 2:
            stat, p = mannwhitneyu(x, y, alternative="two-sided")
        else:
            stat, p = np.nan, np.nan

        rows.append(
            {
                "tax_level": tax_level,
                "sample_type": sample_type,
                "taxon": taxon,
                "group_a": group_a,
                "group_b": group_b,
                "n_patients_total": len(common_patients),
                "n_group_a": n_group_a,
                "n_group_b": n_group_b,
                "n_cancer": n_group_a if group_a == "Cancer" else np.nan,
                "n_control": n_group_b if group_b == "Control" else np.nan,
                "median_group_a": float(np.median(x)) if len(x) else np.nan,
                "median_group_b": float(np.median(y)) if len(y) else np.nan,
                "median_cancer": float(np.median(x)) if group_a == "Cancer" and len(x) else np.nan,
                "median_control": float(np.median(y)) if group_b == "Control" and len(y) else np.nan,
                "delta_median": float(np.median(x) - np.median(y)) if len(x) and len(y) else np.nan,
                "cohens_d": cohens_d(x, y),
                "mw_u": float(stat) if not np.isnan(stat) else np.nan,
                "p_value": float(p) if not np.isnan(p) else np.nan,
            }
        )

    out = pd.DataFrame(rows)
    valid = out["p_value"].notna()
    out["q_value"] = np.nan
    if valid.sum() > 0:
        out.loc[valid, "q_value"] = multipletests(out.loc[valid, "p_value"], method="fdr_bh")[1]
    out["significant_fdr_0.05"] = out["q_value"] < 0.05
    return out.sort_values("q_value", na_position="last")


def parse_csv_values(raw: str) -> list[str]:
    return [x.strip() for x in str(raw).split(",") if x.strip()]


def observed_values(df: pd.DataFrame, col: str) -> list[str]:
    return sorted([str(x) for x in df[col].dropna().astype(str).unique()])


def main() -> None:
    p = argparse.ArgumentParser(description="Taxonomic abundance analysis between two metadata groups on observed data")
    p.add_argument("--data-long", required=True)
    p.add_argument("--tax-levels", default="Phylum,Family")
    p.add_argument("--sample-types", default="BAL,Lung Brush,Oral Rinse", help="Comma-separated strata in --type-col; use empty/all to use all observed values")
    p.add_argument("--sample-col", default="sample")
    p.add_argument("--patient-col", default="Participant_ID")
    p.add_argument("--case-col", default="Case")
    p.add_argument("--case-groups", default="", help="Optional comma-separated pair of labels to compare in --case-col. Defaults to Cancer vs Control/Non-Cancer.")
    p.add_argument("--type-col", default="type_group")
    p.add_argument("--count-col", default="count")
    p.add_argument("--min-prevalence", type=float, default=0.10)
    p.add_argument("--exclude-contralateral-in-cancer", dest="exclude_contralateral_in_cancer", action="store_true")
    p.add_argument("--keep-contralateral-in-cancer", dest="exclude_contralateral_in_cancer", action="store_false")
    p.add_argument("--contralateral-col", default="lung_status")
    p.add_argument("--cancer-site-col", default="Cancer_Site")
    p.add_argument("--lung-side-col", default="lung_code")
    p.add_argument("--contralateral-value", default="Contralateral")
    p.add_argument("--contralateral-sample-types", default="Lung Brush,BAL", help="Comma-separated sample types where contralateral exclusion applies")
    p.set_defaults(exclude_contralateral_in_cancer=True)
    p.add_argument("--transform", choices=["none", "rclr"], default="none")
    p.add_argument("--outdir", required=True)
    args = p.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    long_df = pd.read_csv(args.data_long, sep="\t", low_memory=False)
    long_df[args.type_col] = long_df[args.type_col].map(canonicalize_sample_type)
    sample_types_raw = parse_csv_values(args.sample_types)
    if not sample_types_raw or [x.lower() for x in sample_types_raw] == ["all"]:
        sample_types = observed_values(long_df, args.type_col)
    else:
        sample_types = [canonicalize_sample_type(x) for x in sample_types_raw]
    if args.exclude_contralateral_in_cancer:
        long_df = filter_contralateral_cancer(
            long_df,
            case_col=args.case_col,
            type_col=args.type_col,
            contralateral_sample_types=[canonicalize_sample_type(x.strip()) for x in args.contralateral_sample_types.split(",") if x.strip()],
            contralateral_col=args.contralateral_col,
            cancer_site_col=args.cancer_site_col,
            lung_side_col=args.lung_side_col,
            contralateral_value=args.contralateral_value,
        )

    tax_levels = parse_csv_values(args.tax_levels)
    case_groups = parse_csv_values(args.case_groups)
    if case_groups and len(case_groups) != 2:
        raise SystemExit("--case-groups must contain exactly two comma-separated labels when provided.")

    all_results = []
    for tax_level in tax_levels:
        for st in sample_types:
            res = run_one_group(
                long_df,
                tax_level=tax_level,
                sample_type=st,
                sample_col=args.sample_col,
                patient_col=args.patient_col,
                case_col=args.case_col,
                type_col=args.type_col,
                count_col=args.count_col,
                min_prevalence=args.min_prevalence,
                transform=args.transform,
                case_groups=case_groups,
            )
            if not res.empty:
                all_results.append(res)

    if not all_results:
        results = pd.DataFrame(
            columns=[
                "tax_level", "sample_type", "taxon", "group_a", "group_b",
                "n_patients_total", "n_group_a", "n_group_b", "n_cancer",
                "n_control", "median_group_a", "median_group_b", "median_cancer", "median_control",
                "delta_median", "cohens_d", "mw_u", "p_value", "q_value",
                "significant_fdr_0.05",
            ]
        )
        print("No abundance contrast results produced; writing empty result tables.")
    else:
        results = pd.concat(all_results, ignore_index=True)
    results.to_csv(outdir / "taxonomic_abundance_observed.tsv", sep="\t", index=False)
    results[results["significant_fdr_0.05"]].to_csv(
        outdir / "taxonomic_abundance_observed_significant.tsv", sep="\t", index=False
    )

    print(f"Saved: {outdir / 'taxonomic_abundance_observed.tsv'}")
    print(f"Saved: {outdir / 'taxonomic_abundance_observed_significant.tsv'}")


if __name__ == "__main__":
    main()

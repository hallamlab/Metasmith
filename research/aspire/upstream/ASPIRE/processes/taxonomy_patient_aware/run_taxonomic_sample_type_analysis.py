#!/usr/bin/env python3
"""
Actual-data taxonomic abundance analysis across sample types.

Matches power-analysis taxonomy levels and paired structure.
For each taxon at each taxonomic level:
- Omnibus (optional): Friedman test across sample types for patients with all types
- Pairwise planned comparisons (paired Wilcoxon signed-rank):
  BAL vs Oral Rinse, BAL vs Lung Brush, Oral Rinse vs Lung Brush
- BH-FDR correction across taxa per contrast (and for omnibus separately)
"""

from __future__ import annotations

import argparse
from itertools import combinations
from pathlib import Path
import numpy as np
import pandas as pd
from scipy.stats import friedmanchisquare, wilcoxon
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


def parse_csv_values(raw: str) -> list[str]:
    return [x.strip() for x in str(raw).split(",") if x.strip()]


def observed_values(df: pd.DataFrame, col: str) -> list[str]:
    return sorted([str(x) for x in df[col].dropna().astype(str).unique()])


def filter_contralateral_cancer(df: pd.DataFrame, case_col: str, type_col: str, contralateral_sample_types: list[str], contralateral_col: str,
                                cancer_site_col: str, lung_side_col: str,
                                contralateral_value: str) -> pd.DataFrame:
    work = df.copy()
    if case_col not in work.columns:
        return work
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


def prepare_patient_type_table(
    long_df: pd.DataFrame,
    tax_level: str,
    sample_col: str,
    patient_col: str,
    type_col: str,
    count_col: str,
    min_prevalence: float,
    sample_types: list[str],
    transform: str,
) -> tuple[dict[str, pd.DataFrame], list[str]]:
    df = long_df[long_df[type_col].isin(sample_types)].copy()
    tax_wide = aggregate_to_taxonomy(df, tax_level, sample_col, count_col)
    meta = df[[sample_col, patient_col, type_col]].drop_duplicates().set_index(sample_col)
    sample_ids = meta.index.intersection(tax_wide.index)
    meta = meta.loc[sample_ids].copy()
    tax_wide = tax_wide.loc[sample_ids].copy()
    patient_by_type: dict[str, pd.DataFrame] = {}
    sample_rel = pd.DataFrame(
        relative_abundance(tax_wide.values),
        index=tax_wide.index,
        columns=tax_wide.columns,
    )

    for st in sample_types:
        st_samples = meta.index[meta[type_col] == st]
        if len(st_samples) == 0:
            continue
        st_rel = sample_rel.loc[st_samples].copy()
        st_patients = meta.loc[st_samples, patient_col].values

        patient_by_type[st] = st_rel.assign(_patient=st_patients).groupby("_patient").mean()

    if not patient_by_type:
        return {}, []

    pooled = pd.concat(patient_by_type.values(), axis=0)
    prevalence = (pooled > 0).mean(axis=0)
    keep_taxa = prevalence[prevalence >= min_prevalence].index.tolist()
    if len(keep_taxa) == 0:
        return {}, []

    for st, patient_rel in list(patient_by_type.items()):
        patient_rel = patient_rel[keep_taxa]
        if transform == "rclr":
            patient_by_type[st] = rclr_transform(patient_rel)
        else:
            patient_by_type[st] = patient_rel.copy()

    return patient_by_type, keep_taxa


def run_pairwise_tests(patient_by_type: dict[str, pd.DataFrame], taxa: list[str], pairs: list[tuple[str, str]]) -> pd.DataFrame:
    rows = []
    for a, b in pairs:
        if a not in patient_by_type or b not in patient_by_type:
            continue
        shared = patient_by_type[a].index.intersection(patient_by_type[b].index)
        for taxon in taxa:
            if len(shared) >= 3:
                x = patient_by_type[a].loc[shared, taxon].values
                y = patient_by_type[b].loc[shared, taxon].values
                try:
                    stat, p = wilcoxon(x, y, zero_method="wilcox", correction=False, alternative="two-sided", mode="auto")
                except ValueError:
                    stat, p = np.nan, 1.0
                median_delta = float(np.median(x - y))
            else:
                stat, p, median_delta = np.nan, np.nan, np.nan

            rows.append(
                {
                    "contrast": f"{a}_vs_{b}",
                    "group1": a,
                    "group2": b,
                    "taxon": taxon,
                    "n_paired_patients": len(shared),
                    "median_delta_g1_minus_g2": median_delta,
                    "wilcoxon_w": float(stat) if not np.isnan(stat) else np.nan,
                    "p_value": float(p) if not np.isnan(p) else np.nan,
                }
            )

    out = pd.DataFrame(rows)
    if out.empty:
        return out

    out["q_value"] = np.nan
    for c in out["contrast"].unique():
        idx = out["contrast"] == c
        valid = idx & out["p_value"].notna()
        if valid.sum() > 0:
            out.loc[valid, "q_value"] = multipletests(out.loc[valid, "p_value"], method="fdr_bh")[1]
    out["significant_fdr_0.05"] = out["q_value"] < 0.05
    return out


def run_omnibus_friedman(patient_by_type: dict[str, pd.DataFrame], taxa: list[str], sample_types: list[str]) -> pd.DataFrame:
    available = [s for s in sample_types if s in patient_by_type]
    if len(available) < 3:
        return pd.DataFrame()

    shared = patient_by_type[available[0]].index
    for s in available[1:]:
        shared = shared.intersection(patient_by_type[s].index)

    rows = []
    for taxon in taxa:
        if len(shared) >= 3:
            vectors = [patient_by_type[s].loc[shared, taxon].values for s in available]
            try:
                stat, p = friedmanchisquare(*vectors)
            except ValueError:
                stat, p = np.nan, np.nan
        else:
            stat, p = np.nan, np.nan

        rows.append(
            {
                "test": "friedman",
                "sample_types": "|".join(available),
                "taxon": taxon,
                "n_complete_patients": len(shared),
                "friedman_chi2": float(stat) if not np.isnan(stat) else np.nan,
                "p_value": float(p) if not np.isnan(p) else np.nan,
            }
        )

    out = pd.DataFrame(rows)
    valid = out["p_value"].notna()
    out["q_value"] = np.nan
    if valid.sum() > 0:
        out.loc[valid, "q_value"] = multipletests(out.loc[valid, "p_value"], method="fdr_bh")[1]
    out["significant_fdr_0.05"] = out["q_value"] < 0.05
    return out


def main() -> None:
    p = argparse.ArgumentParser(description="Taxonomic abundance analysis across paired/repeated metadata groups on observed data")
    p.add_argument("--data-long", required=True)
    p.add_argument("--tax-levels", default="Phylum,Family")
    p.add_argument("--sample-types", default="BAL,Oral Rinse,Lung Brush", help="Comma-separated group labels in --type-col; use empty/all to use all observed values")
    p.add_argument("--sample-col", default="sample")
    p.add_argument("--patient-col", default="Participant_ID")
    p.add_argument("--case-col", default="Case")
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
    p.add_argument("--skip-omnibus", action="store_true")
    p.add_argument("--transform", choices=["none", "rclr"], default="none")
    p.add_argument("--outdir", required=True)
    args = p.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    long_df = pd.read_csv(args.data_long, sep="\t", low_memory=False)
    long_df[args.type_col] = long_df[args.type_col].map(canonicalize_sample_type)
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
    sample_types_raw = parse_csv_values(args.sample_types)
    if not sample_types_raw or [x.lower() for x in sample_types_raw] == ["all"]:
        sample_types = observed_values(long_df, args.type_col)
    else:
        sample_types = [canonicalize_sample_type(x) for x in sample_types_raw]
    pairs = list(combinations(sample_types, 2))

    pair_results = []
    omni_results = []

    for tax_level in tax_levels:
        patient_by_type, taxa = prepare_patient_type_table(
            long_df,
            tax_level=tax_level,
            sample_col=args.sample_col,
            patient_col=args.patient_col,
            type_col=args.type_col,
            count_col=args.count_col,
            min_prevalence=args.min_prevalence,
            sample_types=sample_types,
            transform=args.transform,
        )

        pair_df = run_pairwise_tests(patient_by_type, taxa, pairs)
        if not pair_df.empty:
            pair_df.insert(0, "tax_level", tax_level)
            pair_results.append(pair_df)

        if not args.skip_omnibus:
            omni_df = run_omnibus_friedman(patient_by_type, taxa, sample_types)
            if not omni_df.empty:
                omni_df.insert(0, "tax_level", tax_level)
                omni_results.append(omni_df)

    if pair_results:
        pair_out = pd.concat(pair_results, ignore_index=True)
    else:
        pair_out = pd.DataFrame(
            columns=[
                "tax_level", "contrast", "group1", "group2", "taxon",
                "n_paired_patients", "median_delta_g1_minus_g2",
                "wilcoxon_w", "p_value", "q_value", "significant_fdr_0.05",
            ]
        )
        print("No pairwise group results produced; writing empty result tables.")
    pair_out.to_csv(outdir / "taxonomic_sample_type_observed_pairwise.tsv", sep="\t", index=False)
    pair_out[pair_out.get("significant_fdr_0.05", pd.Series(dtype=bool)).fillna(False)].to_csv(
        outdir / "taxonomic_sample_type_observed_pairwise_significant.tsv", sep="\t", index=False
    )
    print(f"Saved: {outdir / 'taxonomic_sample_type_observed_pairwise.tsv'}")

    if omni_results:
        omni_out = pd.concat(omni_results, ignore_index=True)
    else:
        omni_out = pd.DataFrame(
            columns=[
                "tax_level", "test", "sample_types", "taxon",
                "n_complete_patients", "friedman_chi2", "p_value",
                "q_value", "significant_fdr_0.05",
            ]
        )
        print("No omnibus group results produced; writing empty result tables.")
    omni_out.to_csv(outdir / "taxonomic_sample_type_observed_omnibus.tsv", sep="\t", index=False)
    omni_out[omni_out.get("significant_fdr_0.05", pd.Series(dtype=bool)).fillna(False)].to_csv(
        outdir / "taxonomic_sample_type_observed_omnibus_significant.tsv", sep="\t", index=False
    )
    print(f"Saved: {outdir / 'taxonomic_sample_type_observed_omnibus.tsv'}")


if __name__ == "__main__":
    main()

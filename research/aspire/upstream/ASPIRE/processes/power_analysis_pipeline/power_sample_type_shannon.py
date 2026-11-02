#!/usr/bin/env python3
"""
power_sample_type_shannon.py

Shannon diversity power analysis for sample type comparisons.
Tests paired Wilcoxon comparisons (BAL vs Bronchial Brush vs Oral Rinse) with FDR correction.
"""

import argparse
import warnings
from pathlib import Path
import numpy as np
import pandas as pd
from scipy.stats import wilcoxon
from statsmodels.stats.multitest import multipletests
from itertools import combinations

warnings.filterwarnings('ignore')


def normalize_sample_ids(values):
    """Normalize sample IDs so numeric-looking IDs align across long and wide tables."""
    series = pd.Series(values, copy=False)
    as_num = pd.to_numeric(series, errors='coerce')
    normalized = series.astype(str)
    int_like = as_num.notna() & np.isclose(as_num % 1, 0)
    normalized.loc[int_like] = as_num.loc[int_like].astype(np.int64).astype(str)
    return normalized


def resolve_sample_col(df, sample_col):
    if sample_col in df.columns:
        return sample_col
    alias = 'lmp_id' if sample_col == 'sample' else 'sample' if sample_col == 'lmp_id' else None
    if alias and alias in df.columns:
        print(f"[WARN] Sample column '{sample_col}' not found; using legacy alias '{alias}'.")
        return alias
    raise KeyError(f"Sample column '{sample_col}' not found in long-format data.")


def shannon_diversity(counts):
    """Calculate Shannon diversity for a count vector."""
    counts = counts[counts > 0]
    if len(counts) == 0:
        return 0.0
    props = counts / counts.sum()
    return -np.sum(props * np.log(props))


def patient_level_shannon(count_matrix, patient_ids):
    """
    Compute patient-level Shannon diversity by averaging across samples from same patient.
    """
    unique_patients = np.unique(patient_ids)
    patient_shannon = {}

    for patient in unique_patients:
        patient_mask = patient_ids == patient
        patient_samples = count_matrix[patient_mask, :]

        # Average Shannon across samples from this patient
        shannon_vals = [shannon_diversity(sample) for sample in patient_samples]
        patient_shannon[patient] = np.mean(shannon_vals)

    return patient_shannon


def bootstrap_patients_sample_types(count_matrix, patient_ids, sample_types,
                                     n_patients, seed=42):
    """
    Bootstrap resample patients with replacement.
    For each resampled patient, include all their samples across different sample types.
    """
    np.random.seed(seed)

    unique_patients = np.unique(patient_ids)

    # Resample patients with replacement
    boot_patients_selected = np.random.choice(unique_patients, size=n_patients, replace=True)

    # Track draw counts for unique IDs
    patient_draw_count = {}

    boot_indices = []
    boot_patient_list = []
    boot_sample_type_list = []

    for patient in boot_patients_selected:
        draw_idx = patient_draw_count.get(patient, 0)
        patient_draw_count[patient] = draw_idx + 1

        unique_boot_id = f"{patient}__b{draw_idx}"

        # Get all samples from this patient
        patient_samples = np.where(patient_ids == patient)[0]
        boot_indices.extend(patient_samples)
        boot_patient_list.extend([unique_boot_id] * len(patient_samples))
        boot_sample_type_list.extend(sample_types[patient_samples])

    boot_counts = count_matrix[boot_indices, :]
    boot_patient_ids = np.array(boot_patient_list)
    boot_sample_types = np.array(boot_sample_type_list)

    return boot_counts, boot_patient_ids, boot_sample_types


def run_power_simulation(count_matrix, patient_ids, sample_types,
                         sample_type_list, n_patients,
                         n_simulations=1000, alpha=0.05, seed=42):
    """
    Run power simulation for Shannon diversity pairwise comparisons with FDR.

    For each bootstrap:
    1. Compute patient-level Shannon for each sample type
    2. Run pairwise Wilcoxon tests
    3. Apply FDR correction
    4. Record if any comparison is significant at alpha
    """
    significant_any = 0  # Power to detect at least one difference

    for i in range(n_simulations):
        # Bootstrap patients
        boot_counts, boot_patients, boot_stypes = bootstrap_patients_sample_types(
            count_matrix, patient_ids, sample_types,
            n_patients, seed=seed+i
        )

        # Compute patient-level Shannon for each sample type
        patient_shannon_by_type = {}
        for stype in sample_type_list:
            stype_mask = boot_stypes == stype
            if stype_mask.sum() > 0:
                stype_counts = boot_counts[stype_mask, :]
                stype_patients = boot_patients[stype_mask]
                patient_shannon_by_type[stype] = patient_level_shannon(stype_counts, stype_patients)

        # Pairwise Wilcoxon tests
        p_values = []
        for stype1, stype2 in combinations(sample_type_list, 2):
            if stype1 not in patient_shannon_by_type or stype2 not in patient_shannon_by_type:
                continue

            # Get patients who have both sample types (paired)
            patients_both = set(patient_shannon_by_type[stype1].keys()) & \
                           set(patient_shannon_by_type[stype2].keys())

            if len(patients_both) >= 3:
                shannon1 = np.array([patient_shannon_by_type[stype1][p] for p in patients_both])
                shannon2 = np.array([patient_shannon_by_type[stype2][p] for p in patients_both])

                try:
                    _, p = wilcoxon(
                        shannon1,
                        shannon2,
                        zero_method='wilcox',
                        correction=False,
                        alternative='two-sided',
                        mode='auto',
                    )
                except ValueError:
                    continue
                p_values.append(p)

        # FDR correction
        if len(p_values) > 0:
            _, p_corrected, _, _ = multipletests(p_values, alpha=alpha, method='fdr_bh')
            if np.any(p_corrected < alpha):
                significant_any += 1

        if (i + 1) % 100 == 0:
            print(f"    {i+1}/{n_simulations} simulations...", end='\r')

    power = significant_any / n_simulations
    return power


def main():
    parser = argparse.ArgumentParser(
        description="Sample type Shannon diversity power analysis"
    )
    parser.add_argument("--data-wide", required=True, help="Wide count matrix")
    parser.add_argument("--data-long", required=True, help="Long format data")
    parser.add_argument("--sample-sizes", default="10,15,20,25,30,40,50",
                       help="Comma-separated sample sizes (n patients)")
    parser.add_argument("--n-simulations", type=int, default=1000)
    parser.add_argument("--alpha", type=float, default=0.05)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--patient-col", default="Participant_ID")
    parser.add_argument("--type-col", default="type_group")
    parser.add_argument("--sample-col", default="sample")
    parser.add_argument("--outdir", required=True)
    args = parser.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    sample_sizes = [int(x) for x in args.sample_sizes.split(",")]

    print("="*60)
    print("Sample Type Shannon Diversity Power Analysis")
    print("="*60)
    print(f"Sample sizes: {sample_sizes}")
    print(f"Simulations per size: {args.n_simulations}")
    print()

    # Load data
    print("Loading data...")
    long_df = pd.read_csv(args.data_long, sep='\t')
    wide_df = pd.read_csv(args.data_wide, sep='\t', index_col=0)
    sample_col = resolve_sample_col(long_df, args.sample_col)
    long_df[sample_col] = normalize_sample_ids(long_df[sample_col])
    wide_df.columns = normalize_sample_ids(wide_df.columns)

    # Get metadata
    metadata = long_df[[sample_col, args.patient_col, args.type_col]].drop_duplicates()
    metadata = metadata.set_index(sample_col)

    sample_ids = metadata.index.intersection(wide_df.columns)
    if len(sample_ids) == 0:
        raise ValueError(
            "No overlapping samples between long and wide power-analysis inputs after sample-ID normalization."
        )
    metadata = metadata.loc[sample_ids]
    wide_df_filtered = wide_df[sample_ids].T

    count_matrix = wide_df_filtered.values.astype(float)
    patient_ids = metadata[args.patient_col].values
    sample_types = metadata[args.type_col].values
    sample_type_list = np.unique(sample_types)

    print(f"Data: {count_matrix.shape[0]} samples, {count_matrix.shape[1]} ASVs")
    print(f"Patients: {len(np.unique(patient_ids))}")
    print(f"Sample types: {list(sample_type_list)}")
    print()

    # Run power simulations
    results = []

    for n in sample_sizes:
        print(f"\nSample size: n={n} patients")
        power = run_power_simulation(
            count_matrix, patient_ids, sample_types, sample_type_list,
            n_patients=n,
            n_simulations=args.n_simulations,
            alpha=args.alpha,
            seed=args.seed
        )

        print(f"  Power (any pairwise diff after FDR): {power:.3f}")

        results.append({
            'n_patients': n,
            'power_any_difference': power,
            'n_simulations': args.n_simulations
        })

        # Early stopping: if power >= 0.995, skip remaining sample sizes
        if power >= 0.995:
            print(f"  → Power ≥ 0.995 reached. Skipping larger sample sizes.")
            break

    # Save results
    results_df = pd.DataFrame(results)
    outfile = outdir / 'sample_type_shannon_power.tsv'
    results_df.to_csv(outfile, sep='\t', index=False)

    print(f"\n{'='*60}")
    print(f"Results saved to: {outfile}")
    print("="*60)


if __name__ == '__main__':
    main()

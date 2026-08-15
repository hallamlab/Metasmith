#!/usr/bin/env python3
"""
power_sample_type_permanova_CORRECTED.py

PERMANOVA power analysis for sample type comparisons with BLOCKED permutations.

CRITICAL FIX: Now matches R analysis structure:
- Patient × sample_type aggregation (one row per patient-sampletype combination)
- Blocked permutations (permute sample types within patients only)
- Relative abundance normalization
- Both omnibus and pairwise tests
"""

import argparse
import warnings
from pathlib import Path
import numpy as np
import pandas as pd
from scipy.spatial.distance import pdist, squareform
from itertools import combinations
from statsmodels.stats.multitest import multipletests

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


def apply_transform(count_matrix, transform):
    if transform == 'rclr':
        out = np.zeros_like(count_matrix, dtype=float)
        for i in range(count_matrix.shape[0]):
            row = count_matrix[i, :].astype(float)
            pos = row > 0
            if np.any(pos):
                lv = np.log(row[pos])
                out[i, pos] = lv - lv.mean()
        return out
    totals = count_matrix.sum(axis=1, keepdims=True)
    totals[totals == 0] = 1
    return count_matrix / totals

def bray_curtis_from_counts(count_matrix, transform='none'):
    """
    Compute Bray-Curtis dissimilarity from count matrix.
    Converts to relative abundance before computing distance.
    """
    data = apply_transform(count_matrix, transform)
    metric = 'euclidean' if transform == 'rclr' else 'braycurtis'
    bc = pdist(data, metric=metric)
    return squareform(bc)


def mean_relative_by_group(count_matrix, group_ids):
    sample_rel = apply_transform(count_matrix, 'none')
    unique_groups = np.unique(group_ids)
    agg = np.zeros((len(unique_groups), sample_rel.shape[1]))
    for i, group in enumerate(unique_groups):
        mask = group_ids == group
        agg[i, :] = sample_rel[mask, :].mean(axis=0)
    return agg, unique_groups


def permanova_r2(dist_matrix, group_labels):
    """Compute PERMANOVA R² (proportion of variance explained)."""
    n = dist_matrix.shape[0]
    H = np.eye(n) - np.ones((n, n)) / n
    D_squared = dist_matrix ** 2
    G = -0.5 * H @ D_squared @ H
    SS_total = np.trace(G)

    unique_groups = np.unique(group_labels)
    SS_between = 0
    for group in unique_groups:
        mask = group_labels == group
        n_g = mask.sum()
        if n_g > 0:
            G_g = G[np.ix_(mask, mask)]
            centroid_ss = np.sum(G_g) / n_g
            SS_between += centroid_ss

    R2 = SS_between / SS_total if SS_total > 0 else 0
    return R2


def permanova_permutation_test_blocked(dist_matrix, sample_types, patient_ids,
                                       n_perm=199, seed=42):
    """
    PERMANOVA with BLOCKED permutations by patient.

    CRITICAL: Permutes sample type labels within each patient only.
    This respects the repeated measures structure.

    Parameters
    ----------
    dist_matrix : np.ndarray
        Distance matrix (rows × rows)
    sample_types : np.ndarray
        Sample type labels for each row
    patient_ids : np.ndarray
        Patient ID for each row
    n_perm : int
        Number of permutations
    seed : int
        Random seed

    Returns
    -------
    obs_r2 : float
        Observed R²
    p_value : float
        Permutation p-value
    """
    np.random.seed(seed)

    obs_r2 = permanova_r2(dist_matrix, sample_types)

    # Get unique patients and their sample types
    unique_patients = np.unique(patient_ids)

    perm_r2 = []
    for i in range(n_perm):
        # For each patient, permute their sample type labels
        perm_sample_types = sample_types.copy()

        for patient in unique_patients:
            patient_mask = patient_ids == patient
            patient_stypes = sample_types[patient_mask]

            # Shuffle sample types for this patient only
            shuffled = np.random.permutation(patient_stypes)
            perm_sample_types[patient_mask] = shuffled

        perm_r2.append(permanova_r2(dist_matrix, perm_sample_types))

    p_value = (1 + np.sum(np.array(perm_r2) >= obs_r2)) / (n_perm + 1)

    return obs_r2, p_value


def aggregate_to_patient_sample_type(count_matrix, patient_ids, sample_types):
    """
    Aggregate to patient × sample_type level.

    Returns one row per (patient, sample_type) combination.
    Matches R script line 236-237.
    """
    pt_stype_keys = np.array([f"{pid}||{stype}" for pid, stype in zip(patient_ids, sample_types)])
    agg_counts, unique_keys = mean_relative_by_group(count_matrix, pt_stype_keys)
    agg_patients = np.array([key.split("||", 1)[0] for key in unique_keys])
    agg_stypes = np.array([key.split("||", 1)[1] for key in unique_keys])
    return agg_counts, agg_patients, agg_stypes


def bootstrap_patients_sample_types(count_matrix, patient_ids, sample_types,
                                     n_patients, seed=42):
    """
    Bootstrap resample patients, keeping all their sample types.

    CRITICAL: Resamples at patient level, includes all sample types for each patient.
    """
    np.random.seed(seed)

    unique_patients = np.unique(patient_ids)

    # Resample patients with replacement
    boot_patients_selected = np.random.choice(unique_patients, size=n_patients, replace=True)

    # Track draw counts for unique IDs
    patient_draw_count = {}

    boot_indices = []
    boot_patient_list = []
    boot_stype_list = []

    for patient in boot_patients_selected:
        draw_idx = patient_draw_count.get(patient, 0)
        patient_draw_count[patient] = draw_idx + 1

        unique_boot_id = f"{patient}__b{draw_idx}"

        # Get all samples from this patient (across all sample types)
        patient_samples = np.where(patient_ids == patient)[0]
        boot_indices.extend(patient_samples)
        boot_patient_list.extend([unique_boot_id] * len(patient_samples))
        boot_stype_list.extend(sample_types[patient_samples])

    boot_counts = count_matrix[boot_indices, :]
    boot_patients = np.array(boot_patient_list)
    boot_stypes = np.array(boot_stype_list)

    return boot_counts, boot_patients, boot_stypes


def run_power_simulation_omnibus(count_matrix, patient_ids, sample_types,
                                  n_patients, n_simulations=1000, n_perm=199,
                                  alpha=0.05, seed=42, transform='none'):
    """
    Power simulation for omnibus sample type test (all types together).
    """
    significant_count = 0
    r2_values = []

    for i in range(n_simulations):
        # Bootstrap patients
        boot_counts, boot_patients, boot_stypes = bootstrap_patients_sample_types(
            count_matrix, patient_ids, sample_types, n_patients, seed=seed+i
        )

        # Aggregate to patient × sample_type level
        agg_counts, agg_patients, agg_stypes = aggregate_to_patient_sample_type(
            boot_counts, boot_patients, boot_stypes
        )

        # Filter to patients with at least 2 sample types
        patient_counts = pd.Series(agg_patients).value_counts()
        valid_patients = patient_counts[patient_counts >= 2].index
        mask = np.isin(agg_patients, valid_patients)

        agg_counts = agg_counts[mask, :]
        agg_patients = agg_patients[mask]
        agg_stypes = agg_stypes[mask]

        if len(np.unique(agg_stypes)) < 2:
            continue

        # Compute Bray-Curtis
        bc_dist = bray_curtis_from_counts(agg_counts, transform=transform)

        # PERMANOVA with blocked permutations
        r2, p_value = permanova_permutation_test_blocked(
            bc_dist, agg_stypes, agg_patients,
            n_perm=n_perm, seed=seed+i
        )

        r2_values.append(r2)
        if p_value < alpha:
            significant_count += 1

    power = significant_count / n_simulations
    mean_r2 = np.mean(r2_values)

    return power, mean_r2, r2_values


def run_power_simulation_pairwise(count_matrix, patient_ids, sample_types,
                                   stype1, stype2, n_patients,
                                   n_simulations=1000, n_perm=199,
                                   alpha=0.05, seed=42, transform='none'):
    """
    Power simulation for pairwise sample type comparison.

    Only includes patients with BOTH sample types (paired analysis).
    """
    significant_count = 0
    r2_values = []

    for i in range(n_simulations):
        # Bootstrap patients
        boot_counts, boot_patients, boot_stypes = bootstrap_patients_sample_types(
            count_matrix, patient_ids, sample_types, n_patients, seed=seed+i
        )

        # Aggregate to patient × sample_type level
        agg_counts, agg_patients, agg_stypes = aggregate_to_patient_sample_type(
            boot_counts, boot_patients, boot_stypes
        )

        # Filter to the two sample types of interest
        mask = np.isin(agg_stypes, [stype1, stype2])
        agg_counts = agg_counts[mask, :]
        agg_patients = agg_patients[mask]
        agg_stypes = agg_stypes[mask]

        # Keep only patients with BOTH sample types (paired)
        patient_counts = pd.Series(agg_patients).value_counts()
        paired_patients = patient_counts[patient_counts == 2].index
        mask = np.isin(agg_patients, paired_patients)

        agg_counts = agg_counts[mask, :]
        agg_patients = agg_patients[mask]
        agg_stypes = agg_stypes[mask]

        if len(agg_counts) < 4:  # Need at least 2 patients
            continue

        # Compute Bray-Curtis
        bc_dist = bray_curtis_from_counts(agg_counts, transform=transform)

        # PERMANOVA with blocked permutations
        r2, p_value = permanova_permutation_test_blocked(
            bc_dist, agg_stypes, agg_patients,
            n_perm=n_perm, seed=seed+i
        )

        r2_values.append(r2)
        if p_value < alpha:
            significant_count += 1

    power = significant_count / n_simulations
    mean_r2 = np.mean(r2_values)

    return power, mean_r2, r2_values


def main():
    parser = argparse.ArgumentParser(
        description="Sample type PERMANOVA power analysis (CORRECTED with blocked permutations)"
    )
    parser.add_argument("--data-wide", required=True)
    parser.add_argument("--data-long", required=True)
    parser.add_argument("--sample-sizes", default="10,15,20,25,30,40,50")
    parser.add_argument("--n-simulations", type=int, default=1000)
    parser.add_argument("--n-perm", type=int, default=199)
    parser.add_argument("--alpha", type=float, default=0.05)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--outdir", required=True)
    parser.add_argument("--transform", choices=["none", "rclr"], default="none")
    parser.add_argument("--sample-col", default="sample")
    parser.add_argument("--patient-col", default="Participant_ID")
    parser.add_argument("--type-col", default="type_group")
    args = parser.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    print("="*60)
    print("Sample Type PERMANOVA Power Analysis (CORRECTED)")
    print("="*60)
    print("IMPORTANT: Now uses blocked permutations by patient")
    print("Matches R analysis structure")
    print("="*60)
    print()

    print("Loading data...")
    wide_df = pd.read_csv(args.data_wide, sep='\t', index_col=0)
    long_df = pd.read_csv(args.data_long, sep='\t')
    sample_col = resolve_sample_col(long_df, args.sample_col)
    long_df[sample_col] = normalize_sample_ids(long_df[sample_col])
    wide_df.columns = normalize_sample_ids(wide_df.columns)

    metadata = long_df[[sample_col, args.patient_col, args.type_col]].drop_duplicates().set_index(sample_col)
    sample_ids = metadata.index.intersection(wide_df.columns)
    if len(sample_ids) == 0:
        raise ValueError(
            "No overlapping samples between long and wide power-analysis inputs after sample-ID normalization."
        )

    count_matrix = wide_df[sample_ids].T.values
    patient_ids = metadata.loc[sample_ids, args.patient_col].values
    sample_types = metadata.loc[sample_ids, args.type_col].values

    # Aggregate to patient × sample_type level
    agg_counts, agg_patients, agg_stypes = aggregate_to_patient_sample_type(
        count_matrix, patient_ids, sample_types
    )

    print(f"Data: {len(np.unique(patient_ids))} patients, {len(np.unique(sample_types))} sample types")
    print(f"Patient × sample_type combinations: {len(agg_patients)}")
    print()

    sample_sizes = [int(x) for x in args.sample_sizes.split(',')]

    # Omnibus test results
    omnibus_results = []

    print("Running OMNIBUS sample type test (all types together)...")
    print("-"*60)

    for n_patients in sample_sizes:
        print(f"  n={n_patients}...", end=' ', flush=True)

        power, mean_r2, r2_vals = run_power_simulation_omnibus(
            agg_counts, agg_patients, agg_stypes,
            n_patients, args.n_simulations, args.n_perm, args.alpha, args.seed,
            transform=args.transform
        )

        print(f"Power={power:.3f}, R²={mean_r2:.4f}")

        omnibus_results.append({
            'n_patients': n_patients,
            'power': power,
            'mean_r2': mean_r2,
            'r2_ci_lower': np.percentile(r2_vals, 2.5),
            'r2_ci_upper': np.percentile(r2_vals, 97.5),
            'n_simulations': args.n_simulations
        })

        # Early stopping: if power >= 0.995, skip remaining sample sizes
        if power >= 0.995:
            print(f"  → Power ≥ 0.995 reached. Skipping larger sample sizes.")
            break

    omnibus_df = pd.DataFrame(omnibus_results)
    omnibus_df.to_csv(outdir / 'sample_type_permanova_power_omnibus.tsv',
                      sep='\t', index=False)

    print()
    print("Running PAIRWISE sample type tests...")
    print("-"*60)

    # Pairwise tests
    pairwise_results = []
    unique_stypes = np.unique(sample_types)

    for stype1, stype2 in combinations(sorted(unique_stypes), 2):
        print(f"\n{stype1} vs {stype2}:")

        for n_patients in sample_sizes:
            print(f"  n={n_patients}...", end=' ', flush=True)

            power, mean_r2, r2_vals = run_power_simulation_pairwise(
                agg_counts, agg_patients, agg_stypes,
                stype1, stype2, n_patients,
                args.n_simulations, args.n_perm, args.alpha, args.seed,
                transform=args.transform
            )

            print(f"Power={power:.3f}, R²={mean_r2:.4f}")

            pairwise_results.append({
                'group1': stype1,
                'group2': stype2,
                'comparison': f"{stype1} vs {stype2}",
                'n_patients': n_patients,
                'power': power,
                'mean_r2': mean_r2,
                'r2_ci_lower': np.percentile(r2_vals, 2.5),
                'r2_ci_upper': np.percentile(r2_vals, 97.5),
                'n_simulations': args.n_simulations
            })

            # Early stopping: if power >= 0.995, skip remaining sample sizes
            if power >= 0.995:
                print(f"  → Power ≥ 0.995 reached. Skipping larger sample sizes for this comparison.")
                break

    pairwise_df = pd.DataFrame(pairwise_results)
    pairwise_df.to_csv(outdir / 'sample_type_permanova_power_pairwise.tsv',
                       sep='\t', index=False)

    print()
    print("="*60)
    print(f"✓ Results saved to {outdir}/")
    print("  - sample_type_permanova_power_omnibus.tsv")
    print("  - sample_type_permanova_power_pairwise.tsv")
    print("="*60)


if __name__ == '__main__':
    main()

#!/usr/bin/env python3
"""
power_permanova.py

PERMANOVA power analysis using patient-level bootstrap with spike-in scenarios.
"""

import argparse
import json
import warnings
from pathlib import Path
import numpy as np
import pandas as pd
from scipy.spatial.distance import pdist, squareform
from simulate_data import bootstrap_patients, spike_in_fold_change

warnings.filterwarnings('ignore')


def resolve_sample_col(df, sample_col):
    if sample_col in df.columns:
        return sample_col
    alias = 'lmp_id' if sample_col == 'sample' else 'sample' if sample_col == 'lmp_id' else None
    if alias and alias in df.columns:
        print(f"[WARN] Sample column '{sample_col}' not found; using legacy alias '{alias}'.")
        return alias
    raise KeyError(f"Sample column '{sample_col}' not found in long-format data.")


def bray_curtis_from_counts(count_matrix):
    """Compute Bray-Curtis dissimilarity from count matrix (samples × ASVs)."""
    bc = pdist(count_matrix, metric='braycurtis')
    return squareform(bc)


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


def permanova_permutation_test(dist_matrix, group_labels, patient_ids, n_perm=199, seed=42):
    """
    PERMANOVA with restricted permutations of patient labels.
    
    Returns p-value from permutation test.
    """
    np.random.seed(seed)
    
    # Observed R²
    obs_r2 = permanova_r2(dist_matrix, group_labels)
    
    # Permutation test (permute patient labels only)
    unique_patients = np.unique(patient_ids)
    patient_to_group = {}
    for patient in unique_patients:
        patient_mask = patient_ids == patient
        patient_to_group[patient] = group_labels[patient_mask][0]
    
    perm_r2 = []
    for i in range(n_perm):
        # Shuffle patient-to-group mapping
        shuffled_groups = np.random.permutation(list(patient_to_group.values()))
        shuffled_mapping = dict(zip(unique_patients, shuffled_groups))
        
        # Apply to all samples
        perm_groups = np.array([shuffled_mapping[p] for p in patient_ids])
        perm_r2.append(permanova_r2(dist_matrix, perm_groups))
    
    # P-value
    p_value = (1 + np.sum(np.array(perm_r2) >= obs_r2)) / (n_perm + 1)
    
    return obs_r2, p_value


def run_power_simulation(count_matrix, patient_ids, case_status, asv_names,
                         spike_asvs, spike_fold_change, n_cancer, n_control,
                         n_simulations=1000, n_perm=199, alpha=0.05, seed=42):
    """
    Run power simulation for PERMANOVA at given sample size and spike scenario.
    
    Returns power, mean R², and achieved R² values across simulations.
    """
    significant_count = 0
    r2_values = []
    
    for i in range(n_simulations):
        # Bootstrap resample patients
        boot_counts, boot_patients, boot_case = bootstrap_patients(
            count_matrix, patient_ids, case_status,
            n_cancer, n_control, seed=seed+i
        )
        
        # Apply spike-in if specified
        if spike_asvs and spike_fold_change > 1.0:
            asv_indices = [j for j, asv in enumerate(asv_names) if asv in spike_asvs]
            boot_counts = spike_in_fold_change(boot_counts, boot_case, asv_indices,
                                              spike_fold_change, 'Cancer')
        
        # Compute Bray-Curtis
        bc_dist = bray_curtis_from_counts(boot_counts)
        
        # PERMANOVA test
        r2, p_value = permanova_permutation_test(bc_dist, boot_case, boot_patients,
                                                 n_perm=n_perm, seed=seed+i)
        
        r2_values.append(r2)
        if p_value < alpha:
            significant_count += 1
    
    power = significant_count / n_simulations
    mean_r2 = np.mean(r2_values)
    
    return power, mean_r2, r2_values


def main():
    parser = argparse.ArgumentParser(description="PERMANOVA power analysis")
    parser.add_argument("--data-wide", required=True, help="Wide format ASV count matrix")
    parser.add_argument("--data-long", required=True, help="Long format ASV data")
    parser.add_argument("--spike-scenarios", required=True, help="JSON file with spike scenarios")
    parser.add_argument("--sample-sizes", default="6,8,10,15,20,25,30", help="Comma-separated cancer sample sizes")
    parser.add_argument("--n-control", type=int, default=25, help="Number of control patients")
    parser.add_argument("--n-simulations", type=int, default=1000, help="Number of bootstrap simulations")
    parser.add_argument("--n-perm", type=int, default=199, help="Number of permutations per test")
    parser.add_argument("--alpha", type=float, default=0.05, help="Significance level")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument("--sample-col", default="sample", help="Sample ID column in long-format data")
    parser.add_argument("--patient-col", default="Participant_ID", help="Patient ID column in long-format data")
    parser.add_argument("--case-col", default="Case", help="Case/control label column in long-format data")
    parser.add_argument("--outdir", required=True, help="Output directory")
    args = parser.parse_args()
    
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    
    # Load data
    print("Loading data...")
    wide_df = pd.read_csv(args.data_wide, sep='\t', index_col=0)
    long_df = pd.read_csv(args.data_long, sep='\t')
    sample_col = resolve_sample_col(long_df, args.sample_col)

    # Get metadata
    metadata = long_df[[sample_col, args.patient_col, args.case_col]].drop_duplicates().set_index(sample_col)
    sample_ids = metadata.index.intersection(wide_df.columns)

    count_matrix = wide_df[sample_ids].T.values
    patient_ids = metadata.loc[sample_ids, args.patient_col].values
    case_status = metadata.loc[sample_ids, args.case_col].values
    asv_names = wide_df.index.tolist()
    
    print(f"Loaded {count_matrix.shape[0]} samples, {count_matrix.shape[1]} ASVs")
    
    # Load spike scenarios
    with open(args.spike_scenarios, 'r') as f:
        spike_scenarios = json.load(f)
    
    # Define power analysis scenarios
    scenarios = [
        {'name': 'Null', 'asvs': [], 'fold_change': 1.0},
        {'name': 'Weak', 'asvs': spike_scenarios['high_prev_high_abund'][:5], 'fold_change': 1.5},
        {'name': 'Moderate', 'asvs': spike_scenarios['high_prev_high_abund'][:10], 'fold_change': 2.0},
        {'name': 'Strong', 'asvs': spike_scenarios['med_prev_med_abund'][:10] + spike_scenarios['high_prev_high_abund'][:10], 'fold_change': 2.5}
    ]
    
    # Sample size grid
    cancer_sizes = [int(x) for x in args.sample_sizes.split(',')]
    
    # Run power simulations
    results = []
    
    for scenario in scenarios:
        print(f"\n{'='*60}")
        print(f"Scenario: {scenario['name']}")
        print(f"  ASVs: {len(scenario['asvs'])}, Fold-change: {scenario['fold_change']}×")
        
        for n_cancer in cancer_sizes:
            print(f"  n_cancer={n_cancer}, n_control={args.n_control}...", end=' ')
            
            power, mean_r2, r2_vals = run_power_simulation(
                count_matrix, patient_ids, case_status, asv_names,
                scenario['asvs'], scenario['fold_change'],
                n_cancer, args.n_control,
                args.n_simulations, args.n_perm, args.alpha, args.seed
            )
            
            print(f"Power={power:.3f}, R²={mean_r2:.4f}")
            
            results.append({
                'Scenario': scenario['name'],
                'n_ASVs': len(scenario['asvs']),
                'Fold_change': scenario['fold_change'],
                'n_cancer': n_cancer,
                'n_control': args.n_control,
                'Power': power,
                'Mean_R2': mean_r2,
                'R2_CI_lower': np.percentile(r2_vals, 2.5),
                'R2_CI_upper': np.percentile(r2_vals, 97.5)
            })
    
    # Save results
    results_df = pd.DataFrame(results)
    results_df.to_csv(outdir / 'permanova_power_results.tsv', sep='\t', index=False)
    
    print(f"\n{'='*60}")
    print(f"Results saved to {outdir}")
    print("\n✓ PERMANOVA power analysis complete!")


if __name__ == '__main__':
    main()

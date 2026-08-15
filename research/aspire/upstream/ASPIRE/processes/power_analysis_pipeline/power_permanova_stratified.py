#!/usr/bin/env python3
"""
power_permanova_stratified.py

PERMANOVA power analysis stratified by sample type.
Uses patient-level bootstrap with spike-in scenarios.
"""

import argparse
import json
import warnings
from pathlib import Path
import numpy as np
import pandas as pd
from scipy.spatial.distance import pdist, squareform
from simulate_data import bootstrap_patients, spike_in_fold_change, filter_by_sample_type

warnings.filterwarnings('ignore')


def resolve_sample_col(df, sample_col):
    if sample_col in df.columns:
        return sample_col
    alias = 'lmp_id' if sample_col == 'sample' else 'sample' if sample_col == 'lmp_id' else None
    if alias and alias in df.columns:
        print(f"[WARN] Sample column '{sample_col}' not found; using legacy alias '{alias}'.")
        return alias
    raise KeyError(f"Sample column '{sample_col}' not found in long-format data.")


def filter_contralateral_metadata(meta_df, case_col='Case', type_col='type_group',
                                  contralateral_sample_types='Bronchial Brush,BAL',
                                  contralateral_col='lung_status',
                                  cancer_site_col='Cancer_Site',
                                  lung_side_col='lung_code',
                                  contralateral_value='Contralateral'):
    m = meta_df.copy()
    contra = contralateral_col
    if contra not in m.columns and {cancer_site_col, lung_side_col}.issubset(m.columns):
        cancer_side = m[cancer_site_col].astype(str).str[:1].str.upper()
        lung_side = m[lung_side_col].astype(str).str[:1].str.upper()
        case_vals = m[case_col].astype(str)
        m['.derived_lung_status'] = np.where(
            case_vals.isin(['Control', 'Non-Cancer']),
            'Healthy',
            np.where(cancer_side == lung_side, 'TumorSide', 'Contralateral')
        )
        contra = '.derived_lung_status'

    if contra not in m.columns:
        return m

    target_types = {x.strip() for x in str(contralateral_sample_types).split(',') if x.strip()}
    is_cancer = ~m[case_col].astype(str).isin(['Control', 'Non-Cancer'])
    is_contra = m[contra].astype(str) == str(contralateral_value)
    in_target = m[type_col].astype(str).isin(target_types)
    return m.loc[~(is_cancer & is_contra & in_target)].copy()


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
    Compute Bray-Curtis dissimilarity from count matrix (samples × ASVs).

    IMPORTANT: Converts to relative abundance before computing distance.
    This removes library size effects and focuses on compositional differences.
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


def bootstrap_patients_true_null(count_matrix, patient_ids, case_status,
                                  n_cancer, n_control, seed=42):
    """
    TRUE NULL: Pool all patients and randomly assign cancer/control labels.

    This breaks the link between labels and compositions.
    Type I error should be ≈ 0.05 if test is calibrated.
    """
    np.random.seed(seed)

    # Pool ALL patients (ignore original labels)
    unique_patients = np.unique(patient_ids)

    # Sample n_cancer + n_control patients with replacement
    n_total = n_cancer + n_control
    boot_patients_selected = np.random.choice(unique_patients, size=n_total, replace=True)

    # Randomly assign labels
    random_labels = np.random.permutation(
        ['Cancer'] * n_cancer + ['Control'] * n_control
    )

    # Build bootstrap dataset with random labels
    patient_draw_count = {}
    boot_indices = []
    boot_patient_list = []
    boot_case_list = []

    for i, patient in enumerate(boot_patients_selected):
        draw_idx = patient_draw_count.get(patient, 0)
        patient_draw_count[patient] = draw_idx + 1

        unique_boot_id = f"{patient}__b{draw_idx}"

        patient_samples = np.where(patient_ids == patient)[0]
        boot_indices.extend(patient_samples)
        boot_patient_list.extend([unique_boot_id] * len(patient_samples))
        boot_case_list.extend([random_labels[i]] * len(patient_samples))

    boot_counts = count_matrix[boot_indices, :]
    boot_patient_ids = np.array(boot_patient_list)
    boot_case = np.array(boot_case_list)

    return boot_counts, boot_patient_ids, boot_case


def permanova_permutation_test(dist_matrix, group_labels, patient_ids, n_perm=199, seed=42):
    """PERMANOVA with restricted permutations of patient labels."""
    np.random.seed(seed)

    obs_r2 = permanova_r2(dist_matrix, group_labels)

    unique_patients = np.unique(patient_ids)
    patient_to_group = {}
    for patient in unique_patients:
        patient_mask = patient_ids == patient
        patient_to_group[patient] = group_labels[patient_mask][0]

    perm_r2 = []
    for i in range(n_perm):
        shuffled_groups = np.random.permutation(list(patient_to_group.values()))
        shuffled_mapping = dict(zip(unique_patients, shuffled_groups))
        perm_groups = np.array([shuffled_mapping[p] for p in patient_ids])
        perm_r2.append(permanova_r2(dist_matrix, perm_groups))

    p_value = (1 + np.sum(np.array(perm_r2) >= obs_r2)) / (n_perm + 1)

    return obs_r2, p_value


def run_power_simulation(count_matrix, patient_ids, case_status, asv_names,
                         spike_asvs, spike_fold_change, n_cancer, n_control,
                         n_simulations=1000, n_perm=199, alpha=0.05, seed=42,
                         use_true_null=False, transform='none'):
    """
    Run power simulation for PERMANOVA at given sample size and spike scenario.

    Args:
        use_true_null: If True, uses pool-then-label null (for Type I error calibration)
    """
    significant_count = 0
    r2_values = []

    for i in range(n_simulations):
        if use_true_null:
            # True null: pool patients and randomly assign labels
            boot_counts, boot_patients, boot_case = bootstrap_patients_true_null(
                count_matrix, patient_ids, case_status,
                n_cancer, n_control, seed=seed+i
            )
        else:
            # Standard: resample within groups (preserves observed effect)
            boot_counts, boot_patients, boot_case = bootstrap_patients(
                count_matrix, patient_ids, case_status,
                n_cancer, n_control, seed=seed+i
            )

        if spike_asvs and spike_fold_change > 1.0:
            asv_indices = [j for j, asv in enumerate(asv_names) if asv in spike_asvs]
            boot_counts = spike_in_fold_change(boot_counts, boot_case, asv_indices,
                                              spike_fold_change, 'Cancer')

        patient_counts, unique_patients = mean_relative_by_group(boot_counts, boot_patients)
        patient_case_map = {p: boot_case[np.where(boot_patients == p)[0][0]] for p in unique_patients}
        patient_case = np.array([patient_case_map[p] for p in unique_patients])

        bc_dist = bray_curtis_from_counts(patient_counts, transform=transform)
        r2, p_value = permanova_permutation_test(bc_dist, patient_case, unique_patients,
                                                 n_perm=n_perm, seed=seed+i)

        r2_values.append(r2)
        if p_value < alpha:
            significant_count += 1

    power = significant_count / n_simulations
    mean_r2 = np.mean(r2_values)

    return power, mean_r2, r2_values


def main():
    parser = argparse.ArgumentParser(description="Stratified PERMANOVA power analysis")
    parser.add_argument("--data-wide", required=True)
    parser.add_argument("--data-long", required=True)
    parser.add_argument("--spike-scenarios", required=False)
    parser.add_argument("--sample-sizes", default="5,8,10,15,20,25,30,40,50,60,70,80,90,100")
    parser.add_argument("--n-simulations", type=int, default=1000)
    parser.add_argument("--n-perm", type=int, default=199)
    parser.add_argument("--alpha", type=float, default=0.05)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--outdir", required=True)
    parser.add_argument("--transform", choices=["none", "rclr"], default="none")
    parser.add_argument("--sample-col", default="sample")
    parser.add_argument("--patient-col", default="Participant_ID")
    parser.add_argument("--case-col", default="Case")
    parser.add_argument("--type-col", default="type_group")
    parser.add_argument("--exclude-contralateral-in-cancer", type=lambda x: str(x).lower()=="true", default=True)
    parser.add_argument("--contralateral-sample-types", default="Bronchial Brush,BAL")
    parser.add_argument("--scenarios", default="observed,null",
                       help="Comma-separated: observed, null, weak, moderate, strong")
    args = parser.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    print("Loading data...")
    wide_df = pd.read_csv(args.data_wide, sep='\t', index_col=0)
    long_df = pd.read_csv(args.data_long, sep='\t')
    sample_col = resolve_sample_col(long_df, args.sample_col)

    meta_cols = [sample_col, args.patient_col, args.case_col, args.type_col, 'lung_status', 'Cancer_Site', 'lung_code']
    meta_cols = [c for c in meta_cols if c in long_df.columns]
    metadata = long_df[meta_cols].drop_duplicates().set_index(sample_col)
    if args.exclude_contralateral_in_cancer:
        metadata = filter_contralateral_metadata(metadata.reset_index(),
                                                 case_col=args.case_col, type_col=args.type_col,
                                                 contralateral_sample_types=args.contralateral_sample_types,
                                                 contralateral_col='lung_status',
                                                 cancer_site_col='Cancer_Site',
                                                 lung_side_col='lung_code').set_index(sample_col)
    sample_ids = metadata.index.intersection(wide_df.columns)

    count_matrix = wide_df[sample_ids].T.values
    patient_ids = metadata.loc[sample_ids, args.patient_col].values
    case_status = metadata.loc[sample_ids, args.case_col].values
    sample_types = metadata.loc[sample_ids, args.type_col].values
    asv_names = wide_df.index.tolist()

    # Determine actual number of control patients from the data
    control_mask = (case_status == 'Control') | (case_status == 'Non-Cancer')
    actual_n_control = len(np.unique(patient_ids[control_mask]))
    print(f"Detected {actual_n_control} control patients in data")

    # Build scenario list based on user request
    spike_scenarios = None
    if args.spike_scenarios:
        with open(args.spike_scenarios, 'r') as f:
            spike_scenarios = json.load(f)

    requested_scenarios = [s.strip().lower() for s in args.scenarios.split(',')]
    scenarios = []

    if 'null' in requested_scenarios:
        scenarios.append({'name': 'True_Null', 'asvs': [], 'fold_change': 1.0, 'use_true_null': True})
    if 'observed' in requested_scenarios:
        scenarios.append({'name': 'Observed', 'asvs': [], 'fold_change': 1.0, 'use_true_null': False})
    if 'weak' in requested_scenarios:
        if not spike_scenarios:
            raise ValueError("--spike-scenarios required for 'weak' scenario")
        scenarios.append({'name': 'Weak', 'asvs': spike_scenarios['high_prev_high_abund'][:5], 'fold_change': 1.5, 'use_true_null': False})
    if 'moderate' in requested_scenarios:
        if not spike_scenarios:
            raise ValueError("--spike-scenarios required for 'moderate' scenario")
        scenarios.append({'name': 'Moderate', 'asvs': spike_scenarios['high_prev_high_abund'][:10], 'fold_change': 2.0, 'use_true_null': False})
    if 'strong' in requested_scenarios:
        if not spike_scenarios:
            raise ValueError("--spike-scenarios required for 'strong' scenario")
        scenarios.append({'name': 'Strong', 'asvs': spike_scenarios['high_prev_high_abund'][:10] + spike_scenarios['med_prev_med_abund'][:5], 'fold_change': 2.5, 'use_true_null': False})

    if not scenarios:
        raise ValueError(f"No valid scenarios found in: {args.scenarios}")

    cancer_sizes = [int(x) for x in args.sample_sizes.split(',')]

    all_results = []

    for sample_type in np.unique(sample_types):
        print(f"\n{'='*60}")
        print(f"Sample Type: {sample_type}")
        print(f"{'='*60}")

        st_counts, st_patients, st_case = filter_by_sample_type(
            count_matrix, patient_ids, case_status, sample_types, sample_type
        )

        print(f"Data: {st_counts.shape[0]} samples, {len(np.unique(st_patients))} patients")

        for scenario in scenarios:
            print(f"\nScenario: {scenario['name']} ({len(scenario['asvs'])} ASVs @ {scenario['fold_change']}×)")

            for n_cancer in cancer_sizes:
                # Hybrid logic: use actual_n_control if n_cancer <= actual_n_control, else balance
                if n_cancer <= actual_n_control:
                    n_control = actual_n_control
                else:
                    n_control = n_cancer

                print(f"  n_cancer={n_cancer}, n_control={n_control}...", end=' ', flush=True)

                power, mean_r2, r2_vals = run_power_simulation(
                    st_counts, st_patients, st_case, asv_names,
                    scenario['asvs'], scenario['fold_change'],
                    n_cancer, n_control,
                    args.n_simulations, args.n_perm, args.alpha, args.seed,
                    use_true_null=scenario.get('use_true_null', False),
                    transform=args.transform
                )

                print(f"Power={power:.3f}, R²={mean_r2:.4f}")

                all_results.append({
                    'Sample_type': sample_type,
                    'Scenario': scenario['name'],
                    'n_ASVs': len(scenario['asvs']),
                    'Fold_change': scenario['fold_change'],
                    'n_cancer': n_cancer,
                    'n_control': n_control,
                    'Power': power,
                    'Mean_R2': mean_r2,
                    'R2_CI_lower': np.percentile(r2_vals, 2.5),
                    'R2_CI_upper': np.percentile(r2_vals, 97.5)
                })

                # Early stopping: if power >= 0.995, skip remaining sample sizes
                if power >= 0.995:
                    print(f"  → Power ≥ 0.995 reached. Skipping larger sample sizes for this scenario.")
                    break

    results_df = pd.DataFrame(all_results)
    results_df.to_csv(outdir / 'permanova_power_stratified.tsv', sep='\t', index=False)

    print(f"\n{'='*60}")
    print(f"Results saved to {outdir}/permanova_power_stratified.tsv")
    print("\n✓ PERMANOVA power analysis complete!")


if __name__ == '__main__':
    main()

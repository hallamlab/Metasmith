#!/usr/bin/env python3
"""
power_taxonomic_abundance.py

Power analysis for taxonomic differential abundance (phylum and family level).
Tests Cancer vs Control within each sample type, with spike-in scenarios.
"""

import argparse
import json
import warnings
from pathlib import Path
import numpy as np
import pandas as pd
from scipy import stats
from scipy.stats import mannwhitneyu
from statsmodels.stats.multitest import multipletests

warnings.filterwarnings('ignore')


def resolve_sample_col(df, sample_col):
    if sample_col in df.columns:
        return sample_col
    alias = 'lmp_id' if sample_col == 'sample' else 'sample' if sample_col == 'lmp_id' else None
    if alias and alias in df.columns:
        print(f"[WARN] Sample column '{sample_col}' not found; using legacy alias '{alias}'.")
        return alias
    raise KeyError(f"Sample column '{sample_col}' not found in long-format data.")


def filter_contralateral_long_df(df, case_col='Case', type_col='type_group', contralateral_sample_types='Bronchial Brush,BAL',
                                 contralateral_col='lung_status', cancer_site_col='Cancer_Site',
                                 lung_side_col='lung_code', contralateral_value='Contralateral'):
    work = df.copy()
    contra = contralateral_col
    if contra not in work.columns and {cancer_site_col, lung_side_col}.issubset(work.columns):
        cancer_side = work[cancer_site_col].astype(str).str[:1].str.upper()
        lung_side = work[lung_side_col].astype(str).str[:1].str.upper()
        case_vals = work[case_col].astype(str)
        work['.derived_lung_status'] = np.where(
            case_vals.isin(['Control', 'Non-Cancer']),
            'Healthy',
            np.where(cancer_side == lung_side, 'TumorSide', 'Contralateral')
        )
        contra = '.derived_lung_status'
    if contra not in work.columns:
        return work
    target_types = {x.strip() for x in str(contralateral_sample_types).split(',') if x.strip()}
    is_cancer = ~work[case_col].astype(str).isin(['Control', 'Non-Cancer'])
    is_contra = work[contra].astype(str) == str(contralateral_value)
    in_target = work[type_col].astype(str).isin(target_types)
    return work.loc[~(is_cancer & is_contra & in_target)].copy()


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


def aggregate_to_taxonomy(long_df, tax_level='Phylum', min_prevalence=0.1, sample_col='sample'):
    """
    Aggregate ASV counts to taxonomic level.
    Returns: DataFrame with samples × taxa
    """
    # Group by sample and taxon
    agg = long_df.groupby([sample_col, tax_level])['count'].sum().reset_index()

    # Pivot to wide format
    wide = agg.pivot(index=sample_col, columns=tax_level, values='count').fillna(0)

    return wide


def patient_level_abundance(count_matrix, patient_ids):
    """
    Aggregate to patient level by averaging per-sample relative abundance.
    Returns patient × taxa matrix.
    """
    sample_rel = relative_abundance(count_matrix)
    unique_patients = np.unique(patient_ids)
    patient_matrix = []
    for patient in unique_patients:
        patient_mask = patient_ids == patient
        patient_matrix.append(sample_rel[patient_mask, :].mean(axis=0))
    return np.array(patient_matrix), unique_patients


def relative_abundance(count_matrix):
    """Convert counts to relative abundance (proportions)."""
    totals = count_matrix.sum(axis=1, keepdims=True)
    totals[totals == 0] = 1  # Avoid division by zero
    return count_matrix / totals


def case_group_patient_counts(patient_ids, case_status):
    """Return unique patient counts for the case and control analysis groups."""
    patient_ids = np.asarray(patient_ids)
    case_status = np.asarray(case_status).astype(str)
    cancer = np.unique(patient_ids[case_status == 'Cancer']).size
    control = np.unique(patient_ids[np.isin(case_status, ['Control', 'Non-Cancer'])]).size
    return int(cancer), int(control)


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


def bootstrap_patients_with_spike(count_matrix, patient_ids, case_status,
                                   n_cancer, n_control, spike_taxa_idx=None,
                                   spike_fc=1.0, seed=42, use_true_null=False):
    """
    Bootstrap patients with optional spike-in at taxonomic level.

    Parameters:
    - count_matrix: samples × taxa
    - patient_ids: sample-level patient IDs
    - case_status: sample-level case labels
    - spike_taxa_idx: list of taxon indices to spike
    - spike_fc: fold-change multiplier
    - use_true_null: if True, uses pool-then-label null
    """
    if use_true_null:
        # Use true null (no spike-in makes sense)
        return bootstrap_patients_true_null(count_matrix, patient_ids, case_status,
                                            n_cancer, n_control, seed)

    np.random.seed(seed)

    # Get unique patients in each group
    cancer_mask = (case_status == 'Cancer')
    control_mask = (case_status == 'Control') | (case_status == 'Non-Cancer')

    cancer_patients = np.unique(patient_ids[cancer_mask])
    control_patients = np.unique(patient_ids[control_mask])

    # Resample patients
    boot_cancer_patients = np.random.choice(cancer_patients, size=n_cancer, replace=True)
    boot_control_patients = np.random.choice(control_patients, size=n_control, replace=True)

    # Track draw counts for unique IDs
    patient_draw_count = {}

    boot_indices = []
    boot_patient_list = []
    boot_case_list = []

    # Process cancer patients
    for patient in boot_cancer_patients:
        draw_idx = patient_draw_count.get(patient, 0)
        patient_draw_count[patient] = draw_idx + 1

        unique_boot_id = f"{patient}__b{draw_idx}"

        patient_samples = np.where(patient_ids == patient)[0]
        boot_indices.extend(patient_samples)
        boot_patient_list.extend([unique_boot_id] * len(patient_samples))
        boot_case_list.extend(['Cancer'] * len(patient_samples))

    # Process control patients
    for patient in boot_control_patients:
        draw_idx = patient_draw_count.get(patient, 0)
        patient_draw_count[patient] = draw_idx + 1

        unique_boot_id = f"{patient}__b{draw_idx}"

        patient_samples = np.where(patient_ids == patient)[0]
        boot_indices.extend(patient_samples)
        boot_patient_list.extend([unique_boot_id] * len(patient_samples))
        boot_case_list.extend(['Control'] * len(patient_samples))

    boot_counts = count_matrix[boot_indices, :].copy()
    boot_patient_ids = np.array(boot_patient_list)
    boot_case = np.array(boot_case_list)

    # Apply spike-in if specified
    if spike_taxa_idx is not None and spike_fc > 1.0:
        cancer_boot_mask = boot_case == 'Cancer'

        for i in np.where(cancer_boot_mask)[0]:
            counts = boot_counts[i, :].astype(float)
            library_size = counts.sum()

            if library_size == 0:
                continue

            # Convert to relative abundance
            rel_abund = counts / library_size

            # Spike selected taxa
            rel_abund[spike_taxa_idx] *= spike_fc

            # Renormalize
            rel_abund = rel_abund / rel_abund.sum()

            # Resample counts (deterministic for integer reconciliation)
            new_counts = rel_abund * library_size

            # Integer reconciliation (largest remainder method)
            new_counts_int = np.floor(new_counts).astype(int)
            remainders = new_counts - new_counts_int
            deficit = int(library_size) - new_counts_int.sum()

            if deficit > 0:
                sorted_idx = np.argsort(-remainders)
                for j in range(min(deficit, len(sorted_idx))):
                    new_counts_int[sorted_idx[j]] += 1

            boot_counts[i, :] = new_counts_int

    return boot_counts, boot_patient_ids, boot_case


def run_power_simulation(count_matrix, patient_ids, case_status, taxa_names,
                         spike_scenario, n_cancer, n_control,
                         n_simulations=1000, alpha=0.05, seed=42, use_true_null=False,
                         transform='none'):
    """
    Run power simulation for taxonomic differential abundance.

    Parameters:
    - use_true_null: if True, uses pool-then-label null (Type I error calibration)

    Returns:
    - Power (proportion of simulations with at least one significant taxon at FDR < alpha)
    - Sensitivity (proportion of spiked taxa detected)
    - FDR (proportion of null taxa falsely detected)
    """
    spike_taxa_idx = spike_scenario.get('taxa_indices', None)
    spike_fc = spike_scenario.get('fold_change', 1.0)

    n_spiked = len(spike_taxa_idx) if spike_taxa_idx is not None else 0
    n_null = len(taxa_names) - n_spiked

    power_any = 0
    sensitivity_sum = 0
    fdr_sum = 0

    for i in range(n_simulations):
        # Bootstrap with spike-in
        boot_counts, boot_patients, boot_case = bootstrap_patients_with_spike(
            count_matrix, patient_ids, case_status,
            n_cancer, n_control,
            spike_taxa_idx=spike_taxa_idx,
            spike_fc=spike_fc,
            seed=seed+i,
            use_true_null=use_true_null
        )

        # Aggregate to patient level
        patient_counts, unique_patients = patient_level_abundance(boot_counts, boot_patients)

        # Map patients to case status
        patient_to_case = {}
        for patient in unique_patients:
            patient_mask = boot_patients == patient
            patient_to_case[patient] = boot_case[patient_mask][0]

        patient_case_labels = np.array([patient_to_case[p] for p in unique_patients])

        patient_rel_abund = apply_transform(patient_counts, transform)

        # T-tests for each taxon
        p_values = []
        for j in range(patient_rel_abund.shape[1]):
            cancer_vals = patient_rel_abund[patient_case_labels == 'Cancer', j]
            control_vals = patient_rel_abund[patient_case_labels == 'Control', j]

            if len(cancer_vals) >= 2 and len(control_vals) >= 2:
                _, p = mannwhitneyu(cancer_vals, control_vals, alternative='two-sided')
                p_values.append(p)
            else:
                p_values.append(1.0)

        p_values = np.array(p_values)

        # FDR correction
        reject, p_corrected, _, _ = multipletests(p_values, alpha=alpha, method='fdr_bh')

        # Count detections
        if np.any(reject):
            power_any += 1

        if n_spiked > 0:
            # Sensitivity: proportion of spiked taxa detected
            n_spiked_detected = np.sum(reject[spike_taxa_idx])
            sensitivity_sum += n_spiked_detected / n_spiked

            # FDR: proportion of null taxa falsely detected
            null_idx = [j for j in range(len(taxa_names)) if j not in spike_taxa_idx]
            if len(null_idx) > 0:
                n_null_detected = np.sum(reject[null_idx])
                fdr_sum += n_null_detected / len(null_idx) if len(null_idx) > 0 else 0

        if (i + 1) % 100 == 0:
            print(f"    {i+1}/{n_simulations}...", end='\r')

    power = power_any / n_simulations
    sensitivity = sensitivity_sum / n_simulations if n_spiked > 0 else 0
    fdr = fdr_sum / n_simulations

    return power, sensitivity, fdr


def main():
    parser = argparse.ArgumentParser(
        description="Taxonomic differential abundance power analysis"
    )
    parser.add_argument("--data-long", required=True, help="Long format data")
    parser.add_argument("--effect-sizes-dir", required=False, help="Directory with phylum/family effect sizes (required for spike scenarios)")
    parser.add_argument("--sample-sizes", default="5,8,10,15,20,25,30,40,50,60,70,80,90,100",
                       help="Comma-separated cancer patient sample sizes")
    parser.add_argument("--n-simulations", type=int, default=1000)
    parser.add_argument("--alpha", type=float, default=0.05)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--patient-col", default="Participant_ID")
    parser.add_argument("--case-col", default="Case")
    parser.add_argument("--type-col", default="type_group")
    parser.add_argument("--sample-col", default="sample")
    parser.add_argument("--outdir", required=True)
    parser.add_argument("--transform", choices=["none", "rclr"], default="none")
    parser.add_argument("--exclude-contralateral-in-cancer", type=lambda x: str(x).lower()=="true", default=True)
    parser.add_argument("--contralateral-sample-types", default="Bronchial Brush,BAL")
    parser.add_argument("--scenarios", default="observed,null",
                       help="Comma-separated: observed, null, weak, moderate, strong")
    args = parser.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    sample_sizes = [int(x) for x in args.sample_sizes.split(",")]

    print("="*60)
    print("Taxonomic Differential Abundance Power Analysis")
    print("="*60)
    print(f"Sample sizes (cancer): {sample_sizes}")
    print(f"Simulations: {args.n_simulations}")
    print()

    # Load data
    print("Loading data...")
    long_df = pd.read_csv(args.data_long, sep='\t')
    sample_col = resolve_sample_col(long_df, args.sample_col)
    if args.exclude_contralateral_in_cancer:
        long_df = filter_contralateral_long_df(long_df, case_col=args.case_col, type_col=args.type_col, contralateral_sample_types=args.contralateral_sample_types)

    # Determine actual number of control patients from the data
    control_mask = long_df[args.case_col].isin(['Control', 'Non-Cancer'])
    actual_n_control = long_df.loc[control_mask, args.patient_col].nunique()
    print(f"Detected {actual_n_control} control patients in data")

    # Parse requested scenarios
    requested_scenarios = [s.strip().lower() for s in args.scenarios.split(',')]
    needs_spikes = any(s in requested_scenarios for s in ['weak', 'moderate', 'strong'])

    # Load effect sizes if needed for spike scenarios
    phylum_effects = None
    family_effects = None
    if needs_spikes:
        if not args.effect_sizes_dir:
            raise ValueError("--effect-sizes-dir required for spike scenarios (weak, moderate, strong)")
        effect_sizes_dir = Path(args.effect_sizes_dir)
        phylum_effects = pd.read_csv(effect_sizes_dir / 'phylum_effect_sizes.tsv', sep='\t')
        family_effects = pd.read_csv(effect_sizes_dir / 'family_effect_sizes.tsv', sep='\t')
        print(f"Phylum effect sizes: {len(phylum_effects)} taxa")
        print(f"Family effect sizes: {len(family_effects)} taxa")
    print()

    # Process each taxonomic level and sample type
    all_results = []

    for tax_level, effects_df in [('Phylum', phylum_effects), ('Family', family_effects)]:
        print(f"\n{'='*60}")
        print(f"Processing {tax_level} level")
        print("="*60)

        # Get sample types
        sample_types = long_df[args.type_col].unique()

        for stype in sample_types:
            print(f"\n--- Sample Type: {stype} ---")

            # Filter data for this sample type
            stype_df = long_df[long_df[args.type_col] == stype].copy()

            # Aggregate to taxonomy
            tax_wide = aggregate_to_taxonomy(stype_df, tax_level=tax_level, min_prevalence=0.1, sample_col=sample_col)

            # Get metadata
            metadata = stype_df[[sample_col, args.patient_col, args.case_col]].drop_duplicates()
            metadata = metadata.set_index(sample_col)

            sample_ids = metadata.index.intersection(tax_wide.index)
            metadata = metadata.loc[sample_ids]
            tax_wide = tax_wide.loc[sample_ids]

            sample_rel = pd.DataFrame(
                relative_abundance(tax_wide.values.astype(float)),
                index=tax_wide.index,
                columns=tax_wide.columns,
            )
            patient_rel = sample_rel.join(metadata[[args.patient_col]]).groupby(args.patient_col).mean()
            prevalence = (patient_rel > 0).mean(axis=0)
            taxa_keep = prevalence[prevalence >= 0.1].index.tolist()
            tax_wide = tax_wide[taxa_keep]

            count_matrix = tax_wide.values.astype(float)
            taxa_names = tax_wide.columns.tolist()
            patient_ids = metadata[args.patient_col].values
            case_status = metadata[args.case_col].values

            print(f"  Samples: {count_matrix.shape[0]}, Taxa: {count_matrix.shape[1]}")
            print(f"  Patients: {len(np.unique(patient_ids))}")
            n_available_cancer, n_available_control = case_group_patient_counts(patient_ids, case_status)
            if n_available_cancer == 0 or n_available_control == 0:
                print(
                    "  [WARN] Skipping case-control analysis: "
                    f"available cancer patients={n_available_cancer}, "
                    f"control patients={n_available_control}."
                )
                continue

            # Build spike scenarios based on user request
            spike_scenarios = []
            spike_taxa_idx = []

            if needs_spikes:
                # Select top taxa by |Cohen's d|
                effects_df_abs = effects_df.copy()
                effects_df_abs['abs_cohens_d'] = effects_df_abs['Cohens_d'].abs()
                top_taxa = effects_df_abs.nlargest(5, 'abs_cohens_d')['Taxon'].tolist()
                spike_taxa_idx = [i for i, t in enumerate(taxa_names) if t in top_taxa]

            if 'null' in requested_scenarios:
                spike_scenarios.append({'name': 'True_Null', 'taxa_indices': None, 'fold_change': 1.0, 'use_true_null': True})
            if 'observed' in requested_scenarios:
                spike_scenarios.append({'name': 'Observed', 'taxa_indices': None, 'fold_change': 1.0, 'use_true_null': False})
            if 'weak' in requested_scenarios:
                spike_scenarios.append({'name': 'Weak', 'taxa_indices': spike_taxa_idx[:2], 'fold_change': 1.5, 'use_true_null': False})
            if 'moderate' in requested_scenarios:
                spike_scenarios.append({'name': 'Moderate', 'taxa_indices': spike_taxa_idx[:3], 'fold_change': 2.0, 'use_true_null': False})
            if 'strong' in requested_scenarios:
                spike_scenarios.append({'name': 'Strong', 'taxa_indices': spike_taxa_idx[:5], 'fold_change': 2.5, 'use_true_null': False})

            for scenario in spike_scenarios:
                print(f"\n  Scenario: {scenario['name']}")
                if scenario['taxa_indices']:
                    spiked_names = [taxa_names[i] for i in scenario['taxa_indices']]
                    print(f"    Spiking {len(spiked_names)} taxa @ {scenario['fold_change']}×")
                    print(f"    Taxa: {spiked_names[:3]}...")

                for n_cancer in sample_sizes:
                    # Hybrid logic: use actual_n_control if n_cancer <= actual_n_control, else balance
                    if n_cancer <= actual_n_control:
                        n_control = actual_n_control
                    else:
                        n_control = n_cancer

                    print(f"    n_cancer={n_cancer}, n_control={n_control}", end=' ')

                    power, sensitivity, fdr = run_power_simulation(
                        count_matrix, patient_ids, case_status, taxa_names,
                        scenario, n_cancer, n_control,
                        n_simulations=args.n_simulations,
                        alpha=args.alpha,
                        seed=args.seed,
                        use_true_null=scenario.get('use_true_null', False),
                        transform=args.transform
                    )

                    print(f"→ Power={power:.3f}, Sens={sensitivity:.3f}, FDR={fdr:.3f}")

                    all_results.append({
                        'tax_level': tax_level,
                        'sample_type': stype,
                        'scenario': scenario['name'],
                        'n_cancer': n_cancer,
                        'n_control': n_control,
                        'power': power,
                        'sensitivity': sensitivity,
                        'fdr': fdr,
                        'n_simulations': args.n_simulations
                    })

                    # Early stopping: if power >= 0.995, skip remaining sample sizes
                    if power >= 0.995:
                        print(f"    → Power ≥ 0.995 reached. Skipping larger sample sizes for this scenario.")
                        break

    # Save results
    results_df = pd.DataFrame(all_results)
    outfile = outdir / 'taxonomic_abundance_power.tsv'
    results_df.to_csv(outfile, sep='\t', index=False)

    print(f"\n{'='*60}")
    print(f"Results saved to: {outfile}")
    print("="*60)


if __name__ == '__main__':
    main()

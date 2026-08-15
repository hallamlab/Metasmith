#!/usr/bin/env python3
"""
power_taxonomic_sample_type.py

Power analysis for taxonomic abundance differences BETWEEN sample types.
Tests: "Is Phylum A more abundant in BAL vs Oral Rinse vs Bronchial Brush?"
"""

import argparse
import warnings
from pathlib import Path
import numpy as np
import pandas as pd
from scipy import stats
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
    """Aggregate ASV counts to taxonomic level."""
    agg = long_df.groupby([sample_col, tax_level])['count'].sum().reset_index()
    wide = agg.pivot(index=sample_col, columns=tax_level, values='count').fillna(0)

    return wide


def relative_abundance(count_matrix):
    """Convert counts to relative abundance."""
    totals = count_matrix.sum(axis=1, keepdims=True)
    totals[totals == 0] = 1
    return count_matrix / totals


def bootstrap_patients_sample_types(count_matrix, patient_ids, sample_types,
                                     n_patients, seed=42):
    """Bootstrap resample patients, keeping all their sample types."""
    np.random.seed(seed)

    unique_patients = np.unique(patient_ids)
    boot_patients_selected = np.random.choice(unique_patients, size=n_patients, replace=True)

    patient_draw_count = {}
    boot_indices = []
    boot_patient_list = []
    boot_sample_type_list = []

    for patient in boot_patients_selected:
        draw_idx = patient_draw_count.get(patient, 0)
        patient_draw_count[patient] = draw_idx + 1

        unique_boot_id = f"{patient}__b{draw_idx}"

        patient_samples = np.where(patient_ids == patient)[0]
        boot_indices.extend(patient_samples)
        boot_patient_list.extend([unique_boot_id] * len(patient_samples))
        boot_sample_type_list.extend(sample_types[patient_samples])

    boot_counts = count_matrix[boot_indices, :]
    boot_patient_ids = np.array(boot_patient_list)
    boot_sample_types = np.array(boot_sample_type_list)

    return boot_counts, boot_patient_ids, boot_sample_types


def patient_level_abundance_by_type(count_matrix, patient_ids, sample_types):
    """
    Aggregate to patient level WITHIN each sample type.
    Returns dict: {sample_type: {patient: abundance_vector}}
    """
    unique_stypes = np.unique(sample_types)
    patient_abundances = {}

    for stype in unique_stypes:
        stype_mask = sample_types == stype
        stype_counts = count_matrix[stype_mask, :]
        stype_patients = patient_ids[stype_mask]

        unique_patients = np.unique(stype_patients)
        patient_matrix = []
        patient_list = []

        for patient in unique_patients:
            patient_mask = stype_patients == patient
            patient_counts = relative_abundance(stype_counts[patient_mask, :]).mean(axis=0)
            patient_matrix.append(patient_counts)
            patient_list.append(patient)

        # Store as dict
        patient_matrix = np.array(patient_matrix)
        patient_abundances[stype] = {
            patient_list[i]: patient_matrix[i, :]
            for i in range(len(patient_list))
        }

    return patient_abundances, unique_stypes


def run_power_simulation(count_matrix, patient_ids, sample_types, taxa_names,
                         n_patients, n_simulations=1000, alpha=0.05, seed=42,
                         transform='none'):
    """
    Power to detect sample type differences in taxonomic abundance.

    For each taxon, test: Does abundance differ between sample types?
    Uses pairwise Wilcoxon tests with FDR correction.

    Returns power to detect at least one significant difference.
    """
    n_taxa = len(taxa_names)
    power_any = 0

    for i in range(n_simulations):
        # Bootstrap patients
        boot_counts, boot_patients, boot_stypes = bootstrap_patients_sample_types(
            count_matrix, patient_ids, sample_types,
            n_patients, seed=seed+i
        )

        # Get patient-level abundances by sample type
        patient_abund, sample_type_list = patient_level_abundance_by_type(
            boot_counts, boot_patients, boot_stypes
        )

        # For each taxon, test sample type differences
        all_p_values = []

        for taxon_idx in range(n_taxa):
            # Pairwise comparisons between sample types
            pairwise_p = []

            for stype1, stype2 in combinations(sample_type_list, 2):
                # Get patients who have both sample types (paired data)
                patients_both = set(patient_abund[stype1].keys()) & \
                               set(patient_abund[stype2].keys())

                if len(patients_both) >= 3:
                    vals1 = np.array([patient_abund[stype1][p][taxon_idx]
                                     for p in patients_both])
                    vals2 = np.array([patient_abund[stype2][p][taxon_idx]
                                     for p in patients_both])

                    try:
                        _, p = wilcoxon(vals1, vals2, zero_method='wilcox', correction=False, alternative='two-sided', mode='auto')
                    except ValueError:
                        p = 1.0
                    pairwise_p.append(p)

            if len(pairwise_p) > 0:
                # Take minimum p-value across pairwise tests for this taxon
                all_p_values.append(min(pairwise_p))

        # FDR correction across all taxa
        if len(all_p_values) > 0:
            reject, _, _, _ = multipletests(all_p_values, alpha=alpha, method='fdr_bh')
            if np.any(reject):
                power_any += 1

        if (i + 1) % 100 == 0:
            print(f"    {i+1}/{n_simulations}...", end='\r')

    print()
    power = power_any / n_simulations
    return power


def main():
    parser = argparse.ArgumentParser(
        description="Taxonomic abundance power - Sample Type Comparisons"
    )
    parser.add_argument("--data-long", required=True)
    parser.add_argument("--sample-sizes", default="10,15,20,25,30,40,50",
                       help="Comma-separated sample sizes (n patients)")
    parser.add_argument("--n-simulations", type=int, default=1000)
    parser.add_argument("--alpha", type=float, default=0.05)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--patient-col", default="Participant_ID")
    parser.add_argument("--type-col", default="type_group")
    parser.add_argument("--sample-col", default="sample")
    parser.add_argument("--outdir", required=True)
    parser.add_argument("--transform", choices=["none", "rclr"], default="none")
    args = parser.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    sample_sizes = [int(x) for x in args.sample_sizes.split(",")]

    print("="*60)
    print("Taxonomic Abundance - Sample Type Comparisons")
    print("="*60)
    print(f"Sample sizes: {sample_sizes}")
    print(f"Simulations: {args.n_simulations}")
    print()

    # Load data
    print("Loading data...")
    long_df = pd.read_csv(args.data_long, sep='\t')
    sample_col = resolve_sample_col(long_df, args.sample_col)
    long_df[sample_col] = normalize_sample_ids(long_df[sample_col])

    all_results = []

    for tax_level in ['Phylum', 'Family']:
        print(f"\n{'='*60}")
        print(f"Processing {tax_level} level")
        print("="*60)

        # Aggregate to taxonomy
        tax_wide = aggregate_to_taxonomy(long_df, tax_level=tax_level, min_prevalence=0.1, sample_col=sample_col)

        # Get metadata
        metadata = long_df[[sample_col, args.patient_col, args.type_col]].drop_duplicates()
        metadata = metadata.set_index(sample_col)

        sample_ids = metadata.index.intersection(tax_wide.index)
        if len(sample_ids) == 0:
            raise ValueError(
                f"No overlapping samples between long-format metadata and {tax_level} taxonomic table after sample-ID normalization."
            )
        metadata = metadata.loc[sample_ids]
        tax_wide = tax_wide.loc[sample_ids]

        sample_rel = pd.DataFrame(
            relative_abundance(tax_wide.values.astype(float)),
            index=tax_wide.index,
            columns=tax_wide.columns,
        )
        patient_rel_by_type = {}
        for stype in metadata[args.type_col].unique():
            st_samples = metadata.index[metadata[args.type_col] == stype]
            if len(st_samples) == 0:
                continue
            patient_rel_by_type[stype] = sample_rel.loc[st_samples].assign(
                _patient=metadata.loc[st_samples, args.patient_col].values
            ).groupby('_patient').mean()
        pooled = pd.concat(patient_rel_by_type.values(), axis=0) if patient_rel_by_type else pd.DataFrame()
        taxa_keep = ((pooled > 0).mean(axis=0) >= 0.1)
        tax_wide = tax_wide.loc[:, taxa_keep[taxa_keep].index]

        count_matrix = tax_wide.values.astype(float)
        taxa_names = tax_wide.columns.tolist()
        patient_ids = metadata[args.patient_col].values
        sample_types = metadata[args.type_col].values

        print(f"Samples: {count_matrix.shape[0]}, Taxa: {count_matrix.shape[1]}")
        print(f"Patients: {len(np.unique(patient_ids))}")
        print(f"Sample types: {list(np.unique(sample_types))}")

        for n in sample_sizes:
            print(f"\n  n={n} patients", end=' ')

            power = run_power_simulation(
                count_matrix, patient_ids, sample_types, taxa_names,
                n_patients=n,
                n_simulations=args.n_simulations,
                alpha=args.alpha,
                seed=args.seed,
                transform=args.transform
            )

            print(f"→ Power={power:.3f}")

            all_results.append({
                'tax_level': tax_level,
                'comparison': 'Sample_Type',
                'n_patients': n,
                'power': power,
                'n_simulations': args.n_simulations
            })

            # Early stopping: if power >= 0.995, skip remaining sample sizes
            if power >= 0.995:
                print(f"  → Power ≥ 0.995 reached. Skipping larger sample sizes for this level.")
                break

    # Save results
    results_df = pd.DataFrame(all_results)
    outfile = outdir / 'taxonomic_sample_type_power.tsv'
    results_df.to_csv(outfile, sep='\t', index=False)

    print(f"\n{'='*60}")
    print(f"Results saved to: {outfile}")
    print("="*60)


if __name__ == '__main__':
    main()

#!/usr/bin/env python3
"""
estimate_effects.py

Estimate effect sizes from observed data for power analysis.
Computes PERMANOVA R², Shannon Cohen's d summaries, taxonomic Cohen's d summaries
with Mann-Whitney p-values, and classifies ASVs by prevalence/abundance strata.

Outputs:
- Observed effect sizes with bootstrap 95% CI
- ASV stratification for spike-in scenarios
- Patient-level summary statistics
"""

import argparse
import warnings
from pathlib import Path
import numpy as np
import pandas as pd
from scipy.stats import mannwhitneyu
from scipy.spatial.distance import squareform, pdist
from sklearn.utils import resample
import json

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


def filter_contralateral_long_and_wide(long_df, wide_df, sample_col='sample', case_col='Case', type_col='type_group',
                                       contralateral_sample_types='Lung Brush,BAL', contralateral_col='lung_status',
                                       cancer_site_col='Cancer_Site', lung_side_col='lung_code', contralateral_value='Contralateral'):
    work = long_df.copy()
    work[sample_col] = normalize_sample_ids(work[sample_col])
    wide_df = wide_df.copy()
    wide_df.columns = normalize_sample_ids(wide_df.columns)
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
    if contra in work.columns:
        target_types = {x.strip() for x in str(contralateral_sample_types).split(',') if x.strip()}
        is_cancer = ~work[case_col].astype(str).isin(['Control', 'Non-Cancer'])
        is_contra = work[contra].astype(str) == str(contralateral_value)
        in_target = work[type_col].astype(str).isin(target_types)
        work = work.loc[~(is_cancer & is_contra & in_target)].copy()
    keep_samples = set(work[sample_col].astype(str).unique())
    keep_cols = [c for c in wide_df.columns if str(c) in keep_samples]
    return work, wide_df.loc[:, keep_cols].copy()

# ======================== Data Loading ========================

def load_data(long_path, wide_path):
    """Load long and wide format ASV data."""
    print(f"Loading long format data from {long_path}")
    long_df = pd.read_csv(long_path, sep='\t')

    print(f"Loading wide format data from {wide_path}")
    wide_df = pd.read_csv(wide_path, sep='\t')

    # Set ASV as index for wide format
    if 'ASV_ID' in wide_df.columns:
        wide_df = wide_df.set_index('ASV_ID')
    elif wide_df.columns[0].startswith('ASV'):
        wide_df = wide_df.set_index(wide_df.columns[0])

    wide_df.columns = normalize_sample_ids(wide_df.columns)

    print(f"Loaded {len(long_df)} rows (long), {wide_df.shape} (wide: ASVs × samples)")
    return long_df, wide_df


def get_sample_metadata(long_df, sample_col='sample', patient_col='Participant_ID',
                        case_col='Case', type_col='type_group'):
    """Extract sample-level metadata."""
    meta = long_df[[sample_col, patient_col, case_col, type_col]].copy()
    meta[sample_col] = normalize_sample_ids(meta[sample_col])
    meta = meta.drop_duplicates()
    meta = meta.set_index(sample_col)
    print(f"Extracted metadata for {len(meta)} samples from {meta[patient_col].nunique()} patients")
    return meta


# ======================== PERMANOVA Effect Size ========================

def apply_transform(count_matrix, transform='none'):
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
    data = apply_transform(count_matrix, transform=transform)
    metric = 'euclidean' if transform == 'rclr' else 'braycurtis'
    bc = pdist(data, metric=metric)
    return squareform(bc)


def mean_relative_by_group(count_matrix, group_ids):
    """Collapse to equal-weight group profiles by averaging per-sample relative abundance."""
    sample_rel = apply_transform(count_matrix, transform='none')
    unique_groups = np.unique(group_ids)
    group_matrix = np.zeros((len(unique_groups), sample_rel.shape[1]))

    for i, group in enumerate(unique_groups):
        mask = group_ids == group
        group_matrix[i, :] = sample_rel[mask, :].mean(axis=0)

    return group_matrix, unique_groups


def permanova_r2(dist_matrix, group_labels):
    """
    Compute PERMANOVA R² (proportion of variance explained).
    """
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


def bootstrap_permanova_r2(count_matrix, group_labels, patient_ids, n_bootstrap=1000, seed=42, transform='none'):
    """Bootstrap PERMANOVA R² at patient level."""
    np.random.seed(seed)
    dist_matrix = bray_curtis_from_counts(count_matrix, transform=transform)
    observed_r2 = permanova_r2(dist_matrix, group_labels)

    unique_patients = np.unique(patient_ids)
    r2_bootstrap = []

    print(f"  Running {n_bootstrap} bootstrap replicates...")
    for i in range(n_bootstrap):
        boot_patients = resample(unique_patients, replace=True, random_state=seed+i)
        boot_indices = []
        boot_groups = []
        for patient in boot_patients:
            patient_mask = patient_ids == patient
            patient_samples = np.where(patient_mask)[0]
            boot_indices.extend(patient_samples)
            boot_groups.extend(group_labels[patient_samples])

        boot_count_matrix = count_matrix[boot_indices, :]
        boot_group_labels = np.array(boot_groups)
        patient_matrix, boot_unique_patients = mean_relative_by_group(boot_count_matrix, np.array(boot_patients))
        patient_group_map = {p: boot_group_labels[np.where(np.array(boot_patients) == p)[0][0]] for p in boot_unique_patients}
        patient_groups = np.array([patient_group_map[p] for p in boot_unique_patients])
        boot_dist = bray_curtis_from_counts(patient_matrix, transform=transform)
        boot_r2 = permanova_r2(boot_dist, patient_groups)
        r2_bootstrap.append(boot_r2)

    r2_bootstrap = np.array(r2_bootstrap)
    return (observed_r2, np.percentile(r2_bootstrap, 2.5), np.percentile(r2_bootstrap, 97.5),
            np.percentile(r2_bootstrap, 25), np.percentile(r2_bootstrap, 75))


def jackknife_permanova_r2_patient_level(count_matrix, group_labels, patient_ids, transform='none'):
    """
    Leave-one-patient-out (LOPO) jackknife for PERMANOVA R².

    More stable than bootstrap with small n, avoids duplicate-patient artifacts.
    """
    # First aggregate to patient level
    patient_counts, unique_patients = aggregate_counts_to_patient_level(
        count_matrix, patient_ids
    )

    # Map patients to groups
    patient_to_group = {}
    for patient in unique_patients:
        patient_mask = patient_ids == patient
        patient_to_group[patient] = group_labels[patient_mask][0]

    patient_groups = np.array([patient_to_group[p] for p in unique_patients])

    # Observed R² on full patient-level data
    dist_matrix_full = bray_curtis_from_counts(patient_counts, transform=transform)
    observed_r2 = permanova_r2(dist_matrix_full, patient_groups)

    # Jackknife: leave one patient out at a time
    r2_jackknife = []
    for i in range(len(unique_patients)):
        jack_mask = np.ones(len(unique_patients), dtype=bool)
        jack_mask[i] = False

        jack_counts = patient_counts[jack_mask, :]
        jack_groups = patient_groups[jack_mask]

        jack_dist = bray_curtis_from_counts(jack_counts, transform=transform)
        jack_r2 = permanova_r2(jack_dist, jack_groups)
        r2_jackknife.append(jack_r2)

    r2_jackknife = np.array(r2_jackknife)

    # Jackknife statistics
    r2_mean = np.mean(r2_jackknife)
    r2_se = np.sqrt((len(r2_jackknife) - 1) * np.var(r2_jackknife, ddof=0))

    # Approximate CI using jackknife SE
    r2_ci_low = observed_r2 - 1.96 * r2_se
    r2_ci_high = observed_r2 + 1.96 * r2_se

    # Percentiles
    r2_p25 = np.percentile(r2_jackknife, 25)
    r2_p75 = np.percentile(r2_jackknife, 75)

    return (observed_r2, r2_ci_low, r2_ci_high, r2_p25, r2_p75)


def aggregate_counts_to_patient_level(count_matrix, patient_ids):
    """
    Aggregate sample-level data to patient level by mean relative abundance.

    Makes "patient is the experimental unit" explicit in distance calculations.
    """
    unique_patients = np.unique(patient_ids)
    n_asvs = count_matrix.shape[1]
    return mean_relative_by_group(count_matrix, patient_ids)


def aggregate_counts_to_patient_sample_type_level(count_matrix, patient_ids, sample_types):
    """Aggregate to one equal-weight profile per patient × sample type."""
    keys = np.array([f"{patient}||{stype}" for patient, stype in zip(patient_ids, sample_types)])
    group_matrix, unique_keys = mean_relative_by_group(count_matrix, keys)
    agg_patients = np.array([key.split("||", 1)[0] for key in unique_keys])
    agg_types = np.array([key.split("||", 1)[1] for key in unique_keys])
    return group_matrix, agg_patients, agg_types


# ======================== Shannon Diversity ========================

def shannon_diversity(counts):
    """Compute Shannon diversity H = -sum(p * log(p))."""
    counts = np.array(counts)
    counts = counts[counts > 0]
    if len(counts) == 0:
        return 0
    proportions = counts / counts.sum()
    return -np.sum(proportions * np.log(proportions))


def patient_level_shannon(count_matrix, patient_ids):
    """Compute patient-level mean Shannon diversity."""
    patient_shannon = {}
    for patient in np.unique(patient_ids):
        patient_mask = patient_ids == patient
        patient_samples = count_matrix[patient_mask, :]
        shannon_values = [shannon_diversity(sample) for sample in patient_samples]
        patient_shannon[patient] = np.mean(shannon_values)
    return patient_shannon


def cohens_d(group1, group2):
    """Compute Cohen's d effect size."""
    n1, n2 = len(group1), len(group2)
    var1, var2 = np.var(group1, ddof=1), np.var(group2, ddof=1)
    pooled_std = np.sqrt(((n1-1)*var1 + (n2-1)*var2) / (n1+n2-2))
    if pooled_std == 0:
        return 0
    return (np.mean(group1) - np.mean(group2)) / pooled_std


def bootstrap_cohens_d(group1_values, group2_values, n_bootstrap=1000, seed=42):
    """Bootstrap Cohen's d with patient-level resampling."""
    np.random.seed(seed)
    observed_d = cohens_d(group1_values, group2_values)

    d_bootstrap = []
    for i in range(n_bootstrap):
        boot_g1 = resample(group1_values, replace=True, random_state=seed+i)
        boot_g2 = resample(group2_values, replace=True, random_state=seed+i)
        d_bootstrap.append(cohens_d(boot_g1, boot_g2))

    d_bootstrap = np.array(d_bootstrap)
    return (observed_d, np.percentile(d_bootstrap, 2.5), np.percentile(d_bootstrap, 97.5),
            np.percentile(d_bootstrap, 25), np.percentile(d_bootstrap, 75))


# ======================== ASV Stratification ========================

def classify_asvs(wide_df, prevalence_thresholds=(0.2, 0.5)):
    """Classify ASVs by prevalence and abundance."""
    asv_stats = []
    for asv_id in wide_df.index:
        counts = wide_df.loc[asv_id].values
        prevalence = (counts > 0).mean()
        total_counts = wide_df.sum(axis=0)
        rel_abund = counts / total_counts.values
        mean_rel_abund = rel_abund.mean()
        asv_stats.append({'ASV_ID': asv_id, 'prevalence': prevalence, 'mean_rel_abundance': mean_rel_abund})

    asv_df = pd.DataFrame(asv_stats)
    prev_low, prev_high = prevalence_thresholds
    asv_df['prev_class'] = pd.cut(asv_df['prevalence'], bins=[0, prev_low, prev_high, 1.0],
                                   labels=['Low', 'Medium', 'High'], include_lowest=True)

    abund_p_low = np.percentile(asv_df['mean_rel_abundance'], 33.33)
    abund_p_high = np.percentile(asv_df['mean_rel_abundance'], 66.67)
    asv_df['abund_class'] = pd.cut(asv_df['mean_rel_abundance'],
                                    bins=[0, abund_p_low, abund_p_high, np.inf],
                                    labels=['Low', 'Medium', 'High'], include_lowest=True)

    asv_df['stratum'] = asv_df['prev_class'].astype(str) + '_prev_' + asv_df['abund_class'].astype(str) + '_abund'
    return asv_df


# ======================== Taxonomic Abundance Effect Sizes ========================

def calculate_taxonomic_abundances(long_df, tax_level, sample_col='sample', count_col='count', transform='none'):
    """
    Calculate relative abundances at a given taxonomic level.
    Returns a DataFrame with samples as rows and taxa as columns.
    """
    # Aggregate counts by sample and taxonomic level
    agg = long_df.groupby([sample_col, tax_level])[count_col].sum().reset_index()

    # Pivot to wide format (samples × taxa)
    wide = agg.pivot(index=sample_col, columns=tax_level, values=count_col).fillna(0)

    if transform == "rclr":
        arr = wide.values.astype(float)
        out = np.zeros_like(arr, dtype=float)
        for i in range(arr.shape[0]):
            row = arr[i, :]
            pos = row > 0
            if np.any(pos):
                lv = np.log(row[pos])
                out[i, pos] = lv - lv.mean()
        return pd.DataFrame(out, index=wide.index, columns=wide.columns)

    rel_abund = wide.div(wide.sum(axis=1), axis=0)
    return rel_abund


def taxonomic_effect_sizes(long_df, metadata, tax_level='Phylum',
                          case_col='Case', patient_col='Participant_ID', sample_col='sample',
                          n_bootstrap=1000, seed=42, min_prevalence=0.1, transform='none'):
    """
    Compute effect sizes (Cohen's d) for differential abundance at a taxonomic level.
    Uses patient-level aggregation and bootstrap for confidence intervals.
    """
    # Get relative abundances
    rel_abund = calculate_taxonomic_abundances(long_df, tax_level, sample_col=sample_col, transform=transform)

    # Filter to samples in metadata
    sample_ids = metadata.index.intersection(rel_abund.index)
    rel_abund = rel_abund.loc[sample_ids]
    metadata_filt = metadata.loc[sample_ids]

    # Filter taxa by prevalence
    prevalence = (rel_abund > 0).mean(axis=0)
    taxa_keep = prevalence[prevalence >= min_prevalence].index
    rel_abund = rel_abund[taxa_keep]

    print(f"  {tax_level}: {len(taxa_keep)} taxa with prevalence ≥ {min_prevalence:.0%}")

    # Patient-level aggregation (mean across samples per patient)
    patient_abund = {}
    for patient in metadata_filt[patient_col].unique():
        patient_samples = metadata_filt[metadata_filt[patient_col] == patient].index
        patient_abund[patient] = rel_abund.loc[patient_samples].mean(axis=0)

    patient_df = pd.DataFrame(patient_abund).T
    patient_case = metadata_filt.groupby(patient_col)[case_col].first()

    # Separate cancer vs control
    cancer_patients = patient_case[patient_case == 'Cancer'].index
    control_patients = patient_case[(patient_case == 'Control') | (patient_case == 'Non-Cancer')].index

    # Calculate effect sizes for each taxon
    results = []
    for taxon in patient_df.columns:
        cancer_vals = patient_df.loc[cancer_patients, taxon].values
        control_vals = patient_df.loc[control_patients, taxon].values

        # Skip if not enough samples
        if len(cancer_vals) < 2 or len(control_vals) < 2:
            continue

        # Observed Cohen's d
        d_obs = cohens_d(cancer_vals, control_vals)

        # Bootstrap CI
        np.random.seed(seed)
        d_boot = []
        for i in range(n_bootstrap):
            boot_cancer = resample(cancer_vals, replace=True, random_state=seed+i)
            boot_control = resample(control_vals, replace=True, random_state=seed+i)
            d_boot.append(cohens_d(boot_cancer, boot_control))

        d_boot = np.array(d_boot)

        _, pval = mannwhitneyu(cancer_vals, control_vals, alternative='two-sided')

        results.append({
            'Taxon': taxon,
            'Cohens_d': d_obs,
            'CI_lower_95': np.percentile(d_boot, 2.5),
            'CI_upper_95': np.percentile(d_boot, 97.5),
            'p_value': pval,
            'Cancer_mean': cancer_vals.mean(),
            'Control_mean': control_vals.mean(),
            'Cancer_n': len(cancer_patients),
            'Control_n': len(control_patients)
        })

    return pd.DataFrame(results).sort_values('Cohens_d', key=abs, ascending=False)


# ======================== Main ========================

def main():
    parser = argparse.ArgumentParser(description="Estimate effect sizes from observed microbiome data")
    parser.add_argument("--data-long", required=True, help="Long format ASV data (TSV)")
    parser.add_argument("--data-wide", required=True, help="Wide format ASV count matrix (TSV)")
    parser.add_argument("--patient-col", default="Participant_ID")
    parser.add_argument("--sample-col", default="sample")
    parser.add_argument("--case-col", default="Case")
    parser.add_argument("--type-col", default="type_group")
    parser.add_argument("--n-bootstrap", type=int, default=1000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--outdir", required=True)
    parser.add_argument("--transform", choices=["none", "rclr"], default="none")
    parser.add_argument("--exclude-contralateral-in-cancer", type=lambda x: str(x).lower()=="true", default=True)
    parser.add_argument("--contralateral-sample-types", default="Lung Brush,BAL")
    args = parser.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    long_df, wide_df = load_data(args.data_long, args.data_wide)
    sample_col = resolve_sample_col(long_df, args.sample_col)
    long_df[sample_col] = normalize_sample_ids(long_df[sample_col])
    if args.exclude_contralateral_in_cancer:
        long_df, wide_df = filter_contralateral_long_and_wide(
            long_df, wide_df, sample_col=sample_col, case_col=args.case_col, type_col=args.type_col,
            contralateral_sample_types=args.contralateral_sample_types
        )
    metadata = get_sample_metadata(long_df, sample_col, args.patient_col, args.case_col, args.type_col)

    sample_ids = metadata.index.intersection(wide_df.columns)
    print(f"\n{len(sample_ids)} samples in both metadata and count matrix")

    if len(sample_ids) == 0:
        meta_preview = metadata.index.astype(str).tolist()[:5]
        wide_preview = [str(c) for c in wide_df.columns.tolist()[:5]]
        raise ValueError(
            "No overlapping sample IDs between long-format metadata and wide count matrix "
            f"after normalization. Metadata examples: {meta_preview}; wide examples: {wide_preview}"
        )

    metadata = metadata.loc[sample_ids]
    wide_df_filtered = wide_df[sample_ids].T

    count_matrix = wide_df_filtered.values.astype(float)
    patient_ids = metadata[args.patient_col].values
    case_status = metadata[args.case_col].values
    sample_types = metadata[args.type_col].values

    print(f"\nData: {count_matrix.shape[0]} samples, {count_matrix.shape[1]} ASVs")
    print(f"Patients: {len(np.unique(patient_ids))}")
    print(f"Case: {dict(zip(*np.unique(case_status, return_counts=True)))}")
    print(f"Sample types: {dict(zip(*np.unique(sample_types, return_counts=True)))}")

    # PERMANOVA - STRATIFIED BY SAMPLE TYPE
    print("\n" + "="*60)
    print("PERMANOVA R² (Cancer vs Control) - Stratified by Sample Type")

    permanova_results = {}
    sample_type_list = np.unique(sample_types)

    for stype in sample_type_list:
        print(f"\n--- {stype} ---")
        stype_mask = sample_types == stype
        cancer_mask = (case_status == 'Cancer') & stype_mask
        control_mask = ((case_status == 'Control') | (case_status == 'Non-Cancer')) & stype_mask
        cc_mask = cancer_mask | control_mask

        cc_count = count_matrix[cc_mask, :]
        cc_groups = case_status[cc_mask]
        cc_patients = patient_ids[cc_mask]

        n_cancer_pts = len(np.unique(cc_patients[cc_groups == 'Cancer']))
        n_control_pts = len(np.unique(cc_patients[cc_groups != 'Cancer']))

        print(f"  Samples: {cc_count.shape[0]} ({cancer_mask.sum()} cancer, {control_mask.sum()} control)")
        print(f"  Patients: {n_cancer_pts} cancer, {n_control_pts} control")

        if n_cancer_pts >= 2 and n_control_pts >= 2:
            print(f"  Computing patient-level jackknife (LOPO)...")
            r2_obs, r2_ci_low, r2_ci_high, r2_p25, r2_p75 = jackknife_permanova_r2_patient_level(
                cc_count, cc_groups, cc_patients, transform=args.transform)

            print(f"  R² = {r2_obs:.4f} [95% CI: {r2_ci_low:.4f}-{r2_ci_high:.4f}]")
            print(f"  25th/75th: {r2_p25:.4f}/{r2_p75:.4f}")

            permanova_results[stype] = {
                'comparison': 'Cancer_vs_Control',
                'sample_type': stype,
                'n_samples': int(cc_count.shape[0]),
                'n_cancer_patients': int(n_cancer_pts),
                'n_control_patients': int(n_control_pts),
                'observed': float(r2_obs),
                'ci_lower_95': float(r2_ci_low),
                'ci_upper_95': float(r2_ci_high),
                'percentile_25': float(r2_p25),
                'percentile_75': float(r2_p75)
            }
        else:
            print(f"  Skipped (insufficient patients)")

    # Also compute pooled (for comparison, but note this is problematic)
    print(f"\n--- ALL SAMPLE TYPES POOLED (for reference only) ---")
    cancer_mask = case_status == 'Cancer'
    control_mask = (case_status == 'Control') | (case_status == 'Non-Cancer')
    cc_mask = cancer_mask | control_mask
    cc_count = count_matrix[cc_mask, :]
    cc_groups = case_status[cc_mask]
    cc_patients = patient_ids[cc_mask]

    print(f"  Computing patient-level jackknife (LOPO)...")
    r2_obs, r2_ci_low, r2_ci_high, r2_p25, r2_p75 = jackknife_permanova_r2_patient_level(
        cc_count, cc_groups, cc_patients, transform=args.transform)

    print(f"  R² = {r2_obs:.4f} [95% CI: {r2_ci_low:.4f}-{r2_ci_high:.4f}]")
    print(f"  25th/75th: {r2_p25:.4f}/{r2_p75:.4f}")
    print("  WARNING: This pools across sample types and may conflate effects!")

    # Shannon Diversity - STRATIFIED BY SAMPLE TYPE
    print("\n" + "="*60)
    print("Shannon Diversity (Cancer vs Control) - Stratified by Sample Type")

    shannon_results = {}

    for stype in sample_type_list:
        print(f"\n--- {stype} ---")
        stype_mask = sample_types == stype
        cancer_mask_st = (case_status == 'Cancer') & stype_mask
        control_mask_st = ((case_status == 'Control') | (case_status == 'Non-Cancer')) & stype_mask
        cc_mask_st = cancer_mask_st | control_mask_st

        cc_count_st = count_matrix[cc_mask_st, :]
        cc_patients_st = patient_ids[cc_mask_st]
        cc_case_st = case_status[cc_mask_st]

        # Get patient-level Shannon
        patient_shannon_st = patient_level_shannon(cc_count_st, cc_patients_st)

        # Map patients to case status
        patient_case_map_st = {}
        for i, patient in enumerate(cc_patients_st):
            patient_case_map_st[patient] = cc_case_st[i]

        cancer_patients_st = [p for p, c in patient_case_map_st.items() if c == 'Cancer']
        control_patients_st = [p for p, c in patient_case_map_st.items() if c in ['Control', 'Non-Cancer']]

        # Remove duplicates (same patient may have multiple samples)
        cancer_patients_st = list(set(cancer_patients_st))
        control_patients_st = list(set(control_patients_st))

        cancer_shannon_st = np.array([patient_shannon_st[p] for p in cancer_patients_st if p in patient_shannon_st])
        control_shannon_st = np.array([patient_shannon_st[p] for p in control_patients_st if p in patient_shannon_st])

        if len(cancer_shannon_st) >= 2 and len(control_shannon_st) >= 2:
            print(f"  Cancer (n={len(cancer_shannon_st)}): {cancer_shannon_st.mean():.3f}±{cancer_shannon_st.std():.3f}")
            print(f"  Control (n={len(control_shannon_st)}): {control_shannon_st.mean():.3f}±{control_shannon_st.std():.3f}")

            d_obs_st, d_ci_low_st, d_ci_high_st, d_p25_st, d_p75_st = bootstrap_cohens_d(
                cancer_shannon_st, control_shannon_st, args.n_bootstrap, args.seed)

            print(f"  Cohen's d = {d_obs_st:.4f} [95% CI: {d_ci_low_st:.4f}-{d_ci_high_st:.4f}]")
            print(f"  25th/75th: {d_p25_st:.4f}/{d_p75_st:.4f}")

            shannon_results[stype] = {
                'comparison': 'Cancer_vs_Control',
                'sample_type': stype,
                'n_cancer_patients': int(len(cancer_shannon_st)),
                'n_control_patients': int(len(control_shannon_st)),
                'observed': float(d_obs_st),
                'ci_lower_95': float(d_ci_low_st),
                'ci_upper_95': float(d_ci_high_st),
                'percentile_25': float(d_p25_st),
                'percentile_75': float(d_p75_st)
            }
        else:
            print(f"  Skipped (insufficient patients)")

    # Pooled analysis for reference
    print(f"\n--- ALL SAMPLE TYPES POOLED (for reference only) ---")
    patient_shannon = patient_level_shannon(cc_count, cc_patients)
    patient_case_map = metadata.loc[metadata.index.isin(sample_ids[cc_mask])].groupby(args.patient_col)[args.case_col].first()

    cancer_patients = patient_case_map[patient_case_map == 'Cancer'].index
    control_patients = patient_case_map[(patient_case_map == 'Control') | (patient_case_map == 'Non-Cancer')].index

    cancer_shannon = np.array([patient_shannon[p] for p in cancer_patients if p in patient_shannon])
    control_shannon = np.array([patient_shannon[p] for p in control_patients if p in patient_shannon])

    print(f"  Cancer (n={len(cancer_shannon)}): {cancer_shannon.mean():.3f}±{cancer_shannon.std():.3f}")
    print(f"  Control (n={len(control_shannon)}): {control_shannon.mean():.3f}±{control_shannon.std():.3f}")

    d_obs, d_ci_low, d_ci_high, d_p25, d_p75 = bootstrap_cohens_d(
        cancer_shannon, control_shannon, args.n_bootstrap, args.seed)

    print(f"  Cohen's d = {d_obs:.4f} [95% CI: {d_ci_low:.4f}-{d_ci_high:.4f}]")
    print(f"  25th/75th: {d_p25:.4f}/{d_p75:.4f}")
    print("  WARNING: This pools across sample types and may conflate effects!")

    # ASV stratification
    print("\n" + "="*60)
    print("ASV Stratification")
    asv_strata = classify_asvs(wide_df.loc[:, sample_ids])
    print("\nCounts by stratum:")
    print(asv_strata.groupby('stratum').size().sort_values(ascending=False))

    spike_scenarios = {}
    high_high = asv_strata[(asv_strata['prev_class'] == 'High') & (asv_strata['abund_class'] == 'High')].sort_values('mean_rel_abundance', ascending=False)
    spike_scenarios['high_prev_high_abund'] = high_high.head(10)['ASV_ID'].tolist()

    med_med = asv_strata[(asv_strata['prev_class'] == 'Medium') & (asv_strata['abund_class'] == 'Medium')].sort_values('mean_rel_abundance', ascending=False)
    spike_scenarios['med_prev_med_abund'] = med_med.head(10)['ASV_ID'].tolist()

    low_prev = asv_strata[asv_strata['prev_class'] == 'Low'].sort_values('mean_rel_abundance', ascending=False)
    spike_scenarios['low_prev'] = low_prev.head(10)['ASV_ID'].tolist()

    print(f"Selected ASVs: High-prev/High-abund={len(spike_scenarios['high_prev_high_abund'])}, Med/Med={len(spike_scenarios['med_prev_med_abund'])}, Low-prev={len(spike_scenarios['low_prev'])}")

    # Taxonomic abundance effect sizes
    print("\n" + "="*60)
    print("Taxonomic Abundance Effect Sizes (Cancer vs Control)")

    phylum_effects = taxonomic_effect_sizes(
        long_df, metadata, tax_level='Phylum',
        case_col=args.case_col, patient_col=args.patient_col, sample_col=sample_col,
        n_bootstrap=args.n_bootstrap, seed=args.seed, transform=args.transform
    )

    family_effects = taxonomic_effect_sizes(
        long_df, metadata, tax_level='Family',
        case_col=args.case_col, patient_col=args.patient_col, sample_col=sample_col,
        n_bootstrap=args.n_bootstrap, seed=args.seed, transform=args.transform
    )

    print("\n--- Top 5 Phyla by |Cohen's d| ---")
    print(phylum_effects.head(5)[['Taxon', 'Cohens_d', 'CI_lower_95', 'CI_upper_95', 'p_value']].to_string(index=False))

    print("\n--- Top 5 Families by |Cohen's d| ---")
    print(family_effects.head(5)[['Taxon', 'Cohens_d', 'CI_lower_95', 'CI_upper_95', 'p_value']].to_string(index=False))

    # Sample Type Comparisons
    print("\n" + "="*60)
    print("Sample Type Comparisons (Oral vs BAL vs Bronchial Brush)")

    # PERMANOVA R² for sample type differences
    print("\n--- PERMANOVA R² (Sample Type Effect) ---")
    print(f"  Computing patient × sample_type-level jackknife (LOPO)...")
    patient_type_counts, patient_type_patients, patient_type_types = aggregate_counts_to_patient_sample_type_level(
        count_matrix, patient_ids, sample_types
    )
    r2_stype_obs, r2_stype_ci_low, r2_stype_ci_high, r2_stype_p25, r2_stype_p75 = jackknife_permanova_r2_patient_level(
        patient_type_counts, patient_type_types, patient_type_patients, transform=args.transform)

    print(f"  R² = {r2_stype_obs:.4f} [95% CI: {r2_stype_ci_low:.4f}-{r2_stype_ci_high:.4f}]")
    print(f"  25th/75th: {r2_stype_p25:.4f}/{r2_stype_p75:.4f}")

    sample_type_permanova_result = {
        'comparison': 'Sample_Type_Effect',
        'n_patient_type_profiles': int(patient_type_counts.shape[0]),
        'n_patients': int(len(np.unique(patient_type_patients))),
        'sample_types': list(np.unique(sample_types)),
        'observed': float(r2_stype_obs),
        'ci_lower_95': float(r2_stype_ci_low),
        'ci_upper_95': float(r2_stype_ci_high),
        'percentile_25': float(r2_stype_p25),
        'percentile_75': float(r2_stype_p75)
    }

    # Shannon diversity by sample type
    print("\n--- Shannon Diversity (Sample Type Comparison) ---")
    # Compute patient-level Shannon for each sample type
    patient_shannon_by_type = {}
    for stype in sample_type_list:
        stype_mask = sample_types == stype
        stype_counts = count_matrix[stype_mask, :]
        stype_patients = patient_ids[stype_mask]
        patient_shannon_by_type[stype] = patient_level_shannon(stype_counts, stype_patients)

    # For patients with multiple sample types, we have paired data
    # Report effect sizes for pairwise comparisons
    from itertools import combinations
    sample_type_shannon_results = {}

    for stype1, stype2 in combinations(sample_type_list, 2):
        # Get patients who have both sample types
        patients_both = set(patient_shannon_by_type[stype1].keys()) & set(patient_shannon_by_type[stype2].keys())

        if len(patients_both) >= 3:
            shannon1 = np.array([patient_shannon_by_type[stype1][p] for p in patients_both])
            shannon2 = np.array([patient_shannon_by_type[stype2][p] for p in patients_both])

            print(f"\n  {stype1} vs {stype2} (n={len(patients_both)} paired patients)")
            print(f"    {stype1}: {shannon1.mean():.3f}±{shannon1.std():.3f}")
            print(f"    {stype2}: {shannon2.mean():.3f}±{shannon2.std():.3f}")

            # Cohen's d for paired comparison (using differences)
            d_obs, d_ci_low, d_ci_high, d_p25, d_p75 = bootstrap_cohens_d(
                shannon1, shannon2, args.n_bootstrap, args.seed)

            print(f"    Cohen's d = {d_obs:.4f} [95% CI: {d_ci_low:.4f}-{d_ci_high:.4f}]")

            sample_type_shannon_results[f"{stype1}_vs_{stype2}"] = {
                'comparison': f'{stype1}_vs_{stype2}',
                'n_paired_patients': int(len(patients_both)),
                'mean_diff': float(shannon1.mean() - shannon2.mean()),
                'observed': float(d_obs),
                'ci_lower_95': float(d_ci_low),
                'ci_upper_95': float(d_ci_high),
                'percentile_25': float(d_p25),
                'percentile_75': float(d_p75)
            }

    # Save
    print("\n" + "="*60)
    print("Saving results")

    # Save stratified effect sizes (cancer vs control within sample types)
    effect_sizes_stratified = {
        'PERMANOVA_R2_by_sample_type': permanova_results,
        'Shannon_Cohens_d_by_sample_type': shannon_results
    }

    with open(outdir / 'observed_effect_sizes_stratified.json', 'w') as f:
        json.dump(effect_sizes_stratified, f, indent=2)

    # Save sample type comparison effect sizes
    effect_sizes_sample_type = {
        'PERMANOVA_R2_sample_type': sample_type_permanova_result,
        'Shannon_pairwise_comparisons': sample_type_shannon_results
    }

    with open(outdir / 'observed_effect_sizes_sample_type_comparisons.json', 'w') as f:
        json.dump(effect_sizes_sample_type, f, indent=2)

    # Save pooled (for reference)
    effect_sizes_pooled = {
        'PERMANOVA_R2': {'comparison': 'Cancer_vs_Control_pooled', 'observed': float(r2_obs),
                        'ci_lower_95': float(r2_ci_low), 'ci_upper_95': float(r2_ci_high),
                        'percentile_25': float(r2_p25), 'percentile_75': float(r2_p75),
                        'note': 'Pooled across sample types - may conflate effects'},
        'Shannon_Cohens_d': {'comparison': 'Cancer_vs_Control_pooled', 'observed': float(d_obs),
                            'ci_lower_95': float(d_ci_low), 'ci_upper_95': float(d_ci_high),
                            'percentile_25': float(d_p25), 'percentile_75': float(d_p75),
                            'note': 'Pooled across sample types - may conflate effects'}
    }

    with open(outdir / 'observed_effect_sizes_pooled.json', 'w') as f:
        json.dump(effect_sizes_pooled, f, indent=2)
    asv_strata.to_csv(outdir / 'asv_stratification.tsv', sep='\t', index=False)
    with open(outdir / 'spike_scenario_asvs.json', 'w') as f:
        json.dump(spike_scenarios, f, indent=2)
    
    patient_shannon_df = pd.DataFrame([
        {'Patient_ID': p, 'Shannon': shannon, 'Case': patient_case_map.get(p, 'Unknown')}
        for p, shannon in patient_shannon.items()
    ])
    patient_shannon_df.to_csv(outdir / 'patient_level_shannon.tsv', sep='\t', index=False)

    # Save taxonomic effects
    phylum_effects.to_csv(outdir / 'phylum_effect_sizes.tsv', sep='\t', index=False)
    family_effects.to_csv(outdir / 'family_effect_sizes.tsv', sep='\t', index=False)

    print(f"Saved: {outdir}")
    print("  - observed_effect_sizes_stratified.json (Cancer vs Control by sample type)")
    print("  - observed_effect_sizes_sample_type_comparisons.json (Sample type comparisons)")
    print("  - observed_effect_sizes_pooled.json (pooled for reference)")
    print("  - asv_stratification.tsv")
    print("  - spike_scenario_asvs.json")
    print("  - patient_level_shannon.tsv")
    print("  - phylum_effect_sizes.tsv")
    print("  - family_effect_sizes.tsv")
    print("\n✓ Effect size estimation complete!")


if __name__ == '__main__':
    main()

#!/usr/bin/env python3
"""
power_main.py

Main orchestrator for complete power analysis.
Runs all analyses: cancer vs control, sample type comparisons, taxonomic abundance, and ISA.
"""

import argparse
import subprocess
from pathlib import Path
import sys


def run_command(cmd, description):
    """Run a command and report status."""
    print(f"\n{'='*60}")
    print(f"{description}")
    print(f"{'='*60}")
    print(f"Command: {' '.join(cmd)}\n")

    result = subprocess.run(cmd, capture_output=False)

    if result.returncode != 0:
        print(f"ERROR: {description} failed with exit code {result.returncode}")
        return False
    return True


def main():
    parser = argparse.ArgumentParser(
        description="Run complete power analysis for lung microbiome study"
    )
    parser.add_argument("--data-wide", required=True, help="Wide count matrix TSV")
    parser.add_argument("--data-long", required=True, help="Long format data TSV")
    parser.add_argument("--effect-sizes-dir", required=False, help="Directory with effect sizes (required for spike scenarios)")
    parser.add_argument("--sample-sizes-cancer", default="5,8,10,15,20,25,30,40,50,60,70,80,90,100",
                       help="Sample sizes for cancer vs control analyses")
    parser.add_argument("--sample-sizes-stype", default="10,15,20,25,30,40,50,60,70,80,90,100",
                       help="Sample sizes for sample type comparisons")
    parser.add_argument("--n-simulations", type=int, default=1000)
    parser.add_argument("--n-perm", type=int, default=199)
    parser.add_argument("--alpha", type=float, default=0.05)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--outdir", required=True)
    parser.add_argument("--transform", choices=["none", "rclr"], default="none",
                       help="Compositional transform for non-Shannon analyses")
    parser.add_argument("--sample-col", default="sample", help="Sample ID column in long-format data")
    parser.add_argument("--patient-col", default="Participant_ID", help="Patient ID column in long-format data")
    parser.add_argument("--case-col", default="Case", help="Case/control label column in long-format data")
    parser.add_argument("--type-col", default="type_group", help="Sample type column in long-format data")
    parser.add_argument("--exclude-contralateral-in-cancer", type=lambda x: str(x).lower()=="true", default=True)
    parser.add_argument("--contralateral-sample-types", default="Bronchial Brush,BAL")
    parser.add_argument("--scenarios", default="observed,null",
                       help="Comma-separated: observed, null, weak, moderate, strong")

    # Analysis selection flags
    parser.add_argument("--analyses", default="all",
                       help="Comma-separated list: all, cancer_vs_control, sample_type, taxonomic, isa")
    parser.add_argument("--skip-cancer-permanova", action='store_true')
    parser.add_argument("--skip-cancer-shannon", action='store_true')
    parser.add_argument("--skip-stype-permanova", action='store_true')
    parser.add_argument("--skip-stype-shannon", action='store_true')
    parser.add_argument("--skip-taxonomic", action='store_true')
    parser.add_argument("--skip-isa", action='store_true')

    args = parser.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    script_dir = Path(__file__).resolve().parent

    # Determine which analyses to run
    analyses = args.analyses.lower().split(",")
    run_all = "all" in analyses
    contralateral_types = [x.strip() for x in args.contralateral_sample_types.split(",") if x.strip()]
    brush_label = next((x for x in contralateral_types if x != "BAL"), "Bronchial Brush")

    run_cancer = run_all or "cancer_vs_control" in analyses
    run_stype = run_all or "sample_type" in analyses
    run_taxonomic = run_all or "taxonomic" in analyses
    run_isa = run_all or "isa" in analyses

    # Check if spike scenarios are needed
    requested_scenarios = [s.strip().lower() for s in args.scenarios.split(',')]
    needs_spikes = any(s in requested_scenarios for s in ['weak', 'moderate', 'strong'])

    spike_scenarios = None
    if needs_spikes:
        if not args.effect_sizes_dir:
            print("ERROR: --effect-sizes-dir required for spike scenarios (weak, moderate, strong)")
            return 1
        effect_sizes_dir = Path(args.effect_sizes_dir)
        spike_scenarios = effect_sizes_dir / 'spike_scenario_asvs.json'
        if not spike_scenarios.exists():
            print(f"ERROR: Spike scenarios file not found: {spike_scenarios}")
            print("Please run estimate_effects.py first!")
            return 1

    success = True
    completed = []

    print("\n" + "="*60)
    print("COMPLETE POWER ANALYSIS")
    print("="*60)
    print(f"Output directory: {outdir}")
    print(f"Simulations: {args.n_simulations}")
    print(f"Analyses to run: {args.analyses}")
    print("="*60)

    # ========================================================================
    # 1. Cancer vs Control (Stratified by Sample Type)
    # ========================================================================

    if run_cancer and not args.skip_cancer_permanova:
        cmd = [
            sys.executable, str(script_dir / 'power_permanova_stratified.py'),
            '--data-wide', args.data_wide,
            '--data-long', args.data_long,
            '--sample-sizes', args.sample_sizes_cancer,
            '--n-simulations', str(args.n_simulations),
            '--n-perm', str(args.n_perm),
            '--alpha', str(args.alpha),
            '--seed', str(args.seed),
            '--outdir', str(outdir),
            '--transform', args.transform,
            '--sample-col', args.sample_col,
            '--patient-col', args.patient_col,
            '--case-col', args.case_col,
            '--type-col', args.type_col,
            '--exclude-contralateral-in-cancer', str(args.exclude_contralateral_in_cancer),
            '--contralateral-sample-types', args.contralateral_sample_types,
            '--scenarios', args.scenarios
        ]

        if spike_scenarios:
            cmd.extend(['--spike-scenarios', str(spike_scenarios)])

        if run_command(cmd, "1. Cancer vs Control - PERMANOVA (Stratified)"):
            completed.append("cancer_vs_control_permanova")
        else:
            success = False

    if run_cancer and not args.skip_cancer_shannon:
        cmd = [
            sys.executable, str(script_dir / 'power_shannon_stratified.py'),
            '--data-wide', args.data_wide,
            '--data-long', args.data_long,
            '--sample-sizes', args.sample_sizes_cancer,
            '--n-simulations', str(args.n_simulations),
            '--alpha', str(args.alpha),
            '--seed', str(args.seed),
            '--outdir', str(outdir),
            '--sample-col', args.sample_col,
            '--patient-col', args.patient_col,
            '--case-col', args.case_col,
            '--type-col', args.type_col,
            '--exclude-contralateral-in-cancer', str(args.exclude_contralateral_in_cancer),
            '--contralateral-sample-types', args.contralateral_sample_types,
            '--scenarios', args.scenarios
        ]

        if run_command(cmd, "2. Cancer vs Control - Shannon Mann-Whitney (Stratified)"):
            completed.append("cancer_vs_control_shannon")
        else:
            success = False

    # ========================================================================
    # 2. Sample Type Comparisons
    # ========================================================================

    if run_stype and not args.skip_stype_permanova:
        cmd = [
            sys.executable, str(script_dir / 'power_sample_type_permanova.py'),
            '--data-wide', args.data_wide,
            '--data-long', args.data_long,
            '--sample-sizes', args.sample_sizes_stype,
            '--n-simulations', str(args.n_simulations),
            '--n-perm', str(args.n_perm),
            '--alpha', str(args.alpha),
            '--seed', str(args.seed),
            '--outdir', str(outdir),
            '--transform', args.transform,
            '--sample-col', args.sample_col,
            '--patient-col', args.patient_col,
            '--type-col', args.type_col
        ]

        if run_command(cmd, "3. Sample Type Comparisons - PERMANOVA"):
            completed.append("sample_type_permanova")
        else:
            success = False

    if run_stype and not args.skip_stype_shannon:
        cmd = [
            sys.executable, str(script_dir / 'power_sample_type_shannon.py'),
            '--data-wide', args.data_wide,
            '--data-long', args.data_long,
            '--sample-sizes', args.sample_sizes_stype,
            '--n-simulations', str(args.n_simulations),
            '--alpha', str(args.alpha),
            '--seed', str(args.seed),
            '--outdir', str(outdir),
            '--sample-col', args.sample_col,
            '--patient-col', args.patient_col,
            '--type-col', args.type_col
        ]

        if run_command(cmd, "4. Sample Type Comparisons - Shannon Paired Wilcoxon"):
            completed.append("sample_type_shannon")
        else:
            success = False

    # ========================================================================
    # 3. Taxonomic Differential Abundance (Phylum + Family)
    # ========================================================================

    if run_taxonomic and not args.skip_taxonomic:
        # 3a. Cancer vs Control (stratified by sample type)
        cmd = [
            sys.executable, str(script_dir / 'power_taxonomic_abundance.py'),
            '--data-long', args.data_long,
            '--sample-sizes', args.sample_sizes_cancer,
            '--n-simulations', str(args.n_simulations),
            '--alpha', str(args.alpha),
            '--seed', str(args.seed),
            '--outdir', str(outdir),
            '--transform', args.transform,
            '--sample-col', args.sample_col,
            '--patient-col', args.patient_col,
            '--case-col', args.case_col,
            '--type-col', args.type_col,
            '--exclude-contralateral-in-cancer', str(args.exclude_contralateral_in_cancer),
            '--contralateral-sample-types', args.contralateral_sample_types,
            '--scenarios', args.scenarios
        ]

        if args.effect_sizes_dir:
            cmd.extend(['--effect-sizes-dir', str(args.effect_sizes_dir)])

        if run_command(cmd, "5a. Taxonomic Abundance - Cancer vs Control (Phylum + Family)"):
            completed.append("taxonomic_abundance_cancer")
        else:
            success = False

        # 3b. Sample Type Comparison
        cmd = [
            sys.executable, str(script_dir / 'power_taxonomic_sample_type.py'),
            '--data-long', args.data_long,
            '--sample-sizes', args.sample_sizes_stype,
            '--n-simulations', str(args.n_simulations),
            '--alpha', str(args.alpha),
            '--seed', str(args.seed),
            '--outdir', str(outdir),
            '--transform', args.transform,
            '--sample-col', args.sample_col,
            '--patient-col', args.patient_col,
            '--type-col', args.type_col
        ]

        if run_command(cmd, "5b. Taxonomic Abundance - Sample Type Comparison (Phylum + Family)"):
            completed.append("taxonomic_abundance_stype")
        else:
            success = False

    # ========================================================================
    # 4. Indicator Species Analysis (ISA) - Both comparisons
    # ========================================================================

    if run_isa and not args.skip_isa:
        cmd = [
            'Rscript', str(script_dir / 'power_indicspecies.R'),
            '--data-long', args.data_long,
            '--data-wide', args.data_wide,
            '--group-cols', f"status,{args.type_col}",
            '--status-contralateral-sites', args.contralateral_sample_types,
            '--lung-brush-label', brush_label,
            '--sample-sizes-cancer', args.sample_sizes_cancer,
            '--sample-sizes-stype', args.sample_sizes_stype,
            '--n-simulations', str(args.n_simulations),
            '--perms', str(args.n_perm),
            '--alpha', str(args.alpha),
            '--seed', str(args.seed),
            '--outdir', str(outdir),
            '--transform', args.transform,
            '--sample-col', args.sample_col,
            '--patient-col', args.patient_col,
            '--scenarios', args.scenarios
        ]

        if run_command(cmd, "6. Indicator Species Analysis - Cancer vs Control + Sample Type"):
            completed.append("isa")
        else:
            success = False

    # ========================================================================
    # Summary
    # ========================================================================

    print(f"\n{'='*60}")
    if success:
        print("✓ All power analyses complete!")
    else:
        print("⚠ Some analyses failed or were skipped.")

    print(f"{'='*60}")
    print(f"\nResults saved to: {outdir}")
    print("\nCompleted analyses:")
    for analysis in completed:
        print(f"  ✓ {analysis}")

    print("\nOutput files:")
    if "cancer_vs_control_permanova" in completed:
        print("  - permanova_power_stratified.tsv (Cancer vs Control by sample type)")
    if "cancer_vs_control_shannon" in completed:
        print("  - shannon_power_stratified.tsv (Cancer vs Control by sample type)")
    if "sample_type_permanova" in completed:
        print("  - sample_type_permanova_power.tsv")
    if "sample_type_shannon" in completed:
        print("  - sample_type_shannon_power.tsv")
    if "taxonomic_abundance_cancer" in completed:
        print("  - taxonomic_abundance_power.tsv (Cancer vs Control, Phylum + Family)")
    if "taxonomic_abundance_stype" in completed:
        print("  - taxonomic_sample_type_power.tsv (Sample Type, Phylum + Family)")
    if "isa" in completed:
        print("  - isa_power_complete.tsv (Cancer vs Control + Sample Type)")

    print("="*60)

    return 0 if success else 1


if __name__ == '__main__':
    exit(main())

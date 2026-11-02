# ASPIRE

ASPIRE is a Nextflow DSL2 workflow for ASV generation, taxonomy assignment, decontamination, metadata-linked ASV summaries, ecological analyses, VOC association analyses, network/module analyses, and optional ASV-to-MAG linkage.

The supported entrypoint is `run_asv_pipeline.sh`. It bootstraps the controller environment, launches `asv_pipeline.nf`, manages resume behavior, and supports stage-aware reruns with `--rerun-from`.

## Key Files

- `run_asv_pipeline.sh`: main wrapper for routine runs.
- `asv_pipeline.nf`: current Nextflow workflow.
- `asv_pipeline_nextflow.yml`: full config template.
- `examples/set1-2.local.yml`: local example config with absolute paths for the LMP test dataset.
- `examples/mock.local.yml`: full-module template used by the portable mock config generator.
- `examples/configure_mock_run.sh`: validates a supplied mock fixture and writes a machine-local YAML.
- `examples/validate_mock_run.sh`: validates the completed mock run against its truth contract.
- `examples/MOCK_DATASET_TESTING.md`: expanded mock test instructions.
- `processes/`: scripts and conda environment YAMLs used by individual stages.

## Requirements

ASPIRE is developed for a 64-bit Linux environment. Before starting, install:

- Bash and standard GNU command-line utilities.
- Conda or Mamba, with `mamba` available on `PATH`.
- Git for obtaining and identifying the workflow revision.
- Internet access on the first run to solve Conda environments and download
  configured SINA and QIIME2/SILVA references. Fully offline runs require
  pre-populated package caches and local reference paths.

The wrapper creates a repository-local controller environment containing
Nextflow and its Java runtime, then creates process-specific environments from
the committed YAML definitions. Users should not manually combine all process
dependencies into one environment. ASPIRE isolates the run's package cache and
serializes Conda environment creation to prevent concurrent repodata-lock
failures. Environment solves use strict channel priority to avoid pathological
cross-channel backtracking, and module-specific environments avoid the legacy
all-in-one dependency search space. Builds are terminated after 30 minutes by
default rather than hanging indefinitely; set `ASPIRE_MAMBA_BUILD_TIMEOUT` only
when a slower package source is expected. Analysis tasks remain parallel.

Resource needs depend on sample count and sequencing depth. For the complete
mock benchmark, provision at least 8 CPU cores, 32 GB RAM, and 50 GB of free
storage for input data, Conda environments, Nextflow work files, downloaded
references, and final outputs. `resources.threads` controls per-task CPU use; it
does not limit the total storage used by cached tasks.

Verify the entrypoint prerequisites:

```bash
command -v bash
command -v git
command -v mamba
```

If `mamba` is unavailable, install a current Miniforge distribution from
<https://github.com/conda-forge/miniforge> and open a new shell before running
ASPIRE.

## Mock Dataset Quick Start

Download and extract the ASPIRE mock dataset from
[Zenodo (DOI: 10.5281/zenodo.21358300)](https://doi.org/10.5281/zenodo.21358300).
The [direct Zenodo record](https://zenodo.org/records/21358300) provides the
dataset archive used by this test.

Clone ASPIRE and record the exact revision:

```bash
git clone https://github.com/hallamlab/ASPIRE.git
cd ASPIRE
git rev-parse HEAD
```

Using the extracted `mock_dataset/`, generate a portable configuration. Do not
edit the developer paths in `examples/mock.local.yml`:

No controller-environment setup or activation is required. The configuration
script creates or updates `.controller_env`, and the run and validation wrappers
reuse it automatically.

```bash
./examples/configure_mock_run.sh \
  --dataset /absolute/path/to/mock_dataset \
  --output /absolute/path/to/aspire_mock_output \
  --config-out mock_run.generated.yml
```

Run every benchmarked module except optional ASV-to-MAG linkage:

```bash
./run_asv_pipeline.sh mock_run.generated.yml --no-resume
```

Validate both technical completion and recovery of the dataset's known signals:

```bash
./examples/validate_mock_run.sh \
  --dataset /absolute/path/to/mock_dataset \
  --results /absolute/path/to/aspire_mock_output
```

Success is reported as `All mock-run checks passed.` with exit status zero. The
validator derives sample counts from the supplied manifest and metadata, checks
known mitochondrial/contaminant removal, confirms taxonomy and statistical
outputs, rejects a degenerate network, verifies SVG production, and checks every
published module file against its SHA-256 manifest. See
`examples/MOCK_DATASET_TESTING.md` for the dataset schema, restart instructions,
and interpretation of failed checks.

The visible output directories are created when the run starts and updated only
after the workflow completes successfully. Nextflow work files, Conda environments,
and internal staging persist under `<output_dir>/.aspire/`, allowing the same command
to resume interrupted, failed, or completed runs without a separate temporary tree.

For a reproducibility record, retain the ASPIRE Git commit, supplied dataset
checksum table, generated YAML and manifest, launch command, and the completed
`summary/tables/` directory. The latter records the resolved run configuration,
input manifest, reference checksums, module inventory, output SHA-256 values,
and integrated master tables.

**Review the mock outputs:** Open
`<output_dir>/summary/report/ASPIRE_run_report.html` in a web browser as the
starting point for reviewing the completed run. Its opening **Data Accounting
Summary** reports analyzed samples, participants, study-group counts, sequence
totals, and retained ASVs, followed by the filtering Sankey, read-depth
swarmplot, ASV-overlap UpSet plot, and collector's curves. The **Output
Inventory** summarizes the number of tables and plots produced by each module
and links directly to their locations under `<output_dir>/modules/`. The final
**Nextflow run details** section links the execution report, timeline, trace,
workflow DAG, launch command, version record, controller log, task inventory,
and per-task logs under `<output_dir>/logs/`. Exact output paths and SHA-256
checksums remain available in `<output_dir>/summary/tables/`. The report is an
accounting and navigation aid; it does not provide biological interpretation.

## General Quick Start

Create a run config from the full template, then edit all paths for your environment:

```bash
cp asv_pipeline_nextflow.yml my_run.yml
```

At minimum, review:

- `paths.input_dir`
- `paths.output_dir`
- `paths.manifest`
- `paths.runtime_dir`
- `paths.keep_runtime_dir`
- `paths.work_dir`
- `paths.conda_cache_dir`
- any `/abs/path/...` placeholder
- enabled optional branches that require metadata, reference databases, VOC tables, or genome/MAG inputs

Run the pipeline:

```bash
./run_asv_pipeline.sh my_run.yml
```

List valid stage names for targeted reruns:

```bash
./run_asv_pipeline.sh --list-stages
```

Legacy stage names from earlier ASPIRE releases are accepted as rerun aliases
for compatibility, but `--list-stages` reports the canonical current names.

Force a rerun from one stage onward using the retained default runtime cache:

```bash
./run_asv_pipeline.sh my_run.yml --rerun-from PLOT_METADATA
```

Pass extra Nextflow options after `--`:

```bash
./run_asv_pipeline.sh my_run.yml -- -with-report report.html -with-trace trace.tsv
```

Direct Nextflow invocation is intended only for debugging because public-output
finalization is performed by the wrapper:

```bash
nextflow run asv_pipeline.nf --params-file my_run.yml --pipeline_config my_run.yml
```

**Review the outputs:** After a successful wrapper run, open
`<output_dir>/summary/report/ASPIRE_run_report.html` in a web browser. This is
the recommended starting point before biological interpretation. Review the
opening **Data Accounting Summary** for sample and group composition, sequence
and ASV retention, and the Sankey, swarmplot, UpSet, and collector's-curve
figures. Use the **Output Inventory** to open each enabled module's tables and
plots under `<output_dir>/modules/`. Use **Nextflow run details** to inspect task
status and resource use, execution timing, the workflow DAG, trace data,
software/runtime records, and controller or per-task logs under
`<output_dir>/logs/`. Machine-readable inventories, exact paths, and checksums
are stored under `<output_dir>/summary/tables/`.

## Inputs

FASTQs can be discovered from `paths.input_dir`. Every run writes a normalized,
reusable manifest to `<output_dir>/summary/tables/run_manifest.tsv`, regardless of
whether discovery or `paths.manifest` supplied the inputs.

Manifest format:

- Tab-separated; the `sample_id`, `fastq_r1`, `fastq_r2` header is optional.
- Column 1: `sample_id`.
- Column 2: R1 FASTQ.
- Column 3: R2 FASTQ, optional for single-end data.
- Lines starting with `#` are ignored.
- Relative FASTQ paths are resolved relative to the manifest file.

See `examples/manifest.template.tsv` for a reusable template.

Metadata is required by enabled metadata-aware branches such as metadata plots, Sankey, diversity, indicator species, VOC correlation, power analysis, taxonomy patient-aware analysis, lung-status analysis, and several network overlays. The configured sample column must match the manifest sample IDs.

Metadata column names are configured per run (`sample_col`, `type_col`,
`case_col`, `patient_col`, and related settings); ASPIRE does not require fixed
study-specific names. If the configured color column is absent, ASPIRE assigns
deterministic colors and writes both an augmented metadata table and a reusable
two-column palette under `<output_dir>/modules/metadata_plots/tables`. Set `palette_file` in
`metadata_plots` or `sankey` to override it. See
`examples/metadata_palette.template.tsv`.

Reference inputs depend on enabled branches:

- `sina.reference` and taxonomy references are used for SINA alignment and taxonomy assignment. The config can point at local files or URLs.
- Mitochondrial/contaminant decontamination accepts existing database prefixes
  through `mito.mito_db` and `mito.biof_db`, or FASTA inputs through
  `mito.mito_fasta` and `mito.contaminant_fasta`. ASPIRE exports/copies and
  rebuilds both as run references under `<output_dir>/references/reference/blast_databases`.
- `mito.run_mitomaster: false` skips the external MITOMASTER service while
  retaining taxonomy and local BLAST screens, which is useful for offline tests.
- `voc_correlation.voc_table` is required when VOC correlation is enabled.
- `asv_mag_link.*` inputs are required only when ASV-to-MAG linkage is enabled.

### Preparing a new production dataset

The mock quick start demonstrates the software, but it is not a substitute for
configuring study-specific inputs. For a new dataset:

1. Build a TSV manifest with stable, unique sample IDs and readable FASTQ paths,
   or verify that filename discovery produces those IDs.
2. Make every metadata-aware module use the same sample-ID column and ensure
   its values match the manifest exactly.
3. Copy `asv_pipeline_nextflow.yml`, replace every active `/abs/path/...`, and
   disable modules whose required metadata/reference inputs are unavailable.
4. Set trimming, expected amplicon length, and merge parameters for the actual
   assay; the shipped values are not universal sequencing defaults.
5. Run core sequence processing first, inspect read retention and taxonomy,
   then enable statistical modules in groups.
6. Treat palettes, group orders, metadata columns, patient-pairing fields, and
   biological thresholds as examples that must be adapted.
7. Preserve the manifest, supplied YAML, Git revision, logs, and summary
   checksums with the analysis record.

A module with `enabled: true` is only runnable when its conditional inputs and
metadata columns are present.

## Complete Configuration Reference

See [`docs/CONFIGURATION.md`](docs/CONFIGURATION.md) for the exhaustive
field-by-field reference to `asv_pipeline_nextflow.yml`. It records types,
defaults, accepted values, conditional requirements, module dependencies, and
study-specific values. Use this README for the operational walkthrough and the
reference while constructing or reviewing a production YAML.

## Workflow Stages

The wrapper's current stage order is:

1. `FASTP_QC`
2. `MERGE_READS`
3. `FILTER_READS`
4. `RELABEL_FILTERED`
5. `CONCAT_FASTAS`
6. `DEREPLICATE`
7. `DENOISE`
8. `CHIMERA_CHECK`
9. `CREATE_COUNT_MATRIX`
10. `FILTER_TABLE`
11. `SINA_TRIM`
12. `TAXONOMY`
13. `PREPARE_BLAST_DATABASES`
14. `MITOMASTER`
15. `MITO_DECONTAM`
16. `FILTER_COUNTS`
17. `GENERAL_STATS`
18. `PLOT_METADATA`
19. `GROUPING_DIAGNOSTICS`
20. `GROUP_LABEL_AUGMENTATION`
21. `PLOT_UPSET`
22. `ASV_BATCH_CORRECTION`
23. `ASV_META_FROM_CORRECTED`
24. `BUBBLEPLOTTER`
25. `UMAP_CLUSTERING`
26. `OUTLIER_CHECKER`
27. `COLLECTORS_CURVE`
28. `DIVERSITY_ANALYSIS`
29. `INDICSPECIES`
30. `INDICSPECIES_PLOTS`
31. `INDICSPECIES_ALIGNED_PLOTS`
32. `VOC_CORRELATION`
33. `MEASUREMENT_ASSOCIATION`
34. `CLUSTERMAPS`
35. `GROUP_POWER_ANALYSIS`
36. `TAXONOMY_GROUP_ASSOCIATION`
37. `PAIRED_GROUP_CONTRAST`
38. `SPIECEASI`
39. `NETWORK_MODULES`
40. `ASV_MAG_LINK`
41. `ASV_MAG_NETWORK`
42. `GRAPH_NETWORK`
43. `MODULE_MAG_ANCHORS`
44. `SANKEY`
45. `MASTER_SUMMARY`

Disabled optional branches are skipped based on the YAML config.

## Scripts Used By Stage

Stages that do not list a custom ASPIRE script are executed directly by Nextflow using command-line tools. In the core ASV path, fastp is used for read trimming, vsearch is used for read merging, quality filtering, dereplication, denoising, chimera removal, and read-to-ASV mapping, SINA is used for sequence alignment before region trimming, seqkit is used for sequence splitting/statistics, and blastn is used for mitochondrial and contaminant database searches.

| Workflow stage | Script used | Purpose |
|---|---|---|
| Pipeline launch | `run_asv_pipeline.sh` | Initializes the controller environment, resolves work/cache directories, and launches `asv_pipeline.nf` with the selected YAML config. |
| Output finalization | `processes/output_layout/organize_outputs.py` | Atomically publishes runtime staging into module tables/plots, intermediates, references, logs, and integrated summary manifests. |
| Workflow orchestration | `asv_pipeline.nf` | Defines process order, config parsing, inputs/outputs, conda environments, and enabled/disabled analysis branches. |
| `FILTER_TABLE` | `processes/filter_table/filter_ASV_table.py` | Filters the intermediate ASV count table by minimum sample read depth and minimum ASV abundance. |
| `SINA_TRIM` | `processes/sina_trim/parse_sina_log.py` | Parses SINA variable-region annotations from SINA logs. |
| `SINA_TRIM` | `processes/sina_trim/trim_v_sina.py` | Trims dereplicated ASV sequences to configured variable regions. |
| `TAXONOMY` | `processes/taxonomy/qiime_vs_classifier.py` | Calls the QIIME2 Python API and q2-feature-classifier to classify ASVs against configured SILVA artifacts. |
| `PREPARE_BLAST_DATABASES` | Command-line `blastdbcmd` and `makeblastdb` | Normalizes configured FASTA files or existing nucleotide database prefixes into archived run-specific BLAST databases. |
| `MITOMASTER` | `processes/mitomaster/mitomaster.py` | Queries MITOMASTER for candidate mitochondrial ASVs. |
| `MITO_DECONTAM` | `processes/mito_decontam/mito_checker.py` | Integrates MITOMASTER, mitochondrial blastn, contaminant blastn, and taxonomy evidence into non-target calls and plots. |
| `FILTER_COUNTS` | `processes/filter_counts/filter_nontarget.py` | Removes non-target, mitochondrial, low-abundance, low-quality taxonomy, and explicitly excluded taxa; writes final `ASV_target.tsv`. |
| `PLOT_METADATA` | `processes/plot_metadata/plot_metadata.py` | Merges ASV counts with metadata, applies configured control subtraction, and writes ASV metadata tables and plots. |
| `SANKEY` | `processes/sankey/sankey_builder.py` | Builds data-loss Sankey and sample-retention summaries from raw, filtered, decontaminated, and final tables. |
| `PLOT_UPSET` | `processes/plot_upset/plot_upset.py` | Produces ASV/sample overlap plots for raw/final microbial and mitochondrial tables. |
| `BUBBLEPLOTTER` | `processes/bubbleplotter/bubbleplotter.py` | Generates metadata-linked ASV bubble plots. |
| `UMAP_CLUSTERING` | `processes/umap_clustering/umap_clustering.py` | Generates UMAP/HDBSCAN summaries from ASV-linked metadata tables. |
| `ASV_BATCH_CORRECTION` | `processes/asv_batch_correction/asv_batch_correction.py` | Performs optional ASV abundance batch correction and diagnostics. |
| `OUTLIER_CHECKER` | `processes/outlier_checker/outlier_checker.py` | Performs optional outlier detection from configured ASV abundance inputs. |
| `COLLECTORS_CURVE` | `processes/collectors_curve/collectors_curve.py` | Produces species/ASV accumulation curves by metadata group. |
| `DIVERSITY_ANALYSIS` | `processes/diversity_analysis/calc_div.py` | Calculates Shannon diversity, Bray-Curtis distance, and Jaccard distance. |
| `DIVERSITY_ANALYSIS` | `processes/diversity_analysis/plot_diversity.py` | Generates diversity plots, UMAPs, heatmaps, and PERMANOVA outputs. |
| `DIVERSITY_ANALYSIS` patient-aware sub-branch | `processes/bray_patient_aware/run_bray_permanova_patient_aware.R` | Runs optional patient-aware Bray-Curtis PERMANOVA analyses. |
| `DIVERSITY_ANALYSIS` patient-aware sub-branch | `processes/bray_patient_aware/plot_bray_permanova_patient_aware.py` | Plots patient-aware Bray-Curtis PERMANOVA outputs. |
| `INDICSPECIES` | `processes/indicspecies/run_indicspecies.R` | Runs indicator species analysis with the R `indicspecies` package. |
| `INDICSPECIES_PLOTS` | `processes/indicspecies_plots/plot_indicspecies.py` | Generates standard indicator species plots and summaries. |
| `INDICSPECIES_PLOTS` aligned sub-branch | `processes/indicspecies_aligned_plots/plot_indicspecies_aligned.py` | Generates optional aligned indicator species summaries and figures. |
| `VOC_CORRELATION` | `processes/voc_correlation/plot_voc_corr.py` | Matches VOC and ASV samples, filters ASVs, computes ASV-VOC Spearman correlations, applies FDR and direction filtering, and generates VOC/correlation plots. |
| `MEASUREMENT_ASSOCIATION` | `processes/measurement_association/measurement_association.py` | Associates ASV abundances with configured sample measurements using Spearman correlations, clustermaps, and constrained ordination biplots. |
| `MEASUREMENT_ASSOCIATION` ordination sub-branch | `processes/measurement_association/run_measurement_association.R` | Runs CCA, RDA, and dbRDA models with the R `vegan` package. |
| `GROUPING_DIAGNOSTICS` | `processes/grouping_diagnostics/grouping_diagnostics.py` | Compares metadata groupings against ASV community distances and validates optional non-outlier soft labels. |
| `GROUP_LABEL_AUGMENTATION` | `processes/group_label_augmentation/group_label_augmentation.py` | Applies only cross-validated, confidence-gated soft labels while preserving observed labels and assignment provenance. |
| `CLUSTERMAPS` | `processes/clustermaps/plot_clustermaps.py` | Generates ASV and metadata clustermaps from configured count and metadata inputs. |
| `GROUP_POWER_ANALYSIS` input build | `processes/master_summary/build_master_asv_summary.py` | Builds long-format ASV summary inputs used by the group power-analysis branch. |
| `GROUP_POWER_ANALYSIS` | `processes/power_analysis_pipeline/run_power_analysis_pipeline.sh` | Launches the power-analysis subworkflow. |
| `GROUP_POWER_ANALYSIS` subworkflow | scripts under `processes/power_analysis_pipeline/` | Runs simulation-based power analyses for diversity, taxonomic abundance, indicator species, and related plotting. |
| `TAXONOMY_GROUP_ASSOCIATION` input build | `processes/master_summary/build_master_asv_summary.py` | Builds long-format ASV summary inputs used by taxonomy group-association analyses. |
| `TAXONOMY_GROUP_ASSOCIATION` | `processes/taxonomy_patient_aware/run_taxonomic_abundance_analysis.py` | Runs taxonomic abundance comparisons between configured metadata groups. |
| `TAXONOMY_GROUP_ASSOCIATION` | `processes/taxonomy_patient_aware/run_taxonomic_sample_type_analysis.py` | Runs paired or sample-type taxonomic abundance comparisons. |
| `TAXONOMY_GROUP_ASSOCIATION` | `processes/taxonomy_patient_aware/plot_taxonomic_observed_analysis.py` | Plots observed taxonomic abundance analysis outputs. |
| `PAIRED_GROUP_CONTRAST` input build | `processes/master_summary/build_master_asv_summary.py` | Builds long-format ASV summary inputs used by paired group-contrast analyses. |
| `PAIRED_GROUP_CONTRAST` | `processes/lung_status_analysis/prepare_lung_status_data.py` | Derives or accepts paired group labels and prepares per-sample/per-subject tables. |
| `PAIRED_GROUP_CONTRAST` | `processes/lung_status_analysis/run_lung_status_analysis.R` | Runs paired group-contrast statistical analyses. |
| `PAIRED_GROUP_CONTRAST` | `processes/lung_status_analysis/plot_lung_status_analysis.py` | Plots paired group-contrast analysis outputs. |
| `SPIECEASI` | `processes/spieceasi/run_spieceasi.R` | Runs SPIEC-EASI graphical lasso network inference and exports graph/network tables. |
| `NETWORK_MODULES` | `processes/network_modules/network_modules.R` | Detects network modules using configured Leiden/Louvain methods. |
| `ASV_MAG_NETWORK` | `processes/asv_mag_network/asv_mag_network.py` | Integrates ASV-MAG links with optional MAG abundance and functional annotations to build heterogeneous network tables and plots. |
| `GRAPH_NETWORK` | `processes/graph_network/graph_network.py` | Generates network visualizations and ASV/node annotations. |
| `MODULE_MAG_ANCHORS` | `processes/module_mag_anchors/summarize_module_mag_anchors.py` | Summarizes MAG-linked ASVs within network modules. |
| `MASTER_SUMMARY` | `processes/master_summary/build_master_asv_summary.py` | Builds final combined ASV summary tables integrating taxonomy, metadata, indicator species, network, VOC, and optional MAG information. |
| `ASV_MAG_LINK` | `processes/asv_mag_link/asv_mag_barrnap_linker.py` | Links ASVs to barrnap-derived SSU/16S sequences from genome/MAG inputs. |
| `ASV_MAG_LINK` | `processes/asv_mag_link/plot_asv_mag_link.py` | Plots ASV-MAG linkage summaries. |

## Indicator Species Analysis

`INDICSPECIES` always runs the primary analyses listed in `indicspecies.group_cols`, preserving the standard outputs such as `Type_Group_indicator_species_summary.tsv` and `Case_indicator_species_summary.tsv`.

Additional nested ISA runs can be requested with `indicspecies.stratified`. Each analysis tests `group_col` separately within each selected `within_col` value. For example, the VOC-enabled example config tests cancer/control indicators within each respiratory sample type:

```yaml
indicspecies:
  stratified:
    enabled: true
    analyses:
      - within_col: Type_Group
        group_col: Case
        levels:
          - Oral Rinse
          - BAL
          - Bronchial Brush
```

These stratified runs write per-level and pooled tables named like `stratified_Case_within_Type_Group_Bronchial_Brush_indicator_species_summary.tsv` and `stratified_Case_within_Type_Group_indicator_species_summary.tsv`.

## Count Filtering

`FILTER_COUNTS` produces the ASV count tables used by downstream analyses. The important outputs are:

- `ASV_target.tsv`: final microbial ASV table after contaminant removal, mitochondrial removal, abundance/prevalence filtering, taxonomy-quality filtering, and explicit taxon exclusions.
- `ASV_target.micro.tsv`: intermediate microbial table before final abundance/taxonomy filtering; kept for audit and data-loss summaries.
- `ASV_target.decon.tsv`: intermediate decontaminated table.
- `ASV_target.mito.tsv`: mitochondrial table.

Downstream metadata and VOC analyses use `ASV_target.tsv`, not the intermediate `.micro.tsv`, so taxa excluded by final filtering should not re-enter later outputs.

Explicit taxon exclusions are configured with `filter_counts.exclude_taxa`. Entries are exact, case-insensitive matches against parsed taxonomy ranks, and underscores in configured values are normalized to spaces. Supported forms include strings, maps, and mixed lists:

```yaml
filter_counts:
  enabled: true
  exclude_taxa:
    - "Species:Homo sapiens"
    - "Class:Mammalia"
    - Order: Primates
```

Use this for host or other known non-target ranks that should be removed even if they pass sequence and abundance filters.

## Metadata And ASV Outputs

`PLOT_METADATA` builds the run's metadata-linked ASV products. Typical outputs include:

- `modules/metadata_plots/tables/ASV_meta_micro.tsv`
- `modules/metadata_plots/tables/ASV_final.micro.tsv`
- `modules/metadata_plots/tables/metadata_updated_micro.tsv`
- `modules/metadata_plots/tables/master_table_micro.tsv`
- run metadata summaries and plots

When batch correction is enabled, ASPIRE writes before/after diagnostics and then selects the downstream count table according to `batch_correction.correction_policy`. In `auto` mode, corrected counts are used only if they reduce batch structure while preserving count-space and configured biological structure; otherwise downstream branches receive the raw uncorrected table. The decision is written to `batch_correction/batch_correction_decision.tsv`.

## VOC Correlation

`VOC_CORRELATION` links VOC abundances to filtered ASV abundances. It requires `voc_correlation.enabled: true`, a metadata table, the final filtered ASV counts, and `voc_correlation.voc_table`.

Direction filtering is controlled by:

```yaml
voc_correlation:
  enabled: true
  correlation_direction: positive  # positive, negative, or both
```

This setting applies to ASV-VOC correlation tables and ASV-VOC correlation heatmaps. For example, `positive` keeps only positive ASV-VOC correlations in the reported long table and correlation clustermap, allowing statements such as "VOC abundance was positively correlated with ASV X." Multiple-testing q-values are computed across the tested ASV-VOC pairs before direction filtering.

VOC abundance plots are different from correlation plots:

- `asv_voc_clustermap*` shows ASV-VOC correlation values and respects `correlation_direction`.
- `sample_voc_brush_clustermap*` shows per-sample VOC abundance z-scores, not correlations. Blue indicates lower-than-average VOC abundance for that VOC, white is near the VOC mean, and orange indicates higher-than-average abundance.
- `patient_case_voc_barplots_brush*` displays per-VOC patient z-scores so VOCs are visually comparable on one axis; statistical tests are still run on original patient-level VOC values.

## Measurement Association

`MEASUREMENT_ASSOCIATION` is the generalized association layer for VOC-like, biogeochemical, or other sample-level measurement tables. It uses the metadata-linked ASV outputs, optionally joins an external measurement table, filters ASVs, computes ASV-measurement Spearman correlations with FDR correction, and writes CCA/RDA/dbRDA biplot inputs and figures.

Typical configuration:

```yaml
measurement_association:
  enabled: true
  measurement_table: /path/to/measurements.tsv
  sample_col: Sample
  measurement_sample_col: Sample
  measurement_cols: [Oxygen, Nitrate, Temperature, Salinity]
  group_col: Type_Group
  group_palette: "A=#1f78b4,B=#33a02c"
  correlation_direction: both
  ordination_methods: cca,rda,dbrda
```

If `measurement_table` is omitted, numeric measurement columns are selected from the merged metadata table. Outputs are written under `measurement_association/` by default, including `tables/asv_measurement_spearman_long.tsv`, `plots/asv_measurement_spearman_clustermap.*`, ordination score tables, ANOVA tables, and CCA/RDA/dbRDA biplots.

## Optional Analysis Branches

Major optional modules are controlled by YAML `enabled` flags:

- `mito`: BLAST-based mitochondrial and contaminant screening.
- `non_target_filtering`: mitochondrial/contaminant screening, count filtering, audit tables, and explicit taxon exclusions.
- `general_stats`: run-level ASV and sample summaries.
- `metadata_plots`: metadata-linked ASV summary tables and plots.
- `plot_upset`, `bubbleplotter`, `umap_clustering`: metadata visualization branches.
- `batch_correction` and `outlier_detection`: corrected ASV tables and outlier checks.
- `collectors_curve`: rarefaction/collector curve summaries.
- `diversity`: Shannon, Bray-Curtis, Jaccard, and optional patient-aware diversity workflows.
- `indicator_analysis`: indicator species tables, standard plots, and aligned indicator plots.
- `voc_correlation`: VOC-ASV association analysis and VOC abundance visualizations.
- `measurement_association`: generalized sample-measurement associations, clustermaps, and CCA/RDA/dbRDA biplots.
- `clustermaps`: ASV and metadata heatmaps.
- `network_analysis`: SPIEC-EASI inference, module detection, network visualization, and optional module/MAG anchor tables.
- `power_analysis`: patient-aware power analysis using metadata-linked ASV tables.
- `taxonomy`: taxonomic assignment and patient-aware taxonomic comparisons.
- `lung_status_analysis`: patient-aware lung-status comparisons.
- `asv_mag_link` and `module_mag_anchors`: ASV-to-MAG/barrnap linkage and module anchoring.
- `sankey`: data-loss and filtering Sankey summaries.
- `master_summary`: final combined ASV summary export.

## Output Structure

Successful wrapper runs atomically publish a clean output tree:

```text
<output_dir>/
├── .aspire/             # persistent Nextflow work, Conda cache, and internal staging
├── modules/
│   └── <module>/
│       ├── tables/
│       └── plots/
├── intermediates/
├── references/
├── summary/
│   ├── tables/
│   ├── plots/
│   └── report/
└── logs/
```

Each enabled analytical module receives both `tables/` and `plots/`, even when
one is empty. `intermediates/` contains core FASTQ, FASTA, and ASV-processing
artifacts. `summary/tables/module_output_manifest.tsv` inventories and
checksums every module deliverable. The summary also contains the supplied run
configuration, normalized input manifest, per-module file/size totals,
reference checksums, an intermediate-file inventory, and the integrated master
ASV tables when `master_summary` is enabled. `summary/plots/module_output_summary.svg`
visualizes the published module inventory, and
`summary/report/ASPIRE_run_report.html` provides
an integrated, navigable run report. Its opening data-accounting section summarizes
analyzed samples, participants, group membership, retained ASVs, and sequence totals,
and displays the Sankey, read-depth swarmplot, and ASV-overlap UpSet without adding
biological interpretation. A combined collector's curve adds descriptive sampling-
coverage context. The report then inventories module outputs and links to
Nextflow's execution report, timeline, trace table, and workflow DAG. These artifacts are generated by
default under `logs/`, alongside the recorded launch command and Nextflow
version. `logs/controller.log` captures the complete Nextflow/controller stream;
`logs/task_execution.tsv` and `logs/tasks/` preserve the command, stdout, stderr,
trace, and exit code available for every task in the completed run. External
Nextflow integrations that require services or credentials, such as Tower,
webhooks, notifications, and telemetry exporters, remain opt-in. The publication
also writes `summary/tables/nextflow_artifact_manifest.tsv` with paths, sizes,
and SHA-256 checksums for all archived run records. The publication is assembled from runtime
staging only after Nextflow succeeds, so the visible output is never left in a
half-organized state.

## Runtime And Cache Behavior

By default, an output such as `/project/run/ASPIRE_output` keeps its runtime
state in a hidden directory inside that output:

```text
ASPIRE_output/.aspire/
├── nf_work/
├── conda_cache/
└── publication_staging/
```

The wrapper creates the visible modular directories at run start. After a
successful workflow, it atomically replaces only `modules/`, `intermediates/`,
`references/`, `summary/`, and `logs/`; `.aspire/` is never replaced during
publication. Set `paths.runtime_dir` to relocate the runtime tree. Runtime state
is retained by default so normal `-resume` and `--rerun-from` execution remains
available after successful, failed, and interrupted runs. Set
`paths.keep_runtime_dir: false` only when the cache should be discarded after a
successful run. Explicit
`paths.work_dir` and `paths.conda_cache_dir` values override their respective
derived paths.

Rerunning the same wrapper command resumes from the retained Nextflow cache. If a
branch does not rerun because cached outputs are valid, use `--rerun-from STAGE_NAME`.

Several stages include checksums of external process scripts in their task commands, so edits to important Python/R helper scripts invalidate the relevant Nextflow task cache. This avoids stale outputs when a script changes but the input filenames stay the same.

## Troubleshooting

If environment creation fails while collecting repodata with
`BlockingIOError: [Errno 11] Resource temporarily unavailable`, the failure is
a Conda cache-lock collision rather than evidence of an unavailable package.
Current wrapper runs use a run-local package cache and serialize environment
creation through `flock`. Restart the failed run normally; its retained
Nextflow work directory remains resumable. DNS, connection-timeout, or HTTP
errors instead indicate network or repository availability and are retried by
the controller before the run fails.

Interrupted Nextflow sessions can also leave `.env-*.lock` marker files even
when no `mamba` process remains. The wrapper takes an exclusive lock on the
configured Conda cache, rejects concurrent runs that share that cache, and then
removes these orphaned markers automatically. Messages about waiting for the
serialized mamba slot indicate active environment creation, not a deadlock.

- `No usable entries detected in manifest`: check tab separation, sample IDs, and FASTQ paths.
- `metadata file not found`: set the branch-specific metadata path or disable that branch.
- Host taxa still appear downstream: confirm `filter_counts.exclude_taxa` is set and rerun from `FILTER_COUNTS` or at least from `PLOT_METADATA` if the final `ASV_target.tsv` is already corrected.
- VOC direction did not change outputs: rerun from `VOC_CORRELATION`.
- Sankey complains about intermediates: set `filter_counts.save_intermediates: true`.
- BLAST database errors: set `mito.mito_db` and `mito.biof_db` to valid database prefixes or compatible FASTA paths.
- Conda solve errors: confirm `mamba` is available and review the relevant `environments.*` config entry.

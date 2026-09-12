# ASPIRE Configuration Reference

This document covers every public section and key in
`asv_pipeline_nextflow.yml`. Copy the template into a run-specific file. Empty
values mean “not set.” Lists are YAML lists unless a field explicitly uses a
comma-separated string. Unknown keys may be ignored and are not a stable
extension interface.

Values in the template demonstrate the respiratory study and mock workflows.
They are not universal biological defaults. In particular, adapt trimming and
length limits, metadata columns, group labels/orders, palettes, patient-pairing
fields, prevalence cutoffs, and statistical thresholds to the study design.

## Module dependency guide

Core FASTQ processing, ASV generation, SINA trimming, and taxonomy form the
base path. Most later modules consume `FILTER_COUNTS` and/or `PLOT_METADATA`.
Metadata plots require metadata with manifest-matching sample IDs. Diversity,
indicator, clustermap, grouping, power, paired, and network overlays require
their configured grouping columns. `VOC_CORRELATION` additionally needs a VOC
table. `MEASUREMENT_ASSOCIATION` needs measurement columns in metadata or a
joinable external table. MAG branches need genome-QC/barrnap or genome FASTA
inputs. Disable a branch when its conditional inputs are absent.

## Core input and sequence processing

### `paths`, `resources`, and `filename_patterns`

- `paths.input_dir`: directory searched for FASTQs when no manifest is given.
- `paths.output_dir`: required public output directory.
- `paths.manifest`: optional TSV with `sample_id`, `fastq_r1`, and `fastq_r2`.
- `paths.runtime_dir`: persistent runtime root; defaults to
  `<output_dir>/.aspire`.
- `paths.keep_runtime_dir`: retain runtime/cache after success when true.
- `paths.work_dir`, `paths.conda_cache_dir`: optional runtime subdirectory
  overrides. Use storage with sufficient space and safe locking.
- `resources.threads`: positive per-task CPU count.
- `resources.single_end`: allow samples without R2 when true.
- `filename_patterns.r1_tokens`, `r2_tokens`: tokens identifying read mates.
- `filename_patterns.ext_patterns`: regular expressions for accepted FASTQs.
- `filename_patterns.sample_strip_regex`: removes lane/read suffixes to form
  sample IDs. Verify generated IDs before matching metadata.

### Read processing and ASV generation

- `fastp.trim_front_r1`, `trim_tail_r1`, `trim_front_r2`, `trim_tail_r2`:
  non-negative base counts removed from read ends; set for the actual assay.
- `merge.max_diffs`: maximum overlap mismatches; `min_overlap`: minimum overlap
  bases; `trunc_quality`: quality truncation threshold; `allow_stagger`: permit
  staggered pairs.
- `table_filter.min_sample_sum`, `min_asv_sum`: non-negative total-count gates.
  `table_filter.script` is an optional developer script override.
- `filter.max_ee`: maximum expected errors; `min_len` and `max_len`: retained
  sequence length bounds in bases.
- `concat.relabel`: relabel sequences; `concat.label_sep`: label separator.
- `unoise.min_size`: minimum UNOISE abundance.
- `swarm.distance`: Swarm clustering distance.

## Alignment, taxonomy, and non-target filtering

### `sina`

`reference` is a local ARB reference and `reference_url` its download fallback;
`download_subdir` names the cache. `regions` lists tested variable regions and
`trim_to` selects the retained region. `batch_size`, `threads`, `keep_gaps`, and
`verbose` control execution and output.

### `taxonomy`

`ref_taxonomy` and `ref_sequences` are local QIIME artifacts; corresponding
`_url` keys are download fallbacks and `_filename` keys name cached files.
`download_subdir` locates the cache. `output_dir`, `output_tsv`, `stats_tsv`,
and `uppercase_fasta` name products; `threads` sets CPUs.

### `mito`

`enabled` enables screening. `run_mitomaster`, `chunk_dir`, `chunk_size`,
`mitomaster_workers`, `mitomaster_retries`, `mitomaster_timeout`, and
`mitomaster_header_mode` control MITOMASTER. `mito_db`/`biof_db` are BLAST
database prefixes; `mito_fasta`/`contaminant_fasta` are preferred FASTA
alternatives. `blast_threads`, `min_pident`, and `min_percov` control BLAST
acceptance. `mitochondria_substring`, `feature_col`, `taxon_col`,
`consensus_col`, `steps`, and `host_first_step` define evidence fields/order.
`output_dir`, `prefix`, `formats`, `figsize`, `style`, `dpi`, and `no_plots`
control products.

### `filter_counts` and `general_stats`

`filter_counts.enabled` enables final filtering. `metadata` and `sample_id_col`
identify samples; `group_col` and `min_group_size` support group prevalence;
`abundance_threshold` and `min_consensus` are filtering cutoffs.
`exclude_taxa` is the rank-aware exclusion list. `taxon_col`, `consensus_col`,
`biofactorial_col`, and `mito_cols` name evidence fields. `output`,
`mito_output_dir`, and `save_intermediates` control products.
`general_stats.enabled` toggles run statistics.

## Metadata and visualization

For all `sample_col` fields, values must match normalized manifest sample IDs.
`color_col` names a metadata color field; `palette_file` supplies an external
two-column palette; `*_palette` accepts `label=#RRGGBB` mappings.

- `sankey`: `enabled`, `metadata`, `sub_dir`, `sample_col`, `group1_col`,
  `color_col`, `palette_file`, `keep_types`, `vertical_order`, `arrangement`,
  `output_prefix`, `title`, `make_labeled`, and `make_unlabeled` configure the
  data-loss diagram.
- `metadata_plots`: `enabled`, `metadata`, `sub_dir`, `sample_col`, `type_col`,
  `color_col`, `palette_file`, `subtraction_group_col`, `subtraction_groups`,
  `keep_types`, `group_order`, `include_rank`, `run_micro`, and `run_mito`.
  Nested `group_normalization.enabled`, `columns`, `pattern`, `replacement`,
  and `preserve_source` normalize group labels while retaining provenance.
- `collectors_curve`: `enabled`, sample/group/color fields, `group_order`,
  `permutations`, `seed`, `out_prefix`, `title`, `formats`, `xpad`, `max_cols`,
  `show_perms`, and `presence_threshold`.
- `plot_upset`: `enabled`, `sub_dir`, `domain`, `taxonomy_path`, sample/group/
  color fields, `group_order`, `subset_groups`, `skip_venn`, `raw_only`,
  `final_only`, `formats`, and `font_size`.
- `bubbleplotter`: `enabled`, `output_prefix`, count/sample/group/color fields,
  `group1_order`, `formats`, `figsize`, `bubble_scale`, and `no_auto_size`.
- `umap_clustering`: the common output/count/group fields plus `normalize`,
  `transform`, `no_scale`, `n_neighbors`, `min_dist`, `umap_metric`,
  `min_cluster_size`, `min_samples`, and `hdbscan_metric`.

## Batch correction and outliers

### `batch_correction`

`enabled`, `output_dir`, `sample_id_col`, `batch_col`, `asv_orientation`,
`biological_covariates`, `biological_color_col`, `color_palette_col`, and
`biological_palettes` define inputs and biological structure. ConQuR controls
are `conqur_mode`, `conqur_num_core`, `conqur_batch_ref`,
`conqur_logistic_lasso`, `conqur_quantile_type`, `conqur_simple_match`,
`conqur_lambda_quantile`, `conqur_interplt`, `conqur_delta`, and
`conqur_auto_install`. `correction_policy` selects raw, corrected, or `auto`;
auto gates are `auto_min_sample_rho`, `auto_min_bray_rho`,
`auto_max_batch_eta_ratio`, `auto_min_batch_eta_drop`, and
`auto_min_bio_eta_ratio`. Diagnostic clustering uses `umap_neighbors`,
`umap_min_dist`, `hdbscan_min_cluster_size`, `hdbscan_min_samples`,
`hdbscan_selection_method`, `optimize_clustering`, `target_clusters`,
`n_features_plot`, and `random_state`.

### `outlier_detection`

`enabled`, `output_dir`, `sample_col`, `group_cols`, `transform`,
`asv_orientation`, `pre_transformed`, and `scale` define data. `use_iso`,
`use_svm`, and `use_hdb` select detectors; `vote_threshold` is required
agreement. Isolation Forest uses `iso_contamination`, `iso_estimators`, and
`iso_random_state`; SVM uses `svm_kernel`, `svm_gamma`, and `svm_nu`; HDBSCAN
uses `hdbscan_min_cluster_size`, `hdbscan_min_samples`, and `hdbscan_metric`.

## Ecological and association analyses

### `diversity`

`enabled`, `output_dir`, `mito_output_dir`, `mito_input`, sample/group/color/
block fields, `exclude_groups`, `group_order`, `run_mito`, UMAP settings,
`permanova_perms`, `random_state`, and `verbose` configure the general branch.
Nested `patient_aware` fields enable the paired design and name sample, patient,
case, type, and lung-side fields; `sample_types`, contralateral controls,
`transform`, `permutations`, `seed`, and `require_complete_types` define its
cohort and test.

### `indicspecies`

`enabled`, sample/color fields, `group_cols`, palettes/orders, focus labels,
`block_col`, `perms`, `seed`, `q_threshold`, and `min_n` control tests.
`stratified.enabled` and each `analyses` entry (`within_col`, `group_col`,
`levels`) request nested tests. Plot fields are `focus_group1_label`,
`label_focused_asvs`, `plot_enabled`, `plot_pairs_mode`, `plot_output_dir`,
`aligned_plot_enabled`, `aligned_plot_output_dir`, `aligned_alpha`,
`aligned_min_stat`, `aligned_top_n`, `venn`, and `taxonomy`. Legacy
`group1_*`/`group2_*` fields remain supported.

### `voc_correlation` and `measurement_association`

- `voc_correlation`: `enabled`, metadata/patient/case/type fields,
  `sample_types`, required `voc_table`, `output_dir`, `voc_sample_col`,
  `sample_id_mode`, `use_legacy_voc_subset`, `correlation_direction`
  (`positive`, `negative`, or `both`), and optional `voc_columns`.
- `measurement_association`: `enabled`, `output_dir`, optional
  `measurement_table`, sample/ASV IDs, paired metadata/measurement join lists,
  `measurement_cols`, `exclude_cols`, group/palette, `max_asvs`, `min_total`,
  `min_prevalence`, `top_correlations`, direction, `ordination_methods`
  (`cca,rda,dbrda`), `permutations`, `top_vectors`, and `formats`.

### `grouping_diagnostics`

General fields are `enabled`, output/sample/group fields, `baseline_group`,
`primary_group`, palettes/orders, `distance_metrics`, `transform`,
`permutations`, `random_state`, and `formats`. `soft_labeling` uses `enabled`,
`k`, `target_cols`, `exclude_labels`, `min_class_samples`, `distance_quantile`,
`apply_downstream`, `target_col`, `min_confidence`,
`min_neighbor_agreement`, and `min_cv_balanced_accuracy`. Nested `power` uses
`enabled`, `sample_sizes`, `simulations`, `permutations`, `alpha`, and
`min_groups`. Observed labels are not overwritten; excluded labels are neither
training classes nor predictions.

## Study-design analyses

- `power_analysis`: `enabled`, output and sample/patient/case/type fields,
  `sample_sizes_cancer`, `sample_sizes_stype`, `n_simulations`, `n_perm`,
  `alpha`, `seed`, `skip_estimate`, `skip_plot`, `transform`, and contralateral
  controls.
- `taxonomy_patient_aware`: `enabled`, output and cohort fields, `count_col`,
  `tax_levels`, `sample_types`, `min_prevalence`, contralateral/lung-side
  controls, `skip_omnibus`, `transform`, `alpha`, and `top_n`.
- `lung_status_analysis`: `enabled`, output and sample/type/case/patient fields,
  site/side/status fields, `status_a_value`, `status_b_value`,
  `reference_status_value`, `permutations`, and `seed`.

Generalized aliases are `group_power_analysis` for `power_analysis`,
`taxonomy_group_association` for `taxonomy_patient_aware`, and
`paired_group_contrast` for `lung_status_analysis`. Use only one name for each
module in one configuration.

## Clustermaps and networks

### `clustermaps`

Fields cover enable/output/mitochondrial/ISA paths; sample, sample-code, ASV,
count, and up to four grouping columns; orders, exclusions and palettes;
`ranks`, per-rank `topN`, ISA statistic/significance fields and threshold;
`formats`, `figwidth`, `row_height`, `min_height`, `max_height`, and
`mito_sample_mode`.

### `spieceasi`

`enabled`, output/prefix, and `network_enabled` control the branch. Inference
uses `transpose`, abundance/prevalence/zero-variance filters, `method`,
`lambda_min_ratio`, `nlambda`, `rep_num`, `thresh`, `pulsar_criterion`,
`ncores`, and `seed`. Module detection uses `modules_enabled`,
`module_methods`, `module_primary_method`, `module_resolutions`, `module_reps`,
consensus/stability/min-size gates, best-only options, and ISA filters. Graph
inputs/overlays include module/position/node paths, `network_modes`, metadata,
group palettes/orders/focus, and ISA groups. Rendering uses edge, layout,
degree, ISA, and abundance size/scale fields plus `keep_negative` and
`force_filter`, `force_spieceasi`, and `force_graphs`.

`pulsar_criterion: stars` requests ordinary StARS. The ASPIRE compatibility
value `bstars` requests bounded StARS by passing `criterion=stars` with lower
and upper StARS bounds enabled; `bstars` itself is never passed to SPIEC-EASI
as an unsupported criterion.

### MAG and summary modules

- `asv_mag_link`: `enabled`, optional `master_tsv`, `genome_qc_dir` or
  `genome_qc_dirs`, `id_token_indexes`, `barrnap_dir` or `genome_fasta_dir`,
  `output_dir`, `threads`, `min_pident`, `min_qcov`, `top_n`, and `plot_top_n`.
- `asv_mag_network`: `enabled`, output/prefix, `graph_variant`, link identity/
  coverage gates, taxonomy sources, `mag_id_mode`, optional `mag_abundance`,
  its format and genome/sample/value fields, `min_shared_samples`,
  `abundance_transform`, `functional_module_min_fraction`, and
  `functional_annotations`.
- `master_summary`: `enabled`, `output_dir`, source-module directories,
  `max_direct_cols`, and the filename `whitelist` eligible for integration.

## `environments`

Every value is a process-specific Conda YAML. Keys are `main`, `sina`,
`taxonomy`, `mitomaster`, `mito_checker`, `filter_counts`, `general_stats`,
`sankey`, `plot_metadata`, `batch_correction`, `outlier_checker`,
`collectors_curve`, `plot_upset`, `bubbleplotter`, `umap_clustering`,
`diversity`, `indicspecies`, `voc_correlation`, `measurement_association`,
`grouping_diagnostics`, `group_label_augmentation`, `clustermaps`,
`power_analysis`, `taxonomy_patient_aware`, `lung_status_analysis`,
`spieceasi`, `network_modules`, `network`, `master_summary`, `asv_mag_link`, and
`asv_mag_network`. Keep committed values for ordinary runs. Overrides are for
dependency development and change the reproducibility environment.

## Remaining field glossary

The following less-common fields are listed explicitly so configuration review
does not require searching the workflow source:

- Taxonomy cache names: `ref_taxonomy_url`, `ref_taxonomy_filename`,
  `ref_sequences_url`, and `ref_sequences_filename`.
- General join and identifier fields: `metadata_sample_col`, `asv_id_col`,
  `metadata_join_cols`, `measurement_join_cols`, and `sample_code_col`.
- Patient/lung design fields: `cancer_site_col`, `contralateral_col`,
  `contralateral_sample_types`, `contralateral_value`,
  `exclude_contralateral_in_cancer`, `keep_contralateral_in_cancer`,
  `lung_code_col`, `lung_side_col`, `lung_status_col`, `tumor_side_col`, and
  `healthy_col`.
- Clustermap fields: `isa_file`, `exclude_group1`, `group2_col`,
  `group3_col`, `group4_col`, `group1_palette`, `group2_palette`,
  `group3_palette`, `group4_palette`, `group2_order`, `isa_min_stat`,
  `isa_significance_cols`, and `isa_stat_cols`.
- Shared grouping maps: `group_palettes`, `group_orders`, and `focus_labels`.
- Master-summary source locations: `clustermaps_dir`, `indicspecies_dir`,
  `spieceasi_dir`, and `asv_mag_dir`.
- Network module fields: `module_seed`, `module_consensus_threshold`,
  `module_best_only`, `module_best_min_size`, `module_best_min_stability`,
  `module_isa_only`, `module_color_by_isa`, `module_isa_source`,
  `module_isa_min_stat`, and `module_isa_max_q`.
- Optional network input/overlay fields: `modules_sub`, `modules_all`,
  `graph_pos_sub`, `graph_pos_all`, `node_features`, and `isa_overlay_groups`.
- Network filtering/rendering fields: `min_rel_abund`, `remove_zero_var`,
  `edge_threshold`, `edge_width_scale`, `layout_iters`, `layout_seed`,
  `layout_scale`, `degree_scale`, `degree_size_mode`, `degree_min_area`,
  `isa_scale`, `abundance_size_mode`, `abundance_reference`,
  `abundance_reference_area`, `abundance_min_area`, `abundance_max_area`, and
  `abundance_scale_power`.
- MAG taxonomy and abundance schema fields: `asv_taxonomy_source`,
  `mag_taxonomy_source`, `mag_abundance_format`, `mag_abundance_genome_col`,
  `mag_abundance_sample_col`, and `mag_abundance_value_col`.

Path/column fields name inputs or exact case-sensitive columns. Boolean fields
toggle the behavior named by the key. Size, scale, threshold, and iteration
fields are numeric and should retain template values unless the corresponding
analysis design is being deliberately changed.

## Production preflight checklist

- All active paths exist and reference compatible files.
- Manifest IDs are unique, pairing is correct, and metadata IDs match exactly.
- Every active group, color, patient, lung-side, and measurement field exists.
- Palette labels and orders cover observed metadata values.
- Trimming, merge, length, and error parameters match the assay.
- Optional modules without their conditional inputs are disabled.
- A representative run has been reviewed for read retention, filtering,
  taxonomy, sample accounting, and metadata joins before the full cohort run.

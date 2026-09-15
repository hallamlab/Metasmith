# ASPIRE transform contracts

## Purpose & Contents

This file records, for each row of `src/metasmith_libraries/transforms/aspire/_generate.py`, what its upstream Nextflow process reads and writes, and whether the row's declared requirements and products agree. It holds one verdict per row and the reason for it. It does not describe the transforms' protocols, because every body is still a stub.

- `match`: the declared contract equals the real one, after the port conventions in the `_generate.py` docstring.
- `fixed`: the row was wrong, and the table now carries the correction named here.
- `open`: a real discrepancy that a table edit cannot close yet. The reason names what is missing.

Line numbers refer to `research/aspire/upstream/ASPIRE/asv_pipeline.nf` unless a script is named. The fold record is `research/kbase/curation/r4/aspire_topology.md`.

## Tally

46 rows: 29 `match`, 5 `fixed`, 12 `open`.

## Read spine (rows 1-15)

| row | upstream | reads | writes | verdict | reason |
|---|---|---|---|---|---|
| fastp_qc | FASTP_QC@3121 | raw R1/R2 pair (472-480, 2607) | trimmed R1/R2, fastp json/html | match | The single-end branch (3167-3175) is not ported. |
| merge_and_filter_reads | FILTER_READS@3221 + MERGE_READS@3179 | trimmed R1/R2 (2609-2610) | filtered fasta | match | |
| concat_fastas | CONCAT_FASTAS@3274 + RELABEL_FILTERED@3248 | every sample's filtered fasta (2612-2617) | concatenated fasta for derep and counts, byte-identical (3294) | match | The fold assumes `concat.relabel`, which defaults on (489). |
| denoise | DENOISE@3367 + DEREPLICATE@3298 | concatenated fasta (2621) | UNOISE centroids | match | |
| chimera_check | CHIMERA_CHECK@3393 | centroids (2623) | non-chimeric centroids | match | |
| create_count_matrix | CREATE_COUNT_MATRIX@3417 | concatenated fasta, non-chimeric centroids (2625) | ASV counts, ASV fasta | match | |
| filter_table | FILTER_TABLE@3444 | ASV counts and fasta (2628) | filtered counts and fasta | match | |
| sina_trim | SINA_TRIM@3322 | filtered ASV fasta (2630-2631), SINA ARB reference by path (719-735, 3346) | trimmed, aligned, log, v-regions | match | The row note cites the wiring at 3631. The wiring is at 2630-2631. |
| taxonomy | TAXONOMY@3470 | trimmed fasta (2632), reference sequences and taxonomy by path (789-818, 3502-3503) | taxonomy table, uppercase fasta, stats | open | Both references are QIIME2 `.qza` artifacts (`qiime_vs_classifier.py:16-17`). `amplicon::silva_db` is a raw SILVA fasta directory and `silva_ref_taxonomy` is typed `tsv`. The edge is right and the format is not. |
| mitomaster | MITOMASTER@3544 + PREPARE_BLAST_DATABASES@3511 | filtered counts and fasta (2640), mito and contaminant FASTA or prebuilt BLAST db (831-835) | MitoMaster table, mito and contaminant blast6 | match | `mitomaster.py:24-44` calls the MitoMaster web service. That is a runtime network dependency, not a type. |
| mito_decontam | MITO_DECONTAM@3595 | MitoMaster tuple, taxonomy (2641) | non-target table, optional summaries and plots | match | |
| filter_counts | FILTER_COUNTS@3637 | filtered counts, taxonomy, non-target table (2643), optional `filter_counts.metadata` by path (862, 3660) | filtered, micro, mito, decon counts | open | The optional metadata turns on group-size filtering (`filter_nontarget.py:509-523`). It is a config-gated input with no policy token. |
| general_stats | GENERAL_STATS@3755 | concatenated fasta as a barrier (2651). By absolute path: raw reads (757-763), fastp reads (764-770), filtered fasta (771-773) | fastq, fastp, filtered and concat stats | open | The real inputs are per-sample products of three upstream rows. Declaring them needs a fan-in over several per-sample slots, which no row does yet. |
| sankey | SANKEY@3687 | fastq and filtered stats, raw, decon and micro counts (2777-2783). By path: `sankeyMetadataPath` (918, 3717), `manifestPath` (3718) | read-fate renderings (948) | fixed | Added `aspire::sample_metadata`, which `sankey_builder.py:515` reads unconditionally. The sample manifest (`--sample-manifest`) stays untyped. |
| sankey_absent | none | none | placeholder in MASTER_SUMMARY's sankey slot (2790) | match | |

## Metadata and analyses (rows 16-30)

| row | upstream | reads | writes | verdict | reason |
|---|---|---|---|---|---|
| plot_metadata | PLOT_METADATA@3793 | fastq stats, FILTER_COUNTS `filtered_counts`, filtered mito, taxonomy (2663-2668), study metadata by path (980) | micro and mito metadata, long and final ASV tables | fixed | The `micro` slot now requires `aspire::counts_filtered`. The .nf wires `filter_counts_stage.filtered_counts` there (2665), not `filtered_micro`. |
| grouping_diagnostics | GROUPING_DIAGNOSTICS@4929 | micro metadata (2860), final micro ASV table (2861) | diagnostics, soft assignments, validation, summary | match | The rebinding at 2915 comes after the call and is dead. |
| group_label_augmentation | GROUP_LABEL_AUGMENTATION@4986 | micro metadata, ASV meta, soft assignments, validation summary (2867-2870) | augmented metadata, augmented ASV meta, audit | match | |
| augmentation_passthrough | none (2819-2841) | micro metadata, ASV meta | the same tables | match | |
| asv_batch_correction | ASV_BATCH_CORRECTION@4101 + ASV_META_FROM_CORRECTED@4246 | analysis metadata (2903), staged ASV meta (2904), final micro ASV table (2905) | 12 emits; downstream counts are `asv_selected_counts_int` (2908-2919) | match | |
| batch_correction_passthrough | none (2843-2855) | final micro ASV table, staged ASV meta | the same tables | match | |
| plot_upset | PLOT_UPSET@3927 | the staged metadata is unused. By `--data-dir`: micro target and final ASV tables, pre-augmentation metadata, taxonomy, two `_raw` copies (3948-3953) | UpSet renderings | open | The one declared edge is only a barrier. The real inputs include `_raw` files with no type, and a domain switch with no token. |
| bubbleplotter | BUBBLEPLOTTER@4018 | analysis ASV meta (2938) | bubble plots | match | |
| umap_clustering | UMAP_CLUSTERING@4055 | analysis ASV meta only (2941, 4077) | UMAP renderings | open | The r4 lift added an `amplicon::asv_table` requirement the process never reads. Dropping it revises a recorded lift decision. |
| outlier_checker | OUTLIER_CHECKER@4316 | the staged CLR input is unused. By path: `asv_clr_after_correction.tsv` (4336, 4344), `metadata/<name>` (4335) | outlier diagnostics | fixed | The `clr` slot now requires `aspire::asv_clr_after`. The script reads the post-correction CLR, not the selected one. |
| collectors_curve | COLLECTORS_CURVE@4364 | analysis counts (2844, 2908), analysis metadata (2875) | collector's curves | match | |
| diversity_analysis | DIVERSITY_ANALYSIS@4399 | analysis metadata and counts (2956-2958). By path, when `run_mito` (default on, 1290): `mito/ASVs/ASV_target.mito.tsv` (1274) | diversity results | open | Adding `counts_mito` would tie the lifted row back to the ASPIRE lane, because only `filter_counts` produces it. |
| indicspecies | INDICSPECIES@4518 + INDICSPECIES_PLOTS@4608 + INDICSPECIES_ALIGNED_PLOTS@4792 | metadata, counts (2964-2967). The plot fold reads taxonomy by path if it exists (1609, 4780) | summaries, results, tables, plots | open | The taxonomy read is optional and guarded by a file-exists check, so it is not a hard requirement. |
| indicspecies_absent | none (2660, 2661, 2983, 2984, 3036) | none | placeholder tables and group-1 summary | match | |
| voc_correlation | VOC_CORRELATION@4827 | ASV meta, counts, indicator tables (2984-2913). By path: `vocCorrelationVocTablePath` (1641, 4853) | VOC correlation results | open | The VOC study table is a required script input with no type. |

## Networks and summary (rows 31-46)

| row | upstream | reads | writes | verdict | reason |
|---|---|---|---|---|---|
| measurement_association | MEASUREMENT_ASSOCIATION@4875 | ASV meta, metadata, counts (2995-2999). By path: optional measurement table (1681, 4891) | association results | open | The measurement table is a study file with no type. |
| group_power_analysis | GROUP_POWER_ANALYSIS@5030 | ASV meta, counts, indicspecies barrier (3009-3014, 2982). Indicator dir by path (5061) | power analysis | match | The master-summary script run over clustermaps and SpiecEasi dirs (5056-5064) has no barrier. It is a race in the .nf, not a contract. |
| taxonomy_group_association | TAXONOMY_GROUP_ASSOCIATION@5113 | ASV meta, counts (3016-3020) | association results | match | Same unbarriered directory reads (5138-5146). |
| paired_group_contrast | PAIRED_GROUP_CONTRAST@5234 | ASV meta, counts (3022-3026) | paired contrast results | match | Same unbarriered directory reads (5256-5264). |
| clustermaps | CLUSTERMAPS@5324 | ASV meta, metadata, indicspecies barrier (3002-3007), indicator summaries by glob (5381-5399). By path, `run_mito` default on (1851): `mito/ASVs/ASV_target.mito.tsv` (1824) | clustermaps, mito clustermaps | fixed | Added `aspire::counts_mito`, which `filter_counts` produces. The `isa_file` override stays a runtime option. |
| spieceasi | SPIECEASI@5452 | counts, indicspecies group-1 summary or placeholder (2695-2698) | graph all, graph thresholded, node features | match | `spieceasiAllPosOnly` (2702-2704) is a runtime parameter. |
| spieceasi_external | none (2707-2715) | three graph files by config path (1902-1904) | the same three channels | match | |
| network_modules | NETWORK_MODULES@5522 | graph all, graph thresholded (2719-2722) | modules sub, all, summary, runs | match | |
| network_modules_absent | none (2726-2729) | on-disk module files if present (2085-2086) | modules sub, all | match | |
| asv_mag_link | ASV_MAG_LINK@5821 | filtered ASV fasta (2679-2681). By path: MAG master TSV, barrnap dir, genome FASTA and QC dirs (2115-2120, 5835-5847) | pairing table and link results | open | The MAG collection is a required input with no type. The linker exits without it (`asv_mag_barrnap_linker.py:1032`). |
| asv_mag_link_absent | none (2735, 2751, 2764, 2791) | none | placeholder pairing and results | match | |
| graph_network | GRAPH_NETWORK@5579 | three graph files, counts, metadata, taxonomy, indicator tables, module tables, link barrier (2736-2747). Pairing by path (5606) | network renderings, best-stats tables | match | |
| graph_network_absent | none (2763, 2789) | none | placeholder network outputs | match | |
| asv_mag_network | ASV_MAG_NETWORK@5654 | graph, node features, taxonomy, counts, link barrier (2753-2759). By path under the link dir: pairing, genome summary, 16S reference catalog (5682-5686) | MAG-annotated network renderings | fixed | Added `aspire::asv_mag_outputs`, because the genome summary and reference catalog live in the link directory, not the pairing file. MAG abundance and functional annotations (2170, 5672-5673) stay untyped. |
| module_mag_anchors | MODULE_MAG_ANCHORS@5705 | modules all, node features, taxonomy, counts, metadata, link and network barriers (2765-2773). By path: pairing (5741), best-stats (5746) | anchor tables, module scores, heatmaps | match | |
| master_summary | MASTER_SUMMARY@5763 | ASV meta, counts, network, sankey and link barriers (2785-2800). Whitelisted rglob over clustermaps, indicspecies, SpiecEasi and link dirs (`build_master_asv_summary.py:219-240`) | five master tables | open | The sankey edge is a barrier the script never reads. The scan also reads indicator tables, clustermaps and module anchors, which are undeclared. Declaring clustermaps would force it to run, because it has no off-arm. |

## Cross-row findings left open

- **Lifted rows read raw counts inside a run.** `aspire::analysis_counts` does not carry `amplicon::asv_table`'s properties, so `umap_clustering`, `diversity_analysis` and `indicspecies` bind `create_count_matrix`'s pre-filter table. The .nf feeds them the final micro or selected pseudocount counts (2845-2846, 2909-2910). This is a type-graph decision, not a row edit.
- **Off-arm lift is incomplete.** `spieceasi_external` and `graph_network_absent` stay on `aspire::run` while their on-arms moved to `amplicon::survey`.
- **A gate is not modelled.** `networkEnabled` requires indicspecies (1901). The port lets `graph_network` run over the `indicspecies_absent` placeholder.
- **Possible upstream option mismatch.** INDICSPECIES passes `--asv` and `--meta` (4552-4553), and `run_indicspecies.R` requires `data-wide`, `data-long` and `outdir`. Check it before writing a real protocol.

## Mock dataset against the leaf givens

The mock dataset (Zenodo 21358300, DECOI `mock_airway_chemistry`) supplies these leaves:

| leaf given | mock file | status |
|---|---|---|
| `sequences::zipped_forward_short_reads` / `zipped_reverse_short_reads` | `fastq/<sample>_R1.fastq.gz`, `_R2.fastq.gz` via `fastq_manifest.tsv` | present, pair counts verified against `fastq_validation.tsv` |
| `aspire::sample_metadata` | `sample_metadata.tsv` (`sample_id`, `Participant_ID`, `Case`, `Type_Group`, `lung_status`, `batch`, ...) | present |
| `aspire::mito_reference_source` | `references/mitochondria.fasta` | present |
| `aspire::contaminant_reference_source` | `references/contaminants.fasta` | present |
| `aspire::sina_arb_reference` | none | absent |
| `aspire::silva_ref_taxonomy` | none | absent |
| `amplicon::silva_db` | none; `downloadSilvaDB` can plan it | absent |

The mock also ships ground-truth tables (`asv_counts*.tsv`, `asv_taxonomy.tsv`, `ground_truth_*.tsv`) that no transform reads. They are the grading key for a future real run.

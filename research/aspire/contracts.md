# ASPIRE transform contracts

## Purpose & Contents

This file records, for each row of `src/metasmith_libraries/transforms/aspire/_generate.py`, what its upstream Nextflow process reads and writes, and whether the row's declared requirements and products agree. It holds one verdict per row and the reason for it. It does not describe the transforms' protocols. `docs/metasmith_libraries/ASPIRE_PORT.md` says which rows have real bodies and where they differ from upstream.

- `match`: the declared contract equals the real one, after the port conventions in the `_generate.py` docstring.
- `fixed`: the row was wrong, and the table now carries the correction named here.
- `open`: a real discrepancy that a table edit cannot close yet. The reason names what is missing.

Line numbers refer to `research/aspire/upstream/ASPIRE/asv_pipeline.nf` unless a script is named. The r4 fold record is `research/kbase/curation/r4/aspire_topology.md`. The module-1 folds that followed it are in the read-spine table.

## Tally

34 rows: 21 `match`, 12 `fixed`, 1 `open`. The four lung-study analyses are not ported, so they have no row.

## Read spine

Module 1 folds eighteen upstream processes into ten rows. A sample is a `sequences::read_metadata` under `aspire::study_metadata`, and its reads reach `sequences::short_reads` either through `logistics/interleave_zipped_short_reads` (paired) or as a given `short_reads_se` (single-end).

| row | upstream | reads | writes | verdict | reason |
|---|---|---|---|---|---|
| fastp_qc | FASTP_QC@3121 | raw reads of either parity (472-480, 2607), params | QC reads, fastp json/html | match | The parity branch (3167-3175) moves to run time, read off `sequences::read_metadata`. |
| merge_and_filter_reads | FILTER_READS@3221 + MERGE_READS@3179 | QC reads (2609-2610), params | filtered fasta, per-sample read counts | match | Single-end reads skip the merge. The count file replaces the absolute-path read in GENERAL_STATS (771-773). |
| denoise | DENOISE@3367 + RELABEL_FILTERED@3248 + CONCAT_FASTAS@3274 + DEREPLICATE@3298 + CHIMERA_CHECK@3393 + CREATE_COUNT_MATRIX@3417 | every sample's filtered fasta (2612-2625), params | `amplicon::asv_table`, `amplicon::asv_seqs` | match | The study fan-in. The fold assumes `concat.relabel`, which defaults on (489). |
| filter_table | FILTER_TABLE@3444 | ASV counts and fasta (2628), params | filtered counts and fasta | match | |
| sina_trim | SINA_TRIM@3322 | filtered ASV fasta (2630-2631), SINA ARB reference by path (719-735, 3346), now from the `amplicon::silva_db` bundle | trimmed, aligned, log, v-regions | match | |
| taxonomy | TAXONOMY@3470 | trimmed fasta (2632), reference sequences and taxonomy by path (789-818, 3502-3503) | taxonomy table, uppercase fasta, stats | fixed | Both references are QIIME2 `.qza` artifacts (`qiime_vs_classifier.py:16-17`), now carried in the one `amplicon::silva_db` bundle with SINA's ARB file. The bundle also holds the NB classifier, which is the retired placeholder lane's method, not upstream's. |
| mitomaster | MITOMASTER@3544 + PREPARE_BLAST_DATABASES@3511 | filtered counts and fasta (2640), mito and contaminant FASTA or prebuilt BLAST db (831-835) | MitoMaster table, mito and contaminant blast6 | match | The port keeps the BLAST screen only. MitoMaster posts every ASV to mitomap.org, and compute nodes have no internet, so the MitoMaster table is written empty. |
| curate | MITO_DECONTAM@3595 + FILTER_COUNTS@3637 | filtered counts and fasta, taxonomy, MitoMaster tuple (2641-2643), params | clean counts, removed counts with a reason column, mito summaries and plots | open | The four partitioned tables become two. The group-size sample drop (`filter_nontarget.py:509-523`, 862, 3660) is not ported. `plot_metadata` blanks a rare label value instead. Negative controls are not modelled. |
| read_accounting | GENERAL_STATS@3755 | per-sample fastp json and read counts, raw ASV table, clean and removed counts | `aspire::read_fate` | fixed | Upstream reads raw reads, fastp reads and filtered fasta by absolute path (757-773) behind a barrier (2651). The row is now a fan-in over the per-sample products. |
| sankey | SANKEY@3687 | read fate, removed counts (2777-2783), `sankeyMetadataPath` (918, 3717) | read-fate renderings (948) | fixed | The five stats and count tables collapse into `read_fate` and `counts_removed`. The sample manifest (`--sample-manifest`, 3718) stays untyped. |

## Metadata and analyses

Augmentation (GROUP_LABEL_AUGMENTATION@4986), batch correction (ASV_BATCH_CORRECTION@4101 + ASV_META_FROM_CORRECTED@4246), their two passthroughs and OUTLIER_CHECKER@4316 are not ported. Both stages default off and sit outside the reads-to-ASV pipeline, and the outlier checker reads only batch correction's CLR table.

`sankey` and `indicspecies` have no off arm. Each runs whenever a target needs its output. Upstream fills their consumers' slots with zero-row placeholders when they are disabled (2790; 2660, 2661, 2983, 2984, 3036).

| row | upstream | reads | writes | verdict | reason |
|---|---|---|---|---|---|
| plot_metadata | PLOT_METADATA@3793 | read fate, clean and removed counts, taxonomy (2663-2668), study metadata by path (980) | analysis metadata, ASV meta and counts, mito metadata and tables | fixed | With no augmentation or correction stage after it, this row emits the analysis tables directly. |
| grouping_diagnostics | GROUPING_DIAGNOSTICS@4929 | analysis metadata (2860), analysis counts (2861) | diagnostics, soft assignments, validation, summary | match | The rebinding at 2915 comes after the call and is dead. |
| plot_upset | PLOT_UPSET@3927 | the staged metadata is unused. By `--data-dir`: micro target and final ASV tables, pre-augmentation metadata, taxonomy, two `_raw` copies (3948-3953) | UpSet renderings | fixed | Requires `counts_clean` (the target table), `analysis_counts` (the final one) and the taxonomy, passed by path. The `_raw` pass reads tables from before control subtraction, which the port never makes, so it is not run. The domain stays `micro`. |
| bubbleplotter | BUBBLEPLOTTER@4018 | analysis ASV meta (2938) | bubble plots | match | |
| umap_clustering | UMAP_CLUSTERING@4055 | analysis ASV meta only (2941, 4077) | UMAP renderings | fixed | The r4 lift's `amplicon::asv_table` requirement, which the process never reads, is dropped. |
| collectors_curve | COLLECTORS_CURVE@4364 | analysis counts (2844, 2908), analysis metadata (2875) | collector's curves | match | |
| diversity_analysis | DIVERSITY_ANALYSIS@4399 | analysis metadata and counts (2956-2958). By path, when `run_mito` (default on, 1290): `mito/ASVs/ASV_target.mito.tsv` (1274) | diversity results | fixed | The `run_mito` branch is its own row, so this one stays reachable from any count table. |
| diversity_mito | DIVERSITY_ANALYSIS@4399, `run_mito` | `counts_removed`, whose mitochondrial rows are the table upstream reads (1274), analysis metadata | mitochondrial diversity results | match | |
| indicspecies | INDICSPECIES@4518 + INDICSPECIES_PLOTS@4608 + INDICSPECIES_ALIGNED_PLOTS@4792 | metadata, counts (2964-2967). The plot fold reads taxonomy by path if it exists (1609, 4780) | summaries, results, tables, plots | match | The taxonomy read is optional and guarded by a file-exists check, so it is not a requirement. A label with more than 8 levels is tested in level combinations of at most 3. |
| measurement_association | MEASUREMENT_ASSOCIATION@4875 | ASV meta, metadata, counts (2995-2999). By path: optional measurement table (1681, 4891) | association results | match | The measurement table is optional, so it is not a requirement. |
| clustermaps | CLUSTERMAPS@5324 | ASV meta, metadata, indicspecies barrier (3002-3007), indicator summaries by glob (5381-5399). By path, `run_mito` default on (1851): `mito/ASVs/ASV_target.mito.tsv` (1824) | clustermaps, mito clustermaps | fixed | Requires `aspire::counts_removed`, whose mito rows are the table upstream reads. The `isa_file` override stays a runtime option. |

## Networks and summary

| row | upstream | reads | writes | verdict | reason |
|---|---|---|---|---|---|
| spieceasi | SPIECEASI@5452 | counts, indicspecies group-1 summary (2695-2698) | graph all, graph thresholded, node features | fixed | Upstream force-keeps the first grouping's indicators. The row reads `aspire::indicspecies_results` and keeps the ASVs significant for any label. `spieceasiAllPosOnly` (2702-2704) is a runtime parameter. |
| spieceasi_external | none (2707-2715) | three graph files by config path (1902-1904) | the same three channels | match | |
| network_modules | NETWORK_MODULES@5522 | graph all, graph thresholded (2719-2722) | modules sub, all, summary, runs | match | |
| network_modules_absent | none (2726-2729) | on-disk module files if present (2085-2086) | modules sub, all | match | |
| collect_mags | none: upstream takes a genome QC pipeline's output directory | a dedup run's assembly, cluster table, quality bins and their barrnap GFFs | `aspire::mag_collection` | match | Lays the 95% centroid bins out as the linker's `--genome-qc-dir`. |
| asv_mag_link | ASV_MAG_LINK@5821 | filtered ASV fasta (2679-2681). By path: MAG master TSV, barrnap dir, genome FASTA and QC dirs (2115-2120, 5835-5847) | pairing table and link results | fixed | Requires `aspire::mag_collection`, the `--genome-qc-dir` layout that `collect_mags` builds. |
| asv_mag_link_absent | none (2735, 2751, 2764, 2791) | none | placeholder pairing and results | match | |
| graph_network | GRAPH_NETWORK@5579 | three graph files, counts, metadata, taxonomy, indicator tables, module tables, link barrier (2736-2747). Pairing by path (5606) | network renderings, best-stats tables | match | Upstream's natural sort raises on a label whose levels mix a leading number and a word. The port's copy tags each part. |
| graph_network_absent | none (2763, 2789) | none | placeholder network outputs | match | |
| asv_mag_network | ASV_MAG_NETWORK@5654 | graph, node features, taxonomy, counts, link barrier (2753-2759). By path under the link dir: pairing, genome summary, 16S reference catalog (5682-5686) | MAG-annotated network renderings | fixed | Added `aspire::asv_mag_outputs`, because the genome summary and reference catalog live in the link directory, not the pairing file. MAG abundance and functional annotations (2170, 5672-5673) stay untyped. |
| module_mag_anchors | MODULE_MAG_ANCHORS@5705 | modules all, node features, taxonomy, counts, metadata, link and network barriers (2765-2773). By path: pairing (5741), best-stats (5746) | anchor tables, module scores, heatmaps | match | Gated on `asv_mag_link_on`. |
| module_mag_anchors_absent | none (2791) | none | header-only anchor table | match | |
| master_summary | MASTER_SUMMARY@5763 | ASV meta, counts, network, sankey and link barriers (2785-2800). Whitelisted rglob over clustermaps, indicspecies, SpiecEasi and link dirs (`build_master_asv_summary.py:219-240`) | five master tables | fixed | Requires the clustermap, indicator, network, anchor and link outputs the scan reads, and drops the sankey barrier. The whitelist names every label's indicator tables. |

## Module-1 thresholds

Every module-1 row requires `aspire::params`, one YAML file registered under the study. `research/aspire/presets/` holds two presets with the same keys: `tool_defaults.yml` (fastp 0.24.0, vsearch 2.30.0, ASPIRE-only filters off) and `aspire.yml` (the `set1-2` study config). Each non-default value in `aspire.yml` carries its source in a comment, or says it has none.

**CAUTION:** a 0 in a preset means the filter is off. Upstream Groovy's `?:` treats a configured 0 as unset and substitutes the code default, so the two disagree wherever a config says 0.

Questions for Ryan:

1. `curate.abundance_threshold` is 0.5% in `set1-2`, 0.005% in the code default and the `si` config, and 0 in the mock config. The 100-fold spread looks like a percent-versus-fraction slip. Which one is intended?
2. `table_filter.min_sample_sum` (5000 reads) runs before `curate` removes mito and contaminant reads. Should sample depth be checked again after decontamination?
3. `set1-2` sets `min_asv_sum: 0`, which upstream silently runs as 0.01%. Is the 0.01% floor intended?

## Mock dataset against the leaf givens

The mock dataset (Zenodo 21358300, DECOI `mock_airway_chemistry`) supplies these leaves:

| leaf given | mock file | status |
|---|---|---|
| `sequences::zipped_forward_short_reads` / `zipped_reverse_short_reads` | `fastq/<sample>_R1.fastq.gz`, `_R2.fastq.gz` via `fastq_manifest.tsv` | present, pair counts verified against `fastq_validation.tsv` |
| `sequences::short_reads_se` | `fastq/<sample>_R1.fastq.gz` alone | present, as a stand-in for a single-end study |
| `aspire::study_metadata` | `sample_metadata.tsv`, reduced to the sample id and its label columns | present |
| `aspire::mito_reference_source` | `references/mitochondria.fasta` | present |
| `aspire::contaminant_reference_source` | `references/contaminants.fasta` | present |
| `amplicon::silva_db` | none; `downloadSilvaDB` plans the whole bundle | absent |

The mock also ships ground-truth tables (`asv_counts*.tsv`, `asv_taxonomy.tsv`, `ground_truth_*.tsv`) that no transform reads. They are the grading key for a future real run.

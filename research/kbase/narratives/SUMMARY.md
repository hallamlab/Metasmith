# Narrative DAGs

One record per public narrative in `dags.jsonl`. Edges come from an app cell's
job result or its output-name parameter meeting a later cell's input.

| | |
|---|---|
| narratives extracted | 3965 |
| with at least one app cell | 3664 |
| with more than one app cell | 3472 |
| with at least one edge | 2407 |
| total app cells | 72326 |
| total edges | 25716 |
| edges from a workspace reference | 13947 |
| edges from a name match only | 11769 |
| distinct apps used | 416 |
| distinct wired shapes (app sequences) | 1903 |
| apps used but absent from the catalog | 131 |

## Most repeated wired shapes

| copies | steps | app sequence |
|---|---|---|
| 40 | 6 | fastq_stats -> runFastQC -> run_trimmomatic -> run_SPAdes -> annotate_contigset -> run_kb_gtdbtk_classify_wf |
| 34 | 3 | reannotate_microbial_genome -> build_metabolic_model -> run_flux_balance_analysis |
| 26 | 25 | upload_web_file -> import_fastq_sra_as_reads_from_staging -> import_fastq_sra_as_reads_from_staging -> runFastQC -> runFastQC -> run_trimmomatic ... |
| 20 | 17 | import_samples -> import_amplicon -> run_classify -> run_TaxonomyAbundance -> run_metaMDS -> perform_variable_stats ... |
| 12 | 10 | load_single_end_reads_from_URL -> create_sample_set -> runFastQC -> align_reads_using_hisat2 -> run_stringtie -> compute_average_expression_matrix ... |
| 12 | 4 | reannotate_microbial_genome -> build_metabolic_model -> run_flux_balance_analysis -> run_flux_balance_analysis |
| 12 | 14 | load_paired_end_reads_from_URL -> runFastQC -> run_trimmomatic -> runFastQC -> run_kaiju -> run_gottcha2 ... |
| 11 | 7 | load_single_end_reads_from_URL -> create_sample_set -> runFastQC -> align_reads_using_hisat2 -> run_stringtie -> run_DESeq2 ... |
| 10 | 9 | runFastQC -> run_trimmomatic -> run_SPAdes -> run_QUAST_app -> run_checkM_lineage_wf -> annotate_contigs ... |
| 9 | 32 | runFastQC -> fastq_stats -> RQCFilter -> fastq_stats -> execReadLibraryPRINSEQ -> fastq_stats ... |
| 9 | 7 | reannotate_microbial_genome -> build_metabolic_models -> edit_media -> edit_media -> run_flux_balance_analysis -> run_flux_balance_analysis ... |
| 8 | 24 | import_fastq_sra_as_reads_from_staging -> import_fastq_sra_as_reads_from_staging -> runFastQC -> runFastQC -> run_trimmomatic -> run_trimmomatic ... |
| 8 | 54 | import_fastq_sra_as_reads_from_staging -> import_fastq_sra_as_reads_from_staging -> runFastQC -> runFastQC -> run_trimmomatic -> run_trimmomatic ... |
| 8 | 12 | upload_web_file -> runFastQC -> run_trimmomatic -> runFastQC -> run_kaiju -> run_megahit ... |
| 8 | 7 | fastq_stats -> runFastQC -> run_trimmomatic -> runFastQC -> run_SPAdes -> annotate_genome_assembly ... |

## Most used apps the catalog no longer carries

| uses | app |
|---|---|
| 58 | kb_PICRUSt2/run_picrust2_pipeline |
| 57 | kb_faprotax/faprotax |
| 54 | kb_escher/run_kb_pathway_view |
| 38 | kb_circos/run_kb_circos |
| 37 | kraken2/run_kraken2 |
| 35 | kb_model_analysis/create_heatmap_analysis_template |
| 35 | kb_model_analysis/run_kb_model_analysis |
| 29 | kb_jorg/run_kb_jorg |
| 28 | kb_model_analysis/import_fbamodel_attribute_mapping_from_staging |
| 20 | antismash1/run_antismash |
| 18 | kb_pickaxe/pickaxe |
| 17 | kb_reaction_gene_finder/find_genes_from_similar_reactions |

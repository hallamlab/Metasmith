# Curation round 1 -- mechanical

Reduces every public narrative to a set of distinct workflow shapes. A shape is
the sequence of apps that actually participate in an edge, so two narratives
running the same pipeline on different data are one shape.

## Funnel

| stage | count |
|---|---|
| narratives with a DAG record | 3965 |
| with at least one app cell | 3664 |
| with more than one app cell | 3472 |
| with at least one edge | 2407 |
| distinct app sequences | 1903 |
| distinct wired shapes | 1633 |
| shapes whose edges are all reference-derived | 716 |
| shapes using only apps still in the catalog | 1419 |
| shapes that are both | 623 |

`shapes.jsonl` is every shape. `candidates.jsonl` is the subset a later round
should judge: every edge derived from a workspace reference rather than a name
match, and every app still in the catalog so a transform exists for it.

## The 20 most copied shapes

| copies | steps | ref-edges only | apps |
|---|---|---|---|
| 62 | 3 | no | run_SPAdes -> annotate_contigset -> run_kb_gtdbtk_classify_wf |
| 45 | 2 | yes | build_metabolic_model -> run_flux_balance_analysis |
| 30 | 2 | yes | annotate_contigs -> reannotate_microbial_genome |
| 28 | 3 | yes | build_metabolic_model -> run_flux_balance_analysis -> run_flux_balance_analysis |
| 28 | 3 | no | run_SPAdes -> annotate_genome_assembly -> run_kb_gtdbtk_classify_wf |
| 26 | 3 | yes | import_samples -> import_amplicon -> run_classify |
| 24 | 6 | yes | create_sample_set -> runFastQC -> align_reads_using_hisat2 -> run_stringtie -> run_DESeq2 ... |
| 24 | 5 | no | run_SPAdes -> run_QUAST_app -> annotate_contigs -> KButil_Build_GenomeSet -> run_kb_gtdbtk_classify_wf |
| 22 | 4 | no | run_SPAdes -> run_QUAST_app -> KButil_Build_GenomeSet -> run_kb_gtdbtk_classify_wf |
| 16 | 21 | no | import_fastq_sra_as_reads_from_staging -> import_fastq_sra_as_reads_from_staging -> runFastQC -> runFastQC -> run_trimmomatic ... |
| 14 | 2 | yes | load_single_end_reads_from_URL -> run_kb_filtlong |
| 13 | 9 | yes | load_paired_end_reads_from_URL -> runFastQC -> run_trimmomatic -> run_metabat -> run_checkM_lineage_wf ... |
| 12 | 5 | no | create_sample_set -> runFastQC -> align_reads_using_hisat2 -> run_DESeq2 -> upload_featureset_from_diff_expr |
| 11 | 2 | yes | annotate_contigs -> insert_set_of_genomes_into_species_tree |
| 11 | 19 | no | run_megahit -> run_megahit -> run_megahit -> run_metabat -> run_metabat ... |
| 11 | 6 | yes | import_samples -> import_amplicon -> import_samples -> import_amplicon -> run_classify ... |
| 10 | 2 | yes | annotate_genome_assembly -> build_metabolic_model |
| 10 | 18 | no | RQCFilter -> fastq_stats -> execReadLibraryPRINSEQ -> fastq_stats -> runFastQC ... |
| 9 | 4 | no | run_megahit -> run_metabat -> run_checkM_lineage_wf -> run_checkM_lineage_wf_withFilter |
| 9 | 7 | no | run_megahit -> run_metabat -> run_checkM_lineage_wf -> extract_bins_as_assemblies -> annotate_genome_assembly ... |

## The 15 longest candidate workflows

| steps | copies | apps |
|---|---|---|
| 355 | 1 | import_sra_as_reads_from_web -> runFastQC -> runFastQC -> runFastQC -> runFastQC -> runFastQC -> runFastQC ... |
| 242 | 1 | import_sra_as_reads_from_web -> KButil_Merge_MultipleReadsLibs_to_OneLibrary -> KButil_Merge_MultipleReadsLibs_to_OneLibrary -> KButil_Merge_MultipleReadsLibs_to_OneLibrary -> runFastQC -> run_trimmomatic -> runFastQC ... |
| 221 | 1 | import_sra_as_reads_from_web -> runFastQC -> run_trimmomatic -> runFastQC -> runFastQC -> runFastQC -> runFastQC ... |
| 187 | 1 | load_paired_end_reads_from_URL -> KButil_Merge_MultipleReadsLibs_to_OneLibrary -> run_metaSPAdes -> run_maxbin2 -> run_maxbin2 -> run_maxbin2 -> run_maxbin2 ... |
| 185 | 1 | run_kb_concoct -> run_maxbin2 -> run_metabat -> run_kb_das_tool -> run_kb_concoct -> run_maxbin2 -> run_metabat ... |
| 175 | 1 | run_maxbin2 -> run_maxbin2 -> run_kb_concoct -> run_metabat -> extract_bins_as_assemblies -> run_maxbin2 -> run_maxbin2 ... |
| 156 | 1 | run_maxbin2 -> run_maxbin2 -> run_maxbin2 -> run_maxbin2 -> run_maxbin2 -> run_maxbin2 -> run_maxbin2 ... |
| 154 | 1 | run_kb_concoct -> run_maxbin2 -> run_metabat -> run_kb_das_tool -> run_kb_concoct -> run_maxbin2 -> run_metabat ... |
| 146 | 1 | import_sra_as_reads_from_web -> runFastQC -> runFastQC -> runFastQC -> runFastQC -> runFastQC -> runFastQC ... |
| 143 | 1 | run_metabat -> extract_bins_as_assemblies -> run_metabat -> extract_bins_as_assemblies -> run_metabat -> run_metabat -> run_metabat ... |
| 128 | 1 | import_samples -> import_sra_as_reads_from_web -> import_sra_as_reads_from_web -> import_sra_as_reads_from_web -> batch_link_samples -> batch_link_samples -> create_sample_set ... |
| 123 | 1 | run_kb_concoct -> run_maxbin2 -> run_metabat -> run_kb_das_tool -> run_kb_concoct -> run_maxbin2 -> run_metabat ... |
| 120 | 1 | run_kb_concoct -> run_maxbin2 -> run_metabat -> run_kb_das_tool -> run_kb_concoct -> run_maxbin2 -> run_metabat ... |
| 119 | 1 | run_kb_concoct -> run_maxbin2 -> run_metabat -> run_kb_das_tool -> run_kb_concoct -> run_maxbin2 -> run_metabat ... |
| 115 | 1 | import_sra_as_reads_from_web -> runFastQC -> runFastQC -> runFastQC -> runFastQC -> runFastQC -> runFastQC ... |

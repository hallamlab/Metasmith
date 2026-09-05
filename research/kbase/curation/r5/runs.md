# Round 5 — what was run, and what was checked

Every body under `src/metasmith_libraries/transforms/kbase/` was executed on this host
through `run_one.sh`, which is `metasmith run` with `METASMITH_WORK_ROOT` pinned to the
work directory: the real `ExecuteStep`, the real containers, no solver and no Nextflow.
The check column is what was inspected in the product, not that the command exited zero —
a transform that succeeds and writes an empty file is the failure mode this round exists
to rule out.

Fixtures are built by `fixtures.md`'s recipe under `data/scratch/r5_runs/fixtures/`, from
the first 200 kb of E. coli K-12 MG1655 (NC_000913.3) and the 120 proteins whose CDS falls
inside it.

## The shell bodies

| transform | s | product | checked |
|---|---:|---|---|
| `filter_assembly/seqkit_filter_contigs` | 8 | 199 KB fna | 3 contigs in, 2 out — the 500 bp one is gone at `--min-len 1000` |
| `align_reads/bowtie2_align` | 56 | 1.9 MB bam + bai | `samtools flagstat`: 80,000 of 80,000 reads mapped |
| `qc_alignment/samtools_stats` | 20 | 40 KB txt | `reads mapped: 80000`, `average length: 150` |
| `call_variants/bcftools_variants` | 27 | vcf | 30 known substitutions planted in the reads: **30 recovered, 0 false positives** |
| `quantify_expression/stringtie_feature_counts` | 23 | 242-line gtf | 120 transcripts, 238 of 240 rows with non-zero coverage |
| `search_homologs/blast_homolog_hits` | 47 | 122-row tsv | 117 of 120 query proteins hit; the ones inside contig_1 at 100% identity |
| `polish_assembly/polypolish` | 72 | 196 KB fna | all 3 contigs polished, headers stamped `polypolish` |
| `annotate/bakta_cds` | 198 | 5 products | 120 proteins annotated, 12 hypotheticals, 3,655-line json |
| `align_sequences/mafft_msa` | ~330 | 330 KB afa | 120 sequences, all 2,556 columns wide |
| `build_tree/gtdbtk_tree` | — | — | **not run** — see below |

`gtdbtk_tree` is the one body written by reading rather than by running. Its reference is
the GTDB release, ~110 GB, which is not on this host and is out of proportion to a parity
check. It was checked against `metagenomics/taxonomy/gtdbtk.py`, which runs the same image
with the same bind and the same `GTDBTK_DATA_PATH`, and whose `classify_wf` already runs
the `identify` and `align` steps this one calls separately.

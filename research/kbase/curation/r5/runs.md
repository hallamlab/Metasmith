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

## The table bodies

The algorithm is in `resources/lib/` and the protocol is one `python <script> <args>`
line, per the library's own rule. Each fixture was constructed so the answer was known
before the transform ran.

| transform | s | product | checked |
|---|---:|---|---|
| `profile_abundance/kraken_abundance` | 106 | 3x6 tsv | row sums 1000 / 600 / 1000, matching the three hand-written kreports exactly |
| `compare_genomes/ppanggolin_summary` | 19 | 10-row tsv | 4 core, 4 accessory, 2 unique — the planted partition, and the KO join found all four families whose annotation carried one |
| `enrichment/go_overrepresentation_analysis` | 66 | 4-row tsv | the planted term came back at p = 1.3690123882275552e-16, **equal to the last digit** to an independent `math.comb` computation of P(X>=15 | N=200, K=17, n=20) |
| `cluster_expression/expression_clusters` | ~80 | 30-row tsv | three planted co-varying blocks of ten recovered as three clusters of ten, with no gene crossing |

One defect was found by running them: `HierarchicalCluster(..., metric="precomputed")`
feeds `scipy.spatial.distance.squareform`, which rejects a matrix asymmetric by any
amount at all, and `np.corrcoef` is symmetric only up to floating-point rounding.
`lib::expression_clusters.py` averages with the transpose and zeroes the diagonal before
handing the matrix over. Nothing static would have caught it.

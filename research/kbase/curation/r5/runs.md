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

## The modelling bodies

`cobra.env` had no container at all before this round, so none of these four could run
under DOCKER or APPTAINER at any point in rounds 3 and 4. They were run against the
locally built `docker/cobra/` image (`cobra:local`, 506 MB), named in `cobra.env` for the
duration of the runs and taken back out afterwards — the image is not pushed, and an
env naming a registry entry that does not exist fails on whichever host draws the task.

| transform | s | product | checked |
|---|---:|---|---|
| `build_model/fetch_bigg_model` | 34 | SBML | `e_coli_core`: 95 reactions, 72 metabolites, 137 genes — the published model's own counts |
| `build_model/gem_from_gpr` | 95 | SBML | 15 reactions from 15 MNXR, 41 metabolites, 20 genes; 10 named directly by the GPR table and 5 recovered through `mnxr_lookup` from the one row that carried none |
| `run_fba/cobra_fba` | 42 | 3-row csv | 0.8739 on minimal glucose — the canonical e_coli_core growth rate; **infeasible** with the carbon source closed; a `gpmA` knockout neutral, which is right, it has isozymes |
| `gapfill_model/cobra_gapfill` | ~600 | SBML | e_coli_core with enolase deleted grows at 0.0; gapfill adds two reactions and it grows at 0.639 |

Two things this found that a static gate could not.

**cobra builds its `Configuration` at import, and that constructor makes a cache
directory.** The container runs as the calling uid with no passwd entry, so `$HOME` is `/`
and the mkdir fails before a line of the entry point runs. All four bodies export
`XDG_CACHE_HOME=$TMPDIR`, which is what platformdirs reads first.

**A gapfill over an unfiltered MetaNetX universe restores growth with reactions no
organism runs.** The first run added a single entry — `12 NADH + 3 succinate = 4 pyruvate
+ 12 NAD+`, a SABIO-RK lump MetaNetX makes no balance claim about — because the MILP
minimises reaction COUNT and one aggregate is always cheaper than the pathway it stands
for. Restricting the universe to the 44,168 reactions MetaNetX flags `B` (of 83,796) cut
the candidate set from 100 to 66 and returned two named enzymes instead:
2-oxoglutarate:ferredoxin oxidoreductase running reductively, and citrate lyase. The
objective went from 0.276 on the lump to 0.639 on the pair, against 0.874 intact.

`slim_optimize` returns `nan` for an infeasible model rather than raising, and `nan`
compares False against every threshold — so the first version of this body read an
infeasible model as one that needed no gapfill at all and added nothing.

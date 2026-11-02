# Round 5 — the fixtures the runs were made against

Nothing here is committed: the fixtures live under `data/scratch/r5_runs/fixtures/`, which
is the run working directory and is git-ignored. This file is the recipe, so a later
session can rebuild them without guessing what "a small assembly" meant.

Everything derives from one download: E. coli K-12 MG1655, `NC_000913.3`, fetched from
NCBI EFetch. Using a real genome rather than random sequence is what makes the checks
mean something — `tblastn` finds real homology, `bakta` makes real calls, and a variant
caller has a real reference to disagree with.

| file | what it is |
|---|---|
| `assembly.fasta` | three contigs off the genome's first 200 kb: 150,000 bp, 50,000 bp, and a 500 bp one that exists so a `--min-len 1000` filter has something to drop |
| `min_contig_length.txt` | `1000` |
| `orfs.faa` | the 120 proteins whose CDS lies inside contig_1, taken from the K-12 proteome already on this host |
| `annotation.gff3` | gene/transcript/exon over contig_1 at those same CDS coordinates, for `stringtie -G` |
| `clean_reads.fq.gz` | 40,000 interleaved 150 bp pairs off contig_1 and contig_2 at 0.4% substitution — sequencing error, no fixed variation |
| `variant_reads.fq.gz` | the same, from a copy of the two contigs carrying 30 planted substitutions at 0.2% error |
| `variant_truth.tsv` | those 30 substitutions, so the VCF can be scored rather than counted |
| `read_metadata.json` | `{"parity": "paired", "length_class": "short"}` |
| `experiment.txt` | a marker file; `transcriptomics::experiment` is a grouping type with no content |

The bakta database is `db-light` v6.0 (4.0 GB extracted), downloaded once into
`data/scratch/r5_runs/refs/bakta/` with the pinned image's own `bakta_db download
--type light`.

The two read sets are separate on purpose. Random per-read error and fixed strain
variation look the same in a fastq and are opposite results out of a variant caller: the
first set produces two calls in 200 kb, which is the caller correctly refusing to promote
error to variation, and the second produces exactly the thirty that were planted.

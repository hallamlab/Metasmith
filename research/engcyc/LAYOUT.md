# Engcyc published-output layout

## Purpose & Contents

This file is the contract for the files a pipeline publishes for one Engcyc dataset. It defines the directory grammar, file naming, IDs inside files, pooled tables, gaps and provenance. Publishers write to it. Curation, the portal and the Globus archive read from it.

It takes the Spanish Lakes publish dir (`spanish_lakes/metagenomics/`) as the base and fixes the places where that tree and the Cyanoverse `v2/` tree disagree. It describes files on disk only. Keep paths and sample details out of this file, because the git remote is public.

## Directory grammar

Every published file has exactly one of two paths:

    <dataset>/<subject>/<product>/<stem>[.<set>].<ext>
    <dataset>/tables/<table>.<ext>

- `<dataset>` is the `dataset_id` from the Engcyc dataset manifest.
- `<subject>` groups products by what they describe. Use the subjects in the table below.
- `<product>` holds one kind of file. It is a directory with no subdirectories.
- `<stem>` names the sample or bin the file describes. See *Stems*.
- `<set>` names the input set when one product runs on more than one read set, such as `qc` and `disc`. Omit it otherwise.

Subjects:

| Subject | Holds |
|---|---|
| `reads` | QC'd reads, discarded reads, read QC stats |
| `assembly` | contigs, ORFs, GFF, assembly stats, coverage, alignments |
| `taxonomy_reads` | per-read classifiers and profilers |
| `taxonomy_contigs` | per-contig taxonomy, such as Metabuli |
| `annotation_orfs` | ORF-level annotators, including the GPR lanes |
| `annotation_contigs` | contig-level annotators, such as Bakta ncRNA and antiSMASH |
| `amr` | resistome and mobile-element products, at ORF, contig or read level |
| `binning` | bins, contig-to-bin tables, bin QC |
| `taxonomy_binning` | GTDB-Tk and other per-bin taxonomy |
| `models` | per-bin metabolic models (GEMs, MEMOTE) and PGDBs |
| `viral` | per-sample viral calls that feed the dataset's viral catalogue |

**CAUTION** Never nest a per-sample directory inside a product. A tool that writes several files per sample gets one product per file kind.

## Products

Name a product `<tool>_<kind>` in snake case, such as `genomad_virus_summary`, `metabuli_report` or `predictf_results`. Drop `_<kind>` when the tool writes one file kind. A product equals one metasmith output type, so a publisher maps each type to one product directory.

A product directory holds one file per stem and nothing else. Keep superseded outputs, backups and intermediates out of it. Move them to `_redundant/<dataset>/`, outside the archived tree.

Per-binner products put the binner in the product name: `<binner>_bins`, `<binner>_contig_to_bin`, `checkm2_<binner>`, `gtdbtk_<binner>`.

## Stems

- **Sample.** The `sample_id` from the dataset's `samples.tsv`. Use the run accession for public data (`SRR…`, `ERR…`, `DRR…`) and the study's own short code for internal data (`SG10E12`). A stem uses only letters, digits and `_`.
- **Bin.** `<sample>.<binner>.<NNN>`, with `NNN` the binner's own index zero-padded to three digits. The binner stays in the stem as well as the product, so a flat listing of every bin stays unique.
- **Genome dataset.** For a dataset of isolate or MAG genomes, the genome is the sample.

## IDs inside files

- Contigs keep the assembler's names, such as `k141_45`.
- ORFs keep Prodigal's `<contig>_<n>`.
- A file that pools contigs or ORFs from several samples prefixes each ID with `<sample>~`. Use no other separator. A parquet table carries a `sample` column instead of the prefix.
- A publisher strips every reversible batching prefix before it writes a per-sample file.

## Formats

| Content | Extension |
|---|---|
| Nucleotide FASTA | `.fna` for contigs, `.fa` for bins |
| Protein FASTA | `.faa` |
| Feature annotation | `.gff` |
| Table up to about a million rows | `.tsv` with one header row |
| Larger table | `.parquet` |
| A tool's own format | the tool's extension (`.json`, `.bam`, `.html`, `.pkl`) |
| Archive of a tool's output directory | `.tar.gz` |

Append `.gz` only to a full extension, such as `.tsv.gz`. Write a headerless BLAST6 table as `.tsv` and name its columns in the dataset README.

## Pooled tables

A file that describes the whole dataset goes in `<dataset>/tables/`, never in a product directory. Examples: `contig_stats.parquet`, `mags.parquet`, `bin_clusters.tsv`, `votus.parquet`, `gpr.parquet`, `feast_proportions.tsv`.

`mags.parquet` has one row per bin: `bin`, `sample`, `binner`, `completeness`, `contamination`, `gtdb_taxonomy`, `cluster` and `is_representative`.

## Samples and gaps

`<dataset>/samples.tsv` lists every sample: `sample_id`, `source_name` (the name the data arrived with), `accession` and `sets` (the read sets present).

When a product has fewer files than `samples.tsv` has samples, record each missing stem in `<dataset>/_logs/gaps.tsv`: `product`, `stem`, `reason`. A reason is one of these:

- `no_hits`: the tool ran and found nothing, as MobileElementFinder does on most samples.
- `failed`: the tool ran and failed.
- `not_run`: the product is not built for this sample yet.
- `not_applicable`: the product does not apply, such as binning on an isolate.

Never publish a zero-byte placeholder.

## Provenance

`<dataset>/_logs/` holds:

- `ledger.tsv`, one row per published file: `path`, `stem`, `product`, `run`, `source` (the metasmith result path or hash name it came from), `bytes`, `sha256`.
- `runs/<run>.tar.gz`, one per metasmith run: its logs, metadata and manifests.
- `gaps.tsv`.

`<dataset>/README.md` names the dataset, its tools and versions, and any column layout this file leaves open. Generate its product list from the tree.

## Storage

The fir publish dir holds one directory per dataset. The Globus archive mirrors each dataset directory 1:1, `_redundant/` excepted.

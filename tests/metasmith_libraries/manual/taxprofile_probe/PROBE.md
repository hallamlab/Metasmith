# Taxonomic profiler output-shape probes on sockeye

Goal: run each candidate profiler standalone on UBC ARC Sockeye, against a tiny paired-end FASTQ + minimum-viable reference DB, and record the **actual** output filenames, columns, and quirks. The filled tables below are the source of truth for:

- `data_types/taxonomy.yml` (output type declarations — extensions, multi-file vs single-file)
- the exact command-line strings inside each transform under `transforms/metagenomics/taxonomy/`

Until every row is filled, downstream edits do **not** land.

Venue chosen: sockeye over fir because a pre-built Kraken2 standard 16GB DB (with Bracken `kmer_distrib` for read lengths 50/75/100/150/200/250/300) is already staged at `/scratch/st-shallam-1/k2_standard_16_GB_20251015/`. That gives the Kraken2/Bracken probes a realistic DB without a ~500 MB build step on the cluster.

## Cluster facts (sockeye)

- UBC ARC Sockeye, SLURM
- Container runtime: **Apptainer 1.3.1** via `module load gcc/9.4.0 apptainer/1.3.1`
- Compute nodes have internet access (apptainer pull / curl from ENA work)
- Partition: `interactive_cpu` (1-day limit, 32+ cores, 190 GB/node) for `salloc`
- Account: `st-shallam-1`

## Workspace layout

| Asset | Path |
|---|---|
| Workspace root | `/scratch/st-shallam-1/txyliu/taxprofile_probe/` |
| Pulled SIFs | `containers/{kraken2,bracken,centrifuger,sylph,ganon,metaphlan}.sif` |
| Pre-built K2 standard DB | `/scratch/st-shallam-1/k2_standard_16_GB_20251015/` (bind-mounted read-only) |
| Tool-specific mini-DBs | `dbs/{centrifuger_mini,sylph_mini,ganon2_mini,metaphlan_mini}/` |
| Tiny refs (for sylph/ganon2/centrifuger build) | `fixtures/tiny_refs/*.fna` |
| Tiny paired reads (from ENA) | `fixtures/tiny_pe_R1.fq.gz`, `fixtures/tiny_pe_R2.fq.gz` |
| Per-tool run dirs | `runs/{kraken2,centrifuger,sylph,ganon2,metaphlan}/` |

## How to run

```bash
ssh fir
cd /scratch/phyberos/taxprofile_probe
# One-time bootstrap (pulls containers, builds mini-DBs, creates tiny reads):
./bootstrap.sh
# Then run probes; each writes runs/<tool>/probe.log and runs/<tool>/<outputs>
./probe_kraken2.sh
./probe_centrifuger.sh
./probe_sylph.sh
./probe_ganon2.sh
./probe_metaphlan.sh
```

The probe scripts use `salloc --no-shell` + `srun --jobid=$JOBID` so a single 1-hour allocation can drive all five tools in sequence.

## Verified profiler invocations

All probes verified 2026-05-27 against `SRR2584863` (E. coli K-12 MG1655 short-read MiSeq, 5 000 paired reads subsampled). Real Kraken2 standard 16GB DB at `/scratch/st-shallam-1/k2_standard_16_GB_20251015/` used as both the kraken2 source DB and the NCBI taxonomy (nodes.dmp / names.dmp) source for centrifuger/ganon2 builds.

| Tool | Image (tag) | Mem | Command (key flags) | Output files | Columns / delim | Quirks |
|---|---|---|---|---|---|---|
| **Kraken2** | `quay.io/biocontainers/kraken2:2.1.6--pl5321h077b44d_0` | 32 G | `kraken2 --paired --db <dir> --report kraken2.kreport --output kraken2.classifications.tsv --threads N R1 R2` | `kraken2.kreport`, `kraken2.classifications.tsv` | 6 cols / TAB / no header; 5 cols / TAB / no header | DB is a *directory* (hash.k2d + opts.k2d + taxo.k2d). `--paired R1 R2` are positional (the flag is just `--paired`). 5000 PE reads in 0.15 s with full standard DB. |
| **Bracken** | `quay.io/biocontainers/bracken:3.0--h9948957_2` | 32 G | `bracken -d <k2_dir> -i kraken2.kreport -o bracken.species.tsv -w bracken.kreport -r 150 -l S` | `bracken.species.tsv`, `bracken.kreport` | 7 cols / TAB / **WITH header** (`name | taxonomy_id | taxonomy_lvl | kraken_assigned_reads | added_reads | new_est_reads | fraction_total_reads`); 6 cols / TAB / no header (same format as kraken2.kreport) | Requires `database{50,75,100,150,200,250,300}mers.kmer_distrib` next to the kraken2 DB — the staged K2 standard DB already has these. `-w` flag re-emits a kraken-style report with abundance corrections (THIS is the bridge format vs centrifuger.kreport). |
| **Centrifuger** | `quay.io/biocontainers/centrifuger:1.1.1--h3be2455_0` | 32 G | `centrifuger -x <prefix> -1 R1 -2 R2 -t N > centrifuger.classifications.tsv`; then `centrifuger-kreport -x <prefix> centrifuger.classifications.tsv > centrifuger.kreport`; optional `centrifuger-quant -x <prefix> -c centrifuger.classifications.tsv > centrifuger.summary.tsv` | `centrifuger.classifications.tsv`, `centrifuger.kreport`, `centrifuger.summary.tsv` | 8 cols / TAB / **WITH header** (`readID seqID taxID score 2ndBestScore hitLength queryLength numMatches`); 6 cols / TAB / no header (same format as kraken2.kreport — bridge target); 7 cols / TAB / **WITH header** (`name taxID taxRank genomeSize numReads numUniqueReads abundance`) | Index = 4 files `<prefix>.{1,2,3,4}.cfr` (~23 MB for 5-genome mini-DB). **Classifier flag is `-t` (NOT `-p`).** Classifications go to **stdout** (no `--report-file` flag). `centrifuger-kreport` is a separate binary, not a sub-flag. `centrifuger-quant` prepends 3 log lines to stdout before the data — pipe stderr away (`2>/dev/null`) when capturing. Build requires `--taxonomy-tree nodes.dmp --name-table names.dmp` and either `-l <file>` with two-column `file\ttaxID` rows OR `-r <fna>` plus `--conversion-table`. |
| **sylph** | `quay.io/biocontainers/sylph:0.8.0--ha6fb395_0` | 8 G | `sylph sketch -i refs/*.fna -o tiny -t N` (DB build, seconds); then `sylph profile tiny.syldb -1 R1 -2 R2 -t N -o sylph.profile.tsv` | `sylph.profile.tsv` (single file); `tiny.syldb` (DB; single sketch file) | 15 cols / TAB / **WITH header** (`Sample_file Genome_file Taxonomic_abundance Sequence_abundance Adjusted_ANI Eff_cov ANI_5-95_percentile Eff_lambda Lambda_5-95_percentile Median_cov Mean_cov_geq1 Containment_ind Naive_ANI kmers_reassigned Contig_name`) | Lightest tool. Sketch + profile completed in <1 s on 5-genome mini-DB; <500 MB RAM. `sylph profile` (not `sylph query`) is the right subcommand for taxonomic profiling. DB is one `.syldb` file. |
| **ganon2** | `quay.io/biocontainers/ganon:2.4.1--py312h88d62d1_0` | 32 G | `ganon build-custom --input-file <3col.tsv> --input-target file --db-prefix tiny --taxonomy ncbi --taxonomy-files nodes.dmp names.dmp --skip-genome-size --threads N`; then `ganon classify --db-prefix tiny --paired-reads R1 R2 --output-prefix out --threads N` | `out.tre`, `out.rep`; DB = `tiny.hibf` (14 MB Hierarchical IBF) + `tiny.tax` | `out.tre`: 9 cols / TAB / no header (`rank | taxid | lineage(pipe-separated) | name | unique | shared | children | total | percent`); `out.rep`: 7 cols / TAB / no header + `#`-prefixed footer comments | **Subcommand is `ganon` (NOT `ganon2`) even in v2.4.1.** For custom local FNAs use `build-custom`, NOT `build` (the latter downloads from refseq/genbank). `--input-file` is THREE columns: `file <tab> target <tab> taxID` (target is any unique ID, we use the file basename). Default extension is `.hibf` (Hierarchical IBF), not `.ibf` as in older versions. |
| **MetaPhlAn 4** | `quay.io/biocontainers/metaphlan:4.2.4--pyhdfd78af_0` | **64 G** (32 G OOMs!) | DB pre-install on login node: `metaphlan --install --db_dir <dir> --nproc N`; then on compute: `metaphlan R1.fq.gz,R2.fq.gz --input_type fastq --offline --db_dir <dir> --nproc N -o metaphlan.profile.tsv --mapout metaphlan.bowtie2.sam` | `metaphlan.profile.tsv`, `metaphlan.bowtie2.sam` (companion alignment) | 4 cols / TAB / `#`-prefixed comment-header lines (`clade_name | NCBI_tax_id | relative_abundance | additional_species`); preceded by `#mpa_*` (DB version), `#<full cmdline>`, `#NN reads processed`, `#SampleID\tMetaphlan_Analysis`, `#clade_name\t...` | **DB is 33 GB on disk; bowtie2 loads ~33 GB into RAM → need ≥64 GB.** DB download (`mpa_vJan25_CHOCOPhlAnSGB_202503`, ~38 GB transfer, ~50 GB unpacked then trimmed to 34 GB) must run on login/DTN — compute nodes are firewalled. v4.2 renamed `--bowtie2db` → `--db_dir`. v4.2 added `-1`/`-2` paired-end flags, but those *require* `--subsampling_paired` — cleaner to keep the legacy `R1.fq,R2.fq` comma form with `--mapout file.sam` to satisfy the multi-input requirement. Use `--offline` on compute to skip the network check. |

## Bridge-format confirmation

`kraken2.kreport`, `bracken.kreport`, and `centrifuger.kreport` all share the **exact same 6-column kraken-style format** (no header, columns: `pct | reads_clade | reads_taxon | rank | taxid | indented_name`). This is the artifact `tests/test_taxonomy_kmer_workflow.py::test_kmer_bridge_kraken2_vs_centrifuger` can diff to demonstrate the migration.

## Cluster-level gotchas observed

- **`salloc --no-shell` requires `--nodes=1 --ntasks=1` on sockeye** (vs fir which auto-fills). Fixed in `_lib.sh:probe_alloc`.
- **Compute nodes are firewalled.** All container pulls + DB downloads must happen on a login or DTN node. Run `bootstrap.sh` and `metaphlan --install` from login; only run actual classification under `salloc`/`srun`.
- **`set -o pipefail` + `head`** triggers SIGPIPE on the upstream pipe and exits the script. Use `set +o pipefail` around `zcat … | head | gzip` blocks. Fixed in `bootstrap.sh`.
- **MetaPhlAn DB grew significantly.** v4.2 ChocoPhlanSGB_202503 = ~33 GB compressed bowtie2 indexes + 5 GB additional + 2 GB VSG.fna = 34 GB final. Earlier estimates of ~10 GB are out of date.

## Example outputs

Captured at `tests/test_data/taxprofile_examples/<tool>/` (one tool per dir). These become fixtures for the planning + harness tests:

```
tests/test_data/taxprofile_examples/
├── kraken2/{kraken2.kreport, kraken2.classifications.tsv, bracken.species.tsv, bracken.kreport}
├── centrifuger/{centrifuger.classifications.tsv, centrifuger.kreport, centrifuger.summary.tsv}
├── sylph/sylph.profile.tsv
├── ganon2/{tiny.tre, tiny.rep}
└── metaphlan/metaphlan.profile.tsv
```

## Probe completion checklist

- [x] All container SIFs pulled and recorded with their exact tag
- [x] All mini-DBs built and on disk (real K2 standard reused; centrifuger/sylph/ganon2 built from 5 tiny RefSeq genomes; MetaPhlAn full vJan25 DB installed)
- [x] Each probe script run successfully; `runs/<tool>/probe.log` non-empty
- [x] Table above fully populated
- [x] Example outputs copied to `tests/test_data/taxprofile_examples/`
- [x] Plan task #2 ready to mark completed

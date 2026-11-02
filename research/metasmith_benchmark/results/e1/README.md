# What survives of E1 in this repo

E1's two arms live as tar archives on fir at `/scratch/phyberos/bench/archive/e1_long.tar` (35.5 GB, 13,787
members) and `e1_short_out.tar` (99.1 GB, 57,253 members), with a full member manifest beside each
(`e1_*.manifest.txt`, written by job `60958550`). **Ask the manifests, not the tars** — a member question is a
grep, where a listing costs 37 s on the long archive and 425 s on the short one.

The tables here are the E1 side of `findings/E1_E2_REPRODUCTION.md`, which says what each one feeds.

| file | what it is |
|---|---|
| `e1_slurm_tasks.tsv`, `e1_runtime_by_process.tsv` | one row per Slurm attempt, and its per-process summary, from `build_e1_slurm_tables.py` |
| `e1_short_bowtie2_assembly_align.tsv` | E1's own mapping rates, from MultiQC (below) |
| `e1_e2_fastp_compare.tsv`, `e1_e2_assembly_compare.tsv` | E1 against E2 on post-fastp reads and on assemblies |
| `e1_bam_regeneration_check.tsv` | regenerated short BAMs' bowtie2 counts against E1's MultiQC |
| `mag_gaps.tsv` | every E1 sample-by-binner gap, with its cause and what closed it |
| `e1_checkm2.tsv.gz` | one CheckM2 row per scored bin, from `drivers/checkm2_tables.py e1` |
| `e1_amber_summary.tsv`, `e1_amber_bin_metrics.tsv` | AMBER per sample and binner, and per bin, from `drivers/e1_close_amber.sbatch` |

## `e1_short_bowtie2_assembly_align.tsv`

208 rows, one per short-arm sample. E1's read-mapping rates against its own MEGAHIT assemblies.

**This is `BOWTIE2_ASSEMBLY_ALIGN`, not host or phiX removal.** Checked, not assumed: MultiQC's
`multiqc_sources.yaml` names the module `Bowtie2: assembly` and sources each row from
`MEGAHIT-<sample>.bowtie2.log`. nf-core/mag also runs a removal alignment, and confusing the two would
silently compare the wrong thing.

Derived from `out/multiqc/multiqc_data/multiqc_bowtie2_bowtie2-2.yaml` inside `e1_short_out.tar`, extracted by
job `60959768` to `/scratch/phyberos/bench/archive/e1_reports/` along with both arms' full `multiqc_data/` and
`pipeline_info/`. Columns other than `assembler` and `sample` are MultiQC's own bowtie2 fields.

`overall_alignment_rate` is bowtie2's own figure and is reproducible from the other columns:

    rate = 100 * (2*(paired_aligned_one + paired_aligned_multi + paired_aligned_discord_one)
                  + paired_aligned_mate_one + paired_aligned_mate_multi) / (2 * paired_total)

Verified on `marine_sample_0`: 28,686,587 / 33,294,752 = 86.16%, matching the reported value exactly.

Distribution over the 208: min 80.83, median 97.67, mean 96.55, max 99.90.

## Why it matters

**nf-core did not publish E1's short BAMs.** Both manifests report zero `.bam` members. The 208 short BAMs were
regenerated with nf-core's own bowtie2 command, and this table is what proves them faithful: every regenerated
log reproduces these counts exactly (`e1_bam_regeneration_check.tsv`).

**The long arm has no counterpart.** Its MultiQC consumed CheckM2 only — no bowtie2, no minimap2, no mapping
section. E1's long BAMs survive in `long/kept_inputs/` instead.

## Comparability warning

These rates come from parsing bowtie2's end-of-run log. The E2 side of the same metric would most naturally
come from the BAM via `samtools flagstat`. Those are different instruments and should agree but need not
exactly. Either label each column with its source, or parse E2's own bowtie2 logs from its cache shards so
both columns come from the same instrument.

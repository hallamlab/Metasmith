# E1 vs E2: metasmith reproduces nf-core/mag

## Purpose & Contents

This document records the evidence that E2 reproduces E1. E1 is nf-core/mag 5.5.0, and E2 is metasmith solving the same workflow. Both ran on the same 249 CAMI samples, 208 short-read and 41 long-read. The document holds the result, then the evidence in this order:

- the deviations between the two runs
- the pinned configuration diff
- the assemblies compared by sequence
- the MAGs matched pairwise across runs, and why unmatched DAS Tool MAGs are unmatched
- quality tier changes across matched MAGs
- the run-to-run control, two reruns of E1 at new binner seeds
- MAG counts and CheckM2 quality per CAMI subset
- jobs, execution time and nf-core processes against metasmith steps
- why AMBER is not part of the claim

Paths are relative to `research/metasmith_benchmark/`. `E1_E2_COMPARISON.md` holds the earlier plan-shape and runtime comparison. `TOOL_FOR_TOOL.md` holds the earlier prose parity audit. Where the two disagree, `results/pinned_config_diff.tsv` wins, because a script checks every row against its source.

## Result

E2 reproduces E1. On the short arm E2 disagrees with E1 no more than nf-core/mag disagrees with a rerun of itself for DAS Tool and COMEBin, and a few points more for MetaBAT2 and SemiBin2. On the long arm E2 disagrees more, and the excess follows the assembly difference. The two runs are not identical.

- **Workflow.** Every tool call in E1 has a counterpart step in E2, with the same arguments except threads, memory and the deviations listed below.
- **Assemblies.** No sample's assembly is identical between the runs. On the short arm the median sample holds 99.2% of its bases in contigs whose sequence is identical in both runs. The reads going into MEGAHIT are identical, so this is MEGAHIT's own variation. The long arm shares far less exact sequence, and most of it still aligns at 99% identity or better.
- **MAGs.** DAS Tool is the headline binner. 94.6% of E1's short-arm DAS Tool MAGs and 93.9% of E2's have a reciprocal skani partner in the other run, and 90.8% of each on the long arm. Matched pairs have a median ANI of 99.96 to 100.
- **Control.** Two reruns of E1 at new binner seeds, on 25 samples with E1's own assemblies, set the baseline. Their short-arm DAS Tool MAGs match each other at 94.1% and 95.2%. E1 and E2 match at 96.0% and 95.2% on the same samples.
- **Unmatched MAGs.** 38 of the 39 high-quality DAS Tool MAGs without a partner come from a binner bin that does match across runs. The other run's DAS Tool kept a different bin for it.

## Rebuild the tables

Run these from the repository root. The tables below are their output, unedited.

1. Run `python3 research/metasmith_benchmark/results/reproduction.py --markdown`. It prints the CheckM2, jobs and steps tables.
2. Run `python3 research/metasmith_benchmark/results/compare_arms.py --markdown`. It prints the assembly, matching, cause and tier tables.
3. Run `python3 research/metasmith_benchmark/results/compare_arms.py --raw research/metasmith_benchmark/results/compare_arms_ctl/raw --out research/metasmith_benchmark/results/compare_arms_ctl/e1_e1ctl --checkm2 research/metasmith_benchmark/results/e1/e1_checkm2.tsv.gz research/metasmith_benchmark/results/compare_arms_ctl/e1ctl_checkm2.tsv.gz --labels E1 E1ctl --markdown`. It prints E1 against E1ctl.
4. Run `python3 research/metasmith_benchmark/results/compare_arms.py --raw research/metasmith_benchmark/results/compare_arms_ctl2/raw --out research/metasmith_benchmark/results/compare_arms_ctl2/e1_e1ctl2 --checkm2 research/metasmith_benchmark/results/e1/e1_checkm2.tsv.gz research/metasmith_benchmark/results/compare_arms_ctl2/e1ctl2_checkm2.tsv.gz --labels E1 E1ctl2 --markdown`. It prints E1 against E1ctl2.
5. Run `python3 research/metasmith_benchmark/results/compare_arms.py --raw research/metasmith_benchmark/results/compare_arms_ctl_ctl2/raw --out research/metasmith_benchmark/results/compare_arms_ctl_ctl2/e1ctl_e1ctl2 --checkm2 research/metasmith_benchmark/results/compare_arms_ctl/e1ctl_checkm2.tsv.gz research/metasmith_benchmark/results/compare_arms_ctl2/e1ctl2_checkm2.tsv.gz --labels E1ctl E1ctl2 --markdown`. It prints the control baseline.
6. Run `python3 research/metasmith_benchmark/results/compare_arms.py --samples research/metasmith_benchmark/results/compare_arms_ctl/samples.txt --out research/metasmith_benchmark/results/compare_arms_ctl/e1_e2 --markdown`. It prints E1 against E2 on the control's samples.
7. Run `python3 research/metasmith_benchmark/results/control_spread.py`. It prints the four pairs' match percentages side by side, from the `summary.tsv` that steps 3 to 6 write.
8. Run `python3 research/metasmith_benchmark/drivers/check_pinned_config.py --nfcore <nf-core/mag tree> --markdown`. It checks every row of the configuration diff against its cited source, then prints the rows that are not `same`. The nf-core/mag tree is commit `56abab5b`, from fir `~/.nextflow/assets/nf-core/mag`.

| input | what it holds | produced by |
|---|---|---|
| `results/e1/e1_checkm2.tsv.gz` | one CheckM2 row per scored E1 bin, 26,774 rows, with `source` = `nfcore`, `gapfill` or `b19_rerun` | `drivers/checkm2_tables.py e1` |
| `results/e2/e2_checkm2.tsv.gz` | one CheckM2 row per E2 bin, 26,928 rows, relabelled to nf-core bin names | `drivers/checkm2_tables.py e2`, read-only over the task cache |
| `results/{e1,e2}/*_slurm_tasks.tsv` | one row per Slurm job attempt | `results/{e1,e2}/build_*_slurm_tables.py` |
| `results/compare_arms/raw/assembly.tsv.gz` | per sample: both assemblies' statistics, file and sequence-set hashes, and shared contigs | `drivers/compare_arms_sample.py`, one fir task per sample from `drivers/compare_arms.sbatch`, gathered by `drivers/compare_arms_gather.sh` |
| `results/compare_arms/raw/contig_matches.tsv.gz` | per unshared contig: its best minimap2 hit in the other assembly and its coverage at 99% identity or better | same |
| `results/compare_arms/raw/mags.tsv.gz` | every MAG of both runs with its contig and base counts | same |
| `results/compare_arms/raw/skani_pairs.tsv.gz` | skani ANI and alignment fractions, every E2 MAG against every E1 MAG of the same sample and binner | same |
| `results/compare_arms_ctl/raw/*.tsv.gz` | the same four tables for E1 against E1ctl, with E1ctl in the E2 columns | `drivers/compare_arms_sample.py --side2-sheet`, one fir task per sample from `drivers/compare_arms_ctl.sbatch` |
| `results/compare_arms_ctl2/raw/*.tsv.gz`, `results/compare_arms_ctl_ctl2/raw/*.tsv.gz` | the same four tables for E1 against E1ctl2, and for E1ctl against E1ctl2 | same |
| `results/compare_arms_ctl/e1ctl_checkm2.tsv.gz`, `results/compare_arms_ctl2/e1ctl2_checkm2.tsv.gz` | one CheckM2 row per control bin, from nf-core's own CheckM2 | `drivers/compare_arms_ctl.sbatch prep` |
| `results/pinned_config_diff.tsv` | 193 rows, one per tool argument, each value cited to a source line | built by hand, checked by `drivers/check_pinned_config.py` |

`compare_arms.py` writes `results/compare_arms/`: `assemblies.tsv` and `per_sample.tsv` per sample, `mag_matches.tsv.gz` per MAG, `unmatched_mags.tsv`, `dastool_unmatched_causes.tsv` and `summary.tsv`.

CAUTION: `compare_arms_sample.py` reads E2's products from the fir task cache. It never writes there. Keep it that way, since the cache is shared with other experiments.

## Deviations

Each row is a known difference between the runs. Every table below carries it.

| deviation | run | what differs | what it touches |
|---|---|---|---|
| b19 depth rerun | E1 long | nf-core's own MetaBAT2 found no long-arm bins. The hand rerun `drivers/e1_nfcore/b19_long_rerun.sbatch` recomputed depth with jgi 2.17 and `--percentIdentity 80`. nf-core pins 2.15, and E2 matches the pin. | every E1 long MetaBAT2 and DAS Tool MAG |
| two-input DAS Tool | E1 long | nf-core's own DAS Tool chose from COMEBin and SemiBin2 only. The scored set is the b19 three-binner rerun, 567 MAGs. nf-core's 564 stay archived. `drivers/e1_close_checkm2.sbatch` rebuilt the FASTAs from the b19 contig-to-bin map and ran nf-core's CheckM2 command. | E1 long DAS Tool, `source = b19_rerun` |
| `strain_sample_49` gapfill | E1 short | E1's depth step read a bad BAM (mean depth 0.006 against 4.91 regenerated) and MetaBAT2 found nothing. `drivers/e1_close_gapfill.sbatch` reran nf-core's depth, MetaBAT2, three-binner DAS Tool and CheckM2 commands. | 12 E1 MAGs, `source = gapfill` |
| regenerated BAMs | E1 short | nf-core did not publish its BAMs. `drivers/e1_close_bam.sbatch` rebuilt all 208 with nf-core's bowtie2 command and image, from E1's assemblies and E2's fastp reads. Those reads equal E1's by read and base count (`results/e1/e1_e2_fastp_compare.tsv`). Every bowtie2 log matches E1's MultiQC counts (`results/e1/e1_bam_regeneration_check.tsv`). | only the `strain_sample_49` gapfill |
| lambda removal | E1 long | nf-core passes its lambda reference to chopper as `--contam`. E2's `chopper` transform does not. | long reads, then everything downstream |
| threads | both | Thread counts differ on almost every tool. COMEBin runs 12 against 48 and exposes no seed. | see the configuration diff |
| MEGAHIT resources | both | 8 threads and 40 GiB on E1, 16 threads and 64 GiB on E2 | short assemblies |

## Pinned configuration diff

The diff compares what each run was configured to pass, per tool argument. The E1 side is nf-core/mag 5.5.0's source plus E1's `control.config`. The E2 side is the literals in E2's transforms. Every value cites the `path:line` it comes from.

It is a pinned configuration, not an observed invocation. It shows the arguments each run was set up to use, not the command lines that ran. The observed form needs a rerun that keeps every task's command script on both sides.

Of 193 rows, 152 are `same`, including every MetaBAT2, SemiBin2, COMEBin and DAS Tool argument except threads. The rows that differ fall into these groups:

- **Resources.** Threads differ on every tool except FastQC, fastp and short-arm MetaBAT2. Memory differs on FastQC and MEGAHIT.
- **Input plumbing.** E1 passes paired files and E2 passes one interleaved file (`-1/-2` against `--12` or `--interleaved`). fastp writes to files on E1 and to `--stdout` on E2.
- **Images.** bowtie2-build and the minimap2 index use different images of the same tool version.
- **Extras.** CheckM2's `-x fa` exists on E2 only, because E2 names bins `.fa`. E1 indexes its BAMs and E2 does not.
- **Deviations.** Long chopper `--contam` and long depth at jgi 2.17 are in the table above.

CAUTION: `TOOL_FOR_TOOL.md` understates the thread differences. Trust this table.

| arm | tool | argument | e1_value | e2_value | status |
|---|---|---|---|---|---|
| short | fastqc (raw) | memory | 6144 | 4096 | differ |
| short | fastp | --out1 | <path> | (absent) | e1-only |
| short | fastp | --out2 | <path> | (absent) | e1-only |
| short | fastp | --stdout | (absent) | --stdout | e2-only |
| short | fastqc (trimmed) | memory | 6144 | 4096 | differ |
| short | megahit | memory | 42949672960 | 68719476736 | differ |
| short | megahit | -1/-2 | <path> | (absent) | e1-only |
| short | megahit | --12 | (absent) | <path> | e2-only |
| short | megahit | threads | 8 | 16 | threads-differ |
| short | bowtie2-build | image | bowtie2:2.4.2--py38h1c8e9b9_1 | mulled-v2-ac74a7f02cebcfcc07d8e8d1d750af9c83b4d45a:577a697be67b5ae9b16f637fd723b8263a3898b3-0 | differ |
| short | bowtie2-build | threads | 1 | 16 | threads-differ |
| short | bowtie2 | -1/-2 | <path> | (absent) | e1-only |
| short | bowtie2 | --interleaved | (absent) | <path> | e2-only |
| short | bowtie2 | samtools index | <path> | (absent) | e1-only |
| short | bowtie2 | threads | 8 | 16 | threads-differ |
| short | jgi_summarize_bam_contig_depths | threads | 6 | 8 | threads-differ |
| short | semibin2 | threads | 6 | 8 | threads-differ |
| short | comebin | threads | 12 | 48 | threads-differ |
| short | das_tool | threads | 6 | 16 | threads-differ |
| short | checkm2 | -x | (absent) | fa | e2-only |
| short | checkm2 | threads | 6 | 8 | threads-differ |
| long | porechop_abi | threads | 4 | 8 | threads-differ |
| long | chopper | --contam | <path> | (absent) | e1-only |
| long | chopper | threads | 2 | 4 | threads-differ |
| long | flye | threads | 12 | 16 | threads-differ |
| long | minimap2 index | image | minimap2:2.29--h577a1d6_0 | minimap2_samtools:33bb43c18d22e29c | differ |
| long | minimap2 index | threads | 2 | 16 | threads-differ |
| long | minimap2 align | samtools sort --write-index | --write-index | (absent) | e1-only |
| long | minimap2 align | threads | 8 | 16 | threads-differ |
| long | jgi_summarize_bam_contig_depths | image | metabat2:2.17--hd498684_0 | metabat2:2.15--h986a166_1 | differ |
| long | jgi_summarize_bam_contig_depths | OMP_NUM_THREADS | (absent) | <threads> | e2-only |
| long | jgi_summarize_bam_contig_depths | threads | 12 | 8 | threads-differ |
| long | metabat2 | threads | 12 | 8 | threads-differ |
| long | semibin2 | threads | 6 | 8 | threads-differ |
| long | comebin | threads | 12 | 48 | threads-differ |
| long | das_tool | threads | 12 | 16 | threads-differ |
| long | checkm2 | -x | (absent) | fa | e2-only |
| long | checkm2 | threads | 6 | 8 | threads-differ |
| short | slurm | process.array | 0 | 25 | reported |
| long | slurm | process.array | 0 | 25 | reported |
| long | slurm | sbatch --array (B19 rerun) | 0-40%5 | n/a | reported |

## Assemblies

No sample's assembly is identical between the runs. Every pair differs by file hash and by the hash of its sorted sequence set. The weight is in how much they differ, measured per sample:

- **bp fraction in sequence-identical contigs.** The fraction of bases in contigs whose sequence, on either strand, also appears in the other run's assembly. Each sample reports the lower of E1's and E2's fraction.
- **bp fraction identical or covered at >=99% identity.** The same, plus the bases of unshared contigs that minimap2 `asm5` alignments to the other assembly cover at 99% identity or better. Coverage is the union of all such alignments.
- **Contig count difference** and **relative total length difference.** Absolute values.

Fractions print to four decimals, so a sample at 1.0000 still differs. A zero contig count difference means the counts agree, not the contigs.

| arm | samples | identical | metric | min | p10 | median | p90 | max |
|---|---|---|---|---|---|---|---|---|
| short | 208 | 0 | bp fraction in sequence-identical contigs | 0.8791 | 0.9681 | 0.9924 | 0.9996 | 1.0000 |
| short | 208 | 0 | bp fraction identical or covered at >=99% identity | 0.9683 | 0.9961 | 0.9994 | 1.0000 | 1.0000 |
| short | 208 | 0 | contig count difference | 0 | 0 | 8 | 32 | 118 |
| short | 208 | 0 | relative total length difference | 0.0e+00 | 4.9e-07 | 3.2e-05 | 1.8e-04 | 5.5e-04 |
| long | 41 | 0 | bp fraction in sequence-identical contigs | 0.0303 | 0.0357 | 0.1105 | 0.1678 | 0.2218 |
| long | 41 | 0 | bp fraction identical or covered at >=99% identity | 0.7180 | 0.7377 | 0.8045 | 0.9308 | 0.9542 |
| long | 41 | 0 | contig count difference | 2 | 5 | 21 | 43 | 69 |
| long | 41 | 0 | relative total length difference | 1.6e-04 | 6.4e-04 | 2.0e-03 | 5.7e-03 | 9.8e-03 |

On the short arm the reads are identical, so the difference arises inside MEGAHIT. Its thread count and memory are the only configured differences. The worst sample is `toy_hmp_airskinurogenital_sample_13`.

On the long arm the reads are not identical, because E1 removed lambda-phage reads and E2 did not. Flye also ran on different thread counts. Little exact sequence survives, but most bases still align at 99% identity or better.

## MAGs matched across runs

`skani dist` compares every E2 MAG with every E1 MAG of the same sample and binner. A MAG's partner is the MAG in the other run that maximises ANI × min(AF_E1, AF_E2). Two MAGs match when each is the other's partner, the ANI is at least 95 and both alignment fractions are at least 50%.

- `matched_pct` is the share of each run's MAGs that have a match.
- `unmatched_high` counts high-quality MAGs with no match.
- `matched_ani_median` and `median_abs_dcompleteness` describe matched pairs. The completeness difference is in percentage points.
- `tier_changed` counts matched pairs whose quality tier differs. `dcompleteness_gt10` counts pairs whose completeness differs by more than 10 points.

| arm | binner | mags (E1 / E2) | matched_pct (E1 / E2) | unmatched_high (E1 / E2) | matched_pairs | matched_ani_median | median_abs_dcompleteness | tier_changed | dcompleteness_gt10 |
|---|---|---|---|---|---|---|---|---|---|
| short | DASTool | 2594 / 2614 | 94.6 / 93.9 | 16 / 14 | 2455 | 100.0000 | 0.21 | 183 | 72 |
| short | COMEBin | 7593 / 7583 | 74.8 / 74.9 | 8 / 11 | 5680 | 100.0000 | 0.94 | 416 | 300 |
| short | MetaBAT2 | 4294 / 4300 | 96.5 / 96.4 | 0 / 1 | 4145 | 100.0000 | 0.17 | 195 | 88 |
| short | SemiBin2 | 6626 / 6649 | 90.9 / 90.6 | 0 / 4 | 6024 | 100.0000 | 0.56 | 196 | 50 |
| long | DASTool | 567 / 567 | 90.8 / 90.8 | 8 / 1 | 515 | 99.9600 | 1.40 | 75 | 46 |
| long | COMEBin | 1975 / 2106 | 54.3 / 50.9 | 0 / 4 | 1073 | 99.8600 | 2.19 | 126 | 165 |
| long | MetaBAT2 | 1790 / 1791 | 77.1 / 77.1 | 0 / 2 | 1380 | 99.8300 | 1.45 | 142 | 143 |
| long | SemiBin2 | 1335 / 1318 | 72.4 / 73.3 | 2 / 1 | 966 | 99.8550 | 1.55 | 111 | 108 |

The ANI of matched pairs is near 100 by construction and says little. The unmatched share carries the signal. COMEBin matches least on both arms. The control shows this is COMEBin's own variation. On the short arm, with identical assemblies, reads and threads, the two control runs' COMEBin MAGs match each other at 68.7% and 77.7%, no better than E1's and E2's. Every long-arm binner matches less than its short-arm counterpart, which follows the larger assembly difference.

## Unmatched DAS Tool MAGs

A DAS Tool MAG is one binner's bin, sometimes trimmed. Its name gives the source bin. Each unmatched DAS Tool MAG gets one cause:

- **source bin unmatched.** The binner's bin has no partner in the other run.
- **other arm's DAS Tool dropped its partner.** The binner's bin has a partner, but the other run's DAS Tool did not keep it.
- **DAS Tool trimmed them differently.** Both runs' DAS Tool kept the matched source bins, but the trimmed results no longer match.

| arm | pipeline | cause | mags | high_quality |
|---|---|---|---|---|
| long | E1 | source bin unmatched | 14 | 0 |
| long | E1 | source bins matched, other arm's DAS Tool dropped its partner | 38 | 8 |
| long | E2 | source bin unmatched | 15 | 1 |
| long | E2 | source bins matched, other arm's DAS Tool dropped its partner | 37 | 0 |
| short | E1 | source bin unmatched | 28 | 0 |
| short | E1 | source bins matched, DAS Tool trimmed them differently | 3 | 0 |
| short | E1 | source bins matched, other arm's DAS Tool dropped its partner | 108 | 16 |
| short | E2 | source bin unmatched | 42 | 0 |
| short | E2 | source bins matched, DAS Tool trimmed them differently | 2 | 0 |
| short | E2 | source bins matched, other arm's DAS Tool dropped its partner | 115 | 14 |

38 of the 39 high-quality unmatched MAGs are "dropped its partner". The binners agree, and DAS Tool's selection differs. On the short arm it happens in both directions, 16 in E1 against 14 in E2. This fits DAS Tool's selection responding to small input differences, not a defect on one side. The control baseline shows the same cause, 12 against 7, on 21 samples with the assemblies held fixed. On the long arm it is one-sided, 8 against 0. The E1 long DAS Tool inputs include the b19 MetaBAT2 bins, so the inputs differ by design there. The same row holds the one large high-quality gap in the CheckM2 table, 258 against 238.

## Quality tier changes across matched MAGs

Each cell counts matched pairs by the E1 MAG's tier and its E2 partner's tier. The diagonal is agreement.

| arm | E1 tier | E2 high | E2 medium | E2 below |
|---|---|---|---|---|
| short | high | 5824 | 269 | 15 |
| short | medium | 311 | 4557 | 178 |
| short | below | 12 | 205 | 6933 |
| long | high | 794 | 144 | 4 |
| long | medium | 120 | 960 | 94 |
| long | below | 8 | 84 | 1726 |

1,444 of the 22,238 matched pairs change tier, and 39 of them cross two tiers. 972 pairs differ in completeness by more than 10 points.

## Run-to-run control

Two control runs measure how much nf-core/mag disagrees with itself. E1ctl and E1ctl2 rerun E1's configuration on 25 samples, and differ from each other only in their binner seeds:

- **Assemblies.** Both bin E1's own assemblies, passed with `--assembly_input`. All 25 score byte-identical to E1's.
- **Reads.** `--assembly_input` skips nf-core's read QC. Both controls map untrimmed reads, where E1 mapped fastp-trimmed reads. The two controls' contig depths are byte-identical to each other and differ from E1's.
- **Seeds.** E1ctl runs MetaBAT2 at seed 1, as E1 did, and SemiBin2 at seed 2. E1ctl2 runs both at seed 3. COMEBin and DAS Tool expose no seed.
- **Long depth.** `drivers/e1_nfcore/control_long.config` computes long-arm depth with jgi 2.17 at `--percentIdentity 80`, as E1's b19 rerun did. DAS Tool chooses from all three binners, as the b19 rerun did.

E1ctl against E1ctl2 is therefore the seed-only pair, and it is the baseline. A pair against E1 adds the read difference to the seed difference.

The samples are a stratified 10% of each CAMI dataset, rounded to the nearest sample, at least one per dataset: 21 short and 4 long. `drivers/e1_nfcore/control_subset.py` draws them with a fixed seed. `results/compare_arms_ctl/samples.txt` lists them.

CAUTION: nf-core/mag 5.5.0 renders MetaBAT2's `ext.args` as a plain string while it parses its own config (`conf/modules.config:887`). A `params` block in a `-c` file arrives too late, and `--seed` stays 1. Pass seeds on the command line, as `drivers/e1_nfcore/run_e1.sbatch` does. E1ctl asked for MetaBAT2 seed 2 through a config file and ran at seed 1.

CAUTION: an assembly passed with `--assembly_input` loses two fields of the read meta that nf-core/mag 5.5.0 branches on. `control_long.config` corrects both, and its header cites the source lines.

- **Assembler case.** A long assembly reaches the long-read depth process only when the assembler is `FLYE`, but `--assembly_input` accepts only `Flye`. The controls' long assemblies take the short-read depth process, and their bins are named `Flye-*`. `drivers/compare_arms_ctl.sbatch` maps the `Flye-*` names to E1's `FLYE-*`.
- **`lr_platform`.** SemiBin2 picks its read type from `lr_platform`, which the assembly meta lacks. Without the correction SemiBin2 runs in short-read mode on long assemblies.

Matched percentage per pair, first run / second run. `results/control_spread.py` prints it:

| arm | binner | E1ctl vs E1ctl2 | E1 vs E1ctl | E1 vs E1ctl2 | E1 vs E2 |
|---|---|---|---|---|---|
| short | DASTool | 94.1 / 95.2 | 95.6 / 94.1 | 95.2 / 94.8 | 96.0 / 95.2 |
| short | COMEBin | 77.7 / 68.7 | 75.8 / 77.3 | 76.0 / 68.4 | 76.9 / 79.7 |
| short | MetaBAT2 | 97.6 / 99.0 | 93.6 / 93.2 | 93.6 / 94.6 | 96.1 / 96.6 |
| short | SemiBin2 | 95.4 / 97.2 | 95.7 / 94.8 | 94.6 / 95.5 | 90.2 / 90.4 |
| long | DASTool | 96.5 / 94.8 | 94.9 / 98.2 | 96.6 / 98.3 | 86.4 / 91.1 |
| long | COMEBin | 70.8 / 70.4 | 71.0 / 67.0 | 66.0 / 62.0 | 55.5 / 50.5 |
| long | MetaBAT2 | 98.8 / 98.8 | 95.2 / 95.2 | 95.8 / 95.8 | 76.4 / 75.9 |
| long | SemiBin2 | 100.0 / 100.0 | 97.7 / 93.3 | 97.7 / 93.3 | 71.9 / 66.2 |

E1ctl against E1ctl2:

| arm | binner | mags (E1ctl / E1ctl2) | matched_pct (E1ctl / E1ctl2) | unmatched_high (E1ctl / E1ctl2) | matched_pairs | matched_ani_median | median_abs_dcompleteness | tier_changed | dcompleteness_gt10 |
|---|---|---|---|---|---|---|---|---|---|
| short | DASTool | 253 / 250 | 94.1 / 95.2 | 1 / 1 | 238 | 100.0000 | 0.02 | 15 | 11 |
| short | COMEBin | 714 / 808 | 77.7 / 68.7 | 1 / 2 | 555 | 100.0000 | 1.17 | 46 | 48 |
| short | MetaBAT2 | 410 / 404 | 97.6 / 99.0 | 0 / 0 | 400 | 100.0000 | 0.00 | 8 | 3 |
| short | SemiBin2 | 657 / 645 | 95.4 / 97.2 | 0 / 0 | 627 | 100.0000 | 0.06 | 3 | 2 |
| long | DASTool | 57 / 58 | 96.5 / 94.8 | 0 / 0 | 55 | 100.0000 | 0.00 | 0 | 0 |
| long | COMEBin | 212 / 213 | 70.8 / 70.4 | 0 / 0 | 150 | 100.0000 | 0.52 | 8 | 11 |
| long | MetaBAT2 | 165 / 165 | 98.8 / 98.8 | 0 / 0 | 163 | 100.0000 | 0.00 | 0 | 2 |
| long | SemiBin2 | 134 / 134 | 100.0 / 100.0 | 0 / 0 | 134 | 100.0000 | 0.00 | 0 | 0 |

E1 against E2 on the same 25 samples:

| arm | binner | mags (E1 / E2) | matched_pct (E1 / E2) | unmatched_high (E1 / E2) | matched_pairs | matched_ani_median | median_abs_dcompleteness | tier_changed | dcompleteness_gt10 |
|---|---|---|---|---|---|---|---|---|---|
| short | DASTool | 249 / 251 | 96.0 / 95.2 | 0 / 1 | 239 | 100.0000 | 0.22 | 18 | 9 |
| short | COMEBin | 728 / 703 | 76.9 / 79.7 | 1 / 1 | 560 | 100.0000 | 1.00 | 50 | 39 |
| short | MetaBAT2 | 408 / 406 | 96.1 / 96.6 | 0 / 0 | 392 | 100.0000 | 0.16 | 15 | 7 |
| short | SemiBin2 | 651 / 649 | 90.2 / 90.4 | 0 / 0 | 587 | 100.0000 | 0.56 | 21 | 4 |
| long | DASTool | 59 / 56 | 86.4 / 91.1 | 0 / 0 | 51 | 99.9200 | 1.42 | 5 | 4 |
| long | COMEBin | 200 / 220 | 55.5 / 50.5 | 0 / 1 | 111 | 99.7800 | 1.59 | 13 | 13 |
| long | MetaBAT2 | 165 / 166 | 76.4 / 75.9 | 0 / 0 | 126 | 99.7800 | 1.83 | 18 | 14 |
| long | SemiBin2 | 128 / 139 | 71.9 / 66.2 | 0 / 0 | 92 | 99.8500 | 2.36 | 15 | 13 |

- **Short arm.** E2 matches E1 as closely as the two controls match each other for DAS Tool and COMEBin. MetaBAT2 matches 1 to 3 points less. E1 and E2 share its seed and their trimmed reads, but not their assemblies. SemiBin2 matches 5 to 7 points less, which stays unexplained.
- **Reads move MetaBAT2 more than its seed.** E1ctl runs MetaBAT2 at E1's seed, so E1 against E1ctl differs only in the reads. It matches at 93.6% and 93.2% on the short arm. E1ctl against E1ctl2 differs only in the seed, and matches at 97.6% and 99.0%.
- **Long arm.** E2 matches E1 less than any nf-core pair does, for every binner. MetaBAT2 falls to 76.4% and 75.9%, against 95.2% to 98.8%. The long assemblies of E1 and E2 share 11.9% of their bases in identical contigs at the median, and the controls share 100%. The long-arm gap therefore follows the assembly difference, not binning variation.
- **SemiBin2's long-read mode ignores its seed.** Seeds 2 and 3 give identical bins on all four long samples.
- **COMEBin is the noisiest binner** even on identical inputs, at 68.7% to 77.7% between the controls. Its bin count per sample swings both ways: 16 against 47 on `strain_sample_69`, 35 against 10 on `strain_sample_79`.

The long arm has four samples, so its percentages move by several points per sample. Tier changes follow the same pattern: 72 of 1,820 short-arm matched pairs change tier between the controls, and 104 of 1,778 between E1 and E2.

## MAGs per CAMI subset

Counts are per MAG. Tiers use MIMAG's completeness and contamination thresholds on CheckM2 values, without MIMAG's rRNA and tRNA criteria:

- HQ: completeness > 90 and contamination < 5.
- MQ: completeness ≥ 50 and contamination < 10, excluding HQ.
- LQ: everything else.

Quartiles are Q1, median and Q3.

| dataset | arm | binner | samples | bins (E1 / E2) | HQ (E1 / E2) | MQ (E1 / E2) | LQ (E1 / E2) | completeness_q1_med_q3 (E1 / E2) | contamination_q1_med_q3 (E1 / E2) |
|---|---|---|---|---|---|---|---|---|---|
| marine | short | COMEBin | 10 | 1092 / 1067 | 166 / 166 | 244 / 251 | 682 / 650 | 11.4, 32.1, 82.0 / 12.7, 36.0, 84.3 | 0.1, 0.7, 3.1 / 0.1, 0.9, 3.5 |
| marine | short | MetaBAT2 | 10 | 505 / 502 | 145 / 140 | 153 / 152 | 207 / 210 | 41.2, 82.4, 99.2 / 42.1, 82.6, 99.5 | 0.2, 1.2, 4.6 / 0.2, 1.3, 4.6 |
| marine | short | SemiBin2 | 10 | 777 / 781 | 168 / 167 | 206 / 206 | 403 / 408 | 14.5, 51.3, 87.0 / 14.0, 50.5, 87.8 | 0.1, 0.7, 3.1 / 0.1, 0.8, 3.0 |
| marine | short | DASTool | 10 | 319 / 314 | 156 / 160 | 145 / 140 | 18 / 14 | 79.2, 95.2, 100.0 / 82.1, 95.9, 100.0 | 0.7, 2.3, 4.8 / 0.6, 2.3, 4.6 |
| strain | short | COMEBin | 100 | 1824 / 1815 | 324 / 334 | 246 / 230 | 1254 / 1251 | 11.7, 22.2, 79.8 / 11.4, 22.8, 80.9 | 0.2, 1.6, 4.1 / 0.2, 1.5, 4.0 |
| strain | short | MetaBAT2 | 100 | 929 / 931 | 302 / 306 | 240 / 233 | 387 / 392 | 8.5, 84.1, 95.7 / 8.8, 84.6, 95.7 | 0.1, 1.4, 3.8 / 0.1, 1.4, 4.0 |
| strain | short | SemiBin2 | 100 | 1783 / 1797 | 315 / 330 | 249 / 240 | 1219 / 1227 | 6.7, 9.3, 82.6 / 6.7, 9.3, 82.4 | 0.0, 0.1, 1.4 / 0.0, 0.1, 1.3 |
| strain | short | DASTool | 100 | 462 / 464 | 268 / 271 | 174 / 164 | 20 / 29 | 84.4, 95.9, 100.0 / 84.8, 95.7, 100.0 | 1.3, 2.8, 4.5 / 1.2, 2.7, 4.4 |
| toy_hmp_airskinurogenital | short | COMEBin | 29 | 1080 / 1149 | 235 / 235 | 273 / 286 | 572 / 628 | 14.5, 52.4, 92.3 / 16.6, 49.1, 90.7 | 0.1, 1.4, 4.0 / 0.2, 1.5, 4.1 |
| toy_hmp_airskinurogenital | short | MetaBAT2 | 29 | 654 / 660 | 194 / 194 | 196 / 205 | 264 / 261 | 43.3, 82.6, 96.0 / 41.1, 82.7, 96.0 | 0.3, 1.7, 4.3 / 0.3, 1.7, 4.1 |
| toy_hmp_airskinurogenital | short | SemiBin2 | 29 | 958 / 945 | 210 / 210 | 271 / 271 | 477 / 464 | 12.5, 59.7, 90.8 / 12.9, 62.0, 91.2 | 0.1, 1.0, 3.1 / 0.1, 1.0, 3.2 |
| toy_hmp_airskinurogenital | short | DASTool | 29 | 391 / 399 | 241 / 237 | 133 / 140 | 17 / 22 | 88.4, 95.9, 99.9 / 87.2, 95.4, 99.9 | 0.9, 2.7, 4.5 / 0.9, 2.6, 4.3 |
| toy_hmp_gastrooral | short | COMEBin | 20 | 1097 / 1008 | 230 / 241 | 272 / 272 | 595 / 495 | 12.3, 49.3, 91.7 / 15.0, 62.2, 94.5 | 0.2, 1.3, 3.8 / 0.2, 1.8, 4.2 |
| toy_hmp_gastrooral | short | MetaBAT2 | 20 | 608 / 617 | 166 / 164 | 191 / 197 | 251 / 256 | 44.9, 84.6, 96.6 / 41.8, 84.6, 96.2 | 0.3, 1.4, 5.2 / 0.2, 1.4, 5.1 |
| toy_hmp_gastrooral | short | SemiBin2 | 20 | 873 / 887 | 218 / 210 | 237 / 248 | 418 / 429 | 17.6, 64.7, 92.3 / 16.3, 62.2, 91.9 | 0.2, 1.1, 3.3 / 0.2, 1.1, 3.5 |
| toy_hmp_gastrooral | short | DASTool | 20 | 380 / 391 | 231 / 236 | 123 / 127 | 26 / 28 | 88.6, 96.4, 100.0 / 88.2, 96.7, 100.0 | 0.8, 2.3, 4.3 / 0.8, 2.3, 4.4 |
| toy_mousegut | short | COMEBin | 49 | 2500 / 2544 | 681 / 685 | 571 / 596 | 1248 / 1263 | 16.9, 56.2, 96.1 / 16.1, 55.2, 95.7 | 0.3, 1.5, 4.1 / 0.2, 1.4, 3.9 |
| toy_mousegut | short | MetaBAT2 | 49 | 1598 / 1590 | 570 / 570 | 436 / 439 | 592 / 581 | 39.7, 86.7, 98.1 / 39.7, 87.1, 98.3 | 0.2, 1.3, 3.5 / 0.2, 1.2, 3.5 |
| toy_mousegut | short | SemiBin2 | 49 | 2235 / 2239 | 641 / 646 | 524 / 524 | 1070 / 1069 | 13.4, 60.1, 94.8 / 13.4, 60.1, 94.9 | 0.1, 0.8, 2.8 / 0.1, 0.8, 2.8 |
| toy_mousegut | short | DASTool | 49 | 1042 / 1046 | 671 / 675 | 336 / 337 | 35 / 34 | 90.3, 98.0, 99.9 / 89.7, 97.9, 99.9 | 0.6, 2.0, 4.5 / 0.6, 2.0, 4.4 |
| all short | short | COMEBin | 208 | 7593 / 7583 | 1636 / 1661 | 1606 / 1635 | 4351 / 4287 | 13.2, 39.4, 92.0 / 13.9, 41.4, 92.0 | 0.2, 1.4, 3.9 / 0.2, 1.4, 4.0 |
| all short | short | MetaBAT2 | 208 | 4294 / 4300 | 1377 / 1374 | 1216 / 1226 | 1701 / 1700 | 35.0, 84.7, 96.8 / 34.4, 84.8, 97.1 | 0.2, 1.4, 4.1 / 0.2, 1.4, 4.0 |
| all short | short | SemiBin2 | 208 | 6626 / 6649 | 1552 / 1563 | 1487 / 1489 | 3587 / 3597 | 9.4, 42.4, 91.4 / 9.4, 42.4, 91.2 | 0.1, 0.6, 2.6 / 0.1, 0.6, 2.6 |
| all short | short | DASTool | 208 | 2594 / 2614 | 1567 / 1579 | 911 / 908 | 116 / 127 | 87.6, 96.5, 100.0 / 87.4, 96.5, 99.9 | 0.8, 2.4, 4.6 / 0.8, 2.4, 4.4 |
| plant_associated_long_nano | long | COMEBin | 21 | 647 / 642 | 31 / 36 | 106 / 111 | 510 / 495 | 8.2, 32.7, 56.3 / 7.0, 30.0, 57.8 | 0.0, 0.9, 4.1 / 0.0, 0.9, 3.7 |
| plant_associated_long_nano | long | MetaBAT2 | 21 | 645 / 653 | 43 / 43 | 98 / 101 | 504 / 509 | 5.8, 27.0, 59.8 / 5.9, 24.8, 59.4 | 0.0, 0.7, 4.3 / 0.0, 0.6, 4.3 |
| plant_associated_long_nano | long | SemiBin2 | 21 | 447 / 446 | 45 / 44 | 113 / 113 | 289 / 289 | 23.9, 46.8, 74.8 / 21.5, 46.5, 75.9 | 0.3, 2.2, 5.9 / 0.1, 2.0, 6.4 |
| plant_associated_long_nano | long | DASTool | 21 | 144 / 152 | 40 / 41 | 71 / 75 | 33 / 36 | 62.6, 77.9, 95.0 / 55.9, 80.5, 93.7 | 0.4, 1.6, 3.1 / 0.5, 1.4, 3.3 |
| toy_humangut_long | long | COMEBin | 20 | 1328 / 1464 | 206 / 206 | 233 / 236 | 889 / 1022 | 9.9, 26.4, 78.5 / 9.3, 22.4, 73.5 | 0.2, 1.5, 4.5 / 0.1, 1.5, 4.4 |
| toy_humangut_long | long | MetaBAT2 | 20 | 1145 / 1138 | 157 / 154 | 247 / 248 | 741 / 736 | 7.9, 28.2, 78.9 / 7.7, 30.0, 80.1 | 0.0, 0.3, 1.9 / 0.0, 0.3, 2.0 |
| toy_humangut_long | long | SemiBin2 | 20 | 888 / 872 | 212 / 209 | 221 / 216 | 455 / 447 | 17.4, 51.5, 90.8 / 19.1, 53.4, 91.5 | 0.2, 1.2, 3.5 / 0.2, 1.2, 3.4 |
| toy_humangut_long | long | DASTool | 20 | 423 / 415 | 218 / 197 | 169 / 179 | 36 / 39 | 78.8, 91.9, 99.5 / 78.6, 91.0, 99.3 | 0.3, 1.1, 2.8 / 0.3, 1.0, 2.5 |
| all long | long | COMEBin | 41 | 1975 / 2106 | 237 / 242 | 339 / 347 | 1399 / 1517 | 9.5, 29.2, 67.2 / 8.7, 24.0, 64.5 | 0.1, 1.3, 4.5 / 0.1, 1.3, 4.2 |
| all long | long | MetaBAT2 | 41 | 1790 / 1791 | 200 / 197 | 345 / 349 | 1245 / 1245 | 6.7, 27.9, 70.5 / 6.7, 29.2, 70.8 | 0.0, 0.3, 2.5 / 0.0, 0.3, 2.5 |
| all long | long | SemiBin2 | 41 | 1335 / 1318 | 257 / 253 | 334 / 329 | 744 / 736 | 19.4, 48.8, 87.2 / 19.8, 50.0, 87.3 | 0.2, 1.5, 4.2 / 0.2, 1.4, 4.1 |
| all long | long | DASTool | 41 | 567 / 567 | 258 / 238 | 240 / 254 | 69 / 75 | 72.7, 89.9, 99.0 / 72.2, 88.0, 98.7 | 0.3, 1.2, 2.9 / 0.3, 1.2, 2.7 |

The short arm holds five datasets and the long arm holds two. `strain` contributes 100 of the 208 short samples and dominates the short totals. Compare E1 with E2 within a row, never one arm with the other.

## Jobs and execution time

Each row counts every Slurm job attempt the arm submitted, including failed attempts and runs later superseded. `elapsed_h` sums wall time over attempts. `cpu_h` multiplies each attempt's wall time by its allocated CPUs. `first_submit` to `last_end` is the calendar span, including the pauses between resumed sessions.

| pipeline | arm | scope | run_keys | jobs_submitted | completed | failed | cancelled | elapsed_h | cpu_h | first_submit | last_end |
|---|---|---|---|---|---|---|---|---|---|---|---|
| E1 | short | pipeline | 59661599 59679901 59758439 59760798 59868882 59896688 59906444 unattributed | 7122 | 6964 | 125 | 33 | 1030 | 10805 | 2026-09-13T02:40 | 2026-09-17T02:17 |
| E1 | long | pipeline | 59634610 b19 | 1452 | 1442 | 10 | 0 | 119 | 972 | 2026-09-13T02:40 | 2026-09-14T21:23 |
| E2 | short | pipeline | MjMN02CK WfOlaqLT sxDeVO5L | 2861 | 2676 | 138 | 47 | 911 | 30167 | 2026-09-13T02:40 | 2026-09-15T14:35 |
| E2 | short | scoring | MjMN02CK WfOlaqLT sxDeVO5L | 1116 | 1026 | 90 | 0 | 49 | 140 | 2026-09-13T04:29 | 2026-09-15T14:42 |
| E2 | long | pipeline | 33hlLu8Q F1yIPPmC VgUw0A7c | 669 | 485 | 184 | 0 | 118 | 2328 | 2026-09-13T02:49 | 2026-09-15T14:00 |
| E2 | long | scoring | 33hlLu8Q F1yIPPmC VgUw0A7c | 205 | 205 | 0 | 0 | 5 | 13 | 2026-09-13T05:46 | 2026-09-15T14:03 |

E2 took less summed wall time than E1 on the short arm and about the same on the long arm. It reserved 2.4 to 2.8 times the CPU-hours, and COMEBin accounts for most of that: 48 CPUs on E2 against 12 on E1.

E1's close-out jobs (BAM regeneration, the gapfill, long-arm CheckM2) are not in these tables. E2's scoring rows are split out so the two `pipeline` rows compare like for like.

CAUTION: Do not compare job counts per tool. E2 batches up to 200 MAGs per CheckM2 job, and nf-core runs one CheckM2 job per bin set. Some E2 transform totals span more than one run, which is why E2 `das_tool` shows 420 on the short arm.

## nf-core processes against metasmith steps

| arm | e2_transform | nfcore_processes | completed_tasks (E1 / E2) |
|---|---|---|---|
| short | bowtie2_binning_bam | BOWTIE2_ASSEMBLY_ALIGN + BOWTIE2_ASSEMBLY_BUILD | 506 / 442 |
| short | checkm2 | CHECKM2_PREDICT | 1039 / 146 |
| short | comebin | COMEBIN | 208 / 208 |
| short | das_tool | DASTOOL | 208 / 420 |
| short | fastp | FASTP | 208 / 208 |
| short | fastqc_raw | FASTQC_RAW | 208 / 208 |
| short | fastqc_trimmed | FASTQC_TRIMMED | 208 / 208 |
| short | megahit | MEGAHIT | 208 / 208 |
| short | metabat2 | JGISUMMARIZEBAMCONTIGDEPTHS + METABAT2 | 419 / 420 |
| short | semibin2 | SEMIBIN_SINGLEEASYBIN | 215 / 208 |
| short | amber | (scoring, outside nf-core/mag) | 0 / 609 |
| short | gold_standard | (scoring, outside nf-core/mag) | 0 / 417 |
| short | (no transform) | BIN_SUMMARY + CONCAT_CHECKM2_TSV + FASTATOCONTIG2BIN + GUNZIP + MAG_DEPTHS + MAG_DEPTHS_SUMMARY + MULTIQC + PRODIGAL + QUAST + RENAME_POSTDASTOOL + RENAME_PREDASTOOL + SEQKIT_STATS + SPLIT_FASTA | 3537 / 0 |
| long | checkm2 | CHECKM2_PREDICT | 164 / 30 |
| long | chopper | CHOPPER | 41 / 82 |
| long | comebin | COMEBIN | 41 / 41 |
| long | das_tool | DASTOOL | 41 / 42 |
| long | flye | FLYE | 41 / 41 |
| long | metabat2 | JGISUMMARIZEBAMCONTIGDEPTHS + METABAT2 + METABAT2_B19_RERUN | 123 / 44 |
| long | minimap2_binning_bam | MINIMAP2_ASSEMBLY_ALIGN + MINIMAP2_ASSEMBLY_INDEX | 82 / 82 |
| long | porechop_abi | PORECHOP_ABI | 41 / 82 |
| long | semibin2 | SEMIBIN_SINGLEEASYBIN | 41 / 41 |
| long | amber | (scoring, outside nf-core/mag) | 0 / 123 |
| long | gold_standard | (scoring, outside nf-core/mag) | 0 / 82 |
| long | (no transform) | B19_MERGE_BOOKKEEPING + B19_ROWS_BOOKKEEPING + BIN_SUMMARY + CONCAT_CHECKM2_TSV + CONCAT_QUAST_SUMMARY + FASTATOCONTIG2BIN + GUNZIP + MAG_DEPTHS + MAG_DEPTHS_SUMMARY + MULTIQC + NANOPLOT_FILTERED + NANOPLOT_RAW + PRODIGAL + QUAST + QUAST_BINS + RENAME_POSTDASTOOL + RENAME_PREDASTOOL + SEQKIT_STATS + SPLIT_FASTA | 827 / 0 |

The map is per step, not per process. nf-core runs 12 tool-bearing processes on the short arm and 11 on the long arm. E2 runs the same tool calls as 10 and 9 transforms, because three transforms each hold two nf-core processes:

- `bowtie2_binning_bam` holds `BOWTIE2_ASSEMBLY_BUILD` and `BOWTIE2_ASSEMBLY_ALIGN`.
- `minimap2_binning_bam` holds `MINIMAP2_ASSEMBLY_INDEX` and `MINIMAP2_ASSEMBLY_ALIGN`.
- `metabat2` holds `JGISUMMARIZEBAMCONTIGDEPTHS` and `METABAT2`.

`METABAT2_B19_RERUN` is E1's hand rerun, not an nf-core process. The `(no transform)` row lists nf-core's bookkeeping, format-conversion and report processes. E2 does the conversions its tools need inside the transform and runs no report steps. `amber` and `gold_standard` were E2's scoring steps, which nf-core does not ship.

## AMBER is not part of the claim

AMBER scores bins against a gold standard. `cami_gold_standard.py` builds that gold standard by majority vote of CAMISIM's read truth onto each run's own contigs. The vote cannot measure assembly error: a chimeric contig receives one genome at full length. AMBER's purity and completeness are therefore binning quality conditional on the assembly, not genome recovery. The two runs have different assemblies, so each is scored against its own gold standard, and the scores do not compare the same thing.

E2 drops its `amber` and `gold_standard` steps for this reason. `results/{e1,e2}/*_amber_*.tsv` stay committed as a record of the runs. The vote also breaks ties arbitrarily, because it sorts by read count only. Adding `genome_id` to the sort key fixes that.

## Where the data lives

- **E2 products.** Chinook `/Workspace_backups/Tony_Liu/fir_bench_e1_e2/e2/` holds one tar per sample: reads, assembly, BAM, four bin sets, contig-to-bin tables and CheckM2. `results/e2/archive_manifest.tsv.gz` lists each file by task-cache shard.
- **E1 archive tars.** `e1/` on chinook holds `e1_short_out.tar`, `e1_long.tar`, their manifests and `long_kept_inputs/`.
- **E1 close-out products.** `e1/e1_close/` on chinook holds the regenerated BAMs, the gapfill, the rebuilt long bins, the CheckM2 reports and the AMBER outputs.
- **Control products.** `e1ctl/` on chinook holds `e1ctl_short.tar` and `e1ctl_long.tar`, and `e1ctl2/` holds `e1ctl2_short.tar` and `e1ctl2_long.tar`. Each tar holds a run arm's `out/`, `reports/` and `.nextflow.log`, beside its manifest. `drivers/archive_e1ctl.sbatch` builds them. Both runs' `work/` is deleted.
- **Pairwise comparison.** fir `/scratch/phyberos/bench/compare_arms/<sample>/` holds the per-sample files that `results/compare_arms/raw/` gathers. `compare_arms_ctl/`, `compare_arms_ctl2/` and `compare_arms_ctl_ctl2/` do the same for the three control pairs.
- **Originals on fir.** The same files stay on fir under `/scratch/phyberos/bench/`.

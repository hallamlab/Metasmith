# E1 vs E2: metasmith reproduces nf-core/mag

## Purpose & Contents

This document records the evidence that E2 reproduces E1. E1 is nf-core/mag 5.5.0, and E2 is metasmith solving the same workflow. Both ran on the same 249 CAMI samples. The document holds four tables and the caveats a reader needs to interpret them:

- MAGs per CAMI subset
- AMBER per CAMI subset
- jobs and execution time
- nf-core processes against metasmith transforms

Paths are relative to `research/metasmith_benchmark/`. `E1_E2_COMPARISON.md` holds the earlier plan-shape and runtime comparison. `TOOL_FOR_TOOL.md` holds the per-argument parity audit.

## Result

Metasmith solves nf-core/mag's topology process for transform and produces the same results.

- **Transforms.** Each nf-core process that runs a tool maps to one E2 transform. There are 10 on the short arm and 9 on the long arm.
- **Bin counts.** Each arm-by-binner total agrees within 1.3%, except long COMEBin at 6.6%.
- **Quality.** High-quality MAG counts agree within 2.1% everywhere except long DAS Tool, 258 against 238.
- **AMBER.** Median `f1_score_bp` agrees within 0.003 on every short binner and within 0.016 on every long binner. The median paired per-sample difference is within 0.005 on all eight arm-by-binner pairs.

The two runs are not byte-identical. The assemblies already differ slightly between runs (see *The assemblies differ slightly between runs*). Every result downstream inherits that difference.

## Rebuild the tables

Run `python3 research/metasmith_benchmark/results/reproduction.py --markdown` from the repository root. It reads only committed tables and writes `results/reproduction_{mags,amber,jobs,steps}.tsv`. The tables below are its `--markdown` output, unedited.

| input | what it holds | produced by |
|---|---|---|
| `results/e1/e1_checkm2.tsv.gz` | one CheckM2 row per scored E1 bin, 26,774 rows, with `source` = `nfcore`, `gapfill` or `b19_rerun` | `drivers/checkm2_tables.py e1` |
| `results/e2/e2_checkm2.tsv.gz` | one CheckM2 row per E2 bin, 26,928 rows, relabelled to nf-core bin names | `drivers/checkm2_tables.py e2`, read-only over the task cache |
| `results/{e1,e2}/*_amber_summary.tsv` | AMBER per sample and binner, 996 rows each | `drivers/e1_close_amber.sbatch` for E1, E2's own `amber` transform for E2 |
| `results/{e1,e2}/*_slurm_tasks.tsv` | one row per Slurm job attempt | `results/{e1,e2}/build_*_slurm_tables.py` |

## MAGs per CAMI subset

Counts are per bin. Tiers use MIMAG's completeness and contamination thresholds on CheckM2 values, without MIMAG's rRNA and tRNA criteria:

- HQ: completeness ≥ 90 and contamination < 5.
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
| toy_hmp_airskinurogenital | short | SemiBin2 | 29 | 958 / 945 | 210 / 211 | 271 / 270 | 477 / 464 | 12.5, 59.7, 90.8 / 12.9, 62.0, 91.2 | 0.1, 1.0, 3.1 / 0.1, 1.0, 3.2 |
| toy_hmp_airskinurogenital | short | DASTool | 29 | 391 / 399 | 241 / 237 | 133 / 140 | 17 / 22 | 88.4, 95.9, 99.9 / 87.2, 95.4, 99.9 | 0.9, 2.7, 4.5 / 0.9, 2.6, 4.3 |
| toy_hmp_gastrooral | short | COMEBin | 20 | 1097 / 1008 | 231 / 241 | 271 / 272 | 595 / 495 | 12.3, 49.3, 91.7 / 15.0, 62.2, 94.5 | 0.2, 1.3, 3.8 / 0.2, 1.8, 4.2 |
| toy_hmp_gastrooral | short | MetaBAT2 | 20 | 608 / 617 | 166 / 164 | 191 / 197 | 251 / 256 | 44.9, 84.6, 96.6 / 41.8, 84.6, 96.2 | 0.3, 1.4, 5.2 / 0.2, 1.4, 5.1 |
| toy_hmp_gastrooral | short | SemiBin2 | 20 | 873 / 887 | 218 / 210 | 237 / 248 | 418 / 429 | 17.6, 64.7, 92.3 / 16.3, 62.2, 91.9 | 0.2, 1.1, 3.3 / 0.2, 1.1, 3.5 |
| toy_hmp_gastrooral | short | DASTool | 20 | 380 / 391 | 232 / 236 | 122 / 127 | 26 / 28 | 88.6, 96.4, 100.0 / 88.2, 96.7, 100.0 | 0.8, 2.3, 4.3 / 0.8, 2.3, 4.4 |
| toy_mousegut | short | COMEBin | 49 | 2500 / 2544 | 681 / 685 | 571 / 596 | 1248 / 1263 | 16.9, 56.2, 96.1 / 16.1, 55.2, 95.7 | 0.3, 1.5, 4.1 / 0.2, 1.4, 3.9 |
| toy_mousegut | short | MetaBAT2 | 49 | 1598 / 1590 | 570 / 571 | 436 / 438 | 592 / 581 | 39.7, 86.7, 98.1 / 39.7, 87.1, 98.3 | 0.2, 1.3, 3.5 / 0.2, 1.2, 3.5 |
| toy_mousegut | short | SemiBin2 | 49 | 2235 / 2239 | 641 / 646 | 524 / 524 | 1070 / 1069 | 13.4, 60.1, 94.8 / 13.4, 60.1, 94.9 | 0.1, 0.8, 2.8 / 0.1, 0.8, 2.8 |
| toy_mousegut | short | DASTool | 49 | 1042 / 1046 | 671 / 675 | 336 / 337 | 35 / 34 | 90.3, 98.0, 99.9 / 89.7, 97.9, 99.9 | 0.6, 2.0, 4.5 / 0.6, 2.0, 4.4 |
| all short | short | COMEBin | 208 | 7593 / 7583 | 1637 / 1661 | 1605 / 1635 | 4351 / 4287 | 13.2, 39.4, 92.0 / 13.9, 41.4, 92.0 | 0.2, 1.4, 3.9 / 0.2, 1.4, 4.0 |
| all short | short | MetaBAT2 | 208 | 4294 / 4300 | 1377 / 1375 | 1216 / 1225 | 1701 / 1700 | 35.0, 84.7, 96.8 / 34.4, 84.8, 97.1 | 0.2, 1.4, 4.1 / 0.2, 1.4, 4.0 |
| all short | short | SemiBin2 | 208 | 6626 / 6649 | 1552 / 1564 | 1487 / 1488 | 3587 / 3597 | 9.4, 42.4, 91.4 / 9.4, 42.4, 91.2 | 0.1, 0.6, 2.6 / 0.1, 0.6, 2.6 |
| all short | short | DASTool | 208 | 2594 / 2614 | 1568 / 1579 | 910 / 908 | 116 / 127 | 87.6, 96.5, 100.0 / 87.4, 96.5, 99.9 | 0.8, 2.4, 4.6 / 0.8, 2.4, 4.4 |
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

## AMBER per CAMI subset

The metric is AMBER's `f1_score_bp` in genome-binning mode. The paired difference is E2 minus E1 per sample, and its median is shown. The last column counts samples whose two scores are within 0.02.

| dataset | arm | binner | samples | median_f1_bp (E1 / E2) | median_paired_diff_E2_minus_E1 | samples_within_0.02 |
|---|---|---|---|---|---|---|
| marine | short | COMEBin | 10 | 0.4295 / 0.4559 | +0.0062 | 6 |
| marine | short | MetaBAT2 | 10 | 0.1597 / 0.1652 | -0.0003 | 10 |
| marine | short | SemiBin2 | 10 | 0.1542 / 0.1580 | +0.0018 | 10 |
| marine | short | DASTool | 10 | 0.1734 / 0.1869 | +0.0090 | 5 |
| strain | short | COMEBin | 100 | 0.1215 / 0.1205 | +0.0011 | 84 |
| strain | short | MetaBAT2 | 100 | 0.0611 / 0.0603 | +0.0000 | 98 |
| strain | short | SemiBin2 | 100 | 0.0527 / 0.0525 | +0.0008 | 98 |
| strain | short | DASTool | 100 | 0.0301 / 0.0307 | -0.0002 | 95 |
| toy_hmp_airskinurogenital | short | COMEBin | 29 | 0.5120 / 0.4758 | +0.0001 | 18 |
| toy_hmp_airskinurogenital | short | MetaBAT2 | 29 | 0.3707 / 0.3723 | -0.0000 | 29 |
| toy_hmp_airskinurogenital | short | SemiBin2 | 29 | 0.4078 / 0.4066 | -0.0003 | 29 |
| toy_hmp_airskinurogenital | short | DASTool | 29 | 0.2885 / 0.2646 | -0.0059 | 16 |
| toy_hmp_gastrooral | short | COMEBin | 20 | 0.5286 / 0.5378 | -0.0003 | 14 |
| toy_hmp_gastrooral | short | MetaBAT2 | 20 | 0.4104 / 0.4031 | -0.0009 | 20 |
| toy_hmp_gastrooral | short | SemiBin2 | 20 | 0.4139 / 0.4161 | -0.0003 | 20 |
| toy_hmp_gastrooral | short | DASTool | 20 | 0.3720 / 0.3927 | +0.0091 | 12 |
| toy_mousegut | short | COMEBin | 49 | 0.2993 / 0.2993 | -0.0006 | 43 |
| toy_mousegut | short | MetaBAT2 | 49 | 0.2402 / 0.2394 | -0.0000 | 49 |
| toy_mousegut | short | SemiBin2 | 49 | 0.2463 / 0.2454 | -0.0002 | 49 |
| toy_mousegut | short | DASTool | 49 | 0.2018 / 0.2039 | -0.0004 | 47 |
| all short | short | COMEBin | 208 | 0.1897 / 0.1905 | +0.0004 | 165 |
| all short | short | MetaBAT2 | 208 | 0.1401 / 0.1385 | -0.0001 | 206 |
| all short | short | SemiBin2 | 208 | 0.1274 / 0.1260 | +0.0005 | 206 |
| all short | short | DASTool | 208 | 0.0860 / 0.0889 | -0.0001 | 175 |
| plant_associated_long_nano | long | COMEBin | 21 | 0.8933 / 0.8996 | +0.0058 | 8 |
| plant_associated_long_nano | long | MetaBAT2 | 21 | 0.3720 / 0.3774 | -0.0131 | 13 |
| plant_associated_long_nano | long | SemiBin2 | 21 | 0.4540 / 0.4450 | -0.0005 | 13 |
| plant_associated_long_nano | long | DASTool | 21 | 0.1161 / 0.1143 | -0.0076 | 8 |
| toy_humangut_long | long | COMEBin | 20 | 0.9116 / 0.9051 | +0.0012 | 13 |
| toy_humangut_long | long | MetaBAT2 | 20 | 0.4470 / 0.4474 | +0.0050 | 9 |
| toy_humangut_long | long | SemiBin2 | 20 | 0.5073 / 0.5208 | +0.0042 | 7 |
| toy_humangut_long | long | DASTool | 20 | 0.2901 / 0.2939 | +0.0101 | 7 |
| all long | long | COMEBin | 41 | 0.9047 / 0.9034 | +0.0036 | 21 |
| all long | long | MetaBAT2 | 41 | 0.4065 / 0.4138 | -0.0044 | 22 |
| all long | long | SemiBin2 | 41 | 0.4731 / 0.4715 | +0.0028 | 20 |
| all long | long | DASTool | 41 | 0.2109 / 0.1951 | +0.0029 | 15 |

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

E2 took less summed wall time than E1 on the short arm and about the same on the long arm. It reserved 2.4 to 2.8 times the CPU-hours, and COMEBin accounts for most of that. E2 gives COMEBin 48 CPUs where nf-core gives it 12, which comes to about 26,300 CPU-hours on E2 against 8,500 on E1.

E1's scoring and the E1 close-out jobs (BAM regeneration, the `strain_sample_49` gapfill, long-arm CheckM2 and AMBER) are not in these tables. E2's scoring rows are split out so the two `pipeline` rows compare like for like.

CAUTION: Do not compare job counts per tool. E2 batches up to 200 bins per CheckM2 job, and nf-core runs one CheckM2 job per bin set. Some E2 transform totals span more than one run, which is why E2 `das_tool` shows 420 on the short arm.

## Processes against transforms

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

Every tool-bearing nf-core process maps to one E2 transform: 10 on the short arm and 9 on the long arm. A transform that stands for two nf-core processes is one tool step split in two by nf-core. For example, `bowtie2_binning_bam` covers the index build and the alignment. The `(no transform)` row lists nf-core's bookkeeping, format-conversion and report processes. E2 folds the conversions its tools need into the transform itself and runs no report steps. `amber` and `gold_standard` are the shared scoring instrument, which nf-core does not ship.

## E1 long MetaBAT2 comes from the b19 rerun at jgi 2.17

nf-core's own long-arm MetaBAT2 found no bins. E1's hand-rerun `b19_long_rerun.sbatch` recomputed depth with `jgi_summarize_bam_contig_depths` 2.17 and `--percentIdentity 80`. nf-core/mag pins 2.15 for the depth step, and E2 matches that pin. Every E1 long MetaBAT2 row and every E1 long DAS Tool row carries this deviation. It is E1's deviation, not E2's. `TOOL_FOR_TOOL.md` § B has the evidence.

## E1 long DAS Tool is the b19 three-binner rerun

nf-core's own long-arm DAS Tool chose between two bin sets, COMEBin and SemiBin2, because MetaBAT2 gave it nothing. The b19 rerun ran DAS Tool again over all three. The tables score the b19 set, 567 bins. nf-core's two-binner set of 564 bins is excluded, and it stays archived. The b19 rerun ran no CheckM2 and wrote no bin FASTAs. `drivers/e1_close_checkm2.sbatch` rebuilt the FASTAs from the b19 contig-to-bin map and E1's Flye assembly, then ran nf-core's CheckM2 command on them. These rows carry `source = b19_rerun`.

## E1 short strain_sample_49 is a gapfill

E1's MetaBAT2 found no bins on `strain_sample_49`. Its depth file has a mean depth of 0.006, where a regenerated BAM gives 4.91. E1's depth step read a bad BAM. `drivers/e1_close_gapfill.sbatch` reran nf-core's depth, MetaBAT2, three-binner DAS Tool and CheckM2 commands on a regenerated BAM. MetaBAT2 found 7 bins, the same number as E2, and DAS Tool kept 5. These 12 rows carry `source = gapfill`.

## E1 short BAMs are regenerated

nf-core did not publish E1's BAMs. `drivers/e1_close_bam.sbatch` regenerated all 208 short BAMs with nf-core's bowtie2 command and image against E1's own MEGAHIT assemblies. The input reads are E2's fastp reads. They equal E1's on all 208 samples by read and base count (`results/e1/e1_e2_fastp_compare.tsv`). Every regenerated bowtie2 log matches E1's MultiQC bowtie2 counts exactly (`results/e1/e1_bam_regeneration_check.tsv`). AMBER's gold standard uses these BAMs to place each read on a contig. They change no bin.

## The assemblies differ slightly between runs

No E1 assembly is byte-identical to its E2 counterpart (`results/e1/e1_e2_assembly_compare.tsv`). On the short arm the mean contig-count difference is 13.6 contigs, and the relative difference in total length is 4.3e-5. All 41 long assemblies differ. Bin-level differences between the arms therefore include ordinary run-to-run assembly variation, which no parameter change removes.

## Lambda-phage removal runs on E1 long only

nf-core passes its lambda reference to chopper as `--contam`, and E2's `chopper` transform does not. The expected effect on CAMISIM's simulated reads is near zero, but no one measured it. `TOOL_FOR_TOOL.md` § A has the evidence.

## Thread counts differ on COMEBin, Flye and SemiBin2

COMEBin runs on 12 threads on E1 and 48 on E2. Flye runs on 12 and 16, and SemiBin2 on 6 and 8. COMEBin exposes no seed, and nothing establishes that its result is thread-invariant. The largest bin-count gap in the tables is long COMEBin, 1,975 bins against 2,106. That gap sits almost entirely in LQ bins on `toy_humangut_long`, where HQ counts are equal at 206.

## The dataset mix makes the arm medians incomparable

The short arm holds five datasets and the long arm holds two. `strain` contributes 100 of the 208 short samples and has the lowest AMBER scores, so it pulls the short-arm medians down. Compare E1 with E2 within a row, never one arm with the other.

## Gold-standard ties add up to 0.003 of noise

`cami_gold_standard.py` labels each contig with the genome that contributes the most reads. It sorts by read count only, so a tie resolves arbitrarily. Two scorings of the same bins can therefore differ. On the long `sample_0`, 21 of 3,104 contigs changed genome between two runs, and the scores moved by at most 0.003. Both arms use the same script, so the noise is symmetric. Adding `genome_id` to the sort key removes it.

## Where the data lives

- **E2 products.** Chinook `/Workspace_backups/Tony_Liu/fir_bench_e1_e2/e2/` holds one tar per sample: reads, assembly, BAM, four bin sets, contig-to-bin tables and CheckM2. `results/e2/archive_manifest.tsv.gz` lists each file by task-cache shard.
- **E1 archive tars.** `e1/` on chinook holds `e1_short_out.tar`, `e1_long.tar`, their manifests and `long_kept_inputs/`.
- **E1 close-out products.** `e1/e1_close/` on chinook holds the regenerated BAMs, the gapfill, the rebuilt long bins, the CheckM2 reports and the AMBER outputs.
- **Originals on fir.** The same files stay on fir under `/scratch/phyberos/bench/`.

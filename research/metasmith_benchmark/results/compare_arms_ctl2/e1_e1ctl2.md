## assemblies

| arm | samples | identical | metric | min | p10 | median | p90 | max |
|---|---|---|---|---|---|---|---|---|
| short | 21 | 21 | bp fraction in sequence-identical contigs | 1.0000 | 1.0000 | 1.0000 | 1.0000 | 1.0000 |
| short | 21 | 21 | bp fraction identical or covered at >=99% identity | 1.0000 | 1.0000 | 1.0000 | 1.0000 | 1.0000 |
| short | 21 | 21 | contig count difference | 0 | 0 | 0 | 0 | 0 |
| short | 21 | 21 | relative total length difference | 0.0e+00 | 0.0e+00 | 0.0e+00 | 0.0e+00 | 0.0e+00 |
| long | 4 | 4 | bp fraction in sequence-identical contigs | 1.0000 | 1.0000 | 1.0000 | 1.0000 | 1.0000 |
| long | 4 | 4 | bp fraction identical or covered at >=99% identity | 1.0000 | 1.0000 | 1.0000 | 1.0000 | 1.0000 |
| long | 4 | 4 | contig count difference | 0 | 0 | 0 | 0 | 0 |
| long | 4 | 4 | relative total length difference | 0.0e+00 | 0.0e+00 | 0.0e+00 | 0.0e+00 | 0.0e+00 |

## matching

| arm | binner | mags (E1 / E1ctl2) | matched_pct (E1 / E1ctl2) | unmatched_high (E1 / E1ctl2) | matched_pairs | matched_ani_median | median_abs_dcompleteness | tier_changed | dcompleteness_gt10 |
|---|---|---|---|---|---|---|---|---|---|
| short | DASTool | 249 / 250 | 95.2 / 94.8 | 1 / 0 | 237 | 100.0000 | 0.04 | 17 | 9 |
| short | COMEBin | 728 / 808 | 76.0 / 68.4 | 1 / 1 | 553 | 100.0000 | 1.19 | 51 | 50 |
| short | MetaBAT2 | 408 / 404 | 93.6 / 94.6 | 0 / 0 | 382 | 100.0000 | 0.16 | 16 | 13 |
| short | SemiBin2 | 651 / 645 | 94.6 / 95.5 | 0 / 0 | 616 | 100.0000 | 0.08 | 8 | 3 |
| long | DASTool | 59 / 58 | 96.6 / 98.3 | 0 / 0 | 57 | 100.0000 | 0.00 | 4 | 3 |
| long | COMEBin | 200 / 213 | 66.0 / 62.0 | 0 / 1 | 132 | 100.0000 | 0.75 | 9 | 11 |
| long | MetaBAT2 | 165 / 165 | 95.8 / 95.8 | 0 / 0 | 158 | 100.0000 | 0.00 | 2 | 6 |
| long | SemiBin2 | 128 / 134 | 97.7 / 93.3 | 0 / 0 | 125 | 100.0000 | 0.00 | 4 | 5 |

## dastool_unmatched_causes

| arm | pipeline | cause | mags | high_quality |
|---|---|---|---|---|
| long | E1 | source bins matched, other arm's DAS Tool dropped its partner | 2 | 0 |
| long | E1ctl2 | source bins matched, other arm's DAS Tool dropped its partner | 1 | 0 |
| short | E1 | source bin unmatched | 3 | 0 |
| short | E1 | source bins matched, other arm's DAS Tool dropped its partner | 9 | 1 |
| short | E1ctl2 | source bin unmatched | 3 | 0 |
| short | E1ctl2 | source bins matched, other arm's DAS Tool dropped its partner | 10 | 0 |

## tier_changes

| arm | E1 tier | E1ctl2 high | E1ctl2 medium | E1ctl2 below |
|---|---|---|---|---|
| short | high | 535 | 24 | 2 |
| short | medium | 28 | 466 | 17 |
| short | below | 0 | 21 | 695 |
| long | high | 89 | 2 | 0 |
| long | medium | 5 | 120 | 5 |
| long | below | 0 | 7 | 244 |


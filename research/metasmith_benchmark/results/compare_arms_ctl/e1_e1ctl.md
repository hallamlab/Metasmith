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

| arm | binner | mags (E1 / E1ctl) | matched_pct (E1 / E1ctl) | unmatched_high (E1 / E1ctl) | matched_pairs | matched_ani_median | median_abs_dcompleteness | tier_changed | dcompleteness_gt10 |
|---|---|---|---|---|---|---|---|---|---|
| short | DASTool | 249 / 253 | 95.6 / 94.1 | 2 / 0 | 238 | 100.0000 | 0.08 | 18 | 9 |
| short | COMEBin | 728 / 714 | 75.8 / 77.3 | 2 / 1 | 552 | 100.0000 | 1.14 | 45 | 33 |
| short | MetaBAT2 | 408 / 410 | 93.6 / 93.2 | 0 / 0 | 382 | 100.0000 | 0.18 | 16 | 14 |
| short | SemiBin2 | 651 / 657 | 95.7 / 94.8 | 0 / 0 | 623 | 100.0000 | 0.07 | 5 | 4 |
| long | DASTool | 59 / 57 | 94.9 / 98.2 | 0 / 0 | 56 | 100.0000 | 0.00 | 4 | 2 |
| long | COMEBin | 200 / 212 | 71.0 / 67.0 | 0 / 0 | 142 | 100.0000 | 0.71 | 6 | 11 |
| long | MetaBAT2 | 165 / 165 | 95.2 / 95.2 | 0 / 0 | 157 | 100.0000 | 0.00 | 2 | 6 |
| long | SemiBin2 | 128 / 134 | 97.7 / 93.3 | 0 / 0 | 125 | 100.0000 | 0.00 | 4 | 5 |

## dastool_unmatched_causes

| arm | pipeline | cause | mags | high_quality |
|---|---|---|---|---|
| long | E1 | source bins matched, other arm's DAS Tool dropped its partner | 3 | 0 |
| long | E1ctl | source bins matched, other arm's DAS Tool dropped its partner | 1 | 0 |
| short | E1 | source bin unmatched | 3 | 1 |
| short | E1 | source bins matched, other arm's DAS Tool dropped its partner | 8 | 1 |
| short | E1ctl | source bin unmatched | 3 | 0 |
| short | E1ctl | source bins matched, DAS Tool trimmed them differently | 1 | 0 |
| short | E1ctl | source bins matched, other arm's DAS Tool dropped its partner | 11 | 0 |

## tier_changes

| arm | E1 tier | E1ctl high | E1ctl medium | E1ctl below |
|---|---|---|---|---|
| short | high | 534 | 23 | 2 |
| short | medium | 20 | 478 | 17 |
| short | below | 1 | 21 | 699 |
| long | high | 90 | 1 | 0 |
| long | medium | 4 | 122 | 3 |
| long | below | 0 | 8 | 252 |


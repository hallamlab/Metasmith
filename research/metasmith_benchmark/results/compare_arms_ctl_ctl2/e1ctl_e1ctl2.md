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

| arm | binner | mags (E1ctl / E1ctl2) | matched_pct (E1ctl / E1ctl2) | unmatched_high (E1ctl / E1ctl2) | matched_pairs | matched_ani_median | median_abs_dcompleteness | tier_changed | dcompleteness_gt10 |
|---|---|---|---|---|---|---|---|---|---|
| short | DASTool | 253 / 250 | 94.1 / 95.2 | 1 / 1 | 238 | 100.0000 | 0.02 | 13 | 11 |
| short | COMEBin | 714 / 808 | 77.7 / 68.7 | 1 / 2 | 555 | 100.0000 | 1.17 | 46 | 48 |
| short | MetaBAT2 | 410 / 410 | 100.0 / 100.0 | 0 / 0 | 410 | 100.0000 | 0.00 | 0 | 0 |
| short | SemiBin2 | 657 / 645 | 95.4 / 97.2 | 0 / 0 | 627 | 100.0000 | 0.06 | 3 | 2 |
| long | DASTool | 57 / 58 | 98.2 / 96.6 | 0 / 0 | 56 | 100.0000 | 0.00 | 0 | 0 |
| long | COMEBin | 212 / 213 | 70.8 / 70.4 | 0 / 0 | 150 | 100.0000 | 0.52 | 8 | 11 |
| long | MetaBAT2 | 165 / 165 | 100.0 / 100.0 | 0 / 0 | 165 | 100.0000 | 0.00 | 0 | 0 |
| long | SemiBin2 | 134 / 134 | 100.0 / 100.0 | 0 / 0 | 134 | 100.0000 | 0.00 | 0 | 0 |

## dastool_unmatched_causes

| arm | pipeline | cause | mags | high_quality |
|---|---|---|---|---|
| long | E1ctl | source bins matched, other arm's DAS Tool dropped its partner | 1 | 0 |
| long | E1ctl2 | source bins matched, other arm's DAS Tool dropped its partner | 2 | 0 |
| short | E1ctl | source bin unmatched | 2 | 0 |
| short | E1ctl | source bins matched, DAS Tool trimmed them differently | 1 | 0 |
| short | E1ctl | source bins matched, other arm's DAS Tool dropped its partner | 12 | 1 |
| short | E1ctl2 | source bin unmatched | 5 | 0 |
| short | E1ctl2 | source bins matched, other arm's DAS Tool dropped its partner | 7 | 1 |

## tier_changes

| arm | E1ctl tier | E1ctl2 high | E1ctl2 medium | E1ctl2 below |
|---|---|---|---|---|
| short | high | 538 | 16 | 0 |
| short | medium | 21 | 489 | 14 |
| short | below | 0 | 11 | 741 |
| long | high | 93 | 1 | 0 |
| long | medium | 2 | 127 | 4 |
| long | below | 0 | 1 | 277 |


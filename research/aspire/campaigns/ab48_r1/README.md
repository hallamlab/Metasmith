# ab48_r1 — ASPIRE over the Hallam lab's AB48 and purify 16S on sockeye

## Purpose & Contents

This campaign tests the ASPIRE rows that cyano_r1 cannot: the MAG lane, several labels per
study, and measurement association. It runs three studies from one driver. This file says
what each study is for and how to re-run it. `JOURNAL.md` records each run and what it found.

| study | samples | tests |
|---|---|---|
| `ab48_e5` | the 2025-07-23 Enrichment5 sequencing run, one instrument | every row, the MAG lane against the 24 AB48 MAGs |
| `ab48` | AB48's historical run and seven lab runs | the same, across eight sequencing runs |
| `purify` | the purify bioreactor samples that carry primers and sensor readings | `measurement_association` |

| file | holds |
|---|---|
| `run_ab48.py` | the driver; `--study` picks one of the three, and its docstring lists the subcommands |
| `check_results.py` | the acceptance checks over a retrieved `ab48_e5` or `ab48` run |

The sample sheets, the bioreactor logs and the read manifests come from capella and are
pinned at `data/aspire/hallam_16s_inputs.dvc`. The reads are the asv_task project's staging
on sockeye. The MAGs are the 95% ANI centroid bins of cyanoverse/ab48's revio assembly,
staged under the campaign root on sockeye with that assembly and its cluster table.

## Re-running

1. Connect to sockeye through the awm `ssh` domain.
2. Side-load the aspire image with `side-load-images` if the image store lacks it.
3. Run `run` and wait for the workflow, then `retrieve` into an empty results directory.
4. Run `fetch-intermediates filter_table asv_mag_link module_mag_anchors`. The checker reads
   these step outputs, which no target carries.
5. Run `check_results.py --intermediates DIR`, with DIR as `fetch-intermediates` printed it.

The July lane's DADA2 results, one directory per sequencing run, are the checker's
independent baseline. Fetch them from capella's `data/asv_task/cohort_results/` into
`cache/aspire/t10_mags/july/`.

**CAUTION** `retrieve` copies into an existing directory without deleting. Retrieve into an
empty one, or the pin mixes two runs.

## What the data can and cannot test

The MAGs and the amplicons come from the same photobioreactor community, not the same
samples. A pairing shows that a MAG's 16S gene is present in the community. It is not a
same-sample abundance match, so `asv_mag_network` gets no MAG abundance table.

SILVA names the dominant cyanobacterium *Geitlerinema* PCC-7105. GTDB places its MAG, bin
1-15, in *Sodalinema*, the genus split from that group.

Only five purify samples carry sensor readings, and two share one averaging window. Five is
too few for the ordinations to reach significance, so `purify` tests that the row runs on
real measurements, not what the measurements say.

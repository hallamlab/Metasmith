# ab48_r1 — ASPIRE over the Hallam lab's 16S on sockeye

## Purpose & Contents

This campaign tests the ASPIRE rows that cyano_r1 cannot: the MAG lane, several labels per
study, and measurement association. It also builds the lab-wide ASV table and the timeline
page drawn from it. It runs seven studies from one driver. This file says what each study is
for and how to re-run it. `JOURNAL.md` records each run and what it found.

| study | samples | tests |
|---|---|---|
| `ab48_e5` | the 2025-07-23 Enrichment5 sequencing run, one instrument | every row, the MAG lane against the 24 AB48 MAGs |
| `ab48` | AB48's historical run and seven lab runs | the same, across eight sequencing runs |
| `purify` | the purify bioreactor samples that carry primers and sensor readings | `measurement_association` |
| `lab` | every non-control V4–V5 sample: AB48, the 2025 purify runs, the Nostoc and Anabaena cultures | the common ASV table, every row at 231 samples |
| `purify_v4` | the March 2026 purify run, which amplified V4 alone | a V4 table of that run alone |
| `lab2` | `lab` plus the July and September 2026 Biofactorial runs | the V4–V5 table at 243 samples |
| `lab_v4` | `lab2` plus `purify_v4`, every merged read cut to 250 nt | one V4 table over every sample in the lab |

| file | holds |
|---|---|
| `run_ab48.py` | the driver; `--study` picks a study, and its docstring lists the subcommands |
| `check_results.py` | the acceptance checks over a retrieved `ab48_e5`, `ab48`, `lab` or `lab2` run |
| `merge_reactor_logs.py` | rebuilds `pbr_logs_merged.csv` from the reactor's exported ReactorLogs folder |
| `report_data.py` | fills `report_template.html` from the `lab_v4_r1` and `lab_r1` pins to make the timeline page |

The sample sheets, the bioreactor logs and the read manifests come from capella and are
pinned at `data/aspire/hallam_16s_inputs.dvc`. The reads are the asv_task project's staging
on sockeye. The MAGs are the 95% ANI centroid bins of cyanoverse/ab48's revio assembly,
staged under the campaign root on sockeye with that assembly and its cluster table.

## Re-running

1. Connect to sockeye through the awm `ssh` domain.
2. Side-load the aspire image with `side-load-images` if the image store lacks it.
3. Run `run` and wait for the workflow, then `retrieve --key KEY` into an empty results directory.
   A transform edit after the run moves the plan key, and `retrieve` without `--key` re-plans.
4. Run `fetch-intermediates filter_table asv_mag_link module_mag_anchors`. The checker reads
   these step outputs, which no target carries.
5. Run `check_results.py --intermediates DIR`, with DIR as `fetch-intermediates` printed it.

The July lane's DADA2 results, one directory per sequencing run, are the checker's
independent baseline. Fetch them from capella's `data/asv_task/cohort_results/` into
`cache/aspire/t10_mags/july/`.

**CAUTION** `retrieve` copies into an existing directory without deleting. Retrieve into an
empty one, or the pin mixes two runs.

**CAUTION** `ab48_e5`, `ab48` and `lab` share one agent home. A launch rewrites its setup
while another study's tasks read it, and one of them can fail. Launch a study after the
others in that home finish.

## What the data can and cannot test

The MAGs and the amplicons come from the same photobioreactor community, not the same
samples. A pairing shows that a MAG's 16S gene is present in the community. It is not a
same-sample abundance match, so `asv_mag_network` gets no MAG abundance table.

SILVA names the dominant cyanobacterium *Geitlerinema* PCC-7105. GTDB places its MAG, bin
1-15, in *Sodalinema*, the genus split from that group.

The March 2026 purify run arrived without primers and amplified V4 alone (515F/806R, about
253 bp). Every other run covers V4–V5 (515F/926R, about 372 bp). Both amplicons start at 515F,
so a V4–V5 read cut to 250 nt is the V4 read of the same template. `lab_v4` sets
`filter.trunc_len` to make that cut before denoising, which puts every run on one set of ASVs.
The cut merges ASVs that differ only in V5. On `lab_r1` that is 240 curated ASVs to 227, and
no merge crosses a genus.

**CAUTION** The two 2026 Biofactorial runs (`purify_2026_07_12_Enrichment`,
`patrik_2026_09_Biofactorial`) miscall a G as an A at two fixed read-2 cycles, 13 and about 40
nt before the 3' end of the amplicon. Read 2 alone covers those positions, so every abundant
taxon in those runs gets a one-base twin ASV in `lab2`. Both positions lie past the V4 cut.
Read those runs from `lab_v4`, not from `lab2`.

Only five purify samples carry sensor readings, and two share one averaging window. Five is
too few for the ordinations to reach significance, so `purify` tests that the row runs on
real measurements, not what the measurements say.

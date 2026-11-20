# cyano_r1 — ASPIRE over PRJNA801777 on sockeye

## Purpose & Contents

This campaign tests the ASPIRE port end to end on real reads. It runs every row except the
MAG lane over 18 public V4 16S libraries (515F/806R, MiSeq 2x301) from the phycosphere of
*Anabaena* and *Microcystis* cultures. It is also the study the port is compared against
upstream ASPIRE on, by `../upstream_cyano.py`. This file says what the campaign needs and
how to re-run it. `JOURNAL.md` records each run and what it found.

| file | holds |
|---|---|
| `run_cyano.py` | the driver: staging, the run, status and retrieval, in the order its docstring lists |
| `samples.tsv` | the 18 ENA runs: accession, alias, read count, sizes and md5s |
| `study_metadata.tsv` | the study sheet: the sample id and one label, `culture` |
| `params.yml` | the ASPIRE preset with this amplicon's primer trims and length window |
| `check_results.py` | the acceptance checks over a retrieved run, one property per analysis |
| `smoke_local.py` | the lane up to the filtered table, on two samples, in local docker |

## Re-running

1. Connect to sockeye through the awm `ssh` domain. Every ssh the driver runs rides that
   connection.
2. Build the aspire image locally with `docker/aspire/dev.sh --build`.
3. Run the driver's subcommands in the order its docstring lists.
4. Run `check_results.py` over the retrieved results in `data/aspire/cyano_r1/`.

The results are DVC-pinned at `data/aspire/cyano_r1.dvc`.

**CAUTION** Check the ssh control connection with `ssh -O check sockeye` before any plain
ssh. A probe over a dead connection starts a fresh login that the Duo push never reaches.

## What the data can and cannot test

**The culture labels are swapped for 16 of 18 samples in the deposited metadata.** The raw
reads show it before any processing, so the ASV table is faithful and the labels are not.
`study_metadata.tsv` keeps the depositor's labels. Use this study to test that the pipeline
runs and counts correctly, not to test indicator species.

The study sequenced the bacteria living with each culture. In 6 of 18 samples they outweigh
the cyanobacterium itself.

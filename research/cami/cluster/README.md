# CAMI long-read + CAMI III restore

D1's working area for the one CAMI leg it owns: restoring the long-read subtrees for
CAMI II marine, strain and plant_associated, plus both platforms of CAMI III
toy_humangut, all of which had been purged from `/scratch/phyberos/cami/` before this
session. `research/cami/manifest.tsv`, `run_cami_metag.py` and `samples.tsv` belong to
other agents and are not touched here.

## Where the mirror actually is

The campaign plan named chinook; this scope's own history also has a "sockeye" quote
(`2.35 TB moved sockeye to fir at 1.63 GB/s in 24 minutes`). These are not the same
claim. Sockeye was a transient high-bandwidth relay used once, during the *original*
external ingest from Wasabi/de.NBI — it holds no persistent copy of this corpus.
The persistent CAMI mirror is on **chinook**, confirmed 2026-09-12 by directly listing
`globus ls` against the guest collection `2602486c-1e0f-47a0-be15-eec1b0ff0f96`
(`ubcarc#chinook`) at `/Resources/external_data/CAMI/`: the five dataset subtrees named
in the engine/cami scope's journal are present there byte-for-byte (`cami1`,
`cami2_challenge`, `cami2_toy_hmp`, `cami2_toy_mousegut`, `cami3_toy_humangut`).

Use explicit `--recursive-depth-limit` on any listing here — the CLI default of 3 has
previously undercounted a listing by 44 files.

## What was restored

`globus_batch_longreads.txt` is the batch transfer spec (source chinook, dest fir
`8dec4129-9ab4-451d-a45f-5b4b8471f7a3`, "computecanada#cedar-globus &
alliancecan#fir-globus"). Short-read CAMI II data was **not** re-fetched — it is already
unpacked on fir under `/scratch/phyberos/cami/work/*_short_read/` and never left.

Measured via `globus ls -r --recursive-depth-limit 4 -F json` before submitting: 632
files, 1,263,079,489,454 bytes (1.263 TB) — smaller than the plan's ~2.4 TB estimate,
which likely assumed short-read CAMI II would need restoring too. `globus_task_log.md`
has the task id; it completed 2026-09-12 (644 subtasks incl. 6 directories succeeded, 0
failed, 1.14 GB/s average).

## Unpacking

Five new sbatch scripts, modelled on the existing `unpack_marine.sbatch` /
`unpack_strain.sbatch` / `unpack_plant.sbatch` on fir (short-read siblings, untouched):

- `unpack_marine_long.sbatch` — array 0-9, `work/marine_long_read/`
- `unpack_strain_long.sbatch` — array 0-99, `work/strain_long_read/`
- `unpack_plant_long.sbatch` — array 0-20, `PLATFORM=nano|pacbio`,
  `work/plant_long_read_<platform>/`
- `unpack_cami3_toyhumangut.sbatch` — array 0-19, `PLATFORM=short|long`,
  `work/toy_humangut_<platform>_read/`

All four ran a single-index test (array 0-0) before the full fan-out, per the existing
convention of skipping `bam.tar.gz` (nothing downstream reads the alignment). Marine,
strain and both plant platforms unpack into exactly the same shape as their short-read
siblings: `reads/anonymous_reads.fq.gz` plus `contigs/{anonymous_gsa.fasta.gz,
binning_gs.tsv,gsa_mapping.tsv.gz}`.

**CAMI III does not.** Its `_gsa.tar.gz` is not a pooled gold-standard assembly + one
binning table — it unpacks to ~311 per-source-genome `sampleN_ASV<id>_gsa.fasta.gz`
files per sample, with no `binning_gs.tsv` analogue anywhere in the archive. This was
discovered from the test-index run, not assumed in advance; the script comment flags it
as unverified against CAMI III's own documentation. Whatever scores CAMI III against its
truth will need to read this shape, not the CAMI II one — that scoring step does not
exist yet (see the engine/cami scope's own journal: "Nothing scores the bins").

All six unpack arrays completed (`sacct`, all COMPLETED, sub-minute per task):

    marine_long_read           48 GB
    strain_long_read          198 GB
    plant_long_read_nano       39 GB
    plant_long_read_pacbio    100 GB
    toy_humangut_long_read     90 GB
    toy_humangut_short_read    94 GB

`toy_humangut_short_read` and `toy_humangut_long_read` are the two new `work/`
subdirectory names for CAMI III — reported to the orchestrator for A4's
`DATASET_LABELS` addition in `research/cami/build_samples.py` (not edited here; that
file belongs to A4).

The raw archives under `cami2_challenge/{marine,strain,plant_associated}/long_read*`
and `cami3_toy_humangut/{long,short}` (1.263 TB) were left in place rather than deleted
after unpacking — refetching them is now a 20-minute Globus transfer rather than a
multi-hour one, so the tradeoff against scratch's 7.7 PB free favours keeping them for
now. Delete them if scratch pressure ever makes that the wrong call.

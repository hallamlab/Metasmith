# cyano_r1 — run journal

## Purpose & Contents

Dated entries for runs of this campaign: what ran, what broke, what the results say. Newest
last. How to run the campaign is in `README.md`, and what each step does is in its transform.

---

## 2026-09-28 — first green run on sockeye, key Jj2MGd10

All 13 steps over the 18 PRJNA801777 samples, targets from `counts_clean` through
`indicspecies_results`. The final attempt (`logs.2026-09-28_13-27-00`) records 64 tasks,
all COMPLETED, none killed. Earlier attempts executed the counts lane and were served
from the task cache after that. Results are in `data/aspire/cyano_r1/`.

### What the checker says

`check_results.py` passes every criterion but one.

- 57 ASVs in the clean table, 18 sample columns.
- Each sample's raw read count equals ENA's `read_count`. Every stage is no larger than
  the one before: 899,792 raw pairs, 878,058 filtered, 728,191 mapped, 713,265 kept.
- One results table and one summary for `culture`, with no label skipped.
- **Not met: Cyanobacteria is the top phylum in 12 of 18 samples, not all 18.** It is
  28–84% of every sample. The study sequenced the phycosphere, the heterotrophs living
  with each culture, and Proteobacteria or Bacteroidota outweigh the cyanobacterium in
  ANA4, MIC3, MIC5, MIC6, MIC7 and MIC9. The criterion assumed a culture is mostly
  itself. The pipeline is not at fault here.

### ENA's culture labels are swapped for 16 of 18 samples

*Microcystis* dominates ANA2–ANA9 and MIC1. *Nostoc_PCC-7524*, SILVA's genus for
*Anabaena*, dominates ANA1 and MIC2–MIC9. Our sample names are ENA's `sample_alias`
exactly. The raw reads confirm the table: a 30-mer from ASV1 (*Microcystis*) occurs
28,380 times in SRR17818190 (ANA2) and never in SRR17818191 (ANA1), and a 30-mer from
ASV2 (*Nostoc*) is the reverse. So the depositor's metadata swaps ANA2–9 with MIC2–9.

`study_metadata.tsv` keeps the depositor's labels. Relabelling from our own taxonomy
would make indicator species circular. The swap is why no ASV indicates `culture` at
q < 0.05: each label holds eight samples of one culture and one of the other, and the
best indicators reach p ≈ 0.002–0.006 with q ≈ 0.075 over 406 raw ASVs.

### What broke on the way, and the fixes

1. **The engine's cache-replay process died at compile time.** It declares
   `executor 'local'`. The slurm preset's catch-all `process { array = N }` still applied,
   and Nextflow refuses a local job array. Every slurm run with the task cache fails this
   way before any task starts, and it still reports `run completed`. `nextflow_codegen.py`
   now emits `array 0` on the twin.
2. **ARB, numba and kaleido all need a writable `$HOME`.** Sockeye runs apptainer with
   `--no-home`. SINA reports this as "the ARB database is likely corrupted". `sina_trim`,
   `taxonomy` and `sankey` export `HOME=$PWD`, and `taxonomy` also sets `NUMBA_CACHE_DIR`.
3. **`mitomaster` wrote through a staged input.** A reference is staged into the work
   directory under its own file name, so writing `contaminants.fasta` wrote into `/arc`,
   which is read-only on compute nodes. Its temporary files now carry a prefix.
4. **`plot_metadata` failed twice on real data.** The mito block raises when there is no
   mitochondrial ASV, which upstream never meets on host-associated samples, so it now runs
   only when that table has rows. Under pandas 2.2 the taxonomy merge also drops the
   `ASV_ID` index name, and the vendored script now restores it.
5. **Staging.** Building a sif on the sockeye login node took 45 minutes on GPFS, and a
   dropped ssh session killed it. `side-load-images` now builds every image the plan needs
   locally, then rsyncs each sif into the store and stamps it. The ControlMaster dropped
   twice during long transfers, so every ssh and rsync here runs with `BatchMode`.
6. **`check_tasks` read every attempt**, so a run that needed a retry could never be
   retrieved. It takes `attempt="latest"` now.

SINA builds a 248 MB search index (`silva.sidx`) on first use, which takes about ten
minutes. One built locally from the same ARB file sits in the sockeye bundle, so runs
skip that build.

---

## 2026-09-29 — checked against upstream, and the run's DAG

Every step up to the filtered ASV table uses upstream's commands, flags and shipped
values, and the six scripts that decide the table are upstream's unmodified. Curation
differs in three places. `docs/metasmith_libraries/ASPIRE_PORT.md` lists them. No run has
yet diffed the port's table against upstream's on these reads.

The ENA mapping was re-checked: every run's `sample_alias` equals its `library_name`, and
our names match both. The label swap is in the deposit.

`presets/tool_defaults.yml` sets `filter.max_ee: 0`, which `merge_and_filter_reads` passes
as `--fastq_maxee 0`, and vsearch then drops almost every read. This run used the ASPIRE
preset's 1.0 and is unaffected. Fixing it is left for a housekeeping pass.

`run_cyano.py dag` re-plans to the same key, `Jj2MGd10`, and draws the three views into
`reports/`.

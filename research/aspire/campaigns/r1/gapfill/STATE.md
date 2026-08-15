# Gap-fill state — complete

The gap-fill is done: 34 samples × 3 binners all attempted, the catalogue and
census agree, and the result is published. Nothing here is in flight. What
remains open is the Arbutus taxonomy, which is blocked on infrastructure that is
not in this workspace — see *Blocked, not deferred*.

Regenerate the authoritative view rather than trusting this file:

    ssh fir 'cd /scratch/phyberos/gmcf3495/gapfill && bash finish.sh QkqCNJOo hEYVT7HY'
    ssh fir 'squeue -u phyberos -o "%.12i %.26j %.8T %.10M %.12l"'

`finish.sh` is idempotent and is the only thing that should be run by hand. It
sweeps COMEBin strays, collects bins, submits any missing CheckM2, rebuilds the
catalogue and census, redoes skani, and asserts the census and catalogue agree.
It stops rather than building a catalogue over unscored bins.

Everything below is as of 2026-08-06 ~01:15 PDT.

## Published — and verified

`~/project-rpp/steven_c_gmcf3495/metagenomics` on fir: 692 GB, 881 inodes, README
written. Every reads, assembly, annotation and read-taxonomy layer is 34/34, and
every sample's ProteinBERT index row count equals its ORF count. The bin layers
carry 67 quality fastas in 13 per-sample tarballs and 13 cluster tables naming 39
clusters — the same counts the catalogue reports. Verified by walking the tree,
not by the publisher's exit status — the README's layout table is now generated
from the tree for the same reason. Leaf counts under `binning/` are *samples*,
not bins: the fan-outs are tarred per sample to stay inside the inode quota.

    publish_r1.py run --force --replace                    # main run + catalogue
    publish_r1.py --task-key hEYVT7HY --no-catalogue run --force   # S13/S22 layer
    publish_r1.py readme

`--force` is for the two blocked Arbutus campaigns and nothing else; check the
problem list is still exactly those two before using it. `--replace` is mandatory
on a re-publish. Global flags go **before** the subcommand.

## Sizing a comebin run

**comebin's runtime is a formula** for the *training* phase, validated against
S31 (2,321 usable contigs, 400 progress bars, 21 m 27 s at 64 CPUs):

    wall ≈ 400 × ceil(usable_contigs / 1024) × ~2 s

Use it before choosing a wall clock. It is why S25 and S9 were killed at 1 h 40 m
and resubmitted — 24 h was short by a factor of two. It says nothing about
clustering, which is where all three actually died.

## Settled

| | |
|---|---|
| bins | 1140 in the catalogue, across both runs |
| quality MAGs | 67 (completeness ≥50, contamination ≤10) over 13 samples |
| clusters | 39 species-level (95% ANI, per sample) |
| census | 102 cells; 60 BINS, 42 ZERO — **0 never-attempted** |

Every one of 34 samples × 3 binners was attempted, which was the acceptance
criterion, and **every zero carries a cause read off the tool's own output** —
20 `COMEBIN_NO_MARKER_SEEDS`, 11 `METABAT2_NO_BINS_FORMED`, 7
`SEMIBIN_NO_BINS_PRE_RECLUSTER`, 4 degenerate assemblies. That distinction is the
whole point of the census: a task that is never created cannot fail, which is how
S29 and S19 sat unbinned and unnoticed for days.

Bins are not evenly worth having. The three recovered comebin runs added 134 bins
but only 5 quality MAGs — and those 5 were worth the recovery anyway, because 4 of
them are *cluster centroids*, i.e. species representatives nothing else in the set
covers. Judge a binner by centroids, not by bin count.

## The Leiden sweep deadlocks

All three surviving comebin jobs died the same way, and it is not a timeout.
Training succeeded (14–25 h each); then `main.py bin` parked with its parent and
all 64 pool workers in `futex_do_wait`, no file written for 7–12 h, and **11–13
seconds of CPU across a 5-hour window**. The parameter sweep had completed
95/64/48 of its 120 Leiden points — a different count each time, so it is a race,
not a bad parameter combination.

`cluster.py` builds `multiprocessing.Pool(num_threads)` and `apply_async`s 120
tasks, each pickling `norm_embeddings` plus the two ~200 MB kNN arrays. But the
likely trigger is the preceding line, not the pool: `seed_kmeans_full` constructs
`KMeans(n_jobs=-1)`, whose joblib/loky backend leaves worker processes and
threads live in the parent — and the deadlocked parent still had exactly such a
loky process sitting in `pipe_read`. Forking 64 children from a threaded parent
inherits locked mutexes without the threads that would release them.

That diagnosis is what makes the resume worth trying: `seed_kmeans_full`,
`run_leiden` and `run_comebin.sh` all skip work whose output already exists, so a
resumed run never constructs the k-means, never starts loky, and forks clean.

**It works — all three.** Resumed from 95/64/48 of 120, they finished the sweep
and the ensemble in 24 m, 1 h 00 m and 2 h 32 m, writing 30, 50 and 54 bins
against 7–12 h of deadlock apiece and wall clocks they would have hit with
nothing. Net: 62 → 67 quality MAGs, 38 → 39 clusters, and one MAG at 99.12%
complete / 0.09% contamination. Do not read the low marker-seed counts (S13 5,
S9 2, S25 3) as a forecast of few bins; S13 had 5.

Judge a resumed sweep by the Leiden count, not the clock — cost per point scales
with the assembly, so S25 took minutes per point where S13 took seconds. Same
job, same code, a 15× difference in rate that looks exactly like a hang.

    bash submit_comebin_cluster.sh <sample> [cpus] [mem] [time] [threads]

It restores the preserved representation, resumes the sweep where it stalled,
saves `comebin_res` back to `salvage/<S>_progress.tar` on **every** exit path
including the wall clock, and is simply re-run to continue. A watchdog kills the
run if the sweep emits nothing for 60 min — bounded to the sweep, because
`get_result` legitimately runs for hours without touching `cluster_res`.

**Two CPU samples, hours apart, are the only proof of liveness.** One sample says
nothing: `ps` `pcpu` is a lifetime average, so a process that worked for 10
minutes and then hung for 12 hours still reports plausible-looking CPU. `sstat`'s
`TresUsageInTot` is worse — it does not move between samples minutes apart even
on a healthy job. Compare `time=` for the same pid across two checks; and note a
bare `ps -e` on a shared node shows other users' jobs.

`salvage/` holds what cannot be regenerated cheaply: `<S>_comebin_out.tar` (the
trained `embeddings.tsv`, the augmentation set, the Leiden results so far) and
`<S>/` (the marker seed list and FragGeneScan/hmmsearch sidecars, which live next
to `asm.fna` rather than inside `comebin_out` and are easy to miss). It was
copied off `/localscratch` *before* the jobs were cancelled — node-local scratch
dies with the job, and 14–25 h of training with it.

## The proteinbert merges were silently short

`merge_proteinbert` is `group_by=parent_orfs`, and metasmith's `group()` has the
drop race already reported upstream — the one that lost S13 and S22 from the DAG
outright. Here it dropped chunks *inside* a group instead: the merge still ran,
still exited 0, and still published, over fewer chunks than existed. Nothing
downstream said so. It was found by counting merged index rows against ORFs in
the published `.faa`, which is the only check that could have caught it.

Eight chunk pairs were consumed by no merge at all, and the row counts close the
shortfalls exactly — S19 1683 (its whole group was lost, so no merge task existed
at all), S12 4979+4979, S25 4999, S13 4×~4999. Each orphan's ORF ids are fully
contained in exactly one sample's `.faa` and no other, which is what assigns them.

`pbert_remerge.py` re-runs the library's own merge body over the complete chunk
list. Rebuilt products replace rather than patch: `global_row` is recomputed from
scratch, so every row differs, not just the restored tail.

**The general lesson: a step that aggregates a group can come up short without
failing.** Product counts per step catch a missing sample; only counting *rows
against the upstream* catches a short one.

## Nothing is blocked — the dataset is complete

Both Arbutus campaigns ran on 2026-08-06. `arbutus-infra` is not on this node and
a scope for it is not provisionable (its credential and worker key are gitignored,
so a clone cannot authenticate) — but the project exists on **capella**, reachable
by ssh with `secrets/` in place, and the submit scripts are driven in place there.
Stage inputs with `run_arbutus_campaigns.py harvest --from-published`, which reads
the published tree rather than a run directory and therefore attributes every bin
to its binner for free.

The published tree is 693 GB / 995 inodes with all three taxonomy levels on r232,
and it is mirrored to
`chinook:/Manuscripts/Science/2024-09-25_Lung_microbiome/WGA_metagenomics_via_metasmith/`.
`publish_r1.py plan` reports **zero** problems, so `--force` is no longer
appropriate for anything — if you find yourself needing it, something new is wrong.

**Revise the headline before quoting it: 59 prokaryotic MAGs, not 67.** The other
8 carry no prokaryotic marker genes and are >99% *Homo sapiens* by contig; the
dataset is 92.4% Eukaryota overall. See the run log entry of the same date.

## Traps this pass paid for

- **`cp -n` exits 1 when it skips**, on coreutils 9.3 (fir's). Under `set -e` that
  aborted the whole publish at the first destination that already existed, so the
  first publish only worked because the tree was empty. `render_script` emits a
  `cpn()` shell function now — skip-if-present has to be the test, not the copy's
  exit status.
- **A published extension is a claim about the format, and four were wrong**:
  proteinbert was Parquet named `.npy`, its index and kofamscan were
  comma-separated named `.tsv`, read QC stats were JSON named `.txt`. `file` over
  one member of every leaf directory is the check; it takes seconds.
- **A fix to a submission script does not reach queued jobs.** `submit_bin.sh`
  renders a self-contained `job.sh` at submit time. S29's comebin carried the
  pre-staging body for its whole 2 h 36 m life and re-polluted the published
  assembly directory two hours after it was cleaned by hand. The sweep is step 0
  of `finish.sh` so the clean outlives the jobs.
- **`submit_bin.sh` submits.** There is no dry-run, and it opens with
  `rm -rf "$WD"`. Do not invoke it to check syntax — use `bash -n`.
- **Scratch must not be able to delete durable.** `collect_bins.sh` and
  `binner_status.py` both mirrored `out/`, which `submit_bin.sh` destroys. Both now
  read `bins/`, and `finish.sh` step 7 asserts the census and catalogue name the
  same pairs — they disagreed silently once.
- **Never hand COMEBin a published path**: it writes FragGeneScan and hmmsearch
  intermediates next to its input FASTA.
- `--cleanenv` is required (Compute Canada's lmod exports a `which` bash *function*
  that apptainer forwards, and COMEBin locates itself with `dirname $(which …)`),
  and `--no-home` is required (host torch shadows the container's) — which removes
  the implicit cwd bind, so `--bind "$SLURM_TMPDIR" --pwd "$SLURM_TMPDIR"` is too.
- A control run must be deleted after measuring, or it double-counts against that
  sample's pipeline bins. `build_catalogue.py` refuses such a pair and says so.

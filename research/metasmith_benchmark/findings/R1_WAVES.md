# R1 waves

## Purpose & Contents

This file is R1's wave record. Its protocol section says how every wave runs. Below it, each wave has one section: the commit, the launched lanes with their keys and job ids, the failures with causes, what was stopped, and the fixes and gapfills that follow. Cell-by-cell tool status lives in `R1_TABLE_AUDIT.md`.

## Protocol

### Lanes

| Lane | Driver job | Agent home |
|---|---|---|
| E1 short, E1 long | `drivers/e1_nfcore/run_e1.sbatch <checkout> short\|long` | none (nextflow work under `/scratch/phyberos/bench/e1/`) |
| E2 short, E2 long | `e2_cami.py run --arm short\|long` | cami |
| E3 | `e3_pratama.py run` (65 runs) | pratama |
| E4 chunk N | `e4_metagem.py run --chunk N` | metagem |
| E5 cami, pratama, metagem | `e5_pilot.py run --corpus <corpus>` | cami, pratama, metagem |

Every metasmith lane runs as `sbatch -J <lane> $C/research/metasmith_benchmark/drivers/submit_driver.sbatch $C <driver> <args>`, with `C=/scratch/phyberos/bench/checkout/<sha8>`.

### Try

1. Commit, then run `drivers/sync.sh`. It refuses a dirty tree, copies the commit to `$C`, and pushes the dev overlay to all three homes.

   CAUTION run `sync.sh` from the `feat/engine/bench-run` worktree every wave. It copies with preserved mtimes, and a leaf transform id hashes its path and mtime. A clean checkout of the same commit in another tree has different `_metadata` mtimes, so its sync moves every plan key and forks the cache without a single edit.
2. Check the project inode quota with `lfs quota -p 83115734 /scratch` (field 6 against field 8). Launch only if the wave's projection keeps it below 950K.
3. Import, stage and fetch images: run each lane once with `--materialise --import`. It imports the lane's givens, stages the plan, fetches every image the plan names, and exits. Chain the jobs that share a home with `--dependency=afterok`.
4. Launch each lane with `--launch --tag w<N>`, without `--import`, once its materialise job has passed. A lane that does not solve is an issue, not a blocker for the others.

CAUTION a pool is one sqlite database per agent home. Two imports into one home at once fail with `database is locked`. E2 short, E2 long and E5 cami share the cami home, and E3 and E5 pratama share the pratama home, so only step 3 imports, and it runs one job per home at a time.

### Gather

For each failed task, record the lane, the transform, the cause and the fix. Read the cause from `.command.log`, `.command.err` and `.exitcode`, and the cost from `sacct -j <parent ids> -o JobID,JobName,State,MaxRSS,Elapsed`.

CAUTION `sacct`'s `.batch` rows carry MaxRSS under JobName `batch`. Collect the parent job ids from the run's work directory first. A date-and-name filter also matches earlier attempts.

CAUTION sweep for swallowed steps in `<home>/runs/<key>/_metasmith/logs.<timestamp>/agent.log`, which carries `Submitted process`, `Error is ignored` and `Killed`. The driver's sbatch log under `bench/logs/` has none of these lines, so a sweep of it always reads clean. Check that `Submitted process` counts above zero before trusting a zero.

CAUTION retry-then-ignore reports a lane complete with its products missing. Check each lane's products against its sample count.

### Close

Close a wave when every lane has failed, or has passed one real step of each kind (QC, assembly, binning, annotation), or after 48 h.

### Cancel stragglers

1. A stop is per lane, never per task: cancelling a run cancels all its in-flight tasks. Stop a lane only when the next wave's fixes change a step it has not finished. A lane whose remaining steps the fixes do not touch runs on, so its results reach the cache.
2. Stop an E1 head with `scancel --batch --signal=USR1 <job>`. Its trap sends nextflow TERM.
3. Stop a metasmith lane with `scancel --batch --signal=USR1 <job>`, which calls `CancelWorkflow`, or with `drivers/runctl.py cancel <corpus> <key>`.

WARNING USR1 is graceful only for jobs submitted through `submit_driver.sbatch`, whose trap forwards it. A driver launched by the engine route (`METASMITH_DRIVER_SLURM=1`, `start.slurm.sh`) has no trap, so USR1 kills it before nextflow cancels its grid jobs. Stop such a driver by removing its `PID.lock` while the driver is alive.

Check the job's state with `squeue` before choosing the stop. A PENDING driver job has no driver process, no grid jobs and no `PID.lock`, so a plain `scancel` removes it cleanly.

WARNING never use a plain `scancel` on a driver job that has started. It kills the head before nextflow cancels its grid jobs, and those jobs run on with no cache entry.

### Reclaim inodes

The slurm preset keeps every task's work directory (`cleanup = false`), so finished runs hold their inodes. After a lane's run ends and its products are checked, delete that run's nextflow work directory under `<home>/runs/<key>/` as a job. The task cache keeps the results. Check the quota after each deletion, and before every E4 chunk.

### Fix and gapfill

Fix in batches. Add every T21 gapfill that is ready. Rebuild only the library that changed with `library/build.sh <lib>`, because a transform's id hashes its path and mtime. Record which cache entries each fix retires.

### Try again

Relaunch from cache under the next wave's tag.

### Exit

R1 ends when each experiment has its table's targets, or when every open issue waits on outside work.

## Wave 1

Commit `f6d01f00`, checkout `/scratch/phyberos/bench/checkout/f6d01f00`. Driver jobs set `C` to that checkout and `S=$C/research/metasmith_benchmark/drivers/submit_driver.sbatch`.

### Gate

- **Inodes:** reclaiming nine stale run dirs freed 149,336 inodes. The quota read 716,917 before launch.
  - Seven lanes launched first: E1 short and long, E2 short and long, E3, E5 cami and E5 pratama. They fit under 950K without the GTDB tree deletion.
  - E4 chunk 1 and E5 metagem launch after the tree goes, which frees 424,034 inodes. The tree goes once `e4_gtdbtest` classifies a MAG through the squashfs image.
  - Launching in two groups departs from the plan's single launch. The plan's run log records why.
- **GTDB image:** job 59625981 built `release232_skani_genomes.sqfs`, 192 GB. It holds all 199,923 genomes, a count that matches the tree.
- **Probe:** probe2 `Gu9VJmwO` passed at `9525a3a1`.
- **E1 sheets:** `build_samplesheet.py` with its path check found all 498 read files on fir, and regenerated both sheets byte-identical to the committed ones. The offline preflight (59634205) passed:
  - 5.5.0 resolves locally.
  - Both `-preview` runs exit 0, with `control.config` applied.
  - Neither run attempted a fetch.
  - All 60 images are cached.
- **Review:** the adversarial review of `c0b17bb1` is triaged in the plan's run log. Its fixes are in `f6d01f00`.

### Lanes

| Lane | Launch | Materialised | Key | Job |
|---|---|---|---|---|
| E1 short | `sbatch -J e1_short $C/research/metasmith_benchmark/drivers/e1_nfcore/run_e1.sbatch $C short` | n/a (pre-pulled images) | | 59634609 |
| E1 long | `sbatch -J e1_long $C/research/metasmith_benchmark/drivers/e1_nfcore/run_e1.sbatch $C long` | n/a | | 59634610 |
| E2 short | `sbatch -J e2_short $S $C e2_cami.py run --arm short --launch --tag w1` | f6d01f00, 15 steps (59634082) | `WfOlaqLT` | 59634611 |
| E2 long | `sbatch -J e2_long $S $C e2_cami.py run --arm long --launch --tag w1` | f6d01f00, 14 steps (59634083) | `33hlLu8Q` | 59634903 |
| E3 | `sbatch -J e3 $S $C e3_pratama.py run --launch --tag w1` | f6d01f00, 26 steps (59634084) | `Son2YJiI` | 59634612 |
| E4 chunk 1 | `sbatch -J e4_c1 $S $C e4_metagem.py run --chunk 1 --launch --tag w1` | pending, after e4_gtdbtest | | |
| E5 cami | `sbatch -J e5_cami $S $C e5_pilot.py run --corpus cami --launch --tag w1` | c0b17bb1, `52mAOnXS` | | |
| E5 pratama | `sbatch -J e5_pratama $S $C e5_pilot.py run --corpus pratama --launch --tag w1` | c0b17bb1, `8Z7x3L7z` | | |
| E5 metagem | `sbatch -J e5_metagem $S $C e5_pilot.py run --corpus metagem --launch --tag w1` | pending, after E4 chunk 1 | | |

Both E5 lanes relaunch from checkout `c18625a4`, which adds the GPU declaration (see Failures). Nothing under `src/` or either library changed from `f6d01f00`, so their keys hold. The relaunches are jobs 59636769 (E5 pratama) and 59636770 (E5 cami). The first E5 pratama was submitted as job 59635698, after E3 printed `waiting on run`. The first E5 cami was submitted as job 59636004, after E2 long printed `waiting on run 33hlLu8Q`. The GTDB-Tk pass test `e4_gtdbtest` runs as job 59635386, key `Sj7uFNWW`, 4 steps.

Launch lanes that share an agent home one after another. Wait for each to print `waiting on run` before submitting the next, because each launch re-stages into the home.

### Failures and causes

| Lane | Transform | Cause | Fix |
|---|---|---|---|
| E5 pratama, job 59635698, key `8Z7x3L7z` | `functionalAnnotation/clean.py`, step 8 | `RunWorkflow` raised `GpuRequirementError` 37 s after launch, before any grid job. CLEAN declares `Gpus.REQUIRED` with 16 GB, and `stage_and_run` never passed `gpus=`. E5 cami (job 59636004, key `52mAOnXS`) failed the same way at 32 s. | The driver passes `gpus=FIR_GPU`, one MIG 3g.40gb slice (`--gres=gpu:nvidia_h100_80gb_hbm3_3g.40gb:1`), billed to `def-shallam_gpu` through `slurmGpuAccount`. CLEAN stays, because the table lists it for E5. Relaunch both E5 lanes from the fix's checkout. |

| E4 gtdbtest, key `Sj7uFNWW`, and every lane that scores MEMOTE | `metabolicModelling/memote_score.py`, step 4 | `OSError: [Errno 30] Read-only file system: '/home/phyberos'`. cobrapy creates its cache under `$HOME` on import, and `$HOME` points at an unbound path in the container. It is MEMOTE's first run on fir, not a regression, and no branch carried a fix. E5 cami and E5 pratama carry the standard transform and fail the same step this wave. | `library/transforms/modelling/memote_score.py` sets `HOME="$PWD"`. E4 and E5 mask the standard transform. The plan key stays the same: the pinned step has the standard's transform key `PH2q4ubX` and a new protocol source hash. |

CAUTION the solver's `[<type>] resolved by [<library>]` log line names the library that declares the type, not the transform that produces it. Check a pinned step by its `_protocol_source_hash`.

CAUTION `rrg-shallam-ab` has no GPU association, so fir rejects a GPU job under it. The user's only GPU account is `def-shallam_gpu`. CLEAN had never run on fir before wave 1.

CAUTION the engine's GPU check reports a GPU from a failed probe. On the CPU node fc20637, the error quoted nvidia-smi's own failure text as evidence that "a GPU does appear to be present". `_plan_gpu_requests` tests the probe's output for non-empty text rather than a zero exit. Read that sentence as noise until the engine fixes it.

B14 is closed at scale. E2 short's fastp tasks render `--in1`, `--in2`, `--stdout` and `--detect_adapter_for_pe`, with no `--interleaved_in`. All 208 tasks wrote 1,811,472,650 to 4,552,026,605 B of trimmed reads, where the defect wrote 20 B.

### Stopped

- Before the wave, three pre-R1 runs that wave-1 lanes supersede: `iy8YLaGr` (CAMI rung 1), `d6UJuZgF` (Pratama rung 1) and `HQ5SrqFe` (metaGEM li2019). The engine route launched all three, so `scancel --batch --signal=USR1` killed them. That orphaned seven grid jobs: two COMEBin, two DRAM-v (still pending) and three CarveMe. The research agent cancelled all seven by hand. Nothing recoverable was lost, because an orphaned job never writes a cache entry. `C1IM6IG3`'s array covers COMEBin, and E4 covers CarveMe.
- `C1IM6IG3` (CAMI rung 10) keeps running until COMEBin array `59583112` finishes, because it is B5's only source.

### Fixes and gapfills for wave 2

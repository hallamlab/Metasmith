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
2. Check the project inode quota with `lfs quota -p 83115734 /scratch` (field 6 against field 8). Launch only if the wave's projection keeps it below 950K.
3. Import, stage and fetch images: run each lane once with `--materialise --import`. It imports the lane's givens, stages the plan, fetches every image the plan names, and exits. Chain the jobs that share a home with `--dependency=afterok`.
4. Launch each lane with `--launch --tag w<N>`, without `--import`, once its materialise job has passed. A lane that does not solve is an issue, not a blocker for the others.

CAUTION a pool is one sqlite database per agent home. Two imports into one home at once fail with `database is locked`. E2 short, E2 long and E5 cami share the cami home, and E3 and E5 pratama share the pratama home, so only step 3 imports, and it runs one job per home at a time.

### Gather

For each failed task, record the lane, the transform, the cause and the fix. Read the cause from `.command.log`, `.command.err` and `.exitcode`, and the cost from `sacct -j <parent ids> -o JobID,JobName,State,MaxRSS,Elapsed`.

CAUTION `sacct`'s `.batch` rows carry MaxRSS under JobName `batch`. Collect the parent job ids from the run's work directory first. A date-and-name filter also matches earlier attempts.

CAUTION retry-then-ignore reports a lane complete with its products missing. Check each lane's products against its sample count.

### Close

Close a wave when every lane has failed, or has passed one real step of each kind (QC, assembly, binning, annotation), or after 48 h.

### Cancel stragglers

1. A stop is per lane, never per task: cancelling a run cancels all its in-flight tasks. Stop a lane only when the next wave's fixes change a step it has not finished. A lane whose remaining steps the fixes do not touch runs on, so its results reach the cache.
2. Stop an E1 head with `scancel --batch --signal=USR1 <job>`. Its trap sends nextflow TERM.
3. Stop a metasmith lane with `scancel --batch --signal=USR1 <job>`, which calls `CancelWorkflow`, or with `drivers/runctl.py cancel <corpus> <key>`.

WARNING USR1 is graceful only for jobs submitted through `submit_driver.sbatch`, whose trap forwards it. A driver launched by the engine route (`METASMITH_DRIVER_SLURM=1`, `start.slurm.sh`) has no trap, so USR1 kills it before nextflow cancels its grid jobs. Stop such a driver by removing its `PID.lock` while the driver is alive.

WARNING never use a plain `scancel` on a driver job. It kills the head before nextflow cancels its grid jobs, and those jobs run on with no cache entry.

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

- **Inodes:** the project quota read 864,297 of 1M after the w1 materialise jobs, and the wave needs about 212K. Clear GTDB's genome tree, 426,970 inodes, once a GTDB-Tk run through its squashfs image passes. Launch after that.
- **Probe:** probe2 `Gu9VJmwO` passed at `9525a3a1`.
- **E1 sheets:** `build_samplesheet.py` with its path check found all 498 read files on fir, and regenerated both sheets byte-identical to the committed ones. An offline `-preview` of each sheet is pending.
- **Review:** the adversarial review of `c0b17bb1` is triaged in the plan's run log. Its fixes are in `f6d01f00`.

### Lanes

| Lane | Launch | Materialised | Key | Job |
|---|---|---|---|---|
| E1 short | `sbatch -J e1_short $C/research/metasmith_benchmark/drivers/e1_nfcore/run_e1.sbatch $C short` | n/a (pre-pulled images) | | |
| E1 long | `sbatch -J e1_long $C/research/metasmith_benchmark/drivers/e1_nfcore/run_e1.sbatch $C long` | n/a | | |
| E2 short | `sbatch -J e2_short $S $C e2_cami.py run --arm short --launch --tag w1` | f6d01f00, 15 steps (59634082) | `WfOlaqLT` | |
| E2 long | `sbatch -J e2_long $S $C e2_cami.py run --arm long --launch --tag w1` | f6d01f00, 14 steps (59634083) | `33hlLu8Q` | |
| E3 | `sbatch -J e3 $S $C e3_pratama.py run --launch --tag w1` | f6d01f00, 26 steps (59634084) | `Son2YJiI` | |
| E4 chunk 1 | `sbatch -J e4_c1 $S $C e4_metagem.py run --chunk 1 --launch --tag w1` | pending, after e4_gtdbtest | | |
| E5 cami | `sbatch -J e5_cami $S $C e5_pilot.py run --corpus cami --launch --tag w1` | c0b17bb1, `52mAOnXS` | | |
| E5 pratama | `sbatch -J e5_pratama $S $C e5_pilot.py run --corpus pratama --launch --tag w1` | c0b17bb1, `8Z7x3L7z` | | |
| E5 metagem | `sbatch -J e5_metagem $S $C e5_pilot.py run --corpus metagem --launch --tag w1` | pending, after E4 chunk 1 | | |

Launch lanes that share an agent home one after another. Wait for each to print `waiting on run` before submitting the next, because each launch re-stages into the home.

### Failures and causes

### Stopped

- Before the wave, three pre-R1 runs that wave-1 lanes supersede: `iy8YLaGr` (CAMI rung 1), `d6UJuZgF` (Pratama rung 1) and `HQ5SrqFe` (metaGEM li2019). The engine route launched all three, so `scancel --batch --signal=USR1` killed them. That orphaned seven grid jobs: two COMEBin, two DRAM-v (still pending) and three CarveMe. The research agent cancelled all seven by hand. Nothing recoverable was lost, because an orphaned job never writes a cache entry. `C1IM6IG3`'s array covers COMEBin, and E4 covers CarveMe.
- `C1IM6IG3` (CAMI rung 10) keeps running until COMEBin array `59583112` finishes, because it is B5's only source.

### Fixes and gapfills for wave 2

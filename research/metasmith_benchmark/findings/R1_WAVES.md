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
2. Check the project quota with `lfs quota -p 83115734 /scratch`. Inodes are field 6 against field 8, and the launch criterion is below 950K. Bytes are field 2 against field 4, both in KiB, with a cap of 18.63 TiB.
3. Import, stage and fetch images: run each lane once with `--materialise --import`. It imports the lane's givens, stages the plan, fetches every image the plan names, and exits. Chain the jobs that share a home with `--dependency=afterok`.
4. Launch each lane with `--launch --tag w<N>`, without `--import`, once its materialise job has passed. A lane that does not solve is an issue, not a blocker for the others.

CAUTION a pool is one sqlite database per agent home. Two imports into one home at once fail with `database is locked`. E2 short, E2 long and E5 cami share the cami home, and E3 and E5 pratama share the pratama home, so only step 3 imports, and it runs one job per home at a time.

### Gather

For each failed task, record the lane, the transform, the cause and the fix. Read the cause from `.command.log`, `.command.err` and `.exitcode`, and the cost from `sacct -j <parent ids> -o JobID,JobName,State,MaxRSS,Elapsed`.

CAUTION `sacct`'s `.batch` rows carry MaxRSS under JobName `batch`. Collect the parent job ids from the run's work directory first. A date-and-name filter also matches earlier attempts.

CAUTION sweep for swallowed steps in `<home>/runs/<key>/_metasmith/logs.<timestamp>/agent.log`, which carries `Submitted process`, `Error is ignored` and `Killed`. The driver's sbatch log under `bench/logs/` has none of these lines, so a sweep of it always reads clean. Check that `Submitted process` counts above zero before trusting a zero.

CAUTION metasmith renames every product to its content-addressed name, so a search for a tool's own filename (`gtdbtk.bac120.summary.tsv`) finds nothing. Look under `<run>/results/<namespace>-<type>/`.

CAUTION a staged run holds two kinds of `.py` file. `_metasmith/task/transforms/<id>/` holds the transforms that execute. `_metasmith/task/data/<id>/` holds resource payloads such as `lib::modelling`, which carries a copy of the standard `memote_score.py`. Check a pin in `task/transforms/`, or by the step's source hash.

CAUTION attribute a grid job to a run by its `WorkDir` (`scontrol show job`), never by its name. `squeue -u phyberos` spans every home, and `nf-pNN__<step>` carries a per-run plan index, not a home. In wave 1, 36 `nf-p02__chopper` jobs read as E5 metagem's belonged to E2 long (`33hlLu8Q`). Check the job id is non-empty first: `scontrol show job ""` exits 0 and describes another user's job.

CAUTION a task's inputs sit in the FILES manifest of its `.command.sh`, not in staged symlinks. A search for symlinks in the work directory finds none.

CAUTION a job array's chunk parent is not a task. Its `.command.run` has `#SBATCH -o /dev/null`, sits at index 1, 101, 201 and so on, lists every member's inputs, and renders `NXF_SCRATCH=''`. Before recording a property of one task, count how many tasks of that step share it. Count completions as distinct successful task indices, not work directories, because retries add directories.

CAUTION a `cache [promoted]` line in a task log shows the shard write only. The index row comes from `record_run` when the run ends normally. A driver stopped before that leaves shards that serve lookups but are absent from `cache list`.

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

CAUTION a quota sample taken right after a large unlink reads unchanged, because Lustre's quota accounting lags. Re-sample minutes later before recording what a deletion freed. Judge the growth rate over 10 minutes or more: single minutes swing by several TiB per hour as tasks write and delete temporary files.

CAUTION `metasmith cache list --group-by run` files every entry with an empty run under "(imported)", whatever its origin. A task promotes its products into the cache as it finishes, with no run recorded until the run ends. Group by origin to separate products from imports. `--protect-run` cannot guard an entry with an empty run, so build eviction lists only from entries whose run is named.

CAUTION evict task-cache entries through the store with `drivers/evict_cache.py`, which tombstones each listed key and then removes its shard and row. Never delete shard files by hand. Empty work directories with `drivers/prune_work.sbatch`, which refuses any path outside the run's `nxf_work/`.

CAUTION a task work directory is also run-end state. Its `.command.cache` is what `record_run` reads to index the task's promoted shards and write its trace events. Keep that file until the run has ended normally, as `prune_work.sbatch` does. A missing or tombstoned shard reads as a miss (`caching/invocation.py:probe`), so an evicted entry costs only a recompute.

The slurm preset keeps every task's work directory (`cleanup = false`), so finished runs hold their inodes, and bytes that task_cache already holds a copy of. After a lane's run ends and its products are checked, delete that run's nextflow work directory under `<home>/runs/<key>/` as a job. The task cache keeps the results. Check the quota after each deletion, and before every E4 chunk.

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
  - E4 chunk 1 and E5 metagem launch after the tree goes, which frees 424,034 inodes. Job 59638488 deleted it in 9 min, and inodes went from 750,788 to 334,075, a net 416,713 while five lanes wrote. `GCF/` and `GCA/` remain as empty mount points, and the sketch files beside them are untouched. Afterwards bytes, not inodes, were the tight axis: 15.46 of 18.63 TiB (83%) against inodes at 34%. The tree goes once `e4_gtdbtest` classifies a MAG through the squashfs image.
  - Launching in two groups departs from the plan's single launch. The plan's run log records why.
- **GTDB image:** job 59625981 built `release232_skani_genomes.sqfs`, 192 GB. It holds all 199,923 genomes, a count that matches the tree.
- **GTDB-Tk through the image:** `e4_gtdbtest` (job 59635386, key `Sj7uFNWW`, `--study li2019 --limit 1 --with-gtdbtk`) passed. Its GTDB-Tk task exited 0 with one `Traversing tree` line and no missing-genome error. It classified `SRR7664615_bin.1.s` as `g__Castellaniella` by topology and ANI. The closest placement was GCF_004321985.1 at ANI 88.44, and a related reference was GCA_035572875.1 at 86.91, so skani read genomes through both image binds. That pass cleared the tree deletion. One genome cost 17m59s and MaxRSS 90.6 GiB on 8 cpus (grid job 59636007), under the transform's 240 GB declaration.
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
| E4 chunk 1 | `sbatch -J e4_c1 $S $C e4_metagem.py run --chunk 1 --launch --tag w1` | 19609c45, 3 steps (59638489) | `lE94xbfH` | 59638782 |
| E5 cami | `sbatch -J e5_cami $S $C e5_pilot.py run --corpus cami --launch --tag w1` | c0b17bb1, 35 steps | `52mAOnXS` | 59636770 |
| E5 pratama | `sbatch -J e5_pratama $S $C e5_pilot.py run --corpus pratama --launch --tag w1` | c0b17bb1, 35 steps | `8Z7x3L7z` | 59636769 |
| E5 metagem | `sbatch -J e5_metagem $S $C e5_pilot.py run --corpus metagem --launch --tag w1` | 19609c45, 36 steps (59638493) | `YzCrdOoF` | 59639625 |

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

T9's CarveMe resources render as planned. E4 chunk 1's CarveMe task `(868)` in `lE94xbfH` shows `-c 4`, `-t 12:00:00`, `--mem 16384M` and `--account=rrg-shallam-ab` in its `.command.run`. The run's `workflow.config.nf` has closures over `task.attempt` for both memory and time, so retries climb 16 GB/12 h → 32/24 → 64/48 → 128/96. Failing tasks measured before R1 peaked at 14.63 GiB, so some tasks will need the second attempt. When chunk 1 ends, count its models against the 2,000 MAGs it submitted. In `lE94xbfH`, CarveMe has 1,995 ok, 5 running and 0 failed. MEMOTE, running the pinned transform, has 1,900 ok and 0 failed. No gapfill hit the 12 h wall.

CLEAN's GPU route renders as planned. CLEAN's first grid job on fir renders `-c 4`, `-t 04:00:00`, `--mem 32768M` and `--account=def-shallam_gpu --gres=gpu:nvidia_h100_80gb_hbm3_3g.40gb:1`, and Slurm scheduled it. Only this step bills the GPU account. CarveMe stays on `rrg-shallam-ab`. Task `_0` of array 59642405 (E5 cami) COMPLETED in 43:01 at MaxRSS 21.6 GiB. `sacct` AllocTRES shows the MIG slice allocated under `def-shallam_gpu`, and the product holds 139,768 EC predictions (4,042,725 B). GPU use inside the tool is unconfirmed: its log names no device.

CAUTION `sacct --name=nf-p08__clean` returns nothing, because Slurm stores array names with the index suffix, as `nf-p08__clean_(1)`. Filter `sacct -X` output instead.

A finished task's products exist twice while its run is live: in `nxf_work` and as a promoted cache shard. In `WfOlaqLT`, fastp's 208 trimmed-read files take 625.5 GB in `nxf_work`. The research agent matched each one by name to a shard file of identical size and a different inode, and five task logs show `cache [promoted]`. The index does not list these shards yet. Per-task promotion writes only the shard. `caching/promote.py:record_run` adds the index rows when the run ends. A task lookup does not read the index: `Orchestrator.groovy` runs `caching.invocation`, whose `probe` reads the shard's tombstone, manifest and files. The shards therefore serve a later wave now. Downstream tasks of the same run read the `nxf_work` copy, through the FILES manifest in their `.command.sh`. Prune a step's work directories when every consumer of that step has finished and a sample of its shards passes the probe's checks.

A grouped step starts only after every task of its input step finishes. In `WfOlaqLT`, megahit submitted nothing until fastp reached 208 of 208. comebin, semibin2 and metabat2 also had no task dirs while bowtie2 stood at 200 of 208. Each is built by `o.group` with no `expected` count, and `Orchestrator.groovy:_grouped` then flushes a group only when its input channels close. The cost is throughput: one slow sample holds every sample's next step. A work-dir prune behind those steps opens all at once, after the whole next step finishes. T19 candidate: pass `expected` per key where each input is one-to-one with the group key, as fastp's call already does.

CAUTION `metasmith cache list` and `cache explain` read the index only. For a live run's promoted shard they report nothing, or `found: False`, while the shard serves lookups. They also make the store's physical bytes exceed its indexed bytes. Test reachability with the checks in `invocation.py:probe`.

CAUTION an empty inode intersection between two file sets does not show that one copies the other. Match the files by key or name before calling bytes duplicated. Bytes, not inodes, are R1's tight quota: 16.16 of 18.63 TiB (86.8%) at 04:20 PDT.

CAUTION project bytes grow when tasks complete, not while they run. Tasks work on node-local scratch, and every open task dir in wave 1 held 0 GB. A completing task unstages its products into `nxf_work`, and promotion writes a second copy into the cache. The rate swung from 0.28 to 2.16 TiB/h as bowtie2's BAMs completed, then back to 0.33 after the fastp prune. Project the bytes from the completion profile of the large-product steps in flight. Fit the slope over 10 minutes or more. A before-and-after quota pair cannot size a deletion while lanes write, so use the deleting tool's own count.

E2 short's gold_standard failed on 15 of 208 samples, all from `mousegut_short_read`. `lib::cami_gold_standard.py` line 41 reads `reads_mapping.tsv.gz` with polars' type inference. It infers `genome_id` as `f64` from mousegut's early rows, such as `190547.0`, then aborts at the first `denovoN` id with `ComputeError: could not parse`. The failure is deterministic, so every retry failed too: 52 work directories for the 15 samples. Under retry-then-ignore the run still reports complete, but those 15 samples get no gold standard and no AMBER result. The other CAMI datasets carry numeric ids throughout, so their 193 tables are unaffected.

E3's metaSPAdes sample (6) ran out of memory on its first attempt in `Son2YJiI`. Array task `59636440_5` reached MaxRSS 191.99 GiB of 192 GB after 1:28:44. SPAdes logged `Memory limit set to 182 Gb`, but `spades-hammer` exceeded the cgroup anyway. The retry renders `--mem 393216M` and `-t 48:00:00`. The transform's `Resources(memory=Size.GB(192), duration=Duration(hours=24))` doubles per attempt in `workflow.resources.nf`, and its `-m` follows the task's memory. The 192 GB flat block in `workflow.config.nf` matches only `.*__comebin`. Pratama's published setting is `-m 190`, so a second attempt deviates from it for samples that need it.

E5 metagem `YzCrdOoF`'s `p14__semibin2 (3)` was killed for memory (exit 137) at 16 GB, 5 min 20 s after "Start binning." Its retry, job 59652863, renders `--mem 32768M -t 08:00:00` from `workflow.resources.nf:119–123`. The rendered command carries the parity pins `--random-seed 1 --min-len 1500`.

CAUTION a Slurm array task index is not a nextflow task index. `59636440_5` is `p05__spades_pratama (6)`. Map one to the other through `nxf.log` before reading a sample's retry.

### Stopped

- Before the wave, three pre-R1 runs that wave-1 lanes supersede: `iy8YLaGr` (CAMI rung 1), `d6UJuZgF` (Pratama rung 1) and `HQ5SrqFe` (metaGEM li2019). The engine route launched all three, so `scancel --batch --signal=USR1` killed them. That orphaned seven grid jobs: two COMEBin, two DRAM-v (still pending) and three CarveMe. The research agent cancelled all seven by hand. Nothing recoverable was lost, because an orphaned job never writes a cache entry. `C1IM6IG3`'s array covers COMEBin, and E4 covers CarveMe.
- `C1IM6IG3` (CAMI rung 10) keeps running until COMEBin array `59583112` finishes, because it is B5's only source.
- During the wave, to reclaim bytes:
  - Job 59642072 deleted both GTDB source tarballs, 253 GB. Globus holds both.
  - Jobs 59645430 and 59645431 evicted task-cache entries from superseded runs: 16 cami keys (29.4 GiB, from runs 8PHYZXXD, Yt2ZFop0, Gu9VJmwO, wqjsf1Et and CIeAycog) and 3 pratama keys (18.3 GiB, from 1YR8nokN). Container images and the VirSorter2, VIBRANT and vConTACT3 databases stayed, because only a login node can fetch them again.
  - Job 59646555 pruned 189 of `WfOlaqLT`'s 208 fastp work directories (585,493,847,836 B). A fastp directory is pruned when its megahit, bowtie2_binning_bam and fastqc_trimmed tasks each hold `.exitcode` 0 and no task that reads it is open. The plan DAG shows no other step reads `e2::trimmed_short_reads`. The products stay reachable as promoted shards. Job 59648133 pruned the other 19 (86,235,465,647 B) once bowtie2 finished, leaving no fastp work directory in `WfOlaqLT`. The gate is `fir:/scratch/phyberos/_step_refs.py <run> <step> <list> --need STEP ...`. Both prunes deleted each task's `.command.cache`. `caching/promote.py:record_run` reads those files when a run ends. It indexes each promoted shard, copies the task logs into the shard, and writes the member's event to `_metasmith/trace.jsonl`. The 208 fastp shards therefore still serve lookups, but they will get no index row, no copied logs and no trace event when `WfOlaqLT` ends. Each shard's manifest carries the member's `consumes`, `lineage` and files, so a repair can index the shards and write the events. That repair is a T19 item. `prune_work.sbatch` now keeps `.command.cache` in every directory.
  - Job 59649039 emptied 1,995 of E4 chunk 1 `lE94xbfH`'s 2,000 prodigal directories (2,904,402,301 B), gated on CarveMe at exit 0. All 1,995 kept `.command.cache`, and none holds anything else. A task directory holds 12–14 inodes, and an emptied one holds 2. The prune drops the task logs `record_run` would copy into each shard, a deliberate trade for inodes.
  - Job 59649221 emptied 1,900 of E4 chunk 1's CarveMe directories (8,228,025,766 B), gated on MEMOTE at exit 0. All 1,900 kept `.command.cache`. The other 95 wait on MEMOTE tasks that array batching holds until the last 5 CarveMe tasks finish.
  - Job 59652422 emptied all 41 of E2 long `33hlLu8Q`'s porechop_abi directories (127,310,744,447 B), gated on chopper at exit 0. chopper is the only step that reads `e2::adapter_trimmed_long_reads`. All 41 had promoted their product and kept `.command.cache`. The quota rose from 16.938 to 16.984 TiB over the next 5 minutes under concurrent writes.
  - CAUTION a prune gate must name every step that reads the product, not only the next one. E2 long's `e2::filtered_long_reads` feeds flye and minimap2_binning_bam. E3's `sequences::clean_short_reads` feeds megahit, spades_pratama and metawrap_pratama. Check the consumer set against the library's `AddRequirement` lines.
- During the wave, to reclaim inodes, `drivers/delete_stage.sbatch` removed two dead trees. The job aborts when a symlink under the named permanent directory resolves into the tree, or when a run's workflow files or a checkout name it. The first submissions, 59650024 and 59650025, exited 1 before deleting anything: under `pipefail` the reference gate's grep failed when it found no match. Fixed at eb8ec9f5.
  - Job 59650208 deleted `_vs2_stage` (32,551 inodes, 2,188,519,136 B). The project went from 552,776 to 525,411 inodes in the next 5 minutes. It was the container work directory of the VirSorter2 staging script, mostly its conda package cache. No symlink under `refs/virsorter2_2.2.4` resolves into it, and no file in it has a second hard link.
  - Job 59651314 deleted `cami/runs/5vqR1dv8` (9,156 inodes, 18,992,848,545 B), the Sep 9 marine sample_0 run. Its first resubmission, 59650210, aborted because the gate matched the run's own `workflow.nf`. The gate now ignores files inside the tree it deletes (5bbee50a). Only a retracted projection cited it. Its three bin counts, including the campaign's only COMEBin bin count, went into `PROVEN.md` first.
  - Job 59652737 deleted `cami/runs/8PHYZXXD` (182 inodes, 22,387,979,673 B). The run was dead: no `PID.lock`, no Slurm job, and no process group on any login node. Its cache entries were evicted earlier.

### Fixes and gapfills for wave 2

- Relaunch E5 cami and E5 pratama from `19609c45` or later, so MEMOTE runs the pinned transform. Their finished steps serve from cache.
- Fix E2's gold standard for mousegut. Copy `lib::cami_gold_standard.py` into the E2 benchmark library as its own resource type. Read `genome_id` and `tax_id` as strings in the copy. Point `e2/gold_standard.py` at the copy. This retires E2's gold_standard and amber entries, which are cheap to recompute, and leaves the standard `cami_contig_truth` untouched. Built at `2192c40c`. On a mousegut-shaped table the standard script fails with the same parse error and the copy writes `denovo9553.0` as a BINID. Against launch checkout `f6d01f00`, the rebuilt E2 `_metadata` changes one transform id, gold_standard's, and adds `e2::cami_gold_standard.py`.
- E2 long's Flye is not exposed to B12. It runs the pinned `e2/flye.py`, which takes the mode from the declared platform (`OXFORD_NANOPORE` → `--nano-raw`) and declares 64 GB. B12's quality-derived preset lives in the standard `flye.py` and `flye_raw.py`, which no R1 lane runs. Execution confirms it. A `33hlLu8Q` Flye task renders `--mem 65536M` and `-t 24:00:00`, runs Flye 2.9.5-b1801 on 3.93 Gbp of reads (N50 3,144), and passed "Assembling disjointigs" into k-mer counting and index filling. The HiFi preset aborted at that stage with "No disjointigs were assembled". The first completed task closes the proof. `nf-p03__flye_(1)` COMPLETED in 32:45 at 16 cpus. It produced 2,617 contigs, 115,338,976 bp, N50 107,818, at MaxRSS 32.49 GiB, with 0 of 41 failed. Flye measured the read error itself at 15.2% and 22.8%. The 32.49 GiB peak is 101.5% of the standard transform's 32 GB, so the pin's 64 GB also prevents an out-of-memory kill.

CAUTION metasmith's protocol does not echo the tool's command, so `.command.log` and `.command.out` carry only the tool's own output. Verify a setting from the tool's banner and progress lines, or from the staged transform.

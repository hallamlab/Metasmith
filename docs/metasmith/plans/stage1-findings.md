# Stage 1 findings: metasmith bug hunt after Rokaiya's fir run

## Purpose & Contents

This file records what the stage 1 bug hunt of 2026-10-16 found. It feeds the fix plan that
follows it. Each finding states the evidence, the cause where known, and a proposed fix. It is
a working record, not a design doc: delete it once the fixes land and the plan is debriefed.

Severity: **S1** breaks a run outright, **S2** wastes hours or needs a manual workaround,
**S3** is a latent defect or papercut.

## Rokaiya's run, boisterous-agama on fir (metasmith 0.24.1, patched transforms)

Sources: her Slack DM (ts 1792177455 to 1792177486), `sacct -u shatadru` on fir, and process
state on the fir login nodes. Her agent home `/scratch/shatadru/msm.v8Ttsaq8` and her
`/tmp/shatadru` are not readable from Tony's account, so task logs are not available.

### R1 (S1). The GTDB download failed after 19 h 21 min

Job 65233798, `nf-p03__downloadGtdbDB_(1)`, ran 2026-10-15 12:55 to 2026-10-16 08:16 on fc20667
under rpp-shallam_cpu with the 120 h override, and ended FAILED with exit 1. `sacct`: ReqMem 8G,
MaxRSS 8,390,444K (at the limit), MaxDiskRead 1,048 GB, MaxDiskWrite 1,074 GB, TotalCPU 1 h 29 min.
The run stopped with it. Cause not confirmed without the task's `.command.log`.

### R2 (S1). The relaunch hung at relay start

Her relaunch on 2026-10-16 12:02 PDT failed at staging with
`TimeoutError: [relay start] produced no output for 60s`. The ssh landed on login3, a node her
earlier runs never used. On login3, `./relay/msm_relay start` (pid 46596, parent `-bash` 46255
reparented to init) still runs 6.5 h later. It sleeps about 10 times per second and no watcher
daemon of hers exists on that node. So the start parent is in its poll loop, waiting for a
watcher that died or never came up. That loop in `src/bash_relay/src/main.rs` has no timeout and
prints nothing while it waits. Her TMPDIR workaround was in effect: `/tmp/shatadru` was created
on login3 at 12:02. Starting the same relay as phyberos on login3 with the same TMPDIR succeeds
in under a second. So the trigger is specific to her state, and is still unknown.

Also observed: a daemon relay of hers from 2026-10-15 12:42 still runs on login2, and an Oct 9
`/tmp/msm_login1_unknown_user` owned by her remains on login1.

### R3 (S2). comebin hit the Slurm time limit twice

Jobs 65233984 (4 h, 32G, 8 cpus) and 65273686 (8 h, 64G) both ended FAILED 140:0 at their
time limit. The second used 2 d 9 h of CPU time in 8 h, so it was busy, not hung.

### R4 (S2). semibin2 ran out of memory twice

Jobs 65233994 (16G) and 65239398 (32G) ended OUT_OF_MEMORY after about 20 min each.

### R5 (fixed in 0.24.2). phyloFlash DB failed in 16 s

Four attempts on 2026-10-15 failed on `Unknown option: remote_dbsource`. This was before her
workflow copy got the patched transform. 0.24.2 ships the fix.

### R6 (S3). The GUI stored `slurmAccount=rpp-shallam` as one key with an empty value

The first launch on 2026-10-15 submitted under the preset default account, def-shallam_cpu.

## Found while verifying 0.24.2 (2026-10-15)

### V1 (S2). `WaitForWorkflow` reports a live run as errored when its probe gets no answer

Already recorded in `consolidation-followups.md`, under Open bugs.

### V2 (S3). The publishDir straddle warning on fir

On fir, `cache_root and work_dir straddle mounts; forcing publishDir 'copy'` (/ws vs /scratch).
For a GTDB-sized product, a copy doubles the disk and the I/O.

### V3 (S3). A root-owned solver target directory blocks the host solver build

`~/.cache/metasmith/solver-target/host` is owned by root after a docker cross build, so
`dev/metasmith.sh -bel` fails with "Permission denied". Workaround: set
`MSM_SOLVER_TARGET_DIR`.

## Stage 1 sweep

Four parallel agents swept downloads, tool steps, the relay and run control, and the engine. Their findings follow, by area.

### Tool steps: options against pinned versions, and resources

Upstream sources fetched during the sweep, and the solved step list of every template, are in
`~/.claude/jobs/0452781d/tmp/stage1/tools/`.

**Rokaiya's failures and the retry policy**

- **T1 (S2). comebin trains on CPU, so 4 h and then 8 h is far too short.**
  - `binning/comebin.py:64-73` requests no GPU, so the step lands on a CPU node.
  - COMEBin 1.0.4 falls back to CPU: up to 200 epochs over every contig of 1 kb or more, then 120 Leiden runs.
  - Each retry starts in a fresh work dir, so the first attempt's hours are lost.
  - **Fix:** request a GPU, as `clean.py:286` does. If it must stay on CPU, request 32 cpus, 64G and 48h.
- **T2 (S2). semibin2's memory grows with the square of the contig count.**
  - SemiBin 2.1.0 builds a dense N×N KL matrix in single-sample mode, which is always our case: `is_combined = n_sample >= 5`, and we pass one BAM. This is still true in 2.6.
  - At 4 to 12 bytes per pair, 32G runs out at roughly 50k to 90k contigs. The N² constant is inferred from the source, not measured.
  - **Fix:**
    1. Pass `--min-len 2500` for a large assembly. That cuts N about 3× and memory about 9×.
    2. Fail fast when 12·N² bytes exceeds the memory request.
    3. Default the request to 64G.
- **T3 (S2). The retry ladder is one retry for any exit code, with both memory and time doubled.**
  - `nextflow_config/slurm.nf:28,116-118` sets `tries=2`, with errorStrategy retry and then ignore.
  - `models/libraries/resources.py:131-162` doubles both memory and time on every retry.
  - So a timeout doubles memory, an OOM doubles time, and a deterministic bug costs a second slot.
  - `aggregator.py:43-50` needs all three binners, so one failed binner drops the whole quality-MAG chain.
  - **Fix:**
    - Retry only on 137, 140 and 143, with 3 attempts.
    - Escalate only the resource the exit code names.
    - Make `tries` settable per transform.
    - Let the aggregator run on whichever binners succeeded.

**metagenomics_from_paired_reads (29 steps)**

- **T4 (S1). gtdbtk 2.6.1 is pinned against the R232 database.**
  - 2.6.1 supports only r220 and r226 (`config/common.py`). R232 needs at least 2.7.0.
  - 2.7.0 changed the skani layout and deprecated the `--skip_ani_screen` that `gtdbtk.py:51` passes.
  - **Fix:** move to 2.7.2 or 2.8.0 and drop `--skip_ani_screen`. Re-check the downloader's genome_paths shim and layout against that version.
- **T5 (S1, mamba runtime only). The conda yml pins metabuli 1.1.0, which cannot read gtdb232.** The container has 1.2.0. **Fix:** pin 1.2.0.
- **T6 (S2). checkm2 hides failures.**
  - `checkm.py:143,156` ends its commands with `|| true`, and `success=True` is unconditional.
  - A broken database or an OOM writes 0% completeness rows, and the aggregator then drops every bin.
  - **Fix:** fail when `quality_report.tsv` is missing.
- **T7 (S2). phyloFlash's `reformat.sh` reads pigz output without `unbgzip=f`.**
  - `interleave_zipped_short_reads.py:186-200` documents that this hangs on large input.
  - The 2 h limit is also tight with `-readlimit -1`.
  - **Fix:** add `unbgzip=f`, and request 6h or set a read limit.
- **T8 (S3). comebin writes seed and fragment files beside the upstream megahit product.** A killed attempt leaves truncated files that the retry reuses. **Fix:** copy the assembly into the work dir.
- **T9 (S3). assembly_stats writes an uncompressed SAM before sorting.** It also uses minimap2 2.15, and comebin copies the whole BAM. **Fix:** pipe minimap2 into the sort and use symlinks.
- **T10 (S3). diamond chunks of 5000 ORFs re-scan UniRef50 about 400 times for 2M ORFs.** **Fix:** use a larger diamond chunk, or make it a param.

Clean in this plan: seqkit_reads, bbduk, megahit, prodigal/pprodigal, metabuli flags, metabat2,
diamond_uniref50, kofamscan, skani_dedup, interleave, the merges, and the aggregator logic.

**Other templates and the rest of the library (164 transforms)**

- **T11 (S2). `eggnog_mapper.py:21` copies the whole eggNOG database for every chunk**, which is about 50 GB each time.
- **T12 (S3). hifiasm_meta is sized for an isolate.** The gfatools image is unpinned.
- **T13 (S3). genomad hardcodes `/usr/local/bin/_entrypoint.sh`**, which breaks under mamba.
- **S1:**
  - **R1.** fastqc and nanoplot write `read_qc_stats` without `mean_quality`, `phred_encoding` or `GC`. When the solver picks either one, the consuming steps raise KeyError.
  - **R2.** amrfinderplus 4.2.7 needs `-d /amrdb/latest`.
  - **R3.** mobileelementfinder bind-mounts a patch from `/home/phyberos/...`.
  - **R4.** `/ws` handling breaks one of the two deployment modes: pathologic, deeptfactor, predictf, promotech.
  - **R5.** cobra.env has no container. This affects 7 transforms and is already documented.
  - **R6.** deepec's `-p` flag and its output glob are wrong. Unverified.
- **S2:**
  - **Engine-wide:** `ExecWithEnv` uses `set -e` without `pipefail` (`execution.py` ~388), so piped failures pass silently. This covers fastp|gzip, fastq-dump|pigz, mpileup|call and more.
  - **Hidden failures:** `|| true` or placeholder outputs hide errors in cctyper, integronfinder, pathofact and instrain.
  - **iphop_add_to_db** uses `ar122`, but GTDB-Tk 2.x writes `ar53`, so archaeal MAGs are dropped.
  - **Wrong mode:** gtdbtk_tree concatenates the bac120 and ar53 MSAs. star_genome_index lacks `--genomeSAindexNbases`. hifiasm lacks `--ont`. flye's quality-to-mode mapping is wrong. fastp, polypolish and bowtie2_align ignore single-end vs paired reads.
  - **GPU models without a GPU request:** ankh, esmfold, prott5, saprot.
  - **ESM-C mean pooling includes padding.**
  - **Time limits too short:** gtdbtk_de_novo at 48h, and 30 min for the diamond tcdb2, bacmet, vfdb and megares steps. Spacer BLAST also ignores `-num_threads`.
  - **Run-time `pip install`** in organellar_count_matrix and foldseek_3di.
- **S3:**
  - Unused threads in 5 transforms.
  - Bracken hardcodes `-r 150`.
  - das_tool writes into the ORF product.
  - Conda envs with no recipe: amber, polars, metawrap-mg, instrain.
  - Unpinned sra-tools and gfatools images.
  - DRAM prefixes gene IDs.
  - bcftools runs without `--ploidy 1`.

### Reference downloads

Scratch files, including the tarball member listings, are in
`~/.claude/jobs/0452781d/tmp/stage1/downloads/`.

- **D1 (S1). GTDB-Tk 2.6.1 does not support r232.** This is T4, confirmed from the download side.
  - 2.6.1 accepts only `COMPATIBLE_REF_DATA_VERSIONS = ['r220','r226']`.
  - From 2.7.0, GTDB-Tk runs `skani search` against the shipped `skani/database/sketches.db` and needs no genome FASTA files. The reference shrinks from 198 GB to 98 GB.
  - **Fix:**
    - Pin gtdbtk 2.7.2 or 2.8.0.
    - Drop the 192 GB reps download and the genome_paths step.
    - Swap `--skip_ani_screen` for `--place_species`. 2.7.2 rejects the old flag.
    - 2.7 needs at least 140 GB of RAM, and the consumers request 240 GB.
  - The current extraction layout is correct for 2.6.1. The member listings confirm both strip levels and the `%f %h/` format. Only the version is wrong.
- **D2 (S1). R1, the most likely cause.** The step finished both downloads and extractions, then died while copying the ~290 GB and ~400k-inode product.
  - It was not an OOM. Slurm marks an oom_kill as OUT_OF_MEMORY, as it did for semibin2. The 8G MaxRSS is page cache.
  - **The I/O accounts for one extra copy.** Slurm counts rchar and wchar, which include sockets and pipes. The protocol explains about 796 GB read and 833 GB written. The remaining ~250 GB each way is about one copy of the product.
  - **Exit 1 carries no information.** `LocalShell` discards the shell's return code (`execution.py:209`). The task then exits with `[ -e .command.success ] && exit 0 || exit 1` (`nextflow_codegen.py:485`).
  - **The step ran as a Slurm job despite `labels=["local"]`.** The `withLabel 'xlocalx'` selector in slurm.nf should have kept it on the login node with `scratch false`. Why it did not is unknown.
  - As a Slurm job it ran under `scratch=$SLURM_TMPDIR`, so the product was copied twice:
    1. `admission._place` runs `shutil.copytree` on every directory product, even on the same mount (`admission.py:445`).
    2. Nextflow's unstage runs `cp -fRL`.
  - The likely failure is a space or inode quota on one of those copies. A promote failure is only a warning, but an unstage cp failure fails the task.
  - **To confirm:** get her `.command.log` and `.command.err` tail, her `diskusage_report`, and any `*.tmp` shard left in the cache root.
- **D3 (S2). Download design.** 253 GB needs 70 to 234 h on a compute node. After D1 the download is 60.8 GB: 17 to 56 h on a compute node, or about 50 min on the login node.
  - Make the `local` label take effect.
  - Fetch the 12 split parts with `FetchCommand`. It resumes, and the parts have MD5 sums. Stream them into tar.
  - Let a user point at a directory they downloaded once.
  - Make `_place` hardlink on the same filesystem.
  - Tighten the success check to require `index.db`, `markers.bin`, `sketches.db` and `VERSION_DATA=r232`.
- **D4 (S1). `downloadMetaphlanDB` passes `--bowtie2db`, which MetaPhlAn 4.2.4 removed.** `parse_args` exits 2. The empty `mkdir`'d directory then counts as success and is cached.
- **D5 (S1). `downloadCentrifugerDB` writes `{iout.container}.1.cfr` as a sibling of the product.** The consumer globs inside it, and nothing creates `iout.local`, so the step always fails. Part 1 is also 175.6 GB with a 12 h limit, using `wget -q` with no `-c` and no `set -e`.
- **D6 (S1). The five Hugging Face weight downloads produce empty archives** (Ankh, ESMC, ESMFold, ProtT5XL, SaProt).
  - The unpinned `huggingface_hub` 2.2.0 ships only the `hf` command, not `huggingface-cli`.
  - `tar -czf` over a missing dir still writes the output file, so success is True.
  - ESMC has moved to `biohub/`.
- **D7 (S1). `downloadMetabuliDB`: gtdb232.tar.gz is 634 GB against an 8 h limit.** Even the login node needs about 8.8 h, and peak disk is about 1.3 TB. The conda pin is also wrong (T5).
- **D8 (S2). Success checks pass without real data.**
  - Kraken2 and Eggnog have no `set -e`, and their success test is that the directory exists. Eggnog's download strings are also dead code.
  - AntiSmash and Amrfinder succeed when the directory exists.
  - Sylph, Crassphage and PredicTF use `wget -O`, which leaves a partial file.
  - getSraReads runs `fastq-dump | pigz` without pipefail, so it leaves a valid 20-byte gz.
  - Virsorter2's `vs2_db` exists even after a half-failed setup.
- **D9 (S2). Steps without `local` cannot fit their downloads at compute-node speed.** Kraken2 has 12 GB in 6 h. Sylph has 14 GB in 2 h. Also Centrifuger, Metaphlan, AntiSmash, Genomad and Ganon2. **Fix:** add `labels=["local"]` and use `FetchCommand`.
- **D10 (S3). Unpinned sources drift, and none is a run param.** UniRef50, CARD, Kofam, TCDB, CheckV, geNomad, vConTACT3, amrfinder_update, PredicTF HEAD.
- **D11 (S3). `downloadFeastSources` hardcodes `/home/phyberos/...`.**
- **D12 (S3). Eggnog and BUSCO require a source marker type that nothing produces.**

Every URL checked returns 200, with no 404s or HTML redirects. Clean: Phyloflash, Kofam,
InterProScan, UniRef50, DRAM, vConTACT3, VIBRANT, iPHoP, Bakta, CheckV, SILVA, BacMet, MEGARes,
TCDB, VFDB, PathoFact, getNcbiAssembly, pullContainer, ref_genomad.

### Relay and run control

The repro scripts are in `~/.claude/jobs/0452781d/tmp/stage1/relay/`. `fir_race.sh` is the race
reproduction on fir. Test dir `/scratch/phyberos/relaytest_race1` was left in place.

**The login3 hang (R2), most likely cause.**
- Her start parent switched at 10.05/s for 25,164 s, so `active` never appeared.
- Her login3 cgroup shows `oom_kill 0`, peak memory 47 MB and pids max 0. That rules out a resource kill.
- Her own terminal session (pts/157) was on login3 from 12:00 to 12:41, so a ControlMaster was shared with the GUI.
- Ranked causes:
  - **About 60%: two concurrent `start`s (L1).**
  - **About 25%: her terminal removed `active` or the workspace mid-start.**
  - **About 15%: a silent `setup_workspace` failure followed by a child panic (L3).**
- **To confirm:** read `/tmp/shatadru/msm_login3_unknown_user/main.log` on login3. Two `>>>>` banners at 12:02 mean the race.

- **L1 (S1). Two concurrent `start`s on one workspace kill each other, and a parent hangs forever.**
  - The code checks, sets up and forks with no lock (`main.rs:200-253`). It unlinks and relinks the symlink (`main.rs:119-121`), and the child's `wipe` deletes the other watcher's `active` (`watcher.rs:349`).
  - Her exact signature reproduced in 6 of 48 local races and 1 of 12 on login3.
  - **Fix:**
    - Take a node-local `flock` around check, setup, fork and publish.
    - Replace the symlink atomically.
    - Write the watcher pid into `active`, so `wipe` and the exit test respect a live owner.
- **L2 (S1). The start parent's poll loop has no deadline and cannot see its child die.** The parent sets SIGCHLD to ignore before the fork (`main.rs:219`). **Fix:** set the ignore in the child only. Use `waitpid(WNOHANG)` and a 30 s deadline, print the tail of `main.log`, and exit non-zero.
- **L3 (S2). `let _ = setup_workspace(...)` (`main.rs:213`) swallows errors.** The child then panics at `watcher.rs:347/353`. **Fix:** propagate the error before forking.
- **L4 (S2). The GUI job log drops all remote output.** `LogCapture` is thread-local (`gui/jobs.py:166-198`), but the shell callbacks run on the reader thread. **Fix:** carry the job context into the reader thread.
- **L5 (S1). A daemon watcher dies on a stderr write once its ssh session has closed.**
  - It inherits the session's stdio. When a log write fails, `eprintln!` (`logger.rs:52`) panics on the broken pipe. The guard then hits a poisoned mutex and aborts, leaving a stale `active` and undispatched jobs.
  - It reproduced locally. On fir, a `/tmp` charged to a full memory cgroup is a real trigger.
  - **Fix:** after `setsid`, redirect fds 0, 1 and 2 to /dev/null. Remove `eprintln` from Logger. Recover from the poisoned mutex.
- **L6 (S1). The bounced sbatch, squeue and scancel wait forever on a dead watcher.** `remote_shell.rs` exec uses no timeout. **Fix:** poll watcher liveness, and fail when `.start` is not consumed within about 10 s.
- **L7 (S2). A watcher stalled for more than 2 s lets the next `start` kill a live run's jobs.** This reproduced with SIGSTOP. **Fix:** the same pid-in-`active` test as L1.
- **L8 (S2). `WaitForWorkflow` checks the driver pid on whichever login node the probe lands on** (`run_control.py:206-262`). With round-robin ssh or Slurm-driver mode, a live run reads as errored, and the GUI marks it failed (`gui/watcher.py:165`). **Fix:**
  - Record `RUN.host` and probe on that node, or report unknown.
  - In Slurm-driver mode, use `sacct`.
- **L9 (S2). A stale `PID.lock` after a SIGKILL or a reboot makes a dead run read as alive forever.** login3 rebooted on 2026-10-15. **Fix:** put the host in the lock.
- **L10 (S2). Cancel, reap and scan are node-local** (`run_control.py:44-62,100,503,511`).
  - A cancel from the wrong node marks the run cancelled while it still runs.
  - The escalation step can SIGKILL an unrelated process group with the same number.
  - **Fix:** run these on `RUN.host`.
- **L11 (S2). Upgrading metasmith never replaces the relay binary in an existing agent home** (`bootstrap.py:69-73`, `agent.py:306-313`). Old daemons also keep serving. 0.24.2's username fix therefore never reaches Rokaiya's agent. **Fix:** stamp a version on the relay and the watcher, and replace or restart on a mismatch.
- **L12 (S3). Relays and hung parents are never cleaned up.** `Dispose` kills only local processes. **Fix:** L2, plus a remote `timeout -k 5 45` wrapper.
- **L13 (S3). The relay's timeouts use wall-clock time,** and fir login clocks drift. **Fix:** use `Instant`.
- **L14 (S3). Some LiveShell waits have no limit:** the ssh Exec in `agent.py:114`, `workflow_ops.py:169,630`, and Deploy. An unanswered MFA prompt hangs the GUI job.
- **L15 (S3). `cargo test` runs 0 tests.** The lifecycle suite covers none of the races above.

### Engine, Slurm preset, cache and GUI

Scratch files, including the Nextflow probes, are in `~/.claude/jobs/0452781d/tmp/stage1/engine/`.

- **E1 (S1). Under the slurm preset, 8 of the 12 templates abort the whole run at their first big `local` download.**
  - `slurm.nf:22-24,95-96` caps the local executor at 8 GB and 8 cpus.
  - Three steps ask for more: downloadUniRef50DB at 14 GB (in 7 templates), downloadDramDB at 64 GB, and downloadESMFoldWeights at 16 GB.
  - Measured on Nextflow 26.04.1, the request is refused with `Process requirement exceeds available memory`, and the session aborts. `errorStrategy 'ignore'` does not catch it.
  - `check_launch` runs only for the local preset (`workflow_ops.py:497-503`).
  - **Fix:**
    - Apply the ceiling check to `xlocalx` steps under every preset.
    - Size the localExecutor to the shipped downloads, or cap those transforms.
- **E2 (S1 on fir). The retry ladder doubles a GUI per-step override.** A 120 h override asks for 240 h on attempt 2, past fir's 7-day cap.
  - The preset retries every exit code alike, with `tries=2`, and doubles memory and time together.
  - No preset sets `max_duration`, and memory has no ceiling (gtdbtk would go from 240 to 480 GB).
  - `resources.py:139-147` records that an ignored submit refusal wedges the run.
  - **Fix:**
    - Retry by exit code: memory on 137/125, time on 140, and one retry without escalation for anything else.
    - Set `max_duration` and `max_memory` per site preset.
    - Clamp overrides to those ceilings.
    - Consider `tries=3`.
- **E3 (S2). Process keys in slurm.nf that are not closures are fixed when the config is read, so params cannot change them.**
  - Setting `process.tries: 7` left maxRetries at 4, and attempt 5 aborted the session.
  - `process_clusterOptionsExtra`, cpus, array and scratch are silently ignored when passed as params.
  - **Fix:** set `maxRetries = 99` and let errorStrategy own the limit. Render a literal `process {}` block from params, as `_render_executor_config` does for `executor`.
- **E4 (S2). A directory product is copied twice.**
  - Promotion runs `admission._place` → `shutil.copytree` (`admission.py:440-446`), even on the same mount.
  - A cache hit's twin falls back from `ln -f` to `cp -r` (`nextflow_codegen.py:80-82`).
  - For GTDB that is ~250 GB at promotion plus ~250 GB on every hit.
  - **Fix:** use a link `copy_function` with a copy2 fallback, and try `cp -al` in the twin.
- **E5 (S2). Under `scratch=$SLURM_TMPDIR`, promotion always crosses devices.** `bootstrap._promote` uses the node-local cwd (`bootstrap.py:358`), so every product is copied into the cache. Nextflow then also unstages it into the work dir. **Fix:** promote from `$__msm_wd` after unstaging, and set `scratch false` for steps with large products.
- **E6 (S3). The straddle warning (V2) does nothing.**
  - `cache_hit_strategy` has had no reader since 031d82bc.
  - `assert_same_mount` compares mount points rather than devices, so `/ws` and `/scratch` count as a straddle.
  - **Fix:** delete `apply_fs_strategy`, or make it compare `st_dev`.
- **E7 (S2). The GUI param bug (R6).** The frontend keeps any row that has a name (`ParamRows.svelte:88-101`), and the server does not validate names (`api.py:401-409`). An empty value box also sends `""`, which overrides the agent default (`--account=`). **Fix:**
  - Split `name=value` when the field loses focus.
  - Validate names.
  - Treat an empty value as inherit.
  - Refuse launch when slurmAccount is empty or still the placeholder.
  - Warn on undeclared names.
- **E8 (S2). A failed stage shows a GUI-server traceback, and the agent's own error never reaches the user.**
  - This is the same thread-local capture as L4.
  - `StageWorkflow` ignores the exit code of `msm api stage_workflow` (`workflow_ops.py:203-206`), then fails with a misleading "launcher missing" error. That error appears on 7 of 9 runs in her 10-06 bundle.
  - **Fix:**
    - Carry the log context into the reader threads with a ContextVar.
    - Raise on a non-zero exit, with the stderr tail.
    - Keep tracebacks in the server log.
- **E9 (S1, latent). A failed launch can mark a live run failed, and a relaunch then kills that run.**
  - Triggers: a launch timeout, an exception, a GUI restart while launching, or V1.
  - The re-stage goes into the live workspace. The second Nextflow exits on the lock and runs `rm $PIDF` (`runner.py:140`). The first run's supervisor then kills its own run (`runner.py:202-211`).
  - **Fix:** the launcher refuses while `PID.lock` names a live pgid, and the supervisor compares the lock's contents.
- **E10 (S2). There is no per-agent lock across stage, run, deploy and setup.** Concurrent stages each start a relay (feeds L1). `create_run`'s live-run check is check-then-write on a threaded server.
- **E11 (S2). New GUI workflows always start from the `local` preset** (`api.py:913-917`), whatever the agent's `default_preset` says. The preset is a snapshot that never refreshes after an upgrade.
- **E12 (open). Why her GTDB step ran on Slurm is not reproducible with the shipped code.**
  - Her rendered workflow carries `label 'xlocalx'`.
  - The current slurm.nf, combined with her resources and a 120 h `withName` override, resolves to executor local.
  - So the preset she used on 10-15 probably lacked the `xlocalx` block. The GUI lets users edit presets freely.
  - **To confirm:** her current `workflows/boisterous-agama/preset.nf` and the run's `logs.latest/workflow.config.nf`.
- **E13 (S3). CancelWorkflow reads a failed probe as "no PID.lock".** Its noop branch reports cancelling but kills nothing.
- **E14 (S3). A stale unit test.** `test_log_capture.py::test_the_producing_tasks_logs_land_in_the_member_shard` fails with a TypeError since ec89c740.
- **E15 (S3). Smaller issues:**
  - `xlocalx` uses `errorStrategy 'ignore'`, so downloads never retry.
  - Dead config keys draw warnings.
  - `${params.workspace}` is unquoted in shell paths.
  - Under `METASMITH_DRIVER_SLURM`, the GUI's `run_number` points at the previous run.
  - `JobRunner` keeps finished jobs forever.

**Test suites.** The box was loaded, so wall times are inflated.

| Axis | Passed | Failed | Skipped |
|---|---|---|---|
| unit | 2149 | 1 (E14) | 5 |
| cache | 198 | 0 | 0 |
| flow | 165 (+4 xfail) | 0 | 3 |
| gui | 380 | 0 | 0 |
| lifecycle | 7 | 0 | 5 (no relay build) |
| solver | 331 (+1 xfail) | 0 | 0 |
| deploy | 13 | 0 | 8 |
| bootstrap | 309 | 0 | 0 |
| audit | 7 | 0 | 0 |

All 12 shipped templates solve, lint clean, and complete a stub run.

**consolidation-followups open bugs.**
- Most are still open.
- Partly fixed:
  - Cancel in the first minute (E13).
  - Publish copy on a double bind (E4, E6).
  - The Orchestrator.group barrier.
  - The local executor ignoring `-params-file`. It is fixed for `executor`, but the slurm `localExecutor` and the `process` keys still have the trap (E1, E3).
- The logistics e2e class is obsolete.

## Cross-cutting themes

1. **Failures that report success.** LocalShell discards exit codes. ExecWithEnv has no pipefail. Success tests check that a directory exists. Transforms use `|| true`. (D2, D4, D6, D8, T6, the engine-wide pipefail finding.)
2. **fir's shape.** Login nodes sit behind round-robin DNS, each with a node-local tmpfs `/tmp` and its own pid space. Compute nodes download slowly. Mounts are double-bound. The relay, run control and the cache all assume one host and one mount. (L1, L8–L10, E4–E6, D3, D9.)
3. **Resources and retries.** The retry ladder is blind to exit codes. Local-executor ceilings sit below the shipped downloads. Overrides escalate past the cluster cap. (T1–T3, E1–E3.)
4. **Version drift between pins and data.** gtdbtk 2.6.1 vs r232, metabuli 1.1.0 vs gtdb232, metaphlan, huggingface_hub, amrfinder. (T4, T5, D1, D4, D6, R2.)
5. **Upgrades do not reach existing agents.** The relay binary is never replaced, and the GUI preset is a snapshot. (L11, E11.)

## Needs Rokaiya's files to confirm

Ask through Tony only.

1. GTDB task `.command.log` and `.command.err` tail, plus `diskusage_report`. Confirms R1 and D2.
2. `/tmp/shatadru/msm_login3_unknown_user/main.log` on login3. Confirms R2 and L1.
3. Her current `workflows/boisterous-agama/preset.nf` and the run's `logs.latest/workflow.config.nf`. Confirms E12.

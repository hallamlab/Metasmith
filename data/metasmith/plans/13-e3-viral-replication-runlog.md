# E3 viral-only: close the four viral gaps with four Sonnet agents

## Context

E3 moves to a viral-only scope. It keeps assembly, calling, pooling, curation and clustering, and drops the MAG lane and the annotation tail (DRAM, DRAM-v, vConTACT3, iPHoP), which E1/E2 already cover. The paper publishes its 257,252 vOTUs of at least 5 kb, and every name carries its sample, assembler and caller, so recovery can be scored per stage. Four gaps stand between the current plan and the paper's viral pipeline: the hybrid lane, DeepVirFinder, VirSorter2 boundaries, and curation with the island filter. This plan closes all four in code, and the solve and review stay local. Nothing launches on fir.

## What you said

> lets focus on identifying the viruses and matching those with the results of the paper. this would remove the metagnomics portion (which E1/2 already cover) and also give the opportunity to not run dramV if not needed

> we would also need to have 2 areas of focus: assembly -> common pool of contigs (still marked by sample) -> then viral contig identification

> the rest are straightforward as well. we can plan this out and have you orchestrate 4 sonnet agents to close each of the 4 gaps

Decisions taken this session:
- Keep rules follow Supplementary Fig. 1. Keep a contig if it has viral genes > 0, OR it has 0 viral and 0 host genes, OR at least 75% of its genes are unknown. Gene counts come from CheckV. CheckV host trimming then runs before clustering.
- The island filter runs `genomad annotate` on the vOTUs of at least 100 kb and matches Antonio's pattern list from `10_filtering_3.sh`.

## Issues

- I1. The E3 driver still targets the MAG lane and the annotation tail, including DRAM-v, the step that failed in wave 8.
- I2. Only 6 of 17 hybrid assemblies get built. `GivenLibrary` keys givens by path, and 17 pairings share 6 MinION files. Hybrid contigs never reach the viral pool (12% of the published vOTUs).
- I3. DeepVirFinder never feeds the pool. Its transform records "no cut" although the Methods give score ≥0.9 and p ≤0.05 (4% of the published vOTUs).
- I4. The VirSorter2 call writer prefers `trim_bp_*` over the full sequence that `--keep-original-seq` asks for.
- I5. No step applies the keep rules or the CheckV host trimming, and MMseqs2 clusters the raw pool. No step removes island vOTUs of at least 100 kb.
- I6. If the hybrid lane joins the pool, `frozen_id` (`sample|contig|start_end`) can collide. A sample's hybrid SPAdes contigs and its short-read SPAdes contigs both use `NODE_…` names.

## High-level goals

- G1. Make E3 a viral-only replication that stops at vOTUs.
- G2. Pool viral calls the way Pratama did: three assembly lanes, four callers, every contig still marked by sample.
- G3. Curate and cluster the pool the way the paper describes, so our vOTUs are comparable with the published set.
- G4. Get all four gaps closed in parallel without the agents' edits colliding.

## Acceptance criteria

- `e3_pratama.py run --dag` solves locally with no dropped targets. The plan contains no DRAM, DRAM-v, MetaWRAP, GTDB-Tk, vConTACT3, iPHoP or prodigal-gv steps.
- `check_plan` counts `e3::nanopore_reads` at 17. The plan builds 17 distinct hybrid assemblies, each with one Illumina parent.
- The merge pools 3 lanes (SPAdes, MEGAHIT, hybrid SPAdes) × 4 callers (geNomad, VirSorter2, VIBRANT, DeepVirFinder). `frozen_id` and the provenance table both carry the lane.
- The DeepVirFinder calls keep only score ≥0.9 and p ≤0.05. A unit test proves the cut.
- The VirSorter2 writer emits whole-contig boundaries for `||full` and `||lt2gene` rows, and `full_bp_*` for partial rows. A unit test proves both.
- A curation step applies the Supp Fig 1 rules to CheckV's counts. It emits CheckV-trimmed sequences, and MMseqs2 clusters that curated set. A unit test covers each rule and the trimming choice.
- An island step runs `genomad annotate` on the vOTU representatives of at least 100 kb and removes those matching Antonio's list. The final vOTU set of at least 5 kb is a target. The recovery comparison scores the published vOTUs against it.
- The "paper gives no cut" statements are corrected in every file that repeats them.
- All new and existing `tests/metasmith_libraries/` viromics tests pass. The work sits on `feat/engine/bench-E3`, pushed. Nothing is staged or launched on fir.

## Tasks

- T1. Cut the driver to the viral lane and make the merge's lanes and callers table-driven (G1, G4)
- T2. Set up four agent worktrees from the checkpoint (G4)
- T3. Compact
- T4. Dispatch four Sonnet agents in parallel, one per gap (G2, G3)
- T5. Integrate the four branches, rebuild, solve and render the DAG (G1, G2, G3)
- T6. Compact
- T7. Update the E3 parity record and the page card (G1, G3)
- T8. Commit and push
- T9. Debrief

## Approach by task

### T1. Cut the driver and make the merge table-driven

I do this myself before any agent starts, because it touches the lines every agent would otherwise fight over.
- **Driver.** In `research/metasmith_benchmark/drivers/e3_pratama.py`, `build_targets` drops the following:
  - the MAG lane targets: prodigal orfs/gff, `assembly_stats`, the MetaWRAP targets, MAG DRAM and `--with-gtdbtk`
  - the annotation tail: vConTACT3, prodigal-gv, DRAM-v and `--with-host-prediction`

  It keeps read QC, the three assemblies, the frozen set, `contig_length_table`, CheckV, the vOTU table and the recovery table. It also loads `LIBRARY/resources/bench`, the DeepVirFinder image's resource library.
- **Merge.** In `e3/merge_candidate_calls_pratama.py`, lift the lane loop into two module constants, `LANES` (full type names) and `CALLERS`. Add a `lane` segment to `frozen_id` and a `lane` column to the provenance table. That closes I6 before the hybrid lane exists.
- **Checkpoint.** Rebuild with `library/build.sh e3`, solve, and commit the result as a checkpoint.

Gotchas:
- The flags `--with-gtdbtk` and `--with-host-prediction` go away. Delete them, don't leave them as dead arguments.
- Changing the merge body changes its transform id, so the frozen set recomputes. That is expected.

### T2. Set up four agent worktrees

Create four branches from the checkpoint: `feat/engine/bench-E3-hybrid`, `-dvf`, `-vs2` and `-curation`. Give each a worktree under `.claude/worktrees/`. Copy the staged `msm_solver` into each, then run `dev/libraries.sh -bm` and `library/build.sh e3 bench` there, so each agent can solve on its own.

Gotchas:
- A branch nested under `feat/engine/bench-E3/` is impossible, because git stores refs as paths. Hence the dash-suffixed names.
- Run git unsandboxed.

### T3. Compact

Handoff, on disk:
- this plan
- the checkpoint commit
- the four worktree paths and branches
- Antonio's scripts at `research/metasmith_libraries/viromics/reference/email/2026-05-15_viral-workflow-update/07_filtering2.sh` and `10_filtering_3.sh`

Facts a re-read will not recover:
- The published tally: SPAdes 113,105, MEGAHIT 112,924 and long-read 31,223 vOTUs. By caller, VIBRANT has 105,001, geNomad 89,689, VirSorter2 52,601 and DeepVirFinder 9,961.
- The island filter removed exactly 562. It took the counts from 257,814 to 257,252 at ≥5 kb, and from 82,807 to 82,245 at ≥10 kb.
- Supp Fig 1 has Filtering 2 taking 4,717,962 to 4,708,626.

### T4. Dispatch four Sonnet agents

Launch four Agent calls with `model: sonnet` in one message. Each works only in its own worktree, and each prompt carries the following:
- its gap, with the explorer findings from this session (file paths and line numbers)
- the files it owns and the files it must not touch
- the env recipe: `PYTHONPATH="$PWD/src" ~/.local/bin/mamba run -n msm`, and git unsandboxed
- its tests
- the rule: commit on its branch, never launch on fir, never push

**Agent A, hybrid lane.**
- Owns:
  - `hybrid_partners`, `check_plan` and the `LANES` constant
  - a new splitter `e3/split_hybrid_contigs_pratama.py`, which copies `splitContigsForAmr` but requires `e3::hybrid_spades_assembly`
  - a new fir staging script that makes one hard link per pairing under `/scratch/phyberos/pratama2026/hybrid_pairs/`, written but not run
- The fix gives each pairing its own path, so `GivenLibrary` stops collapsing them. `e3::hybrid_spades_assembly` keeps its type fence and gets no `sequences::assembly` properties.

**Agent B, DeepVirFinder.**
- Owns:
  - the new `e3/deepvirfinder_pratama.py`, which runs on `sequences::contig_batch`
  - a `deepvirfinder_candidate_virus` type in `e3.yml`
  - the `CALLERS` constant
- The new transform keeps the prefilter and the Theano warm-up from `bench/deepvirfinder.py`. It writes the merge's call-table columns, using the first header token as `contig_id`, and applies the cut. The bench transform must not also bind, so it is masked or given a distinct product type.
- B also fixes the "no cut" statements in:
  - `bench/deepvirfinder.py:1-2`
  - `drivers/e5_pilot.py:42`
  - `page/tool_table.py:74`
  - `drivers/e3_pratama.py:143-144`

**Agent C, VirSorter2 boundaries.**
- Owns only `e3/virsorter2_pratama.py` and its test. `||full` and `||lt2gene` rows emit 1..contig length. Partial rows emit `full_bp_*`.

**Agent D, curation and island filter.** It owns everything downstream of the merge:
- **CheckV per batch.** A CheckV batch step that keeps `viruses.fna`, `proviruses.fna` and `quality_summary.tsv`.
- **Curation per batch.** A curation transform applies the Supp Fig 1 rules to CheckV's `viral_genes`, `host_genes` and `gene_count`. It emits the trimmed sequence for proviruses and the whole sequence otherwise, with the lane-bearing `frozen_id` preserved.
- **Curated set and clustering.** A merge produces a curated set, and `mmseqs_votu_pratama` clusters it.
- **Island filter.** Representatives of at least 5 kb are selected. Those of at least 100 kb go through `genomad annotate` and Antonio's pattern list, applied to the columns his script reads.
- **Final set and recovery.** A final type for the vOTU set of at least 5 kb, plus a recovery transform that runs skani of the published vOTUs against it. The existing recovery on the frozen set stays as the pool-level score.
- D owns the viral target list in `build_targets` and the new types in `e3.yml`.

Gotchas:
- Antonio's contig-id regex `_[0-9]+$` collides with CheckV's `_1` provirus suffix. Key on the `frozen_id`, never on a stripped name.
- The keep rules read CheckV counts, not geNomad's, per the decision above.
- A and B both touch the merge's constants. Each edits only its own constant line.
- Both A's and B's own tests solve without the other's work, which is fine: the merge requires exactly the lanes and callers its constants list.

### T5. Integrate, rebuild, solve

Merge the branches into `feat/engine/bench-E3` in this order: C, B, A, D. That goes from the smallest diff to the largest. Resolve the `e3.yml` append conflicts by hand. Rebuild `e3` and `bench`, then run the viromics tests and `e3_pratama.py run --dag`. Check each acceptance criterion against the step list and the plan's task counts. Render `page/dags/e3_pratama.dag.svg`. Send any failure back to the owning agent through SendMessage rather than fixing it in place.

Gotchas:
- Every one of these changes forks the cache from wave 8. Name that in the report rather than hiding it.
- The recovery step needs the unpacked Zenodo FASTA on fir, which is not checked here.

### T6. Compact

Handoff:
- the merged HEAD
- the new step list and plan key
- each agent's test results
- anything the integration left open

### T7. Update the parity record and page card

Update `findings/E3_PARITY.md`:
- Close the four gaps in the viral table.
- Mark the MAG half and the annotation tail as out of scope for E3. They are not gaps.
- Record the two decisions: the keep-rule wording and the island list, with its deviation from the paper's shorter list.

Refresh the E3 card in `page/experiments.src.html`, rebuild the page and republish it to its existing URL. Point `reproduction_map.md` B5 at the new curation step.

Gotchas: load `write-docs`. Read the whole live page file before republishing.

### T8. Commit and push

Commit the integration and the docs on `feat/engine/bench-E3` and push them to origin. Delete the four agent worktrees and their branches once they are merged.

### T9. Debrief

Run the `debrief` skill.

## Callouts

- The hybrid fix only takes effect on fir once the staging script has made the hard links. Running it is a fir write, which belongs to the relaunch decision.
- Antonio's island list is broader than the paper's, so the removed count may exceed 562. The final report compares the two.
- The paper's own manual spot checks cannot be reproduced. The six example contigs in its repo are figure illustrations, not a sample.
- Scoring wave 8's archived recovery table for a baseline is still open. It needs a restore from chinook, which is outside this plan.

## Autopilot

Started 2026-09-28 08:20 after the principal said: "iterate on the pilot until whole pipeline completes sucesfully for at least 1 sample. Then complete workflow for all pratama samples". Tasks #23 (pilot), #24 (all 65 runs) and #25 (recovery curve per lane and caller) carry it.

Guardrails:
- fir ssh goes through the mux (`ssh -o BatchMode=yes fir`, unsandboxed). If the mux is down, reconnect with `mcp__awm__ssh connect host=fir`. Never a bare interactive ssh (Duo lockout).
- Stop a driver only with `scancel --batch --signal=USR1 <job>`.
- Launch protocol: commit, then `sync.sh` (with `SYNC_HOMES=/scratch/phyberos/pratama2026/metasmith`), then the dev overlay push, then check the quota (inodes below 950K), then materialise with `--import`, then launch.
- `ps` inside the sandbox sees its own PID namespace. Check pollers unsandboxed.

Resume handles:
- Pilot driver job 61912218, run key 6zpGUpXC, tag e3_hpilot, checkout `/scratch/phyberos/bench/checkout/5aaeafad`, log `/scratch/phyberos/bench/logs/e3_hpilot.61912218.out`.
- Run dir `/scratch/phyberos/pratama2026/metasmith/runs/6zpGUpXC/` (agent.log under `_metasmith/logs.*`).
- Watch log `W/.claude/e3_pilot_watch.log`, fed by `e3_pilot_poll.sh` (sacct state) and `e3_pilot_progress.sh` (agent.log counts, every 10 min). A Monitor tails it.
- Backup cron a11634e6, every 4 h at :17.

Failure modes to catch:
- Nextflow's "Error is ignored" hides dead steps. Read `nxf_tasks.csv`, not the exit status.
- The Zenodo FASTA for recovery must exist unpacked on fir.
- Success for #23 means `e3::final_votu_recovery_table` and `pratama::votu_recovery_table` both land for the pilot's samples.

### Live state

09-30 13:10 — #24 DONE, and #25 is next.
- The full run nP0Jxo8W COMPLETED at 12:52 with no unrecovered failures (4,704 COMPLETED and 14 recovered FAILED).
- The pool and final tables are scored (see the Run log from 11:15 and 13:10). Artifacts are in `$CLAUDE_JOB_DIR/tmp/e3_w9/`: `pool_curve.tsv`, `final_curve.tsv`, `diag_same.tsv` (pool), `diag_final.tsv`, `labels.txt`.
- The scoring mini tree on fir is `/scratch/phyberos/bench/e3_score`.
- The watch is ended: crons deleted, pollers killed, Monitor stopped.
- Next steps for #25, in order:
  1. Load write-docs.
  2. Update `findings/E3_PARITY.md` with the curve by lane and caller, the stage counts and the 2019 SPAdes same-sample finding.
  3. Update page artifact HsKkBar4tsmje8RZn1TfNS, reading it first.
  4. Record the pilot and wave e3_w9 in `findings/R1_WAVES.md`, including the env-index cache finding, the restore and the next-wave resource fixes (DVF 32 GB, merge 32 GB, pool recovery 128 GB, mmseqs `--split-memory-limit`).
  5. Run the pre-report adversarial review.
  6. Commit and push.
  7. Debrief.

09-29 08:10 — #24 RUNNING as driver 62089381, key **nP0Jxo8W**. The env ids fold into the plan key, so the relaunch staged a new key and RewyVqFo is dead.
- The cache serves:
  - seqkit, bbduk, fastp, MEGAHIT and SPAdes: 65 of 65 `_cached`
  - both splits: 65 + 65
  - VIBRANT and geNomad on every batch: 286 + 561
- Fresh work: VirSorter2 on every batch (its protocol changed for the boundary fix, and the cache key folds the protocol hash, so this is expected), DeepVirFinder (new), 17 hybrid SPAdes, the hybrid lane and the tail.
- Pollers: poll 3950869, progress restarted on nP0Jxo8W. Crons: aa55a508 hourly at :47, 6b3f3c54 every 4 h at :17. Their prompts name RewyVqFo, so read nP0Jxo8W instead.

09-29 07:40 — #24 is LAUNCHED. Driver 62088624 (`e3_w9`), key `RewyVqFo`, checkout 85f49722.
- Materialise 62087657 COMPLETED: `Plan OK -- 34 steps, key=RewyVqFo`, 12 images present.
- All 65 read_pair, short_reads_pe and read_metadata givens, plus the refs, carry their archived identities (checked against the untouched archived index). Only the 17 hybrid nanopore givens are new.
- Pollers 3936385 (progress) and 3936386 (poll) feed `.claude/e3_pilot_watch.log`.
- Crons: ea64a6e1 runs hourly at :47 and 90374f27 every 4 h at :17. The old a1ea695a and a11634e6 are deleted.
- Next: confirm that `_cached` processes serve p01, p02, p04, p05, the splits and the 3 callers. Hybrid SPAdes (17), DVF, the hybrid-lane callers and the whole tail compute fresh.

09-29 08:00 — #24 is restoring wave 8's cache before launch.
- Globus shard restore 11ff4a86 is running, 1.23 TB into `task_cache/`. A background waiter watches it, and the hourly cron is the backstop.
- On SUCCEEDED, swap the index and launch:
  1. On fir, move `task_cache/cache.sqlite` to `cache.sqlite.pilot_6zpGUpXC`. Copy `/scratch/phyberos/bench/e3_restore/cache.sqlite` into place and check its sha starts `eaee257a`. Confirm there is no `-wal` file.
  2. Check the quota.
  3. Run `e3_pratama.py run --materialise --import --launch --tag e3_w9` (no `--runs`) through `submit_driver.sbatch` from checkout `/scratch/phyberos/bench/checkout/85f49722`, submitted in `/scratch/phyberos/bench/logs`.
  4. Verify the hits in the first `agent.log` minutes: the `p01`, `p02`, `p04` and `p05` tasks should be served from cache.
  5. Repoint the pollers and crons at the new job.
- Checkout 85f49722 is synced and pushed.

09-29 07:10 — #23 is done, and #24 is in pre-launch review.
- Pilot 61918508 COMPLETED at 06:35.
  - All 34 steps show COMPLETED in `nxf_tasks.csv`. The only FAILED rows are the 3 retried tasks.
  - Record counts: pool 180,898, then curated 150,290, then 97,828 vOTUs, then 8,739 reps of at least 5 kb, then 8,734 after the island filter (5 of 5 large reps removed).
  - The paper kept 75 vOTUs of at least 100 kb and removed 562, about 88%, so the pilot's island filter rate is consistent with it.
- Final score: same-sample recovery 79.0% at ANI 95 / AF 85, 84.5% at 95/50 and 85.6% at 90/30.
  - By lane at 95/85: hybrid 83.9%, SPAdes 68.6%, MEGAHIT 62.3%.
  - Pool-level same-sample recovery at 95/85 is 84.4%.
  - Tables: `$CLAUDE_JOB_DIR/tmp/e3_pilot/{pool,final}_curve.tsv`. Label map: `labels.txt`.
- Commit 0de8d108 drops `spades_pratama` from IN_PLACE_STEPS. Its in-place `spades_ws` was about 120 GB per sample, which comes to about 10 TB at 65 runs against 8.2 TB free. The same commit raises island annotate to 32 GB.
- An adversarial review of the launch is running. Once it returns: triage the findings, then push, sync.sh, check the quota, materialise with `--import` and no `--runs`, and launch with tag `e3_w9`.

20:50 — Waiting on SPAdes only.
- The MEGAHIT lane is done through all 4 callers.
- `spades_pratama` (3) is done. The 384 GB retries of (1) and (2) are 5 h in, still in hammer error correction and past the old failure point. `-m` is 364 GB.
- Hybrid SPAdes (1) is done. (2) and (3) are at 10.6 and 11.2 h, against a 36 h flat cap.
- Once all SPAdes finish, the order is: split → callers on the spades and hybrid lanes → merge → CheckV → curate → MMseqs2 → island → recovery.
- Watch: cron a1ea695a runs hourly at :47, and a11634e6 every 4 h. Pollers 336972 and 336973 feed the watch log.
- #25 tool is ready: `results/e3/votu_recovery_curve.py curve <skani table> --scope final --runs SRR32696677 SRR32696679 SRR32696689`.

16:20 — The MEGAHIT lane is through all 4 callers on 15 batches. VIBRANT and DVF are done, and geNomad (5) and VS2 (3) are finishing.
- `spades_pratama` (3) is done. The 384 GB retries of (1) and (2) are running.
- One hybrid SPAdes is done and 2 are running.
- No failures beyond the 2 SPAdes memory retries.

Earlier state below.
12:20 — Driver 61918508 (checkout a59c7332, key 6zpGUpXC) is RUNNING.
- Read QC is done.
- MEGAHIT (3) exited 0.
- The other 8 assemblies are running, the longest at 2:51.
- Downstream steps submit only once a step's whole array closes (`array = 25`), so the split waits for all 3 MEGAHIT tasks.
- Pollers: `e3_pilot_poll.sh 61918508` and `e3_pilot_progress.sh <log> 61918508 6zpGUpXC`.
- Cron a1ea695a checks hourly at :47, replacing 30-min Monitor re-arms. Cron a11634e6 stays as the 4 h backup.

### Run log

- 08:20 Autopilot started. Pollers from the previous context were alive, and Monitor bif7ssqwe was reused.
- 08:25 Six tasks failed and were ignored (seqkit_reads ×3, fastp_report ×3), and bbduk (3) retried. Cause: this morning's `Deploy` of the pratama home wrote the driver's relay setup lines into `lib/agent.yml`. The CAMI home's file, deployed 09-09, has only `module load apptainer`. Every task shell prepends those lines, and the echo's extra output line fails bootstrap's `assert len(res.out)==1` on `pwd`. Fix: rewrote the home's `agent.yml` in place (backup `agent.yml.relaylines.bak`), mid-run, so pending bbduk tasks pick it up. Added `_common.deploy_home`, commit c3b8ceb8. The six ignored tasks need a relaunch to rerun them. A relaunch is cache-safe.
- 09:15 Principal: "go over the resource asks. refer to previuos projects like cyanoverse and spanish-lakes. 128GB ram is pretty rediculous and nextflow auto-doubles anyways. use rpp-shallam account". Evidence:
  - The September sacct (`/tmp/sacct_sep.txt`, 373K rows) put every caller's MaxRSS at or under 12 GB, while each asked for 24–32 GB.
  - bbduk's and MEGAHIT's RSS follows the grant, so it measures nothing about need.
  - spanish-lakes (`fir:/home/phyberos/project-rpp/spanish_lakes/metagenomics/_logs`) ran bbduk at 2 cpus / 32 GB and MEGAHIT at 8 / 32 with no memory failure. Its sacct records are purged.
  - Nextflow's trace `peak_rss` reads about 165 MB for every task, because it measures the wrapper, not the container.
  - metaSPAdes keeps 192 GB (40 of 65 failed at 192 and finished at 384).

  Change: an E3 `SCALED` of 8 steps (commit a59c7332), all driver-side, so no cache key moves. The account was already rpp-shallam everywhere E3 submits.
- 10:20 #25 prep, done while the assemblers run.
  - `results/e3/votu_recovery_curve.py` plus `published_votu_tally.tsv.gz` (commit e8a47537).
  - The tally reproduces all 7 published counts.
  - The pilot's 3 runs hold 6,932 published vOTUs, 5,156 of them hybrid.
  - To score a table: fetch it from `runs/6zpGUpXC/results/`, then run `curve <table> --scope final --runs …`.
- 12:31 `spades_pratama (2)` (SRR32696679) failed at 2:49 into hammer.
  - The log said: "too many k-mers … need approx. 248.368GB". This is the known 192 GB failure. The retry at 384 GB is PENDING on a large node.
  - Kept 192 GB for #24. The waste is about 40 failures × 2.8 h × 48 cpus, or about 5.4K core-hours. A 256 GB base bills about 16 extra cpu-equivalents × 65 × ~8 h, or about 8.3K. Roughly a wash, and a first-attempt success stays cheaper.
- 13:08 `spades_pratama (1)` (SRR32696677) failed the same way, at 3:24: "need approx. 257.224GB". Its 384 GB retry is PENDING.
  - Both needs sit just over 192 GB. A 256 GB base (`-m` 243) would still miss 257.
  - Revisit the #24 base once (3) finishes or fails.
- 21:06 Both `spades_pratama` 384 GB retries are through hammer and into k-mer counting at 5:13. Their spades.log peaks read 319 GB and 333 GB, so the need is well above the 248/257 GB that the 192 GB failures estimated. For #24, 256 GB would fail too. The choice is 192 (fail about 3 h, then retry) or 384 from the first attempt.
- 09-29 02:20 All 9 pilot assemblies were done. The spades_pratama retries peaked at MaxRSS 324 and 332 GB. Hybrid peaked at 180, 375 and 384 GB.
- 09-29 05:26 The merge ran, followed by `pratama::votu_recovery_table` (403K skani rows, landed 05:33) and CheckV (2 batches).
  - Finding: the merge labels a sample `read_pair@<hash>`, not the run accession. `_label`'s JSON read falls back to the file stem. The curve script now takes `--labels` (an `ls -d imports/e3/*/read_pair@*` listing) and fails on an unmapped label (commit 2a098181).
  - The pool score is in `$CLAUDE_JOB_DIR/tmp/e3_pilot/pool_curve.tsv`. Across all lanes and callers, same-sample recovery is 95.1% at ANI 90 / AF 30, 93.1% at 95/50 and 84.4% at 95/85. Hybrid scores 87.9% at 95/85, MEGAHIT 72.0% and SPAdes 77.6%.
- 09-29 06:27–06:30 CheckV batch 1 finished after 54 min. The rest of the tail submitted within 3 min: checkv_merge, curate_trim ×2, curate_merge, mmseqs_votu, votu_representatives and island annotate.
  - `genomad_island_annotate_pratama (1)` hit an OOM at the 16 GB SCALED grant, in `mmseqs prefilter ... --split 0` against the whole geNomad DB. It is retrying at 32 GB. My SCALED row was sized from the end-to-end geNomad MaxRSS (11.3 GB), and that run's shape differs.
  - For #24, raise the SCALED row to 32 GB if the retry passes.
- 09-29 07:50 Pre-launch review triage (fresh agent, commit 0de8d108):
  1. **Verified, acted on.** The pilot recomputed short-read QC, assembly, splits and calls. Wave 8's archive holds them under the same transform keys (mGUj0qK9, SXCRlr08, 5sydgJyV, WNqbyv4N, YWuk2pSk, rt01cPWU, 1dNUzYw4, VI2Yo4tR, P6wZyE3C, all matched against `step_transform_map.tsv`), and the cache epoch is 6 on both sides.
     - The pilot missed because the fir index was fresh. It minted new import ids, so 0 of 330 lineage keys and 0 of 27 import identities are shared with the archive.
     - `EnsurePoolEntries` reuses a held entry by name, so a restored index brings back the archived identities.
     - Restoring 3,262 shard dirs (1.23 TB) plus `imports/` via Globus: task 11ff4a86 (shards) and 13bf88e7 (imports, SUCCEEDED). The archived index is at `/scratch/phyberos/bench/e3_restore/cache.sqlite` (sha matches `eaee257a`).
  2. **Verified, acted on.** Hybrid SPAdes' uncapped ladder would reach 3,072 GB on attempt 4, and fir refuses that at submission (768 to 1,500 GB pass `sbatch --test-only`, on cpularge). Commit 85f49722 adds hybrid to SCALED at (48, 384, 36) with LARGE_MEMORY_STEPS at 768. The `-m 380` clamp stays, for parity with Pratama.
  3. **Accepted as a reporting rule.** Final-set same-sample recovery shrinks as samples are added, because MMseqs2 picks one representative across samples. #25 reports `fraction_any` for the final set and same-sample for the pool.
  4. **Rejected.** The pilot trace shows bbduk at 6 min and MEGAHIT at 1.7 to 6.3 h, well inside (4, 16, 2) and (16, 64, 12). The restore makes both hits anyway.
  5. **Accepted.** MMseqs2 or the island filter may take one extra memory rung. The pilot ran them in under 1 min.
  6. **Verified clean.** `hybrid_pairs` holds 17 entries. The launch uses `--materialise --import`.
  7. **Rejected.** The island regex's 5 of 5 matches the paper's 88% removal rate (562 of 637).
- 09-29 07:17 Globus shard restore 11ff4a86 SUCCEEDED: 27,118 files, 1,230.8 GB. All 3,262 roots carry `manifest.cbor`. Quota is 13.04 TB and 582,554 inodes.
- 09-29 07:19 Swapped the index. `task_cache/cache.sqlite` is now the archived one (sha `eaee257a`, 243 imported, 11,083 lineage). The pilot's index is kept as `cache.sqlite.pilot_6zpGUpXC`.
- 09-29 07:20 Submitted materialise and import job 62087657 (`e3_w9_mat`, checkout 85f49722). It went in without `--exclude`, because my regex missed the multi-line FIR_BAD_NODES. I added the exclusion with `scontrol update` while the job was PENDING.
- 09-29 07:27 The first launch, 62088624, missed cache on every task. All 65 seqkit, bbduk and fastp tasks took the miss channel (`__miss_N`, not `_cached`).
  - Cause: the environment leaf ids differ. The archived seqkit payload consumes env `1e206e2899…`, while checkout 85f49722 compiled `1e20b7997500…`. The env library content is identical (`git diff 61c0eebc 85f49722 -- resources/` is empty). Only the compiled `_metadata/index.yml` differs, and its ids are tree-local.
  - Stopped with USR1 at 3:47. No orphans.
- 09-29 07:35 Fix: copied `checkout/61c0eebc/src/metasmith_libraries/resources/env/_metadata/index.yml` (Sep 21 mtime) into the local worktree, which is an untracked build product. The previous local copy is backed up at `$CLAUDE_JOB_DIR/tmp/e3_pilot/env_index_local_backup.yml`. Re-synced, and the fir checkout now reads seqkit `1e206e2899`.
  - Chained materialise 62089379 and driver 62089381 (afterok). Pollers 3950869 and 3950870.
  - CAUTION: until this is resolved in the engine, any E3 relaunch that must reuse wave 8's cache needs this env index in the worktree it syncs from.
- 09:17 Stopped 61912218 (USR1, clean). Relaunched as 61918508 from checkout a59c7332, same key 6zpGUpXC, tag e3_hpilot.
- 09-29 12:45 w9 healthy at 5:07: 0 ignored or retried. The 17 hybrid SPAdes are all running. Only 11 short-read caller tasks remain. Four DeepVirFinder tasks are at 5:00–5:03 of their 6 h SCALED limit (8 cpus). The pilot's 1.7 h was measured at 16 cpus, and some batches are larger. A TIMEOUT retries at a doubled limit. The retry costs up to 12 h, which is off the critical path because the hybrid lane still has SPAdes (to about 23:00) and its callers ahead. For the next wave, give DVF (8, 16, 12) or 16 cpus.
- 09-30 01:42 All 17 hybrid SPAdes finished with no retries. The last one ran about 16 h, past the pilot's 12.5 h but inside the 36 h cap. The hybrid split (p07) submitted 17 tasks. Next come the four callers on the hybrid batches, then merge, CheckV, curation, MMseqs2, the island filter and recovery. Callers on the hybrid lane are now the critical path. DVF's 6 h limit could force a 12 h retry again, as it did on the short-read lane.
- 09-30 07:15 Cause of the DVF 6 h timeouts: memory, not cpus. sstat on the hybrid-lane DVF array 62231659 reads MaxRSS 16,767,324 K. That sits at the 16 GiB cgroup cap, with AveCPU only 14:48 after 5.3 h, so the task thrashes against the limit instead of dying of OOM. It explains why every 32 GB retry finished within an hour. For the next wave, set SCALED `deepvirfinder_pratama` to (8, 32, 6). The pilot's 8.2 GB MaxRSS came from smaller batches.
- 09-30 09:15 The merge (p22, 62268542) submitted at 08:24, when the last DVF retry finished. At 47 min its RSS sits at the 16 GiB cap (MaxRSS 16,766,788 K) but it is still progressing: 54 s of cpu per 90 s of wall, and 13.7 GB written. If it passes its 4 h limit, it retries at 32 GB. For the next wave, give `merge_candidate_calls_pratama` 32 GB. It holds every call over 65 runs × 3 lanes × 4 callers in memory.
- 09-30 10:09 The merge finished after 1 h 37 min, then contig_length_table, split_viral_contigs and CheckV were submitted. The pool recovery (p25, skani dist of the 257K published vOTUs against the whole merged pool) hit an OOM at 32 GB after 1 min (MaxRSS 33.5 GB). It is retrying at 64 GB (62277378), and the declared ladder doubles uncapped from there. For the next wave, start it at 128 GB: the pool's reference is about 20 times the pilot's.
- 09-30 11:10 The pool recovery table landed at 10:14 as a 7.0 GB skani TSV, passing on the 64 GB retry. I scored it on fir, not locally: sbatch 62283560 (`e3_w9_poolscore`, 48 GB, 1 cpu) ran from a mini tree at `/scratch/phyberos/bench/e3_score`, which holds the script, the tally and runs.tsv. The sync leaves `results/` out of the checkout. Labels are at `/scratch/phyberos/bench/e3_w9_labels.txt` (65 lines) and output goes to `e3_score/pool_curve.tsv`. All 48 CheckV tasks are running.
- 09-30 11:15 Pool score, 65 runs, 257,252 published (`$CLAUDE_JOB_DIR/tmp/e3_w9/pool_curve.tsv`), any-sample / same-sample:
  - 90/30: 99.2% / 96.5%
  - 95/50: 97.3% / 90.9%
  - 95/85: 82.1% / 67.6%
  - By lane at 95/85: hybrid 92.6 / 88.4, MEGAHIT 85.4 / 74.7, SPAdes 75.9 / 54.7.
  - By caller at 95/85 (any): geNomad 83.2, VIBRANT 82.3, DVF 81.1, VS2 80.0.

  Same-sample at 95/85 falls well below the pilot's 84.4%, and the any/same gap is widest on SPAdes (21 points). Before trusting the same-sample column, I submitted diag 62284167. It reports same vs any per published sample and lane, with the runs whose sequences match instead, to catch a sample-key mismatch.
- 09-30 11:25 Diag 62284167 (`tmp/e3_w9/diag_same.tsv`, per sample × lane at 95/85) rules out a key mismatch. Every published sample maps to exactly one run. The sources that match in place of a sample's own are always its biological neighbours: the other replicates of the same well and filter, then the same well in the other year. So the same-sample column measures something real.
  - **Finding:** the SPAdes same-sample shortfall is a 2019 effect. 2019 SPAdes reaches only 0.32–0.49 same-sample (any-sample 0.58–0.84), while 2019 MEGAHIT reaches 0.59–0.82. 2022 SPAdes reaches 0.57–0.91. Hybrid is 0.79–0.92 everywhere.
  - **Hypotheses for #25, not yet tested:** Pratama's 2019 SPAdes vOTUs may come from a different assembly setting or read set, for example co-assembly or a different metaSPAdes version or k-mers. Or our cached wave-8 metaSPAdes on 2019 NextSeq reads may differ.
  - **Cheap check:** compare total length and N50 of our 2019 SPAdes assemblies against 2022.
- 09-30 12:00 CheckV (48) finished, then curate_trim (48), curate_merge and MMseqs2. The `mmseqs_votu_pratama` first attempt hit an OOM at 64 GB after 48 s, in linclust's kmermatcher. mmseqs sizes its k-mer table from the node's RAM, not the cgroup, so it never splits under a Slurm grant. The retry at 128 GB (62287201) is PENDING. For the next wave, pass `--split-memory-limit` at about 80% of the grant in the transform, which makes it cheaper and deterministic.
- 09-30 12:52 **w9 driver 62089381 COMPLETED**, "run completed at 12:52:04". `nxf_tasks.csv` holds 4,718 rows: 4,704 COMPLETED and 14 FAILED, with every FAILED row recovered by a later COMPLETED. The 14 are 12 DVF memory stalls, the pool-recovery OOM and the MMseqs2 OOM. Record counts:
  - pool: 7,552,829
  - curated: 7,102,468
  - vOTUs: 4,018,877 clusters
  - reps of at least 5 kb: 243,688
  - large (at least 100 kb): 639, of which the island filter removed 502 (78.6%)
  - final: 243,186

  Against the paper: 257,814 reps at ≥5 kb before its island filter and 257,252 after. The paper had 637 large reps and removed 562 (88.2%). Our final set is 94.5% the size of the published one. Final scoring job 62291085 writes `e3_score/final_curve.tsv` and `diag_final.tsv`.
- 09-30 13:10 **Final score**, 65 runs, 257,252 published, headline `fraction_any` (`tmp/e3_w9/final_curve.tsv`, diag `tmp/e3_w9/diag_final.tsv`). Figures are any-sample / same-sample:
  - Overall:
    - 90/30: 93.4 / 73.2
    - 95/50: 91.4 / 69.7
    - 95/85: 78.5 / 55.8
  - By lane at 95/85:
    - hybrid 89.8 / 81.3
    - MEGAHIT 81.8 / 61.1
    - SPAdes 72.2 / 43.4
  - By caller at 95/85 (any): geNomad 81.0, DVF 77.8, VIBRANT 77.6, VS2 76.1.

  Pool to final loses about 3.6 points at 95/85 (82.1 to 78.5) and about 6 at 90/30 (99.2 to 93.4). The ≥5 kb cut on reps, curation and clustering explain the loss. Watch ended: crons aa55a508 and 6b3f3c54 deleted, pollers killed, Monitor stopped. #24 done, and #25 is in progress.
- 09-30 14:10 #25 report checkpoint, commit 0053afb1 (not pushed yet):
  - E3_PARITY.md gains a Recovery section, a refreshed relaunch paragraph with the env-index CAUTION, and resource heuristics updated to the current SCALED and line numbers. Two new open gaps: 2019 metaSPAdes recovery, and pool size plus keep-rule drop.
  - R1_WAVES.md gains § HH (the pilot and e3_w9).
  - SCALED now has DVF (8, 32, 6), merge (4, 32, 4), pratama_votu_recovery (8, 128, 4) and mmseqs (16, 128, 6). These are driver-side, so no cache key moves.
  - Scripts committed: `results/e3/votu_recovery_by_sample.py` (the per-sample diag) and `assembly_size.py`.
  - Cheap check, job 62321255: our 2019 metaSPAdes is NOT smaller. The median run has 134 Mbp ≥5 kb against 118 Mbp in 2022, N50 375 against 385. 2019 SPAdes is also low on any-sample (68% against 83%), so the cause stays open.
  - Pool per-year same-sample (weighted): 2019 SPAdes 0.42, 2019 MEGAHIT 0.75, 2022 SPAdes 0.66, 2022 MEGAHIT 0.75, hybrid 0.88.
  - Page draft at `$CLAUDE_JOB_DIR/tmp/e3_viral_parity/e3_viral_parity.html`. It adds a results section and a new open question "next-gap". The live db's 7 earlier answers are all answered and acted on.
  - The pre-report adversarial review is running.
- 09-30 14:50 Pre-report review triage (fresh agent, commit 0053afb1). Everything was re-checked before any edit:
  1. **Verified, fixed.** DVF failures were not TIMEOUTs. sacct shows 12 tasks at 5:59 elapsed: 11 OUT_OF_MEMORY and 1 FAILED. Units are now in GiB.
  2. **Verified, fixed.** "A 2019 effect" overstated it: 2022 metaSPAdes reaches 66% against MEGAHIT's 75%. Now reads "trails in both years, most in 2019".
  3. **Verified, fixed.** The neighbour column was keyed by sample, not lane, and counted hit rows. The script now counts missed vOTUs per sample and lane (re-run 62322621; any/same unchanged). The doc cites the 2019 MEGAHIT control instead.
  4. **Accepted.** Pool-any column added. The R1 attribution of the reporting rule is corrected.
  5. **Accepted.** N50 dropped, 2019 MEGAHIT 142 Mbp added, and the ref. 57 / NextSeq hypothesis added to the open gap.
  6. **Accepted.** Stage-table units are caveated, and the source of 637 (562 + 75) is stated.
  7. **Accepted.** Stale page questions removed. Only next-gap and other remain.
  8. **Accepted.** SCALED comments and the mmseqs note.
  - Commit 4e30f1b2 pushed. Page republished (v6).
- 09-30 15:00 #25 DONE. The watch had already ended. Debrief: plan copied to `data/metasmith/plans/13-e3-viral-replication-runlog.md`.

# Archive E3 to chinook, then release its scratch

## Context

E3 is done in practice: 38 of 39 steps, 0 errors, 0 ignored. The 39th cannot run — `DRAM-v.py annotate` feeds
all 58,593 contigs to one call whose final act is a GenBank writer measured at O(N^1.96), extrapolating to ~57 h
against a 24 h cap, and `dramv_distill` would then crash on a `vogdb_categories` column the staged database
cannot produce.

Meanwhile E4 is held: scratch is at 784,373 of 1,000,000 inodes and E4 needs ~161,800 more to finish its
corpus. E3's home is 166,941 inodes and 2.0 TB, so releasing it alone unblocks E4.

This moves E3's home to the chinook Globus collection, verifies it, then releases the scratch copy — so that
coming back means replaying from cache, not recomputing.

## What you said

> can you archive the current E3 work on chinook (so that we can resume later)? This frees the inode and disk
> space for E4 and may be sufficient if we come back later to find that the results are acceptable.

> It is crucial to get right and not lose data to prevent rerunning from scratch, so please plan this out first

> verify option 1 is sufficient to resume. Some computation is fine, but redoing assembly, binning, dram, or
> other long compute tasks must be avoided

> should also include a "how to resume" readme for the next agent that picks this up

## Issues

- I1. E3's `task_cache` is the only copy of 2.15 TB of products and has no backup.
- I2. E4 is ~161,800 inodes short; E3's home holds 166,941 of them.
- I3. E3's driver is still running, so the tree is live and `cache.sqlite` cannot be quiesced.
- I4. `cache.sqlite` is WAL-mode, nothing checkpoints it, and no code can rebuild it — losing it makes every shard unaddressable with no error raised.
- I5. Addressability folds absolute paths that live outside the home, so restoring elsewhere yields a silently cold cache.
- I6. Nothing here has ever restored an archive; the one precedent archived outputs, not a cache, and discarded its verification listing.

## High-level goals

- G1. Put E3's work somewhere durable, off scratch.
- G2. Be able to resume later without repeating the long compute.
- G3. Free enough space for E4 to finish.
- G4. Leave restore instructions good enough for someone who wasn't here.

## Acceptance criteria

- Globus transfers with `--verify-checksum`, and a recursive destination manifest is **saved and committed**, not piped away.
- `cache.sqlite` is checkpointed (`wal_checkpoint(TRUNCATE)`) with no driver holding it, and reads **10,951 lineage / 243 imported / 0 tombstoned** both before the transfer and from the archived copy afterwards.
- Destination file count and bytes match the source, measured independently of Globus's own success report.
- Nothing is deleted until all the above pass.
- `RESUME.md` exists in the archive and the repo, naming the exact restore path, pinned image and checkout, and the external trees that must survive.
- `lfs quota` read directly shows ≥160,000 more free inodes afterwards.

## Tasks

- T0. Recover the campaign run log from the transcript and land it under version control (G4)
- T1. Stop E3's driver by gated USR1 and confirm the finalizer indexed the run (G1)
- T2. Quiesce and fingerprint `cache.sqlite` (G1, G2)
- T3. Record the pre-archive inventory (G1, G4)
- T4. Transfer to chinook with checksum verification (G1)
- T5. Verify independently of Globus's own report (G1, G2)
- T6. Write and commit `RESUME.md` plus the manifests (G4)
- T7. Compact
- T8. Release the scratch copy (G3)
- T9. Confirm the reclaim; leave E4 for Tony to restart (G3)
- T10. Debrief

## Approach by task

**T0.** I overwrote this plan file before extracting the campaign's run log, and no backup of it exists. It is
fully recoverable: line 8873 of
`~/.claude/projects/-home-tony-agentic-workspace-projects-metasmith-engine-bench-run/865b490c-….jsonl` holds a
241,740-char string containing the whole plan as of my last edit — verified by locating both `# R1 wave 7
continued` at char 13561 and the final run-log entry at the tail. Slice from char 13561 to the end (~228 KB,
~820 lines) and write it to `data/metasmith/plans/11-r1-benchmark-campaign-runlog.md`, matching the ten
git-tracked siblings there, then register it with `artifact(verb="register", artifact_type:"report")`.
*Gotchas:* `data/metasmith/plans/` is **git-tracked, not DVC-pinned** — there is no `data/metasmith.dvc`, and
`data/.gitignore` excludes only `scratch/`, `kbase` and `lineage-report-demo`. So a plain commit versions it. If
you want a real DVC object instead, that means `dvc add data/metasmith`, which converts a currently git-tracked
tree belonging to the whole engine project — do that deliberately, not as a side effect of this task. Do this
task **first**: the transcript is the only copy, and it is a live file that rotates.

**T1.** `scancel --batch --signal=USR1 60837426`, through `submit_driver.sbatch`'s trap — never a plain scancel,
which orphans grid jobs. The hook cancels p37/p38; the finalizer is a separate process, survives, and runs
`record_run` to index this run's shards. Wait for `COMPLETED 0:0` and no `PID.lock`.
*Gotchas:* `record_run` is what writes the newest rows into `cache.sqlite` — archiving before it finishes
archives a stale index. Its group-kill fires 30 s after `PID.lock` removal, which the record already calls too
short; check for orphans rather than assuming.

**T2.** The most important step. With no driver and no `PID.lock`, `PRAGMA wal_checkpoint(TRUNCATE)`, confirm
the `-wal` sidecar is empty or gone, then record entry counts by origin, `schema_meta`, and the file's sha256.
Archive any sidecars anyway.
*Gotchas:* every read uses `file:...?immutable=1` — a plain WAL open recreates sidecars and bumps the
`task_cache/` root mtime. Never write inside `task_cache`.

**T3.** Save as files: recursive listing with sizes and mtimes, per-directory inode counts, total bytes,
`cache.sqlite` sha256. Store outside the archived tree and commit.
*Gotchas:* E1's tars were verified by `tar tf | wc -l` piped to nothing and had to be re-listed days later.

**T4.** fir endpoint `8dec4129-9ab4-451d-a45f-5b4b8471f7a3`, chinook `2602486c-1e0f-47a0-be15-eec1b0ff0f96`;
both confirmed readable today. fir has its own endpoint, so bytes go fir → chinook and never cross this
workstation — no tar, no staging space. **Destination must be a sibling of `altair/`**, e.g.
`/Workspace_backups/Tony_Liu/fir_bench_e3/`: the nightly `workspace_backup` mirrors into `<prefix>/workspace/`
*with deletion*, so an archive under there would be erased for not existing locally.

Archive set: `metasmith/task_cache/` (79,371 inodes, 2.0 TB), `metasmith/imports/`, `metasmith/lib/`,
`runs/Qt0rbV1R/_metasmith/` and its `workflow.*`, `interleaved/`, `reads_2019/`, `reads_2022/`,
`zenodo_17897233_unpacked/`, and p38's rescued `annotations.tsv`.

Excluded, and this is the exclusion you asked me to verify: `runs/Qt0rbV1R/nxf_work/` (~79,269 inodes of
nextflow scratch, never consulted for a hit) and `runs/Qt0rbV1R/results/` (5,317 inodes of **hardlinks into
task_cache**, sampled at `links=3`; Globus does not preserve hardlinks so these would duplicate the bytes).
**The check passed: of 10,951 lineage shards, ZERO reference an absolute path** — every product is held under
its own shard by relpath, and a hit is decided only from the shard on disk. The census confirms the expensive
work is present: `spades_pratama` 65, `megahit` 68, each `metawrap_*` 65, genomad/virsorter2/vibrant 847 each,
checkm2 2,261, DRAM and kofam lanes 562 each.
*Gotchas:* skip `relay/` (per-node sockets) and `dev/` (comes back from git). Many small files, not bytes, will
dominate the wall time.

**T5.** Globus reporting success is not verification. `globus ls -r` the destination, diff file count and bytes
against T3 per subtree, then fetch `cache.sqlite` back and re-read its counts.
*Gotchas:* `--verify-checksum` proves each transferred file's integrity, not that every file was offered — the
count diff is what catches a dropped subtree.

**T6.** `RESUME.md` must state: the exact restore path and why it is mandatory (`msm` bakes `AGENT_HOME`,
`agent.yml` records `real_path`, and `given_name()` folds the **absolute path string** of every declared input,
so a different path renames every pool entry and silently cools the whole cache); the external trees not in this
archive that must still exist (`refs` 217 G, `viromics_refs` 324 G, `databases` 80 G, `staging` 273 G,
`wave2_b3_nfcore` — note `ref::vibrant_db` was itself copied out of this cache); the pins — image
`metasmith:0.22.1`, checkout `61c0eebc`, `CACHE_KEY_VERSION = 6`, **a higher key version mass-tombstones every
non-imported row on first open and a later `gc --delete` would reclaim 2 TB**; that `cache.sqlite` cannot be
rebuilt; where E3 stands and the three reasons its 39th step cannot run; and that a resume re-stages a fresh
run directory.

**T7.** Handoff: Globus task ID, destination path, verification numbers, todo position.

**T8.** Only after T5 passes. `delete_stage.sbatch` for `runs/Qt0rbV1R` so its gates run, then the rest of the
home. *Gotchas:* irreversible — re-read T5's verification immediately before, not from memory. Do **not** delete
`refs/`, `viromics_refs/`, `databases/`, `staging/`, `wave2_b3_nfcore`.

**T9.** Read `lfs quota -p 83115734 /scratch` directly. Expect ~784,373 → ~617,400, leaving ~382,600 free
against E4's ~161,800 need. Report it; **do not restart E4** — that hold is Tony's to lift.

**T10.** Run the `debrief` skill.

## Callouts

- The only irreversible act is T8; everything before it is additive.
- Stopping the driver costs nothing real, since p37 and p38 provably cannot finish. If 39/39 later matters, the route is chunking the contig FASTA (every task then lands under 2 h) plus staging VOGDB — and this archive is what makes that a resume rather than a restart.
- The previous campaign plan occupied this file; its findings live in `research/metasmith_benchmark/findings/` and the scope journals, but its long-form run log was node-local only.

## Autopilot

Run state for whoever picks this up. Everything above this line is the approved plan.

### Live state (2026-09-23 01:00 PDT — the run is closed)

**Every task A1-A10 is done. E3 is archived on chinook, verified, and its scratch home is
deleted.** The watch is ended deliberately: monitor `b8c61qjs2` stopped, cron `9edc5c96` deleted.
Nothing in this plan is left to execute, and no guard of this run's is armed.

What a later session needs is not this section but
`research/metasmith_benchmark/results/e3/RESUME.md`, which is the restore procedure. The three
facts that matter most: restore to `/scratch/phyberos/pratama2026` **exactly**, run the archived
SIF rather than the `0.22.1` tag, and keep the 15 external paths that the archive does not
contain.

**The reclaim, measured.** Job `61039924`, COMPLETED 0:0 in 29:03.

    before   17.799 TiB   800,096 inodes
    after    15.765 TiB   630,882 inodes
    freed     2.034 TiB   169,214 inodes

Against predictions of 2.03 TiB and ~174,000 inodes. Bytes are at 84.64% of the limit, down from
the 95.58% peak. 369,110 inodes free against E4's ~161,800 need, so T38 is satisfied with 2.3x
margin. **T16 stays held — restarting E4 is Tony's alone.** Told `msm E4 GEM` by message and
`engine/bench-E2` by scope post `a4e8892e-76c4-4228-b3e3-fd1038046ce0`, since E2's session had
exited and the quota is its launch signal.

**The archive.** `/Workspace_backups/Tony_Liu/fir_bench_e3/` on chinook
(`2602486c-1e0f-47a0-be15-eec1b0ff0f96`), a sibling of `altair/`. 141,702 entries,
3,777,095,098,371 bytes. Archive task `c83cf9a3-b6b9-11f1-8511-0effcb3df825` SUCCEEDED. Source
manifest, destination manifest, the named cache census, the step/transform map and the 78,296-row
probe dependency list are all committed under
`research/metasmith_benchmark/results/e3/archive_manifests/`.

**Why the archive is believed to be resumable**, which is a stronger claim than "the transfer
succeeded":

- `invocation.probe` from the engine's own code, run inside the 0.22.1 container against all
  11,326 keys, returns **11,083 hits of 11,083 lineage entries**. The 243 misses are the imported
  givens, which is correct: probe looks under `shard_dir` while imported rows live under
  `imported/` and resolve through the pool.
- All **78,296** files those hits depend on are at the destination — 78,229 as files, and 67 as
  **empty directories** confirmed individually.
- The archived `cache.sqlite` was fetched back to fir and read with `?immutable=1`: 11,326 / 11,083
  / 243 / 0, the pre-archive fingerprint. The index reads, not merely survives.
- Path sets and sizes diff exactly outside `task_cache`: 277 directories, 2,756 files,
  1,575,120,046,427 bytes, 0 missing, 0 extra, 0 size mismatches. Inside `task_cache`, all
  108,004 shard files were counted from Globus's own `--successful-transfers` record.

**CAUTION Do not judge a Globus task by `Faults`.** It is a cumulative retry count and `Details`
sticks on the last one forever. The archive task recovered a `CONNECTION_RESET` in 31 s and still
reports `Faults: 1`. Worse, **a running task reports `Subtasks Failed 0` and `Bytes Transferred 0`,
which is indistinguishable from a passing one** — only `Status` separates them. Judge by `Status`
and `Subtasks Failed`.

**Still open, none of it this run's:** T16 (held, Tony's), T24, T28 and T29 (need Tony), T39
(`e4_chain`'s wall renewal fires only between chunks). E3's route to 39/39, if it ever matters, is
chunking the contig FASTA and affi table at k=16 plus staging VOGDB — and this archive is what
makes that a resume rather than a restart.

### Run log

**2026-09-23 01:00 PDT. The deletion landed, the prediction held, and the run is closed.**
Job `61039924` COMPLETED 0:0 in 29:03. `/scratch/phyberos/pratama2026/metasmith` is gone;
all five corpus directories beside it were verified present afterwards. Quota went
**17.799 → 15.765 TiB (2.034 TiB freed) and 800,096 → 630,882 inodes (169,214 freed)**, against
predictions of 2.03 TiB and ~174,000. The job's own pre-delete `du` read 2,236,401,639,787 bytes
against my computed 2,236,401,413,806 — 226 KB apart, from files touched between the two
measurements. A9 needed no separate work: the sbatch measures the reclaim itself, on both sides of
the `rm`, which is the right place for it.

The last thing worth recording is a **method note about what verification bought**. Globus
reported the archive SUCCEEDED early on 2026-09-22, and every subsequent check agreed with it. It
would be easy to read that as the checks having been unnecessary. They were not, for two reasons.
Two of them found real defects before the delete: the path diff caught that `.staging`'s 20 files
and 55,992,517,500 bytes were in the manifest but deliberately not in the transfer, which would
have failed the byte comparison falsely; and building the expected set found that
`recursive_symlinks=ignore` means none of the 3,511 symlinks exist at the destination, which would
have reported 3,511 false absences and hidden a real one among them. And the probe check answered a
different question than the transfer check does — not "did the bytes arrive" but "will the engine
find them", which is the only question the user actually asked. That is the check I would keep if I
could only keep one.

**Three of my own verify bugs are the durable lesson, and all three failed the same way — quietly.**
`list_subtree_shallow` mangled `$WORK`'s slashes into its output filename, so two listings were
written and then silently ignored by the glob that was meant to collect them, and the tool reported
all 29 top-level entries absent. Their 112,336 bytes was exactly the byte gap, which is what gave it
away. `listing()` recursed into regular files, where `globus ls -r` returns an error rather than
JSON. And `list_subtree ""` recursed the entire tree including `task_cache`, which was the real
cause of the 600 s timeout I first blamed on the endpoint. A verify tool that reports a failure is
not evidence of a failure until the tool itself is checked.

**2026-09-22 18:40 PDT tick. The deletion's own guard killed it, and it failed closed.**
Job `61021319` aborted with exit 1 after 1:33, having cleared the stamp and printed the
"nothing is writing to the home" header. Nothing was deleted. The bug is mine: the guard pipes
`squeue` into a `while` loop whose body ends in `grep -q`, so when the last job checked does not
match, the loop exits non-zero, the command substitution propagates it, and `set -e` aborts. It
passed every test because `squeue` was empty then and the loop never ran; E4's memote arrays started
and it broke. Replaced `grep -q … && echo` with an explicit `if` in both this sbatch and
`verify_e3_archive.sh`'s resync guard, and proved the fix returns exit 0 with 10 jobs queued and none
rooted in the home. **Resubmitted as `61039924`, monitor `b8c61qjs2`.**

The lesson worth keeping: a guard that has only ever been exercised in the quiet case is untested.
This one would have read as "the guard tripped" when it meant "the last unrelated job did not match".

**Both scopes signed off, and both checked for themselves.** `bench-E2` reads only
`cami/metasmith/task_cache` and `bench/`, and asked not to be waited for since its control run needs
the bytes first. `bench-E4` found 0 rows mentioning pratama in its own index, no symlinks into E3 to
depth 4, and no driver in checkout `0a1904fc` naming it -- and it had already evicted its superseded
CarveMe-CPLEX work in its **own** home (14,886 entries plus `runs/QIStKVhb`), which accounts for the
809,411 → 705,703 inode drop I could not explain. Neither would speak for the 3,042 E5-tagged
entries; that is moot, because the whole cache went to chinook, E5's shards included, so the
deletion preserves them rather than destroying them.

**2026-09-22 16:53 PDT tick. Bytes overtook inodes as the binding constraint, and it is not ours.**
`lfs quota -p 83115734 /scratch` read directly: **19,116,152,608 of 20,000,000,000 KB = 95.58%**,
843 GB left, and 809,411 of 1,000,000 inodes. Bytes rose 2.37 TB in two hours. `squeue` is empty
for this account, so none of it is this session's. The trees with fresh mtimes are
`metagem/e4_gems_work`, `metagem/refs`, `metagem/images` -- `agent:engine/bench-E4`'s rerun -- plus
`bench/e1_close` and `bench/compare_arms` from the E1/E2 scope.

This inverts the campaign's premise. The plan and T38 both treat inodes as the constraint at
784,373/1,000,000. Bytes are now the tighter one at 95.58%, and A8 relieves exactly that: deleting
`metasmith/` frees 2.24 TB, taking the project to roughly 83%. **A8 is not being brought forward on
that account.** The gate is the resync, the archive is the only copy of 2.2 TB of irreplaceable
compute, and quota pressure created by another scope is not a reason to delete it on partial
evidence. Posted the numbers to `engine/bench-E4` as a message (post
`2301b4aa-e3d5-49e1-a6ef-aa56a7c25154`) so they can see the wall coming and know what A8 will free.

**The resync is slow, not stalled.** 31,682 of 141,806 subtasks after 2.6 h: about 31,500 in the
first 1.5 h, then a crawl. That is the shape of `--sync-level checksum` reaching the large files --
it reads and checksums every byte on *both* ends, so 3.78 TB twice. Judge it by `Bytes Transferred`
staying 0, which it has. A second `CONNECTION_RESET` on a NOOP keepalive against the fir endpoint
recovered on its own with `Subtasks Retrying 0`.

**2026-09-22 12:55 PDT tick. The guard rule has no work to do, and that is the correct read.**
The cron tick's first action is to confirm a live `quota_stop` and a live `e4_chain`. Neither
exists and neither should: `squeue` is empty, so a `quota_stop` would have nothing to scancel and
the chainer's remaining range cannot launch without Tony lifting T16. Arming either now would be
a launch on fir, not guard maintenance. Re-arm before the next relaunch.

The tick's state block is stale in every particular and is superseded by this log. p37's
attempt-2 measurement came back and closed the question: it is O(N^1.96) in contigs, so no cap
reaches it and the answer is to chunk the input rather than to ask for walltime. E3 closed at
36/39. Chunk 10 was stopped by `agent:engine/bench-E4`. T39's mid-chunk wall defect is moot with
the chain gone. Bundle items 7 and 9-15 remain unanswered and unexecuted.

**A8 is narrower than the plan assumed, and that is the useful finding.** Delete only
`/scratch/phyberos/pratama2026/metasmith/`. `interleaved/`, `reads_2019/`, `reads_2022/` and
`zenodo_*` together cost about 600 inodes, so deleting them buys almost nothing while giving up the
corpus on the one filesystem that still has it. Leave them.

**Resumability is verified, with the engine's own code.** `invocation.probe` run inside the 0.22.1
container against all 11,326 keys returns **11,083 hits of 11,083 lineage entries**. The 243 misses
are the imported givens, which is correct rather than a fault: probe looks under `shard_dir` =
`1e/<key>` while imported rows live at `imported/1e/<key>` and resolve through the pool. Every one
of the **78,296** files those hits depend on is in the archive. 78,229 arrived as files; the other
**67 are empty directories**, which is why they were absent from the per-file delivery record, and
all 67 were confirmed present at the destination individually.

**File to engine/dev: a shard whose declared output is an empty directory is not portable.** probe
accepts any existing path, so Globus preserving empty directories is what saved these 67. tar
without directory entries, rsync in some modes, zip and object storage would drop them, those shards
would miss, and the tasks would recompute with nothing raised.

**Staged deletion does not work, and the reclaim numbers were wrong twice.** Measured per inode:

    stage 1, nxf_work + results     30,651 inodes      38,119,391,775 B   0.035 TiB
      of which held by task_cache   49,632 inodes   1,821,931,575,874 B   1.66 TiB  NOT freed
    whole metasmith/ tree          130,564 files    2,236,401,413,806 B   2.03 TiB

An inode's bytes go only when the last link goes, and 49,396 of nxf_work's 50,054 multi-link files
also have a task_cache link. So deleting the two "safe" subtrees first releases 35 GB, not 2.24 TiB,
and there is no cheap partial win. My intermediate figure of 4.20 TiB was worse: it summed the du
totals of nxf_work, results and task_cache as if they were disjoint. The correct reclaim is
**2,236,401,413,806 bytes = 2.03 TiB = 2.24 TB decimal, and about 174,000 quota inodes** -- so the
plan's original 2.24 TB was right in decimal units all along, and `lfs find`'s 233,032 counts
dentries rather than inodes.

**E3 delivered 36 of 39 steps, not 38.** 38 were submitted, `p37` and `p38` failed and were
ignored, `p39` never ran. The earlier "38/39" counted submissions.

**E4 chunk 10 was stopped by `agent:engine/bench-E4`, not by this session.** Its USR1 landed at
11:45:46, 40 s before mine to E3, and that agent posted a scope message saying so. It also says
every previous E4 CarveMe-CPLEX result under `/scratch/phyberos/metagem/metasmith` is superseded
by a rerun from metaGEM's published proteins, and it owns that reclaim. So T42 is not this
session's to do, and E4's tree is not this session's to touch.

**Both `quota_stop` guards stand down on their own now** that 60837426 and 60924835 are gone
("all targets are gone, standing down"). Nothing is running, so nothing needs guarding. Re-arm
before anything relaunches.

**The adversarial review (A5b) found four real defects. Triage, all verified before acting:**

1. *Verified.* Globus runs with `recursive_symlinks=ignore`, so **none** of the 3,511 symlinks are
   written. The verify built its expected set including them and would have reported 3,511 false
   absences, hiding a real one. Fixed by subtracting them. No data lost: 3,510 pointed into the
   excluded `nxf_work/` and their content is in `meta/step_logs_Qt0rbV1R.tar.gz`.
2. *Verified.* `metasmith/relay/` is 25 sockets **and `msm_relay`**, a 1,799,672-byte executable
   `SETUP_COMMANDS` starts before every run. Archived in place by task `3fe6a467` (SUCCEEDED).
3. *Verified.* **No container image was in the archive at all.** `APPTAINER_CACHEDIR` resolves the
   `0.22.1` tag to `/scratch/phyberos/cache/apptainer/`, outside the home. The 978 MB SIF is now at
   `deps/`, same task. The engine inside reports `0.23.0`, so the tag and the version disagree.
4. *Verified.* `genomad_pratama` and `vibrant_pratama` are **854** shards each, not 847 -- the
   census is grouped by transform *and run* and one row of each was read. Replaced the hand-copied
   table with `meta/cache_census_named.tsv`.
5. *Rejected.* Cutting the 1.57 TB re-downloadable read corpus to beat the deadline. The transfer
   sustains 240 MB/s against 21 h, and A8 does not delete the corpus anyway.

**The cache is a shared store; E3's own run contributed 132 shards.** By `run` tag: 4,398 untagged,
2,284 `JtWdzRCY`, 1,581 `Son2YJiI`, 1,494 `bqyYO0Ip`, 758 `AvPNgFtP`, 380 `F3KJbPJK`, 299
`qcMKf68s`, 132 `Qt0rbV1R`. Reconciles with the finalizer's 132 promoted / 3,510 served.

**The byte target for A5 was wrong, and it would have failed verification falsely.**
`manifest_full.tsv.gz` lists the whole home minus `nxf_work/`, `results/` and `relay/` -- but it
still lists `.staging`, which the transfer deliberately skips. So the manifest's 145,220 entries
and 3,832,855,343,605 bytes are not the archive. The archive is **145,199 entries and
3,776,862,826,105 bytes**: 110,760 files, 30,928 directories, 3,511 symlinks. `.staging` is 20
files and 55,992,517,500 bytes. `restore_e3.sh` and `RESUME.md` are corrected, and
`drivers/verify_e3_archive.sh` subtracts `.staging` when it builds the expected set.

**A5 runs as `drivers/verify_e3_archive.sh`**, four subcommands: `listing` caches a recursive
`globus ls` per top-level subtree so an interrupted listing resumes, `diff` compares path sets and
sizes against the manifest, `cache-fetch` pulls the archived `cache.sqlite` back onto fir at
`/scratch/phyberos/bench/e3_verify/`, `cache-check` hashes it and re-reads its index counts.

**No shell access to chinook.** `ssh chinook` does not resolve, so `globus ls` is the only
instrument on the destination side.

**No write happened inside `task_cache`.** The finalizer's clean close at 11:47:48 had already
checkpointed and removed the WAL, so no `-wal` or `-shm` sidecar existed and every read used
`?immutable=1`. The plan's `wal_checkpoint(TRUNCATE)` step was unnecessary, not skipped.

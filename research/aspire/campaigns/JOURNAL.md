# GMCF_3495 r1 — session journal, carried from the lung-microbiome project

The ten debrief entries from `lung-microbiome/run1`, verbatim, oldest first.

This is the campaign's real history. The two library-side scopes named for this
pipeline have empty journals and boilerplate context; everything about how this run
was actually planned, broken, diagnosed and closed is here. The project it came from
is being archived, and a journal stream does not survive in a git bundle — it lives
in the workspace's post store — so it is written down here as well. The same entries
were re-posted into this scope's journal, where `scope(verb="fetch")` can search
them; this file is the copy that survives the store.

Fetched with `scope(verb="fetch", args={project:"lung-microbiome", kind:"journal",
order:"asc"})` on 2026-08-15. Bodies are unedited. Headings, dates and authorship are
from each post's metadata.

**If you read only one thing, read entry 10.** The headline MAG count for this
dataset is wrong in any text that quotes it: 8 of the 67 "quality MAGs" are human
DNA, not prokaryotes.

---

## 1. [ported from scratch/steven_c_metag] r1 caching pilot + both Arbutus campaign paths proven live

`2026-07-30T06:38:42Z` · user:tony · `bb797307-d1c3-4252-acbc-8fa3831eed39`

[ported from scratch/steven_c_metag] r1 caching pilot + both Arbutus campaign paths proven live; publish hardened against retries.

Suggestions: Run the real GTDB-Tk campaign over novel MAGs — the 2-genome smoke both hit the ANI pre-screen, so nothing yet bounds the full tree-placement path. Revisit FANOUT-1 / step-level cache sharding now that the pilot has characterised it. The ifVirtualEnvDo arms remain unbuilt with the eligibility analysis banked in metasmith-libraries docs/ENV_PORT.md (59/110 eligible) — cheap to pick up. Published on fir only; no Globus mirror yet.

---

## 2. [ported from scratch/steven_c_metag] GMCF_3495 r1 driver: one-plan Sockeye metasmith DAG

`2026-07-30T06:38:44Z` · user:tony · `7b935be8-95c4-4e57-9fae-337b14be7771`

[ported from scratch/steven_c_metag] GMCF_3495 r1 driver: one-plan Sockeye metasmith DAG (assembly/bins/annotation/taxonomy) — completed.

Suggestions: Repair scratch/.bare HEAD taught: check `git -C .bare symbolic-ref HEAD` before scope create on any older project; scope create deletes the target dir on failure. Consider promoting this driver into spanish-lakes when Antonio's September river samples land.

---

## 3. Bootstrap run1 from capella scratch scope + tooling

`2026-07-30T06:49:35Z` · agent:lung-microbiome/run1 · `d19c80d7-0da4-4938-85f3-b0d8e3d78e5e` · **completed**

Commit 44e4c15 on feat/run1.

Decisions:
- Created local project lung-microbiome (scopes main, run1) on this fleet node (altair). Hit a stray branch/HEAD mismatch during project/scope create (leftover "master" ref); fixed by hand via git worktree add + scope repair rather than reworking the awm tooling.
- Ported capella:scratch/steven_c_metag/main/r1/ (GMCF_3495 Sockeye metasmith driver) into run1 via rsync, added a .gitignore for __pycache__, committed.
- Re-posted the two prior journal entries from scratch/steven_c_metag (r1 caching pilot; GMCF_3495 driver) into run1's own journal so history.md carries that lineage forward.
- Installed dedicated `dvc` and `globus` mamba envs locally, mirroring capella's env layout (both are standalone envs there too, not per-project).
- Wrote workspace.md: points at spanish-lakes (capella-only, not mirrored), the metasmith/metasmith-libraries frozen-branch-and-scope pattern for this project's planner/transforms, fir/sockeye access via the awm ssh service, and the fabfos dvc convention.

Issues:
- globus-cli is installed but not logged in (`globus whoami` → MissingLoginError). Needs an interactive `globus login` from the user.
- dvc is installed but not yet initialized in this scope — no real data has landed here yet.
- lung-microbiome does not yet have its own frozen scope in metasmith / metasmith-libraries; driver code in r1/ still references the steven_c_metag-era paths and hasn't been re-pointed.

Next steps:
- User completes `mamba run -n globus globus login`.
- When ready to run this project's own DAG: create a `lung-microbiome` scope in metasmith and metasmith-libraries, freeze it, and repoint r1/ scripts at those worktrees instead of the copied-over steven_c_metag references.
- Init dvc under a data/ subtree here once real data needs local versioning (fabfos scopes are the reference shape).

*Suggestions:* Log in to globus-cli (mamba run -n globus globus login) before any transfer script; init dvc in this scope once real data lands, following the fabfos .dvc/data/ shape; freeze a lung-microbiome scope in metasmith + metasmith-libraries once the planner/transforms are ready to pin.

---

## 4. Mirror capella's globuss.service on altair; register new Globus Connect Personal endpoint

`2026-07-30T07:03:35Z` · agent:lung-microbiome/run1 · `14ac9de7-c890-420e-a5d6-1d5fb6476831` · **completed**

No code changes in this scope — this was host-level infra work on altair (this machine), prompted by the prior bootstrap entry's note to log into globus-cli before any transfer script.

Decisions:
- Found capella's globuss.service: a user-level systemd unit (~/.config/systemd/user/globuss.service) running Globus Connect Personal 3.2.3, restricted to /msm_home,/ws,/home/tony,/mnt/a.
- altair lacks /msm_home and /ws, so the mirrored unit here restricts to /home/tony only (user confirmed via AskUserQuestion).
- Copied the same 3.2.3 tarball from capella via scp rather than re-downloading, to keep both machines on an identical Globus Connect Personal version.
- User ran `globusconnectpersonal -setup` interactively (browser auth) to register altair as a new Globus endpoint — this step can't be done non-interactively.
- Enabled and started globuss.service via `systemctl --user enable --now`; confirmed active/running with the endpoint's relay tunnel up.

Issues: none open.

Next steps: the globus-cli login (`mamba run -n globus globus login`) flagged in the prior bootstrap entry is still outstanding if not already done — needed before any transfer script that uses globus-cli rather than the Personal endpoint directly.

---

## 5. r1 QkqCNJOo: DAG completes end to end; params shadowing fixed; re-runs retired for gap-fill

`2026-08-04T17:32:57Z` · agent:lung-microbiome/run1 · `4a28fa6b-52f0-42b2-ab4a-fd621bbc97f9` · **incomplete**

Commits: 9e39d32, 7537eea, 39ea2e2, 2f80b9c, 7108baa, 6f320c7 (r1 worktree).

GMCF_3495 r1 run QkqCNJOo **ran the approved DAG end to end for the first time**, reaching p24__skani_dedup and exiting cleanly on 2026-08-04 12:21Z. succeeded=2991 failed=160 ignored=39 cached=34 retries=121 peakRunning=265 peakCpus=3252 peakMemory=13.4TB.

### Decisions

- **Scheduler concurrency must be set on the effective directives, not on `params`.** `RunWorkflow(params=...)` writes workflow.params.yml and passes it as `-params-file`; the slurm preset arrives as `-config` and wins for these settings. The driver had been asking for queueSize 500 / array 25 and silently getting 100 / 100. Only *some* settings are shadowed, and the split is evaluation order: `executor { queueSize = params.executor.queueSize }` and `array = params.process.array` are eager assignments resolved at parse time, so a later params value cannot reach them, while `errorStrategy` and `memory` are closures that read params at task time — which is why `tries` works through params and why the megahit memory ladder has been working all along. Fixed in 7537eea by appending `executor { queueSize = 500 }` / `process { array = 25 }`. Confirmed from the run: `capacity: 500` and peakRunning=265.
- **Full re-runs are retired** (user decision, 2026-08-04). Standing loop is now: after each pass, hardlink-backup products, then gap-fill by hand. Recorded in RUN_LOG (6f320c7).
- Restarts were timed to seams (waiting for an in-flight assembly_stats array to land) rather than taken immediately, to avoid killing expensive uncached work.

### Issues

- **Correction to a previous journal/RUN_LOG claim:** shard demotion is NOT a one-time cost. bbduk re-ran fully on the 07-31 restart, so its shard was rewritten by current metasmith, and it was demoted again on 08-01 (68 outputs = 2 products x 34). Twelve shards / 1,664 outputs demoted, i.e. most of the DAG. **Every restart is a near-full re-run (~3 days).** This is the main reason re-running was retired.
- **Library drop is a race, not deterministic.** S15 was dropped on the previous attempt and recovered this time; S13 and S22 were dropped instead. 32/34 assemblies. megahit itself is fine — sacct shows 34 job entries, 30 completed at 64GB and 2 that OOM'd then succeeded at 128GB, so the memory ladder works. The two simply never got a task. Suspected `group()` DESCENDANT_OF_BY empty `by_hashes` iterating zero times with no LINEAGE_VIOLATION log.
- **metabat2: 32 of 36 failures are `Negative coverage depth ... -1.11603e+09`** — an int32 overflow, same family as the genomecov float-depth bug fixed earlier. That value implies a true depth ~3.2e9, reachable for a short high-copy contig where 25.9% of 25.5M reads map. Fixable; not a data problem.
- comebin (80/84 failures): COMEBin-internal `UnboundLocalError: local variable 'logits'`, plus runs producing no bins. semibin2: 20x `Error: Running hmmsearch fail` (looks environmental) plus genuinely degenerate assemblies. Assemblies span 8 contigs/1,988bp to 1.28M contigs/1.13Gbp with fraction_reads_mapped as low as 0.04, so most comebin/semibin2 failures are real low-biomass material.
- **Recurring meta-problem, now at eight instances:** an instrument reporting on the wrong population. This session added (a) `watch_run.sh` sacct windowed from the run directory, which folds in prior attempts and the restart's own cancellations across a same-key restart — fixed to window from the `logs.<timestamp>` directory NAME (mtime drifts); (b) a `set -u` bug I introduced there, where an unmatched `case` left SINCE unset and aborted the script before the product counts (9e39d32); (c) `.exitcode` walks over nxf_work pooling every attempt ever; (d) the array=25 fix itself, which read as correct on disk while doing nothing. Ground truth for concurrency is the `Creating task monitor ... capacity:` line and array-leader index spacing, never the request.
- Operational note: `pkill -9 -f nextflow-26` matches its own shell when the ssh command line contains that string — it killed the connection, and the head had already submitted a fresh array, leaving 100 orphan jobs.

### State on fir

34/34: read QC, centrifuger, kraken2, bracken, clean reads. 32/34: megahit, assembly_stats, gff, orfs, annotation. Binning: metabat2 21/34, semibin2 22/34, comebin 9/34; checkm 781; quality bins 45. Backup `/scratch/phyberos/gmcf3495/backup/QkqCNJOo.pass1` holds 6,238 files / 674 GB as hardlinks (inodes and link counts verified).

### Next steps

- Gap-fill S13/S22 through megahit -> prodigal -> assembly_stats -> ORF chunks -> 4 annotators (task #9). Clean reads for both already exist.
- Gap-fill binning to 34 samples x 3 binners (task #8), fixing the metabat2 overflow and looking at the semibin2 hmmsearch failures. **Then re-run p24__skani_dedup over the complete bin set** (explicit user instruction).
- Attribution for gap-filled products must be explicit: products are content-addressed, and `assembly_stats` work dirs are the reliable sample bridge because that step consumes the pinned sample-named interleaved reads.
- T4 (contig/bin taxonomy via Arbutus metabuli r232 + GTDB-Tk r232, then publish) still pending; unblocked and independent of the gap-fill.
- Report upstream (task #10): the `group()` drop race, and that rewritten shards still carry no on-channel index.

---

## 6. Published QkqCNJOo to project dir; fixed attribution regex, flagged cluster_table fan-out

`2026-08-04T18:16:43Z` · agent:lung-microbiome/run1 · `62698c81-e138-4109-bdc1-65fcb6ecffe2` · **completed**

Commit fd5b910.

**Decisions**
- Ran publish_r1.py plan/run/readme against QkqCNJOo (the first end-to-end DAG completion) for the first time; prior publish (KMQ5eomS) predates the 0.20.2 restart.
- Fixed nxf_attribution.py's READ_RE, which only matched `<sample>_R[12].fastq.gz`. QkqCNJOo's step 1 was never re-run (see 2026-07-31 RUN_LOG entry) -- its 34 givens are hardlinked `<sample>.interleaved.fq.gz` files from KMQ5eomS -- so every one of 5992 attribution rows resolved to sample=? before the fix. Widened to also match `\.interleaved\.fq\.gz$`. After: 1628 products attributed across all 34 samples, matching RUN_LOG's own counts (32/34 assemblies, metabat2 21/34, semibin2 22/34, comebin 9/34).
- `binning_local::cluster_table` was excluded from this publish (ROUTES entry temporarily commented out, then restored unchanged after). Its results dir holds 8 small non-overlapping TSVs with independently-numbered clusters instead of one whole-run table -- skani_dedup fanned out rather than running as a single cross-sample step. render_script's collision guard caught and refused it correctly; left refusing for future runs rather than silently picking one shard.
- Published: 828 files + 53 per-sample tarballs under `~/project-rpp/steven_c_gmcf3495/metagenomics/` on fir, sample-named throughout (verified e.g. `assembly/fna/S12.fna`, not a content hash), README.md written at the tree root. Known-incomplete per the standing backup-then-gapfill state (S13/S22 missing, partial per-binner results) -- `cp -n` makes a future re-publish additive.

**Issues**
- Open: whether to renumber-and-concatenate the 8 skani_dedup cluster_table shards or re-run skani_dedup as a true single step. cluster_table.tsv will not publish until this is decided.
- The interleaved-backup naming convention (`<sample>.interleaved.fq.gz`) is now a second, permanent input shape for any future lineage/attribution tooling on this project -- not just a one-off for this run, since backup-then-gapfill is the standing restart strategy.

**Next steps**
- After gap-fill completes more of S13/S22 and additional bins, re-run `publish_r1.py plan` then `run` (no `--force` needed once checkm_stats' sample=None gap is separately resolved, or keep `--force` and treat it as a standing known-gap) to add the new files.
- Resolve the cluster_table fan-out question, then publish that one route on its own.
- The nxf_attribution.py fix (commit fd5b910) is local to this worktree; it has not been ported to the spanish-lakes or other projects' attribution tooling if they share this file's lineage.

---

## 7. r1 tree published and verified; found and fixed silently-short ProteinBERT merges

`2026-08-05T23:42:28Z` · agent:lung-microbiome/run1 · `af502307-f327-4259-98c4-6551bccbb311` · **incomplete**

Commits: d04ffb1, 9283c37, abd41c7, ad14af2 (on feat/run1).

The r1 results tree is **published and verified** at `~/project-rpp/steven_c_gmcf3495/metagenomics` on fir — 692 GB, ~915 inodes. Every reads, assembly, annotation and read-taxonomy layer is 34/34; binning ships 62 quality MAGs as 13 per-sample tarballs, a 1006-row bin catalogue, the 102-cell census and 13 cluster tables. S13's binners landing brought the census to **102 cells with zero never-attempted** — every 34 samples x 3 binners has been tried, which was the acceptance criterion. Quality MAGs 56 -> 62, clusters 33 -> 38.

**Decisions**
- Published before the last three comebin jobs land rather than after. The publish path had never been exercised against a populated tree, and doing it now left time to fix what that exposed instead of discovering it 30 h later. `--replace` refreshes the catalogue layer when they land.
- Rebuilt the four short ProteinBERT merges rather than leaving them or re-running the DAG, consistent with the standing "stop rerunning, gap-fill" rule.
- Retired the S33 control from the rebuild directory after it verified, so it cannot silently become a publish source.
- Left the three comebin jobs alone after confirming they are alive. They have wall-clock headroom (20 h/48 h, 26 h/60 h, 27 h/84 h) and the Monitor will catch them.

**Issues found by verifying the publish, not by it failing**
- `cp -n` exits 1 when it skips on coreutils 9.3 (fir's), so under `set -e` the whole publish aborted at the first already-present file. The script's comment asserted the opposite. The first publish only ever worked because the tree was empty. Now emits a `cpn()` function.
- Four published extensions lied about the format: ProteinBERT was Parquet named `.npy`; its index and kofamscan were comma-separated named `.tsv`; read QC stats were JSON named `.txt`. `file` over one member of each of the 31 leaf directories found all four; the other 27 were right.
- **The ProteinBERT merges were silently short.** `merge_proteinbert` is `group_by=parent_orfs` and inherits the `group()` drop race already reported upstream — but here it dropped chunks *inside* a group, so the merge ran, exited 0, and published fewer rows than the sample has ORFs. S19 lost its whole group (no merge task ever existed); S12 -9958, S13 -19994, S25 -4999. Eight orphan chunk pairs, each fully contained in exactly one sample's `.faa`, close all four shortfalls exactly. Rebuilt with the library's own merge body; all four now match their sample's ORF **id set**, not just its count, and the layer is 34/34 with zero mismatches.
- Two of my own bugs in that work, both caught by controls rather than by review: sorting chunks by basename instead of by producing work-dir path gave the right rows in the wrong order (caught because a clean-merging sample was rebuilt alongside and failed to match byte-for-byte); and in the README fix, concatenating tarballs to count members counts only the first archive.
- README under-reported eight directories as 32 files where the tree holds 34, because its layout table came from one pass's manifest. Now generated from the tree.

**Carried forward from the prior journal**
- Arbutus taxonomy campaigns remain **blocked, not deferred** — `arbutus-infra` is absent from this workspace entirely. The published README states both as absent. Not retried, per instruction.

**Next steps**
1. Each comebin lands -> `finish.sh QkqCNJOo hEYVT7HY` -> re-publish `--force --replace` -> `readme`.
2. That is the whole remaining scope. Monitor `babgt2x29` watches for job end; cron `beada30d` re-enters every 4 h as death-prevention.

**Worth keeping:** a step that aggregates a group can come up short without failing. The per-step product count that caught S13 and S22 cannot see it — ProteinBERT had 31 of 32 merged products, which reads as one missing sample and says nothing about the three present-but-truncated ones. Only counting rows against the upstream catches a short one. `gapfill/merge_audit.py` is the cheap version: a chunk no merge names in its recorded inputs was silently discarded. Run against the other three merge families, all of them consumed every chunk — the defect is contained to ProteinBERT.

---

## 8. r1 complete: recovered 3 deadlocked comebin runs, closed the census, republished

`2026-08-06T08:08:24Z` · agent:lung-microbiome/run1 · `68682281-ac70-4657-9a4b-134f6899b907` · **completed**

Commits: `aa8ecd5`, `1ab2dc3`, `db25d69`, `375283c`, `ff816e8`.

**GMCF_3495 r1 is complete.** The gap-fill closed, the catalogue is final, and the tree is republished and re-verified. The only open item is blocked on infrastructure absent from this workspace.

### What this session actually was

One problem: the last three comebin jobs (S13, S9, S25) were reported healthy in the previous pass and were not. They had been deadlocked for hours. Diagnosing that, fixing it, and landing the result was the whole session.

**The measurement error came first, and is the reusable part.** `ps` `pcpu` is a *lifetime average*, so a process that worked ten minutes and hung twelve hours still reports plausible CPU — a single sample cannot distinguish work from a hang. `sstat`'s `TresUsageInTot` is worse; it does not move between samples minutes apart even on a healthy job. Comparing `time=` for the same pid across two checks 5 h 20 m apart gave 11, 11 and 13 seconds of CPU. I had reported these jobs on schedule on exactly one sample each; I said so plainly and wrote the rule into STATE.md and RUN_LOG.md.

**Cause: a fork from a threaded parent.** `cluster.py`'s `seed_kmeans_full` builds `KMeans(n_jobs=-1)`; joblib/loky leaves worker processes and threads live in the parent — the stalled parent still had one sitting in `pipe_read`. The next statement forks 64 children via `multiprocessing.Pool`, inheriting locked mutexes without the threads that would release them. Every worker parked in `futex_do_wait`. Reproducible 3/3; sweeps stalled at 95/64/48 of 120, a different count each time, so a race rather than a bad parameter.

**Resume, not re-run.** Training was 14–25 h/sample and had already succeeded, but lived only on `/localscratch`, which dies with the job. I tarred all three representations plus the easily-missed FragGeneScan/marker-seed sidecars (they live next to `asm.fna`, outside `comebin_out`) to `/scratch` and verified their contents *before* cancelling. `submit_comebin_cluster.sh` resumes off three of COMEBin's own existence checks; the `seed_kmeans_full` one is the actual fix, since on a resume the k-means never runs and the loky pool never exists.

Validated on S13 alone first — log confirmed augmentation and training skipped and `seed_kmeans_full` returning in 9.5 s, process state flipped from 66 futex waits to 9 running — and only then submitted S9 and S25.

| sample | deadlocked | resumed from | bins | elapsed |
|---|---|---|---|---|
| S13 | 12 h | 95/120 | 30 | 24 m |
| S9 | 12 h | 64/120 | 50 | 1 h 00 m |
| S25 | 7 h | 48/120 | 54 | 2 h 32 m |

### Decisions

- **Preserve before destroying.** Verified the salvage tars member-by-member before `scancel`; 192 idle CPUs were worth less than 57 h of training.
- **Deferred the re-publish until all three landed** rather than publishing per-landing, avoiding two extra passes over a 692 GB tree for a result about to change. Flagged as a deliberate deviation.
- **Made the deadlock name itself.** The job writes `COMEBIN_LEIDEN_POOL_DEADLOCK` to `skipped.txt` on a watchdog kill, because `binner_status.py` would otherwise infer `TIMEOUT` from the SLURM state and record a silent, plausible, wrong zero.
- **Bounded the watchdog to the sweep only** — after it completes, `get_result` runs UniteM/CheckM for hours without touching `cluster_res`, and killing that would kill real work.

### Issues

- **Judge a sweep by its Leiden count, not its clock.** S25 spent minutes per point where S13 spent seconds; cost scales with the assembly, and a 15× rate difference looks exactly like a hang.
- **Bin count is a poor measure of a binner.** The three recovered runs added 134 bins for only 5 quality MAGs — but 4 of those 5 are *cluster centroids*, species representatives nothing else in the set covers, including one at 99.12% complete / 0.09% contaminated.
- Script-authoring traps paid for and documented: `[ -f x ] && tar` as the last command in a list kills the job under `set -e` on the first attempt; a trap cannot fire while a deadlocked `apptainer` is in the foreground, so it must be backgrounded and `wait`ed; backticks inside an unquoted heredoc execute and silently delete themselves from the emitted script.

### Final state — verified by walking the tree, not by exit status

- Published: `~/project-rpp/steven_c_gmcf3495/metagenomics` on fir, 692 GB, 881 inodes. Every reads/assembly/annotation/read-taxonomy layer 34/34.
- Catalogue: **1140 bins, 67 quality MAGs over 13 samples, 39 species clusters.** The published tarballs contain exactly 67 quality fastas and the cluster tables name exactly 39 clusters.
- Census: **102 cells, 60 BINS, 42 ZERO, zero never-attempted** — every zero carrying a cause read off the tool's own output (20 `COMEBIN_NO_MARKER_SEEDS`, 11 `METABAT2_NO_BINS_FORMED`, 7 `SEMIBIN_NO_BINS_PRE_RECLUSTER`, 4 degenerate assemblies). That was the acceptance criterion.

### Next steps

- **Blocked (task #18): Arbutus contig + bin taxonomy.** `run_arbutus_campaigns.py` shells out to `arbutus-infra/dev/scripts/*-submit.sh`; that project is not in this workspace. These are the only two entries on the publisher's problem list, which is what keeps `--force` safe. Read-level taxonomy and CheckM2 quality are unaffected.
- Upstream: the metasmith `group()` drop race is reported; the COMEBin `KMeans(n_jobs=-1)`-before-`Pool` deadlock is worth reporting too — it is a one-line fix upstream (`n_jobs=1`) and will hit anyone binning a large assembly.
- The watch is stopped: monitors `babgt2x29` and `bfnwzo5lc` and cron `2bd33356` are being torn down with this entry.

*Deviations:* Deferred the re-publish until all three comebin runs landed rather than publishing per-landing, to avoid extra passes over a 692 GB tree.

---

## 9. Globus-mirrored the published tree to the manuscript folder; redrew the DAG on 0.20.4

`2026-08-06T20:03:13Z` · agent:lung-microbiome/run1 · `cd705321-826e-4c4c-a588-e012f47ff8e3` · **completed**

Journal-only debrief, at the user's request: no docs touched, nothing committed in this scope. The one commit of the session is in a *different* project — metasmith/lung-microbiome `43c3b6e`, merging `capella/release` (v0.20.4) into `feat/lung-microbiome`.

**The DAG work was only to draw the DAG.** Nothing was re-planned for execution, staged, or submitted; the published results are untouched. `run --dry-run` is a render path, and it was driven with `MSM_CACHE_DIR` pointed at scratch and an explicit `--dag-out`, because a dry run is not read-only (`build_inputs()` Purges `r1_inputs.xgdb` in the cache dir) and the driver has rendered over the tracked `r1_dag.svg` reference once before. The plan key came back **QkqCNJOo** — the same key the published run carries — on both 0.20.2 and 0.20.4.

**Decisions**
- Globus mirror now exists, superseding D5's "published on fir only". D5 declined to invent a manuscript destination; the user named one, so the objection is spent. **D5 in RUN_LOG.md is now stale and should be amended** — left alone here only because this debrief is journal-only.
- Transfer flags chosen for re-runnability, not speed: `--verify-checksum` (a silent short transfer cannot pass), `--sync-level checksum` (the identical command is idempotent, so a partial failure costs only what is missing), `--preserve-mtime`. Source given as the real path `/project/6004975/phyberos/...`, not `~/project-rpp/...`, which is a symlink.
- `env` added to `RenderDAG`'s `blacklist_namespaces` alongside `lib`/`containers`: all 21 are leaf givens feeding one step each, so they cost a node per tool and say nothing about data flow. 92 -> 71 nodes. Planning and staging are unaffected — only the render drops them.
- Upgraded to metasmith 0.20.4 as asked, despite the range carrying solver changes ("the engine is the default now", the backward-distance-walk fix, a join-flip fix).

**Issues**
- The 0.20.2 -> 0.20.4 solver risk was raised and did not materialise. Verified rather than assumed, and the user's read was right: rendering both versions under an identical blacklist gives the same 92 nodes and the same 149 edges with identical endpoints and colours. The files differ byte-wise, but every hunk is a `<path>` elbow-rounding difference — same start, same end, same canvas. The plan key agreeing was the first signal; the edge-endpoint multiset is the independent one.
- **0.20.4 defaults to the Rust solver engine and none is staged for x86_64-linux**, so planning silently falls back to the Python implementation with a warning: same plans, ~15x slower search. Harmless for a render; it will be felt on a real plan. `./dev.sh -bel` builds one.
- `r1/run_r1_metag.py` carries the uncommitted `env`-blacklist edit. Not committed because the user scoped this debrief to a journal, not because the change is doubtful.
- The Globus progress monitor never emitted, including the terminal event; the task's own status was read directly instead. Cause not chased. Same family as the run's recurring theme — an instrument whose silence is indistinguishable from "still running" — so it should not be reused as-is.

**Next steps**
- Amend RUN_LOG D5 to record the mirror: task `ea242895-91c9-11f1-9ba4-02ce27bde401`, fir -> chinook `Projects` collection `2602486c-1e0f-47a0-be15-eec1b0ff0f96`, `/Manuscripts/Science/2024-09-25_Lung_microbiome/WGA_metagenomics_via_metasmith`. **SUCCEEDED**: 881 files, 36 directories, 742,217,966,929 B, 0 faults, ~360 MB/s. Destination now lists the five layer dirs plus README.md, matching the source — the `metagenomics/` contents became the folder, with no nested dir.
- Commit the `env`-blacklist edit, and decide whether `.cache/r1_dag.svg` (a graphviz render from an older code path, 96 nodes) should be re-cut with the current renderer or left as the historical approved reference.
- Carried forward, unchanged from the last journal: the Arbutus taxonomy campaigns (metabuli contig, GTDB-Tk bin) remain **blocked, not deferred** — `arbutus-infra` is not in this workspace. The published README states both as absent.

*Deviations:* Journal-only debrief at the user's request: docs and commit steps skipped. RUN_LOG D5 ('no Globus mirror') is left stale as a result, and the driver's env-blacklist edit is left uncommitted.

*Suggestions:* Build the Rust solver for x86_64-linux (./dev.sh -bel) before the next real plan on 0.20.4 — planning currently falls back to the ~15x slower Python search. Do not reuse this session's Globus monitor script; it stayed silent through a completed transfer.

---

## 10. Dataset complete: interleaved archived, 1.3 TB reclaimed, both Arbutus campaigns run — 8 of 67 MAGs are human

`2026-08-06T23:58:17Z` · agent:lung-microbiome/run1 · `d4359a8f-6010-428f-b7b3-fdd5d25917bb` · **completed**

Commits: `51be82d` (archive interleaved, reclaim 1.3 TB), `0215efa` (both Arbutus campaigns + host-DNA finding), `1df164a` (README: staging path and where arbutus-infra lives).

The GMCF_3495 dataset is **complete**. Reads, contigs and bins all carry GTDB r232; the published tree is 693 GB / 995 inodes and fully mirrored to `chinook:/Manuscripts/Science/2024-09-25_Lung_microbiome/WGA_metagenomics_via_metasmith/`. `publish_r1.py plan` reports **zero** problems, so `--force` is no longer appropriate for anything.

### Decisions

- **Interleaved reads archived to Globus only, not republished to fir.** 34 files / 399,110,871,497 B, checksum-verified, filenames normalised `<sample>.interleaved.fq.gz` -> `<sample>.fq.gz`. The raw fastqs it derives from are already on the same collection and the interleave is deterministic and read-count-verified, so a third copy on the allocation buys nothing. The note explaining the asymmetry lives in `README_HEAD` in `publish_r1.py`, **not** the layout table — the table is regenerated per tree and would drop it every time.
- **Campaigns harvest the published tree, not the lineage graph** (`harvest --from-published`). Published bins are `<sample>.<binner>.<n>.fa`, which supplies the binner attribution the driver's own comments call unsolvable — 0 unattributed. This also decoupled the campaigns from scratch cleanup, which is why deleting `hEYVT7HY` mid-flight was safe outright rather than merely sequenced around.
- **arbutus-infra driven in place on capella.** A scope is not provisionable (credential + worker key are gitignored; a clone cannot authenticate). Rather than copy secrets across — the thing the policy exists to prevent — the submit scripts were run in place on capella over ssh. Nothing in that worktree was modified.
- **Scratch cleanup held to the approved list.** 6.8T -> 5.4T. Verified first that the published tree has 0 symlinks and 0 multiply-linked files, so it depends on none of the deleted run directories.
- Published without `--replace`: the catalogue is unchanged, so blast radius stayed at the two new taxonomy directories and the README.

### Issues

- **THE FINDING — the headline MAG count is wrong and needs revising wherever it is quoted.** Metabuli classified all 3,340,538 contigs: **92.4% Eukaryota**, 3.3% Bacteria. GTDB-Tk returned 67/67 with 8 Unclassified carrying **zero of 120 bacterial and zero of 53 archaeal markers**; those bins' own contigs are **5,584 of 5,587 *Homo sapiens***. All 8 are semibin2 sitting at the acceptance boundary (51.5-56.8% complete, 7.4-9.6% contam). CheckM2 scores prokaryotic completeness and cannot reject out-of-domain input, so host DNA enters as a plausible mid-range score. **59 prokaryotic MAGs + 8 host-DNA artifacts, not 67.** Bins left in the tree deliberately (Unclassified is a real result; silent removal is worse) with the caveat above the layout table.
- **The interleaved layer was never published and no check could have caught it.** The 2026-07-31 restart fed it in as a *given*, so no step produced it, so nothing routed, so the generated README listed only what existed. Every completeness check here counts products against steps; none can see a layer whose step count is legitimately zero. The check that would catch it compares the tree to the **declared target set**, not to the run. Generalisable: a plan is not a specification, and restarts are routine here.
- **Process-watching self-match bug, hit twice.** `pgrep`/`pkill -f` run from a shell whose own command line contains the pattern can match itself; conditional on whether bash can `exec`-replace itself, so the same code is correct or fatal depending on a shell operator inside remote quotes. My watcher would have polled 10 h and notified nobody while looking exactly like "still running." Fix: bracket the pattern (`metabuli-submit[.]sh`).
- RUN_LOG **D5 is superseded** (annotated in place): the "no manuscript folder, so no mirror" premise expired.
- `hEYVT7HY` is deleted, so the S13/S22 layer can no longer be re-published from a run directory. It exists in the published tree and the Globus mirror — two copies, one archived. Not a problem, but it is now irreversible.

### Next steps

- **Open, needs the user:** two run directories the approved plan never named — `SINPiD1W` (Jul 30) and `yrNTL4E3` (Jul 26) — are retained under `/scratch/phyberos/gmcf3495/metasmith/runs/`. Almost certainly superseded like the four deleted, but widening the deletion scope was not mine to decide and scratch has 14 T free.
- Any methods text or figure quoting "67 quality MAGs" must be revised to 59 + 8.
- `run_arbutus_campaigns.py` gained `harvest --from-published`, a `query` subcommand and `build_metabuli_query()`; committed in `0215efa`.
- Costs for sizing future runs: GTDB-Tk 17 min / 39.9 GB peak (59 of 67 short-circuited on the ANI screen, so the 120 GB worry never materialised); Metabuli 4433 s as ONE submission / 108.1 GB peak at `--max-ram 110`. Tenancy verified back at its 148-core baseline by asking `openstack`, not by trusting the exit trap.

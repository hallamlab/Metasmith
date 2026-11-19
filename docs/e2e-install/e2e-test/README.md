# e2e-test

> whats needed is an agentic E2E test that is siloed from additional context (maybe haiku?) that just isntalls and runs metasmith on sockeye on 1 sample E2E. reporting issues and guesses until there are none.

## Purpose & Contents

This is the plan for the first-time-install test and its fix loop. It covers the child topics `watch-list` and `scratch-blowup`. Settled inputs live in the sibling topic folders: `test-env/`, `software-suite/` and `ws-bloat/`.

## Context

Metasmith must install and run from its docs alone, for a user with little technical skill. A naive, context-free agent plays that user. It installs metasmith into a clean env on altair and runs one sample through the metagenomics template on nibi. This session supervises: it watches the nibi filesystem and the transcript, triages what the agent reports, fixes metasmith, and reruns until a run comes back clean.

## What you said

> we want metasmith to be isntallable and runnable from the instructions and API without issue and minimal technical skill.

> lets try from scratch setup then -> need clean env somewhere (altair?) that installs locally and tries a run on nibi. Watch another naive agent try from only docs.

> a beta tester reports that as is, metasmith just fills up the scratch space without finishing (millions of files) so this is the first error to check for

> Please watch for things like container pulls and correct cache allocation. Should also report with a series of steps take so that we can check for any ambiguities or strangeness that was required.

> you delete after, otherwise wirks

> can you plan out the test and iteration for first time install to document errors and difficulties tto fix?

## Issues

- I1. No test runs a first-time install and a real cluster run from the docs alone. The existing agentic harness gives the agent literal commands, spoofs the conda channel and deploys only to local docker.
- I2. A beta tester's run filled scratch with millions of files without finishing, and nobody knows which directory holds them.
- I3. The harness records only a one-line `report_issue` from the agent, so the steps, guesses and workarounds a user needed are lost.
- I4. The two install docs disagree: `agentic/setup.rst` says `pip install -e .`, and `setup/install.rst` says `mamba create ... metasmith`.
- I5. Most of the 22 Rokaiya issues are still open, and nobody knows which still bite a new user.
- I6. Nothing caps a test run's footprint on a shared 1M-file, 1 TB scratch quota.

## High-level goals

- G1. A new user can install metasmith and run a real workflow on a cluster from the docs alone.
- G2. Every error, ambiguity, guess and workaround that new user hits is written down so it can be fixed.
- G3. A test run cannot flood the cluster filesystem, and it shows where the files go.
- G4. The test reruns after each round of fixes until it comes back clean.

## Acceptance criteria

- The naive agent runs as Haiku with no workspace CLAUDE.md, memory, journals, awm MCP tools or source tree. Its transcript shows only the docs copy, the sandbox and `ssh nibi`.
- Each run leaves `STEPS.md`, one entry per action: the command, the doc section it followed, the outcome, and any ambiguity, guess or workaround. It also leaves `REPORT.md`, with every issue classed as error, ambiguity, guess or workaround.
- The watcher logs files and GB per top-level subdirectory of the run dir, plus nibi `/home`, every 5 minutes. A dry test with a lowered threshold shows it stops the run and cancels the run's Slurm jobs.
- Each run's triage lands in `e2e-test/runs/run-N/ISSUES.md`. Each issue names its fix commit, or says why it stays open.
- Done: a run from a clean env completes all 31 steps of `metagenomics_from_paired_reads` on one sample and collects the results. Its report holds no error or workaround entries, and the watcher records its peak file count.
- This session deletes each run's nibi dir and altair env by exact path after reading the report.

## Tasks

- T1. Build the first-install scenario on the existing harness (G1, G2)
- T2. Build the remote watcher with caps and a per-directory breakdown (G3)
- T3. Dry-check the silo, the ssh path and the kill switch (G1, G3)
- T4. Commit a checkpoint
- T5. Compact
- T6. Run 1 against the published 0.25.1 (G1, G2, G3)
- T7. Triage run 1 and present the issue list to Tony (G2)
- T8. Compact
- T9. Fix, rerun and triage until a run comes back clean (G1, G2, G4)
- T10. Simplify
- T11. Draft the doc updates
- T12. Debrief

## Approach by task

### T1. Build the first-install scenario on the existing harness

Add one scenario under `tests/metasmith/e2e/agentic/scenarios/`. It reuses the claude driver (Haiku, stream-json transcripts, dollar cap), the loop with its `CONTROL.json` stop protocol, and the sandbox `HOME`/condarc isolation. The prompt states only the goal:

> You are a new metasmith user. Install metasmith using only the docs in ./docs. Deploy an agent to the cluster reachable as `ssh nibi` (Slurm account def-shallam_cpu), with its home under /scratch/phyberos/msm_e2e/run-N. Run the metagenomics_from_paired_reads workflow without its metabuli target on sample S. Before each action, append it to STEPS.md. After it, append the outcome. When done or stuck, write REPORT.md.

The install uses the real `hallamlab` channel, with no spoof. The docs copy comes from the branch under test, so a docs fix reaches the next run with no release. Raise the per-iteration timeout from 300 s to about 30 min, because a Slurm queue wait outlasts it.

Removes: the stub `t5_adapt_hpc` target-host field that nothing reads, if this scenario supersedes it.

Gotchas: the bwrap jail mounts the host `~/.claude` for auth, which also exposes `~/.claude/CLAUDE.md` and memory. Bind only the credentials file. ssh through the jail needs `~/.ssh/config` and the ControlMaster socket bound read-only. Do not copy keys. A Haiku loop that re-sends one prompt can repeat a failing step. `PROGRESS.md` must carry what it already tried.

### T2. Build the remote watcher with caps and a per-directory breakdown

Write one script next to the harness. It polls nibi through the ControlMaster every 5 minutes. It records per-subdirectory inode and GB counts of the run dir with `lfs find` and `du --inodes`, plus `diskusage_report` for `/home` and `/scratch`. It also notes container pulls: where `.sif` files and the apptainer cache land, and `~/.apptainer` growth in `/home`. Past 600K files or 600 GB it cancels the run's Slurm jobs, kills the driver by PID and writes a stop record. It writes to a CSV that this session follows with Monitor.

Removes: none, as the script is new.

Gotchas: a Monitor cannot ssh from the sandbox, so run the poller detached and unsandboxed and tail its log. `find | wc -l` counts names, not Lustre inodes. A recursive `lfs find` over millions of files is itself slow, so time-box each sample. Kill by PID, never `pkill -f`.

### T3. Dry-check the silo, the ssh path and the kill switch

Run the scenario with a trivial goal (`msm --version` locally, then `ssh nibi hostname`) and read the transcript for any leak of workspace context. Run the watcher against a scratch dir holding a few hundred files, with the cap set below that count. Confirm it stops and logs.

Removes: none.

Gotchas: one Duo push opens the nibi master. Open it with `ssh(verb=connect)` before the run, and check it again before each rerun, because the master expires.

### T4. Commit a checkpoint

Commit the scenario, the watcher and the dry-check notes on `feat/engine/install-loop`.

### T5. Compact

Handoff: this plan, the commit from T4, the watcher command line, and the nibi master state.

### T6. Run 1 against the published 0.25.1

Start the harness in the background. Follow the watcher CSV and the transcript. Step in only to stop the run at the cap, or when the agent is wedged and its budget is spent. Run 1 is the baseline that a real user gets today, metabuli deselection included.

Gotchas: GTDB downloads at about 1 MB/s on compute nodes. The docs say large downloads run on the login node, and the run shows whether that holds on nibi. A run left waiting in the queue is not wedged.

### T7. Triage run 1 and present the issue list to Tony

Read `STEPS.md`, `REPORT.md`, the transcript and the watcher CSV. Merge them into `runs/run-1/ISSUES.md`, one line per issue. Each line gets its class, its doc section or code path, and any matching Rokaiya issue number. Rank the issues by how far each one blocks a new user. Present the list. Delete the nibi run dir and the altair env by exact path.

### T8. Compact

Handoff: `runs/run-1/ISSUES.md`, the fix order Tony picked, and the open questions.

### T9. Fix, rerun and triage until a run comes back clean

For each round:

1. Fix the top issues in docs or code.
2. Rerun from a clean env.
3. Triage into `runs/run-N/ISSUES.md`.

Docs fixes ship through the docs copy at once. Code fixes need a build the clean install can reach. See *Still unsure about*. Stop at the first clean run, per the acceptance criteria.

Gotchas: each rerun costs real core-hours and about 190 GB of reference downloads. Weigh a narrow rerun, such as install and deploy only, when a fix touches only that stage.

### T10. Simplify

Run the `simplify` skill over the scenario, the watcher and every fix from T9.

### T11. Draft the doc updates

Correct the harness README, which names stale paths and test files. Settle I4 on a single install path. Record nibi setup facts in the user docs where a user needs them.

### T12. Debrief

Run the `debrief` skill for `metasmith/engine/install-loop`.

## Still unsure about

- Which sample: one Tony supplies, such as the beta tester's, or a small public SRA paired-end metagenome given only as an accession?
- How code fixes reach a clean install between runs: a release-candidate conda label and quay tag, or the harness's local spoof channel with the `.sif` copied to nibi?

## Callouts

- Haiku stands in for "minimal technical skill". If it fails on reasoning rather than on docs, a Sonnet run tells the two apart.
- The template change (metabuli dropped, commit 17f0232a) reaches users only at the next release.
- Run 1 downloads the reference databases inside its run dir. Whether later runs reuse them is decided after run 1.

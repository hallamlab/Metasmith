# e2e-test

> whats needed is an agentic E2E test that is siloed from additional context (maybe haiku?) that just isntalls and runs metasmith on sockeye on 1 sample E2E. reporting issues and guesses until there are none.

## Purpose & Contents

The approved plan (2026-11-19) for the first-install test and its fix loop. It covers the child topics `watch-list` and `scratch-blowup`. Run triage lands in `runs/run-N/ISSUES.md`.


## Context

Metasmith must install and run from its docs alone, for a user with little technical skill. A naive, context-free Haiku agent plays that user. It installs metasmith into a clean env on altair and runs one public sample through the metagenomics template on nibi. This session supervises: it watches the nibi filesystem and the transcript, triages what the agent reports, fixes metasmith, and reruns until a run comes back clean. The discussion that settled the inputs lives in `docs/e2e-install/` on `feat/engine/install-loop` (topics `test-env`, `software-suite`, `ws-bloat`, `e2e-test`).

## What you said

> we want metasmith to be isntallable and runnable from the instructions and API without issue and minimal technical skill.

> whats needed is an agentic E2E test that is siloed from additional context (maybe haiku?) that just isntalls and runs metasmith on sockeye on 1 sample E2E. reporting issues and guesses until there are none.

> lets try from scratch setup then -> need clean env somewhere (altair?) that installs locally and tries a run on nibi. Watch another naive agent try from only docs.

> a beta tester reports that as is, metasmith just fills up the scratch space without finishing (millions of files) so this is the first error to check for

> Please watch for things like container pulls and correct cache allocation. Should also report with a series of steps take so that we can check for any ambiguities or strangeness that was required.

> then just drop metabuli's output targets

> you delete after, otherwise wirks

> small public SRA, check cyanoverse

## Issues

- I1. No test runs a first-time install and a real cluster run from the docs alone. The existing agentic harness gives the agent literal commands, spoofs the conda channel and deploys only to local docker.
- I2. A beta tester's run filled scratch with millions of files without finishing, and nobody knows which directory holds them.
- I3. The harness keeps only a one-line `report_issue` from the agent, so the steps, guesses and workarounds a user needed are lost.
- I4. The two install docs disagree: `agentic/setup.rst` says `pip install -e .`, and `setup/install.rst` says `mamba create ... metasmith`.
- I5. Most of the 22 Rokaiya issues are still open, and nobody knows which still bite a new user.
- I6. Nothing caps a test run's footprint on nibi's shared per-user quota of 1M files and 1 TB.

## High-level goals

- G1. A new user can install metasmith and run a real workflow on a cluster from the docs alone.
- G2. Every error, ambiguity, guess and workaround that new user hits is written down so it can be fixed.
- G3. A test run cannot flood the cluster filesystem, and it shows where the files go.
- G4. The test reruns after each round of fixes until it comes back clean.

## Acceptance criteria

- The naive agent runs as Haiku with no workspace CLAUDE.md, user memory, journals, awm MCP tools or source tree. Its transcript shows only the docs copy, its sandbox and `ssh nibi`.
- Each run leaves `STEPS.md`, one entry per action: the command, the doc section it followed, the outcome, and any ambiguity, guess or workaround. It also leaves `REPORT.md`, with every issue classed as error, ambiguity, guess or workaround.
- The watcher logs inodes and GB per top-level subdirectory of the run dir, plus nibi `/home` and `/scratch` usage, every 5 minutes. A dry test with a lowered threshold shows it cancels the run's Slurm jobs and kills the driver.
- Each run's triage lands in `docs/e2e-install/e2e-test/runs/run-N/ISSUES.md`. Each issue names its fix commit, or says why it stays open.
- Done: a run from a clean env completes all 31 steps of `metagenomics_from_paired_reads` on SRR9658108 and collects the results. Its report holds no error or workaround entries, and the watcher records its peak file count.
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
- T10. Compact
- T11. Simplify
- T12. Draft the doc updates
- T13. Debrief

## Approach by task

### T1. Build the first-install scenario on the existing harness

Add one scenario under `tests/metasmith/e2e/agentic/scenarios/`. Reuse the claude driver in `drivers/claude.py` (Haiku, stream-json transcripts, `--max-budget-usd`), the loop in `harness/loop.py` with its `CONTROL.json` stop protocol (`harness/control.py`), and the sandbox `HOME`/condarc isolation in `harness/sandbox.py`. Copy the docs from the branch under test into `sandbox/docs`, so a docs fix reaches the next run with no release. The prompt states only the goal:

> You are a new metasmith user. Install metasmith using only the docs in ./docs. Deploy an agent to the cluster reachable as `ssh nibi` (Slurm account def-shallam_cpu), with its home under /scratch/phyberos/msm_e2e/run-N. Run the metagenomics_from_paired_reads workflow without its metabuli target on SRA run SRR9658108. Before each action, append it to STEPS.md. After it, append the outcome. When done or stuck, write REPORT.md.

The sample is SRR9658108. It is a 0.87 Gbp paired Illumina metagenome from a Microcystis enrichment in freshwater lakes. cyanoverse processed it into 27 bins across three binners, which gives a reference for the result. Run 1 installs from the real `hallamlab` channel. Later runs use the harness's local channel in `install_mock/` (see T9). Raise the per-iteration timeout from 300 s to about 30 min, because a Slurm queue wait outlasts it.

Removes: the `t5_adapt_hpc` stub's `target_host` field, which nothing reads, if this scenario supersedes it.

Gotchas: the bwrap jail in `harness/jail.py` mounts the host `~/.claude` for auth, which also exposes `~/.claude/CLAUDE.md` and memory. Bind only the credentials file. ssh through the jail needs `~/.ssh/config` and the ControlMaster socket bound read-only. Do not copy keys. A Haiku loop that re-sends one prompt can repeat a failing step, so `PROGRESS.md` must carry what it already tried.

### T2. Build the remote watcher with caps and a per-directory breakdown

Write one script next to the harness. It polls nibi through the awm ssh master every 5 minutes. It records per-subdirectory inode and GB counts of the run dir with `lfs find` and `du --inodes`, plus `diskusage_report` for `/home` and `/scratch`. It notes container pulls: where `.sif` files and the apptainer cache land, and any `~/.apptainer` growth in `/home`. Past 600K files or 600 GB it cancels the run's Slurm jobs, kills the driver by PID and writes a stop record. It appends to a CSV that this session follows with Monitor.

Removes: none, as the script is new.

Gotchas: a Monitor cannot ssh from the sandbox, so run the poller detached and unsandboxed and tail its log. `find | wc -l` counts names, not Lustre inodes. A recursive `lfs find` over millions of files is slow, so time-box each sample. Kill by PID, never with `pkill -f`.

### T3. Dry-check the silo, the ssh path and the kill switch

Run the scenario with a trivial goal: `msm --version` locally, then `ssh nibi hostname`. Read the transcript for any leak of workspace context. Run the watcher against a scratch dir holding a few hundred files, with the cap set below that count. Confirm that it stops and logs.

Removes: none.

Gotchas: one Duo push opens the nibi master through `ssh(verb=connect, host=nibi)`. Check that the master is up before each run, because it expires.

### T4. Commit a checkpoint

Commit the scenario, the watcher and the dry-check notes on `feat/engine/install-loop`. Push the branch.

### T5. Compact

Handoff: this plan, the T4 commit, the watcher command line, the nibi master state and the todo list.

### T6. Run 1 against the published 0.25.1

Start the harness in the background. Follow the watcher CSV and the transcript. Step in only to stop the run at the cap, or when the agent is wedged and its budget is spent. Run 1 is the baseline a real user gets today, metabuli deselection included.

Gotchas: GTDB downloads at about 1 MB/s on compute nodes. The docs say large downloads run on the login node, and this run shows whether that holds on nibi. A run waiting in the Slurm queue is not wedged.

### T7. Triage run 1 and present the issue list to Tony

Read `STEPS.md`, `REPORT.md`, the transcript and the watcher CSV. Merge them into `runs/run-1/ISSUES.md`, one line per issue. Give each line its class, its doc section or code path, and any matching Rokaiya issue number. Rank the issues by how far each one blocks a new user. Present the list. Delete the nibi run dir and the altair env by exact path.

### T8. Compact

Handoff: `runs/run-1/ISSUES.md`, the fix order Tony picked, and the open questions.

### T9. Fix, rerun and triage until a run comes back clean

For each round:

1. Fix the top issues in docs or code.
2. Build the local channel and the `.sif` with `install_mock/build_local_artifacts.sh`.
3. Copy the `.sif` into the apptainer cache path that nibi's agent reads.
4. Rerun from a clean env.
5. Triage into `runs/run-N/ISSUES.md`.

Stop at the first clean run, per the acceptance criteria.

Gotchas: runs after run 1 skip the real container pull, which run 1 covered. Rerun the published path once a release carries the fixes. Each full rerun costs real core-hours and about 190 GB of reference downloads. When a fix touches only one stage, such as install and deploy, run only that stage.

### T10. Compact

Handoff: the last clean run, its `ISSUES.md`, and the commits that fixed each issue.

### T11. Simplify

Run the `simplify` skill over the scenario, the watcher and every fix from T9.

### T12. Draft the doc updates

Correct the harness README, which names stale paths and missing test files. Settle I4 on one install path. Add nibi setup facts to the user docs where a user needs them.

### T13. Debrief

Run the `debrief` skill for `metasmith/engine/install-loop`.

## Callouts

- Haiku stands in for "minimal technical skill". If it fails on reasoning rather than on docs, a Sonnet run tells the two apart.
- The template change (metabuli dropped, commit 17f0232a) reaches conda users only at the next release.
- Run 1 downloads the reference databases inside its run dir. Whether later runs reuse them is decided after run 1.
- nibi scratch already holds 57K files from earlier work. The 600K cap leaves room for them.
## Revision after run 1 (2026-11-19)

Run 1 stopped at deploy. Triage is in `docs/e2e-install/e2e-test/runs/run-1/ISSUES.md` (commit 63f981ec). Tony's decision is quoted there. It changes T9 as follows:

- **Browser route first.** The naive agent installs metasmith, starts `msm gui`, and drives it in a browser like a person: deploy to nibi, then run `metagenomics_from_paired_reads` without metabuli on SRR9658108. Give it a browser through the Playwright MCP server; node, npx and the Playwright chromium builds already sit on altair. Keep the browser to localhost origins, and keep the silo. A Python API run follows as a comprehension test: are the calls understandable, and do they do what the docs say?
- **Longer time budget.** One hour is not enough. Agent processes, such as the GUI server and a deploy, must survive across passes, and the run stops only when it ends. Raise the pass limit as well.
- **Active supervision.** Check the transcript and the watcher about every 15 minutes. When the agent does something wasteful that a fix would remove, stop the run, fix it, and rerun. Do not let a run grind.
- **Fixes before run 2:** issue 1 and issue 6 (setup commands: unpack in node-local `/tmp`, cache on scratch, `module load apptainer`), issue 5 (metasmith sets `APPTAINER_CACHEDIR` for the pull), issue 4 (narrow the `rootfs="sandbox"` note), and H1 (process lifetime). Then issue 2 (deploy progress, orphaned remote build) and issue 3 (template docs page) as the run shows they bite through the GUI.

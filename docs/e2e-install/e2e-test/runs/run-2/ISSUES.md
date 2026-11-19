# run-2 (published 0.25.1, branch docs at 4bdd7f2a, 2026-11-19)

Goal: install from `./docs`, start `msm gui`, and drive it in a headless browser to deploy to nibi and run `metagenomics_from_paired_reads` without metabuli on SRA run SRR9658108. Haiku, one pass, 13:07 to 13:40. Stopped by the operator at 13:40, because the plan solve could never succeed (issue 1). The agent deployed, but it never ran a Slurm job.

Kept beside this file: the agent's `STEPS.md` and `PROGRESS.md`, the watcher's `watch.csv`, the agent file the GUI wrote (`agent.yml`) and the workflow request it built (`workflow_request.yml`).

## What happened

1. It installed 0.25.1 with the documented `mamba create` (step 3). No docs page names the GUI, so it found `msm gui` in `msm --help` (step 8).
2. In the GUI it made an agent for nibi with the slurm preset and `module load apptainer`, then deployed (steps 11 to 24). The deploy took 6 minutes. The image unpacked in about one minute on nibi's node-local `/localscratch`.
3. It made a workflow from the template and could not discard the template's input rows (step 34). It started again from a blank workflow, typed in the 24 outputs, and rebuilt some lineage links by hand (steps 35 to 42).
4. It found `ncbi::sra_accession` and the need for a `read_metadata` parent in the installed package, not in the docs (steps 33, 45 to 47).
5. Its solve ran 12 minutes at 100% CPU with no result (steps 48, 49).

## Footprint on nibi

- 9 files and 1.0 GB in the run dir: the deployed agent and its `.sif`. No Slurm jobs.
- `~/.apptainer` in `/home` grew to 992 MB. This is issue 5 from run 1. Its fix (commit 4bdd7f2a) reaches users at the next release. Another session on nibi pulled into the same cache after this run, so it stays. Clear it with `apptainer cache clean` once that session is idle.

## Issues, ranked by how far each blocks a new user

| # | class | where | issue | fix |
|---|---|---|---|---|
| 1 | error | planner, `getSraReads` fork | With an SRA accession as input, the solve fails once a workflow names about six targets. Every target alone, and every tested pair with the assembly, solves. The template with its file inputs solves all 25 targets in 6 s. The likely cause is that `getSraReads` is a fork (long, paired short, single short reads), and the planner solves a case per outcome even though the given `read_metadata` says paired short reads. | open, under investigation |
| 2 | error | same | The no-plan hints point at unrelated dead ends ("orfs needs an NCBI assembly accession"), not at the fork. A user has nothing to act on. | open, with 1 |
| 3 | error | GUI agent form | The param name box shows "slurmAccount" as grey placeholder text. The agent read it as filled in and typed only the value. A row with no name was dropped without a message. | 553838c3 |
| 4 | error | GUI recipe rows | Discarding a row needs a second click within 2 s. The agent's clicks came slower, so the template rows never went away, and it rebuilt the workflow by hand. | 553838c3 |
| 5 | error | GUI solve | A hard solve shows "solving…" with no progress, no time limit and no way to stop it. The agent waited 12 minutes. | open |
| 6 | ambiguity | docs tree | No docs page covers the GUI: how to start it, the agent form, recipes, parents, solve and run. | open |
| 7 | ambiguity | docs tree | No page covers the shipped templates, dropping a target, or SRA input. This is run 1's issue 3. | open |

Fixes from run 1 that this run confirmed: issue 1 and issue 6 (the setup commands; the unpack now takes one minute, not over an hour) and H1 (the agent's GUI server and deploy survived across tool calls).

## Harness issues

- H4. `--mcp-config` takes several values, so it swallowed the prompt. The first launch ran 20 empty passes in one minute. Fixed by putting a named flag after it.
- H5. The loop does not stop on passes that do no work. A pass that ends in seconds with no tool call should stop the run.
- H6. Stopping the first launch killed the watcher's `setsid` parent, not the watcher, so two watchers sampled until 13:24. Start the watcher inside the run's systemd scope, so stopping the scope stops it.
- H7. With the host pid namespace shared (H1's fix), the agent can read other processes' command lines in `/proc`. Nothing in this run read them.

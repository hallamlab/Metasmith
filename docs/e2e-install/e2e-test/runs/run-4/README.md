# Run 4

## Purpose & Contents

How to launch and stop run 4 of the first-install test: two stations running at once, one naive Haiku agent each. Triage lands beside this file as `ISSUES.md` after the run.

## Stations

| station | cluster | workflows | targets |
|---|---|---|---|
| A | sockeye | `engcyc_prep_references_lite`, then `engcyc_metagenomics_lite` | the template defaults |
| B | fir | `engcyc_prep_references`, then `engcyc_metagenomics` | all of them, unsplit |

Both use sample SRR9658108 (`--sample sra`). nibi is unreachable for this run, so it has no station. `--host nibi` still works, and `--keep FILE` or `--drop FILE` hands an agent a subset of targets to deselect in the GUI. Per-host facts, caps and GUI ports live in `tests/metasmith/e2e/agentic/hosts.py`.

Every per-run name carries the run and host: local dir `~/msm_e2e/run-4-<host>`, cluster dir `<scratch_root>/run-4-<host>`, systemd scope `msm-e2e-run-4-<host>`, and `STOP.json` and `watch.csv` inside the local dir. The two stations share nothing. Their GUIs use ports 8091 (sockeye) and 8092 (fir).

## Before launching

- Open the ssh master for each cluster (`ssh(verb=connect, host=<host>)`, one Duo push each). The agent's ssh rides it and cannot log in on its own.
- Build the release candidate with `install_mock/build_local_artifacts.sh`. That gives the channel dir, the `.sif` and the image tag.
- Run from the repo root of the worktree with `PYTHONPATH=$PWD/src:$PWD` and the `msm` env python.

## Launch

Each command starts the harness, which starts its own watcher. Run each from a terminal that outlives it, such as `nohup setsid ... &`.

Station A, sockeye lite:

    python -m tests.metasmith.e2e.agentic.first_install --run 4 --host sockeye \
      --prep engcyc_prep_references_lite --template engcyc_metagenomics_lite \
      --channel <CHANNEL> --sif <SIF> --image-tag <IMAGE_TAG>

Station B, fir full:

    python -m tests.metasmith.e2e.agentic.first_install --run 4 --host fir \
      --channel <CHANNEL> --sif <SIF> --image-tag <IMAGE_TAG>

To split a full run across stations, add `--keep <SUBSET_FILE>` or `--drop <SUBSET_FILE>` to a command. The file lists one target type per line (`taxonomy::gtdbtk`); `#` lines are ignored. The agent gets it as `TARGETS.txt` in its sandbox.

Watcher caps default per host: sockeye 300 GB, fir 1500 GB, 700K files each. Override with `--max-gb` and `--max-files` on a standalone `watch_remote.py --host <host> --run 4`.

## Watch

`tail -f ~/msm_e2e/run-4-<host>/watch.out` shows files, GB and job count every 5 minutes. The transcripts are under `~/msm_e2e/run-4-<host>/logs`.

## Stop

- Normal end: the agent writes `CONTROL.json` and the harness stops its own scope.
- Cap crossed: the watcher cancels the run's Slurm jobs, kills the driver tree (on every fir login node), writes `STOP.json` and stops the scope.
- By hand, one station only: `systemctl --user stop msm-e2e-run-4-<host>.scope` ends the harness, the agent and its detached processes on altair. Then clear the cluster side with the watcher's stop, `python -m tests.metasmith.e2e.agentic.watch_remote --host <host> --run 4 --max-files 1`, which trips on its first sample and cancels and kills. Read `STOP.json` to confirm.
- After reading the report, delete `~/msm_e2e/run-4-<host>` and the cluster dir by exact path. The harness refuses to start while either exists.

## Open items

- sockeye: the Slurm account `st-shallam-1` and the scratch root `/scratch/st-shallam-1/txyliu/msm_e2e` come from memory notes and path names, not a checked run.
- fir: `/scratch/phyberos/msm_e2e` is assumed. The watcher's file walk runs on a login node whose 16 GiB cgroup fills with page cache; watch for oom kills of the driver.
- The fir cross-login-node stop (`ssh fir ssh loginN`) is untested.
- sockeye has no `diskusage_report`, so its quota rows are empty; the run-dir cap is unaffected.

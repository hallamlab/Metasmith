# run-1 (published 0.25.1, 2026-11-19)

Goal: install from `./docs`, deploy to nibi, run `metagenomics_from_paired_reads` without metabuli on SRR9658108. Haiku, two full passes and the start of a third, 11:12 to 12:43. Stopped by the operator at 12:43 (`STOP.json`), because the agent was in a deploy restart loop it could not leave. The agent never deployed, so it never staged or ran the workflow.

Kept beside this file: the agent's `STEPS.md` and `PROGRESS.md`, its `deploy_nibi.py` and the unrun `run_srr.py`, the watcher's `watch.csv` and the stop record.

## What happened

1. It read the docs and installed metasmith 0.25.1 with the documented `mamba create` (step 5). It confirmed `ssh nibi` and `module load apptainer/1.3.5` (steps 4, 12).
2. The docs have no page for the workflow. It cloned MetasmithLibraries from GitHub and adapted the Sockeye example script (steps 7 to 11).
3. Its first `Deploy()` copied the image blobs in 40 s. Then the unpack on nibi's NFS scratch outlasted its 10-minute command limit (step 13).
4. It read the timeout as the squashfs failure that `install.rst` warns about, and switched to `rootfs="sandbox"` (step 14).
5. Each later deploy ran in the background and died when its pass ended. The orphaned build kept running on nibi. Each new pass killed it and started again (steps 15 to 18b). The last build had unpacked 7 of 15 layers in 41 minutes. One layer took 30 minutes.

## Footprint on nibi

- Peak 20,551 files and 1.7 GB in the run dir. All of it was the half-unpacked rootfs in `tmp/` and `container_images/build-temp-*`. The run did not reach the workflow, so the beta tester's scratch blowup was not tested.
- `~/.apptainer/cache` in `/home` grew from empty to 3.2 GB (2.3 GB `blob`, 934 MB `oci-tmp`).
- No Slurm jobs. No `.sif` was ever finished.
- nibi `/home` and `/scratch` are both NFS. Login-node `/tmp` is a 126 GB tmpfs.

## Issues, ranked by how far each blocks a new user

| # | class | where | issue | Rokaiya | fix |
|---|---|---|---|---|---|
| 1 | error | `setup/deployment.rst:48`, `tutorials/remote_agent.rst:147,175` | The documented setup commands set `TMPDIR`, `APPTAINER_TMPDIR` and `APPTAINER_CACHEDIR` to `/home/$USER/tmp`. On a cluster whose home and scratch are network filesystems, the image unpack then runs over NFS and takes over an hour. The login node's local `/tmp` is the fast path. Rokaiya's working fir agent left `TMPDIR` and `APPTAINER_TMPDIR` unset and put only the cache on scratch. | #17 | open |
| 2 | error | `Agent.Deploy` over ssh | Deploy prints nothing during a pull or unpack that runs for many minutes. When the local client dies, the remote build keeps running with no client. A rerun starts a second build beside it instead of noticing the first. | #17 | open |
| 3 | ambiguity | docs tree | No page covers the shipped templates: how to run `metagenomics_from_paired_reads`, how to drop a target, or how to start from an SRA accession. The agent cloned MetasmithLibraries `HEAD` from GitHub. That clone need not match the library vendored into 0.25.1. | — | open |
| 4 | workaround | `setup/install.rst:157` | The squashfs warning offers `rootfs="sandbox"` with no symptom to match against. The agent applied it to a timeout. A sandbox rootfs is loose files on the cluster filesystem, which is slower to unpack and heavier on the file quota. | — | open |
| 5 | error | apptainer cache default | Apptainer's blob and OCI caches landed in `~/.apptainer` in the 50 GB `/home`. Metasmith keeps the `.sif` under `${APPTAINER_CACHEDIR:-$AGENT_HOME/container_images}`, but it does not set `APPTAINER_CACHEDIR` for the pull itself. Apptainer's own cache therefore defaults to home. | #16 | open |
| 6 | ambiguity | `setup/deployment.rst` | The `module load apptainer` line is a note, not part of the example setup commands. The agent found the module by probing nibi. | #14 | open |
| 7 | ambiguity | `setup/install.rst` vs `agentic/setup.rst` | Two install paths. Carried from run-0. This is I4. | — | open |
| 8 | ambiguity | `setup/install.rst` expected output | The docs show `metasmith v#.#.#`. `msm --version` prints `metasmith 0.25.1`. Carried from run-0. | — | open |

## Harness issues

- H1. Every process the agent starts dies when its pass ends, and a pass ends at 45 minutes. A deploy longer than one pass can therefore never finish. A real user at a terminal would wait, so this loop is a harness artifact on top of issues 1 and 2. Keep the agent's background processes alive across passes, or let a pass run longer while the agent sleeps.
- H2. The agent ended pass 1 with a background deploy still running, against the protocol's "sleep and check" rule.
- H3. The first watcher check came 5 minutes after launch, and the probe's own runtime spaces samples 5 to 6 minutes apart. That is adequate.

## Decision (Tony, 2026-11-19 13:00)

> make appropriate fixes, but I thikn 1 hr in general is not enough. please check in on tests and stop them if doing in efficient stuff that can be fixed/improved -> fix them -> then try again. Please start with the browser route (test like a person) then we can try python API after (which should be just a comprehension test: are the calls understandable and do what is expected/documented).

All issues above are open, and every one is in scope for the next round. The next run drives the GUI in a browser. The Python API run comes after it.

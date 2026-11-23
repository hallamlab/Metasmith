# run-3 (published 0.25.1, branch docs at b8d5c05c, 2026-11-19)

Goal: install from `./docs`, start `msm gui` and drive it in a headless browser. Deploy to nibi, then run `metagenomics_from_paired_reads` without metabuli on SRR9658108's paired reads, given as ENA's two FASTQ URLs. Haiku, one pass, 14:36 to 16:48. Stopped by the operator at 16:48 (`STOP.json`). The setup run was downloading the metabuli database, which the goal excludes, and the download alone had passed 205 GB. With the GTDB unpack still to come, it put the shared 1 TB scratch quota at risk.

Kept beside this file: the agent's `STEPS.md` and `PROGRESS.md`, the watcher's `watch.csv`, the stop record, the agent file (`agent.yml`) and the workflow request (`workflow_request.yml`).

## What happened

1. It installed 0.25.1, started `msm gui`, and deployed an agent to nibi with the slurm preset and `slurmAccount` (steps 1 to 12). This time it typed the param name, so `slurmAccount` saved.
2. It made the workflow from the template and put the ENA URLs in the file rows. The solve rejected them (steps 13 to 16).
3. It downloaded the reads into its sandbox and solved: 34 steps, the full template (steps 17 to 21).
4. The setup run refused local files that were not on nibi. It copied the reads to nibi, solved again and started the setup run (steps 22 to 26).
5. Setup built about 20 tool images on the login node, one at a time, from 15:00 to 15:58. Then it submitted the first Slurm job (`buildPhyloflashDB`) and started the reference downloads (steps 27, 28).

## Footprint on nibi

- Peak 814 files and 243 GB in the run dir: `task_cache/staging` held 205 GB of metabuli tarball, 26 GB of GTDB parts, 2.4 GB of UniRef50 and 1.1 GB of KOfam. `container_images` held 9.9 GB, and the input reads 618 MB.
- The file count stayed flat through setup. The workflow steps never ran, so the beta tester's file blowup is still untested.
- One Slurm job (4 CPUs, 16 GB, 50 minutes), cancelled by the stop.
- `~/.apptainer` in `/home` grew to 3.8 GB, shared with another session. This is run 1's issue 5, fixed in 4bdd7f2a for the next release.

## Issues, ranked by how far each blocks a new user

| # | class | where | issue | fix |
|---|---|---|---|---|
| 1 | error | GUI recipe rows | "stop wanting this" on the metabuli output only armed the cross. The 2 s window ran out before the agent's second click. The agent then checked the page text, which leaves out input values, and believed metabuli was gone. The plan kept metabuli, and setup fetched its 200 GB+ database. This is run 2's issue 4, with a far higher cost. | 553838c3 |
| 2 | error | GUI file rows, `ops/inputs.py` | A URL in a file row is rejected at solve time with "relative path", and the GUI collapses `https://` to `https:/`. The error appears in the server log only, not on the page. The docs show `HttpSource` for the Python API, so a user expects URLs to work. | open |
| 3 | error | staging | A local input file is bound into the remote container, not copied. The setup run fails on nibi with "external input folder must be bound … do not exist on the remote host". The user must copy inputs to the cluster by hand, and no doc says so. | open |
| 4 | error | setup run | Setup fetches every reference the plan could use before the first job, with no size estimate up front. A user has no warning that 300 GB or more is coming, and no way to see how much is left. | feat/engine/setup-steps: download contracts declare `download_size`; the plan, the set up button and the CLI show the total |
| 5 | ambiguity | GUI setup run | Setup builds about 20 tool images one at a time, up to 10 minutes each for large ones, with no "image N of M" progress. | feat/engine/setup-steps: each image is a `pullContainer` step of the setup run, three at a time, counted in Nextflow's done/total |
| 6 | ambiguity | docs tree | No page covers the GUI, the shipped templates, or how inputs reach a remote agent. These are runs 1 and 2's issues 3 and 6. | open |

Fixes from earlier runs that this run confirmed: the setup commands (each tool image unpacked in node-local `/localscratch`), and the `slurmAccount` default saved once its name was typed.

## Harness issues

- H8. The watcher's stop matched processes by cwd only. Container steps run with cwd `/ws`, so 15 processes survived the stop, including three wgets. Fixed: the stop now matches the run dir in cwd or argv and kills every descendant. It was tested on nibi against a dummy process tree.
- H9. The agent waited for the cluster queue to empty with `squeue -u phyberos`. That wait never ends while another session on the account has jobs. Give the prompt the run's job filter, or accept this as agent behaviour.
- H10. The stall detector replayed old watcher lines on each re-arm. Fixed in the session script.

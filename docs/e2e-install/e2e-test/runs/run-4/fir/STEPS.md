# STEPS
1. Command: mamba create -y -p /home/tony/msm_e2e/run-4-fir/sandbox/envs/msm_env -c hallamlab -c bioconda metasmith
   Doc: docs/setup/install.rst section "Installation > Conda" (adapted: -p prefix inside sandbox instead of -n so env stays in sandbox)
   Note: WORKAROUND - used -p instead of -n so conda env lives under sandbox/envs.
2. Command: mamba create -y -p .../sandbox/envs/msm_env -c hallamlab -c bioconda metasmith (background, log tmp/install.log)
   Outcome: OK. metasmith 0.26.0 installed at sandbox/envs/msm_env/bin/msm. Warnings about empty local .channels/hallamlab repodata were non-fatal.
3. Command: msm --help ; msm gui --help
   Doc: no doc (docs have no GUI section; grep for gui/8092 found nothing). GUESS: `msm gui` from --help listing.
4. Command: nohup setsid msm gui --port 8092 --no-browser --project <sandbox> > tmp/gui.log 2>&1 &
   Doc: no doc: guess (from `msm gui --help`)
5. Action: browser_navigate http://127.0.0.1:8092/ (GUI via browser tools, no curl)
   Doc: no doc for GUI; task prompt says use GUI via browser tools.
6. GUI: Agents > + agent; "a remote host" tab; host=fir; home=/scratch/phyberos/msm_e2e/run-4-fir; runtime apptainer (default); nextflow preset=slurm; param slurmAccount=def-shallam_cpu; setup commands left default (#!/bin/bash only).
   Doc: docs/setup/deployment.rst ("Agents can also be deployed to remote machines through SSH", SLURM section). Setup commands: no doc-required module since `apptainer` is on PATH on fir (checked via ssh: /cvmfs .../apptainer/1.3.5). GUESS-free.
   Agent name auto-set to careful-fir.
7. GUI: clicked deploy on careful-fir.
   Outcome: ERROR. Deploy log: "failed to get checksum for docker://quay.io/hallamlab/metasmith:0.26.0-ea75c3f: manifest unknown". (Also apptainer 1.3.5 on fir rejected --mksquashfs-args; the deploy then fell back.) The default container image tag in GUI is 0.26.0-ea75c3f.
8. GUI: changed container image to docker://quay.io/hallamlab/metasmith:0.26.0 (tag 0.26.0 exists on quay; probed with curl to registry manifest API -- GUESS/WORKAROUND, docs install.rst use the untagged name), clicked deploy again.
9. Outcome of deploy #2 (image :0.26.0): OK. Log "deployment complete relay-deployed deployed to [ssh://fir:/scratch/phyberos/msm_e2e/run-4-fir]". GUI shows warning "pinned to something other than this metasmith's own image" (expected, from the tag change).
10. GUI: Workflows > + workflow > template=engcyc_prep_references > create -> workflow "rough-piculet" (8 inputs and 35 outputs pre-filled by template).
   Doc: docs/workflows/engcyc_metagenomics.rst ("Run engcyc_prep_references first, once per agent") and docs/setup/deployment.rst "Set up a workflow before its first run" (GUI: the workflow's set up button).
11. GUI: clicked solve on rough-piculet. Outcome: solved, 49 steps, "solved just now against library 0.26.0+a0d52". Plan notice: "solved from an unfinished recipe: ref::label_transfer_landmarks, modelling::media, ref::lane_calibration, ref::mnxr_lookup, modelling::medium_name have no path. Fill them in and solve again."
   Template spec (MetasmithLibraries/templates/engcyc_prep_references/spec.yml) lists these five as origin:leaf with no values. AMBIGUITY: docs do not say whether prep_references needs them. Plan to try set up first (the plan is solved and runnable), and report the five unfilled rows.
12. GUI: selected agent careful-fir and preset slurm in "stage and run". Outcome: "stage and run", "set up" and "environments only" stay DISABLED while the plan has unfilled leaf inputs (the five listed above). Those five are "deferred template inputs" (data_types/modelling.yml: media, medium_name; ref.yml: label_transfer_landmarks, mnxr_lookup, lane_calibration) and no files for them exist in the sandbox. engcyc_metagenomics's template lists the same five leaves, so it is blocked too.
   Decision: did not invent a growth medium or reference tables, and did not drop targets, since that changes what was requested. Stopping here and reporting.
13. Wrote REPORT.md and CONTROL.json (give_up: blocked on five deferred inputs).

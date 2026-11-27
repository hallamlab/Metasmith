# PROGRESS
## Done
- metasmith 0.26.0 installed at sandbox/envs/msm_env (msm, metasmith)
- GUI running: `msm gui --port 8092` pid 3100348, log tmp/gui.log, http://127.0.0.1:8092 (200)
- agent careful-fir DEPLOYED via GUI to ssh://fir:/scratch/phyberos/msm_e2e/run-4-fir, runtime apptainer, container docker://quay.io/hallamlab/metasmith:0.26.0, slurm preset, slurmAccount=def-shallam_cpu
- workflow rough-piculet = engcyc_prep_references created and SOLVED (49 steps)

## Blocked
- Stage/set up buttons disabled: 5 leaf inputs have no path: ref::label_transfer_landmarks, modelling::media, ref::lane_calibration, ref::mnxr_lookup, modelling::medium_name. They are "deferred template inputs"; no data in sandbox. engcyc_metagenomics needs the same five.
- engcyc_metagenomics NOT created/run. Results not collected.

## Failed (do not repeat)
- deploy with default image tag 0.26.0-ea75c3f: manifest unknown on quay.

## Next (needs user decision)
- Supply the five inputs (growth medium name+table, MNXR lookup, landmarks, lane calibration), or
- Choose targets that do not need them, then set up and run.

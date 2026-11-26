# REPORT

## Achieved
- Installed metasmith 0.26.0 with conda into sandbox/envs/msm_env (docs/setup/install.rst).
- Started the web GUI on port 8092 (`msm gui --port 8092`, detached, pid 3100348, log tmp/gui.log). Used the browser tools only for the GUI.
- Deployed agent `careful-fir` to `ssh fir` with home /scratch/phyberos/msm_e2e/run-4-fir, runtime apptainer, slurm preset, slurmAccount def-shallam_cpu. The deploy log ends with "deployment complete".
- Created workflow `rough-piculet` from template engcyc_prep_references and solved it (49 steps).

## Not achieved
- engcyc_prep_references was not set up or run. "stage and run", "set up" and "environments only" are disabled because the plan has five unfilled inputs.
- engcyc_metagenomics was not run. It needs the same five inputs.
- No results were collected to ./results.

## Issues
- ERROR (docs silent; GUI default): the GUI's default container image `docker://quay.io/hallamlab/metasmith:0.26.0-ea75c3f` does not exist on quay ("manifest unknown"). The first deploy failed. Changed to `:0.26.0`, which exists, and the second deploy succeeded. Section: setup/deployment.rst "Container Runtime" and "Agents can also be deployed..." do not name an image.
- GUESS: the docs have no section on the web GUI. The `msm gui --port 8092 --no-browser` command came from `msm --help`/`msm gui --help`.
- WORKAROUND: installed with `mamba create -p <sandbox>/envs/msm_env` instead of `-n msm_env`, so the env stays inside the sandbox (setup/install.rst, Conda section).
- WORKAROUND: the container image tag was changed in the GUI (see the ERROR above). The GUI shows a "pinned to something other than this metasmith's own image" warning.
- AMBIGUITY (blocker): workflows/engcyc_metagenomics.rst says engcyc_prep_references "takes no samples", but its template (MetasmithLibraries/templates/engcyc_prep_references/spec.yml) has five leaf inputs with no source: ref::label_transfer_landmarks, modelling::media, ref::lane_calibration, ref::mnxr_lookup, modelling::medium_name. data_types/modelling.yml calls media and medium_name "a deferred template input". The docs do not say how to supply them or whether the setup run can skip them. setup/deployment.rst "Set up a workflow before its first run" assumes the setup button works.
- AMBIGUITY: the docs do not say whether one could deselect the targets that need the five inputs. engcyc_metagenomics.rst says "Deselect a target to drop its lane", but that is not the same as setting up the references.
- Observation (not an error): on fir, apptainer 1.3.5 rejected `--mksquashfs-args`. The deploy fell back automatically, as setup/install.rst describes.
- Observation: `.no-fresh-login` in the sandbox is the ProxyCommand for `ssh fir`. Plain BatchMode ssh to fir still worked during this run.

## Next steps for the user
- Decide how to fill the five inputs (for example a growth medium for CarveMe, and the MetaNetX/ProteinBERT/calibration products), or approve dropping the targets that need them. Then run set up and the real run in the GUI.

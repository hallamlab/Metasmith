# run-4 (release candidate 0.26.0+ea75c3f, 2026-11-24)

Goal: install the release candidate from a local conda channel and SIF, start `msm gui`, and drive it in a headless browser. Two stations ran at once, one Haiku agent each, both on sample SRR9658108. Station A on sockeye ran `engcyc_prep_references_lite`, then `engcyc_metagenomics_lite`. Station B on fir ran `engcyc_prep_references`, then `engcyc_metagenomics`, unsplit. Both started at 17:19.

- fir gave up at 17:31 (`fir/REPORT.md`, `CONTROL.json` give_up).
- sockeye was stopped by the operator at 17:40 (`sockeye/STOP.json`). Its setup could not run, so its compute jobs could not pull images, and its downloads had reached 35.6 GB.

Kept beside this file, per station: the agent's `STEPS.md` and `PROGRESS.md`, the watcher's `watch.csv`, and fir's `REPORT.md` and sockeye's stop record.

## What happened

1. Both installed the candidate, started `msm gui` and deployed. On fir the first deploy failed: the image tag `0.26.0-ea75c3f` is unpublished, and the staged SIF was not where the deploy looked. The agent changed the tag to `0.26.0` and deployed the old release image.
2. Both prep templates solved, then flagged five empty inputs: `ref::mnxr_lookup`, `ref::label_transfer_landmarks`, `ref::lane_calibration`, `modelling::media` and `modelling::medium_name`. The GUI refuses to run a plan with an empty row. fir stopped here.
3. sockeye deleted the five rows (no prep step reads them) and pressed set up. Setup died at staging with "spec not found for the module 'buildAspireLiteratureContaminants'" (steps 14 to 16).
4. sockeye ran the prep as a real run instead. Its downloads ran on the login node. Its first compute job could not pull its image, because sockeye compute nodes have no route to quay.io and only setup pulls images (step 18).

## Issues, ranked by how far each blocks a new user

| # | class | where | issue | fix |
|---|---|---|---|---|
| 1 | error | setup run | Setup never worked on a staged workflow. The staged bundle prunes its transform libraries to the plan's steps, but keeps the full index. Setup walked the whole index to find `pullContainer`, loaded a pruned file, and died. `pullContainer` itself was never staged. | 2d3c6af5: staging always keeps `pullContainer`; setup finds it by key |
| 2 | error | engcyc templates | Both main templates plan the GPR, ProteinBERT and CarveMe lanes, which read five inputs no step makes. Three are fabfos bake products with no download, and the other two are a growth medium. A new user cannot run either template. The prep templates copied the five empty rows although no prep step reads them. | feat/engine/gpr-refs: port the `mnxr_lookup` and `label_transfer_landmarks` builds from fabfos and ship `lane_calibration` as a resource. feat/engine/complete-medium: CarveMe gapfills on a supplied complete medium by default |
| 3 | error | e2e harness | The harness staged the candidate SIF in `<home>/container_images`. fir's login shell sets `APPTAINER_CACHEDIR`, which the engine's image store follows, so the deploy pulled from quay and failed. | 2d3c6af5: stage under the remote `${APPTAINER_CACHEDIR:-<home>/container_images}` |
| 4 | guess | docs | No docs page launches the web GUI. Both agents found `msm gui --port` from `--help`. | T16 docs page (run 3 issue 5) |
| 5 | ambiguity | GUI recipe rows | Removing a row takes two clicks ("click again to confirm"). The sockeye agent's first pass only armed the crosses. | none: the confirm is deliberate (run 3 issue 1) |

## Not tested yet

Setup as steps, the size warning against the measured footprint, the second-setup re-link, split SRA accessions through the GUI, and every workflow step. All wait on the next round.

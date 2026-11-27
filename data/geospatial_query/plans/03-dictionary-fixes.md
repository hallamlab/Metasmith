# geoquery fixes from the dictionary pass

## Context

Sourcing a definition for every geoquery feature (635 features, 372 verbatim upstream quotes) surfaced about 125 problems in the adapters. Some features compute something other than their name says. Some names, classes and units differ from what the source publishes. Some features do not describe the AOI at all. Seven judgement topics were settled in a DFS interview and recorded in `docs/geospatial_query/DECISIONS.md` on `archive/geoquery-tomer-0.26.1`. This plan lands those decisions and the clear bug fixes, then reruns the dataset Tomer will receive. The driver's generic executor flags and the `fetch_source` memory fix already landed (9d0d4643).

## What you said

> the source wins, right? And so if it's named incorrectly, well, we got to match the source. And if the computation isn't what the source expects, then, well, we match the source.

> we should do like a full scan and make sure we're using the actual original published names or categories. At least for the data output. We do, I think, make simplified names for the transforms, and that's fine.

> you take the area cross-section of the AOE against the CSD, and that is the value contribution from the CSD. Then you sum all the CSDs

> We just report the nearest plant and then the distance. Some downstream curation step will then probably need to match the plant to its [sewershed]

> all we have to do is just also add a unit column. We should do that anyways for any column with variable units.

> You don't want to fetch the latest by default. You want to fetch by the timeline, as part of the query. Can we do version per source or something? Have like a separate versions table?

> We're still in dev, so renaming everything is fine, as long as the final package is internally coherent.

## Issues

- I1. Several features compute something other than their name or their source intends: sums across RVDSS viruses, NPRI particulates counted up to three times, a 0.25° pad named 25 km, bounding-box counts beside polygon counts, GBIF's 1,200-species cap, Sentinel-2 reflectance without its offset.
- I2. Census features report whole-subdivision values, blended across subdivisions, so no value describes the AOI.
- I3. PHAC wastewater averages every plant in the province, mixes liquid and solid fractions, and hides how far the plants are.
- I4. Class values sit inside feature names, and some class labels are geoquery inventions rather than the source's published names.
- I5. Some units are wrong, and the wide table cannot carry a unit for a feature whose unit varies between AOIs.
- I6. A run does not record which edition of each source it read, and some editions are frozen on outdated releases such as the 2016 census.
- I7. The feature dictionary describes the old feature set, and no class dictionary exists.
- I8. The fixes reach Tomer only through a new geoquery image and the metasmith package that pins it.

## High-level goals

- G1. Every feature describes the AOI the way its name says, computed the way its source intends.
- G2. Every name, class and unit in the output matches what the source publishes, and the package agrees with itself.
- G3. A reader can tell which edition of each source a run used.
- G4. Tomer receives a rerun dataset, with its dictionaries, from a pipeline release that carries these fixes.

## Acceptance criteria

- Source tables and `observations_long.parquet` carry a `class` column. `features_wide.parquet` keeps one row per AOI with columns named `feature[class]`.
- Every value row carries its own `SI_unit`. A feature whose unit varies across AOIs has a `<feature>_unit` column in `features_wide.parquet`.
- `_logs/versions.csv` holds one row per source: query time requested, data time returned, release or edition used, and URL fetched.
- Every class or category value in the outputs equals the source's published name. A scan report lists each source's classes beside the published list it was checked against.
- Census counts are additive areal weights over 2021 dissemination blocks and areas. Medians and averages are household-weighted and labelled approximate.
- PHAC wastewater reports the nearest plant: name, distance in km, fraction, latest value and 7-day average, with no cross-fraction averaging.
- RVDSS reports each virus exactly as PHAC publishes it. Its tests and detections sums are gone, and the virus is a class.
- GHS-SMOD uses GHSL's class names and first-level groups. `ghssmod_is_urban` is gone.
- Every lead in `feature_descriptions_issues.md` carries a disposition: fixed (commit), renamed to match the source, or not a bug (why).
- A full 11-AOI run on fir completes, and every coverage change against the pinned dataset has a stated cause.
- `feature_descriptions.csv` (one row per measure) and `class_descriptions.csv` are regenerated. Every definition is a verbatim upstream quote that the verify script confirms on the cited page, or it is blank with a reason.
- A subagent given only the public README and one AOI installs the released metasmith and finishes a geoquery run, and every step where it stalled or guessed is fixed in the docs or driver.
- `mmap_canada_mining` is re-pinned from the new run and ships both dictionaries and `versions.csv`.

## Tasks

- T0. Archive the 0.26.1 deliverable plan into the repo (G4)
- T1. Add the class column, per-row units and the versions table to the pipeline core (G2, G3)
- T2. Fix the adapter bugs and apply source-wins names, one worker per adapter file (G1, G2)
- T3. Rebuild the census features on 2021 dissemination blocks and areas (G1, G3)
- T4. Rebuild PHAC wastewater, RVDSS and GHS-SMOD to their decisions (G1, G2)
- T5. Scan every source's classes and categories against its published names (G2)
- T6. Compact
- T7. Build, push and pin the geoquery image (G4)
- T8. Run all 11 AOIs on fir and explain every change against the pinned dataset (G1, G4)
- T9. Regenerate the feature and class dictionaries and verify every quote (G2)
- T10. Compact
- T11. Publish and re-pin mmap_canada_mining with its dictionaries and versions table (G3, G4)
- T12. Cut the metasmith release that carries the new image (G4)
- T13. Commit a checkpoint
- T14. Simplify
- T15. Draft the doc updates
- T17. Check a cold-start run of one AOI with a no-context subagent (G4), executed after T15 and before T16
- T16. Debrief

## Approach by task

### T0. Archive the old plan

Copy `~/.claude/plans/plan-please-what-is-velvet-beaver-of-8c4fdc44-….md` (the 0.26.1 deliverable plan, now done) to `data/geospatial_query/plans/02-deliverable-0-26-1.md`. That follows `01-mvp-build.md` on `geospatial_query/dev`. Commit it on the archive branch, then delete the copy under `~/.claude/plans/`.

Removes: the stale plan file under `~/.claude/plans/`.

Gotchas: `data/geospatial_query/plans/` is git-tracked, not DVC-tracked, so `git add` it, not `dvc add`.

### T1. Pipeline core

Layer: `research/geospatial_query/geoquery/fetch.py`, `aggregate.py`, `registry.py`, and `publish()` in `geoquery_run.py`. An adapter's feature value may become a record carrying `value`, `class` and `unit`. A plain value keeps meaning "no class, unit from `feature_units.tsv`", so untouched adapters keep working. `rows()` writes `class` and the per-row unit, and it stops dropping the adapter's `item`, which becomes the release or edition field. `aggregate` pivots the wide table on `feature[class]`, adds `<feature>_unit` columns where a feature's unit varies, and writes `versions.csv` from each source's `time` and `item`. `publish()` copies `versions.csv` into `_logs/`.

Removes: nothing. The plain-value path stays because most adapters return unclassed features.

Gotchas: `class` must not join the pivot index, or the wide table breaks its one-row-per-AOI shape. An `item` holds tile ids for raster sources, so `versions.csv` keeps the release (collection and version), not every tile.

### T2. Adapter bugs and names

Layer: the five `adapters_*.py` files and `pipeline.py`, edited by five parallel subagent workers, one file each, briefed from the decisions and the issues file. Workers do not edit `feature_units.tsv`. Each reports its unit and name changes, and the session applies them centrally. Rules for every worker: the source wins (a source value takes the source's name and computation, a geoquery-computed feature is named for exactly what it computes); classes move into the class record; units follow the source. The clear fixes include:

- GBIF species paging past 1,200.
- The USGS count endpoint instead of a capped event query.
- RVDSS exact virus matching.
- The Sentinel-2 `BOA_ADD_OFFSET`.
- The NPRI particulate triple-counting, per NPRI's guide, and NPRI all-media names.
- A real 25 km buffer for ENA and MGnify.
- Polygon instead of bounding box for GBIF, OSM and Wikidata.
- Slope with cos(latitude), and precipitation divided by the actual years.
- Hansen 2001–2025.
- Landfill out of extractive area.
- Duplicate features, constants and self-check probes removed.
- No name truncation.
- The HGB scale and unit checked against the raster.
- The surficial-geology file identified.

Removes: `iolulc_built_frac`, `ovt_substation_n_aoi`, `ovt_opencellid_status`, `nrcan900a_total_records`, the `*_control_*` probe features, and the two RVDSS cross-virus sums.

Gotchas: workers share `feature_units.tsv` and `sources.tsv`, so a worker that edits either one creates a merge conflict. A rename must reach `feature_units.tsv` in the same commit as the adapter, or the row loses its unit silently.

### T3. Census on dissemination blocks and areas

Layer: `_a_statcan_csd` and `_a_statcan_dwelling_value` in `adapters_census.py`. First confirm that StatCan publishes 2021 dissemination-block population and dwelling counts and dissemination-area household and income profiles as downloadable files. Counts sum each unit's overlap share with the AOI. Median household income and average dwelling value become household-weighted means across the contributing areas, labelled approximate. The CSD name, type and overlap count stay as context features.

Removes: the area-weighted `wmean` blend over CSDs and the whole-CSD sum in `statcan_owner_occupied_dwellings`.

Gotchas: block files are large (about 500k blocks nationally), so cache them per province in the image's working cache. Some units have suppressed income, which must stay missing, not zero. Compare the summed population with `ghs_pop` per AOI as the sanity check.

### T4. PHAC wastewater, RVDSS, GHS-SMOD

Layer: `_a_phac_ww` and `_a_rvdss` in `adapters_health_env.py`, and `_a_ghs_smod` in `adapters_census.py`. Wastewater needs plant coordinates the open CSV lacks. Use PHAC's published site metadata if it exists. Otherwise ship a small plant-to-coordinate table built from the plant names, stating where each coordinate came from. RVDSS emits tests, detections and percent positive per virus, with the virus as class. GHS-SMOD emits GHSL's class names and first-level groups.

Removes: the provincial wastewater mean and its province-pooled p90 ratio, the RVDSS sums, `ghssmod_is_urban`, and the `SMOD_LABEL` relabelling.

Gotchas: PHAC labels solids "copies/mL" too, so the unit is written as PHAC labels it, with the solid caveat. A plant name such as "Toronto Humber" names a plant, not a town, so geocoding by town name misplaces it.

### T5. Published-names scan

Layer: a read-only sweep over every adapter's class and category values against each source's published list. Run it as subagents that report differences, then apply the fixes in the adapters. The report lands beside the dictionary.

Removes: geoquery-invented class labels wherever they reach the data output.

Gotchas: feature identifiers stay geoquery's (the user allows simplified names there). Only class and category values in the data must match.

### T6. Compact

Handoff: decisions in `docs/geospatial_query/DECISIONS.md`. Code changes are committed on `archive/geoquery-tomer-0.26.1`. Worker reports are folded into the issues file's dispositions. The todo list holds T7 onward. Fir agent home: `/scratch/phyberos/geoquery/msm_home`. Run with `examples/slurm.yml`, `--runtime apptainer` and `--setup "module load apptainer"`. Verify script: the job tmp copy, moved into the repo in T9.

### T7. Image

Layer: `docker/geoquery/dev.sh -p`, which pins `src/metasmith_libraries/resources/env/geoquery.env` to the new tag and digest. The tag comes from `src/metasmith/version.txt`, so claim the release number from the e2e session (T12's first step) and set it before building.

Removes: none, because the old tag stays on quay for 0.26.x users.

Gotchas: an unpinned rebuild reuses cached step results (README caution). A clean build took 29 minutes on altair's degraded disk, so build on a healthy host or allow the time.

### T8. Full run on fir

Layer: `geoquery_run.py` with the fir flags, using a library compiled from this checkout (`dev/libraries.sh -bm`) so the run plans with the new image pin. Compare coverage, feature counts and values with the pinned dataset. Every change needs a cause: a bug fix, a rename or live data.

Removes: none.

Gotchas: the driver clones the installed package's library, so planning against this checkout needs an explicit library path or the dev overlay. A partial smoke on the changed sources first saves a full run when something breaks.

### T9. Dictionaries

Layer: rerun the sourcing over the new feature set. Reuse each verified quote whose upstream field did not change, and source only the new or renamed measures and the class definitions. Move `verify.py` into `research/geospatial_query/` and run it over both files.

Removes: the old per-class rows in `feature_descriptions.csv`, which move to `class_descriptions.csv`.

Gotchas: canada.ca and www12.statcan.gc.ca refuse scripted fetches, so those quotes need the hand-check path used before.

### T10. Compact

Handoff: the run key and its results path on fir, the comparison notes, the dictionaries and the verify report, and the todo list from T11 on.

### T11. Dataset

Layer: `publish()` writes `feature_descriptions.csv`, `class_descriptions.csv` and `_logs/versions.csv` into the dataset and names them in its README. Re-pin with `dvc add`, then push with `dvc(verb="push")`.

Removes: the converted 0.7 dataset as the pinned version.

Gotchas: the DVC dir is read-only hardlinks into the shared cache, so publish to a fresh folder and replace it, never edit in place.

### T12. Release

Layer: `RELEASE_PROTOCOL.md`, in its order: version bump, image (T7), library, conda package, quay. Before the version bump, message the e2e install-test session (find it with `ListAgents`) to claim a release number, because it also cuts releases. Take the number it gives, then cut the release without waiting on its own release. The geoquery template's goldens move with the transform's memory change.

Removes: none.

Gotchas: the number must be claimed before T7, because the image tag comes from `version.txt`. Anaconda upload uses the API token, never `anaconda org login`. Check `release` upstream before pushing, because 0.26.2 raced 0.26.1.

### T13. Commit a checkpoint

Push `archive/geoquery-tomer-0.26.1` with the dataset pin and the image pin together.

### T14. Simplify

Run the `simplify` skill over the pipeline core and the adapters this plan touched. Consolidate the class-record path so adapters share one way to emit classed features.

### T15. Draft the doc updates

Edit `research/geospatial_query/README.md` for the class column, the versions table, the dictionaries and the generic executor flags, with `examples/slurm.yml` named as an example only. Mirror that in the sphinx geoquery page (`docs/metasmith/source/workflows/geoquery`). Close the `osm` stack topic if the run confirms OpenStreetMap works.

### T17. Cold-start check

Layer: the user-facing path only. Dispatch a subagent with no session context: it gets the public README (`research/geospatial_query/README.md` and the sphinx page), one AOI from the dataset, and a clean working folder. It installs the released metasmith package from conda, writes its own Nextflow config from the shipped example, and runs `geoquery_run.py` end to end. It reports each step where it stalled, guessed or read source code to proceed. Fix each gap in the docs or the driver, then rerun the check if a fix changed the path.

Removes: none.

Gotchas: the brief must not leak repo paths, fir details or the worktree, or it tests our context instead of the docs. Run it on a host with docker or apptainer and network, local executor, so it needs no cluster account.

### T16. Debrief

Run the `debrief` skill. Journal the session and its decisions pointer.

## Callouts

- You said: "coordinate with e2e session to get a release number, but dont wait for them to release". T12 claims the number early, because T7's image tag depends on it.

- Query time stays out of this plan. It is its own build on the stack, and `versions.csv` is where it plugs in.
- The archive branch is now the working branch for geoquery. Merging it back to `feat/dev` is not in this plan.

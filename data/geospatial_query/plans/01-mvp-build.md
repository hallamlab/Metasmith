# MVP POC plan: AOIs in, one table per source out, planned by metasmith

## Context

Koonkie accepted the plan: build an MVP POC, hand back a dataset for their AOIs, then turn it into an open-source tool they run themselves. Tomer sent a KML of 11 known Canadian mining sites as the test set. The POC adapters exist and pass on 15 test AOIs, but they run from a flat Python script. The approved design page (panel 4) says the pipeline is a scatter-gather across sources that metasmith plans and compiles to Nextflow. This build makes that DAG real, runs it on Koonkie's AOIs and publishes the result as a DVC-tracked dataset in the engcyc layout. Discussion state is in `docs/geospatial_query/STACK.md` and `DECISIONS.md` on `geospatial_query/dev`. This file replaces the draft at `docs/geospatial_query/PLAN.md`.

## What you said

> The objective is to build the MVP POC that the email mentions. And of course, to run it on his AOIs.

> it is data and im not under non disclosure. dvc track it?

> have a look at cyanoverse style and engcyc data project folder structures. I want the same layout for these AOIs

> the pipeline should ingest 1 table of AOIs and produce tables per source

> can I see the metasmith DAG as part of the plan? mock of transforms?

## Issues

- I1. No single environment or container runs every adapter. `geoquery` lacks pyarrow, `msm` lacks rasterio, and `pipeline.py` needs a local `projdata` folder.
- I2. The POC reads only GeoJSON with five fixed properties and a `tier` filter. Koonkie's AOIs arrive as KML.
- I3. No metasmith types or transforms exist for the geospatial pipeline. Panel 4 of the design page draws a DAG that nothing implements.
- I4. The POC writes flat CSVs into its working folder, not one table per source in a dataset tree with provenance and gaps.
- I5. The POC output lacks the approved spec's `SI_unit` and does not use its column names.
- I6. The largest Koonkie AOI is about 4,000 km², six times the largest AOI the POC was tested on.

## High-level goals

- G1. Have one pipeline that turns one table of AOIs into one table per source.
- G2. Have metasmith plan and run that pipeline, as the design page promised.
- G3. Hand Koonkie a dataset for their mining AOIs.
- G4. Lay that dataset out like the engcyc and Cyanoverse datasets, versioned with DVC.

## Acceptance criteria

- A planning-only driver prints and renders the DAG below: one `normalise_aois`, one `fetch_source` per source, one `aggregate`.
- One driver command takes one AOI table (KML or GeoJSON) and a dataset id. It plans, runs and publishes into `data/geospatial_query/mmap_canada_mining/`.
- The tree holds one table per source that found data, each covering every AOI it found.
- Each of the 11 AOIs × each source resolves to `found`, `unavailable` or `not-attempted`. `_logs/gaps.tsv` lists every pair that is not `found`, with its reason.
- `integrated/observations_long.parquet` has one row per value with `aoi`, `source`, `observation_type`, `time`, `feature`, `family`, `value`, `SI_unit`, plus `licence` and `native_res_m`. Every row has `observation_type`. A blank `SI_unit` occurs only for unitless or categorical features.
- `_logs/ledger.tsv` has one row per published file with `bytes` and `sha256`. No published file has zero bytes.
- The 15 test AOIs, run through the DAG, reach the October re-pull result: 719 adapter features ok.
- The dataset is DVC-tracked and pushed to chinook. Its `.dvc` file is committed on `geospatial_query/dev`.

## The metasmith DAG

The DAG is a scatter-gather across sources. It lives in a small metasmith library inside the scope at `research/geospatial_query/msm/`, with its own `data_types/geoquery.yml`, `transforms/geoquery/` and `resources/containers/geoquery.oci`. Keeping it in the scope keeps it ready to open-source as Koonkie's tool.

Given inputs:

- `geoquery::aoi_table`: the one AOI table, KML or GeoJSON.
- `geoquery::source`: one value per source, such as `{"adapter": "esa_worldcover"}`. The driver adds one for each of the 75 POC sources from `reports/source_registry.csv`.
- `containers::geoquery.oci`: one image built from `envs/geoquery.yml` with the POC adapter code inside.

### Planned DAG

Square brackets are given inputs, parentheses are transforms, and angle brackets are targets. `fetch_source` runs once per source. The diagram collapses the 75 runs by feature family, with the source count and examples on each line.

```
                      [aoi_table]  KML or GeoJSON, one row per AOI
                           |
                    (normalise_aois)          x1
                           |
                         aois  ------------------------------+
                           |                                 |
 [source] x75 ---> (fetch_source)  scatter, group_by=source  |
                           |                                 |
   infrastructure     17   epa_echo, overture_buildings, osm_overpass ...
   socio-economic     14   statcan_csd_2016, ghs_pop_r2023a, bc_tantalis_crown ...
   macrobiology       10   esa_worldcover, nrcan_landcover, hansen_gfc_v1_13 ...
   public health       8   phac_wastewater, mgnify, statcan_odhf ...
   geology             8   mrdem_30, gsc_surficial_1880a, macrostrat ...
   hazards             8   eccc_npri, usgs_fdsn, eccc_naps ...
   spectral            5   sentinel2_l2a, landsat_c2l2_st, sentinel1_rtc ...
   climate             2   era5_open_meteo, noaa_nsidc_sic_cdr
   latent              2   alphaearth_v1_annual, clay_v0_embeddings
   water bodies        1   jrc_gsw
                           |
                    <source_table> x75   one table per source, rows = AOI x feature
                           |
                      (aggregate)             x1, gathers all 75
                           |
     +---------------+-----+---------+-----------+
     |               |               |           |
<observations_long> <features_wide> <coverage>  <gaps>
```

Every transform also requires `[containers::geoquery.oci]`. The diagram leaves that edge out.

### Mock transforms

Each transform is one file under `msm/transforms/geoquery/`, in the shape of `metasmith-libraries/transforms/amplicon/blast_map_asvs.py`. Each protocol calls one entry point of the POC package inside the container.

```python
# normalise_aois.py — one AOI table in, normalised AOIs out
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("containers::geoquery.oci"))
table = model.AddRequirement(lib.GetType("geoquery::aoi_table"))
aois  = model.AddProduct(lib.GetType("geoquery::aois"))

def protocol(context: ExecutionContext):
    i, o = context.Input(table), context.Output(aois)
    context.ExecWithContainer(image=image,
        cmd=f"python -m geoquery.normalise --in {i.container} --out {o.container}")
    return ExecutionResult(manifest=[{aois: o.local}], success=o.local.exists())

TransformInstance(protocol=protocol, model=model, group_by=table,
                  resources=Resources(cpus=1, memory=Size.GB(1), duration=Duration(minutes=5)))
```

```python
# fetch_source.py — one source over every AOI, one table out (the scatter)
from metasmith.python_api import *

lib    = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model  = Transform()
image  = model.AddRequirement(lib.GetType("containers::geoquery.oci"))
aois   = model.AddRequirement(lib.GetType("geoquery::aois"))
source = model.AddRequirement(lib.GetType("geoquery::source"))      # value: {"adapter": "esa_worldcover"}
table  = model.AddProduct(lib.GetType("geoquery::source_table"))

def protocol(context: ExecutionContext):
    a, s, o = context.Input(aois), context.Input(source), context.Output(table)
    context.ExecWithContainer(image=image,
        cmd=f"python -m geoquery.fetch --aois {a.container} --source {s.container} --out {o.container}")
    # a table of gap rows is still a success: absence is declared, never silent
    return ExecutionResult(manifest=[{table: o.local}], success=o.local.exists())

TransformInstance(protocol=protocol, model=model, group_by=source,
                  resources=Resources(cpus=1, memory=Size.GB(2), duration=Duration(minutes=20)))
```

```python
# aggregate.py — every source table in, integrated tables out (the gather)
from metasmith.python_api import *

lib      = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model    = Transform()
image    = model.AddRequirement(lib.GetType("containers::geoquery.oci"))
aois     = model.AddRequirement(lib.GetType("geoquery::aois"))
tables   = model.AddRequirement(lib.GetType("geoquery::source_table"))   # all 75, grouped under aois
long_    = model.AddProduct(lib.GetType("geoquery::observations_long"))
wide     = model.AddProduct(lib.GetType("geoquery::features_wide"))
coverage = model.AddProduct(lib.GetType("geoquery::coverage"))
gaps     = model.AddProduct(lib.GetType("geoquery::gaps"))

def protocol(context: ExecutionContext):
    ins  = " ".join(str(t.container) for t in context.InputGroup(tables))
    outs = {p: context.Output(p) for p in (long_, wide, coverage, gaps)}
    context.ExecWithContainer(image=image,
        cmd=f"python -m geoquery.aggregate --tables {ins} --out-dir {outs[long_].container.parent}")
    return ExecutionResult(manifest=[{p: o.local for p, o in outs.items()}],
                           success=all(o.local.exists() for o in outs.values()))

TransformInstance(protocol=protocol, model=model, group_by=aois,
                  resources=Resources(cpus=2, memory=Size.GB(4), duration=Duration(minutes=15)))
```

`context.InputGroup` stands for whatever call the metasmith API uses to hand a transform every instance of a grouped input. T3 confirms the real call against the binner aggregator in the metagenomics library.

### Types

`msm/data_types/geoquery.yml` declares `aoi_table` (KML or GeoJSON), `aois` (normalised GeoJSON), `source` (JSON value), `source_table` (TSV), `observations_long` and `features_wide` (parquet), `coverage` and `gaps` (TSV). `msm/data_types/containers.yml` declares `geoquery.oci`.

### Source table columns

One row per AOI × feature found, plus one status row per AOI with no feature: `aoi`, `source`, `observation_type`, `time`, `feature`, `family`, `value`, `value_num`, `SI_unit`, `licence`, `native_res_m`, `status`, `reason`.

Publishing stays outside the DAG. The driver copies the targets into the engcyc layout:

- `<dataset>/README.md`, generated: sources, run date, image, column notes, table list from the tree.
- `<dataset>/aois.tsv` and `<dataset>/aois/`: the AOI table as ingested and as normalised.
- `<dataset>/<family>/<source>.tsv`: each `source_table`, at the root of its feature family.
- `<dataset>/integrated/observations_long.parquet`, `features_wide.parquet` and `coverage.tsv`.
- `<dataset>/_logs/ledger.tsv`, `gaps.tsv` and `runs/<run>.tar.gz` (the metasmith run logs).

## Tasks

- T1. Pin one conda env and build one container image that runs every adapter (G1, G2)
- T2. Turn the POC into a package with fetch, normalise and aggregate entry points (G1)
- T3. Write the metasmith types and transforms, and render the DAG from a planning-only driver (G2)
- T4. Commit a checkpoint
- T5. Compact
- T6. Run the DAG on the 15 test AOIs, then on Koonkie's AOIs, and fix the failures that large AOIs cause (G2, G3)
- T7. Publish, DVC-track and push the dataset (G3, G4)
- T8. Compact
- T9. Simplify
- T10. Draft the doc updates
- T11. Debrief

## Approach by task

### T1. Pin one conda env and build one container image that runs every adapter

Add `envs/geoquery.yml` with rasterio, pyproj, pystac-client, shapely, requests, numpy, pandas, pyarrow and fiona (for KML). Point PROJ at the env's own data. Build `geoquery` as a Docker image from the env and the POC package. Write its URI to `msm/resources/containers/geoquery.oci` and its type to `msm/data_types/containers.yml`.

Removes: the `./projdata` path logic at the top of `poc/pipeline.py`, and the gitignored-pyarrow workaround in `reports/recheck/adapters/go.sh`.

Gotchas: try a local image tag first. Pushing to quay failed for Cyanoverse on missing auth, so fall back to ghcr if the runtime needs a registry. `mamba run` drops stdin, so call the env's python by path. The container needs outbound HTTPS to every source host.

### T2. Turn the POC into a package with fetch, normalise and aggregate entry points

Move the adapter modules under `poc/geoquery/` as one package. Add three entry points that the transforms call: `normalise` (KML or GeoJSON in, normalised GeoJSON out), `fetch` (one adapter over every AOI, one table out) and `aggregate` (every source table in, integrated tables out). `fetch` reuses `run_one` and the `ADAPTERS` registry from `pipeline.py` and the lane modules. Add `reports/feature_units.csv`, seeded by rules on feature-name suffixes and filled by hand for the rest.

Removes: `poc/run_all.py` and `poc/build_outputs.py`. Their roles move to the DAG and the driver.

Gotchas: KML names carry en dashes and slashes. Drop the `tier` filter in `load_aois` for files that carry no tier. Mixed-type `value` breaks a parquet column, so store `value` as a string with a numeric `value_num` beside it. Dedup on (slot, source) as `run_all.py` does today. Pull the KML with `productivity download_attachments` from the 2026-11-12 message in the Tomer thread.

### T3. Write the metasmith types and transforms, and render the DAG from a planning-only driver

Write `msm/data_types/geoquery.yml` with the types named in *The metasmith DAG*. Write the three transforms under `msm/transforms/geoquery/` in the shape of `blast_map_asvs.py`. Build the library metadata with `msm build`. Write `msm/main/plan.py` in the shape of `metasmith-libraries/main/diamond_uniref50_from_assembly.py`: it adds the AOI table and the 75 source values, generates the workflow and stops. Render the DAG to SVG with `metasmith.models.dag_draw`, as `figures/design/build.py` does for panel 4.

Removes: none. The hand-drawn panel 4 DAG in `build.py` can later read this one instead.

Gotchas: confirm early that the planner fans `fetch_source` out over 75 `source` values and gathers all 75 tables into one `aggregate`. The binner aggregator in the metagenomics workflow is the precedent. Every type a transform names must exist in `geoquery.yml` or the build fails.

### T4. Commit a checkpoint

Commit the env, image files, package, `msm/` library, unit table and the rendered DAG on `geospatial_query/dev`. Push to capella.

### T5. Compact

Handoff: T1–T4 committed. The rendered DAG is in `msm/`. The KML sits in a temp dir from `download_attachments`. Continue with T6.

### T6. Run the DAG on the 15 test AOIs, then on Koonkie's AOIs, and fix the failures that large AOIs cause

Extend the driver to stage and run the workflow. Run it on `examples/test_aois.geojson` and compare against the October re-pull in `reports/recheck/`. Then run it on the 11 Koonkie AOIs. Compare each source's failure rate against the 15-AOI baseline. Fix failures that come from AOI size: vector API page limits, request timeouts and raster windows. A failure the source causes stays a gap.

Removes: none expected.

Gotchas: the flat POC took about 3 minutes for 15 AOIs at 8 threads. 75 container runs add start-up time, so expect 10–20 minutes. Rerun only the failing sources while fixing. Do not loosen the mid-Pacific control check to make a source pass. Raster reads cap at `MAXPIX` = 320 px a side, so a 100 km AOI reads at about 300 m per pixel. Record that in the README rather than raise the cap. Planetary Computer SAS tokens 429 under concurrent runs.

### T7. Publish, DVC-track and push the dataset

Run the driver's publish step into `data/geospatial_query/mmap_canada_mining/`. Track the dataset folder with `dvc add`. Push it with the `dvc` tool. Commit the `.dvc` file on `geospatial_query/dev`.

Removes: none.

Gotchas: `data/` is the shared data view of the metasmith project. Track only this dataset folder. Check `dvc scope_status` before and after. A source with no found AOI gets no table, only gaps rows.

### T8. Compact

Handoff: dataset published, pushed and committed. Record the found and gap counts. Continue with T9.

### T9. Simplify

Run the `simplify` skill over the package entry points, the `msm/` transforms and the driver.

### T10. Draft the doc updates

Rewrite `poc/README.md` for the driver command, the image and the output tree. Remove its stale `requirements.txt` instruction and its flat-CSV output table. Point at the engcyc `LAYOUT.md` for the grammar rather than restate it.

### T11. Debrief

Run the `debrief` skill in `metasmith/geospatial_query/dev`. Close the `mvp-build` topic in `DECISIONS.md` and `STACK.md`.

## Callouts

- The MVP asks each source for its default moment, usually the latest data or a climatology. The spec's `Query.time` is not built. It stays on the stack.
- The 152 probed features without adapters and OpenStreetMap stay out of the MVP and on the stack.
- Packaging as an open-source tool for Koonkie, and what goes back to Tomer, come after the MVP. The `msm/` library and the image are the start of that tool.

## Autopilot

Guardrails: email content stays off disk except the KML. Never loosen the mid-Pacific control check. Rerun only failing sources while fixing. Test adapter fixes by mounting `poc/geoquery` into `geoquery:local` (`docker run -v`), rebuild the image (`docker/geoquery/dev.sh`, ~4.5 min, mostly image export) once per batch.

Resume handles:
- Python for metasmith: `PYTHONPATH=src /home/tony/lib/miniforge3/envs/msm/bin/python` from the worktree root.
- Solver engine: staged host-linked at `src/metasmith/engine/` (gitignored). Rebuild with `MSM_SOLVER_TARGET_DIR=$HOME/.cache/metasmith/solver-target-tony PATH=$HOME/.cargo/bin:$PATH ./dev/metasmith.sh -bel` (the default target dir has a root-owned `host/`).
- Driver: `research/geospatial_query/msm/main/geoquery.py --aois X --dataset ID --work DIR [--sources a,b]`. Plans only so far; T6 adds stage/run/wait (`StageWorkflow`, `RunWorkflow` local preset, `WaitForWorkflow`, `GetResultSource`). First pool import of 75 values takes ~7 min; warm replans take ~1 s.
- Work dirs: `$CLAUDE_JOB_DIR/tmp/plan75` (test15 pool, warm).
- KML: productivity request qoD5POsTVoXCf awaits Tony; then download the KML Koonkie sent on 2026-11-12.

### Live state

Run finished 2026-11-19 23:40. T1–T11 done. Dataset re-pinned in 0a977204 with the PHAC README caveats and pushed; driver is local-only; poc/README.md rewritten (9bde6c47). Open items moved to docs/geospatial_query/STACK.md (deliverable to Tomer first).

### Run log

- 2026-11-19 10:58 — Deviation: `feature_units.tsv` and `sources.tsv` live inside the package (`poc/geoquery/`), not `reports/`, because the image needs them. Units filled by a subagent; 113 of 814 blank (categorical/text), 3 PHAC wastewater load units unconfirmed.
- 2026-11-19 10:58 — The metasmith API now requires pool-imported givens (`agent.PoolGivens()` → `Build`); the plan's `inputs.AddItem` sketch is obsolete. Source values are children of the aoi_table given so they share its sample.
- 2026-11-19 10:58 — The `aggregate` gather uses `parents={aois}` + `context.InputGroup`, after `metagenomics/binning/aggregator.py`.
- 2026-11-19 10:58 — Gap mapping: not-attempted → not_applicable; unavailable with LookupError (no pixels) → no_hits; other unavailable → failed.
- 2026-11-19 11:50 — Transforms moved from `ExecWithContainer` (now refused by metasmith's dispatch scan) to `context.ExecWithEnv(env=image, cmd=...)`. `LocalShell` stays allowed.
- 2026-11-19 11:50 — `source_table` added as a target: results hold only targets, and publish needs the per-source tables.
- 2026-11-19 11:50 — Runtime: first `Deploy` into a fresh msm home ~5 min; each container step ~1–3 min to start at host load ~14. 2-source smoke took 745 s. Do not wrap the driver in `timeout`; use its `--timeout`.
- 2026-11-19 12:45 — Moved runs to nibi. Nibi compute nodes have direct outbound HTTPS (no proxy); fir is reserved for maintenance. Image shipped as `docker save` → nibi → `apptainer build` into `<home>/container_images/docker..geoquery..local.sif`, which the engine uses in place of a pull.
- 2026-11-19 12:45 — Remote quirks met: Deploy must precede PoolGivens (remote pool reads need the agent); absolute-path inputs must already sit on the remote (driver rsyncs the AOI table to `<home>/inputs/<dataset>/`); a pool name keeps its first path, so a misregistered name needs a new dataset id.
- 2026-11-19 12:45 — Engine 0.24.1 bugs: cache twins inherit the Slurm preset's `array = 100` and Nextflow aborts ("local does not support job arrays"), fixed in the worktree source but not in the deployed image; worked around with a derived slurm.nf with `array = 0`. And metasmith reports `run completed` for a run whose Nextflow aborted with 0 outputs; the driver now checks every target dir.
- 2026-11-19 14:15 — First nibi test15 (jOkE6lLf, 48 min): 691/719 baseline features. Fixes in geoquery 0.2: serial retry with backoff for 429/timeout/5xx in fetch.py; netcdf4 in env; aggregate reads all columns as text (native_res_m mixes metres and "vector"). Image tag now versioned in geoquery.oci because the tag is part of each step's cache key. BC WFS 400 on stress_mine_hvc_full was transient (reproduced 200, 164 parcels).
- 2026-11-19 15:20 — 0.2 full test15 run on nibi (NEf0I4Pg, 50 min): 698/719. modis_13q1 and usda_cdl timed out on every retry: `pc_item` passed only `limit` (page size), so `.items()` walked the whole archive. 0.3 adds `max_items=limit`; locally modis 45 rows, cdl 30, landsat 75, s2 180, each under 45 s, dates match October. Family dirs now snake case (`public_health_microbiology`, `socio_economic`). aafc_aci, acs5_api, mapillary_kartaview stay gaps (dead endpoint, API key, OAuth), as in October.
- 2026-11-19 15:33 — Gmail approved; KML downloaded and DVC-tracked under `data/geospatial_query/originals/koonkie_aois/` at Tony's request. 11 AOIs, 5–4,351 km², all CA.
- 2026-11-19 15:33 — Caught before the long run: the KML has two layers and the default one is empty, so normalise produced 0 AOIs silently. 0.4 reads every layer and exits on zero AOIs.
- 2026-11-19 15:33 — modis_13q1 (3 features, 15 AOIs) and usda_cdl (18 features, 3 US AOIs) pass through the DAG on nibi with 0.3 (YTDs43MZ, 859 s): test15 at 719/719.
- 2026-11-19 16:58 — First Koonkie run died in planning after 80 min: pool import of 76 givens on nibi is one ssh+apptainer call per item (~1 min each), and one call returned no JSON (likely a dropped ssh). `EnsurePoolEntries` skips held names, so the rerun (log tmp/nibi_mmap2.log) resumes. Engine issue to report: serial per-item remote imports with no retry.
- 2026-11-19 18:35 — Koonkie run s0bDfXfV (0.4, 3071 s): 57/75 sources found data, 6,081 values; no timeouts or size failures. Coverage gaps traced to tile choice: same-date PC tiles came back in arbitrary order and STAC footprints are loose (io-lulc 60U/01U wrap the antimeridian), so nrcan_landcover (2 AOIs) and io_lulc (1) read tiles with no AOI pixels. 0.5 adds `read_best` (read every newest-date tile, keep the one with the most non-zero AOI pixels) for dem, worldcover, nrcan_lc, hgb, gsw, palsar, io_lulc; pc_item sorts newest first. Local check: nrcan 9→11 and io_lulc 10→11 on Koonkie, nrcan 10→12 on test15; changed DEM values all have more pixels. Remaining gaps are real coverage: Clay (9, sparse tile set), usgs_namrad (4, outside grid), overture_divisions (2, remote), sentinel1_rtc (Nunavut), phac_wastewater (Nunavut), statcan_dwelling_value (suppressed).
- 2026-11-19 18:50 — The 0.5 publish run took 181 s and reused key s0bDfXfV: every step hit the 0.4 cache. A leaf input's id is path + mtime, minted once and stored in `_metadata/index.yml`; editing `geoquery.oci` never moves it (the earlier "tag is part of the cache key" note was wrong). Re-minted with `DataInstanceLibrary.Invalidate`; dev.sh now does this after every build (8a521f03). Rerun with publish in flight (log tmp/nibi_mmap4.log). The stale publish in data/geospatial_query/mmap_canada_mining gets overwritten.
- 2026-11-19 19:46 — Nibi run cancelled at Tony's request (Nextflow head killed, Slurm jobs scancelled); runs switched to local. Timing of the 0.4 nibi run: fetch median compute 32 s but median submit-to-finish 941 s, so queue wait was ~80% of wall. Local 0.5 run pRwaXjtl took 2609 s with fetch at 2 CPUs (8 at a time); fetch_source lowered to 1 CPU / 2 GB. Versus 0.4: only gains (nrcan +2, io_lulc +1 AOIs); non-coverage failures are only aafc_aci and mapillary_kartaview.
- 2026-11-19 20:25 — Adversarial review of the 0.5 tree: 7 real findings, all verified in code and fixed in 0.6 (83c59476): wqp US-only; ALTA single-province + EQA distance (AOI 2 now 17.6 km to Kawawachikamach, was 571 km); donneesquebec first-CSD province; permafrost "No permafrost" polygons counted as present; IDs parsed numeric; epa_echo duplicate gap rows (two lanes); 9 missing units; ledger missing README/gaps/run tar. Retest also caught my own bug (`.str` on an all-NaN feature column crashed every source with no found rows). Left as is: overture_divisions point search (AOIs 7, 10), 22 found rows with null value, family display names in the `family` column. Full 0.6 local run with publish in flight (log tmp/local_mmap6.log). Reviewer scripts for re-check: $CLAUDE_JOB_DIR/tmp/review/c1.py..c6.py.

- 2026-11-19 22:00 — 0.6 run Qg0SotL9 (3301 s) died: osm_overpass stalled past the 30-min job limit, Nextflow killed it, the gather never ran (metasmith again reported "run completed"). 0.7 adds a 20-min fetch deadline (`_run` in fetch.py; unanswered AOIs → DeadlineExceeded, not retried; `os._exit` so stuck threads cannot hold the process). Test: 5 s deadline returns 11 DeadlineExceeded rows in 5.0 s. First test version also hit CancelledError from `cancel_futures` (fixed: `done() and not cancelled()`). Killed the orphaned Overpass container from the failed run. Nibi scratch `/scratch/phyberos/geoquery` deleted and its msm relay stopped at Tony's request.
- 2026-11-19 22:05 — Engine bugs posted to metasmith/engine/dev at Tony's request (4 posts: array=100 twins, false run-completed, leaf id not re-minted, serial remote imports). PHAC units: subagent found no official unit statement; magnitudes (median ~20) and secondary sources (Laval report, PEI page) point to gene copies/mL, low confidence. Filled `copies/mL` for phacww_viral_load_latest, _7day_avg_latest, _7day_avg_p90_1yr in feature_units.tsv (uncommitted; not in the 0.7 image). After the 0.7 run lands, patch SI_unit for these 3 features in the published tree before dvc add. README caveats: unit inferred; adapter averages solid/liquid/mixed fractions (solids may be per g); PHAC CSV ends 2024-06-28.
- 2026-11-19 22:35 — PHAC units confirmed from primary source: PHAC's dashboard script (archived 2024-06-07 wastewater.js, lines 1053-1054 and 1673-1675) plots viral_load and seven_day_rolling_avg under the y-axis label "Viral load (copies/mL)" for covN2, not normalised, one label for all fractions. `copies/mL` stays in feature_units.tsv. Remaining README caveat: solid-fraction rows are labelled copies/mL by PHAC too, unverified physically. The Claude Science workbench was unresponsive (daemon listens on 12203 but HTTP hangs; same DB-timeout pattern as its 2026-11-12 log); routed around with a general-purpose agent.
- 2026-11-19 23:05 — 0.7 run 01cwQadP (3135 s) landed: 56 tables (wqp now correctly not_applicable, so 57→56), 19 empty sources (13 US-only + wqp + aafc_aci, acs5_api, mapillary_kartaview etc.), 6,082 values, 238 gaps = 825 − 587 found pairs, no duplicates. Deadline worked: osm_overpass DeadlineExceeded on AOIs 7, 8, 9, 10. Reviewer checks c1–c6 clean apart from the known family display-name mismatch. PHAC units patched into a copy of the results (tmp/patched01, script tmp/scripts/patch_units.sh: awk on the phac source table, re-aggregate in geoquery:0.7, republish). DVC-pinned in 55c14a39 (md5 0d89e302…, 66 files, 2.25 MB), pushed; dvc push task df818d55 (58,411 objects, 1 skipped absent). TODO in T9/T10: generated README caveats (PHAC unit inferred from dashboard label; solids fraction; PHAC CSV ends 2024-06-28) — republish then re-pin.
- 2026-11-19 23:40 — T9: dropped the nibi host path from the driver and docker/geoquery/to_hpc.sh; publish() prints per-source caveats (PHAC unit source, solid fraction, CSV end 2024-06-28); republished over tmp/patched01 without a rerun (only README and ledger changed, +260 bytes) and re-pinned (0a977204). Left pipeline.py as is: pc_item's short retry overlaps fetch.py's backoff, but changing it needs a new image and run for no data gain. Deleted docker images geoquery:0.2–0.6. T10: poc/README.md rewritten for the driver, image and engcyc layout (9bde6c47). T11: mvp-build closed in DECISIONS.md and STACK.md; docs/geospatial_query/PLAN.md removed in favour of this copy.

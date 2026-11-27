# geospatial_query discussion stack

Goal: build the MVP POC promised to Koonkie and run it on their test AOIs.
Decisions: docs/geospatial_query/DECISIONS.md

Email content stays out of this repo, journals and commits. Koonkie's AOI KML is data under no NDA and is DVC-tracked (aoi-handling). Source: the KML Koonkie sent on 2026-11-12.

## Current
fixes-plan: executing data/geospatial_query/plans/03-dictionary-fixes.md (todo list mirrors T0-T17). T0-T5 landed (e3e9d8d7 core; 25b906cf, 3a9e0ee4, fd211751, 0ac60b6f, 592758bd adapters; 017014bc lane reports in research/geospatial_query/fixes/). Next T7 image.

## Waiting on the user
- census-db: StatCan 2021 dissemination-block files sit behind a Cloudflare challenge; census runs on dissemination areas until the block files are staged (2021_92-151_X.zip, ldb_000b21a_e.zip).
- rvdss-regions: outside QC/ON/BC PHAC publishes only SARS-CoV-2 at province level; add region-level virus rows?
none

## Preferences
- 2026-11-17: discuss before building; drafts go to the user before anything is sent
- 2026-11-19: pipeline ingests one AOI table and writes one table per source; metasmith scatter-gather DAG; dataset id mmap_canada_mining
- 2026-11-27: feature definitions come from the original sources, quoted with a URL, or stay blank; never written by us
- 2026-11-27: the driver takes a Nextflow config and params; Tomer writes his own config, ours ships only as an example

## Stack
- fixes-plan: a formal plan for the dictionary-pass fixes (feature_descriptions_issues.md), written in plan mode
- deliverable: send Tomer the rerun dataset (fir run bZZhVN3O), feature_descriptions.csv, the driver on archive/geoquery-tomer-0.26.1 and metasmith 0.26.2
- probed-features: adapters for the 152 probed features
- query-time: build Query.time after the fixes; selects time-indexed editions per source (decision source-pinning)
- postgis-loader: whether the tool ships a PostGIS loader
- packaging: standalone open-source CLI vs metasmith workflow
- osm: get OpenStreetMap working (Tomer asked); the fir rerun found osm_overpass for 6 more AOIs, check before closing
- country-coverage: parked unless Tomer raises it again
- claude-plan: follow up with Hallam (not build work)

## Workers
none

## Closed
- class-column (2026-11-27)
- feature-renames (2026-11-27)
- source-pinning (2026-11-27)
- ghsl-urban (2026-11-27)
- rvdss-aggregate (2026-11-27)
- phac-wastewater (2026-11-27)
- census-multi-csd (2026-11-27)
- reply-tomer (2026-11-17)
- aoi-handling (2026-11-19)
- mvp-build (2026-11-19)

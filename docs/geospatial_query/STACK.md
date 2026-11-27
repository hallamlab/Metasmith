# geospatial_query discussion stack

Goal: build the MVP POC promised to Koonkie and run it on their test AOIs.
Decisions: docs/geospatial_query/DECISIONS.md

Email content stays out of this repo, journals and commits. Koonkie's AOI KML is data under no NDA and is DVC-tracked (aoi-handling). Source: the KML Koonkie sent on 2026-11-12.

## Current
rvdss-aggregate: what the RVDSS respiratory-virus features describe and how they aggregate

## Waiting on the user
none

## Preferences
- 2026-11-17: discuss before building; drafts go to the user before anything is sent
- 2026-11-19: pipeline ingests one AOI table and writes one table per source; metasmith scatter-gather DAG; dataset id mmap_canada_mining
- 2026-11-27: feature definitions come from the original sources, quoted with a URL, or stay blank; never written by us
- 2026-11-27: the driver takes a Nextflow config and params; Tomer writes his own config, ours ships only as an example

## Stack
- fixes-plan: a formal plan for the dictionary-pass fixes (feature_descriptions_issues.md), written in plan mode
  - rvdss-aggregate: tests and detections summed across viruses
  - ghsl-urban: GHSL class names, where "urban" starts
  - source-pinning: pin source versions or fetch latest
  - feature-renames: confirm renaming features is fine before release
- deliverable: send Tomer the rerun dataset (fir run bZZhVN3O), feature_descriptions.csv, the driver on archive/geoquery-tomer-0.26.1 and metasmith 0.26.2
- probed-features: adapters for the 152 probed features
- query-time: build Query.time after the MVP
- postgis-loader: whether the tool ships a PostGIS loader
- packaging: standalone open-source CLI vs metasmith workflow
- osm: get OpenStreetMap working (Tomer asked)
- country-coverage: parked unless Tomer raises it again
- claude-plan: follow up with Hallam (not build work)

## Workers
none

## Closed
- phac-wastewater (2026-11-27)
- census-multi-csd (2026-11-27)
- reply-tomer (2026-11-17)
- aoi-handling (2026-11-19)
- mvp-build (2026-11-19)

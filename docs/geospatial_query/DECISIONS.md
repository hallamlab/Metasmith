# geospatial_query decisions

## reply-tomer (2026-11-17)
- Question: how to answer Tomer's open questions on the Evidence column and Postgres.
- Decision: the user wrote and sent the reply. It explains the Evidence column, states no prior opinion on Postgres, accepts the KML AOIs as the test set and says the pipeline is in progress.
- Landed: sent by email 2026-11-17.

## aoi-handling (2026-11-19)
- Question: where Koonkie's AOI file and the run outputs live.
- User: "it is data and im not under non disclosure. dvc track it?" and "I want the same layout for these AOIs" as the cyanoverse and engcyc datasets.
- Decision: the KML and outputs go in the metasmith data view under data/geospatial_query/<dataset>/, DVC-tracked, in the engcyc LAYOUT.md grammar.
- Landed: data/geospatial_query/plans/01-mvp-build.md tasks T2, T3, T6, T7.

## mvp-build (2026-11-19)
- Question: what the MVP POC is and how it runs on Koonkie's AOIs.
- User: "the pipeline should ingest 1 table of AOIs and produce tables per source" and "can I see the metasmith DAG as part of the plan?"
- Decision: a metasmith scatter-gather (normalise_aois, fetch_source per source, aggregate) over 75 sources, in one docker image built from the POC package. Runs are local docker only; the nibi path was dropped. The driver publishes into data/geospatial_query/<dataset>/ in the engcyc layout, DVC-tracked.
- Landed: plan data/geospatial_query/plans/01-mvp-build.md. Dataset mmap_canada_mining (run 01cwQadP, geoquery 0.7): 56 source tables, 587 found and 238 gap pairs over 11 AOIs. Pin 0a977204, README research/geospatial_query/poc/README.md.

## census-multi-csd (2026-11-27)
- Question: what a census value means for an AOI that touches several census subdivisions; statcan_csd reported whole-CSD values, area-weighted across CSDs.
- User: "you take the area cross-section of the AOE against the CSD, and that is the value contribution from the CSD. Then you sum all the CSDs" and, after the Wood Buffalo / Elliot Lake numbers, "Yeah, that's good."
- Decision: additive areal weighting, run on the finest StatCan units instead of CSDs: dissemination blocks for population and dwellings, dissemination areas for households and income. Counts sum each unit's overlap share; medians and averages (income, dwelling value) become a household-weighted mean across contributing areas, labelled approximate. ghs_pop stays as an independent cross-check. Confirm DB counts are downloadable before building.
- Landed: fixes-plan (pending).

## phac-wastewater (2026-11-27)
- Question: what the PHAC wastewater features describe; they averaged every plant in the AOI's province on its latest date, mixing liquid and solid fractions, so Baie-James got Montreal's sewage.
- User: "stale is fine. It's just the nearest time that we can get", "all we have to do is just also add a unit column. We should do that anyways for any column with variable units", "We just report the nearest plant and then the distance. Some downstream curation step will then probably need to match the plant to its [sewershed] ... That's some processing that is really not our concern."
- Decision: replace the provincial average with the nearest monitored plant: its latest value and 7-day average, plant name, distance in km and fraction. No distance cutoff. Never average across fractions. Keep the 2024-06-28 snapshot. General rule: every value carries its unit; a feature whose unit varies across AOIs gets a `<feature>_unit` companion column in features_wide. PHAC's unit is written as PHAC labels it, with the solid-fraction caveat.
- Landed: fixes-plan (pending).

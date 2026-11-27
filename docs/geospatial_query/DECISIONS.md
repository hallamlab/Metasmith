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

## rvdss-aggregate (2026-11-27)
- Question: what the RVDSS features describe and whether their aggregation is right; RVDSS is published only by province (or region).
- User: "It looks to me that some class data is put into a label that should probably be made into a column with classes instead", "you're sure these are just bugs? ... Okay, seems good."
- Decision: keep RVDSS as the province's values, labelled by `rvdss_jurisdiction` and native_res_m admin:province. Report each virus exactly as PHAC publishes it (tests, detections, percent positive), matched by exact virus name. Drop the cross-virus sums of tests and detections. The virus becomes a class column rather than part of the feature name; the general rule is topic class-column.
- Landed: fixes-plan (pending).

## ghsl-urban (2026-11-27)
- Question: geoquery's GHS-SMOD class labels ("dense town", "very dispersed rural") differ from GHSL's published names, and `ghssmod_is_urban` is a geoquery-made 50% rule.
- User: "This is a serious issue. I agree on drop. For the changed names, we should do like a full scan and make sure we're using the actual original published names or categories. At least for the data output. We do, I think, make simplified names for the transforms, and that's fine."
- Decision: use GHSL's published class names. Report GHSL's first-level groups (rural 11-13, urban cluster 21-23, urban centre 30) in place of `ghssmod_urban_frac`; class 21 is inside Urban Clusters by GHSL's own definition, so the >=21 rule stands under its proper name. Drop `ghssmod_is_urban`. Rule for every source: class and category values in the data outputs are the source's own published names, verified by a full scan of all sources; simplified names stay allowed in transform code and feature identifiers.
- Landed: fixes-plan (pending).

## source-pinning (2026-11-27)
- Question: whether sources fetch a pinned version or the latest; a dozen pick "newest at run time" unrecorded, others are frozen on old editions (census 2016).
- User: "The data just needs to line up with what's available online", "You don't want to fetch the latest by default. You want to fetch by the timeline, as part of the query. Can we do version per source ... a separate versions table?", then "The proposed design looks correct."
- Decision: query time and release version are separate. Query time (topic query-time) selects time-indexed editions (NPRI year, census year, scenes, CAMS window); "latest" is only the default query time, now. Release versions (Overture release, GPPD commit, GHSL R2023A, Hansen v1.13) default to the latest release. Now, in fixes-plan: a `_logs/versions.csv`, one row per source (query time requested, data time returned, release or edition used, URL fetched), and editions frozen in code move to the current one (census 2021). Query time itself is built after the fixes.
- Landed: fixes-plan (versions table); query-time topic (time selection).

## feature-renames (2026-11-27)
- Question: whether the fixes may rename features, and whether a name or its computation changes when they disagree.
- User: "We're still in dev, so renaming everything is fine, as long as the final package is internally coherent. It depends on who does the computation and where does the name come from. Essentially, like, the source wins ... if it's named incorrectly, well, we got to match the source. And if the computation isn't what the source expects, then, well, we match the source."
- Decision: rename freely; no old-to-new mapping table while in dev, but the package (adapters, feature_units.tsv, feature_descriptions.csv, README, docs) must agree with itself. The source wins: a source-provided value takes the source's name and the source's computation. A geoquery-computed feature is named for exactly what geoquery computes.
- Landed: fixes-plan (pending).

## class-column (2026-11-27)
- Question: class values (virus, land-cover class, substance, pollutant, facility type) are baked into feature names; where should a class column apply, and what shape does the wide table take.
- User: "some class data is put into a label that should probably be made into a column with classes instead" (rvdss-aggregate), then "Option A it is then."
- Decision: source tables and observations_long gain a `class` column; `feature` names the measure and `class` holds the source's published class name, blank for unclassed features. Applies to every source that publishes a class list, found by the published-names scan. features_wide stays one row per AOI, with columns named `feature[class]`. Class definitions go in a sourced `class_descriptions.csv`; feature_descriptions.csv becomes one row per measure.
- Landed: fixes-plan (pending).

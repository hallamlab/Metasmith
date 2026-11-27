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

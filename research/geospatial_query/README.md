# geoquery: AOI table in, one table per source out

## Purpose & Contents

This file explains how to run geoquery and how its adapters behave. It holds the run command, the image, the design rules that every adapter honours, the steps to add an adapter, and the known limits of the data. The output tree follows the engcyc dataset grammar in `research/engcyc/LAYOUT.md` (branch `feat/engcyc/data`). Each published dataset's own `README.md` describes its files and columns.

geoquery takes one table of areas of interest (KML or GeoJSON polygons) and queries 74 public open-data sources for each AOI. Every adapter is plain HTTPS against a public endpoint, with no credentials, API keys or vendor SDKs.

## Run

The pipeline is the shipped metasmith template `geoquery_from_aoi_table`, a scatter-gather. `normalise_aois` runs once, `fetch_source` runs once per source, and `aggregate` gathers every source table. The types and transforms live in the standard library under `src/metasmith_libraries/`. https://metasmith.readthedocs.io/en/latest/workflows/geoquery.html describes the template.

1. Install metasmith: `conda create -n msm -c conda-forge -c bioconda -c hallamlab metasmith=<version>`.
2. Get the driver. It is not in the conda package. Clone https://github.com/hallamlab/Metasmith (branch `release`) or download `research/geospatial_query/geoquery_run.py` from it.
3. Run the driver with the env's python: `python geoquery_run.py --aois <table.kml|table.geojson> --out <dataset_dir>`.
4. In the lab workspace, track the published folder with `dvc add` and push it with the `dvc` tool.

Add `--plan-only` to plan and stop. Use `--sources a,b` to run a subset. Run `--help` for the rest. By default the driver runs every step in local Docker.

- The first plan compiles the transform library into `<work>`. It can sit silent for several minutes. Later plans take seconds.
- The driver logs nothing while the steps run. Follow progress in `<work>/msm_home/runs/<plan>/_metasmith/logs.latest/nxf_trace.tsv`.
- One small AOI across all sources takes about an hour on the `local` preset, which runs at most 4 steps at once in 8 GB. To raise those limits, pass your own Nextflow config file to `--config` with larger `executor.queueSize` and `executor.memory`.

To run elsewhere, give the driver a metasmith agent home (`--home`, local or `ssh://`), a container runtime (`--runtime`), and a Nextflow config (`--config`, a metasmith preset or your own file) with its params (`--params`). `examples/slurm.yml` is the params file for one Slurm cluster. Treat it as an example and write your own for your cluster.

The dataset ships `feature_descriptions.csv` and `class_descriptions.csv` from this folder. Each definition is quoted verbatim from the source's documentation with its URL, or left blank with the reason. Never write a definition by hand. Check every quote with `python verify.py <dictionary.csv> <report.csv>` before you publish.

**WARNING** The driver deletes `<dataset_dir>` before it writes the new one.

**CAUTION** For an image change, build with `docker/geoquery/dev.sh -p`. It pins `geoquery.env` to the new tag and digest. Metasmith keys each cached step on that file, so an unpinned rebuild reuses the old image's results.

`fetch_source` gives each source 20 minutes. AOIs still waiting at the deadline get a `DeadlineExceeded` gap row, so one stalled API cannot stop the run.

## Design

**Slots, not sources.** A *slot* is a semantic layer (`land_cover`, `bedrock_geology`,
`power_plants`); a *source* is one provider that can fill it. Several sources may compete for a
slot, and the same source can fill different slots in different jurisdictions. The registry of
adapters is the asset, not the code around it.

**Absence is declared, never silent.** Every (AOI, source) pair resolves to exactly one of three
states, and all three are written to the output:

- `found` — data returned, with provenance
- `unavailable` — the source was asked and could not answer (HTTP error, network block, auth wall,
  empty window); the verbatim reason is kept
- `not-attempted` — the source does not cover this AOI by construction (a US-only layer for a
  Canadian AOI). Raised as `PermissionError`, recorded, never retried.

`coverage.csv` uses these three states. `_logs/gaps.csv` gives the finer reason: `not_applicable` for not-attempted, and `no_hits` or `failed` for unavailable.

**A 200 is not verification.** Every geometry-filtering endpoint was tested against a nonsense
control box in the mid-Pacific; a source returning the same answer for a real AOI and the control is
not filtering and was not shipped. (MGnify returns HTTP 200 and well-formed JSON for unrecognised
query parameters while silently serving the unfiltered catalogue; EPA Envirofacts ignores lat/long
operators and returns byte-identical payloads. Both were caught this way.)

**Fill types.** Every value is labelled `observed`. The design reserves `inferred` (model output,
e.g. an embedding decode) and `analogue` (a value borrowed from an environmentally-matched donor
AOI); neither is implemented here, and neither may ever be silently substituted for an observation.

## Writing an adapter

```python
def _a_mysource(aoi):
    """aoi: dict with aoi_id, country, area_km2, geom (GeoJSON), bbox, centroid"""
    arr = read_window(href, aoi["bbox"], aoi["geom"])        # decimated, AOI-clipped COG read
    return stats(arr, "mysource_band"), {"obs_date": "2024-06-01", "res_m": 30}

ADAPTERS = [("slot_name", "source_id", "family", "CC-BY-4.0", "none", _a_mysource)]
```

Rules:

- Return `(features, meta)`. A feature value is a plain value, a `{"value", "unit"}` record when the unit varies between AOIs, or a list of `{"class", "value"}` records for a measure reported per category.
- Use the source's own published names for class values. Name a geoquery-computed feature for exactly what it computes.
- Put `obs_date`, `res_m`, `item` (the tile or record read), `release` (the edition) and `url` (the address queried) in `meta`. `release` and `url` feed `_logs/versions.csv`.
- Prefix each feature name with its source. Keep names globally unique.
- Raise `PermissionError` for out-of-coverage and any other exception for failure. Never return an empty dict. Drop the module in as `adapters_<name>.py` and add it to `LANES` in `registry.py`. Add the source's family and observation type to `sources.tsv` and each feature's unit to `feature_units.tsv`. Rebuild the image.

Helpers in `pipeline.py`: `read_window` (decimated AOI-clipped read, masks only *declared* nodata),
`stats`, `class_fractions`, `pc_item` (Planetary Computer STAC search with backoff), `pc_sign`.

## Licence tiers

Features carry their source licence so a query can constrain the acceptable set. Licences include
public domain (US federal), OGL-Canada-2.0, CC-BY-4.0, ODbL-1.0 (OpenStreetMap-derived,
**share-alike**), BC OGL, CC0, CDLA-Permissive and the Copernicus DEM EULA. ODbL and share-alike sources are tagged and must not be mixed into a permissively-licensed
derivative without checking.

## Known constraints

- **Some features are joined at the publisher's unit, not measured on the AOI.** CDC NNDSS
  (state), CDC NWSS (county) and RVDSS (province) carry `res_m='admin:<level>'`. StatCan 2021
  census counts are weighted by each dissemination area's overlap with the AOI. Their medians
  and averages are household-weighted approximations. PHAC wastewater reports the nearest plant
  and its distance.
- **Several raster sources read one tile** when an AOI spans tile edges.
- **The public Overpass mirrors time out under load.** Rerun `osm_overpass` alone when it fails.
- **US income and age structure are missing**: the Census ACS API requires a key, so US demography
  comes from the TIGERweb 2020 decennial counts instead. No key is embedded anywhere.
- **Three slots have no keyless route**: `ground_level_imagery` (Mapillary and KartaView are both
  token-gated), `tailings_facilities` via the Global Tailings Portal (login-gated; a published
  shapefile substitute is used), and NSIDC cryosphere products (Earthdata Login; NOAA-hosted
  equivalents are used).
- **Partial-coverage sources report honestly**: `field_boundaries` is US-only (NASS Crop Sequence
  Boundaries), `embeddings` covers 5 of the 15 test AOIs (`../examples/test_aois.geojson`) (Clay v0 MGRS tiles), `gamma_radiometrics` covers 3
  of 15 (genuine survey gaps in the North American grid), `glaciers` is Natural Earth 1:10M and is
  provisional below ~10 km.
- **Raster features are decimated samples**, capped at 320 px per side (`pipeline.MAXPIX`), not every
  native pixel.

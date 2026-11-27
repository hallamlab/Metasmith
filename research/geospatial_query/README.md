# geoquery: AOI table in, one table per source out

## Purpose & Contents

This file explains how to run geoquery and how its adapters behave. It holds the run command, the image, the design rules that every adapter honours, the steps to add an adapter, and the known limits of the data. The output tree follows the engcyc dataset grammar in `research/engcyc/LAYOUT.md` (branch `feat/engcyc/data`), and each published dataset's own `README.md` describes its columns.

geoquery takes one table of areas of interest (KML or GeoJSON polygons) and queries 75 public open-data sources for each AOI. Every adapter is plain HTTPS against a public endpoint, with no credentials, API keys or vendor SDKs.

## Run

The pipeline is the shipped metasmith template `geoquery_from_aoi_table`, a scatter-gather. `normalise_aois` runs once, `fetch_source` runs once per source, and `aggregate` gathers every source table. The types and transforms live in the standard library under `src/metasmith_libraries/`. https://metasmith.readthedocs.io/en/latest/workflows/geoquery.html describes the template.

1. Install metasmith: `conda create -n msm -c conda-forge -c bioconda -c hallamlab metasmith`.
2. Run the driver: `python geoquery_run.py --aois <table.kml> --out <dataset_dir>`.
3. Track the published folder with `dvc add` and push it with the `dvc` tool.

Add `--plan-only` to plan and stop. Use `--sources a,b` to run a subset. Run `--help` for the rest. The driver runs every step in local Docker. Three small AOIs across all 75 sources take about 70 minutes on 0.26.1.

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

Rules: return flat scalars only, globally unique feature names prefixed by source, raise
`PermissionError` for out-of-coverage and any other exception for failure, never return an empty
dict. Drop the module in as `adapters_<name>.py` and add it to `LANES` in `registry.py`. Add the source's family and observation type to `sources.tsv` and each feature's unit to `feature_units.tsv`. Rebuild the image.

Helpers in `pipeline.py`: `read_window` (decimated AOI-clipped read, masks only *declared* nodata),
`stats`, `class_fractions`, `pc_item` (Planetary Computer STAC search with backoff), `pc_sign`.

## Licence tiers

Features carry their source licence so a query can constrain the acceptable set. Across the 69
working adapters: 22 public-domain (US federal), 16 OGL-Canada-2.0, 14 CC-BY-4.0, 8 ODbL-1.0
(OpenStreetMap-derived, **share-alike**), plus BC OGL, CC0, CDLA-Permissive and the Copernicus DEM
EULA. ODbL and share-alike sources are tagged and must not be mixed into a permissively-licensed
derivative without checking.

## Known constraints

- **Admin-aggregate features are not AOI measurements.** CDC NNDSS (state), CDC NWSS (county),
  PHAC (province) and StatCan census attributes (census subdivision) are joined at the publisher's
  jurisdiction, flagged with `res_m='admin:<level>'` and a jurisdiction prefix in the feature name.
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

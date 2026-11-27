# core: REPORT

Files changed: `research/geospatial_query/geoquery/pipeline.py`, `adapters_s2signature.py`, `adapters_embeddings.py`. `run_one` and `load_aois` are byte-identical. Scratch: `/home/tony/.claude/jobs/07dc4bd7/tmp/t2/core/` (run.sh, per-source CSV/log, probe/cmp.py comparison, s2sig_reference_v3.json).

New shared helpers in pipeline.py: `pc_meta` (release/url/item/obs_date for every PC STAC adapter; obs_date from the item's own start/end or datetime), `legend` (class names read from the STAC asset's `classification:classes` / `file:values`), `class_fractions`/`_fractions` (classed list output), `s2_offsets` (BOA_ADD_OFFSET from the product-metadata XML), `buffered` + `n_within` (true km buffer around the AOI polygon in an azimuthal-equidistant projection, then a point-in-polygon count), `_polys` (2-D AOI polygons, simplified only above 200 vertices). Needs `pyproj` and `shapely>=2` (both in the 0.26.0 image).

## 1. Dispositions

geo_climate_water
- clim_coldest_day_c / clim_hottest_day_c are min/max of the daily mean: renamed -> `clim_daily_mean_temp_min_c` / `clim_daily_mean_temp_max_c`.
- clim_annual_precip_mm divides by hard-coded 10: fixed, divides by the actual years of data (non-null days / 365.25). Values move by <=0.1 mm.
- Open-Meteo documents no daily `temperature_2m_mean`: not a bug; the API serves it (3653 values per AOI). Dictionary matter.
- Macrostrat t_age/b_age undocumented: fixed. Emits the documented `t_int_age`/`b_int_age` as `geo_t_int_age_ma`/`geo_b_int_age_ma`. First unit only: kept (API returns the largest map scale first); new `geo_map_scale` records the scale. geo_n_units renamed -> `geo_n_map_units_at_centroid`. Removed `[:80]`/`[:60]` truncation.
- slope_deg_* cos(latitude): fixed. x spacing = 111320*cos(lat)*dlon/cols; both extents now from the AOI bbox clipped to the tile actually read (old code divided the whole AOI extent by the tile-clipped row count). aoi_1 slope mean 0.47 -> 1.41 deg.
- mrdem_30 asset unspecified: fixed. Reads `dtm` only, renamed `mrdem_elev_m_*` -> `mrdem_dtm_elev_m_*`, item `mrdem/dtm`. Values unchanged.
- blank definitions for geoquery-computed features: not a code bug; dictionary work.
- eq count capped / HTTP 400 above 20000: fixed via FDSN `count`; max magnitude via `query?orderby=magnitude&limit=1` when count > 0. Values identical.
- eq_max_mag mixes magnitude types: not a bug; ComCat's preferred magnitude per event. Flag in dictionary.
- sources.tsv "fixed sensor" for usgs_fdsn: not my file.
- osm_* over bbox: fixed, Overpass `poly:` filter on the AOI polygon (outer ring of each part), POST. `osm_building_per_km2` renamed -> `osm_building_ways_per_km2`. Only ways counted, as the names say (nodes/relations: not a bug).

latent_spectral
- aef_emb_tile_path truncated in in/latent.tsv: not a code bug (CSV has the full path). meta `item` is now the full relative tile path.
- aef_emb_l2_norm misleads: renamed -> `aef_emb_mean_vector_l2_norm`; `aef_emb_l2_norm_pixel_mean` -> `aef_emb_pixel_l2_norm_mean`.
- aef_emb_component_std / component_absmax: removed (derivable from a00..a63; name implied spatial spread).
- aef_emb_pixel_count decimated: renamed -> `aef_emb_px_sampled`. aef_emb_n_tiles capped at 4: renamed -> `aef_emb_n_tiles_read`.
- aef_emb_a00..a63 not unit length: not a bug.
- lst_c_* degC vs Kelvin: fixed per "units follow the source": `lst_k_*` in K; scale/offset from raster:bands, no hard-coded fallback.
- palsar epoch varies: not a bug; obs_date records it. CF now read from item `cf` ("83.0 dB"); values unchanged.
- s1_vv_db_mean one scene: not a bug (name claims no composite; obs_date/item record the scene).
- s2_* no BOA_ADD_OFFSET: fixed. Baseline >= 04.00 -> per-band BOA_ADD_OFFSET from product-metadata XML (all -1000, baseline 05.11), (DN+offset)/10000. Indices masked where a band's BOA reflectance <= 0 (else NDVI explodes; first run gave mean -1551). Red -0.100 exactly; NDVI ~x2.
- s2_cloud_cover_pct is scene eo:cloud_cover: renamed -> `s2_eo_cloud_cover_pct`.
- s2 ndvi/ndwi n_px differ: not a bug (independent masks).
- s2sig in slot "embeddings" / quantile-grid indices: not a bug (documented design; slot is the analogue join key).
- s2sig no offset: fixed (same `s2_offsets`; DN < 16000, then reflectance > 0). Whitening reference REF_MU/REF_SD rebuilt since it was measured on uncorrected values: 28 random 0.1-degree boxes on North American land (seed 11, centres -125..-65 E, 30..60 N, boxes with ndwi_med >= 0 rejected), computed by the adapter itself. Boxes in scratch `s2sig_reference_v3.json`.
- s2sig_year always 2024 / version / ref_n / dim: removed; obs_date = first/last scene date; release `s2sig-v3 (sentinel-2-l2a, BOA offset applied)`.
- s2sig_wNN index must track sort: renamed -> `s2sig_w_<component>`. Item list no longer truncated at 200 chars.

macrobiology
- hgb Mg C/ha + scale 0.1: fixed. PC COG is UInt16, scale 1.0, no nodata/unit (rasterio); raw 0..981. ORNL guide p.3-4: "Scaling: 0.1", "Data units: Mg C/ha", "No data value: 65536" (65535 masked). Renamed `agb_mgha_*` -> `agb_carbon_mgc_ha_*`; values /10 (aoi_1 336.9 -> 33.7).
- hgb asset-to-file link: not a bug (collection is the ORNL dataset; scale behaviour confirms).
- modis_n_composites float example: not an adapter bug (value_num formatting).
- gbif capped at 1200 + bbox: fixed. AOI polygon WKT (anticlockwise), facetOffset paging by 1000. aoi_1 1200 -> 1709.
- nrcanlc labels/pairing: fixed. Moved to NRCan datacube STAC `landcover`, newest `landcover-2020` (PC holds only 2015). Classes verbatim from NRCan 2020 Class Index.
- nrcanlc urban high: not a code bug; 2020 differs from 2015 anyway.
- wc_* obs_date: fixed from item; release `esa-worldcover 2.0.0`. No mangroves: not a bug.
- modis sampling/obs_date: fixed. Tile under the centroid, Terra only, every 16-day composite of the last 365 days (23), fill -3000 masked, scale from raster:bands, obs_date = first/last composite. Amplitude now full-year max-min. Values change (aoi_1 mean 0.69 -> 0.39, winter included).
- jrc_gsw obs_date "1984-2021": fixed (1984-03-01/2020-12-31); release `jrc-gsw v1.3 (2020)`.

public_health_microbiology
- ena/mgnify "25km" was 0.25 deg pad: fixed with a real 25 km polygon buffer; query on its bbox, count returned lat/lon inside. Names kept. ENA aoi_2 988 -> 1004, aoi_9 190 -> 164; MGnify aoi_2 59 -> 65, aoi_4 6 -> 4.
- mgnify_control_count: removed with the control request and its ValueError gate.
- ENA limit=0 truncation: fixed in effect (count is our own count of returned coordinates; aoi_1 5253 vs portal count 5252 for the box).

## 2. Units

```
feature	SI_unit
s2_eo_cloud_cover_pct	%
lst_k_mean	K
lst_k_std	K
lst_k_p10	K
lst_k_p90	K
lst_k_n_px	count
mrdem_dtm_elev_m_mean	m
mrdem_dtm_elev_m_std	m
mrdem_dtm_elev_m_p10	m
mrdem_dtm_elev_m_p90	m
mrdem_dtm_elev_m_n_px	count
geo_t_int_age_ma	Ma
geo_b_int_age_ma	Ma
geo_map_scale	
geo_n_map_units_at_centroid	count
wc_frac	1
nrcanlc_frac	1
cdl_frac	1
aafc_aci_frac	1
aafc_aci_dominant	
aafc_aci_n_classes	count
agb_carbon_mgc_ha_mean	Mg C/ha
agb_carbon_mgc_ha_std	Mg C/ha
agb_carbon_mgc_ha_p10	Mg C/ha
agb_carbon_mgc_ha_p90	Mg C/ha
agb_carbon_mgc_ha_n_px	count
clim_daily_mean_temp_min_c	degC
clim_daily_mean_temp_max_c	degC
osm_building_ways_per_km2	1/km2
aef_emb_n_tiles_read	count
aef_emb_px_sampled	count
aef_emb_mean_vector_l2_norm	1
aef_emb_pixel_l2_norm_mean	1
s2sig_w_<component>	1
```
(`s2sig_w_<component>`: 30 rows, one per REF_NAMES entry, e.g. `s2sig_w_b02_med`.)

Removed: `lst_c_*` (5), `mrdem_elev_m_*` (5), `agb_mgha_*` (5), `geo_age_top_ma`, `geo_age_bottom_ma`, `geo_n_units`, `clim_coldest_day_c`, `clim_hottest_day_c`, `osm_building_per_km2`, `s2_cloud_cover_pct`, `mgnify_control_count`, `aafc_crop_class_code`, all `wc_frac_*` (11), `nrcanlc_frac_*` (15), `cdl_frac_*` (19), `aef_emb_dim`, `aef_emb_year`, `aef_emb_n_tiles`, `aef_emb_pixel_count`, `aef_emb_primary_window_native_px` (= win_width*win_height), `aef_emb_l2_norm`, `aef_emb_l2_norm_pixel_mean`, `aef_emb_component_std`, `aef_emb_component_absmax`, `aef_emb_tile_utm_zone` (duplicate of tile_crs), `aef_emb_win_orientation` (constant), `s2sig_w00_*..s2sig_w29_*` (30), `s2sig_dim`, `s2sig_year`, `s2sig_version`, `s2sig_ref_n`.

`wc_dominant`, `nrcanlc_dominant`, `cdl_dominant` keep their names but now hold the published class name ("Tree cover", not "tree").

## 3. Classes

- `wc_frac`: Tree cover, Shrubland, Grassland, Cropland, Built-up, Bare / sparse vegetation, Snow and ice, Permanent water bodies, Herbaceous wetland, Mangroves, Moss and lichen. Read at run time from the PC asset `classification:classes`; matches https://esa-worldcover.s3.eu-central-1.amazonaws.com/v200/2021/docs/WorldCover_PUM_V2.0.pdf
- `nrcanlc_frac`: Temperate or sub-polar needleleaf forest, Sub-polar taiga needleleaf forest, Temperate or sub-polar broadleaf deciduous forest, Mixed forest, Temperate or sub-polar Shrubland (capital S as published), Temperate or sub-polar grassland, Sub-polar or polar shrubland-lichen-moss, Sub-polar or polar grassland-lichen-moss, Sub-polar or polar barren-lichen-moss, Wetland, Cropland, Barren land, Urban and built-up, Water, Snow and ice. https://datacube-prod-data-public.s3.ca-central-1.amazonaws.com/store/land/landcover/ClassIndex_v2.pdf (open.canada.ca ee1580ab-a23d-4f86-a09b-79763677eb47). PC's 2015 labels differ ("Barren lands", "Urban", "Snow and Ice").
- `cdl_frac`: all CDL codes, names from the PC `usda-cdl` asset `classification:classes` (Corn, Soybeans, Grassland/Pasture, Developed/Low Intensity, ...). USDA legend: https://www.nass.usda.gov/Research_and_Science/Cropland/metadata/meta.php
- `aafc_aci_frac`: every code of AAFC's crop classification CSV (Cloud, Water, Exposed land/barren, Urban/developed, Greenhouses, Shrubland, Forest fire/burnt area, Wetland, Peatland, Grassland, crop classes, Forest (undifferentiated), Coniferous, Broadleaf, Mixedwood), read at run time from https://agriculture.canada.ca/atlas/data_donnees/annualCropInventory/supportdocument_documentdesupport/aci_crop_classifications_iac_classifications_des_cultures.csv (trailing space on "Forest fire/burnt area " stripped).

## 4. Sources

No source id changed. `aafc_aci` moved address (old `/atlas/rest/.../annual_crop_inventory_2023` 404s; all 11 failed in the shipped run): it reads the newest year listed under https://agriculture.canada.ca/imagery-images/rest/services/annual_crop_inventory (2025) and computes a class histogram over the AOI polygon (`computeHistograms`, pixel ~bbox/320 in Web Mercator, >= 30 m), replacing one centroid identify. `nrcan_landcover` moved from PC (2015) to NRCan's datacube STAC (2020).

| source | release | url |
|---|---|---|
| sentinel2_l2a | sentinel-2-l2a | https://planetarycomputer.microsoft.com/api/stac/v1/search |
| landsat_c2l2_st | landsat-c2-l2 | PC search |
| alos_palsar_mosaic | alos-palsar-mosaic | PC search |
| sentinel1_rtc | sentinel-1-rtc | PC search |
| cop_dem_glo30 | cop-dem-glo-30 | PC search |
| esa_worldcover | esa-worldcover 2.0.0 | PC search |
| usda_cdl | usda-cdl 2021 | PC search |
| hgb | hgb | PC search |
| jrc_gsw | jrc-gsw v1.3 (2020) | PC search |
| modis_13q1 | modis-13Q1-061 | PC search |
| mrdem_30 | mrdem-30 | https://datacube.services.geo.ca/stac/api/search |
| nrcan_landcover | landcover-2020 | https://datacube.services.geo.ca/stac/api/search |
| aafc_aci | annual_crop_inventory/2025 | https://agriculture.canada.ca/imagery-images/rest/services/annual_crop_inventory/2025/ImageServer |
| macrostrat | macrostrat api v2 | https://macrostrat.org/api/v2/geologic_units/map |
| usgs_fdsn | USGS ComCat FDSN event 1 | https://earthquake.usgs.gov/fdsnws/event/1/count |
| gbif | GBIF occurrence API v1 | https://api.gbif.org/v1/occurrence/search |
| mgnify | MGnify API v1 | https://www.ebi.ac.uk/metagenomics/api/v1/samples |
| ena_geo | ENA Portal API | https://www.ebi.ac.uk/ena/portal/api/search |
| osm_overpass | OpenStreetMap (live) | the Overpass endpoint that answered |
| wqp | WQX 2.2 Station | https://www.waterqualitydata.us/data/Station/search |
| era5_open_meteo | ERA5 (Open-Meteo archive, models=era5) | https://archive-api.open-meteo.com/v1/archive |
| alphaearth_v1_annual | aef v1 annual 2024 | https://data.source.coop/tge-labs/aef/v1/annual/aef_index.parquet (item = tile path) |
| s2_signature_v1 | s2sig-v3 (sentinel-2-l2a, BOA offset applied) | PC search (item = scene ids) |

`wqp` also uses a real 20 km polygon buffer now (was a 0.2 deg pad); name kept.

## 5. Test results (aois11; usda_cdl and wqp on us2.geojson)

- sentinel2_l2a 122 found. Red -0.100 (aoi_1 0.1539 -> 0.0539); NDVI mean ~x2 (0.298 -> 0.574); n_px drop 1-13% from the <=0 mask.
- landsat_c2l2_st 55. Kelvin; aoi_1 got a different least-cloudy scene (2024-08-23 vs 09-08; catalogue order).
- alos_palsar_mosaic 44; sentinel1_rtc 10 + 1 unavailable (aoi_7 no scene, as shipped): identical.
- cop_dem_glo30 88: elevation identical; slopes up (aoi_1 0.47 -> 1.41, aoi_2 2.17 -> 3.65).
- mrdem_30 55 identical. macrostrat 66: interval ages (aoi_1 100.5-143.1 Ma vs 110.5-111.75).
- esa_worldcover 108 identical fractions. nrcan_landcover 123: 2020 edition, fractions shift.
- aafc_aci 98 found, 4 unavailable (aoi_2, 3, 7, 10: LookupError "no ACI pixels", outside the mapped extent). Shipped: 11 failed.
- hgb 55 values /10. jrc_gsw 66 identical. modis_13q1 33, 23 composites each. gbif 22 (aoi_1 species 1709). era5_open_meteo 55. usgs_fdsn 16 identical.
- mgnify 11, ena_geo 11 (changes above). alphaearth_v1_annual 825, values identical. s2_signature_v1 693, raw band medians -0.1, new whitening.
- osm_overpass 30 found, 1 unavailable (aoi_3, 20-min deadline; Overpass busy). Shipped: 5 found, 6 failed. aoi_1 highways 5647 -> 6010 (live edits; AOIs are rectangles so the polygon filter changes little here).
- usda_cdl 38 (us_iowa 25 classes, us_nv 9): now read_best over the tiled 2021 CDL (old code read `it[0]`, an arbitrary tile). wqp 2 (926, 204 stations).

## 6. Open

- `geoquery/s2sig_reference.json` (not my file; nothing reads it) holds the stale v2 reference; replace with scratch `s2sig_reference_v3.json` or delete.
- usda_cdl: PC's collection stops at 2021, an outdated edition; moving to USDA's own service needs another access path. Not done (US-only).
- read_best reads one tile, so COP DEM, WorldCover, PALSAR, GSW, HGB and CDL statistics cover only the tile with most AOI pixels when an AOI straddles tiles (aoi_1 spans two COP tiles). Not in the leads; unchanged.
- Overpass and ACI use outer rings only (holes ignored); these AOIs have none.
- MODIS applies no pixel-reliability QA.
- Macrostrat states no unit for t_int_age/b_int_age; Ma assumed.
- No image packages missing.

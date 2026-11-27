# mmap_canada_mining rerun on geoquery 0.26.3: changes against the 0.26.1 dataset

## Purpose & Contents

This file explains every coverage change and every value change over 5% between the 0.26.1 dataset and the 0.26.3 rerun of the same 11 AOIs on fir. Renames and removed features are listed per lane in the sibling `<lane>.md` reports. Each change below names its cause: a fix, an edition change or live data.

## Coverage (AOIs found per source)

| source | 0.26.1 | 0.26.3 | cause |
|---|---|---|---|
| aafc_aci | 0 | 7 | fix: the old ArcGIS address returned 404. The adapter reads the 2025 inventory. The 4 misses are AOIs outside the crop-inventory extent. |
| gsc_surficial_1880a -> gsc_surficial_cgm195 | 11 | 11 | rename: the file read is Canadian Geoscience Map 195, not map 1880A |
| statcan_csd_2016 -> statcan_census_2021 | 11 | 11 | edition: the 2021 Census on dissemination areas |
| donneesquebec_role | 6 | removed | removed: it returned catalogue hits, not data about the AOI |
| phac_wastewater | 10 | 11 | fix: the nearest plant is reported, so no AOI lacks a value because its province has no plant |
| osm_overpass | 5 | see OSM | live data: the public Overpass mirrors time out under load |
| noaa_nsidc_sic_cdr | 11 | 11 | edition: CDR version 6. All 11 AOIs are land-masked, so concentration features are absent and `nsidc_sic_valid_cells` is 0. |

## Value changes over 5%

- Sentinel-2 (`s2_*`, `s2sig_*`): reflectance now applies `BOA_ADD_OFFSET`. Red reflectance falls to about 0.28 of before, and NDVI and NDWI roughly double.
- MODIS (`modis_*`): every 16-day composite of the last year (23) with the published scale, not one composite. NDVI mean rises about 1.9 times.
- `slope_deg_*`: pixel spacing now uses cos(latitude). Slopes rise about 1.2 times.
- Distances and buffers (`npri_*`, `ghgrp_*`, `fcsi_*`, `mgnify_*`, `ena_*`, `ovt_division_n_in_radius`): measured from the AOI polygon, with real km buffers, not from the centroid or a 0.25 degree pad. A facility inside the AOI is now at 0 km. Large AOIs count 2 to 7 times more facilities within 10 and 25 km.
- `gbif_distinct_species`: paging passes the old 1,200-species cap (AOI 1: 1,200 to 1,709).
- `ovt_extractive_area_km2`: landfill is no longer extractive. The new value equals the old value minus `ovt_landuse_km2_landfill` in every AOI.
- `nrcanlc_n_classes`: NRCan land cover 2020 replaces 2015.
- `agb_carbon_mgc_ha_*` (was `agb_mgha_*`): HGB stores carbon in Mg C/ha at scale 0.1. Values are a tenth of before.
- `ovt_comm_n_within_10km` (AOI 2: 1 to 0) and `osm_highway_ways` (AOI 1: +6%): live data in newer releases.

## Known gaps

- USDA CDL and CSB, EPA ECHO, SEMS, NNDSS, NWSS, CDC PLACES, LODES, TIGER, AQS and WQP are US-only. They report `not_applicable` for these AOIs.
- BC crown tenure and BC parcel sources report `not_applicable`: no AOI is in BC.

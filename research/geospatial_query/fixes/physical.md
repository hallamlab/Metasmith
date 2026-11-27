# physical: REPORT

File changed: `research/geospatial_query/geoquery/adapters_physical.py` (only). Scratch: `/home/tony/.claude/jobs/07dc4bd7/tmp/t2/physical/`.

## 1. Dispositions

- hansen_loss_frac_2001_2024 vs v1.13: renamed -> `hansen_loss_frac_2001_2025`. Bucket listing shows GFC-2025-v1.13 is the newest release (no 2026/v1.14 prefix; download.html says "2001-2025"), so the version does not move; the old name was wrong.
- Hansen lossyear range 1-20 vs 2001-2025 (upstream inconsistency): not a bug in code. Pixel values 1-25 decode as 2000+n, and the run returns last year 2025.
- hansen_loss_n_years_affected unit `yr`: unit change, see section 2 (count). modal/last year are calendar years, not durations: suggest `year`.
- hansen_n_px float example / decimated count: renamed -> `hansen_px_sampled` (it counts pixels of the decimated read, not native 30 m pixels). The int/float display is a formatting matter outside the adapter.
- hansen_loss_modal_year = 0 with no loss: fixed. Modal and last year are omitted when nothing was lost; `hansen_loss_n_years_affected` stays 0.
- hansen_net_change_frac mixes 2000-2012 gain with 2001-2025 loss: removed (periods differ; gain 0 when tile missing).
- hansen_lossyear/gain_tile_published flags: removed (probe flags, absent tile = absent feature).
- noaa_nsidc_sic_cdr no version pin: fixed. The old adapter read NCEI `.../sea-ice-concentration/access/`, which is frozen at CDR V2 (shipped data was 2015-12-31, f17 v02r00). Now pinned to NSIDC G02202 **V6** (`https://noaadata.apps.nsidc.org/NOAA/G02202_V6/north/daily/`, file `sic_psn25_YYYYMMDD_am2_v06r00.nc`, DOI 10.7265/b18j-z797). V4 and V5 are retired. V6 has no lat/lon variables and the field is `cdr_seaice_conc` (scaled 0-1, 255 fill = land), so the adapter builds lat/lon from x,y and the file's own proj4text. Newest day in the V6 tree is read (default query time = now); the version is the pin.
- nsidc_sic_frac_* 0.0 for both land-masked and ice-free: fixed. Concentration features are emitted only when a valid cell exists; land-masked AOIs now lack them (valid_cells=0 tells why). Renamed `nsidc_sic_frac_mean/max` -> `nsidc_sic_conc_mean/max` (source variable is "concentration"), `nsidc_sic_present` -> `nsidc_sic_ge15pct_present` (the 0.15 threshold is geoquery's).
- etopo 30 arc-s vs guide 15 arc-s: not a bug. The file read is the 30 arc-second global bedrock GeoTIFF; `release` now says 30 arc-second.
- etopo_max_depth_m (0.0 for any land AOI): removed (exact negative of etopo_min_elev_m, which is kept).
- etopo_is_submarine: removed (frac_below_sea_level > 0.5, duplicate).
- etopo_frac_below_sea_level vs EGM2008 reference: not a bug; kept, name states the test (value < 0).
- gsc_surficial_1880a identity: **fixed + renamed**. The FTP file `Surficial-geology_Geologie-des-depots-meubles/surficial_geology_index_geologie_surface/gsc_surficial_geology.gdb.zip` belongs to open.canada.ca "Surficial geology index map" (cebc283f-...). I downloaded both that gdb and the one under `Geology_Geologie/surficial_geology_geologie_de_surface/` (dataset 317ebf06-..., "Surficial geology map of Canada, 1:5 000 000"): the `geology` layers are identical (12385 features, identical attributes and geometry); only the `index` layer differs (1060 vs 581). The data the adapter reads is therefore the 1:5M map, GSC Canadian Geoscience Map 195, "Surficial geology of Canada" (prelim. ed., ver. 2.0, 2014; DOI 10.4095/295462), a conversion of Map 1880A (Fulton 1995). Download URL moved to the 1:5M dataset's own path. Source id `gsc_surficial_1880a` -> `gsc_surficial_cgm195`.
- gsc_surf_* no prose dictionary: not a bug. Class values are the layer's `geology` field verbatim.
- gsc_surf_unit/label/description/dominant_frac/n_units_in_aoi: replaced by classed `gsc_surf_frac_in_aoi` plus `gsc_surf_polygon_count_in_aoi` (rename of n_units; it counts polygons). The old strings were also name-truncated ([:60],[:12],[:110]).
- nrcanpf_zone_class/ice_class/zone_code: replaced by classed `nrcanpf_zone_frac_in_aoi` (PMF_E) and `nrcanpf_ice_frac_in_aoi` (ICE_E); the truncated, dominant-only strings and the geoquery string "no permafrost" are gone. `nrcanpf_zone_code` (PMF_CLASS letter) removed as a duplicate of PMF_E.
- nrcanpf_count_within_100km: renamed `nrcanpf_polygon_count_within_100km` (counts polygons, not areas).
- nrcan_permafrost obs_date "2019": false. The dataset is "Permafrost, Atlas of Canada, 5th Edition" (open.canada.ca d1e2048b-..., published 2016-09-25). obs_date now 2016 and `release` says so.
- neglac_* named for glaciers: renamed prefix `ne_glaciated_` (frac_in_aoi, nearest_km, nearest_name, polygon_count_within_100km/250km, present_in_aoi). Counts are polygon counts.
- neglac_nearest_name "unnamed" fallback: removed; the feature is omitted when the source `name` is null. Also removed `[:48]`.
- natural_earth master branch unpinned and "NE v5.1.1" claim: fixed. Pinned to tag v5.1.2 (the latest tag); cache file name includes the tag.
- usgs_namrad citation / dates: fixed. Citation is Duval, Carson, Holman & Darnley 2005, USGS OFR 2005-1413 (per https://mrdata.usgs.gov/metadata/narad.txt); the 1990 and "2009 compilation" claims are gone. obs_date: NURE surveys 1975-83, grids processed 1999-2005.
- namrad_th_k_ratio unit: ppm eTh per % K is not dimensionless. Unit `ppm/%` (section 2). namrad_u_th_ratio (ppm eU / ppm eTh) is a true ratio, `1` stays.
- namrad_sample_pad_deg: removed (constant).
- nrcan900a_total_records: removed (layer row count, constant). Same for `usgsmineplant_total_records`.
- nrcan900a_nearest_commodity reads PRD_DESC_E: not a bug (commodity); `[:48]` truncation removed from names and commodity strings for both mine adapters.
- usgs_mineplant obs_date "2016": false. The USGS metadata title is "Active Mines and Mineral Processing Plants in the United States in 2003" (pub. 2005). obs_date 2003. Removed the "schema drift falls back to all sites" branch.
- clay_emb_component_std: renamed `clay_emb_mean_component_std` (std across the 768 components of the AOI mean vector). Related renames: clay_emb_l2_norm -> `clay_emb_mean_l2_norm`, clay_emb_l2_norm_patch_mean -> `clay_emb_patch_l2_norm_mean`, clay_emb_c0..c3 -> `clay_emb_mean_c0..c3`.
- clay_emb_cos_to_tile_mean vs Clay's per-tile average: renamed `clay_emb_mean_cos_to_tile_patch_mean` (computed against the mean of all patch rows in the file, not Clay's own tile embedding).
- clay_emb_dim (constant 768) and clay_emb_mgrs_tile (per-AOI tile id, now in meta `item`): removed.
- clay_emb_mgrs_tile picked from centroid only: **open**, see section 6.
- csb: the fiboa file dissolves adjacent same-crop fields (per its README), so "field" counts and areas were polygon counts. Renamed `csb_field_count` -> `csb_polygon_count`, `csb_field_area_median_ha/max_ha` -> `csb_polygon_area_median_ha/max_ha`, `csb_aoi_frac_in_field` -> `csb_aoi_frac_in_polygon`. `[:40]` truncation of `csb_dominant_crop` removed. The empty-AOI return no longer carries a median area of 0.0.

## 2. Units

Features new or renamed (feature<TAB>SI_unit), all emitted by this file:

    hansen_loss_frac_2001_2025	1
    hansen_px_sampled	count
    nsidc_sic_conc_mean	1
    nsidc_sic_conc_max	1
    nsidc_sic_ge15pct_present	
    ne_glaciated_nearest_km	km
    ne_glaciated_nearest_name	
    ne_glaciated_frac_in_aoi	1
    ne_glaciated_polygon_count_within_100km	count
    ne_glaciated_polygon_count_within_250km	count
    ne_glaciated_present_in_aoi	
    nrcanpf_polygon_count_within_100km	count
    nrcanpf_zone_frac_in_aoi	1
    nrcanpf_ice_frac_in_aoi	1
    gsc_surf_frac_in_aoi	1
    gsc_surf_polygon_count_in_aoi	count
    csb_polygon_count	count
    csb_polygon_area_median_ha	ha
    csb_polygon_area_max_ha	ha
    csb_aoi_frac_in_polygon	1
    clay_emb_mean_l2_norm	1
    clay_emb_patch_l2_norm_mean	1
    clay_emb_mean_cos_to_tile_patch_mean	1
    clay_emb_mean_component_std	1
    clay_emb_mean_c0	1
    clay_emb_mean_c1	1
    clay_emb_mean_c2	1
    clay_emb_mean_c3	1

Unit changes on existing features:

    hansen_loss_n_years_affected	count
    hansen_loss_modal_year	year
    hansen_loss_last_year	year
    namrad_th_k_ratio	ppm/%

(`year` for the two calendar-year features is a suggestion: `yr` reads as a duration. csb_n_crop_classes stays `count`; csb_dominant_crop, nrcan900a/usgsmineplant name and commodity stay unitless.)

Removed features: hansen_net_change_frac, hansen_loss_frac_2001_2024, hansen_n_px, hansen_lossyear_tile_published, hansen_gain_tile_published, nsidc_sic_frac_mean, nsidc_sic_frac_max, nsidc_sic_present, etopo_max_depth_m, etopo_is_submarine, neglac_nearest_km, neglac_nearest_name, neglac_frac_in_aoi, neglac_count_within_100km, neglac_count_within_250km, neglac_present_in_aoi, nrcanpf_count_within_100km, nrcanpf_zone_class, nrcanpf_ice_class, nrcanpf_zone_code, gsc_surf_unit, gsc_surf_label, gsc_surf_description, gsc_surf_dominant_frac, gsc_surf_n_units_in_aoi, namrad_sample_pad_deg, nrcan900a_total_records, usgsmineplant_total_records, clay_emb_dim, clay_emb_mgrs_tile, clay_emb_l2_norm, clay_emb_l2_norm_patch_mean, clay_emb_cos_to_tile_mean, clay_emb_component_std, clay_emb_c0..c3, csb_field_count, csb_field_area_median_ha, csb_field_area_max_ha, csb_aoi_frac_in_field.

## 3. Classes

- `gsc_surf_frac_in_aoi` (fraction of AOI area, aeqd-projected): class = the layer's `geology` value verbatim, code prefix included (e.g. "Tv: Till veneer", "GLn: Littoral and nearshore sediments"); the prefix distinguishes the repeated names (Offshore sediments occurs under Lo, GLo, Mo, GMo). Full list of 25 published values (12385 polygons): A, C, Cv, E, GFc, GFp, GLn, GLo, GMn, GMo, GMv, I, Lo, Ln, Mn, Mo, O, R, Th, Tb, Tm, Tv, V, W, Wv, as "<code>: <name>". Source: REST layer https://maps-cartes.services.geo.ca/server_serveur/rest/services/NRCan/gsc_surficial_geology_en/MapServer/3 (field "Geology category"); data https://ftp.maps.canada.ca/pub/nrcan_rncan/Geology_Geologie/surficial_geology_geologie_de_surface/gsc_surficial_geology.gdb.zip. The per-class prose (field `description_en`) belongs in class_descriptions.csv.
- `nrcanpf_zone_frac_in_aoi`: class = `PMF_E` values verbatim: "Continuous permafrost (90% to 100%)", "Extensive discontinuous permafrost (50 % to 90%)" (sic, upstream spacing), "Sporadic discontinuous permafrost (10 % to 50%)", "Isolated patches (0% to 10%)", "Subsea permafrost", "No permafrost (0%)". `nrcanpf_ice_frac_in_aoi`: class = `ICE_E` verbatim: "High (>20%)", "High to medium (10% to >20%)", "Medium to low (20% to <10%)", "Low (<10%)", "Low to nil (<10% to 0%)", "Nil (0%)" (as published, including the upstream "20% to <10%" wording). Source: https://maps-cartes.services.geo.ca/server_serveur/rest/services/NRCan/permafrost_atlas_of_canada_en/MapServer/6 ; data `permafrost_atlas_of_canada_en.shp.zip` (fields PMF_E, ICE_E). Only classes with a nonzero AOI fraction are emitted.
- No other classed features: csb_dominant_crop is a single value (crop:name), not a per-class measure.

## 4. Sources and meta

Source id change: `gsc_surficial_1880a` -> `gsc_surficial_cgm195` (sources.tsv must follow; I did not edit it). Other ids unchanged: `hansen_gfc_v1_13` is still correct (GFC-2025-v1.13 is current), `natural_earth_10m`, `noaa_nsidc_sic_cdr` (id kept; version in `release`), `nrcan_permafrost`, `etopo_2022_30s_bed`, `usgs_namrad`, `nrcan_900a`, `usgs_mineplant`, `usda_csb_fiboa`, `clay_v0_embeddings`. The function `_a_glaciers` is renamed `_a_glaciated_areas`.

`release` / `url` returned:
- hansen_gfc_v1_13: "Hansen/UMD/Google/USGS/NASA Global Forest Change GFC-2025-v1.13" / treecover2000 tile URL (item = tile).
- usda_csb_fiboa: "USDA NASS Crop Sequence Boundaries 2016-2023 rev23, fiboa GeoParquet (2023 crop)" / https://data.source.coop/fiboa/us-usda-cropland/us_usda_cropland.parquet
- clay_v0_embeddings: "Clay Foundation clay-model-v0 embeddings (768-d, Sentinel-2 L2A patches)" / the tile .gpq URL.
- noaa_nsidc_sic_cdr: "NOAA/NSIDC Climate Data Record of Passive Microwave Sea Ice Concentration V6 (v06r00)" / the daily file URL.
- natural_earth_10m: "Natural Earth vector v5.1.2, 10m Glaciated Areas" / raw.githubusercontent tag v5.1.2 geojson.
- nrcan_permafrost: "Permafrost, Atlas of Canada, 5th Edition (published 2016-09-25)" / the shp.zip.
- etopo_2022_30s_bed: "NOAA NCEI ETOPO 2022 v1, 30 arc-second bedrock elevation" / the global GeoTIFF.
- gsc_surficial_cgm195: "GSC Canadian Geoscience Map 195, Surficial geology of Canada (prelim. ed., ver. 2.0, 2014)" / the Geology_Geologie gdb.zip.
- usgs_namrad: "USGS Open-File Report 2005-1413, Terrestrial radioactivity and gamma-ray exposure in the United States and Canada (gridded images)" / https://mrdata.usgs.gov/radiometric/
- nrcan_900a: "NRCan Map 900A 75th edition (2025), ProducingMines layer" / the shape.zip.
- usgs_mineplant: "USGS Active Mines and Mineral Processing Plants in the United States in 2003 (2005; MRData mineplant)" / mineplant-csv.zip.

obs_date changes (shown in `time`): hansen 2000-2025, namrad "1975-1983 (NURE aerial surveys; grids processed 1999-2005)", permafrost 2016, mineplant 2003, glaciated areas 2023, 900A 2025, sea ice = file day (2026-11-21).

## 5. Test results

Run live in the image, 0.26.0 with source mounted. aois11 (CA) unless stated.
- hansen_gfc_v1_13 (11 AOIs): 99 found. Common features identical to shipped; loss fraction unchanged under the new name (same GFC-2025-v1.13). Dropped: net_change, tile flags.
- noaa_nsidc_sic_cdr (11): 44 found. Shipped was V2 2015-12-31; now V6 2026-11-21. Cells, nearest-cell km and valid cells identical to shipped for all 11 AOIs (all land-masked, valid_cells 0), so conc features are now absent where shipped had 0.0. Extra check on 3 ocean AOIs (ocean.geojson): Hudson Bay and Labrador Sea 4 valid cells, conc 0.0 (ice-free, present False); Beaufort Sea 1 nearest cell, conc 0.94 (present True).
- natural_earth_10m (11): 55 found. Values identical to shipped under new names.
- nrcan_permafrost (11): 63 found. Common numerics identical; new class lists. Nunavut AOI 7: 100% "Continuous permafrost (90% to 100%)" / "Low (<10%)". Ontario AOI 5: only "No permafrost (0%)" = 1.0, nrcanpf_frac_in_aoi 0.
- etopo_2022_30s_bed (11): 77 found. Values identical; two removed features.
- gsc_surficial_cgm195 (aois11, run on a scratch package copy whose sources.tsv carried the new id): 36 found. Example AOI 5: Tv 0.8167, GLn 0.1112, Tm 0.0539. No shipped baseline.
- usgs_namrad (aois2): 8 found, 1 unavailable (AOI 2 outside grid, as before). Values identical.
- clay_v0_embeddings (aois2): 9 found, 1 unavailable (tile 19UFA not published). Values identical under new names.
- nrcan_900a (11): 66 found. Values identical; total_records dropped.
- usda_csb_fiboa and usgs_mineplant (us2.geojson, two scratch US AOIs): 8 and 16 found. No shipped baseline (shipped AOIs are Canada).

## 6. Open

- usda_csb_fiboa is still on the 2023 crop. NASS now publishes CSB through 2025 (rev23 zips, "2018-2025 released March 27, 2026", current version v2024), but only as multi-GB national shapefile zips; the fiboa GeoParquet on source.coop stops at 2016-2023. Moving needs a national download and a conversion step; left on 2023 and recorded in `release`.
- clay_v0_embeddings picks the MGRS tile from the AOI centroid only, so an AOI straddling two tiles reflects one. Not changed.
- Hansen tile choice uses bbox west/north corner only, so an AOI straddling a 10 degree tile edge reads one tile. Not changed.
- `etopo_bed_elev_m_n_px` is made by pipeline `stats()` (not my file) and counts decimated pixels like hansen_px_sampled did; the name should follow if stats() is changed.
- sources.tsv, feature_units.tsv, feature_descriptions.csv and class_descriptions.csv need the renames and unit rows above (not edited).
- The NSIDC V6 newest-day file is chosen at run time; query time selection belongs to the query-time work.

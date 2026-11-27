# census REPORT (condensed by coordinator from the worker's final reply; full text in the session transcript)

DB counts unreachable (www12.statcan.gc.ca Cloudflare 403); census built on 2021 DAs via geo.statcan.gc.ca MapServer + api.statcan.gc.ca SDMX STC_CP DF_DA.
Source statcan_csd_2016 -> statcan_census_2021; donneesquebec_role removed (catalogue-count probe).
Classes: ghssmod_frac (GHSL DP2023 s2.6 Table 21), ghssmod_group_frac (Table 24), iolulc_frac (PC file:values), odhf_* by type (ODHF_Metadata.pdf), aiannh_frac (TIGERweb layer names), bccrown_owner_type_frac (OWNER_TYPE values).
Open: DB swap needs browser-staged 2021_92-151_X.zip + ldb_000b21a_e.zip; income year is 2020; csd_type abbreviations; odhf/alta distances from centroid; bccrown crown definition and overlapping parcels; cdc_places 60-tract cap; nrcan_alta hard-coded obs_date.

## 2. Units

    statcan_population	count
    statcan_private_dwellings	count
    statcan_occupied_private_dwellings	count
    statcan_private_households	count
    statcan_median_household_income_approx	CAD
    statcan_n_da	count
    statcan_csd_uid	
    statcan_csd_name	
    statcan_csd_type	
    statcan_csd_n_overlapping	count
    statcan_csd_dominant_frac	1
    statcan_popest	count
    statcan_avg_dwelling_value_approx	CAD
    statcan_median_dwelling_value_approx	CAD
    statcan_owner_households	count
    statcan_dwelling_value_n_da	count
    ghssmod_frac	1
    ghssmod_group_frac	1
    iolulc_frac	1
    odhf_nearest_by_type_km	km
    odhf_n_by_type_within_25km	count
    aiannh_frac	1
    bccrown_owner_type_frac	1

Removed features: statcan_csd_pop_2016, statcan_csd_pop_density_km2, statcan_csd_households_2016, statcan_csd_med_hh_income_cad, statcan_csd_land_area_km2, statcan_aoi_area_km2, statcan_province, statcan_ct_uid, statcan_cma_name, statcan_cma_uid, statcan_ct_n_overlapping, statcan_avg_dwelling_value_cad, statcan_owner_occupied_dwellings, statcan_dwelling_value_csd_uid, statcan_dwelling_value_n_csd, statcan_popest_latest, statcan_popest_ref_period, statcan_popest_vector_id, ghssmod_dominant_label, ghssmod_urban_frac, ghssmod_urban_centre_frac, ghssmod_is_urban, ghssmod_tiles, ghspop_tiles, iolulc_built_frac, iolulc_is_urban, iolulc_year, odhf_nearest_hospital_km, odhf_nearest_ambulatory_km, odhf_nearest_nursing_km, odhf_n_hospital_within_25km, odhf_n_ambulatory_within_25km, odhf_n_nursing_within_25km, qc_assessment_catalogue_hits, qc_assessment_source_host, tiger_transfer_limit_hit, lodes_scope, lodes_verified_absence, lodes_year, aiannh_reservation_frac, aiannh_trust_land_frac, aiannh_aoi_area_km2, bccrown_crown_frac_lower_bound, bccrown_tenure_error

## 3. Classes
- ghssmod_frac (and ghssmod_dominant_class): Urban Centre grid cell (30), Dense Urban Cluster grid cell (23), Semi-dense Urban Cluster grid cell (22), Suburban or peri-urban grid cell (21), Rural cluster grid cell (13), Low Density Rural grid cell (12), Very low density rural grid cell (11), Water grid cell (10). GHSL Data Package 2023 s2.6 p.55-56, Table 21, https://ghsl.jrc.ec.europa.eu/documents/GHSL_Data_Package_2023.pdf (class 21 text says "per-urban"; Table 21 term used).
- ghssmod_group_frac: Urban Centre grid cell (30), Urban Cluster grid cell (21-23), Rural grid cell (11-13). Same document, level-1 classes p.55, Table 24. No water class at level 1; groups sum to 1 - water fraction.
- iolulc_frac (and iolulc_dominant): Water, Trees, Flooded vegetation, Crops, Built area, Bare ground, Snow/ice, Clouds, Rangeland. https://planetarycomputer.microsoft.com/api/stac/v1/collections/io-lulc-annual-v02 (item_assets.data.file:values)
- odhf_nearest_by_type_km, odhf_n_by_type_within_25km: Ambulatory health care services, Hospitals, Nursing and residential care facilities. https://ftp.maps.canada.ca/pub/statcan_statcan/Health-care-facilities_Etablissement-de-sante/ODHF_BDOES/ODHF_Metadata.pdf
- aiannh_frac: Federal American Indian Reservations, Off-Reservation Trust Lands. TIGERweb tigerWMS_Census2020 MapServer layers 34 and 36.
- bccrown_owner_type_frac: ParcelMap BC OWNER_TYPE values as served (Crown Agency, Crown Provincial, Federal, Local Government, Mixed Ownership, Private, Unclassified, Untitled Provincial).

## 1. Dispositions

- statcan_csd_* area-weighted whole-CSD means: fixed (DA-apportioned sums, household-weighted _approx medians). statcan_csd_pop_2016 -> statcan_population; statcan_csd_households_2016 -> statcan_private_households; statcan_csd_med_hh_income_cad -> statcan_median_household_income_approx; statcan_csd_land_area_km2 removed.
- statcan_avg_dwelling_value_cad area-weighted: renamed+fixed -> statcan_avg_dwelling_value_approx (owner-household weighted); statcan_median_dwelling_value_approx added.
- statcan_owner_occupied_dwellings whole-CSD sum: fixed -> statcan_owner_households, apportioned.
- statcan_csd_pop_density_km2: removed (population / AOI area; area is in aois.csv).
- wds_popest 2016 CSD code against a 2021-boundary cube: fixed (CSD from the 2021 boundary service; values identical on all 11 AOIs).
- statcan_popest_latest preliminary estimate: renamed -> statcan_popest; reference period in the time column.
- statcan_popest_yoy_pct compared to the previous returned point: fixed (point dated one year earlier only).
- statcan_popest_ref_period: removed (duplicates time). statcan_popest_vector_id: moved to item.
- statcan_ct_uid sentinel and CMA/CA naming: removed with the 2016 CT/CMA block.
- statcan_aoi_area_km2: removed (duplicate).
- GHS-SMOD labels: fixed to GHSL names. ghssmod_is_urban removed; ghssmod_urban_frac -> ghssmod_group_frac; ghssmod_urban_centre_frac removed (duplicate of class 30); ghssmod_dominant_label removed; ghssmod_tiles moved to item.
- GHS-SMOD fractions summing to 1.0056: not a bug (per-AOI sums 0.9999-1.0001; the example mixed AOIs).
- ghspop unit contradiction: not a bug (GHSL s2.5 people per cell; adapter sums cells).
- ghspop_cell_p90 / cell_mean scale with latitude: not a bug (per-cell by name). ghspop_tiles moved to item.
- ALTA docs 2010 edition vs CLSS extracts: not a code bug; release names the CLSS per-province shapefiles.
- alta_nearest_name KAWAWACHIKAMACH with extract AB: not a bug (examples from different AOIs; live values correct).
- alta_overlap_km2 consistency: not a bug.
- qc_assessment_*: removed with the donneesquebec_role adapter (catalogue count, not AOI data).
- StatCan HTML 403: not a code bug (Cloudflare).
- No definitions for old census features: renamed onto Census Profile characteristics (labels verbatim in PROFILE in the code).
- iolulc_built_frac duplicate: removed; iolulc_is_urban (geoquery 50% rule) and iolulc_year (duplicate of time) removed.
- io-lulc clouds in fractions: not a bug (Clouds is a published class, reported as its own class).
- ODHF version ambiguity: fixed (odhf_bdoes_v1.csv = Version 1.0; release says so).
- ODHF lowercased facility type: fixed (published capitalization).
- ODHF coordinate accuracy caveat: not a bug.
- ODHF per-type features only when present: fixed (all three types always emitted as classes).
- No-lead fixes: tiger_transfer_limit_hit removed (truncated answers raise); lodes_scope, lodes_verified_absence, lodes_year removed; AIANNH fractions -> aiannh_frac classes; bccrown_frac_* -> bccrown_owner_type_frac with verbatim OWNER_TYPE; bccrown_crown_frac_lower_bound and bccrown_tenure_error removed; _esri_rings accepts Z coordinates (KML AOIs crashed every ArcGIS query).

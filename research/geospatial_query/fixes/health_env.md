# health_env: adapter fixes report

Files changed:
- `research/geospatial_query/geoquery/adapters_health_env.py`: rewritten, 1042 -> 874 lines.
- `research/geospatial_query/geoquery/phac_ww_sites.csv`: new, 65 plants with columns Location, lat, lon, coord_source.

Cross-cutting change: point-source distances and `_<N>km` counts are now measured from the AOI polygon, not its centroid. A point inside the AOI is 0 km.
- This applies to NAPS, AQS, NPRI, GHGRP, FCSI, SEMS and PHAC wastewater.
- `_aoi_km` measures distance in an azimuthal-equidistant frame centred on the AOI.
- The code always called `BUF_KM` an "AOI buffer", so the computation now matches the name.
- When several points are inside the AOI, the one nearest the centroid becomes "nearest" (`_ranked`).
- On large AOIs, the counts change a lot (see section 5).

## 1. Dispositions

Leads come from `feature_descriptions_issues.md`. The hazards lines are 26-46 and 49-50. The public_health_microbiology lines are 120-127 and 132.

- L26 NPRI totals count particulates three times: fixed.
  - The cross-substance totals `npri_{air,water,land,total}_releases_kg_25km` are removed.
  - Releases are now reported per substance as classes, under `npri_air_releases_25km`, `npri_water_releases_25km` and `npri_land_releases_25km`.
  - Total PM, PM10 and PM2.5 are separate class records and are never summed.
- L27 `npri_{so2,co,nox,pm10,pm25}_kg_25km` summed all media but read as air: fixed.
  - The features are removed.
  - Each substance now sits under one medium's feature, read from NPRI's own per-medium "Total" column.
- L28 top substance duplicates SO2: fixed. `npri_top_substance` and `npri_top_substance_kg_25km` are removed.
- L29 g TEQ rows were dropped silently: fixed. They are kept, with `unit: "g TEQ"` on the record.
  - The road-dust part of this lead is not a bug. The 2025 CSV has a separate "Total Releases - Road Dust Emissions" column (col 44).
  - "Air Emissions - Total" is stack + storage + fugitive + spills + other (cols 27-32), so it excludes road dust.
- L30 newest NPRI year chosen at run time: fixed.
  - The year now goes in `release` ("NPRI 2025 reporting year").
  - "Newest" stays the default, per the source-pinning decision.
  - The newest year is now 2025, against 2024 in the shipped data.
- L31 no NPRI/GHGRP field dictionary: not a code bug. It is dictionary work for T9.
- L32 GHGRP excludes CO2bio: renamed to match the source column "Total Emissions (tonnes CO2e) / Émissions totales (tonnes éq. CO2) - excl. CO2bio".
  - `ghgrp_co2e_kt_25km` -> `ghgrp_co2e_excl_co2bio_kt_25km`.
  - `ghgrp_max_facility_co2e_kt` -> `ghgrp_max_facility_co2e_excl_co2bio_kt_25km`.
  - The CSV is now read as utf-8-sig. Reading it as latin-1 garbled the headers.
- L33 GHGRP divided by 1000: not a bug. The source is in tonnes and the output is in kt.
- L34 NAPS counts only continuous stations: renamed to `naps_continuous_stations_25km/50km` and `naps_nearest_continuous_station_km/id/city`.
- L35 NAPS Average criteria: not a bug. The file header states them.
- L36 NAPS "NOX" label: now a class value, verbatim from the file.
- L37 NAPS station id stored as text: not a bug. It is an identifier.
- L38 CAMS resolution: res_m changed from 44000 to 45000, matching Open-Meteo's "0.4° (~45 km)".
- L39 CAMS grid cell centre: not a bug. The names already say "grid".
- L40 CAMS window moves with the run date: renamed to `cams_hourly_mean_30d` and `cams_hourly_p95_30d`.
  - The window is shown in `time`.
  - `cams_n_hours` was a constant 720 and moved to `item`.
- L41 one description shared by all gases: dictionary only.
- L42 FCSI soil volume: renamed to `fcsi_contamination_volume_m3_25km`. The source dictionary says "cubic meters of contamination", not soil.
  - The "first estimate only" part is not a bug. A full XML parse finds at most one ContaminationEstimates per site.
- L43 FCSI "high priority" meant code 1: fixed. It is replaced by `fcsi_sites_by_classification_25km`, with classes from `Classification/Name/EN`.
- L44 FCSI identifier attribute name: no output was affected. The adapter no longer keeps site_id at all.
- L45 name truncation: fixed. All `[:N]` slicing is removed from the file.
- L46 FCSI ignored Suspected sites: fixed. Active, Suspected and Closed are now classes of `fcsi_sites_by_status_25km`.
- L49 observation types in sources.tsv: not my file. cams_open_meteo should be "modelled", not "blended".
- L50 NAPS URL form: fixed. `url` now uses the `api/file?path=` form.
- L120 PHAC dictionary mismatch: not a code bug.
  - The open CSV has no dictionary of its own.
  - The unit "copies/mL" is PHAC's own label: Figure 1 text table, "7-day rolling average of COVID-19 viral load (copies/mL)".
  - Seen in Wayback snapshots 20220531232243 and 20230222041808 of https://health-infobase.canada.ca/covid-19/wastewater/
- L121 mean across fractions and sites: fixed. The adapter now reports the nearest plant, with one record per fractionid class.
- L122 pooled p90 and ratio: removed.
- L124 influenza matched by substring: fixed. Every virus is matched by its exact name.
- L125 cross-virus sums: removed. Counts are now ints.
- L126 RVDSS dictionary names differ from the CSV: not a bug. Classes follow the CSV that is read.
- L127 non-ISO `rvdss_week`: the feature is removed.
  - `time` is now PHAC's week-ending `date`.
  - `item` reads "2026 week 46".
- L132 PHAC province/pruid/count lookups: all removed.

Brief items beyond the leads:

- PHAC wastewater: rebuilt to the nearest monitored plant, per the decision.
- RVDSS: reports the province's values, labelled by `rvdss_jurisdiction`, with res_m admin:province.
  - The region fallback is gone, because every province has rows.
- NNDSS (found while testing): 3 of the 7 watched labels never matched a published label.
  - The labels are now verbatim from the table.
  - Lyme disease is not in the weekly table, so it is dropped.
  - Conditions are now classes.
  - Removed: the cross-condition totals, the top condition, condition counts, year and week.
- ECHO-TRI: the counts are over the AOI bbox padded by N km, so the features are renamed `*_bbox_pad25km` and `*_bbox_pad10km`.
  - `echotri_density_per_km2_25km` used a wrong area formula and is removed.
  - `echotri_tri_frac_of_regulated_25km` is removed.
- epa_echo (coordinator note): I removed my `epa_echo` registration, `_a_echo` and `_ECHO_MAP`.
  - It ran the same ECHO get_facilities query as the infrastructure lane's `epa_echo` (bbox + 2 km against bbox).
  - It emitted the same Rows fields, plus penalties and two ratios, so its features were duplicates.
  - `epa_echo_tri` stays. It counts at different pads (25 km and 10 km) under its own source id.
- SEMS: the old code searched 16 mi from the centroid and called it 25 km.
  - It now asks FRS for 25 km plus the AOI half-diagonal, then filters by distance to the polygon.
  - The found and absent branches are now one code path. The absent branch no longer emits a None feature.
- Constants removed: naps_year, aqs_year, npri_year and ghgrp_year (now in release and time), cams_n_hours, echo_bbox_pad_km.
- Other code removed: the stale `CONTROL_DIFFERENTIALS` block, and the unused `PRUID` and `RVDSS_REGION` maps.
- Cache parquets whose schema changed are renamed to `*_v2`, so old caches are not read.

## 2. Units

```
naps_nearest_continuous_station_km	km
naps_nearest_continuous_station_id	
naps_nearest_continuous_station_city	
naps_continuous_stations_25km	count
naps_continuous_stations_50km	count
naps_annual_mean	per-record (ppb, ppm, ug/m3)
naps_annual_mean_station_km	km
aqs_annual_mean	per-record (ppb, ppm, ug/m3)
aqs_annual_mean_site_km	km
cams_hourly_mean_30d	per-record (ug/m3; AOD 1)
cams_hourly_p95_30d	per-record (ug/m3; AOD 1)
npri_air_releases_25km	per-record (kg or g TEQ)
npri_water_releases_25km	per-record (kg or g TEQ)
npri_land_releases_25km	per-record (kg or g TEQ)
ghgrp_co2e_excl_co2bio_kt_25km	kt
ghgrp_max_facility_co2e_excl_co2bio_kt_25km	kt
echotri_tri_reporters_bbox_pad25km	count
echotri_caa_air_sources_bbox_pad25km	count
echotri_rcra_handlers_bbox_pad25km	count
echotri_regulated_facilities_bbox_pad25km	count
echotri_tri_reporters_bbox_pad10km	count
echotri_caa_air_sources_bbox_pad10km	count
echotri_rcra_handlers_bbox_pad10km	count
echotri_regulated_facilities_bbox_pad10km	count
fcsi_sites_by_status_25km	count
fcsi_sites_by_classification_25km	count
fcsi_contamination_volume_m3_25km	m3
nndss_cases_current_week	count
nndss_cases_ytd	count
nndss_cases_prev_year_ytd	count
rvdss_tests	count
rvdss_detections	count
rvdss_percent_positive	%
phacww_location	
phacww_location_km	km
phacww_date	date
phacww_viral_load	copies/mL (also on each record)
phacww_seven_day_rolling_avg	copies/mL (also on each record)
```

Removed features, by source:
- NAPS: naps_nearest_station_{km,id,city}, naps_stations_{25,50}km, naps_year, every naps_<pollutant>_<unit>_mean, every naps_*_src_km.
- AQS: aqs_year, every aqs_*_<unit>_mean, every aqs_*_src_km.
- CAMS: the old per-pollutant cams_*_mean and cams_*_p95 names, cams_n_hours.
- NPRI: npri_year, npri_{air,water,land,total}_releases_kg_25km, npri_top_substance, npri_top_substance_kg_25km, npri_{pm25,pm10,so2,nox,co}_kg_25km.
- GHGRP: ghgrp_year, ghgrp_co2e_kt_25km, ghgrp_max_facility_co2e_kt.
- ECHO-TRI: every echotri_*_25km and echotri_*_10km, echotri_density_per_km2_25km, echotri_tri_frac_of_regulated_25km.
- ECHO (this file's copy only): every echo_* feature. The infrastructure lane keeps its own.
- FCSI: fcsi_active_sites_25km, fcsi_closed_sites_25km, fcsi_high_priority_sites_25km, fcsi_contaminated_soil_m3_25km.
- NNDSS: nndss_year, nndss_week, nndss_conditions_tabulated, nndss_conditions_with_cases_ytd, nndss_total_cases_ytd, nndss_total_cases_prev_year_ytd, the plain nndss_cases_current_week, nndss_top_condition, nndss_top_condition_cases_ytd, every nndss_<condition>_ytd.
- RVDSS: rvdss_week, rvdss_viruses_reported, rvdss_tests_latest_week, rvdss_detections_latest_week, rvdss_{sarscov2,influenza,rsv}_pct_positive.
- PHAC wastewater: phacww_province, phacww_pruid, phacww_sites_province, phacww_sites_latest_date, phacww_latest_date, phacww_viral_load_latest, phacww_7day_avg_latest, phacww_records_province, phacww_7day_avg_p90_1yr, phacww_latest_vs_1yr_p90.

## 3. Classes

- `naps_annual_mean`, `naps_annual_mean_station_km`: CO, NO, NO2, NOX, O3, PM10, PM2.5, SO2.
  - Source: the Pollutant column of 2024_Means_EN.csv, https://data-donnees.az.ec.gc.ca/api/file?path=air%2Fmonitor%2Fnational-air-pollution-surveillance-naps-program%2FData-Donnees%2F2024%2FContinuousData-DonneesContinu%2FAnnualSummaries-SommairesAnnuels%2F2024_Means_EN.csv
- `aqs_annual_mean`, `aqs_annual_mean_site_km`: EPA "Parameter Name" verbatim. Values: PM2.5 - Local Conditions, Ozone, Nitrogen dioxide (NO2), Sulfur dioxide, Carbon monoxide, PM10 Total 0-10um STP, Acceptable PM2.5 AQI & Speciation Mass.
  - Source: https://aqs.epa.gov/aqsweb/airdata/annual_conc_by_monitor_2025.zip
- `cams_hourly_mean_30d`, `cams_hourly_p95_30d`: pm2_5, pm10, carbon_monoxide, nitrogen_dioxide, sulphur_dioxide, ozone, aerosol_optical_depth.
  - Source: https://open-meteo.com/en/docs/air-quality-api
- `npri_{air,water,land}_releases_25km`: NPRI "Substance Name (English)" verbatim. Examples: "Total Particulate Matter", "PM10 - Particulate Matter <= 10 Micrometers", "PM2.5 - Particulate Matter <= 2.5 Micrometers", "Sulphur dioxide".
  - Source: NPRI-INRP_DataDonnées_2025.csv (url in section 4).
  - Published substance list: https://www.canada.ca/en/environment-climate-change/services/national-pollutant-release-inventory/substances-list.html
- `fcsi_sites_by_status_25km`: Active, Suspected, Closed.
- `fcsi_sites_by_classification_25km`: High Priority for Action, Medium Priority for Action, Low Priority for Action, Not a Priority for Action, Insufficient Information.
  - Source for both FCSI features: the XML in https://map-carte.tbs-sct.gc.ca/fcsi-rscf/oddo/fcsi-rscf.zip
- `nndss_cases_*`: "Salmonellosis (excluding Salmonella Typhi infection and Salmonella Paratyphi infection)", Pertussis, "Hepatitis C, acute, Confirmed", Campylobacteriosis, Legionellosis, Tuberculosis.
  - Source: https://data.cdc.gov/resource/x9gk-5huc.json
- `rvdss_*`: SARS-CoV-2, Influenza, Influenza A, Influenza B, RSV, HPIV, ADV, HMPV, EV/RV, HCoV.
  - Source: https://health-infobase.canada.ca/src/data/respiratory-virus-detections/RVD_WeeklyData.csv
- `phacww_*` (fractionid): liquid, solid, mixed.
  - Source: https://health-infobase.canada.ca/src/data/covidLive/wastewater/covid19-wastewater.csv

## 4. Sources

No source id is renamed. epa_echo is no longer registered from this file.

| source | release | url |
|---|---|---|
| eccc_naps | NAPS 2024 continuous annual means | api/file?path=.../2024_Means_EN.csv |
| epa_aqs_airdata | EPA AirData annual concentration by monitor 2025 | https://aqs.epa.gov/aqsweb/airdata/annual_conc_by_monitor_2025.zip |
| cams_open_meteo | CAMS global (Open-Meteo domains=cams_global) | https://air-quality-api.open-meteo.com/v1/air-quality |
| eccc_npri | NPRI 2025 reporting year | api/file?path=.../NPRI-INRP_DataDonnées_2025.csv |
| eccc_ghgrp | GHGRP 2024 reference year | api/file?path=.../PDGES-GHGRP-GHGEmissionsGES-2004-Present.csv |
| epa_echo_tri | current snapshot | https://echodata.epa.gov/echo/echo_rest_services.get_facilities |
| cdc_nndss | NNDSS weekly tables 2026 | https://data.cdc.gov/resource/x9gk-5huc.json |
| phac_rvdss | RVDSS 2026-2027 season | RVD_WeeklyData.csv URL above |
| cdc_nwss | current snapshot | https://data.cdc.gov/resource/2ew6-ywp6.json |
| phac_wastewater | covid19-wastewater.csv, data to 2024-06-28 | covid19-wastewater.csv URL above |
| epa_frs_sems | current snapshot | https://frs-public.epa.gov/ords/frs_public2/frs_rest_services.get_facilities |
| tbs_fcsi | current snapshot | https://map-carte.tbs-sct.gc.ca/fcsi-rscf/oddo/fcsi-rscf.zip |

### Where the coordinates in phac_ww_sites.csv come from

PHAC publishes site metadata in its dashboard file, https://health-infobase.canada.ca/src/json/wastewater/siteLoc1.json, but it covers only part of the plants.

- **PHAC siteLoc1.json (36 rows, counting the NML Historical Data duplicates of a plant).** The rows that name a plant are within 0.4 km of the matching OSM plant. 8 of the 36 are PHAC town-centre coordinates for Saskatchewan towns where OSM has no plant, and those rows are labelled as town centres.
- **OpenStreetMap (20 rows).** Each plant or lagoon was located by plant name, not by town name. Examples: Highland Creek, North Toronto, Lions Gate, Lulu Island, Northwest Langley, Winnipeg West End WPCC, Dartmouth, Mill Cove. "Lancaster" is the Lancaster WWTF in Saint John, not Lancaster near Moncton.
- **Nominatim town centres (9 rows).** No plant was found for these.

Two rows use OSM even though PHAC lists them:
- North Battleford: PHAC's coordinate is 6.9 km from the named OSM plant.
- Swift Current: PHAC's coordinate is the town centre, 2.2 km from the plant.

## 5. Test results

All runs used quay.io/hallamlab/geoquery:0.26.0 with my source mounted over the packaged copy.
- Canadian sources ran on aois11.
- US sources ran on two US test AOIs I made, Detroit and Bingham (`aois_us.geojson` in this directory). The shipped data has no US AOIs.

| source | new status | shipped status | notes |
|---|---|---|---|
| phac_rvdss | found 11 | found 11 | 260 rows (82 shipped); now week 46, 2026-11-21 (shipped W44). Provinces that publish only SARS-CoV-2 get 1 virus; QC, ON and BC get all 10. |
| phac_wastewater | found 11 | found 10, unavail 1 | Nunavut now gets La Ronge at 1190 km (was "no sites for pruid 62"). Quebec AOIs get Montreal North at 402-604 km. aoi_1 Alberta gets Île-à-la-Crosse at 231 km, last sample 2024-03-26. NL/QC AOIs get Campbellton, liquid plus NML solid. |
| cams_open_meteo | found 11 | found 11 | 176 rows (187 shipped). The window is the 30 days before this run. |
| eccc_naps | found 11 | found 11 | aoi_1 has 8 continuous stations within 25 km (4 shipped). Its nearest is now Fort McMurray at 0 km (was Fort McKay at 13.7 km), so PM2.5 is 15 (12 shipped). |
| eccc_npri | found 11 | found 11 | Year 2025 (2024 shipped); 508 rows. Facilities within 25 km, shipped -> new: aoi_1 7->30, aoi_3 2->5, aoi_4 8->14, aoi_5 21->31, aoi_7 0->1, aoi_9 0->1. |
| eccc_ghgrp | found 11 | found 11 | Still 2024. aoi_1: facilities 4->21, CO2e 21,737->49,088 kt. aoi_7 and aoi_9 are now non-zero. |
| tbs_fcsi | found 11 | found 11 | Sites within 25 km, shipped -> new: aoi_1 9->36, aoi_2 0->11, aoi_3 0->10, aoi_4 5->12, aoi_5 6->13, aoi_8 0->1. |
| epa_aqs_airdata | found 2 | n/a | Classes and units are as designed. |
| epa_echo_tri | found 2 | n/a | Renamed features. |
| cdc_nndss | found 2 | n/a | 6 labels now match (4 of 7 before). |
| cdc_nwss | found 2 | n/a | Only meta changed. |
| epa_frs_sems | unavailable 2 | n/a | frs-public.epa.gov resets the connection from this host, curl included. The new code is not tested live. |

For the Canadian sources, the count and distance changes come from the polygon-based distance. NPRI also gains a reporting year.

## 6. Open

- epa_frs_sems is not tested live, because FRS is unreachable from this host.
- RVDSS region-level viruses: provinces other than QC, ON and BC have only SARS-CoV-2 at province level. Their other viruses exist only at region level (Prairies, Atlantic, Territories). The adapter emits province values only, per the decision. Whether to add the region rows under a second jurisdiction label is the user's call.
- PHAC plant locations: 17 rows are town centres, 9 from Nominatim and 8 from PHAC. Small-town lagoons are usually 1-5 km from the town centre, so these distances can be off by that much.
- "Pasqua" is ambiguous: it can mean the hamlet or Pasqua First Nation. I used the hamlet.
- Dominion-Bridgeport uses an OSM feature named "Dominion Water Treatment Plant" that is tagged as a wastewater plant. Its identity is not confirmed.
- PHAC solid-fraction unit: PHAC labels solid-fraction values copies/mL too. The caveat belongs in the feature and class dictionaries, which are not my files.
- AQS keeps several rows per monitor and parameter (sample duration, pollutant standard). After sorting by distance, the first row is used, as before. This needs a rule for which duration to keep.
- ECHO-TRI: Bingham's regulated_facilities_bbox_pad25km (22,682) is almost the same as Detroit's (22,821). This may be a cap in ECHO. I did not investigate.
- NWSS still averages the percentile across the sewersheds in a county. No lead or decision covers it, so it is unchanged.

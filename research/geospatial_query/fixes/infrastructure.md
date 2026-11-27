# infrastructure report

File changed: research/geospatial_query/geoquery/adapters_infrastructure.py (only). Scratch: t2/infrastructure/.

## 1. Dispositions (issues file, infrastructure section and related leads)
- ovt_region_code unit "count" but value is ISO 3166-2 string: not fixed in code (units file is not mine). Unit must be blank. See Units.
- ovt_control_infra_rows (shared by four adapters) and every other control probe (ovt_control_*_rows, echo_control_facilities, gppd_control_plants_50km, wd_control_entities): fixed. All control machinery (mid-Pacific fingerprints, _guard, _ctrl) deleted; features removed.
- ovt_substation_n_aoi duplicate of ovt_power_substation_n: fixed, removed. ovt_power_substation_n itself now the class record `substation` in ovt_power_n_by_class.
- ovt_power_plant_present / ovt_comm_tower_present / ovt_*_present / ovt_address_regional_coverage: fixed, removed (flags derivable from counts, names broader than rules).
- ovt_power_plant_named takes substation names: renamed ovt_power_plant_names; now only class=plant rows, all names joined with "; ", no truncation.
- ovt_extractive_area_km2 includes landfill: fixed. Now area of Overture land_use subtype `resource_extraction` (Alberta 445.47 km2 = quarry only; was 450.277 with landfill 4.807). AOI 2 was 6.577 (all landfill), now 0.
- ovt_industrial_area_km2/_frac/_n are a 12-class union: removed. Replaced by ovt_landuse_km2_by_class (every class present, verbatim Overture names; industrial 32.17 km2 for Alberta).
- ovt_opencellid_status constant: removed.
- ovt_building_footprint_skipped_n: removed (misnamed).
- ovt_building_mean_footprint_m2 numerator/denominator mismatch: fixed, divides by the number of clipped footprints (same set as the area sum).
- ovt_*_n_bbox vs ovt_*_n equal: removed all n_bbox (ovt_segment_n_bbox, ovt_infra_n_bbox, ovt_landuse_n_bbox, ovt_address_n_bbox, ovt_building_n_bbox). ovt_division_n_in_radius kept (now real).
- ovt_control_building/segment/division/landuse_rows non-empty control: removed (probes).
- gppd obs_date says v1.3.0 but fetched from master: fixed. CSV pinned to commit 7a91cfb (HEAD of the unmaintained repo, 2022-01-26), release "v1.3.0 (7a91cfb)".
- gppd_nearest_km unbounded: not a bug, named for what it is (distance to nearest plant anywhere); kept.
- gppd_median_commission_year_50km float: fixed, integer.
- gppd_plants_in_bbox: renamed gppd_plants_in_aoi, now polygon test.
- gppd_dominant_fuel_50km, gppd_n_fuel_types_50km: removed, replaced by fuel class lists.
- GPPD has no data dictionary: fuel class values are the CSV's own primary_fuel values (see Classes). Open item below.
- wd_* counts bbox, multi-P31 upper bound: fixed. Polygon filter in Python (box query then point-in-polygon). wd_entities_bbox renamed wd_entities. wd_top5_class_share and wd_dominant_class_* removed, replaced by wd_entities_by_class (top 5 P31 classes, an entity with several P31 counts under each; note in the feature description).
- Overture release unrecorded: fixed, release and S3 path returned by every Overture adapter.
- 0.25 degree / "25km" and 10 km pads: fixed. ovt_address_n_within_25km and ovt_comm_n_within_10km now use the AOI polygon buffered by that many km on the ground (real buffer). Divisions search radius likewise buffers the polygon (15 km, else 60 km).
- ECHO counts are bbox: renamed with _bbox; ovt echo per-km2 now divides by bbox area (was AOI polygon area). INSPRows etc. are facility counts, not inspection counts, per EPA's results help page: renamed accordingly.
- Name truncation ([:60], [:50], [:120], [:12], [:8], [:4], [:N] on place and plant names, GPPD name, ECHO error): removed. Remaining slices are only in error text.
- Release listing not recorded in manifest cache: manifest cache file names now include the release, so a new release rebuilds instead of silently reusing old file bboxes.

## 2. Units
Features and units (SI_unit, blank = none; class records inherit the measure unit):
ovt_rail_km_by_class	km
ovt_road_km_by_class	km
ovt_utility_n_by_class	count
ovt_power_n_by_class	count
ovt_power_km_by_class	km
ovt_power_plant_names	
ovt_landuse_km2_by_class	km2
ovt_building_n_by_class	count
ovt_comm_n_by_class	count
ovt_division_n_by_subtype	count
ovt_region_code	
gppd_plants_in_aoi	count
gppd_plants_50km_by_fuel	count
gppd_capacity_mw_50km_by_fuel	MW
echo_caa_facilities_bbox	count
echo_cwa_facilities_bbox	count
echo_rcra_facilities_bbox	count
echo_tri_facilities_bbox	count
echo_inspected_facilities_5yr_bbox	count
echo_formal_action_facilities_5yr_bbox	count
echo_violation_facilities_bbox	count
echo_significant_violation_facilities_bbox	count
echo_facilities_per_bbox_km2	1/km2
echo_total_penalties_5yr_usd_bbox	USD
wd_entities	count
wd_entities_by_class	count

(unchanged name, still emitted: ovt_rail_km, ovt_rail_km_per_km2, ovt_rail_n_segments, ovt_road_km, ovt_road_km_per_km2, ovt_road_n_segments, ovt_pipeline_km, ovt_pipeline_km_per_km2, ovt_powerline_km_per_km2, ovt_power_tower_per_km2, ovt_power_plant_n, ovt_power_plant_area_km2, ovt_generator_n, ovt_extractive_area_km2, ovt_address_*, ovt_building_* (heights, footprint, built_frac, n, per_km2), ovt_comm_n_within_10km, ovt_division_* , ovt_nearest_place*, gppd_* others, wd_entities_per_km2, wd_distinct_classes.)

Removed features: ovt_substation_n_aoi, ovt_opencellid_status, ovt_control_infra_rows, ovt_control_segment_rows, ovt_control_landuse_rows, ovt_control_address_rows, ovt_control_building_rows, ovt_control_division_rows, echo_control_facilities, gppd_control_plants_50km, wd_control_entities, ovt_segment_n_bbox, ovt_infra_n_bbox, ovt_landuse_n_bbox, ovt_address_n_bbox, ovt_building_n_bbox, ovt_rail_present, ovt_pipeline_present, ovt_powerline_present, ovt_power_plant_present, ovt_comm_tower_present, ovt_address_regional_coverage, ovt_rail_km_<class> and ovt_road_km_<class> (now classes), ovt_road_dominant_class, ovt_utility_dominant_class, ovt_landuse_dominant_class, ovt_building_dominant_class, ovt_building_n_classes, ovt_landuse_n_classes, ovt_landuse_km2_<class>, ovt_industrial_area_km2, ovt_industrial_frac, ovt_industrial_n, ovt_pipeline_n, ovt_storage_tank_n, ovt_utility_n_aoi, ovt_powerline_km, ovt_powerline_n, ovt_minor_line_km, ovt_power_n_aoi, ovt_power_<class>_n, ovt_comm_tower_n, ovt_mobile_phone_tower_n, ovt_comm_n_aoi, ovt_building_footprint_skipped_n, ovt_address_dominant_postcode, ovt_division_n_subtypes, ovt_power_plant_named (renamed), gppd_dominant_fuel_50km, gppd_n_fuel_types_50km, gppd_plants_in_bbox (renamed), gppd_control..., echo_facilities_per_km2, echo_clean_air_act_n, echo_clean_water_act_n, echo_rcra_hazwaste_n, echo_tri_releasers_n, echo_inspections_n, echo_formal_enforcement_n, echo_current_violations_n, echo_significant_violations_n, echo_total_penalties_usd (all renamed to *_bbox forms above), wd_entities_bbox, wd_dominant_class_qid/_label/_n, wd_top5_class_share.
Unit changes: ovt_region_code count -> blank.

## 3. Classes
- ovt_rail_km_by_class: Overture RailClass (funicular, light_rail, monorail, narrow_gauge, standard_gauge, subway, tram, unknown). https://docs.overturemaps.org/schema/reference/transportation/types/segment/rail_class/
- ovt_road_km_by_class: Overture RoadClass (motorway, primary, ..., cycleway, bridleway, unknown). https://docs.overturemaps.org/schema/reference/transportation/types/segment/road_class/
- ovt_utility_n_by_class, ovt_power_n_by_class, ovt_power_km_by_class, ovt_comm_n_by_class: Overture InfrastructureClass (pipeline, storage_tank, power_line, minor_line, power_tower, substation, communication_tower, mobile_phone_tower, ...). https://docs.overturemaps.org/schema/reference/base/types/infrastructure_class/
- ovt_landuse_km2_by_class: Overture LandUseClass. https://docs.overturemaps.org/schema/reference/base/types/land_use_class/ (ovt_extractive_area_km2 uses LandUseSubtype resource_extraction, https://docs.overturemaps.org/schema/reference/base/types/land_use_subtype/)
- ovt_building_n_by_class: Overture BuildingClass; buildings with no class are not counted in a class. https://docs.overturemaps.org/schema/reference/buildings/types/building_class/
- ovt_division_n_by_subtype: Overture division subtype (locality, macrohood, neighborhood, ...). Overture divisions schema page.
- gppd_*_by_fuel: the CSV's primary_fuel values (Biomass, Coal, Cogeneration, Gas, Geothermal, Hydro, Nuclear, Oil, Other, Petcoke, Solar, Storage, Waste, Wave and Tidal, Wind). WRI publishes no column-level dictionary in the repo README; fuel names come from the data itself (README says fuels are mapped through a "fuel type thesaurus"). https://github.com/wri/global-power-plant-database, https://datasets.wri.org/dataset/globalpowerplantdatabase
- wd_entities_by_class: English label of each P31 value (Wikidata's own label; "label (Qid)" when two labels collide). https://www.wikidata.org/wiki/Property:P31

## 4. Sources
No source id changes. release / url now returned:
- overture_* (transportation, utility, power, power_plant, land_use, addresses, buildings, communication, divisions): release = newest Overture release listed in S3 (2026-11-18.0 at test), url = s3://overturemaps-us-west-2/release/<release>/theme=<theme>/type=<type>/. item keeps the per-read label.
- wri_gppd: release "v1.3.0 (7a91cfb)", url the raw CSV at commit 7a91cfbb2a4e272597acbc00506d61fc1ec73b3d, obs_date 2022-01-26.
- epa_echo: release = the service's own Version field ("ALL DATA v2017-06-16 0923" at test), url https://echodata.epa.gov/echo/echo_rest_services.get_facilities.
- wikidata_sparql: release = query date (WDQS has no version), url https://query.wikidata.org/sparql.
- mapillary_kartaview: success paths return release/url; both APIs currently need a token (Mapillary HTTP 500 code 190 Invalid OAuth; KartaView apiCode 400 Restricted), so it still returns unavailable.

## 5. Tests (docker, aois2 for Overture, one US triangle AOI for ECHO/Wikidata/GPPD)
- overture_transportation found 30; rail 11.918/11.119 km and road 2462.792/2.682 km identical to shipped.
- overture_utility found 7; overture_power found 15 (after the line-geometry fix); overture_power_plant found 7; counts by class sum to shipped ovt_power_n_aoi 4852 for Alberta. Power km by class restricted to line geometries after first run (first run included plant and substation polygon perimeters).
- overture_land_use found 43; extractive Alberta 445.47 (was 450.277), AOI 2 0 (was 6.577, landfill only). Landfill appears as class in the by-class list.
- overture_addresses found 5; 28777 Alberta, same as shipped. AOI 2 within-25km 1091 (real buffer).
- overture_buildings found 42; 25223 Alberta same as shipped; mean footprint 254 m2.
- overture_communication found 4; towers 12 and 6 as shipped; AOI 2 within-10km 0 (shipped 1, bbox pad counted it; real 10 km buffer does not).
- overture_divisions found 25; nearest place unchanged in kind; place names no longer truncated.
- wri_gppd found 21 rows on the US triangle; classes Cogeneration, Gas, Hydro, Solar, Waste, Wind.
- epa_echo and wikidata_sparql ran on the US triangle (2326 facilities in bbox; 64 Wikidata items inside the polygon).
- mapillary_kartaview unavailable 2 (token), unchanged.
- The 11-AOI set was not run (time); the two-AOI and US runs exercise every code path.

## 6. Open
- epa_echo registers two adapters under one source id: this file (slot industrial_facilities) and adapters_health_env.py:888 (slot facility_compliance). Both run under `epa_echo` and emit overlapping counts (the health_env one adds bbox_pad2km names). One should be deleted; I did not touch the other file. sources.tsv lists epa_echo as infrastructure.
- GPPD column definitions: the repository publishes no dictionary; confirm whether commissioning_year is first-generation year or capacity-weighted (Earth Engine wording).
- Wikidata class label for items with no English label falls back to the Q-id.
- ECHO release is the service's odd "v2017-06-16" version string, which is what the service returns.
- A large Wikidata AOI bbox could exceed WDQS 60 s limits; none of the tested AOIs did.

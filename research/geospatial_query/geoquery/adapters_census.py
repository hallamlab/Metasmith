"""adapters_census.py -- census, population and socio-economic lane.

Adapter contract (see pipeline.py):
    _a_<name>(aoi) -> (features: dict[str, value | [{class, value}]], meta: dict)
    PermissionError  -> out of coverage      (pipeline records 'not-attempted')
    any other raise  -> unavailable          (pipeline records 'unavailable')

Every source here is plain public HTTPS: no API keys, no credentials, no
connectors.  Hosts used:
    www150.statcan.gc.ca              StatCan WDS (population estimate cube)
    geo.statcan.gc.ca                 StatCan 2021 boundary map service
    api.statcan.gc.ca                 StatCan 2021 Census Profile (SDMX)
    ftp.maps.canada.ca                Open Government Canada static archive
    tigerweb.geo.census.gov           US Census TIGERweb ArcGIS REST
    lehd.ces.census.gov               LEHD LODES commute flows
    jeodpp.jrc.ec.europa.eu           JRC GHSL GHS-POP / GHS-SMOD
    openmaps.gov.bc.ca                BC Data Catalogue WFS
    data.cdc.gov                      CDC PLACES (Socrata)
    planetarycomputer.microsoft.com   io-lulc-annual-v02 (urban cross-check)

NOT used, and why:
    api.census.gov   ACS 5-year now returns an HTML "Missing Key" page for every
                     /data/ query, keyless or not (verified 2026-10 on
                     2023/acs/acs5 at tract and block-group level).  Population
                     and housing come from TIGERweb POP100/HU100 (2020
                     decennial, keyless) instead; ACS-only attributes such as
                     median household income are therefore absent for US AOIs.
                     No key is embedded -- see _a_acs_income, which records the
                     wall.
    www12.statcan.gc.ca   Census downloads (boundary, attribute and profile
                     files, dissemination-block counts) answer scripts with a
                     Cloudflare browser challenge (HTTP 403, verified 2026-11).

Admin-aggregate features carry meta['res_m'] = 'admin:<level>' so the dossier
can tell an areal-interpolated statistic from a measurement.
"""
import io, os, json, threading, warnings

import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import shape

from . import pipeline as P
from .pipeline import S, read_best, pc_item, _dt

import rasterio
from rasterio.windows import from_bounds as win_from_bounds
from rasterio.features import geometry_mask
from rasterio.transform import from_bounds as aff_from_bounds

warnings.filterwarnings("ignore")

CACHE = os.path.abspath("./cache")
os.makedirs(CACHE, exist_ok=True)

EQA = 6933                                # World Cylindrical Equal Area, for areas

_LOCKS = {}
_MEM = {}

def _lock(name):
    # one lock per cache key; the pipeline calls adapters from an 8-thread pool
    l = _LOCKS.get(name)
    if l is None:
        l = _LOCKS.setdefault(name, threading.Lock())
    return l


def _get(url, dest, timeout=900, **kw):
    """Download once to ./cache, atomically."""
    if os.path.exists(dest):
        return dest
    tmp = dest + f".{os.getpid()}.{threading.get_ident()}.tmp"
    r = S.get(url, timeout=timeout, stream=True, **kw)
    r.raise_for_status()
    with open(tmp, "wb") as fh:
        for ch in r.iter_content(1 << 20):
            fh.write(ch)
    os.replace(tmp, dest)
    return dest


def _area_km2(geom_4326):
    g = gpd.GeoSeries([geom_4326], crs=4326).to_crs(EQA)
    return float(g.area.iloc[0]) / 1e6


def _overlap(aoi, gdf):
    """Clip gdf (EPSG:4326) to the AOI; return (clipped_eqa, aoi_area_km2).

    Areas are computed in EPSG:6933 so fractions are equal-area, not
    conformal-projection artefacts.
    """
    g = shape(aoi["geom"])
    sub = gdf.iloc[list(gdf.sindex.query(g, predicate="intersects"))]
    if len(sub) == 0:
        return sub.to_crs(EQA) if len(sub) else sub, _area_km2(g)
    inter = sub.copy()
    inter["geometry"] = sub.geometry.intersection(g)
    inter = inter[~inter.geometry.is_empty & inter.geometry.notna()]
    return inter.to_crs(EQA), _area_km2(g)


def _haversine(lat1, lon1, lat2, lon2):
    R = 6371.0088
    p1, p2 = np.radians(lat1), np.radians(lat2)
    dp = p2 - p1
    dl = np.radians(lon2 - lon1)
    a = np.sin(dp / 2) ** 2 + np.cos(p1) * np.cos(p2) * np.sin(dl / 2) ** 2
    return 2 * R * np.arcsin(np.sqrt(np.clip(a, 0, 1)))


# ====================================================================== loaders
# Each loader downloads ONCE into ./cache, reduces to a slim GeoParquet/Parquet
# and drops the bulky original.  Thread-safe: the pipeline runs an 8-thread pool.

FTP = "https://ftp.maps.canada.ca/pub"

# 2021 Census.  www12.statcan.gc.ca (the boundary, attribute and profile downloads) sits behind a
# Cloudflare browser challenge, so units and values come from the two StatCan APIs that answer
# scripts: the boundary map service and the Census Profile SDMX service.  Neither publishes
# dissemination-block counts, so the dissemination area is the finest unit reachable.
STATCAN_GEO = "https://geo.statcan.gc.ca/geo_wa/rest/services/2021/Cartographic_boundary_files/MapServer"
CSD_LAYER, DA_LAYER = 9, 12
CENSUS_PROFILE = "https://api.statcan.gc.ca/census-recensement/profile/sdmx/rest/data/STC_CP,DF_DA"
CENSUS_RELEASE = "2021 Census of Population, Census Profile 98-316-X2021001 (DA), 2021 cartographic boundary files"
# Census Profile characteristic id -> StatCan's label, as the CL_CHARACTERISTIC code list names it
PROFILE = {"1": "Population, 2021",
           "4": "Total private dwellings",
           "5": "Private dwellings occupied by usual residents",
           "50": "Total - Private households by household size - 100% data",
           "229": "Median total income of household in 2020 ($)",
           "1468": "Total - Owner households in non-farm, non-reserve private dwellings - 25% sample data",
           "1474": "Median value of dwellings ($)",
           "1475": "Average value of dwellings ($)"}


def _apportion(units, aoi):
    """Units clipped to the AOI, each with _w = the share of its own area inside the AOI."""
    g = shape(aoi["geom"])
    out = units.assign(_full=units.to_crs(EQA).area)
    out["geometry"] = out.geometry.intersection(g)
    out = out[~out.geometry.is_empty & out.geometry.notna()].copy()
    out["_cut"] = out.to_crs(EQA).area
    out["_w"] = np.where(out["_full"] > 0, out["_cut"] / out["_full"], 0.0)
    return out


def statcan_units(aoi, layer, fields):
    """2021 census units of one boundary layer that intersect the AOI, apportioned."""
    feats, over = arcgis_query(f"{STATCAN_GEO}/{layer}", geom=aoi["geom"], out_fields=fields,
                               return_geometry=True, count=6000, timeout=180)
    if over:
        raise LookupError(f"StatCan boundary layer {layer} truncated its answer for this AOI")
    if not feats:
        raise LookupError(f"AOI intersects no 2021 census unit of boundary layer {layer}")
    return _apportion(_esri_gdf(feats), aoi)


def da_profile(dguids):
    """Census Profile values (columns PROFILE ids) for each DA DGUID; suppressed values are NaN."""
    parts = []
    for i in range(0, len(dguids), 40):
        key = f"A5.{'+'.join(dguids[i:i + 40])}.1.{'+'.join(PROFILE)}.1"
        r = S.get(f"{CENSUS_PROFILE}/{key}", timeout=180,
                  headers={"Accept": "application/vnd.sdmx.data+csv"})
        if r.status_code == 404:
            continue
        r.raise_for_status()
        parts.append(pd.read_csv(io.StringIO(r.text), dtype={"REF_AREA": str, "CHARACTERISTIC": str}))
    if not parts:
        raise LookupError("the Census Profile has no record for any DA in the AOI")
    t = pd.concat(parts)
    return (t.pivot_table(index="REF_AREA", columns="CHARACTERISTIC", values="OBS_VALUE", aggfunc="first")
             .reindex(index=dguids, columns=list(PROFILE)))


def _census_das(aoi):
    das = statcan_units(aoi, DA_LAYER, "DGUID")
    p = da_profile(sorted(set(das["DGUID"])))
    return p.reindex(das["DGUID"]).assign(_w=das["_w"].to_numpy())


def _apportioned(p, col):
    ok = p[col].notna()
    return round(float((p[col][ok] * p["_w"][ok]).sum()), 1) if ok.any() else None


def _weighted_mean(p, col, by):
    w = p[by] * p["_w"]
    ok = p[col].notna() & (w > 0)
    return round(float((p[col][ok] * w[ok]).sum() / w[ok].sum())) if ok.any() else None


ALTA = FTP + "/nrcan_rncan/vector/geobase_al_ta/shp_eng"
PRUID2PROV = {"10": "NL", "11": "PE", "12": "NS", "13": "NB", "24": "QC", "35": "ON",
              "46": "MB", "47": "SK", "48": "AB", "59": "BC", "60": "YT", "61": "NT",
              "62": "NU"}


def ca_alta(prov):
    """Aboriginal Lands of Canada (CLSS/NRCan), per-province shapefile.

    Per-province extracts are 30 KB - 10 MB; the national file is 55 MB and is
    not used.  Keyed by two-letter province code.
    """
    key = f"alta_{prov}"
    if key in _MEM:
        return _MEM[key]
    with _lock(key):
        if key in _MEM:
            return _MEM[key]
        pq = os.path.join(CACHE, f"alta_{prov}.parquet")
        if not os.path.exists(pq):
            z = _get(f"{ALTA}/AL_TA_{prov}_SHP_DCM_eng.zip",
                     os.path.join(CACHE, f"_alta_{prov}.zip"))
            import pyogrio
            lyrs = [l[0] for l in pyogrio.list_layers(z)]
            pick = [l for l in lyrs if "CONFIRMED" in l.upper()] or lyrs
            parts = []
            for l in pick:
                gg = gpd.read_file(z, layer=l, columns=["NAME1", "ALTYPE", "ALCODE", "JUR1"])
                if len(gg):
                    parts.append(gg.to_crs(4326))
            g = gpd.GeoDataFrame(pd.concat(parts, ignore_index=True), crs=4326)
            g.columns = [c.lower() if c != "geometry" else c for c in g.columns]
            g.to_parquet(pq, index=False)
            try:
                os.remove(z)
            except OSError:
                pass
        g = gpd.read_parquet(pq)
        g.sindex
        _MEM[key] = g
        return g


# ODHF facility types as the ODHF Version 1.0 metadata names them (NAICS 621-623):
# https://ftp.maps.canada.ca/pub/statcan_statcan/Health-care-facilities_Etablissement-de-sante/ODHF_BDOES/ODHF_Metadata.pdf
ODHF_URL = FTP + "/statcan_statcan/Health-care-facilities_Etablissement-de-sante/ODHF_BDOES/odhf_bdoes_v1.csv"
ODHF_TYPES = ["Ambulatory health care services", "Hospitals", "Nursing and residential care facilities"]


def ca_odhf():
    """Open Database of Healthcare Facilities v1.0 (StatCan 13-26-0001), geocoded rows."""
    key = "odhf"
    if key in _MEM:
        return _MEM[key]
    with _lock(key):
        if key in _MEM:
            return _MEM[key]
        pq = os.path.join(CACHE, "odhf_v1.parquet")
        if not os.path.exists(pq):
            c = _get(ODHF_URL, os.path.join(CACHE, "_odhf.csv"))
            d = pd.read_csv(c, encoding="latin-1", low_memory=False,
                            usecols=["facility_name", "odhf_facility_type", "province",
                                     "CSDname", "CSDuid", "latitude", "longitude"])
            d = d[d["latitude"].notna() & d["longitude"].notna()].reset_index(drop=True)
            # two rows spell a type in lower case
            d["odhf_facility_type"] = (d["odhf_facility_type"].str.strip().str.lower()
                                       .map({t.lower(): t for t in ODHF_TYPES}))
            d.to_parquet(pq, index=False)
            try:
                os.remove(c)
            except OSError:
                pass
        d = pd.read_parquet(pq)
        _MEM[key] = d
        return d


WDS = "https://www150.statcan.gc.ca/t1/wds/rest"
WDS_POPEST_PID = 17100155       # Population estimates, July 1, by census subdivision


def wds_csd_members():
    """classificationCode -> memberId for StatCan cube 17-10-0155 geography."""
    key = "wds_members"
    if key in _MEM:
        return _MEM[key]
    with _lock(key):
        if key in _MEM:
            return _MEM[key]
        jf = os.path.join(CACHE, "wds_17100155_geo.json")
        if not os.path.exists(jf):
            r = S.post(f"{WDS}/getCubeMetadata", json=[{"productId": WDS_POPEST_PID}],
                       headers={"Content-Type": "application/json"}, timeout=180)
            r.raise_for_status()
            o = r.json()[0]["object"]
            m = {str(x["classificationCode"]): int(x["memberId"])
                 for x in o["dimension"][0]["member"] if x.get("classificationCode")}
            tmp = jf + ".tmp"
            json.dump(m, open(tmp, "w"))
            os.replace(tmp, jf)
        m = json.load(open(jf))
        _MEM[key] = m
        return m


LODES = "https://lehd.ces.census.gov/data/lodes/LODES8"
FIPS2USPS = {
    "01": "al", "02": "ak", "04": "az", "05": "ar", "06": "ca", "08": "co", "09": "ct",
    "10": "de", "11": "dc", "12": "fl", "13": "ga", "15": "hi", "16": "id", "17": "il",
    "18": "in", "19": "ia", "20": "ks", "21": "ky", "22": "la", "23": "me", "24": "md",
    "25": "ma", "26": "mi", "27": "mn", "28": "ms", "29": "mo", "30": "mt", "31": "ne",
    "32": "nv", "33": "nh", "34": "nj", "35": "nm", "36": "ny", "37": "nc", "38": "nd",
    "39": "oh", "40": "ok", "41": "or", "42": "pa", "44": "ri", "45": "sc", "46": "sd",
    "47": "tn", "48": "tx", "49": "ut", "50": "vt", "51": "va", "53": "wa", "54": "wv",
    "55": "wi", "56": "wy", "72": "pr"}


def lodes_state(st):
    """LODES8 origin-destination + block-coordinate crosswalk for one state.

    Returns (od, xwalk, year).  od keeps only w_geocode / h_geocode / S000 so a
    big state stays ~25 MB on disk; earnings bands come from the WAC file.
    Years are probed newest-first because coverage differs by state (Michigan
    stops at 2021).
    """
    key = f"lodes_{st}"
    if key in _MEM:
        return _MEM[key]
    with _lock(key):
        if key in _MEM:
            return _MEM[key]
        odp = os.path.join(CACHE, f"lodes_{st}_od.parquet")
        xwp = os.path.join(CACHE, f"lodes_{st}_xwalk.parquet")
        wacp = os.path.join(CACHE, f"lodes_{st}_wac.parquet")
        yrf = os.path.join(CACHE, f"lodes_{st}_year.txt")
        if not (os.path.exists(odp) and os.path.exists(xwp) and os.path.exists(wacp)):
            year = None
            for y in range(2022, 2014, -1):
                h = S.head(f"{LODES}/{st}/od/{st}_od_main_JT00_{y}.csv.gz", timeout=60,
                           allow_redirects=True)
                if h.status_code == 200:
                    year = y
                    break
            if year is None:
                raise LookupError(f"no LODES8 od_main year available for state '{st}'")
            raw = _get(f"{LODES}/{st}/od/{st}_od_main_JT00_{year}.csv.gz",
                       os.path.join(CACHE, f"_lodes_{st}_od.csv.gz"))
            od = pd.read_csv(raw, compression="gzip",
                             usecols=["w_geocode", "h_geocode", "S000"],
                             dtype={"w_geocode": "int64", "h_geocode": "int64",
                                    "S000": "int32"})
            od.to_parquet(odp, index=False)
            os.remove(raw)

            raw = _get(f"{LODES}/{st}/{st}_xwalk.csv.gz",
                       os.path.join(CACHE, f"_lodes_{st}_xwalk.csv.gz"))
            xw = pd.read_csv(raw, compression="gzip",
                             usecols=["tabblk2020", "trct", "blklatdd", "blklondd"],
                             dtype={"tabblk2020": "int64", "trct": str},
                             low_memory=False)
            xw = xw[xw["blklatdd"].notna()].astype({"blklatdd": "float32",
                                                    "blklondd": "float32"})
            xw.to_parquet(xwp, index=False)
            os.remove(raw)

            raw = _get(f"{LODES}/{st}/wac/{st}_wac_S000_JT00_{year}.csv.gz",
                       os.path.join(CACHE, f"_lodes_{st}_wac.csv.gz"))
            wac = pd.read_csv(raw, compression="gzip",
                              usecols=["w_geocode", "C000", "CE01", "CE02", "CE03"],
                              dtype="int64")
            wac.to_parquet(wacp, index=False)
            os.remove(raw)
            json.dump(year, open(yrf + ".tmp", "w")); os.replace(yrf + ".tmp", yrf)
        year = json.load(open(yrf))
        out = (pd.read_parquet(odp), pd.read_parquet(xwp).set_index("tabblk2020"),
               pd.read_parquet(wacp).set_index("w_geocode"), year)
        _MEM[key] = out
        return out


# ============================================================= live-query APIs
TIGER2020 = "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/tigerWMS_Census2020/MapServer"
BCWFS = "https://openmaps.gov.bc.ca/geo/pub/wfs"
GHSL = "https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/GHSL"
PLACES = "https://data.cdc.gov/resource/yjkw-uj5s.json"      # PLACES tract, 2025


def _esri_rings(geom):
    """GeoJSON Polygon/MultiPolygon -> Esri polygon rings."""
    t = geom["type"]
    if t == "Polygon":
        polys = [geom["coordinates"]]
    elif t == "MultiPolygon":
        polys = geom["coordinates"]
    else:
        raise ValueError(f"unsupported AOI geometry type {t}")
    rings = []
    for poly in polys:
        for ring in poly:
            rings.append([[float(c[0]), float(c[1])] for c in ring])
    return rings


def arcgis_query(layer_url, geom=None, bbox=None, out_fields="*",
                 return_geometry=False, count=2000, timeout=90):
    """ArcGIS MapServer layer spatial query. Pass `geom` (GeoJSON) or `bbox`."""
    if geom is not None:
        g = {"rings": _esri_rings(geom), "spatialReference": {"wkid": 4326}}
        gtype = "esriGeometryPolygon"
    else:
        g = {"xmin": bbox[0], "ymin": bbox[1], "xmax": bbox[2], "ymax": bbox[3],
             "spatialReference": {"wkid": 4326}}
        gtype = "esriGeometryEnvelope"
    p = {"f": "json", "geometry": json.dumps(g), "geometryType": gtype, "inSR": 4326,
         "outSR": 4326, "spatialRel": "esriSpatialRelIntersects",
         "outFields": out_fields, "returnGeometry": str(bool(return_geometry)).lower(),
         "resultRecordCount": count}
    r = S.post(f"{layer_url}/query", data=p, timeout=timeout)
    if r.status_code != 200:
        raise ConnectionError(f"ArcGIS HTTP {r.status_code} from {layer_url}")
    d = r.json()
    if d.get("error"):
        raise ConnectionError(f"ArcGIS error {d['error'].get('message')} from {layer_url}")
    return d.get("features", []), bool(d.get("exceededTransferLimit"))


def _esri_gdf(feats):
    """Esri JSON polygon features -> GeoDataFrame (rings unioned per feature)."""
    from shapely.geometry import Polygon
    from shapely.ops import unary_union
    rows, geoms = [], []
    for f in feats:
        g = f.get("geometry") or {}
        rings = [r for r in g.get("rings", []) if len(r) >= 4]
        if not rings:
            continue
        polys = []
        for r in rings:
            p = Polygon(r)
            if not p.is_valid:
                p = p.buffer(0)
            if not p.is_empty:
                polys.append(p)
        if not polys:
            continue
        geoms.append(unary_union(polys))
        rows.append(f["attributes"])
    if not geoms:
        return gpd.GeoDataFrame({"_empty": []}, geometry=[], crs=4326)
    return gpd.GeoDataFrame(rows, geometry=geoms, crs=4326)


def wfs_features(type_name, bbox, count=10000, timeout=120, props=None):
    """BC Data Catalogue WFS 2.0 GetFeature, GeoJSON out, EPSG:4326 bbox."""
    p = {"service": "WFS", "version": "2.0.0", "request": "GetFeature",
         "typeNames": type_name, "outputFormat": "application/json",
         "srsName": "EPSG:4326", "count": count,
         "bbox": f"{bbox[1]},{bbox[0]},{bbox[3]},{bbox[2]},urn:ogc:def:crs:EPSG::4326"}
    if props:
        p["propertyName"] = props
    r = S.get(BCWFS, params=p, timeout=timeout)
    if r.status_code != 200:
        raise ConnectionError(f"BC WFS HTTP {r.status_code}")
    d = r.json()
    feats = d.get("features", [])
    if not feats:
        return gpd.GeoDataFrame(columns=["geometry"], geometry="geometry", crs=4326)
    return gpd.GeoDataFrame.from_features(feats, crs=4326)


# GHSL's 10-degree tile grid is NOT aligned to whole degrees: tile R1_C8 has
# left = -110.00791621 and top = 89.09958318, i.e. the grid origin is offset by
# about -0.0079 deg in longitude and -0.9004 deg in latitude from (-180, 90).
# Measured from the R2023A 4326 tiles of both GHS-POP (3ss) and GHS-SMOD (30ss),
# which share one grid.  Indexing off (-180, 90) puts AOIs within 0.9 deg below
# a 10-degree line in the tile BELOW the one that holds them; the read then
# falls outside the raster and boundless=True returns fill, not an error.  That
# is a silently wrong zero, so _ghs_tiles uses the measured origin and every
# reader additionally checks the AOI against the opened tile's real bounds.
GHS_OX, GHS_OY, GHS_STEP = -180.00791621, 89.09958318, 10.0


def _ghs_tiles(bbox, kind):
    """GHSL 10-degree tile ids covering bbox -> list of /vsizip//vsicurl/ URLs."""
    c0 = int((bbox[0] - GHS_OX) // GHS_STEP) + 1
    c1 = int((bbox[2] - GHS_OX) // GHS_STEP) + 1
    r0 = int((GHS_OY - bbox[3]) // GHS_STEP) + 1
    r1 = int((GHS_OY - bbox[1]) // GHS_STEP) + 1
    out = []
    for R in range(min(r0, r1), max(r0, r1) + 1):
        for C in range(min(c0, c1), max(c0, c1) + 1):
            if kind == "pop":
                stem = f"GHS_POP_E2020_GLOBE_R2023A_4326_3ss_V1_0_R{R}_C{C}"
                base = (f"{GHSL}/GHS_POP_GLOBE_R2023A/"
                        f"GHS_POP_E2020_GLOBE_R2023A_4326_3ss/V1-0/tiles/")
            else:
                stem = f"GHS_SMOD_E2020_GLOBE_R2023A_4326_30ss_V2_0_R{R}_C{C}"
                base = (f"{GHSL}/GHS_SMOD_GLOBE_R2023A/"
                        f"GHS_SMOD_E2020_GLOBE_R2023A_4326_30ss/V2-0/tiles/")
            out.append((f"R{R}_C{C}", f"/vsizip//vsicurl/{base}{stem}.zip/{stem}.tif"))
    return out


# The GHSL tiles are LZW-tiled COGs inside a zip, so every read pays a zip
# central-directory lookup plus a TIFF tile-index fetch.  Merging consecutive
# ranges and keeping a larger curl cache cuts the per-AOI round trips.
GDAL_ENV = dict(P.GDAL_ENV,
                CPL_VSIL_CURL_ALLOWED_EXTENSIONS=".zip,.tif",
                GDAL_HTTP_MERGE_CONSECUTIVE_RANGES="YES",
                CPL_VSIL_CURL_CACHE_SIZE="268435456",
                GDAL_INGESTED_BYTES_AT_OPEN="65536")


_TILES = {}


def _ghs_open(url):
    """Open a GHSL tile once per process and reuse it (dataset, lock).

    Every read of a zipped COG over HTTP pays a zip central-directory parse and
    a TIFF tile-index fetch; keeping the handle open removes that from the
    per-AOI cost.  rasterio datasets are not safe for concurrent reads, so each
    handle carries its own lock.
    """
    ent = _TILES.get(url)
    if ent is not None:
        return ent
    with _lock("ghs_open"):
        ent = _TILES.get(url)
        if ent is None:
            with rasterio.Env(**GDAL_ENV):
                src = rasterio.open(url)
            ent = (src, threading.Lock())
            _TILES[url] = ent
        return ent


def _covers(src, bbox):
    """True if the AOI bbox actually intersects this tile's raster extent."""
    b = src.bounds
    return not (bbox[2] <= b.left or bbox[0] >= b.right
                or bbox[3] <= b.bottom or bbox[1] >= b.top)


def ghs_pop_window(bbox, geom, maxside=4096):
    """Native-resolution, AOI-masked GHS-POP read.

    Population is a COUNT per cell, so this must NOT be decimated the way
    pipeline.read_window decimates (nearest-neighbour subsampling would corrupt
    the sum).  AOI windows here are at most a few hundred pixels a side.
    """
    acc, used, skipped = None, [], []
    for tid, url in _ghs_tiles(bbox, "pop"):
        try:
            src, lk = _ghs_open(url)
        except rasterio.errors.RasterioIOError:
            continue                      # tile absent (all-ocean 10-degree cell)
        if not _covers(src, bbox):
            skipped.append(tid)           # grid-index guard: never read outside
            continue
        with lk:
            with rasterio.Env(**GDAL_ENV):
                w = win_from_bounds(*bbox, transform=src.transform)
                h, wd = int(round(w.height)), int(round(w.width))
                if h > maxside or wd > maxside:
                    raise ValueError(f"AOI window {h}x{wd} exceeds cap {maxside}")
                a = src.read(1, window=w, boundless=True, fill_value=0.0).astype("f8")
        a = np.where(np.isfinite(a) & (a > 0), a, 0.0)
        if a.size == 0:
            continue
        # every tile is read on the SAME AOI window grid (identical 3 arcsec
        # transform) and is zero-filled outside its own extent, so tiles that
        # straddle the AOI accumulate without double counting.
        acc = a if acc is None else acc + a[:acc.shape[0], :acc.shape[1]]
        used.append(tid)
    if acc is None:
        raise LookupError(
            f"no GHS-POP tile covers this AOI (candidates "
            f"{[t for t, _ in _ghs_tiles(bbox, 'pop')]}, outside-extent {skipped})")
    tr = aff_from_bounds(*bbox, acc.shape[1], acc.shape[0])
    m = geometry_mask([geom], out_shape=acc.shape, transform=tr, invert=True)
    return acc[m], used


# ================================================== census_demography (Canada)
def _csds(aoi):
    return statcan_units(aoi, CSD_LAYER, "CSDUID,CSDNAME,CSDTYPE,PRUID").sort_values("_cut", ascending=False)


def _a_statcan_census(aoi):
    """2021 Census counts apportioned to the AOI from dissemination areas, with the CSD context."""
    if aoi["country"] != "CA":
        raise PermissionError("out of coverage (Canada-only)")
    p = _census_das(aoi)
    csd = _csds(aoi)
    dom = csd.iloc[0]
    f = {"statcan_population": _apportioned(p, "1"),
         "statcan_private_dwellings": _apportioned(p, "4"),
         "statcan_occupied_private_dwellings": _apportioned(p, "5"),
         "statcan_private_households": _apportioned(p, "50"),
         "statcan_median_household_income_approx": _weighted_mean(p, "229", "50"),
         "statcan_n_da": int(len(p)),
         "statcan_csd_uid": str(dom["CSDUID"]),
         "statcan_csd_name": str(dom["CSDNAME"]),
         "statcan_csd_type": str(dom["CSDTYPE"]),
         "statcan_csd_n_overlapping": int(len(csd)),
         "statcan_csd_dominant_frac": round(float(dom["_cut"] / csd["_cut"].sum()), 4)}
    return {k: v for k, v in f.items() if v is not None}, {
        "obs_date": "2021", "res_m": "admin:da", "item": f"DA n={len(p)}",
        "release": CENSUS_RELEASE, "url": CENSUS_PROFILE}


def _a_statcan_wds_popest(aoi):
    """Population estimate (July 1) of the AOI's dominant 2021 CSD, StatCan cube 17-10-0155.

    Falls back to the province row when the CSD code is not in the cube
    (the estimate programme does not carry every small subdivision); res_m
    records which level actually answered.
    """
    if aoi["country"] != "CA":
        raise PermissionError("out of coverage (Canada-only)")
    csduid = str(_csds(aoi).iloc[0]["CSDUID"])
    mem = wds_csd_members()
    level, code = "csd", csduid
    mid = mem.get(csduid)
    if mid is None:
        code = csduid[:2]
        mid = mem.get(code)
        level = "province"
    if mid is None:
        raise LookupError(f"neither CSDUID {csduid} nor PRUID {csduid[:2]} in cube 17-10-0155")
    r = S.post(f"{WDS}/getDataFromCubePidCoordAndLatestNPeriods",
               json=[{"productId": WDS_POPEST_PID,
                      "coordinate": f"{mid}.0.0.0.0.0.0.0.0.0", "latestN": 2}],
               headers={"Content-Type": "application/json"}, timeout=90)
    if r.status_code != 200:
        raise ConnectionError(f"StatCan WDS HTTP {r.status_code}")
    o = r.json()[0]
    if o.get("status") != "SUCCESS":
        raise LookupError(f"WDS status {o.get('status')}")
    dps = {p["refPerRaw"][:4]: p["value"] for p in o["object"]["vectorDataPoint"]
           if p.get("value") is not None}
    if not dps:
        raise LookupError("WDS returned no population data points")
    year = max(dps)
    prev = dps.get(str(int(year) - 1))
    f = {"statcan_popest": int(dps[year]),
         "statcan_popest_geo_level": level,
         "statcan_popest_geo_code": code}
    if prev:
        f["statcan_popest_yoy_pct"] = round(100.0 * (dps[year] - prev) / prev, 3)
    return f, {"obs_date": f"{year}-07-01", "res_m": f"admin:{level}",
               "item": f"vector {o['object']['vectorId']}",
               "release": "StatCan table 17-10-0155-01, 2021 boundaries",
               "url": f"{WDS}/getDataFromCubePidCoordAndLatestNPeriods"}


# ====================================================== census_demography (US)
def _tiger_blocks(aoi):
    """2020 decennial blocks intersecting the AOI, with areal weights."""
    feats, over = arcgis_query(f"{TIGER2020}/10", geom=aoi["geom"],
                              out_fields="GEOID,POP100,HU100,AREALAND,TRACT,STATE,COUNTY",
                              return_geometry=True, count=4000)
    if not feats:
        raise LookupError("no 2020 census block intersects this AOI "
                          "(outside US TIGERweb coverage)")
    if over:
        raise LookupError("TIGERweb truncated the 2020 block answer for this AOI")
    return _apportion(_esri_gdf(feats), aoi)


def _a_tigerweb_blocks(aoi):
    """US 2020 decennial population / housing, areally apportioned to the AOI."""
    if aoi["country"] != "US":
        raise PermissionError("out of coverage (US-only)")
    b = _tiger_blocks(aoi)
    if len(b) == 0:
        raise LookupError("blocks returned but none overlap the AOI polygon")
    pop = pd.to_numeric(b["POP100"], errors="coerce").fillna(0).to_numpy(dtype="f8")
    hu = pd.to_numeric(b["HU100"], errors="coerce").fillna(0).to_numpy(dtype="f8")
    w = b["_w"].to_numpy(dtype="f8")
    aoi_km2 = _area_km2(shape(aoi["geom"]))
    apop, ahu = float((pop * w).sum()), float((hu * w).sum())
    f = {"tiger_block_pop_2020": int(round(apop)),
         "tiger_block_housing_units_2020": int(round(ahu)),
         "tiger_pop_density_km2": round(apop / aoi_km2, 2) if aoi_km2 > 0 else None,
         "tiger_housing_density_km2": round(ahu / aoi_km2, 2) if aoi_km2 > 0 else None,
         "tiger_n_blocks": int(len(b)),
         "tiger_n_tracts": int(b["TRACT"].nunique()),
         "tiger_mean_block_area_km2": round(float(b["_full"].mean()) / 1e6, 4),
         "tiger_persons_per_housing_unit": round(apop / ahu, 3) if ahu > 0 else None,
         "tiger_state_fips": str(b["STATE"].iloc[0]),
         "tiger_county_fips": str(b["STATE"].iloc[0]) + str(b["COUNTY"].iloc[0])}
    f = {k: v for k, v in f.items() if v is not None}
    return f, {"obs_date": "2020-04-01", "res_m": "admin:block",
               "item": f"blocks n={len(b)}", "release": "TIGERweb tigerWMS_Census2020, 2020 Census POP100/HU100",
               "url": f"{TIGER2020}/10"}


def _a_acs_income(aoi):
    """ACS 5-year socio-economic attributes -- BLOCKED, recorded not embedded.

    api.census.gov /data/ endpoints return an HTML 'Missing Key' page for every
    query (verified at tract and block-group level on 2023/acs/acs5).  A key is
    free but this module ships without credentials, so the gap is recorded
    rather than papered over.  /data/.../variables.json still answers keylessly,
    which is what distinguishes a key wall from an outage -- this adapter probes
    both so the dossier records the real reason.
    """
    if aoi["country"] != "US":
        raise PermissionError("out of coverage (US-only)")
    meta = S.get("https://api.census.gov/data/2023/acs/acs5/variables.json", timeout=60)
    q = S.get("https://api.census.gov/data/2023/acs/acs5",
              params={"get": "NAME,B19013_001E", "for": "tract:*",
                      "in": f"state:{aoi.get('_state_fips', '19')}"}, timeout=60)
    body = (q.text or "")[:400].lower()
    if q.status_code == 200 and body.lstrip().startswith("[["):
        raise LookupError("ACS answered keylessly after all -- revisit this adapter")
    reason = "missing key" if "missing key" in body else f"HTTP {q.status_code}"
    raise ConnectionError(
        f"api.census.gov ACS5 requires an API key ({reason}); metadata endpoint "
        f"HTTP {meta.status_code} so the host is reachable. No key embedded by "
        f"design -- set one up at api.census.gov/data/key_signup.html to enable "
        f"median income / age structure for US AOIs.")


# ========================================================== population_gridded
def _a_ghs_pop(aoi):
    """GHS-POP R2023A, 2020 epoch, 3 arcsec (~100 m) -- a true gridded measure.

    Read windowed out of the zipped LZW-tiled COGs on the JRC open-data host via
    /vsizip//vsicurl/; nothing is downloaded whole (the 10-degree tiles run to
    179 MB).
    """
    v, tiles = ghs_pop_window(aoi["bbox"], aoi["geom"])
    if v.size == 0:
        raise LookupError("AOI mask selected no GHS-POP cells")
    aoi_km2 = _area_km2(shape(aoi["geom"]))
    tot = float(v.sum())
    f = {"ghspop_total_persons": round(tot, 1),
         "ghspop_density_km2": round(tot / aoi_km2, 2) if aoi_km2 > 0 else None,
         "ghspop_cell_max": round(float(v.max()), 3),
         "ghspop_cell_p90": round(float(np.percentile(v, 90)), 3),
         "ghspop_cell_mean": round(float(v.mean()), 4),
         "ghspop_populated_cell_frac": round(float((v > 0.1).mean()), 4),
         "ghspop_n_cells": int(v.size)}
    f = {k: val for k, val in f.items() if val is not None}
    return f, {"obs_date": "2020", "res_m": 100, "item": ",".join(tiles),
               "release": "GHS_POP_E2020_GLOBE_R2023A_4326_3ss V1-0",
               "url": f"{GHSL}/GHS_POP_GLOBE_R2023A/GHS_POP_E2020_GLOBE_R2023A_4326_3ss/V1-0/tiles/"}


# =============================================================== urban_extent
# GHS-SMOD class names as GHSL publishes them: GHSL Data Package 2023 (JRC 133256), section
# 2.6, level-2 classes 10-30 and level-1 classes 1-3 (the "per-urban" typo in the class-21
# line is corrected from Table 21's grid-level term "SUBURBAN OR PERI-URBAN GRID CELL").
# https://ghsl.jrc.ec.europa.eu/documents/GHSL_Data_Package_2023.pdf
SMOD_L2 = {30: "Urban Centre grid cell", 23: "Dense Urban Cluster grid cell",
           22: "Semi-dense Urban Cluster grid cell", 21: "Suburban or peri-urban grid cell",
           13: "Rural cluster grid cell", 12: "Low Density Rural grid cell",
           11: "Very low density rural grid cell", 10: "Water grid cell"}
SMOD_L1 = {30: "Urban Centre grid cell", 23: "Urban Cluster grid cell", 22: "Urban Cluster grid cell",
           21: "Urban Cluster grid cell", 13: "Rural grid cell", 12: "Rural grid cell",
           11: "Rural grid cell"}
SMOD_CELL = 1.0 / 120.0          # 30 arcsec


def class_records(v, names):
    """One {class, value} record per published class name (zeros included) and the dominant name."""
    codes, cnt = np.unique(v, return_counts=True)
    frac = dict(zip(codes.tolist(), (cnt / v.size).tolist()))
    by = {}
    for code, name in names.items():
        by[name] = by.get(name, 0.0) + frac.get(code, 0.0)
    return [{"class": n, "value": round(x, 4)} for n, x in by.items()], max(by, key=by.get)


def _a_ghs_smod(aoi):
    """GHS-SMOD R2023A degree-of-urbanisation grid, 2020 epoch, 1 km.

    Most AOIs in this dossier are smaller than one 1 km SMOD cell, so the read
    window is padded to at least 3 cells a side and centred on the AOI; a
    sub-pixel AOI gets the settlement class of its neighbourhood rather than a
    fractional-window read that degenerates to nodata.  The padding is recorded
    in ghssmod_subpixel_aoi / ghssmod_window_deg.
    """
    x0, y0, x1, y1 = aoi["bbox"]
    sub = (x1 - x0) < 3 * SMOD_CELL or (y1 - y0) < 3 * SMOD_CELL
    if sub:
        cx, cy = (x0 + x1) / 2.0, (y0 + y1) / 2.0
        h = 1.5 * SMOD_CELL
        bb = [min(x0, cx - h), min(y0, cy - h), max(x1, cx + h), max(y1, cy + h)]
        mask_geom = None
    else:
        bb, mask_geom = list(aoi["bbox"]), aoi["geom"]
    arrs, used, skipped = [], [], []
    for tid, url in _ghs_tiles(bb, "smod"):
        try:
            src, lk = _ghs_open(url)
        except rasterio.errors.RasterioIOError:
            continue
        if not _covers(src, bb):
            skipped.append(tid)
            continue
        with lk:
            with rasterio.Env(**GDAL_ENV):
                w = win_from_bounds(*bb, transform=src.transform)
                oh = max(1, int(round(w.height)))
                ow = max(1, int(round(w.width)))
                a = src.read(1, window=w, out_shape=(oh, ow), boundless=True,
                             fill_value=src.nodata if src.nodata is not None else -200)
        a = np.ma.masked_equal(a, src.nodata if src.nodata is not None else -200)
        if mask_geom is not None and a.size > 4:
            tr = aff_from_bounds(*bb, a.shape[1], a.shape[0])
            m = geometry_mask([mask_geom], out_shape=a.shape, transform=tr, invert=True)
            a = np.ma.masked_array(a, mask=a.mask | ~m)
        v = a.compressed()
        v = v[np.isin(v, list(SMOD_L2))]
        if v.size:
            arrs.append(v)
            used.append(tid)
    if not arrs:
        raise LookupError(
            f"no GHS-SMOD tile / no valid settlement class over this AOI "
            f"(candidates {[t for t, _ in _ghs_tiles(bb, 'smod')]}, "
            f"outside-extent {skipped})")
    v = np.concatenate(arrs)
    l2, dom = class_records(v, SMOD_L2)
    # level 1 has no water class, so its fractions share level 2's denominator and omit water
    l1, _ = class_records(v, SMOD_L1)
    f = {"ghssmod_frac": l2,
         "ghssmod_group_frac": l1,
         "ghssmod_dominant_class": dom,
         "ghssmod_n_cells": int(v.size),
         "ghssmod_subpixel_aoi": bool(sub),
         "ghssmod_window_deg": round(max(bb[2] - bb[0], bb[3] - bb[1]), 5)}
    return f, {"obs_date": "2020", "res_m": 1000, "item": ",".join(used),
               "release": "GHS_SMOD_E2020_GLOBE_R2023A_4326_30ss V2-0",
               "url": f"{GHSL}/GHS_SMOD_GLOBE_R2023A/GHS_SMOD_E2020_GLOBE_R2023A_4326_30ss/V2-0/tiles/"}


# Impact Observatory class names as the collection's item_assets file:values publish them:
# https://planetarycomputer.microsoft.com/api/stac/v1/collections/io-lulc-annual-v02
IO_LULC = {1: "Water", 2: "Trees", 4: "Flooded vegetation", 5: "Crops", 7: "Built area",
           8: "Bare ground", 9: "Snow/ice", 10: "Clouds", 11: "Rangeland"}


def _a_iolulc(aoi):
    """Impact Observatory 10 m annual land use / land cover class fractions."""
    it = pc_item("io-lulc-annual-v02", aoi["bbox"], limit=20)
    if not it:
        raise LookupError("no io-lulc-annual-v02 item for this AOI")
    i, a = read_best(it, "data", aoi)
    v = a.compressed()
    v = v[v != 0]
    if v.size == 0:
        raise LookupError("io-lulc window is all nodata")
    recs, dom = class_records(v, IO_LULC)
    built = float((v == 7).mean())
    f = {"iolulc_frac": recs,
         "iolulc_dominant": dom,
         "iolulc_n_classes": int(len(np.unique(v))),
         "iolulc_built_area_km2": round(built * _area_km2(shape(aoi["geom"])), 4)}
    return f, {"obs_date": (_dt(i) or "unknown")[:10], "res_m": 10, "item": i.id,
               "release": "io-lulc-annual-v02",
               "url": "https://planetarycomputer.microsoft.com/api/stac/v1/collections/io-lulc-annual-v02"}


# =============================================================== mobility_flows
def _a_lodes_flows(aoi):
    """LEHD LODES8 commute flows for the blocks the AOI covers.

    jobs-in  = jobs located in AOI blocks (sum S000 over w_geocode)
    jobs-out = jobs held by AOI residents (sum S000 over h_geocode)
    Commute distances are great-circle between LODES block centroids, weighted
    by job count.  od_main is within-state only, so cross-border commuting
    (relevant at Detroit/Windsor) is NOT counted -- see lodes_scope.
    """
    if aoi["country"] != "US":
        raise PermissionError("out of coverage (LODES is US-only)")
    b = _tiger_blocks(aoi)
    if len(b) == 0:
        raise LookupError("no 2020 census block overlaps the AOI")
    st_fips = str(b["STATE"].iloc[0])
    st = FIPS2USPS.get(st_fips)
    if st is None:
        raise LookupError(f"no LODES state code for FIPS {st_fips}")
    blocks = set(int(x) for x in b["GEOID"].astype(str))
    od, xw, wac, year = lodes_state(st)

    inb = od[od["w_geocode"].isin(blocks)]
    out = od[od["h_geocode"].isin(blocks)]
    if len(inb) == 0 and len(out) == 0:
        # A verified absence, not a failure: the state table loaded, the AOI
        # resolved to real 2020 blocks, and none of them appears as an origin or
        # destination.  Typical of an open-pit mine or other unpopulated block.
        if len(od) == 0:
            raise LookupError(f"{st.upper()} LODES {year} od_main table is empty")
        return ({"lodes_jobs_in_aoi": 0, "lodes_jobs_held_by_residents": 0,
                 "lodes_flow_records_matched": 0,
                 "lodes_n_blocks_queried": int(len(blocks)),
                 "lodes_state": st.upper()},
                {"obs_date": str(year), "res_m": "admin:block",
                 "item": f"{st}_od_main_JT00_{year}", "release": "LODES8",
                 "url": f"{LODES}/{st}/od/{st}_od_main_JT00_{year}.csv.gz"})

    def wdist(df, acol, bcol):
        if len(df) == 0:
            return None, None
        A = xw.reindex(df[acol].to_numpy())
        B = xw.reindex(df[bcol].to_numpy())
        ok = A["blklatdd"].notna().to_numpy() & B["blklatdd"].notna().to_numpy()
        if not ok.any():
            return None, None
        d = _haversine(A["blklatdd"].to_numpy()[ok], A["blklondd"].to_numpy()[ok],
                       B["blklatdd"].to_numpy()[ok], B["blklondd"].to_numpy()[ok])
        w = df["S000"].to_numpy(dtype="f8")[ok]
        if w.sum() <= 0:
            return None, None
        o = np.argsort(d)
        cw = np.cumsum(w[o]) / w.sum()
        p90 = float(d[o][int(np.searchsorted(cw, 0.90))]) if cw.size else None
        return float((d * w).sum() / w.sum()), p90

    jobs_in = int(inb["S000"].sum())
    jobs_out = int(out["S000"].sum())
    din, din90 = wdist(inb, "w_geocode", "h_geocode")
    dout, dout90 = wdist(out, "h_geocode", "w_geocode")
    f = {"lodes_jobs_in_aoi": jobs_in,
         "lodes_jobs_held_by_residents": jobs_out,
         "lodes_flow_records_matched": int(len(inb) + len(out)),
         "lodes_n_blocks_queried": int(len(blocks)),
         "lodes_jobs_balance_ratio": round(jobs_in / jobs_out, 3) if jobs_out else None,
         "lodes_n_origin_blocks": int(inb["h_geocode"].nunique()),
         "lodes_n_destination_blocks": int(out["w_geocode"].nunique()),
         "lodes_mean_inbound_commute_km": round(din, 2) if din is not None else None,
         "lodes_p90_inbound_commute_km": round(din90, 2) if din90 is not None else None,
         "lodes_mean_outbound_commute_km": round(dout, 2) if dout is not None else None,
         "lodes_internal_job_frac": round(
             float(inb[inb["h_geocode"].isin(blocks)]["S000"].sum()) / jobs_in, 4)
             if jobs_in else None,
         "lodes_state": st.upper()}
    we = wac.reindex(sorted(blocks)).dropna(how="all")
    if len(we) and float(we["C000"].sum()) > 0:
        tot = float(we["C000"].sum())
        f.update({"lodes_wac_jobs_total": int(tot),
                  "lodes_wac_frac_earn_low": round(float(we["CE01"].sum()) / tot, 4),
                  "lodes_wac_frac_earn_mid": round(float(we["CE02"].sum()) / tot, 4),
                  "lodes_wac_frac_earn_high": round(float(we["CE03"].sum()) / tot, 4)})
    f = {k: v for k, v in f.items() if v is not None}
    return f, {"obs_date": str(year), "res_m": "admin:block",
               "item": f"{st}_od_main_JT00_{year}", "release": "LODES8",
               "url": f"{LODES}/{st}/od/{st}_od_main_JT00_{year}.csv.gz"}


# ========================================================== property_assessment
def _a_statcan_dwelling_value(aoi):
    """2021 Census owner-reported dwelling value across the AOI's dissemination areas.

    A census self-reported value, NOT an assessment roll.
    """
    if aoi["country"] != "CA":
        raise PermissionError("out of coverage (Canada-only)")
    p = _census_das(aoi)
    f = {"statcan_avg_dwelling_value_approx": _weighted_mean(p, "1475", "1468"),
         "statcan_median_dwelling_value_approx": _weighted_mean(p, "1474", "1468"),
         "statcan_owner_households": _apportioned(p, "1468")}
    f = {k: v for k, v in f.items() if v is not None}
    if not f:
        raise LookupError("dwelling value suppressed for every overlapping DA "
                          "(StatCan confidentiality screen)")
    f["statcan_dwelling_value_n_da"] = int(p["1475"].notna().sum())
    return f, {"obs_date": "2021", "res_m": "admin:da", "item": f"DA n={len(p)}",
               "release": CENSUS_RELEASE, "url": CENSUS_PROFILE}


# ===================================================== healthcare_accessibility
def _a_odhf_access(aoi):
    """Distance from the AOI centroid to the nearest healthcare facility (StatCan ODHF).

    A proximity measurement from point data, not an areal statistic.
    """
    if aoi["country"] != "CA":
        raise PermissionError("out of coverage (Canada-only)")
    d = ca_odhf()
    cx, cy = aoi["centroid"]
    km = _haversine(cy, cx, d["latitude"].to_numpy(dtype="f8"),
                    d["longitude"].to_numpy(dtype="f8"))
    if km.size == 0 or not np.isfinite(km).any():
        raise LookupError("ODHF carries no usable coordinates")
    order = np.argsort(km)
    nearest = d.iloc[int(order[0])]
    f = {"odhf_nearest_facility_km": round(float(km[order[0]]), 3),
         "odhf_nearest_facility_type": nearest["odhf_facility_type"] if pd.notna(nearest["odhf_facility_type"]) else None,
         "odhf_nearest_facility_name": str(nearest["facility_name"]),
         "odhf_n_within_5km": int((km <= 5).sum()),
         "odhf_n_within_10km": int((km <= 10).sum()),
         "odhf_n_within_25km": int((km <= 25).sum()),
         "odhf_n_within_50km": int((km <= 50).sum())}
    types = d["odhf_facility_type"].to_numpy()
    f["odhf_nearest_by_type_km"] = [{"class": t, "value": round(float(km[types == t].min()), 3)}
                                    for t in ODHF_TYPES]
    f["odhf_n_by_type_within_25km"] = [{"class": t, "value": int((km[types == t] <= 25).sum())}
                                       for t in ODHF_TYPES]
    f = {k: v for k, v in f.items() if v is not None}
    return f, {"obs_date": "2020", "res_m": "point", "item": "odhf_bdoes_v1.csv",
               "release": "ODHF Version 1.0 (StatCan 13-26-0001)", "url": ODHF_URL}


def _a_places_access(aoi):
    """CDC PLACES tract-level healthcare-access barriers, population-weighted.

    access2 = adults 18-64 without health insurance; checkup = adults with a
    routine checkup in the past year; lacktrpt = adults lacking reliable
    transportation.  Modelled small-area estimates, hence admin:tract.
    """
    if aoi["country"] != "US":
        raise PermissionError("out of coverage (CDC PLACES is US-only)")
    b = _tiger_blocks(aoi)
    if len(b) == 0:
        raise LookupError("no 2020 census block overlaps the AOI")
    tracts = sorted({f"{r.STATE}{r.COUNTY}{r.TRACT}" for r in b.itertuples()})
    if not tracts:
        raise LookupError("could not assemble tract FIPS codes from TIGERweb blocks")
    q = ",".join(f"'{t}'" for t in tracts[:60])
    r = S.get(PLACES, params={"$where": f"tractfips in ({q})", "$limit": 200}, timeout=90)
    if r.status_code != 200:
        raise ConnectionError(f"data.cdc.gov HTTP {r.status_code}")
    rows = r.json()
    if not rows:
        raise LookupError(f"PLACES has no record for tracts {tracts[:4]} "
                          f"(n={len(tracts)} queried)")
    df = pd.DataFrame(rows)
    pop = pd.to_numeric(df.get("totalpopulation"), errors="coerce").fillna(0).to_numpy("f8")
    if pop.sum() <= 0:
        pop = np.ones(len(df))

    def pw(col):
        if col not in df:
            return None
        v = pd.to_numeric(df[col], errors="coerce").to_numpy("f8")
        ok = np.isfinite(v)
        return float((v[ok] * pop[ok]).sum() / pop[ok].sum()) if ok.any() and pop[ok].sum() else None

    f = {"places_tract_population": int(pop.sum()),
         "places_n_tracts": int(len(df)),
         "places_uninsured_pct": pw("access2_crudeprev"),
         "places_routine_checkup_pct": pw("checkup_crudeprev"),
         "places_no_dental_visit_pct": pw("dental_crudeprev"),
         "places_lacks_transport_pct": pw("lacktrpt_crudeprev"),
         "places_fair_poor_health_pct": pw("ghlth_crudeprev"),
         "places_disability_pct": pw("disability_crudeprev"),
         "places_food_insecurity_pct": pw("foodinsecu_crudeprev")}
    f = {k: (round(v, 2) if isinstance(v, float) else v)
         for k, v in f.items() if v is not None}
    if len(f) <= 2:
        raise LookupError("PLACES rows carried no usable prevalence measures")
    return f, {"obs_date": "2025-release", "res_m": "admin:tract", "item": f"tracts n={len(df)}",
               "release": "PLACES 2025 release, census tract", "url": PLACES}


# ============================================================ indigenous_lands
def _a_alta_indigenous(aoi):
    """Aboriginal Lands of Canada (NRCan CLSS): AOI overlap fraction.

    Reports the measured overlap fraction (0.0 is a verified absence, not a
    missing value) plus the distance to the nearest parcel.
    """
    if aoi["country"] != "CA":
        raise PermissionError("out of coverage (Canada-only)")
    # every province the AOI touches: an AOI on a border (Labrador / Quebec) has its
    # nearest parcels in the neighbouring province's extract
    provs = sorted({PRUID2PROV[u] for u in _csds(aoi)["PRUID"].astype(str) if u in PRUID2PROV})
    if not provs:
        raise LookupError("could not resolve AOI province for the ALTA extract")
    al = pd.concat([ca_alta(p) for p in provs], ignore_index=True)
    prov = ",".join(provs)
    clip, aoi_km2 = _overlap(aoi, al)
    ov_km2 = float(clip.geometry.area.sum()) / 1e6 if len(clip) else 0.0
    cx, cy = aoi["centroid"]
    # distances in an equidistant projection centred on the AOI; the equal-area EQA
    # stretches north-south distance at these latitudes
    aeqd = f"+proj=aeqd +lat_0={cy} +lon_0={cx} +datum=WGS84"
    pt = gpd.GeoSeries([shape({"type": "Point", "coordinates": [cx, cy]})], crs=4326).to_crs(aeqd).iloc[0]
    dists = al.to_crs(aeqd).distance(pt)
    f = {"alta_overlap_frac": round(min(ov_km2 / aoi_km2, 1.0) if aoi_km2 > 0 else 0.0, 5),
         "alta_overlap_km2": round(ov_km2, 5),
         "alta_n_parcels_intersecting": int(len(clip)),
         "alta_nearest_km": round(float(dists.min()) / 1000.0, 3) if len(dists) else None,
         "alta_province_extract": prov,
         "alta_n_parcels_in_province": int(len(al))}
    if len(clip):
        top = clip.assign(_a=clip.geometry.area).sort_values("_a", ascending=False).iloc[0]
        f["alta_dominant_name"] = top.get("name1")
        f["alta_dominant_type"] = top.get("altype")
        f["alta_dominant_code"] = top.get("alcode")
    else:
        i = int(dists.to_numpy().argmin())
        f["alta_nearest_name"] = al.iloc[i].get("name1")
        f["alta_nearest_type"] = al.iloc[i].get("altype")
    f = {k: v for k, v in f.items() if v is not None and v == v}
    return f, {"obs_date": "2026-10 (CLSS snapshot)", "res_m": "vector",
               "item": ",".join(f"AL_TA_{p}_SHP_DCM_eng.zip" for p in provs),
               "release": "Aboriginal Lands of Canada Legislative Boundaries (NRCan CLSS), per-province SHP",
               "url": f"{ALTA}/"}


def _a_tiger_aiannh(aoi):
    """US federal American Indian reservations + off-reservation trust lands."""
    if aoi["country"] != "US":
        raise PermissionError("out of coverage (US-only)")
    aoi_km2 = _area_km2(shape(aoi["geom"]))
    recs, names, total = [], [], 0.0
    # layer names as TIGERweb publishes them
    for lid, layer in ((34, "Federal American Indian Reservations"), (36, "Off-Reservation Trust Lands")):
        feats, _ = arcgis_query(f"{TIGER2020}/{lid}", geom=aoi["geom"],
                                out_fields="GEOID,NAME,AREALAND", return_geometry=True, count=500)
        sub = 0.0
        if feats:
            clip = _apportion(_esri_gdf(feats), aoi)
            sub = float(clip["_cut"].sum()) / 1e6
            names += clip["NAME"].astype(str).tolist()
        total += sub
        recs.append({"class": layer, "value": round(min(sub / aoi_km2, 1.0), 5)})
    f = {"aiannh_frac": recs,
         "aiannh_overlap_km2": round(total, 5),
         "aiannh_overlap_frac": round(min(total / aoi_km2, 1.0), 5),
         "aiannh_n_areas": len(names)}
    if names:
        f["aiannh_names"] = "; ".join(names)
    return f, {"obs_date": "2020", "res_m": "vector", "item": "layers 34,36",
               "release": "TIGERweb tigerWMS_Census2020", "url": TIGER2020}


# ====================================== parcels_cadastre / crown_land (BC only)
PMBC = "pub:WHSE_CADASTRE.PMBC_PARCEL_FABRIC_POLY_SVW"
TANTALIS = "pub:WHSE_TANTALIS.TA_CROWN_TENURES_SVW"


def _bc_guard(aoi):
    """BC Data Catalogue covers British Columbia only."""
    if aoi["country"] != "CA":
        raise PermissionError("out of coverage (BC Data Catalogue is Canada/BC-only)")
    x0, y0, x1, y1 = aoi["bbox"]
    if not (-139.1 <= x0 <= -114.0 and -139.1 <= x1 <= -114.0
            and 48.2 <= y0 <= 60.1 and 48.2 <= y1 <= 60.1):
        raise PermissionError("out of coverage (outside British Columbia)")


def _a_bc_parcels(aoi):
    """ParcelMap BC parcel fabric: count, size distribution, tenure mix."""
    _bc_guard(aoi)
    g = wfs_features(PMBC, aoi["bbox"], count=20000)
    if len(g) == 0:
        raise LookupError("ParcelMap BC returned no parcel in the AOI bbox")
    clip, aoi_km2 = _overlap(aoi, g)
    if len(clip) == 0:
        raise LookupError("parcels found in bbox but none intersect the AOI polygon")
    full = g.iloc[list(g.sindex.query(shape(aoi["geom"]), predicate="intersects"))]
    a_full = full.to_crs(EQA).area / 1e6
    a_cut = clip.geometry.area / 1e6
    f = {"bcpmbc_parcel_count": int(len(clip)),
         "bcpmbc_parcel_density_per_km2": round(len(clip) / aoi_km2, 2) if aoi_km2 > 0 else None,
         "bcpmbc_median_parcel_area_km2": round(float(a_full.median()), 6),
         "bcpmbc_mean_parcel_area_km2": round(float(a_full.mean()), 6),
         "bcpmbc_max_parcel_area_km2": round(float(a_full.max()), 6),
         "bcpmbc_parcel_coverage_frac": round(min(float(a_cut.sum()) / aoi_km2, 1.0), 4)
             if aoi_km2 > 0 else None,
         "bcpmbc_frac_with_pid": round(float(clip["PID"].notna().mean()), 4)
             if "PID" in clip else None}
    if "PARCEL_CLASS" in clip:
        vc = clip["PARCEL_CLASS"].value_counts()
        if len(vc):
            f["bcpmbc_dominant_parcel_class"] = str(vc.index[0])
            f["bcpmbc_n_parcel_classes"] = int(len(vc))
    if "MUNICIPALITY" in clip:
        vc = clip["MUNICIPALITY"].value_counts()
        if len(vc):
            f["bcpmbc_municipality"] = str(vc.index[0])
    if "REGIONAL_DISTRICT" in clip:
        vc = clip["REGIONAL_DISTRICT"].value_counts()
        if len(vc):
            f["bcpmbc_regional_district"] = str(vc.index[0])
    f = {k: v for k, v in f.items() if v is not None}
    return f, {"obs_date": "live (ParcelMap BC)", "res_m": "vector",
               "item": PMBC, "release": "live", "url": BCWFS}


def _a_bc_crown_land(aoi):
    """Crown-land share of the AOI from the parcel fabric, plus TANTALIS tenures.

    OWNER_TYPE on the parcel fabric distinguishes Crown Provincial / Crown
    Federal / private, which gives an areal Crown fraction; TANTALIS adds the
    active disposition count over the same footprint.
    """
    _bc_guard(aoi)
    g = wfs_features(PMBC, aoi["bbox"], count=20000)
    if len(g) == 0 or "OWNER_TYPE" not in g:
        raise LookupError("ParcelMap BC returned no parcel / no OWNER_TYPE in AOI bbox")
    clip, aoi_km2 = _overlap(aoi, g)
    if len(clip) == 0:
        raise LookupError("parcels found in bbox but none intersect the AOI polygon")
    clip = clip.assign(_a=clip.geometry.area / 1e6)
    byown = clip.groupby("OWNER_TYPE")["_a"].sum()
    tot_parcelled = float(clip["_a"].sum())
    crown = float(sum(v for k, v in byown.items() if str(k).lower().startswith("crown")))
    f = {"bccrown_crown_frac_of_aoi": round(min(crown / aoi_km2, 1.0), 4) if aoi_km2 > 0 else None,
         "bccrown_crown_frac_of_parcelled": round(crown / tot_parcelled, 4)
             if tot_parcelled > 0 else None,
         "bccrown_crown_km2": round(crown, 5),
         "bccrown_parcelled_frac_of_aoi": round(min(tot_parcelled / aoi_km2, 1.0), 4)
             if aoi_km2 > 0 else None,
         "bccrown_dominant_owner_type": str(byown.idxmax()) if len(byown) else None,
         "bccrown_n_owner_types": int(len(byown)),
         "bccrown_owner_type_frac": [{"class": str(k), "value": round(min(float(v) / aoi_km2, 1.0), 4)}
                                     for k, v in byown.items()]}
    # The parcel fabric only covers SURVEYED land.  Unsurveyed BC land is Crown
    # by default, so the Crown share of the AOI is bracketed: the surveyed
    # Crown fraction is a lower bound, plus the unparcelled remainder an upper.
    if aoi_km2 > 0:
        unp = max(0.0, 1.0 - min(tot_parcelled / aoi_km2, 1.0))
        f["bccrown_unparcelled_frac_of_aoi"] = round(unp, 4)
        f["bccrown_crown_frac_upper_bound"] = round(min(crown / aoi_km2 + unp, 1.0), 4)
    t = wfs_features(TANTALIS, aoi["bbox"], count=5000)
    tclip = _overlap(aoi, t)[0] if len(t) else t
    f["bccrown_tenure_count"] = int(len(tclip))
    if len(tclip):
        f["bccrown_tenure_area_km2"] = round(float(tclip.geometry.area.sum()) / 1e6, 5)
        for col, name in (("TENURE_PURPOSE", "bccrown_dominant_tenure_purpose"),
                          ("TENURE_TYPE", "bccrown_dominant_tenure_type")):
            vc = tclip[col].value_counts() if col in tclip else []
            if len(vc):
                f[name] = str(vc.index[0])
    f = {k: v for k, v in f.items() if v is not None}
    return f, {"obs_date": "live (ParcelMap BC + TANTALIS)", "res_m": "vector",
               "item": f"{PMBC} + {TANTALIS}", "release": "live", "url": BCWFS}


# =================================================================== registry
# slot, source_id, family, licence, auth, fn
OGL = "OGL-Canada-2.0"
ADAPTERS = [
 ("census_demography",       "statcan_census_2021", "census", OGL,                   "none", _a_statcan_census),
 ("census_demography",       "statcan_wds_popest", "census", OGL,                    "none", _a_statcan_wds_popest),
 ("census_demography",       "tigerweb_blocks2020","census", "public-domain (US Census)", "none", _a_tigerweb_blocks),
 ("census_demography",       "acs5_api",           "census", "public-domain (US Census)", "api-key-required", _a_acs_income),
 ("population_gridded",      "ghs_pop_r2023a",     "census", "CC-BY-4.0 (JRC GHSL)",  "none", _a_ghs_pop),
 ("urban_extent",            "ghs_smod_r2023a",    "census", "CC-BY-4.0 (JRC GHSL)",  "none", _a_ghs_smod),
 ("urban_extent",            "io_lulc_annual_v02", "census", "CC-BY-4.0 (Impact Observatory)", "none", _a_iolulc),
 ("mobility_flows",          "lehd_lodes8",        "census", "public-domain (US Census)", "none", _a_lodes_flows),
 ("property_assessment",     "statcan_dwelling_value", "census", OGL,                "none", _a_statcan_dwelling_value),
 ("healthcare_accessibility","statcan_odhf",       "census", OGL,                     "none", _a_odhf_access),
 ("healthcare_accessibility","cdc_places",         "census", "public-domain (US CDC)", "none", _a_places_access),
 ("indigenous_lands",        "nrcan_alta",         "census", OGL,                     "none", _a_alta_indigenous),
 ("indigenous_lands",        "tigerweb_aiannh",    "census", "public-domain (US Census)", "none", _a_tiger_aiannh),
 ("parcels_cadastre",        "bc_pmbc_parcel_fabric", "census", "Open Government Licence - BC", "none", _a_bc_parcels),
 ("crown_land",              "bc_tantalis_crown",  "census", "Open Government Licence - BC", "none", _a_bc_crown_land),
]


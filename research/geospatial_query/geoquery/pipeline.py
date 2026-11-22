"""AOI list -> feature set per AOI. Pilot POC.

Design follows the sister-session decisions:
  * layer x source matrix; slots are semantic, sources are providers
  * three declared states per (aoi, slot): found / unavailable / not-attempted
  * every fill labelled observed | inferred | analogue  (this POC emits observed only)
  * no source is silently dropped
"""
import os, time, json, math, warnings
import numpy as np, requests

import rasterio
from rasterio.windows import from_bounds, Window, WindowError
from rasterio.warp import transform_bounds
from rasterio.features import geometry_mask
from rasterio.transform import from_bounds as aff_from_bounds
from pystac_client import Client
from shapely.geometry import shape

warnings.filterwarnings("ignore")

PC = "https://planetarycomputer.microsoft.com/api/stac/v1"
PC_SAS = "https://planetarycomputer.microsoft.com/api/sas/v1/token"
NRCAN = "https://datacube.services.geo.ca/stac/api"
MAXPIX = 320          # decimated-read cap per side; keeps stress AOIs cheap
GDAL_ENV = dict(GDAL_DISABLE_READDIR_ON_OPEN="EMPTY_DIR", GDAL_HTTP_MULTIPLEX="YES",
                VSI_CACHE="TRUE", GDAL_HTTP_MAX_RETRY="2", GDAL_HTTP_RETRY_DELAY="1",
                CPL_VSIL_CURL_NON_CACHED="")

S = requests.Session()
S.headers.update({"Accept": "*/*"})
_sas, _cat = {}, {}

def pc_sign(href):
    base = href.split("?")[0]
    parts = base.split("/")
    acct, cont = parts[2].split(".")[0], parts[3]
    key = (acct, cont)
    tok, exp = _sas.get(key, (None, 0))
    if tok is None or exp < time.time():
        last = None
        for k in range(4):                       # the SAS endpoint 429s under an 8-thread pool
            r = S.get(f"{PC_SAS}/{acct}/{cont}", timeout=20)
            if r.status_code == 200:
                tok = r.json()["token"]; _sas[key] = (tok, time.time() + 1500)
                break
            last = r
            if r.status_code not in (429, 500, 502, 503, 504):
                r.raise_for_status()
            time.sleep(1.5 * (2 ** k))
        else:
            last.raise_for_status()
    return base + "?" + tok

def catalog(url):
    if url not in _cat:
        _cat[url] = Client.open(url)
    return _cat[url]

# ---------------------------------------------------------------- raster core
def read_window(href, bbox, geom=None, maxpix=MAXPIX, band=1, sign=True):
    """Decimated, AOI-clipped read of one band. Returns masked 2-D array.

    Only pixels equal to a DECLARED nodata value are masked. Earlier versions used
    boundless+masked reads, which silently masked every legitimate zero on sources that
    declare no nodata (Hansen tree cover read 50.6% instead of 22.2%, and all-dry JRC
    surface-water windows raised 'all pixels nodata'). The window is now clipped to the
    dataset extent instead of padded, so an AOI outside the footprint raises LookupError.
    """
    url = pc_sign(href) if sign else href
    with rasterio.Env(**GDAL_ENV):
        with rasterio.open(url) as src:
            b = transform_bounds("EPSG:4326", src.crs, *bbox, densify_pts=21)
            win = from_bounds(*b, transform=src.transform)
            full = Window(0, 0, src.width, src.height)
            try:
                win = win.intersection(full)
            except WindowError:
                raise LookupError("AOI does not intersect raster footprint")
            h, w = max(1, int(round(win.height))), max(1, int(round(win.width)))
            if h < 1 or w < 1:
                raise LookupError("AOI does not intersect raster footprint")
            sc = min(1.0, maxpix / max(h, w))
            oh, ow = max(1, int(h * sc)), max(1, int(w * sc))
            arr = src.read(band, window=win, out_shape=(oh, ow), masked=False)
            nod = src.nodata
            win_bounds = rasterio.windows.bounds(win, src.transform)
            src_crs = src.crs
    arr = np.ma.masked_array(arr, mask=np.zeros(arr.shape, bool))
    if nod is not None:
        arr = np.ma.masked_equal(arr, nod)
    if np.issubdtype(arr.dtype, np.floating):
        arr = np.ma.masked_invalid(arr)
    if geom is not None and arr.size > 4:
        gb = transform_bounds(src_crs, "EPSG:4326", *win_bounds, densify_pts=21)
        tr = aff_from_bounds(*gb, arr.shape[1], arr.shape[0])
        m = geometry_mask([geom], out_shape=arr.shape, transform=tr, invert=True)
        if m.any():
            arr = np.ma.masked_array(arr, mask=arr.mask | ~m)
    return arr

def read_best(items, asset, aoi, **kw):
    """Read one asset from the newest date's tiles; keep the tile with the most AOI pixels.

    A tiled collection holds several tiles per date. STAC footprints are loose (some wrap the
    antimeridian and claim every longitude), so a footprint can touch the AOI while the
    raster holds no pixels there. Reading every same-date tile is the only reliable pick.
    Pixels equal to 0 do not count, since several sources leave their 0 nodata undeclared.
    """
    newest, best = _dt(items[0]), None
    for i in (x for x in items if _dt(x) == newest):
        try:
            arr = read_window(i.assets[asset].href, aoi["bbox"], aoi["geom"], **kw)
        except LookupError:
            continue
        n = int((arr.filled(0) != 0).sum())
        if best is None or n > best[2]:
            best = (i, arr, n)
    if best is None:
        raise LookupError("AOI does not intersect raster footprint")
    return best[0], best[1]

def stats(arr, pref, scale=1.0, offset=0.0, nd=3):
    v = arr.compressed().astype("float64")
    if v.size == 0:
        return {}
    v = v * scale + offset
    return {f"{pref}_mean": round(float(v.mean()), nd), f"{pref}_std": round(float(v.std()), nd),
            f"{pref}_p10": round(float(np.percentile(v, 10)), nd),
            f"{pref}_p90": round(float(np.percentile(v, 90)), nd), f"{pref}_n_px": int(v.size)}

def class_fractions(arr, codes, pref):
    v = arr.compressed()
    if v.size == 0:
        return {}
    out, tot = {}, v.size
    vals, cnt = np.unique(v, return_counts=True)
    frac = dict(zip(vals.tolist(), (cnt / tot).tolist()))
    for code, name in codes.items():
        if frac.get(code, 0) > 0:
            out[f"{pref}_frac_{name}"] = round(frac[code], 4)
    dom = int(vals[np.argmax(cnt)])
    out[f"{pref}_dominant"] = codes.get(dom, f"class_{dom}")
    out[f"{pref}_n_classes"] = int(len(vals))
    return out

def _dt(i):
    return (i.properties.get("datetime") or i.properties.get("start_datetime")
            or i.properties.get("end_datetime") or "")

def pc_item(coll, bbox, dt=None, query=None, sortby=None, limit=10, retries=3):
    # max_items caps the search; `limit` alone is only the page size, and .items() would
    # walk every page of a long archive (MODIS 13Q1 since 2000) until the API times out
    kw = dict(collections=[coll], bbox=bbox, limit=limit, max_items=limit)
    if dt: kw["datetime"] = dt
    if query: kw["query"] = query
    if sortby: kw["sortby"] = sortby
    last = None
    for k in range(retries):
        try:
            items = list(catalog(PC).search(**kw).items())
            if not sortby:
                items.sort(key=_dt, reverse=True)
            return items
        except Exception as e:                      # 429 / 5xx from the STAC API
            last = e
            if "429" not in str(e) and "50" not in str(e)[:40]:
                raise
            time.sleep(2 ** k * 3)
    raise last

WC = {10:"tree",20:"shrub",30:"grass",40:"crop",50:"builtup",60:"bare",70:"snowice",
      80:"water",90:"wetland",95:"mangrove",100:"moss"}
NRC = {1:"temperate_needleleaf",2:"subpolar_needleleaf",5:"temperate_broadleaf",6:"mixed_forest",
       8:"shrubland",10:"grassland",11:"subpolar_shrub",12:"subpolar_grass",13:"subpolar_barren",
       14:"wetland",15:"cropland",16:"barren",17:"urban",18:"water",19:"snow_ice"}
CDL = {1:"corn",5:"soybeans",24:"winter_wheat",36:"alfalfa",37:"other_hay",61:"fallow",
       111:"open_water",121:"developed_open",122:"developed_low",123:"developed_med",
       124:"developed_high",131:"barren",141:"deciduous_forest",142:"evergreen_forest",
       143:"mixed_forest",152:"shrubland",176:"grass_pasture",190:"woody_wetlands",195:"herb_wetlands"}

# ---------------------------------------------------------------- adapters
# each adapter: fn(aoi) -> (features: dict, meta: dict)  |  raises on failure
def _a_s2(aoi):
    it = pc_item("sentinel-2-l2a", aoi["bbox"], "2024-05-01/2024-09-30",
                 {"eo:cloud_cover": {"lt": 20}},
                 [{"field": "properties.eo:cloud_cover", "direction": "asc"}], 10)
    if not it: raise LookupError("no scene in the catalogue covers this AOI in the search window")
    i = it[0]
    red = read_window(i.assets["B04"].href, aoi["bbox"], aoi["geom"])
    nir = read_window(i.assets["B08"].href, aoi["bbox"], aoi["geom"])
    grn = read_window(i.assets["B03"].href, aoi["bbox"], aoi["geom"])
    n = min(red.shape[0], nir.shape[0], grn.shape[0]); m = min(red.shape[1], nir.shape[1], grn.shape[1])
    red, nir, grn = red[:n,:m].astype("f8"), nir[:n,:m].astype("f8"), grn[:n,:m].astype("f8")
    ndvi = (nir - red) / (nir + red + 1e-9)
    ndwi = (grn - nir) / (grn + nir + 1e-9)
    f = stats(ndvi, "s2_ndvi"); f.update(stats(ndwi, "s2_ndwi"))
    f.update({"s2_red_reflect_mean": round(float(red.mean()/10000), 4),
              "s2_cloud_cover_pct": i.properties.get("eo:cloud_cover")})
    return f, {"obs_date": (_dt(i) or "unknown")[:10], "res_m": 10, "item": i.id,
               "n_scenes_available": len(it)}

def _a_landsat_st(aoi):
    it = pc_item("landsat-c2-l2", aoi["bbox"], "2024-05-01/2024-09-30",
                 {"eo:cloud_cover": {"lt": 25}, "platform": {"in": ["landsat-8", "landsat-9"]}},
                 [{"field": "properties.eo:cloud_cover", "direction": "asc"}], 10)
    if not it: raise LookupError("no scene in the catalogue covers this AOI in the search window")
    i = it[0]
    a = i.assets["lwir11"]
    rb = (a.extra_fields.get("raster:bands") or [{}])[0]
    sc, off = rb.get("scale", 0.00341802), rb.get("offset", 149.0)
    arr = read_window(a.href, aoi["bbox"], aoi["geom"])
    f = stats(arr, "lst_c", scale=sc, offset=off - 273.15, nd=2)
    return f, {"obs_date": (_dt(i) or "unknown")[:10], "res_m": 30, "item": i.id,
               "n_scenes_available": len(it)}

def _a_dem(aoi):
    it = pc_item("cop-dem-glo-30", aoi["bbox"], limit=4)
    if not it: raise LookupError("no scene in the catalogue covers this AOI in the search window")
    i, arr = read_best(it, "data", aoi)
    f = stats(arr, "elev_m", nd=1)
    v = arr.astype("f8").filled(np.nan)
    if v.shape[0] > 2 and v.shape[1] > 2:
        px = 111320 * (aoi["bbox"][3] - aoi["bbox"][1]) / max(v.shape[0], 1)
        gy, gx = np.gradient(v, max(px, 1e-6))
        slope = np.degrees(np.arctan(np.sqrt(gx**2 + gy**2)))
        f["slope_deg_mean"] = round(float(np.nanmean(slope)), 2)
        f["slope_deg_p90"] = round(float(np.nanpercentile(slope, 90)), 2)
        f["relief_m"] = round(float(np.nanmax(v) - np.nanmin(v)), 1)
    return f, {"obs_date": "2021", "res_m": 30, "item": i.id}

def _a_worldcover(aoi):
    it = pc_item("esa-worldcover", aoi["bbox"], limit=4)
    if not it: raise LookupError("no scene in the catalogue covers this AOI in the search window")
    i, arr = read_best(it, "map", aoi)
    return class_fractions(arr, WC, "wc"), {"obs_date": "2021", "res_m": 10, "item": i.id}

def _a_nrcan_lc(aoi):
    if aoi["country"] != "CA": raise PermissionError("out of coverage (Canada-only)")
    it = pc_item("nrcan-landcover", aoi["bbox"], limit=4)
    if not it: raise LookupError("no scene in the catalogue covers this AOI in the search window")
    k = "landcover" if "landcover" in it[0].assets else list(it[0].assets)[0]
    i, arr = read_best(it, k, aoi)
    return class_fractions(arr, NRC, "nrcanlc"), {"obs_date": (_dt(i) or "unknown")[:10],
                                                  "res_m": 30, "item": i.id}

def _a_cdl(aoi):
    if aoi["country"] != "US": raise PermissionError("out of coverage (US-only)")
    it = pc_item("usda-cdl", aoi["bbox"], limit=12)
    it = [x for x in it if x.id.startswith("cropland")]
    if not it: raise LookupError("no scene in the catalogue covers this AOI in the search window")
    i = it[0]
    arr = read_window(i.assets["cropland"].href, aoi["bbox"], aoi["geom"])
    return class_fractions(arr, CDL, "cdl"), {"obs_date": (_dt(i) or "unknown")[:10],
                                              "res_m": 30, "item": i.id}

def _a_aafc(aoi):
    if aoi["country"] != "CA": raise PermissionError("out of coverage (Canada-only)")
    tried = []
    for base in ("https://www.agr.gc.ca/atlas/rest/services/imageservices/annual_crop_inventory_2023/ImageServer",
                 "https://agriculture.canada.ca/atlas/rest/services/imageservices/annual_crop_inventory_2023/ImageServer"):
        r = S.get(base, params={"f": "json"}, timeout=25)
        tried.append(f"{base.split('/')[2]}:{r.status_code}")
        if r.status_code != 200 or "ImageServer" not in r.text[:400]:
            continue
        x0, y0, x1, y1 = aoi["bbox"]
        c = aoi["centroid"]
        q = S.get(base + "/identify", params={
            "f": "json", "geometry": json.dumps({"x": c[0], "y": c[1],
            "spatialReference": {"wkid": 4326}}), "geometryType": "esriGeometryPoint",
            "returnGeometry": "false"}, timeout=30)
        if q.status_code != 200: raise ConnectionError(f"HTTP {q.status_code} on identify")
        v = q.json().get("value")
        if v in (None, "NoData"): raise LookupError("identify returned NoData")
        return ({"aafc_crop_class_code": v}, {"obs_date": "2023", "res_m": 30})
    raise ConnectionError("AAFC no longer serves the crop inventory at a public address "
                          f"(HTTP 404 on {'; '.join(tried)})")

def _a_biomass(aoi):
    it = pc_item("hgb", aoi["bbox"], limit=4)
    if not it: raise LookupError("no scene in the catalogue covers this AOI in the search window")
    i, arr = read_best(it, "aboveground", aoi)
    return stats(arr, "agb_mgha", nd=1), {"obs_date": "2010", "res_m": 300, "item": i.id}

def _a_gsw(aoi):
    it = pc_item("jrc-gsw", aoi["bbox"], limit=4)
    if not it: raise LookupError("no scene in the catalogue covers this AOI in the search window")
    i, arr = read_best(it, "occurrence", aoi)
    f = stats(arr, "water_occurrence_pct", nd=2)
    if f: f["water_frac_ge50pct"] = round(float((arr.compressed() >= 50).mean()), 4)
    return f, {"obs_date": "1984-2021", "res_m": 30, "item": i.id}

def _a_palsar(aoi):
    it = pc_item("alos-palsar-mosaic", aoi["bbox"], "2020-01-01/2025-12-31", limit=6)
    if not it: raise LookupError("no scene in the catalogue covers this AOI in the search window")
    i, hh = read_best(it, "HH", aoi)
    out = {}
    for pol in ("HH", "HV"):
        if pol in i.assets:
            a = (hh if pol == "HH" else read_window(i.assets[pol].href, aoi["bbox"], aoi["geom"])).astype("f8")
            v = a.compressed()
            v = v[v > 0]
            if v.size:
                db = 10 * np.log10(v**2) - 83.0
                out[f"palsar_{pol.lower()}_db_mean"] = round(float(db.mean()), 2)
                out[f"palsar_{pol.lower()}_db_std"] = round(float(db.std()), 2)
    if not out: raise LookupError("no valid backscatter pixels")
    return out, {"obs_date": (_dt(i) or "unknown")[:10], "res_m": 25, "item": i.id}

def _a_s1rtc(aoi):
    it = pc_item("sentinel-1-rtc", aoi["bbox"], "2024-06-01/2024-09-30", limit=4)
    if not it: raise LookupError("no scene in the catalogue covers this AOI in the search window")
    i = it[0]
    a = read_window(i.assets["vv"].href, aoi["bbox"], aoi["geom"]).astype("f8")
    v = a.compressed(); v = v[v > 0]
    if not v.size: raise LookupError("no valid pixels")
    return ({"s1_vv_db_mean": round(float((10*np.log10(v)).mean()), 2)},
            {"obs_date": (_dt(i) or "unknown")[:10], "res_m": 10, "item": i.id})

def _a_modis_ndvi(aoi):
    it = pc_item("modis-13Q1-061", aoi["bbox"], limit=40)
    if not it: raise LookupError("no scene in the catalogue covers this AOI in the search window")
    vals = []
    for i in sorted(it, key=_dt)[::6][:4]:
        k = [a for a in i.assets if "NDVI" in a]
        if not k: continue
        arr = read_window(i.assets[k[0]].href, aoi["bbox"], aoi["geom"], maxpix=64)
        v = arr.compressed().astype("f8") * 1e-4
        if v.size: vals.append(float(v.mean()))
    if not vals: raise LookupError("no valid pixels")
    return ({"modis_ndvi_mean": round(float(np.mean(vals)), 4),
             "modis_ndvi_amplitude": round(float(max(vals) - min(vals)), 4),
             "modis_n_composites": len(vals)},
            {"obs_date": "2024", "res_m": 250, "item": it[0].id})

def _a_nrcan_dem(aoi):
    if aoi["country"] != "CA": raise PermissionError("out of coverage (Canada-only)")
    cat = catalog(NRCAN)
    it = list(cat.search(collections=["mrdem-30"], bbox=aoi["bbox"], limit=4).items())
    if not it: raise LookupError("no scene in the catalogue covers this AOI in the search window")
    a = it[0].assets.get("dtm") or it[0].assets.get("dsm") or list(it[0].assets.values())[0]
    arr = read_window(a.href, aoi["bbox"], aoi["geom"], sign=False)
    return stats(arr, "mrdem_elev_m", nd=1), {"obs_date": "2024", "res_m": 30, "item": it[0].id}

def _a_macrostrat(aoi):
    c = aoi["centroid"]
    r = S.get("https://macrostrat.org/api/v2/geologic_units/map",
              params={"lat": c[1], "lng": c[0]}, timeout=25)
    r.raise_for_status()
    d = r.json().get("success", {}).get("data", [])
    if not d: raise LookupError("no geologic unit at centroid")
    u = d[0]
    return ({"geo_lithology": (u.get("lith") or "")[:80], "geo_unit_name": (u.get("name") or "")[:60],
             "geo_age_top_ma": u.get("t_age"), "geo_age_bottom_ma": u.get("b_age"),
             "geo_n_units": len(d)}, {"obs_date": "n/a", "res_m": "vector"})

def _a_eq(aoi):
    c = aoi["centroid"]
    r = S.get("https://earthquake.usgs.gov/fdsnws/event/1/query",
              params={"format": "geojson", "latitude": c[1], "longitude": c[0],
                      "maxradiuskm": 100, "starttime": "1980-01-01", "minmagnitude": 2.5},
              timeout=30)
    r.raise_for_status()
    fs = r.json()["features"]
    mags = [f["properties"]["mag"] for f in fs if f["properties"].get("mag") is not None]
    return ({"eq_count_100km_m2.5": len(fs),
             "eq_max_mag_100km": round(max(mags), 2) if mags else None},
            {"obs_date": "1980-present", "res_m": "point"})

def _a_gbif(aoi):
    x0, y0, x1, y1 = aoi["bbox"]
    wkt = f"POLYGON(({x0} {y0},{x1} {y0},{x1} {y1},{x0} {y1},{x0} {y0}))"
    r = S.get("https://api.gbif.org/v1/occurrence/search",
              params={"geometry": wkt, "limit": 0, "facet": "speciesKey", "facetLimit": 1200},
              timeout=40)
    r.raise_for_status(); j = r.json()
    nsp = len((j.get("facets") or [{}])[0].get("counts", [])) if j.get("facets") else None
    return ({"gbif_occurrences": j["count"], "gbif_distinct_species": nsp},
            {"obs_date": "all-time", "res_m": "point"})

def _a_mgnify(aoi):
    x0, y0, x1, y1 = aoi["bbox"]
    pad = 0.25
    r = S.get("https://www.ebi.ac.uk/metagenomics/api/v1/samples",
              params={"latitude_gte": y0-pad, "latitude_lte": y1+pad,
                      "longitude_gte": x0-pad, "longitude_lte": x1+pad, "page_size": 1},
              timeout=40, headers={"Accept": "application/json"})
    r.raise_for_status()
    n = r.json().get("meta", {}).get("pagination", {}).get("count")
    ctl = S.get("https://www.ebi.ac.uk/metagenomics/api/v1/samples",
                params={"latitude_gte": -20, "latitude_lte": -19, "longitude_gte": -140,
                        "longitude_lte": -139, "page_size": 1}, timeout=40,
                headers={"Accept": "application/json"}).json().get("meta", {}).get("pagination", {}).get("count")
    if n is not None and n == ctl and n > 0:
        raise ValueError(f"filter_unverified: AOI count {n} == mid-Pacific control {ctl}")
    if n == 0:
        return ({"mgnify_samples_25km": 0, "mgnify_control_count": ctl},
                {"obs_date": "all-time", "res_m": "point", "note": "verified absence"})
    return ({"mgnify_samples_25km": n, "mgnify_control_count": ctl},
            {"obs_date": "all-time", "res_m": "point"})

def _a_ena(aoi):
    x0, y0, x1, y1 = aoi["bbox"]
    r = S.get("https://www.ebi.ac.uk/ena/portal/api/search",
              params={"result": "sample", "query": f"geo_box1({y0-0.25},{x0-0.25},{y1+0.25},{x1+0.25})",
                      "limit": 0, "format": "json", "fields": "accession"}, timeout=60)
    if r.status_code != 200: raise ConnectionError(f"HTTP {r.status_code}")
    try:
        n = len(r.json())
    except Exception:
        n = max(0, len(r.text.strip().splitlines()) - 1)
    return ({"ena_geolocated_samples_25km": n}, {"obs_date": "all-time", "res_m": "point"})

def _a_overpass(aoi):
    x0, y0, x1, y1 = aoi["bbox"]
    q = (f'[out:json][timeout:90];way["highway"]({y0},{x0},{y1},{x1});out count;'
         f'way["building"]({y0},{x0},{y1},{x1});out count;')
    last = None
    for ep in ("https://overpass-api.de/api/interpreter",
               "https://overpass.kumi.systems/api/interpreter"):
        for _ in range(2):
            try:
                r = S.get(ep, params={"data": q}, timeout=120,
                          headers={"User-Agent": "aoi-dossier-poc/0.1"})
            except Exception as e:
                last = f"{type(e).__name__}: {str(e)[:60]}"; continue
            if r.status_code == 200:
                els = r.json().get("elements", [])
                cnt = [int(e.get("tags", {}).get("ways", e.get("tags", {}).get("total", 0)))
                       for e in els]
                while len(cnt) < 2: cnt.append(0)
                return ({"osm_highway_ways": cnt[0], "osm_building_ways": cnt[1],
                         "osm_building_per_km2": round(cnt[1] / max(aoi["area_km2"], 1e-6), 1)},
                        {"obs_date": "live", "res_m": "vector"})
            last = f"HTTP {r.status_code}"
            if r.status_code in (400, 404): break
            time.sleep(3)
    raise ConnectionError(f"overpass server busy/blocked after retries ({last})")

def _a_wqp(aoi):
    if aoi["country"] != "US": raise PermissionError("out of coverage (US-only: NWIS/STORET)")
    x0, y0, x1, y1 = aoi["bbox"]
    r = S.get("https://www.waterqualitydata.us/data/Station/search",
              params={"bBox": f"{x0-0.2},{y0-0.2},{x1+0.2},{y1+0.2}", "mimeType": "csv",
                      "zip": "no", "providers": "NWIS", "providers2": "STORET"}, timeout=90)
    if r.status_code not in (200, 206): raise ConnectionError(f"HTTP {r.status_code}")
    n = max(0, len(r.text.strip().splitlines()) - 1)
    return ({"wqp_stations_20km": n}, {"obs_date": "all-time", "res_m": "point"})

def _a_climate(aoi):
    c = aoi["centroid"]
    r = S.get("https://archive-api.open-meteo.com/v1/archive",
              params={"latitude": round(c[1], 3), "longitude": round(c[0], 3),
                      "start_date": "2015-01-01", "end_date": "2024-12-31",
                      "daily": "temperature_2m_mean,precipitation_sum",
                      "timezone": "UTC", "models": "era5"}, timeout=60)
    if r.status_code != 200: raise ConnectionError(f"HTTP {r.status_code}")
    d = r.json()["daily"]
    t = np.array([x for x in d["temperature_2m_mean"] if x is not None], dtype="f8")
    p = np.array([x for x in d["precipitation_sum"] if x is not None], dtype="f8")
    if not t.size: raise LookupError("empty series")
    return ({"clim_mean_annual_temp_c": round(float(t.mean()), 2),
             "clim_coldest_day_c": round(float(t.min()), 1),
             "clim_hottest_day_c": round(float(t.max()), 1),
             "clim_annual_precip_mm": round(float(p.sum() / 10), 1),
             "clim_n_days": int(t.size)}, {"obs_date": "2015-2024", "res_m": 11000})

# slot, source_id, family, licence, auth, fn
ADAPTERS = [
 ("optical_multispectral", "sentinel2_l2a",   "spectral",       "CC-BY-4.0 (Copernicus)", "none", _a_s2),
 ("thermal_surface_temp",  "landsat_c2l2_st", "spectral",       "public-domain (USGS)",   "none", _a_landsat_st),
 ("sar_backscatter_l",     "alos_palsar_mosaic","spectral",     "JAXA open",              "none", _a_palsar),
 ("sar_backscatter_c",     "sentinel1_rtc",   "spectral",       "CC-BY-4.0 (Copernicus)", "none", _a_s1rtc),
 ("dem_medium_res",        "cop_dem_glo30",   "geology",        "ESA Copernicus DEM EULA","none", _a_dem),
 ("dem_medium_res_ca",     "mrdem_30",        "geology",        "OGL-Canada-2.0",         "none", _a_nrcan_dem),
 ("bedrock_geology",       "macrostrat",      "geology",        "CC-BY-4.0",              "none", _a_macrostrat),
 ("land_cover",            "esa_worldcover",  "macrobiology",   "CC-BY-4.0",              "none", _a_worldcover),
 ("land_cover_ca",         "nrcan_landcover", "macrobiology",   "OGL-Canada-2.0",         "none", _a_nrcan_lc),
 ("crop_type_us",          "usda_cdl",        "macrobiology",   "public-domain (USDA)",   "none", _a_cdl),
 ("crop_type_ca",          "aafc_aci",        "macrobiology",   "OGL-Canada-2.0",         "none", _a_aafc),
 ("biomass",               "hgb",             "macrobiology",   "CC-BY-4.0",              "none", _a_biomass),
 ("vegetation_timeseries", "modis_13q1",      "macrobiology",   "public-domain (NASA)",   "none", _a_modis_ndvi),
 ("species_occurrence",    "gbif",            "macrobiology",   "CC-BY / CC0 mixed",      "none", _a_gbif),
 ("surface_water_extent",  "jrc_gsw",         "water",          "CC-BY-4.0",              "none", _a_gsw),
 ("water_quality",         "wqp",             "water",          "public-domain (US)",     "none", _a_wqp),
 ("reanalysis_climate",    "era5_open_meteo", "climate",        "CC-BY-4.0 (ERA5)",       "none", _a_climate),
 ("earthquake_catalogue",  "usgs_fdsn",       "hazards",        "public-domain (USGS)",   "none", _a_eq),
 ("infrastructure_osm",    "osm_overpass",    "infrastructure", "ODbL-1.0 (share-alike)", "none", _a_overpass),
 ("geolocated_sequence",   "mgnify",          "microbiology",   "EBI terms / open",       "none", _a_mgnify),
 ("geolocated_sequence_insdc","ena_geo",      "microbiology",   "INSDC open",             "none", _a_ena),
]

def run_one(aoi, spec):
    slot, src, fam, lic, auth, fn = spec
    rec = {"aoi_id": aoi["aoi_id"], "slot": slot, "source": src, "family": fam,
           "licence": lic, "auth": auth, "fill_type": "observed"}
    t0 = time.time()
    try:
        feats, meta = fn(aoi)
        if not feats:
            raise LookupError("adapter returned no features (all pixels nodata)")
        rec.update(status="found", reason="", n_features=len(feats), features=feats, **meta)
    except PermissionError as e:
        rec.update(status="not-attempted", reason=str(e), n_features=0, features={})
    except Exception as e:
        kind = type(e).__name__
        msg = str(e)[:160].replace("\n", " ")
        blocked = "Proxy" in kind or "403" in msg or "ProxyError" in msg
        rec.update(status="unavailable", reason=f"{'allowlist-blocked' if blocked else kind}: {msg}",
                   n_features=0, features={})
    rec["wall_s"] = round(time.time() - t0, 2)
    return rec

def load_aois(path):
    """Normalised AOI GeoJSON (see normalise.py) -> the aoi dicts every adapter takes."""
    out = []
    for f in json.load(open(path))["features"]:
        p, g = f["properties"], shape(f["geometry"])
        out.append({**p, "geom": f["geometry"], "bbox": list(g.bounds),
                    "centroid": [g.centroid.x, g.centroid.y]})
    return out

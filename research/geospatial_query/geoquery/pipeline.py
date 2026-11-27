"""AOI list -> feature set per AOI. Pilot POC.

Design follows the sister-session decisions:
  * layer x source matrix; slots are semantic, sources are providers
  * three declared states per (aoi, slot): found / unavailable / not-attempted
  * every fill labelled observed | inferred | analogue  (this POC emits observed only)
  * no source is silently dropped
"""
import os, re, io, csv, time, json, math, datetime, warnings
import numpy as np, requests

import rasterio
from rasterio.windows import from_bounds, Window, WindowError
from rasterio.warp import transform_bounds
from rasterio.features import geometry_mask
from rasterio.transform import from_bounds as aff_from_bounds
from pystac_client import Client
import shapely
from shapely.geometry import shape
from shapely.geometry.polygon import orient
from shapely.ops import transform as shp_transform
from pyproj import Transformer

warnings.filterwarnings("ignore")

PC = "https://planetarycomputer.microsoft.com/api/stac/v1"
PC_SEARCH = PC + "/search"
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

def _fractions(vals, cnt, names, pref):
    tot = cnt.sum()
    if tot == 0:
        return {}
    frac = [{"class": names[v], "value": round(c / tot, 4)}
            for v, c in sorted(zip(vals.tolist(), cnt.tolist())) if c and v in names]
    dom = int(vals[np.argmax(cnt)])
    return {f"{pref}_frac": frac, f"{pref}_dominant": names.get(dom, str(dom)),
            f"{pref}_n_classes": int((cnt > 0).sum())}

def class_fractions(arr, names, pref):
    v = arr.compressed()
    if v.size == 0:
        return {}
    return _fractions(*np.unique(v, return_counts=True), names, pref)

def legend(item, asset):
    """{pixel value: class name} from a STAC asset's classification:classes or file:values."""
    a = item.assets[asset].extra_fields
    if "classification:classes" in a:
        return {c["value"]: c.get("description") or c["name"] for c in a["classification:classes"]}
    return {v: x["summary"] for x in a["file:values"] for v in x["values"]}

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

def _span(i):
    p = i.properties
    if p.get("start_datetime") and p.get("end_datetime") and p["start_datetime"][:10] != p["end_datetime"][:10]:
        return f"{p['start_datetime'][:10]}/{p['end_datetime'][:10]}"
    return (_dt(i) or "unknown")[:10]

def pc_meta(i, res_m, release=None):
    return {"obs_date": _span(i), "res_m": res_m, "item": i.id,
            "release": release or i.collection_id, "url": PC_SEARCH}

_s2off = {}

def s2_offsets(item):
    """{band: BOA_ADD_OFFSET in DN} from the L2A product metadata; empty before baseline 04.00."""
    if item.properties.get("s2:processing_baseline", "00.00") < "04.00":
        return {}
    if item.id not in _s2off:
        x = S.get(pc_sign(item.assets["product-metadata"].href), timeout=60).text
        band = {i: "B%02d" % int(b[1:]) if b[1:].isdigit() else b
                for i, b in re.findall(r'bandId="(\d+)" physicalBand="(B\w+)"', x)}
        off = {band[i]: int(v) for i, v in re.findall(r'<BOA_ADD_OFFSET band_id="(\d+)">(-?\d+)<', x)}
        if not off:
            raise ValueError(f"baseline >= 04.00 but no BOA_ADD_OFFSET in {item.id} product metadata")
        _s2off[item.id] = off
    return _s2off[item.id]

def buffered(aoi, km):
    """The AOI polygon grown by `km` kilometres, in lon/lat, buffered in an equidistant projection."""
    c = aoi["centroid"]
    aeqd = f"+proj=aeqd +lat_0={c[1]} +lon_0={c[0]} +datum=WGS84 +units=m"
    fwd = Transformer.from_crs("EPSG:4326", aeqd, always_xy=True).transform
    inv = Transformer.from_crs(aeqd, "EPSG:4326", always_xy=True).transform
    return shp_transform(inv, shp_transform(fwd, shape(aoi["geom"])).buffer(km * 1000))

def n_within(geom, lons, lats):
    lons, lats = np.asarray(lons, "f8"), np.asarray(lats, "f8")
    ok = np.isfinite(lons) & np.isfinite(lats)
    return int(shapely.contains_xy(geom, lons[ok], lats[ok]).sum())

def _polys(aoi, tol=0.001, maxpts=200):
    g = shapely.force_2d(shape(aoi["geom"]))
    if len(shapely.get_coordinates(g)) > maxpts:
        g = g.simplify(tol)
    return list(getattr(g, "geoms", [g]))

# ---------------------------------------------------------------- adapters
# each adapter: fn(aoi) -> (features: dict, meta: dict)  |  raises on failure
def _a_s2(aoi):
    it = pc_item("sentinel-2-l2a", aoi["bbox"], "2024-05-01/2024-09-30",
                 {"eo:cloud_cover": {"lt": 20}},
                 [{"field": "properties.eo:cloud_cover", "direction": "asc"}], 10)
    if not it: raise LookupError("no scene in the catalogue covers this AOI in the search window")
    i = it[0]
    off = s2_offsets(i)
    red, nir, grn = (read_window(i.assets[b].href, aoi["bbox"], aoi["geom"]) for b in ("B04", "B08", "B03"))
    n = min(red.shape[0], nir.shape[0], grn.shape[0]); m = min(red.shape[1], nir.shape[1], grn.shape[1])
    red, nir, grn = ((a[:n, :m].astype("f8") + off.get(b, 0)) / 10000
                     for a, b in ((red, "B04"), (nir, "B08"), (grn, "B03")))
    # BOA reflectance can be <= 0 after the offset; a normalised difference there is meaningless
    ndvi = np.ma.masked_where((nir <= 0) | (red <= 0), (nir - red) / (nir + red + 1e-9))
    ndwi = np.ma.masked_where((grn <= 0) | (nir <= 0), (grn - nir) / (grn + nir + 1e-9))
    f = stats(ndvi, "s2_ndvi"); f.update(stats(ndwi, "s2_ndwi"))
    f.update({"s2_red_reflect_mean": round(float(red.mean()), 4),
              "s2_eo_cloud_cover_pct": i.properties.get("eo:cloud_cover")})
    return f, pc_meta(i, 10)

def _a_landsat_st(aoi):
    it = pc_item("landsat-c2-l2", aoi["bbox"], "2024-05-01/2024-09-30",
                 {"eo:cloud_cover": {"lt": 25}, "platform": {"in": ["landsat-8", "landsat-9"]}},
                 [{"field": "properties.eo:cloud_cover", "direction": "asc"}], 10)
    if not it: raise LookupError("no scene in the catalogue covers this AOI in the search window")
    i = it[0]
    a = i.assets["lwir11"]
    rb = a.extra_fields["raster:bands"][0]
    arr = read_window(a.href, aoi["bbox"], aoi["geom"])
    return stats(arr, "lst_k", scale=rb["scale"], offset=rb["offset"], nd=2), pc_meta(i, 30)

def _a_dem(aoi):
    it = pc_item("cop-dem-glo-30", aoi["bbox"], limit=4)
    if not it: raise LookupError("no scene in the catalogue covers this AOI in the search window")
    i, arr = read_best(it, "data", aoi)
    f = stats(arr, "elev_m", nd=1)
    v = arr.astype("f8").filled(np.nan)
    if v.shape[0] > 2 and v.shape[1] > 2:
        # the read is clipped to this one tile, so pixel spacing comes from the clipped extent
        x0, y0 = max(aoi["bbox"][0], i.bbox[0]), max(aoi["bbox"][1], i.bbox[1])
        x1, y1 = min(aoi["bbox"][2], i.bbox[2]), min(aoi["bbox"][3], i.bbox[3])
        dy = 111320 * (y1 - y0) / v.shape[0]
        dx = 111320 * math.cos(math.radians((y0 + y1) / 2)) * (x1 - x0) / v.shape[1]
        gy, gx = np.gradient(v, dy, dx)
        slope = np.degrees(np.arctan(np.sqrt(gx**2 + gy**2)))
        f["slope_deg_mean"] = round(float(np.nanmean(slope)), 2)
        f["slope_deg_p90"] = round(float(np.nanpercentile(slope, 90)), 2)
        f["relief_m"] = round(float(np.nanmax(v) - np.nanmin(v)), 1)
    return f, pc_meta(i, 30)

def _a_worldcover(aoi):
    it = pc_item("esa-worldcover", aoi["bbox"], limit=4)
    if not it: raise LookupError("no scene in the catalogue covers this AOI in the search window")
    i, arr = read_best(it, "map", aoi)
    return (class_fractions(arr, legend(i, "map"), "wc"),
            pc_meta(i, 10, f"esa-worldcover {i.properties['esa_worldcover:product_version']}"))

# https://datacube-prod-data-public.s3.ca-central-1.amazonaws.com/store/land/landcover/ClassIndex_v2.pdf
NRC = {1: "Temperate or sub-polar needleleaf forest", 2: "Sub-polar taiga needleleaf forest",
       5: "Temperate or sub-polar broadleaf deciduous forest", 6: "Mixed forest",
       8: "Temperate or sub-polar Shrubland", 10: "Temperate or sub-polar grassland",
       11: "Sub-polar or polar shrubland-lichen-moss", 12: "Sub-polar or polar grassland-lichen-moss",
       13: "Sub-polar or polar barren-lichen-moss", 14: "Wetland", 15: "Cropland", 16: "Barren land",
       17: "Urban and built-up", 18: "Water", 19: "Snow and ice"}

def _a_nrcan_lc(aoi):
    if aoi["country"] != "CA": raise PermissionError("out of coverage (Canada-only)")
    it = sorted(catalog(NRCAN).search(collections=["landcover"], bbox=aoi["bbox"], limit=10).items(),
                key=_dt, reverse=True)
    if not it: raise LookupError("no scene in the catalogue covers this AOI in the search window")
    i = it[0]
    arr = read_window(i.assets["classification"].href, aoi["bbox"], aoi["geom"], sign=False)
    return class_fractions(arr, NRC, "nrcanlc"), {"obs_date": _dt(i)[:4], "res_m": 30, "item": i.id,
                                                  "release": i.id, "url": NRCAN + "/search"}

def _a_cdl(aoi):
    if aoi["country"] != "US": raise PermissionError("out of coverage (US-only)")
    it = pc_item("usda-cdl", aoi["bbox"], limit=12)
    it = [x for x in it if x.id.startswith("cropland")]
    if not it: raise LookupError("no scene in the catalogue covers this AOI in the search window")
    i, arr = read_best(it, "cropland", aoi)
    return class_fractions(arr, legend(i, "cropland"), "cdl"), pc_meta(i, 30, f"usda-cdl {_dt(i)[:4]}")

ACI = "https://agriculture.canada.ca/imagery-images/rest/services/annual_crop_inventory"
ACI_CLASSES = ("https://agriculture.canada.ca/atlas/data_donnees/annualCropInventory/"
               "supportdocument_documentdesupport/aci_crop_classifications_iac_classifications_des_cultures.csv")
_aci = {}

def _aci_edition():
    if not _aci:
        r = S.get(ACI, params={"f": "json"}, timeout=30); r.raise_for_status()
        year = max(int(s["name"].split("/")[1]) for s in r.json()["services"])
        r = S.get(ACI_CLASSES, timeout=30); r.raise_for_status()
        rows = csv.DictReader(io.StringIO(r.content.decode("cp1252")))
        _aci.update(year=year, legend={int(x["Code"]): x["Label"].strip() for x in rows})
    return _aci["year"], _aci["legend"]

def _a_aafc(aoi):
    if aoi["country"] != "CA": raise PermissionError("out of coverage (Canada-only)")
    year, leg = _aci_edition()
    base = f"{ACI}/{year}/ImageServer"
    rings = [list(orient(p, -1).exterior.coords) for p in _polys(aoi)]
    x0, y0, x1, y1 = transform_bounds("EPSG:4326", "EPSG:3857", *aoi["bbox"])
    px = max(30.0, max(x1 - x0, y1 - y0) / MAXPIX)
    r = S.post(base + "/computeHistograms", data={
        "f": "json", "geometryType": "esriGeometryPolygon",
        "geometry": json.dumps({"rings": rings, "spatialReference": {"wkid": 4326}}),
        "pixelSize": json.dumps({"x": px, "y": px, "spatialReference": {"wkid": 3857}})}, timeout=90)
    r.raise_for_status()
    h = (r.json().get("histograms") or [{}])[0]
    if not h.get("counts"): raise LookupError("no ACI pixels in this AOI")
    cnt = np.array(h["counts"])
    vals = np.arange(cnt.size) + int(round(h["min"] + 0.5))
    keep = vals != 0                                   # 0 is outside the mapped area
    f = _fractions(vals[keep], cnt[keep], leg, "aafc_aci")
    if not f: raise LookupError("no ACI pixels in this AOI")
    return f, {"obs_date": str(year), "res_m": 30, "release": f"annual_crop_inventory/{year}", "url": base}

def _a_biomass(aoi):
    it = pc_item("hgb", aoi["bbox"], limit=4)
    if not it: raise LookupError("no scene in the catalogue covers this AOI in the search window")
    i, arr = read_best(it, "aboveground", aoi)
    # ORNL user guide: UInt16, scaling 0.1, Mg C/ha, no-data 65535; the COG declares none of it
    arr = np.ma.masked_equal(arr, 65535)
    return stats(arr, "agb_carbon_mgc_ha", scale=0.1, nd=1), pc_meta(i, 300)

def _a_gsw(aoi):
    it = pc_item("jrc-gsw", aoi["bbox"], limit=4)
    if not it: raise LookupError("no scene in the catalogue covers this AOI in the search window")
    i, arr = read_best(it, "occurrence", aoi)
    f = stats(arr, "water_occurrence_pct", nd=2)
    if f: f["water_frac_ge50pct"] = round(float((arr.compressed() >= 50).mean()), 4)
    v = re.search(r"v(\d+)_(\d+)_(\d{4})", i.id)
    return f, pc_meta(i, 30, f"jrc-gsw v{v[1]}.{v[2]} ({v[3]})")

def _a_palsar(aoi):
    it = pc_item("alos-palsar-mosaic", aoi["bbox"], "2020-01-01/2025-12-31", limit=6)
    if not it: raise LookupError("no scene in the catalogue covers this AOI in the search window")
    i, hh = read_best(it, "HH", aoi)
    cf = float(i.properties["cf"].split()[0])
    out = {}
    for pol in ("HH", "HV"):
        if pol in i.assets:
            a = (hh if pol == "HH" else read_window(i.assets[pol].href, aoi["bbox"], aoi["geom"])).astype("f8")
            v = a.compressed()
            v = v[v > 0]
            if v.size:
                db = 10 * np.log10(v**2) - cf
                out[f"palsar_{pol.lower()}_db_mean"] = round(float(db.mean()), 2)
                out[f"palsar_{pol.lower()}_db_std"] = round(float(db.std()), 2)
    if not out: raise LookupError("no valid backscatter pixels")
    return out, pc_meta(i, 25)

def _a_s1rtc(aoi):
    it = pc_item("sentinel-1-rtc", aoi["bbox"], "2024-06-01/2024-09-30", limit=4)
    if not it: raise LookupError("no scene in the catalogue covers this AOI in the search window")
    i = it[0]
    a = read_window(i.assets["vv"].href, aoi["bbox"], aoi["geom"]).astype("f8")
    v = a.compressed(); v = v[v > 0]
    if not v.size: raise LookupError("no valid pixels")
    return {"s1_vv_db_mean": round(float((10*np.log10(v)).mean()), 2)}, pc_meta(i, 10)

def _a_modis_ndvi(aoi):
    # one tile (the one under the centroid), Terra only, every 16-day composite of the last year
    c = aoi["centroid"]
    end = datetime.date.today()
    it = pc_item("modis-13Q1-061", [c[0] - 1e-4, c[1] - 1e-4, c[0] + 1e-4, c[1] + 1e-4],
                 f"{end - datetime.timedelta(days=365)}/{end}", {"platform": {"eq": "terra"}}, limit=30)
    it = [i for i in it if i.properties.get("modis:tile-id") == it[0].properties.get("modis:tile-id")] if it else []
    if not it: raise LookupError("no scene in the catalogue covers this AOI in the search window")
    vals, used = [], []
    for i in sorted(it, key=_span):
        a = i.assets["250m_16_days_NDVI"]
        arr = read_window(a.href, aoi["bbox"], aoi["geom"], maxpix=64)
        v = arr.compressed().astype("f8")
        v = v[v >= -2000] * a.extra_fields["raster:bands"][0]["scale"]    # -3000 is fill
        if v.size:
            vals.append(float(v.mean())); used.append(i)
    if not vals: raise LookupError("no valid pixels")
    return ({"modis_ndvi_mean": round(float(np.mean(vals)), 4),
             "modis_ndvi_amplitude": round(float(max(vals) - min(vals)), 4),
             "modis_n_composites": len(vals)},
            {"obs_date": f"{_span(used[0])[:10]}/{_span(used[-1])[-10:]}", "res_m": 250,
             "item": used[-1].id, "release": "modis-13Q1-061", "url": PC_SEARCH})

def _a_nrcan_dem(aoi):
    if aoi["country"] != "CA": raise PermissionError("out of coverage (Canada-only)")
    it = list(catalog(NRCAN).search(collections=["mrdem-30"], bbox=aoi["bbox"], limit=4).items())
    if not it: raise LookupError("no scene in the catalogue covers this AOI in the search window")
    i = it[0]
    arr = read_window(i.assets["dtm"].href, aoi["bbox"], aoi["geom"], sign=False)
    return stats(arr, "mrdem_dtm_elev_m", nd=1), {"obs_date": _dt(i)[:10], "res_m": 30, "item": f"{i.id}/dtm",
                                                  "release": "mrdem-30", "url": NRCAN + "/search"}

MACROSTRAT = "https://macrostrat.org/api/v2/geologic_units/map"

def _a_macrostrat(aoi):
    c = aoi["centroid"]
    r = S.get(MACROSTRAT, params={"lat": c[1], "lng": c[0]}, timeout=25)
    r.raise_for_status()
    d = r.json().get("success", {}).get("data", [])
    if not d: raise LookupError("no geologic unit at centroid")
    u = d[0]                                  # largest map scale first
    return ({"geo_lithology": u.get("lith"), "geo_unit_name": u.get("name"),
             "geo_t_int_age_ma": u.get("t_int_age"), "geo_b_int_age_ma": u.get("b_int_age"),
             "geo_map_scale": u.get("scale"), "geo_n_map_units_at_centroid": len(d)},
            {"obs_date": "n/a", "res_m": "vector", "item": str(u.get("map_id")),
             "release": "macrostrat api v2", "url": MACROSTRAT})

FDSN = "https://earthquake.usgs.gov/fdsnws/event/1/"

def _a_eq(aoi):
    c = aoi["centroid"]
    q = {"format": "geojson", "latitude": c[1], "longitude": c[0], "maxradiuskm": 100,
         "starttime": "1980-01-01", "minmagnitude": 2.5}
    r = S.get(FDSN + "count", params=q, timeout=30); r.raise_for_status()
    n = int(r.json()["count"])
    f = {"eq_count_100km_m2.5": n}
    if n:
        r = S.get(FDSN + "query", params={**q, "orderby": "magnitude", "limit": 1}, timeout=30)
        r.raise_for_status()
        f["eq_max_mag_100km"] = r.json()["features"][0]["properties"]["mag"]
    return f, {"obs_date": "1980-present", "res_m": "point", "release": "USGS ComCat FDSN event 1",
               "url": FDSN + "count"}

GBIF = "https://api.gbif.org/v1/occurrence/search"

def _a_gbif(aoi):
    polys = [orient(p, 1.0) for p in _polys(aoi)]
    wkt = (polys[0] if len(polys) == 1 else shapely.MultiPolygon(polys)).wkt
    nsp, off, count = 0, 0, None
    while True:
        r = S.get(GBIF, params={"geometry": wkt, "limit": 0, "facet": "speciesKey",
                                "facetLimit": 1000, "facetOffset": off}, timeout=60)
        r.raise_for_status(); j = r.json()
        count = j["count"]
        page = (j.get("facets") or [{}])[0].get("counts", [])
        nsp += len(page)
        if len(page) < 1000:
            break
        off += 1000
    return ({"gbif_occurrences": count, "gbif_distinct_species": nsp},
            {"obs_date": "all-time", "res_m": "point", "release": "GBIF occurrence API v1", "url": GBIF})

MGNIFY = "https://www.ebi.ac.uk/metagenomics/api/v1/samples"

def _a_mgnify(aoi):
    g = buffered(aoi, 25)
    x0, y0, x1, y1 = g.bounds
    url, params, lon, lat = MGNIFY, {"latitude_gte": y0, "latitude_lte": y1, "longitude_gte": x0,
                                     "longitude_lte": x1, "page_size": 250}, [], []
    while url:
        r = S.get(url, params=params, timeout=60, headers={"Accept": "application/json"})
        r.raise_for_status(); j = r.json()
        for s in j["data"]:
            lon.append(s["attributes"].get("longitude")); lat.append(s["attributes"].get("latitude"))
        url, params = j["links"].get("next"), None
    return ({"mgnify_samples_25km": n_within(g, [x if x is not None else np.nan for x in lon],
                                              [x if x is not None else np.nan for x in lat])},
            {"obs_date": "all-time", "res_m": "point", "release": "MGnify API v1", "url": MGNIFY})

ENA = "https://www.ebi.ac.uk/ena/portal/api/search"

def _a_ena(aoi):
    g = buffered(aoi, 25)
    x0, y0, x1, y1 = g.bounds
    r = S.get(ENA, params={"result": "sample", "query": f"geo_box1({y0},{x0},{y1},{x1})",
                           "limit": 0, "format": "tsv", "fields": "accession,lat,lon"}, timeout=120)
    if r.status_code != 200: raise ConnectionError(f"HTTP {r.status_code}")
    rows = list(csv.DictReader(io.StringIO(r.text), delimiter="\t"))
    num = lambda s: float(s) if s else np.nan
    return ({"ena_geolocated_samples_25km": n_within(g, [num(x["lon"]) for x in rows],
                                                      [num(x["lat"]) for x in rows])},
            {"obs_date": "all-time", "res_m": "point", "release": "ENA Portal API", "url": ENA})

def _a_overpass(aoi):
    polys = ['poly:"' + " ".join(f"{y} {x}" for x, y in p.exterior.coords) + '"' for p in _polys(aoi)]
    sel = lambda tag: "(" + "".join(f'way["{tag}"]({p});' for p in polys) + ");out count;"
    q = "[out:json][timeout:90];" + sel("highway") + sel("building")
    last = None
    for ep in ("https://overpass-api.de/api/interpreter",
               "https://overpass.kumi.systems/api/interpreter"):
        for _ in range(2):
            try:
                r = S.post(ep, data={"data": q}, timeout=120,
                           headers={"User-Agent": "aoi-dossier-poc/0.1"})
            except Exception as e:
                last = f"{type(e).__name__}: {str(e)[:60]}"; continue
            if r.status_code == 200:
                els = r.json().get("elements", [])
                cnt = [int(e.get("tags", {}).get("ways", e.get("tags", {}).get("total", 0)))
                       for e in els]
                while len(cnt) < 2: cnt.append(0)
                return ({"osm_highway_ways": cnt[0], "osm_building_ways": cnt[1],
                         "osm_building_ways_per_km2": round(cnt[1] / max(aoi["area_km2"], 1e-6), 1)},
                        {"obs_date": "live", "res_m": "vector", "release": "OpenStreetMap (live)", "url": ep})
            last = f"HTTP {r.status_code}"
            if r.status_code in (400, 404): break
            time.sleep(3)
    raise ConnectionError(f"overpass server busy/blocked after retries ({last})")

WQP = "https://www.waterqualitydata.us/data/Station/search"

def _a_wqp(aoi):
    if aoi["country"] != "US": raise PermissionError("out of coverage (US-only: NWIS/STORET)")
    g = buffered(aoi, 20)
    x0, y0, x1, y1 = g.bounds
    r = S.get(WQP, params={"bBox": f"{x0},{y0},{x1},{y1}", "mimeType": "csv", "zip": "no",
                           "providers": ["NWIS", "STORET"]}, timeout=90)
    if r.status_code not in (200, 206): raise ConnectionError(f"HTTP {r.status_code}")
    rows = list(csv.DictReader(io.StringIO(r.text)))
    num = lambda s: float(s) if s else np.nan
    return ({"wqp_stations_20km": n_within(g, [num(x["LongitudeMeasure"]) for x in rows],
                                           [num(x["LatitudeMeasure"]) for x in rows])},
            {"obs_date": "all-time", "res_m": "point", "release": "WQX 2.2 Station", "url": WQP})

OPEN_METEO = "https://archive-api.open-meteo.com/v1/archive"

def _a_climate(aoi):
    c = aoi["centroid"]
    r = S.get(OPEN_METEO,
              params={"latitude": round(c[1], 3), "longitude": round(c[0], 3),
                      "start_date": "2015-01-01", "end_date": "2024-12-31",
                      "daily": "temperature_2m_mean,precipitation_sum",
                      "timezone": "UTC", "models": "era5"}, timeout=60)
    if r.status_code != 200: raise ConnectionError(f"HTTP {r.status_code}")
    d = r.json()["daily"]
    t = np.array([x for x in d["temperature_2m_mean"] if x is not None], dtype="f8")
    p = np.array([x for x in d["precipitation_sum"] if x is not None], dtype="f8")
    if not t.size or not p.size: raise LookupError("empty series")
    return ({"clim_mean_annual_temp_c": round(float(t.mean()), 2),
             "clim_daily_mean_temp_min_c": round(float(t.min()), 1),
             "clim_daily_mean_temp_max_c": round(float(t.max()), 1),
             "clim_annual_precip_mm": round(float(p.sum() / (p.size / 365.25)), 1),
             "clim_n_days": int(t.size)},
            {"obs_date": "2015-2024", "res_m": 11000, "release": "ERA5 (Open-Meteo archive, models=era5)",
             "url": OPEN_METEO})

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

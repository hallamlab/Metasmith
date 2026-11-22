"""adapters_physical.py -- physical layers from static open archives.

Lane: forest_change, sea_ice, permafrost_extent, glaciers, field_boundaries,
embeddings, bathymetry, surficial_geology, gamma_radiometrics,
tailings_facilities, mines_producing.

Contract matches pipeline.py:  _a_<name>(aoi) -> (features: dict[str, scalar], meta: dict)
  * flat scalars only, source-prefixed lowercase names
  * PermissionError  -> out of coverage      (recorded not-attempted)
  * any other raise   -> unavailable
  * ADAPTERS is a list of (slot, source_id, family, licence, auth, fn)

Every endpoint is plain public HTTPS: no API keys, no tokens, no connectors.

Static archives are fetched ONCE into ./cache/ by thread-safe lazy loaders and
joined in memory afterwards; global single rasters are read through windowed
/vsicurl range requests and never downloaded whole.

Requires, beyond pipeline.py's own imports: geopandas, pyogrio, pandas,
fsspec (HTTP range reads of the remote GeoParquet) and netCDF4.  Measured
cold-start cache footprint for the whole lane: 161 MB in ./cache/.

NOTE on the local windowed reader (_rwin): pipeline.read_window() calls
rasterio with masked=True together with boundless=True.  For a raster whose
nodata is undefined, rasterio then builds a boundless VRT whose nodata equals
fill_value (0), so every genuine zero pixel is masked out.  That is harmless
for continuous PC assets but destroys Hansen-type byte layers where 0 is a
valid value ("no tree cover", "no loss", "no gain") -- measured on
stress_oilsands_region, Hansen treecover2000 reads 33.35 % with zeros kept and
67.49 % with them masked.  _rwin() is pipeline.read_window() with that single
behaviour fixed (plain read + explicit nodata/geometry mask); pipeline.py is
not modified.
"""
import os, io, sys, time, json, math, zipfile, threading, warnings
import numpy as np
import requests

from . import pipeline as P
from .pipeline import S, stats, MAXPIX     # reused helpers

import rasterio
from rasterio.windows import from_bounds
from rasterio.warp import transform_bounds
from rasterio.features import geometry_mask
from rasterio.transform import from_bounds as aff_from_bounds
import geopandas as gpd
import pandas as pd
import pyproj
from shapely.geometry import shape, box
from shapely.ops import unary_union

warnings.filterwarnings("ignore")

CACHE = os.path.abspath("./cache")
os.makedirs(CACHE, exist_ok=True)

# Striped (1 x 40000) Hansen tiles need large VSI chunks or every scanline
# becomes its own range request: 900 rows took 217 s untuned, 1.9 s tuned.
GENV = dict(P.GDAL_ENV)
GENV.update(GDAL_HTTP_MERGE_CONSECUTIVE_RANGES="YES",
            CPL_VSIL_CURL_CHUNK_SIZE="10485760",
            CPL_VSIL_CURL_CACHE_SIZE="268435456",
            VSI_CACHE_SIZE="134217728",
            GDAL_HTTP_VERSION="2")

CONTROL_BBOX = [-140.0, -20.0, -139.0, -19.0]     # mid-Pacific filtering control

# ----------------------------------------------------------------- endpoints
HANSEN_VER = "GFC-2025-v1.13"
HANSEN = ("https://earthenginepartners-hansen.storage.googleapis.com/"
          f"{HANSEN_VER}/Hansen_{HANSEN_VER}_{{lyr}}_{{tile}}.tif")
ETOPO = ("https://www.ngdc.noaa.gov/mgg/global/relief/ETOPO2022/data/30s/"
         "30s_bed_elev_gtif/ETOPO_2022_v1_30s_N90W180_bed.tif")
CSB = "https://data.source.coop/fiboa/us-usda-cropland/us_usda_cropland.parquet"
CLAY = "https://data.source.coop/clay/clay-model-v0-embeddings/"
SIC_ROOT = "https://www.ncei.noaa.gov/data/sea-ice-concentration/access/"
NE_GLAC = ("https://raw.githubusercontent.com/nvkelso/natural-earth-vector/"
           "master/geojson/ne_10m_glaciated_areas.geojson")
NRCAN_FTP = "https://ftp.maps.canada.ca/pub/nrcan_rncan/"
PERMAFROST = (NRCAN_FTP + "Surficial-geology_Geologie-des-depots-meubles/"
              "permafrost-pergelisol/permafrost_atlas_of_canada_en.shp.zip")
SURFICIAL = (NRCAN_FTP + "Surficial-geology_Geologie-des-depots-meubles/"
             "surficial_geology_index_geologie_surface/gsc_surficial_geology.gdb.zip")
MINES_900A = (NRCAN_FTP + "Mining-industry_Industrie-miniere/900A_and_top_100/"
              "SHP/900A_75th_2025_shape.zip")
MINEPLANT = "https://mrdata.usgs.gov/mineplant/mineplant-csv.zip"
NAMRAD = "https://mrdata.usgs.gov/radiometric/data/NAMrad_{el}.zip"
# Geosoft '*Dnag' grid projection declared in NAMrad_*.flt.xml:
# transverse mercator on a sphere of the Clarke-1866 semi-major axis,
# lon_0 = -100, k = 0.926, false easting/northing 0, datum NAD27.
NAMRAD_CRS = ("+proj=tmerc +lat_0=0 +lon_0=-100 +k=0.926 +x_0=0 +y_0=0 "
              "+R=6378206.4 +units=m +no_defs")

_LOCKS = {}
_MEM = {}


def _once(key, build):
    """Thread-safe module-level lazy loader (pipeline runs an 8-thread pool)."""
    if key in _MEM:
        return _MEM[key]
    lk = _LOCKS.setdefault(key, threading.Lock())
    with lk:
        if key not in _MEM:
            _MEM[key] = build()
    return _MEM[key]


def _fetch(url, path, timeout=600):
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        tmp = path + ".part"
        r = S.get(url, timeout=timeout, stream=True)
        r.raise_for_status()
        with open(tmp, "wb") as fh:
            for ch in r.iter_content(1 << 20):
                fh.write(ch)
        os.replace(tmp, path)
    return path


def _unzip(zpath, dest):
    if not os.path.isdir(dest):
        with zipfile.ZipFile(zpath) as z:
            z.extractall(dest + ".part")
        os.replace(dest + ".part", dest)
    return dest


# ------------------------------------------------------------- raster reader
def _rwin(url, bbox, geom=None, maxpix=MAXPIX, band=1, nodata=None):
    """Decimated AOI-clipped read that preserves zero-valued pixels.

    Returns a masked 2-D array: masked where outside the AOI geometry or equal
    to an explicitly declared nodata value.  Unlike pipeline.read_window this
    never masks data just because it equals the boundless fill value.
    """
    with rasterio.Env(**GENV):
        with rasterio.open(url) as src:
            b = transform_bounds("EPSG:4326", src.crs, *bbox, densify_pts=21) \
                if src.crs and src.crs.to_epsg() != 4326 else tuple(bbox)
            win = from_bounds(*b, transform=src.transform)
            h, w = max(1, int(round(win.height))), max(1, int(round(win.width)))
            sc = min(1.0, maxpix / max(h, w))
            oh, ow = max(1, int(h * sc)), max(1, int(w * sc))
            nod = src.nodata if nodata is None else nodata
            arr = src.read(band, window=win, out_shape=(oh, ow), boundless=True,
                           fill_value=(nod if nod is not None else 0))
    arr = np.ma.masked_array(arr, mask=np.zeros(arr.shape, dtype=bool))
    if nod is not None:
        arr.mask |= (arr.data == nod)
    arr.mask |= ~np.isfinite(arr.data) if arr.dtype.kind == "f" else False
    if geom is not None and arr.size > 4:
        tr = aff_from_bounds(*bbox, arr.shape[1], arr.shape[0])
        gm = geometry_mask([geom], out_shape=arr.shape, transform=tr, invert=True)
        arr.mask |= ~gm
    return arr


def _aeqd(lon, lat):
    """Local azimuthal-equidistant CRS: distance from (lon, lat) is exact."""
    return pyproj.CRS.from_proj4(
        f"+proj=aeqd +lat_0={lat} +lon_0={lon} +x_0=0 +y_0=0 "
        "+datum=WGS84 +units=m +no_defs")


def _gc_km(lon0, lat0, lon1, lat1):
    p0 = math.radians(float(lat0))
    p1 = np.radians(np.asarray(lat1, dtype="f8"))
    dl = np.radians(np.asarray(lon1, dtype="f8") - float(lon0))
    return 6371.0 * np.arccos(np.clip(
        math.sin(p0) * np.sin(p1) + math.cos(p0) * np.cos(p1) * np.cos(dl), -1, 1))


def _near_far(gdf, aoi, keep_deg=None):
    """Reproject gdf into the AOI-centred AEQD after a coarse great-circle cut."""
    cx, cy = aoi["centroid"]
    rep = gdf.representative_point()
    d = _gc_km(cx, cy, rep.x.values, rep.y.values)
    sub = gdf[d < 8000.0].copy()                    # drop the antipodal half
    if sub.empty:
        return sub, None
    crs = _aeqd(cx, cy)
    return sub.to_crs(crs), crs


# =========================================================== forest_change
def _hansen_tile(bbox):
    lat = int(math.ceil(bbox[3] / 10.0) * 10)
    lon = int(math.floor(bbox[0] / 10.0) * 10)
    lat = min(80, max(-50, lat))
    return (f"{abs(lat):02d}{'N' if lat >= 0 else 'S'}_"
            f"{abs(lon):03d}{'W' if lon < 0 else 'E'}")


def _a_hansen(aoi):
    """Hansen/UMD Global Forest Change v1.13 -- windowed COG read, 30 m."""
    tile = _hansen_tile(aoi["bbox"])
    tc = _rwin(HANSEN.format(lyr="treecover2000", tile=tile),
               aoi["bbox"], aoi["geom"], nodata=None)
    v = tc.compressed().astype("f8")
    if v.size == 0:
        raise LookupError(f"no pixels inside AOI for Hansen tile {tile}")
    f = {"hansen_treecover2000_pct_mean": round(float(v.mean()), 2),
         "hansen_treecover2000_pct_p90": round(float(np.percentile(v, 90)), 1),
         "hansen_treecover2000_frac_ge25pct": round(float((v >= 25).mean()), 4),
         "hansen_n_px": int(v.size)}

    # lossyear / gain tiles are published only where the layer has content, so
    # a 404 on them is data, not a failure: keep the tree-cover features.
    try:
        ly = _rwin(HANSEN.format(lyr="lossyear", tile=tile),
                   aoi["bbox"], aoi["geom"], nodata=None)
        lv = ly.compressed().astype("int16")
    except rasterio.errors.RasterioIOError:
        lv = np.array([], dtype="int16")
        f["hansen_lossyear_tile_published"] = False
    if lv.size:
        lost = lv[lv > 0]
        f["hansen_loss_frac_2001_2024"] = round(float(lost.size / lv.size), 5)
        if lost.size:
            yrs, cnt = np.unique(lost, return_counts=True)
            f["hansen_loss_modal_year"] = int(2000 + int(yrs[int(np.argmax(cnt))]))
            f["hansen_loss_n_years_affected"] = int(yrs.size)
            f["hansen_loss_last_year"] = int(2000 + int(yrs.max()))
        else:
            f["hansen_loss_modal_year"] = 0
            f["hansen_loss_n_years_affected"] = 0

    try:
        gn = _rwin(HANSEN.format(lyr="gain", tile=tile),
                   aoi["bbox"], aoi["geom"], nodata=None)
        gv = gn.compressed()
    except rasterio.errors.RasterioIOError:
        gv = np.array([], dtype="uint8")
        f["hansen_gain_tile_published"] = False
    if gv.size:
        f["hansen_gain_frac_2000_2012"] = round(float((gv > 0).mean()), 5)
    net = f.get("hansen_gain_frac_2000_2012", 0.0) - f.get("hansen_loss_frac_2001_2024", 0.0)
    f["hansen_net_change_frac"] = round(float(net), 5)
    return f, {"obs_date": "2000-2024", "res_m": 30,
               "item": f"Hansen_{HANSEN_VER}_{tile}"}


# ========================================================= field_boundaries
def _csb_fs():
    def build():
        import fsspec
        # aiohttp ignores HTTP(S)_PROXY unless trust_env is set
        return fsspec.filesystem("https", client_kwargs={"trust_env": True})
    return _once("csb_fs", build)


def _a_field_boundaries(aoi):
    """USDA NASS Crop Sequence Boundaries 2023 as fiboa GeoParquet (16.2 M
    fields, EPSG:5070, GeoParquet 1.1 bbox covering -> row-group pruning)."""
    if aoi["country"] != "US":
        raise PermissionError("out of coverage (USDA CSB is conterminous-US only)")
    tr = pyproj.Transformer.from_crs("EPSG:4326", "EPSG:5070", always_xy=True)
    x0, y0 = tr.transform(aoi["bbox"][0], aoi["bbox"][1])
    x1, y1 = tr.transform(aoi["bbox"][2], aoi["bbox"][3])
    bb = (min(x0, x1), min(y0, y1), max(x0, x1), max(y0, y1))
    fs = _csb_fs()
    with fs.open(CSB, "rb") as fh:
        gdf = gpd.read_parquet(fh, bbox=bb,
                              columns=["geometry", "crop:name", "crop:code"])
    aoi_g = gpd.GeoSeries([shape(aoi["geom"])], crs="EPSG:4326").to_crs(5070).iloc[0]
    if len(gdf) == 0:
        return ({"csb_field_count": 0, "csb_aoi_frac_in_field": 0.0,
                 "csb_field_area_median_ha": 0.0},
                {"obs_date": "2023", "res_m": "vector",
                 "note": "verified absence: no CSB field polygon intersects AOI"})
    gdf = gdf[gdf.intersects(aoi_g)]
    if len(gdf) == 0:
        return ({"csb_field_count": 0, "csb_aoi_frac_in_field": 0.0,
                 "csb_field_area_median_ha": 0.0},
                {"obs_date": "2023", "res_m": "vector",
                 "note": "verified absence: bbox hits but no AOI intersection"})
    inter = gdf.geometry.intersection(aoi_g)
    a_ha = gdf.geometry.area / 1e4                       # EPSG:5070 is equal-area
    f = {"csb_field_count": int(len(gdf)),
         "csb_field_area_median_ha": round(float(a_ha.median()), 2),
         "csb_field_area_max_ha": round(float(a_ha.max()), 2),
         "csb_aoi_frac_in_field": round(float(min(1.0, inter.area.sum() / aoi_g.area)), 4),
         "csb_n_crop_classes": int(gdf["crop:name"].nunique()),
         "csb_dominant_crop": str(gdf["crop:name"].mode().iloc[0])[:40]}
    return f, {"obs_date": "2023", "res_m": "vector", "item": "usda_csb_2023"}


# ================================================================ embeddings
_MGRS_COL = {1: "ABCDEFGH", 2: "JKLMNPQR", 0: "STUVWXYZ"}
_MGRS_ROW_ODD = "ABCDEFGHJKLMNPQRSTUV"
_MGRS_ROW_EVEN = "FGHJKLMNPQRSTUVABCDE"
_MGRS_BANDS = "CDEFGHJKLMNPQRSTUVWX"


def _mgrs_tile(lon, lat):
    """MGRS grid-zone designator + 100 km square id (e.g. 17TPJ)."""
    zone = int(math.floor((lon + 180.0) / 6.0) % 60) + 1
    band = _MGRS_BANDS[int(math.floor((max(-80.0, min(83.9, lat)) + 80.0) / 8.0))]
    utm = pyproj.Transformer.from_crs(
        "EPSG:4326", pyproj.CRS.from_proj4(
            f"+proj=utm +zone={zone} +{'south' if lat < 0 else 'north'} "
            "+datum=WGS84 +units=m +no_defs"), always_xy=True)
    e, n = utm.transform(lon, lat)
    col = _MGRS_COL[zone % 3][int(e // 100000) - 1]
    row = (_MGRS_ROW_ODD if zone % 2 else _MGRS_ROW_EVEN)[int(n // 100000) % 20]
    return f"{zone}{band}{col}{row}"


def _clay_keys():
    def build():
        import re
        keys, tok = [], None
        for _ in range(8):
            p = {"list-type": "2", "max-keys": "1000"}
            if tok:
                p["continuation-token"] = tok
            r = S.get(CLAY, params=p, timeout=120); r.raise_for_status()
            keys += [k for k in re.findall(r"<Key>([^<]+)</Key>", r.text)
                     if k.endswith(".gpq")]
            m = re.search(r"<NextContinuationToken>([^<]+)<", r.text)
            if not m:
                break
            tok = m.group(1)
        idx = {}
        for k in keys:
            idx.setdefault(os.path.basename(k)[:5], []).append(k)
        return idx
    return _once("clay_keys", build)


def _a_clay_embeddings(aoi):
    """Clay v0 foundation-model patch embeddings (768-d) on source.coop,
    one GeoParquet per Sentinel-2 MGRS tile.  Summary scalars only."""
    idx = _clay_keys()
    tid = _mgrs_tile(*aoi["centroid"])
    keys = idx.get(tid)
    if not keys:
        raise LookupError(f"Clay published no embedding for this area (Sentinel-2 tile {tid}; "
                          f"{len(idx)} of ~57k land tiles exist)")
    key = sorted(keys)[-1]
    fs = _csb_fs()
    with fs.open(CLAY + os.path.basename(key), "rb") as fh:
        gdf = gpd.read_parquet(fh)
    tile_mean = np.vstack(gdf["embeddings"].values).mean(axis=0)
    aoi_g = shape(aoi["geom"])
    sel = gdf[gdf.intersects(aoi_g)]
    if len(sel) == 0:
        sel = gdf[gdf.intersects(box(*aoi["bbox"]))]
    if len(sel) == 0:
        raise LookupError(f"Clay tile {tid} published but no patch overlaps AOI")
    E = np.vstack(sel["embeddings"].values).astype("f8")
    m = E.mean(axis=0)
    cos = float(m @ tile_mean / (np.linalg.norm(m) * np.linalg.norm(tile_mean) + 1e-12))
    f = {"clay_emb_dim": int(E.shape[1]),
         "clay_emb_patch_count": int(E.shape[0]),
         "clay_emb_l2_norm": round(float(np.linalg.norm(m)), 4),
         "clay_emb_l2_norm_patch_mean": round(float(np.linalg.norm(E, axis=1).mean()), 4),
         "clay_emb_cos_to_tile_mean": round(cos, 5),
         "clay_emb_component_std": round(float(m.std()), 5),
         "clay_emb_c0": round(float(m[0]), 5), "clay_emb_c1": round(float(m[1]), 5),
         "clay_emb_c2": round(float(m[2]), 5), "clay_emb_c3": round(float(m[3]), 5),
         "clay_emb_mgrs_tile": tid}
    return f, {"obs_date": str(sel["date"].iloc[0])[:10], "res_m": 10,
               "item": os.path.basename(key)}


# =================================================================== sea_ice
def _sic_grid():
    def build():
        import re
        yrs = sorted(re.findall(r'href="(\d{4})/?"',
                                S.get(SIC_ROOT, timeout=120).text))
        if not yrs:
            raise LookupError("NCEI sea-ice CDR year index empty")
        y = yrs[-1]
        fl = sorted(set(re.findall(r'href="(seaice_conc_daily_nh_[^"]+\.nc)"',
                                   S.get(SIC_ROOT + y + "/", timeout=120).text)))
        if not fl:
            raise LookupError(f"NCEI sea-ice CDR {y} file index empty")
        fn = fl[-1]
        p = _fetch(SIC_ROOT + y + "/" + fn, os.path.join(CACHE, fn))
        import netCDF4
        d = netCDF4.Dataset(p)
        lat = np.asarray(d.variables["latitude"][:], dtype="f8")
        lon = np.asarray(d.variables["longitude"][:], dtype="f8")
        sic = d.variables["seaice_conc_cdr"][0]
        return dict(lat=lat, lon=lon, sic=sic, file=fn,
                    date=fn.split("_")[-2])
    return _once("sic", build)


def _a_sea_ice(aoi):
    """NOAA/NSIDC passive-microwave Sea Ice Concentration CDR, 25 km northern-
    hemisphere polar-stereographic grid.  A continental AOI is land-masked in
    this grid: that is a verified zero, not a failure."""
    g = _sic_grid()
    lat, lon, sic = g["lat"], g["lon"], g["sic"]
    x0, y0, x1, y1 = aoi["bbox"]
    inbox = (lat >= y0) & (lat <= y1) & (lon >= x0) & (lon <= x1)
    cx, cy = aoi["centroid"]
    dkm = _gc_km(cx, cy, lon, lat)
    if cy < 25.0:
        raise PermissionError(
            f"out of coverage (AOI centroid {cy:.1f} deg lat; the CDR grid is "
            "northern-hemisphere sea ice only)")
    j = np.unravel_index(int(np.argmin(dkm)), dkm.shape)
    near_km = float(dkm[j])
    cells = int(inbox.sum())
    v = sic[inbox] if cells else sic[j[0]:j[0] + 1, j[1]:j[1] + 1]
    nval = int(np.ma.count(v))
    outside = near_km > 400.0
    f = {"nsidc_sic_cells_in_aoi": cells,
         "nsidc_sic_valid_cells": 0 if outside else nval,
         "nsidc_sic_nearest_cell_km": round(near_km, 2),
         "nsidc_sic_frac_mean": round(float(np.ma.mean(v)), 4) if (nval and not outside) else 0.0,
         "nsidc_sic_frac_max": round(float(np.ma.max(v)), 4) if (nval and not outside) else 0.0,
         "nsidc_sic_present": bool(nval and not outside and float(np.ma.max(v)) > 0.15),
         "nsidc_sic_in_grid_footprint": bool(not outside)}
    if outside:
        note = (f"verified absence: AOI lies {near_km:.0f} km outside the NH "
                "25-km sea-ice grid footprint")
    elif not nval:
        note = "verified absence: nearest 25-km CDR cell is land-masked"
    else:
        note = ""
    return f, {"obs_date": f"{g['date'][:4]}-{g['date'][4:6]}-{g['date'][6:8]}",
               "res_m": 25000, "item": g["file"], "note": note}


# ================================================================== glaciers
def _ne_glaciers():
    def build():
        p = _fetch(NE_GLAC, os.path.join(CACHE, "ne_10m_glaciated_areas.geojson"))
        g = gpd.read_file(p)
        g["gname"] = g.get("name").fillna("unnamed") if "name" in g else "unnamed"
        return g[["gname", "geometry"]].set_crs(4326, allow_override=True)
    return _once("ne_glac", build)


def _a_glaciers(aoi):
    """Natural Earth 10 m glaciated areas -- generalised global ice extent
    (~1:10 M).  Reports presence, AOI fraction and distance to nearest."""
    g = _ne_glaciers()
    sub, crs = _near_far(g, aoi)
    if sub is None or sub.empty:
        raise LookupError("no glaciated polygon within 8000 km of AOI")
    aoi_g = gpd.GeoSeries([shape(aoi["geom"])], crs="EPSG:4326").to_crs(crs).iloc[0]
    d_m = sub.distance(aoi_g)
    k = int(np.argmin(d_m.values))
    inter = sub.geometry.intersection(aoi_g).area.sum()
    f = {"neglac_nearest_km": round(float(d_m.min()) / 1000.0, 2),
         "neglac_nearest_name": str(sub["gname"].iloc[k])[:48],
         "neglac_frac_in_aoi": round(float(min(1.0, inter / aoi_g.area)), 5),
         "neglac_count_within_100km": int((d_m <= 100_000).sum()),
         "neglac_count_within_250km": int((d_m <= 250_000).sum()),
         "neglac_present_in_aoi": bool(inter > 0)}
    return f, {"obs_date": "2023 (NE v5.1.1)", "res_m": "vector (1:10M generalised)",
               "item": "ne_10m_glaciated_areas",
               "note": "coarse extent: use for >=10 km presence/distance only"}


# ========================================================= permafrost_extent
def _permafrost():
    def build():
        z = _fetch(PERMAFROST, os.path.join(CACHE, "permafrost_atlas.zip"))
        d = _unzip(z, os.path.join(CACHE, "permafrost_atlas"))
        g = gpd.read_file(os.path.join(d, "permafrost_pg.shp"))
        return g.to_crs(4326)
    return _once("permafrost", build)


def _a_permafrost(aoi):
    """Permafrost Atlas of Canada (GSC) -- permafrost zonation and ground-ice
    content polygons."""
    if aoi["country"] != "CA":
        raise PermissionError("out of coverage (Permafrost Atlas of Canada is CA-only)")
    g = _permafrost()
    sub, crs = _near_far(g, aoi)
    if sub is None or sub.empty:
        raise LookupError("permafrost atlas returned no polygon near AOI")
    aoi_g = gpd.GeoSeries([shape(aoi["geom"])], crs="EPSG:4326").to_crs(crs).iloc[0]
    # the atlas maps "No permafrost (0%)" as a zone too; only the other zones are permafrost
    sub = sub[~sub["PMF_E"].astype(str).str.startswith("No permafrost")]
    if sub.empty:
        raise LookupError("no permafrost zone near AOI")
    d_m = sub.distance(aoi_g)
    hit = sub[d_m <= 0]
    f = {"nrcanpf_nearest_km": round(float(d_m.min()) / 1000.0, 2),
         "nrcanpf_present_in_aoi": bool(len(hit) > 0),
         "nrcanpf_count_within_100km": int((d_m <= 100_000).sum())}
    if len(hit):
        inter = hit.geometry.intersection(aoi_g)
        f["nrcanpf_frac_in_aoi"] = round(float(min(1.0, inter.area.sum() / aoi_g.area)), 4)
        dom = hit.loc[inter.area.idxmax()]
        f["nrcanpf_zone_class"] = str(dom.get("PMF_E") or "")[:48]
        f["nrcanpf_ice_class"] = str(dom.get("ICE_E") or "")[:40]
        f["nrcanpf_zone_code"] = str(dom.get("PMF_CLASS") or "")[:8]
    else:
        f["nrcanpf_frac_in_aoi"] = 0.0
        f["nrcanpf_zone_class"] = "no permafrost"
    return f, {"obs_date": "2019 (Atlas of Canada, permafrost)", "res_m": "vector",
               "item": "permafrost_pg"}


# ================================================================ bathymetry
def _a_bathymetry(aoi):
    """ETOPO 2022 v1 30-arcsec bedrock global relief, windowed /vsicurl read of
    the single 1.6 GB global GeoTIFF (256x256 tiled)."""
    arr = _rwin(ETOPO, aoi["bbox"], aoi["geom"], maxpix=128)
    v = arr.compressed().astype("f8")
    if v.size == 0:
        raise LookupError("ETOPO window returned no valid cells")
    f = stats(arr, "etopo_bed_elev_m", nd=1)
    f.update({"etopo_min_elev_m": round(float(v.min()), 1),
              "etopo_frac_below_sea_level": round(float((v < 0).mean()), 4),
              "etopo_max_depth_m": round(float(-v.min()), 1) if v.min() < 0 else 0.0,
              "etopo_is_submarine": bool((v < 0).mean() > 0.5)})
    return f, {"obs_date": "2022", "res_m": 926,
               "item": "ETOPO_2022_v1_30s_bed",
               "note": "global relief model: negative = below sea level"}


# ========================================================= surficial_geology
def _surficial():
    def build():
        z = _fetch(SURFICIAL, os.path.join(CACHE, "gsc_surficial.zip"))
        d = _unzip(z, os.path.join(CACHE, "gsc_surficial"))
        gdb = None
        for root, dirs, _ in os.walk(d):
            for dd in dirs:
                if dd.endswith(".gdb"):
                    gdb = os.path.join(root, dd)
        if gdb is None:
            raise LookupError("no .gdb inside gsc_surficial_geology.gdb.zip")
        g = gpd.read_file(gdb, layer="geology").to_crs(4326)
        # a few GSC polygons are self-intersecting and raise
        # TopologyException on intersection (hit on stress_city_toronto_full)
        bad = ~g.geometry.is_valid
        if bad.any():
            g.loc[bad, "geometry"] = g.loc[bad, "geometry"].make_valid()
        return g
    return _once("surficial", build)


def _a_surficial(aoi):
    """Surficial Geology of Canada (GSC Map 1880A, 1:5 M) -- Quaternary
    sediment units."""
    if aoi["country"] != "CA":
        raise PermissionError("out of coverage (GSC surficial geology is CA-only)")
    g = _surficial()
    aoi_g = shape(aoi["geom"])
    hit = g[g.intersects(aoi_g)]
    if len(hit) == 0:
        raise LookupError("no GSC surficial polygon intersects AOI")
    inter = hit.geometry.intersection(aoi_g)
    dom = hit.loc[inter.area.idxmax()]
    f = {"gsc_surf_unit": str(dom.get("geology") or "")[:60],
         "gsc_surf_label": str(dom.get("label") or "")[:12],
         "gsc_surf_description": str(dom.get("description_en") or "")[:110],
         "gsc_surf_n_units_in_aoi": int(len(hit)),
         "gsc_surf_dominant_frac": round(float(min(1.0, inter.area.max() / aoi_g.area)), 4)}
    return f, {"obs_date": "2014 (GSC Map 1880A)", "res_m": "vector (1:5M)",
               "item": "gsc_surficial_geology/geology"}


# ======================================================== gamma_radiometrics
def _namrad(el):
    def build():
        z = _fetch(NAMRAD.format(el=el), os.path.join(CACHE, f"NAMrad_{el}.zip"))
        d = _unzip(z, os.path.join(CACHE, f"namrad_{el}"))
        flt = os.path.join(d, f"NAMrad_{el}.flt")
        tif = os.path.join(CACHE, f"namrad_{el}.tif")
        if not os.path.exists(tif):
            with rasterio.open(flt) as src:
                prof = src.profile
                prof.update(driver="GTiff", crs=NAMRAD_CRS, tiled=True,
                            blockxsize=256, blockysize=256, compress="deflate",
                            predictor=2, nodata=-9999.0)
                a = src.read(1)
            with rasterio.open(tif + ".part", "w", **prof) as dst:
                dst.write(a, 1)
            os.replace(tif + ".part", tif)
        for junk in (z, flt):                      # keep the cache small
            try:
                os.remove(junk)
            except OSError:
                pass
        return tif
    return _once(f"namrad_{el}", build)


def _a_radiometrics(aoi):
    """USGS/GSC North American airborne gamma-ray compilation (Duval, Darnley &
    Garrett 1990) -- %K, ppm eU, ppm eTh on a 2 km Geosoft '*Dnag' grid."""
    # the grid cell is 2 km, so a 5 km2 AOI is sub-pixel: sample the cells
    # overlapping the AOI padded by one cell and do not apply a sub-pixel mask
    x0, y0, x1, y1 = aoi["bbox"]
    pad = 0.02
    bb = [x0 - pad, y0 - pad, x1 + pad, y1 + pad]
    out, got = {"namrad_sample_pad_deg": pad}, 0
    for el, nm, unit in (("K", "k_pct", "%K"), ("U", "u_ppm", "ppm eU"),
                         ("Th", "th_ppm", "ppm eTh")):
        tif = _namrad(el)
        arr = _rwin(tif, bb, None, maxpix=64, nodata=-9999.0)
        v = arr.compressed().astype("f8")
        if v.size == 0:
            continue
        got += 1
        out[f"namrad_{nm}_mean"] = round(float(v.mean()), 3)
        out[f"namrad_{nm}_max"] = round(float(v.max()), 3)
    if got == 0:
        raise LookupError("AOI outside the North American radiometric grid "
                          "(no surveyed cell)")
    k = out.get("namrad_k_pct_mean"); th = out.get("namrad_th_ppm_mean")
    u = out.get("namrad_u_ppm_mean")
    if k and th:
        out["namrad_th_k_ratio"] = round(th / k, 3)
    if th and u:
        out["namrad_u_th_ratio"] = round(u / th, 4)
    return out, {"obs_date": "1960-1983 (NURE/GSC surveys, 2009 compilation)",
                 "res_m": 2000, "item": "NAMrad_K/U/Th"}


# =========================================================== mines_producing
def _mines_ca():
    def build():
        z = _fetch(MINES_900A, os.path.join(CACHE, "900A_2025.zip"))
        d = _unzip(z, os.path.join(CACHE, "mines_900a"))
        shp = None
        for root, _, files in os.walk(d):
            for fn in files:
                if fn.endswith(".shp") and "ProducingMines" in fn:
                    shp = os.path.join(root, fn)
        if shp is None:
            raise LookupError("ProducingMines shapefile not found in 900A archive")
        return gpd.read_file(shp).to_crs(4326)
    return _once("mines_ca", build)


def _mines_us():
    def build():
        z = _fetch(MINEPLANT, os.path.join(CACHE, "mineplant.zip"))
        with zipfile.ZipFile(z) as zf:
            df = pd.read_csv(io.BytesIO(zf.read("mineplant.csv")),
                             encoding="latin-1", low_memory=False)
        df.columns = [c.upper() for c in df.columns]
        df = df.dropna(subset=["LONGITUDE", "LATITUDE"])
        return gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df["LONGITUDE"],
                                                                df["LATITUDE"]),
                                crs="EPSG:4326")
    return _once("mines_us", build)


def _mines_feats(g, aoi, pref, namecol, prodcol):
    sub, crs = _near_far(g, aoi)
    if sub is None or sub.empty:
        raise LookupError("no producing-mine record near AOI")
    aoi_g = gpd.GeoSeries([shape(aoi["geom"])], crs="EPSG:4326").to_crs(crs).iloc[0]
    d_m = sub.distance(aoi_g)
    k = d_m.idxmin()
    f = {f"{pref}_nearest_km": round(float(d_m.min()) / 1000.0, 3),
         f"{pref}_count_in_aoi": int((d_m <= 0).sum()),
         f"{pref}_count_within_25km": int((d_m <= 25_000).sum()),
         f"{pref}_count_within_100km": int((d_m <= 100_000).sum()),
         f"{pref}_nearest_name": str(sub.loc[k, namecol])[:48],
         f"{pref}_total_records": int(len(g))}
    if prodcol and prodcol in sub.columns:
        f[f"{pref}_nearest_commodity"] = str(sub.loc[k, prodcol])[:48]
    return f


def _a_mines_ca(aoi):
    """NRCan Map 900A (75th ed., 2025) -- Principal mineral areas, producing
    mines and oil & gas fields in Canada; ProducingMines point layer."""
    if aoi["country"] != "CA":
        raise PermissionError("out of coverage (NRCan Map 900A is CA-only)")
    f = _mines_feats(_mines_ca(), aoi, "nrcan900a", "NAME_E", "PRD_DESC_E")
    return f, {"obs_date": "2025 (900A 75th edition)", "res_m": "point",
               "item": "900A_75th_ProducingMines"}


def _a_mines_us(aoi):
    """USGS MRData 'Mines and mineral plants' (mineplant) -- active US mines and
    mineral processing plants."""
    if aoi["country"] != "US":
        raise PermissionError("out of coverage (USGS mineplant is US-only)")
    g = _mines_us()
    mines = g[g["PLANT_MIN"].astype(str).str.upper().str.startswith("M")]
    if mines.empty:                       # schema drift -> fall back to all sites
        mines = g
    f = _mines_feats(mines, aoi, "usgsmineplant", "SITE_NAME", "COMMODITY")
    plants = g[~g.index.isin(mines.index)]
    if len(plants):
        sub, crs = _near_far(plants, aoi)
        if sub is not None and not sub.empty:
            aoi_g = gpd.GeoSeries([shape(aoi["geom"])],
                                  crs="EPSG:4326").to_crs(crs).iloc[0]
            dp = sub.distance(aoi_g)
            f["usgsmineplant_plant_nearest_km"] = round(float(dp.min()) / 1000.0, 3)
            f["usgsmineplant_plant_count_within_25km"] = int((dp <= 25_000).sum())
    return f, {"obs_date": "2016 (USGS MRData mineplant)", "res_m": "point",
               "item": "usgs_mineplant"}


# -------------------------------------------------------------------- registry
# slot, source_id, family, licence, auth, fn
ADAPTERS = [
 ("forest_change",      "hansen_gfc_v1_13",   "macrobiology", "CC-BY-4.0 (Hansen/UMD/Google/USGS/NASA)", "none", _a_hansen),
 ("field_boundaries",   "usda_csb_fiboa",     "macrobiology", "public-domain (USDA NASS)",               "none", _a_field_boundaries),
 ("embeddings",         "clay_v0_embeddings", "embeddings",   "CC-BY-4.0 (Clay Foundation)",             "none", _a_clay_embeddings),
 ("sea_ice",            "noaa_nsidc_sic_cdr", "cryosphere",   "public-domain (NOAA/NSIDC CDR)",          "none", _a_sea_ice),
 ("glaciers",           "natural_earth_10m",  "cryosphere",   "public-domain (Natural Earth)",           "none", _a_glaciers),
 ("permafrost_extent",  "nrcan_permafrost",   "cryosphere",   "OGL-Canada-2.0",                          "none", _a_permafrost),
 ("bathymetry",         "etopo_2022_30s_bed", "geology",      "public-domain (NOAA NCEI)",               "none", _a_bathymetry),
 ("surficial_geology",  "gsc_surficial_1880a","geology",      "OGL-Canada-2.0",                          "none", _a_surficial),
 ("gamma_radiometrics", "usgs_namrad",        "geology",      "public-domain (USGS)",                    "none", _a_radiometrics),
 ("mines_producing",    "nrcan_900a",         "infrastructure","OGL-Canada-2.0",                         "none", _a_mines_ca),
 ("mines_producing",    "usgs_mineplant",     "infrastructure","public-domain (USGS)",                   "none", _a_mines_us),
]

# tailings_facilities is deliberately NOT registered.  The Global Tailings
# Portal (tailing.grida.no) serves its facility database only behind account
# registration -- /map/data/, /map/overview/ and /map/geojson/ all return the
# identical 76 280-byte shell to an anonymous client and there is no bulk
# CSV/shapefile endpoint.  Per the no-credentials rule this lane ships nothing
# for that slot rather than embedding a login.  The open substitutes located
# (Zenodo 7894216 global mining footprint, 320 MB; Zenodo 7307210, 229 MB) each
# exceed the whole lane's ~300 MB cache budget on their own.

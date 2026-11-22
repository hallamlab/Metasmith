"""Infrastructure & built-form adapters for the AOI -> feature pipeline.

LANE: infrastructure and built form.
Slots filled: railways, pipelines, transmission_lines, power_plants,
industrial_facilities, addresses, cell_towers, ground_level_imagery, gazetteer.

ROUTE NOTE (deviation from the task's preferred route #1, Geofabrik):
    Measured 2026-10-13 with HEAD on download.geofabrik.de, the
    `-latest-free.shp.zip` sub-region extracts needed to cover the test AOIs are
        saskatchewan 252 MB | ontario 2059 MB | alberta 671 MB | quebec 2145 MB
        iowa 265 MB | utah 327 MB | michigan 665 MB | british-columbia: none published (404)
    Every one exceeds the lane's ~150 MB per-extract rule and the ~300 MB total
    cache budget, and BC ships no shapefile at all.  The Geofabrik *free
    shapefile* schema also has no power, pipeline or address layers
    (railways/roads/buildings/pois/landuse/places/traffic/transport only), so
    three of the nine slots are unreachable by that route regardless of size.
    Substituted route: Overture Maps public GeoParquet (keyless anonymous HTTPS,
    bbox predicate pushdown via parquet row-group statistics).  Overture carries
    the OSM planet plus Google/Microsoft open buildings and OpenAddresses, so it
    covers the same ODbL content with ~12 MB of cache instead of ~6.8 GB.

ALL ROUTES ARE KEYLESS PUBLIC HTTPS.  No credentials, no connectors, no host.*
calls.  Token-gated sources (OpenCelliD, Mapillary, KartaView) are probed and
reported as auth-blocked rather than silently filled.

Cache: ./cache/ -- ovt_manifest_<theme>_<type>.json (file-level bboxes, ~180 KB
total) + gppd_power_plants.csv (12 MB).  Total lane footprint ~12.2 MB.
Call warm_cache() once before a batch run to pay the manifest build (~40 s) up
front; afterwards every adapter is a few MB of HTTP range reads per AOI.
"""
import os, sys, io, json, time, math, threading, collections

import numpy as np
import pandas as pd
import requests
import shapely
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
from shapely.geometry import shape

from . import pipeline as P

S = P.S                                   # reuse pipeline's requests.Session
# Sent per-request rather than mutated onto the shared session, so other lanes'
# adapters are unaffected.  WDQS rejects the requests default UA with HTTP 403.
UA = {"User-Agent": "open-geospatial-dossier/0.1 (open AOI feature extraction; "
                    "query-only, modest rate)"}

CACHE = os.path.abspath("./cache")
os.makedirs(CACHE, exist_ok=True)

OVT = "https://overturemaps-us-west-2.s3.amazonaws.com/"
OVT_PIN = "2026-09-23.1"                  # fallback if the release listing is unavailable
CONTROL_BBOX = [-140.0, -20.0, -139.0, -19.0]     # mid-Pacific no-data control
_S3NS = "{http://s3.amazonaws.com/doc/2006-03-01/}"

# Scoped locks.  _MLOCK/_GLOCK are reentrant and may be held across network IO
# (one-time builds); _CLOCK/_XLOCK only ever guard a dict access, never IO.
_MLOCK = threading.RLock()                # release id + manifest build
_GLOCK = threading.RLock()                # GPPD CSV fetch
_CLOCK = threading.Lock()                 # read cache dict
_XLOCK = threading.Lock()                 # control-probe dict
_REL, _MAN, _CTRL, _GPPD = {}, {}, {}, {}
_RDC = collections.OrderedDict()          # bounded read cache, shared across threads

DEG_LAT_M = 110540.0
DEG_LON_M = 111320.0


# ----------------------------------------------------------------- range reader
class _RangeFile(io.RawIOBase):
    """Seekable read-only file over HTTP range requests (parquet footer + row groups)."""

    def __init__(self, url, size=None):
        self.url = url
        self._pos = 0
        if size is None:
            h = S.head(url, timeout=30, allow_redirects=True)
            h.raise_for_status()
            size = int(h.headers["Content-Length"])
        self.size = int(size)

    def readable(self):
        return True

    def seekable(self):
        return True

    def tell(self):
        return self._pos

    def seek(self, off, whence=0):
        self._pos = off if whence == 0 else (self._pos + off if whence == 1 else self.size + off)
        return self._pos

    def read(self, n=-1):
        if n is None or n < 0:
            n = self.size - self._pos
        n = min(n, self.size - self._pos)
        if n <= 0:
            return b""
        r = S.get(self.url, headers={"Range": f"bytes={self._pos}-{self._pos + n - 1}"}, timeout=120)
        r.raise_for_status()
        b = r.content
        self._pos += len(b)
        return b

    def readall(self):
        return self.read(-1)


def _s3ls(prefix, delim="/"):
    import xml.etree.ElementTree as ET
    out, tok = [], None
    while True:
        p = {"list-type": "2", "prefix": prefix, "max-keys": "1000"}
        if delim:
            p["delimiter"] = delim
        if tok:
            p["continuation-token"] = tok
        r = S.get(OVT, params=p, timeout=60)
        r.raise_for_status()
        x = ET.fromstring(r.content)
        out += [(c.find(_S3NS + "Key").text, int(c.find(_S3NS + "Size").text))
                for c in x.findall(_S3NS + "Contents")]
        out += [(c.find(_S3NS + "Prefix").text, -1) for c in x.findall(_S3NS + "CommonPrefixes")]
        tok = x.findtext(_S3NS + "NextContinuationToken")
        if not tok:
            break
    return out


def _release():
    """Newest Overture release id; falls back to the pinned one."""
    if "r" not in _REL:
        with _MLOCK:
            if "r" not in _REL:
                try:
                    rels = sorted(k for k, _ in _s3ls("release/"))
                    _REL["r"] = rels[-1].split("/")[1] if rels else OVT_PIN
                except Exception:
                    _REL["r"] = OVT_PIN
    return _REL["r"]


def _bbox_cols(md):
    p = [md.row_group(0).column(j).path_in_schema for j in range(md.row_group(0).num_columns)]
    return {q.split(".")[-1]: j for j, q in enumerate(p) if q.startswith("bbox.")}


def _file_bbox(key, size):
    md = pq.ParquetFile(_RangeFile(OVT + key, size=size)).metadata
    ix = _bbox_cols(md)
    lx, hx, ly, hy = [], [], [], []
    for i in range(md.num_row_groups):
        rg = md.row_group(i)
        lx.append(rg.column(ix["xmin"]).statistics.min)
        hx.append(rg.column(ix["xmax"]).statistics.max)
        ly.append(rg.column(ix["ymin"]).statistics.min)
        hy.append(rg.column(ix["ymax"]).statistics.max)
    return key, [min(lx), min(ly), max(hx), max(hy)], size


def _manifest(theme, tp):
    """{key: {bbox, size}} for one Overture theme/type.  Built once, cached on disk."""
    k = (theme, tp)
    if k in _MAN:
        return _MAN[k]
    with _MLOCK:
        if k in _MAN:
            return _MAN[k]
        fp = os.path.join(CACHE, f"ovt_manifest_{theme}_{tp}.json")
        if os.path.exists(fp):
            _MAN[k] = json.load(open(fp))
            return _MAN[k]
        import concurrent.futures as cf
        keys = [(a, b) for a, b in _s3ls(f"release/{_release()}/theme={theme}/type={tp}/", delim=None)
                if a.endswith(".parquet")]
        if not keys:
            raise LookupError(f"no Overture parquet parts for {theme}/{tp}")
        out = {}
        with cf.ThreadPoolExecutor(12) as ex:
            for key, bb, sz in ex.map(lambda a: _file_bbox(*a), keys):
                out[key] = {"bbox": [round(v, 5) for v in bb], "size": sz}
        json.dump(out, open(fp, "w"))
        _MAN[k] = out
        return out


def _read(theme, tp, bbox, columns):
    """Rows of one Overture theme/type whose feature bbox intersects `bbox`.

    Prunes to intersecting part files via the cached manifest, then to
    intersecting row groups via parquet bbox statistics, then filters exactly.
    Returns a pyarrow.Table (possibly 0 rows), or None if no part file covers it.
    """
    ck = (theme, tp, tuple(round(v, 6) for v in bbox), tuple(columns))
    with _CLOCK:
        if ck in _RDC:
            _RDC.move_to_end(ck)
            return _RDC[ck]
    man = _manifest(theme, tp)
    x0, y0, x1, y1 = bbox
    cands = [(k, v) for k, v in man.items()
             if not (v["bbox"][2] < x0 or v["bbox"][0] > x1
                     or v["bbox"][3] < y0 or v["bbox"][1] > y1)]
    if not cands:
        return None
    cols = list(dict.fromkeys(list(columns) + ["bbox"]))
    tabs = []
    for key, v in cands:
        pf = pq.ParquetFile(_RangeFile(OVT + key, size=v["size"]))
        md = pf.metadata
        ix = _bbox_cols(md)
        sel = []
        for i in range(md.num_row_groups):
            rg = md.row_group(i)
            a = rg.column(ix["xmin"]).statistics.min
            b = rg.column(ix["xmax"]).statistics.max
            c = rg.column(ix["ymin"]).statistics.min
            d = rg.column(ix["ymax"]).statistics.max
            if not (b < x0 or a > x1 or d < y0 or c > y1):
                sel.append(i)
        if not sel:
            continue
        t = pf.read_row_groups(sel, columns=cols)
        m = ((pc.field("bbox", "xmax") >= x0) & (pc.field("bbox", "xmin") <= x1)
             & (pc.field("bbox", "ymax") >= y0) & (pc.field("bbox", "ymin") <= y1))
        tabs.append(t.filter(m))
    out = pa.concat_tables(tabs) if tabs else None
    if out is None:                        # files matched but no row group did
        pf = pq.ParquetFile(_RangeFile(OVT + cands[0][0], size=cands[0][1]["size"]))
        out = pf.read_row_groups([0], columns=cols).slice(0, 0)
    with _CLOCK:
        _RDC[ck] = out
        while len(_RDC) > 20:
            _RDC.popitem(last=False)
    return out


def _fp(tb):
    """Content fingerprint of a read: (row count, summed x-centroid).

    Equality of counts alone is a weak control test -- a remote AOI holding 1
    feature trivially 'equals' a 1-feature control box.  The fingerprint only
    collides when the endpoint actually returned the *same rows*, which is what
    'this source is not filtering on geometry' means.
    """
    if tb is None or tb.num_rows == 0:
        return (0, 0.0)
    x, _ = _xy(tb)
    return (int(tb.num_rows), round(float(np.nansum(x)), 2))


def _ctrl_fp(theme, tp):
    """Memoised mid-Pacific control fingerprint for one Overture theme/type."""
    return _ctrl(f"fp_{theme}_{tp}", lambda: _fp(_read(theme, tp, CONTROL_BBOX, ["bbox"])))


def _ctrl_rows(c):
    return c[0] if isinstance(c, tuple) else c


def _guard(tb, ctl, what="rows"):
    """Raise if this AOI's read is byte-for-byte the control's read."""
    if isinstance(ctl, tuple) and tb is not None and tb.num_rows and _fp(tb) == ctl:
        raise ValueError(f"filter_unverified: AOI {what} fingerprint {_fp(tb)} == control {ctl}")


def _ctrl(tag, fn):
    """Memoised mid-Pacific control measurement (one per process, per tag)."""
    with _XLOCK:
        if tag in _CTRL:
            return _CTRL[tag]
    try:                                   # computed OUTSIDE the lock: fn() does network
        v = fn()                           # IO and re-enters _read/_manifest
    except Exception as e:
        v = f"control-probe-failed:{type(e).__name__}"
    with _XLOCK:
        _CTRL.setdefault(tag, v)
        return _CTRL[tag]


# ------------------------------------------------------------------- geometry
def _aoi_geom(aoi):
    g = shape(aoi["geom"])
    shapely.prepare(g)
    return g


def _xy(tb):
    bb = tb["bbox"].combine_chunks()
    f = (lambda n: np.asarray(bb.field(n), dtype="float64"))
    return (f("xmin") + f("xmax")) / 2.0, (f("ymin") + f("ymax")) / 2.0


def _in_aoi(tb, g):
    """Mask of rows whose bbox centre is inside the AOI polygon (no WKB decode)."""
    if tb is None or tb.num_rows == 0:
        return np.zeros(0, dtype=bool)
    x, y = _xy(tb)
    return shapely.contains_xy(g, x, y)


def _wkb(tb):
    return shapely.from_wkb(np.asarray(tb["geometry"].combine_chunks(), dtype=object))


def _clip(tb, g):
    """Decode geometries and clip to the AOI polygon.  Returns (clipped, keep_mask)."""
    if tb is None or tb.num_rows == 0:
        return np.empty(0, dtype=object), np.zeros(0, dtype=bool)
    gs = _wkb(tb)
    keep = shapely.intersects(gs, g)
    if not keep.any():
        return np.empty(0, dtype=object), keep
    return shapely.intersection(gs[keep], g), keep


def _km(geoms, lat0):
    """Per-geometry length in km, local equirectangular scaling (<0.3% over an AOI).

    No PROJ dependency; multipart geometries are exploded first so no spurious
    part-to-part connecting segment is counted.
    """
    if len(geoms) == 0:
        return np.zeros(0)
    parts, src = shapely.get_parts(geoms, return_index=True)
    if len(parts) == 0:
        return np.zeros(len(geoms))
    co, gi = shapely.get_coordinates(parts, return_index=True)
    if len(co) < 2:
        return np.zeros(len(geoms))
    kx = DEG_LON_M * math.cos(math.radians(lat0))
    d = np.hypot(np.diff(co[:, 0]) * kx, np.diff(co[:, 1]) * DEG_LAT_M)
    keep = np.diff(gi) == 0
    pl = np.bincount(gi[:-1][keep], weights=d[keep], minlength=len(parts))
    return np.bincount(src, weights=pl, minlength=len(geoms)) / 1000.0


def _area_km2(geoms, lat0):
    if len(geoms) == 0:
        return np.zeros(0)
    kx = DEG_LON_M * math.cos(math.radians(lat0))
    return shapely.area(geoms) * kx * DEG_LAT_M / 1e6


def _pad(bbox, km):
    x0, y0, x1, y1 = bbox
    dy = km * 1000.0 / DEG_LAT_M
    dx = km * 1000.0 / (DEG_LON_M * max(math.cos(math.radians((y0 + y1) / 2)), 0.05))
    return [x0 - dx, y0 - dy, x1 + dx, y1 + dy]


def _lat(aoi):
    return aoi["centroid"][1]


def _r(v, n=3):
    return round(float(v), n)


def _col(tb, name):
    return np.asarray(tb[name].combine_chunks(), dtype=object)


# ------------------------------------------------------------------- adapters
_ROADCLS = ("motorway", "trunk", "primary", "secondary", "tertiary",
            "residential", "service", "track", "unclassified", "footway")


def _a_rail(aoi):
    """railways -- Overture transportation/segment: rail network length by class.

    Also emits road-network length by class from the same single read (the lane's
    'highway length by class' derivation).
    """
    tb = _read("transportation", "segment", aoi["bbox"], ["subtype", "class", "geometry"])
    if tb is None:
        raise LookupError("no Overture transportation coverage for bbox")
    ctl = _ctrl_fp("transportation", "segment")
    if tb.num_rows == 0:
        raise LookupError(f"no transportation segments in AOI bbox (control={ctl})")
    _guard(tb, ctl, "segment")
    g, lat = _aoi_geom(aoi), _lat(aoi)
    sub, cls = _col(tb, "subtype"), _col(tb, "class")
    geoms, keep = _clip(tb, g)
    km = _km(geoms, lat)
    sub, cls = sub[keep], cls[keep]
    a = max(aoi["area_km2"], 1e-6)
    rail, road = sub == "rail", sub == "road"
    f = {"ovt_rail_km": _r(km[rail].sum(), 3),
         "ovt_rail_km_per_km2": _r(km[rail].sum() / a, 4),
         "ovt_rail_n_segments": int(rail.sum()),
         "ovt_rail_present": bool(km[rail].sum() > 0),
         "ovt_road_km": _r(km[road].sum(), 3),
         "ovt_road_km_per_km2": _r(km[road].sum() / a, 4),
         "ovt_road_n_segments": int(road.sum()),
         "ovt_segment_n_bbox": int(tb.num_rows),
         "ovt_control_segment_rows": _ctrl_rows(ctl)}
    for c in ("standard_gauge", "light_rail", "subway", "tram", "monorail", "narrow_gauge"):
        v = km[rail & (cls == c)].sum()
        if v > 0:
            f[f"ovt_rail_km_{c}"] = _r(v, 3)
    for c in _ROADCLS:
        v = km[road & (cls == c)].sum()
        if v > 0:
            f[f"ovt_road_km_{c}"] = _r(v, 3)
    if road.any():
        f["ovt_road_dominant_class"] = str(collections.Counter(cls[road].tolist()).most_common(1)[0][0])
    return f, {"obs_date": _release()[:10], "res_m": "vector",
               "item": f"overture/{_release()}/transportation.segment"}


def _infra(aoi, subtypes, want_geom=True):
    """Shared base/infrastructure read + control differential for the power/utility/comms slots."""
    cols = ["subtype", "class"] + (["geometry"] if want_geom else [])
    tb = _read("base", "infrastructure", aoi["bbox"], cols)
    if tb is None:
        raise LookupError("no Overture base/infrastructure coverage for bbox")
    ctl = _ctrl_fp("base", "infrastructure")
    _guard(tb, ctl, "infrastructure")
    sub = _col(tb, "subtype") if tb.num_rows else np.empty(0, object)
    sel = np.isin(sub, list(subtypes)) if tb.num_rows else np.zeros(0, bool)
    return tb, sel, ctl


def _a_pipelines(aoi):
    """pipelines -- Overture base/infrastructure subtype=utility."""
    tb, sel, ctl = _infra(aoi, ("utility",))
    g, lat = _aoi_geom(aoi), _lat(aoi)
    a = max(aoi["area_km2"], 1e-6)
    f = {"ovt_pipeline_km": 0.0, "ovt_pipeline_km_per_km2": 0.0, "ovt_pipeline_n": 0,
         "ovt_pipeline_present": False, "ovt_storage_tank_n": 0, "ovt_utility_n_aoi": 0,
         "ovt_infra_n_bbox": int(tb.num_rows), "ovt_control_infra_rows": _ctrl_rows(ctl)}
    if sel.any():
        s2 = tb.filter(pa.array(sel))
        cls = _col(s2, "class")
        geoms, keep = _clip(s2, g)
        cls = cls[keep]
        km = _km(geoms, lat)
        pipe = cls == "pipeline"
        f.update({"ovt_pipeline_km": _r(km[pipe].sum(), 3),
                  "ovt_pipeline_km_per_km2": _r(km[pipe].sum() / a, 4),
                  "ovt_pipeline_n": int(pipe.sum()),
                  "ovt_pipeline_present": bool(km[pipe].sum() > 0),
                  "ovt_storage_tank_n": int((cls == "storage_tank").sum()),
                  "ovt_utility_n_aoi": int(len(cls))})
        if len(cls):
            f["ovt_utility_dominant_class"] = str(collections.Counter(cls.tolist()).most_common(1)[0][0])
    return f, {"obs_date": _release()[:10], "res_m": "vector",
               "item": f"overture/{_release()}/base.infrastructure[utility]"}


_PWR_PT = ("power_tower", "power_pole", "substation", "transformer", "portal")


def _a_transmission(aoi):
    """transmission_lines -- Overture base/infrastructure subtype=power (lines + supports)."""
    tb, sel, ctl = _infra(aoi, ("power",))
    g, lat = _aoi_geom(aoi), _lat(aoi)
    a = max(aoi["area_km2"], 1e-6)
    f = {"ovt_powerline_km": 0.0, "ovt_powerline_km_per_km2": 0.0, "ovt_powerline_n": 0,
         "ovt_minor_line_km": 0.0, "ovt_power_n_aoi": 0, "ovt_powerline_present": False,
         "ovt_power_tower_per_km2": 0.0, "ovt_control_infra_rows": _ctrl_rows(ctl)}
    for c in _PWR_PT:
        f[f"ovt_power_{c}_n"] = 0
    if sel.any():
        s2 = tb.filter(pa.array(sel))
        cls = _col(s2, "class")
        geoms, keep = _clip(s2, g)
        cls = cls[keep]
        km = _km(geoms, lat)
        pl = cls == "power_line"
        f.update({"ovt_powerline_km": _r(km[pl].sum(), 3),
                  "ovt_powerline_km_per_km2": _r(km[pl].sum() / a, 4),
                  "ovt_powerline_n": int(pl.sum()),
                  "ovt_minor_line_km": _r(km[cls == "minor_line"].sum(), 3),
                  "ovt_power_n_aoi": int(len(cls)),
                  "ovt_powerline_present": bool(km[pl].sum() > 0)})
        for c in _PWR_PT:
            f[f"ovt_power_{c}_n"] = int((cls == c).sum())
        f["ovt_power_tower_per_km2"] = _r(f["ovt_power_power_tower_n"] / a, 3)
    return f, {"obs_date": _release()[:10], "res_m": "vector",
               "item": f"overture/{_release()}/base.infrastructure[power]"}


def _a_power_plants_ovt(aoi):
    """power_plants -- Overture base/infrastructure power generation assets."""
    cols = ["subtype", "class", "geometry", "names"]
    tb = _read("base", "infrastructure", aoi["bbox"], cols)
    if tb is None:
        raise LookupError("no Overture base/infrastructure coverage for bbox")
    ctl = _ctrl_fp("base", "infrastructure")
    _guard(tb, ctl, "infrastructure")
    g, lat = _aoi_geom(aoi), _lat(aoi)
    f = {"ovt_power_plant_n": 0, "ovt_power_plant_area_km2": 0.0, "ovt_generator_n": 0,
         "ovt_substation_n_aoi": 0, "ovt_power_plant_present": False,
         "ovt_power_plant_named": "", "ovt_control_infra_rows": _ctrl_rows(ctl)}
    if tb.num_rows:
        cls = _col(tb, "class")
        pm = (_col(tb, "subtype") == "power") & np.isin(cls, ["plant", "generator", "substation"])
        if pm.any():
            s2 = tb.filter(pa.array(pm))
            cls2 = _col(s2, "class")
            geoms, keep = _clip(s2, g)
            cls2 = cls2[keep]
            ar = _area_km2(geoms, lat)
            f.update({"ovt_power_plant_n": int((cls2 == "plant").sum()),
                      "ovt_power_plant_area_km2": _r(ar[cls2 == "plant"].sum(), 5),
                      "ovt_generator_n": int((cls2 == "generator").sum()),
                      "ovt_substation_n_aoi": int((cls2 == "substation").sum())})
            f["ovt_power_plant_present"] = bool(f["ovt_power_plant_n"] + f["ovt_generator_n"] > 0)
            nm = [(d.get("primary") if isinstance(d, dict) else None)
                  for d in s2.filter(pa.array(keep))["names"].to_pylist()]
            f["ovt_power_plant_named"] = str(next((n for n in nm if n), ""))[:60]
    return f, {"obs_date": _release()[:10], "res_m": "vector",
               "item": f"overture/{_release()}/base.infrastructure[power/plant]"}


def _gppd():
    """WRI Global Power Plant Database -- cached once (12 MB CSV)."""
    if "df" not in _GPPD:
        with _GLOCK:
            if "df" not in _GPPD:
                fp = os.path.join(CACHE, "gppd_power_plants.csv")
                if not os.path.exists(fp):
                    r = S.get("https://raw.githubusercontent.com/wri/global-power-plant-database/"
                              "master/output_database/global_power_plant_database.csv", timeout=180)
                    r.raise_for_status()
                    open(fp, "wb").write(r.content)
                d = pd.read_csv(fp, low_memory=False,
                                usecols=["country", "name", "capacity_mw", "latitude",
                                         "longitude", "primary_fuel", "commissioning_year"])
                _GPPD["df"] = d.dropna(subset=["latitude", "longitude"]).reset_index(drop=True)
    return _GPPD["df"]


def _a_gppd(aoi):
    """power_plants -- WRI GPPD: generating capacity within 50 km of the AOI centroid."""
    d = _gppd()
    lon, lat = d["longitude"].to_numpy(), d["latitude"].to_numpy()
    cx, cy = aoi["centroid"]
    kx = DEG_LON_M * math.cos(math.radians(cy)) / 1000.0
    dist = np.hypot((lon - cx) * kx, (lat - cy) * (DEG_LAT_M / 1000.0))
    near = dist <= 50.0
    x0, y0, x1, y1 = aoi["bbox"]
    inb = (lon >= x0) & (lon <= x1) & (lat >= y0) & (lat <= y1)
    def _cf():
        cd = np.hypot((lon + 139.5) * DEG_LON_M * math.cos(math.radians(-19.5)) / 1000.0,
                      (lat + 19.5) * DEG_LAT_M / 1000.0)
        return (int((cd <= 50.0).sum()), str(d.at[int(np.argmin(cd)), "name"])[:60])
    ctl = _ctrl("gppd_control", _cf)
    f = {"gppd_plants_50km": int(near.sum()),
         "gppd_capacity_mw_50km": _r(d.loc[near, "capacity_mw"].sum(), 1),
         "gppd_plants_in_bbox": int(inb.sum()),
         "gppd_nearest_km": _r(dist.min(), 2),
         "gppd_control_plants_50km": _ctrl_rows(ctl)}
    if near.any():
        s = d.loc[near]
        f["gppd_dominant_fuel_50km"] = str(s["primary_fuel"].mode().iloc[0])
        f["gppd_max_capacity_mw_50km"] = _r(s["capacity_mw"].max(), 1)
        f["gppd_n_fuel_types_50km"] = int(s["primary_fuel"].nunique())
        cy_ = s["commissioning_year"].dropna()
        if len(cy_):
            f["gppd_median_commission_year_50km"] = _r(cy_.median(), 0)
    j = int(np.argmin(dist))
    f["gppd_nearest_name"] = str(d.at[j, "name"])[:60]
    f["gppd_nearest_fuel"] = str(d.at[j, "primary_fuel"])
    f["gppd_nearest_capacity_mw"] = _r(d.at[j, "capacity_mw"], 1)
    if isinstance(ctl, tuple) and (f["gppd_plants_50km"], f["gppd_nearest_name"]) == ctl:
        raise ValueError(f"filter_unverified: AOI (n={f['gppd_plants_50km']}, "
                         f"nearest={f['gppd_nearest_name']}) == control {ctl}")
    return f, {"obs_date": "2021 (GPPD v1.3.0)", "res_m": "point",
               "item": "wri/global-power-plant-database@master"}


_IND = ("industrial", "quarry", "brownfield", "landfill", "military", "railway",
        "commercial", "retail", "construction", "port", "harbour", "depot")


def _a_landuse(aoi):
    """industrial_facilities -- Overture base/land_use industrial-class polygons."""
    tb = _read("base", "land_use", aoi["bbox"], ["subtype", "class", "geometry"])
    if tb is None:
        raise LookupError("no Overture base/land_use coverage for bbox")
    ctl = _ctrl_fp("base", "land_use")
    _guard(tb, ctl, "land_use")
    g, lat = _aoi_geom(aoi), _lat(aoi)
    a = max(aoi["area_km2"], 1e-6)
    f = {"ovt_landuse_n_bbox": int(tb.num_rows), "ovt_industrial_area_km2": 0.0,
         "ovt_industrial_frac": 0.0, "ovt_industrial_n": 0, "ovt_extractive_area_km2": 0.0,
         "ovt_landuse_n_classes": 0, "ovt_control_landuse_rows": _ctrl_rows(ctl)}
    if tb.num_rows:
        cls = _col(tb, "class")
        geoms, keep = _clip(tb, g)
        cls = cls[keep]
        ar = _area_km2(geoms, lat)
        ind = np.isin(cls, list(_IND))
        f.update({"ovt_industrial_area_km2": _r(ar[ind].sum(), 5),
                  "ovt_industrial_frac": _r(min(ar[ind].sum() / a, 1.0), 4),
                  "ovt_industrial_n": int(ind.sum()),
                  "ovt_extractive_area_km2": _r(ar[np.isin(cls, ["quarry", "landfill"])].sum(), 5),
                  "ovt_landuse_n_classes": int(len(set(cls.tolist())))})
        for c in ("industrial", "quarry", "brownfield", "landfill", "commercial",
                  "retail", "military"):
            v = ar[cls == c].sum()
            if v > 0:
                f[f"ovt_landuse_km2_{c}"] = _r(v, 5)
        if len(cls):
            f["ovt_landuse_dominant_class"] = str(collections.Counter(cls.tolist()).most_common(1)[0][0])
    return f, {"obs_date": _release()[:10], "res_m": "vector",
               "item": f"overture/{_release()}/base.land_use"}


_ECHO = {"QueryRows": "echo_facilities_bbox", "CAARows": "echo_clean_air_act_n",
         "CWARows": "echo_clean_water_act_n", "RCRRows": "echo_rcra_hazwaste_n",
         "TRIRows": "echo_tri_releasers_n", "INSPRows": "echo_inspections_n",
         "FEARows": "echo_formal_enforcement_n", "CVRows": "echo_current_violations_n",
         "SVRows": "echo_significant_violations_n"}


def _echo_rows(bbox):
    x0, y0, x1, y1 = bbox
    r = S.get("https://echodata.epa.gov/echo/echo_rest_services.get_facilities",
              params={"output": "JSON", "p_c1lon": x0, "p_c1lat": y1,
                      "p_c2lon": x1, "p_c2lat": y0, "responseset": "1"},
              headers=UA, timeout=90)
    if r.status_code != 200:
        raise ConnectionError(f"HTTP {r.status_code}")
    j = r.json().get("Results", {})
    if j.get("Error"):
        raise LookupError(str(j["Error"])[:140])
    return j


def _a_echo(aoi):
    """industrial_facilities -- US EPA ECHO regulated-facility census in the AOI bbox."""
    if aoi["country"] != "US":
        raise PermissionError("out of coverage (US-only: EPA ECHO)")
    j = _echo_rows(aoi["bbox"])
    def _cf():
        c = _echo_rows(CONTROL_BBOX)
        return (int(c.get("QueryRows", -1)), str(c.get("TotalPenalties", "")))
    ctl = _ctrl("echo_control", _cf)
    f = {}
    for k, name in _ECHO.items():
        v = j.get(k)
        if v is None:
            continue
        try:
            f[name] = int(str(v).replace(",", ""))
        except ValueError:
            pass
    if "echo_facilities_bbox" not in f:
        raise LookupError("ECHO returned no QueryRows")
    f["echo_facilities_per_km2"] = _r(f["echo_facilities_bbox"] / max(aoi["area_km2"], 1e-6), 3)
    pen = str(j.get("TotalPenalties") or "").replace("$", "").replace(",", "")
    if pen.replace(".", "").isdigit():
        f["echo_total_penalties_usd"] = _r(float(pen), 0)
    f["echo_control_facilities"] = _ctrl_rows(ctl)
    if isinstance(ctl, tuple) and ctl[0] > 0 and \
            (f["echo_facilities_bbox"], str(j.get("TotalPenalties", ""))) == ctl:
        raise ValueError(f"filter_unverified: AOI (n={f['echo_facilities_bbox']}) == control {ctl}")
    return f, {"obs_date": "current (ECHO live)", "res_m": "point",
               "item": "epa_echo_all_programs"}


def _a_addresses(aoi):
    """addresses -- Overture addresses/address point census (addressed-node count)."""
    big = aoi["area_km2"] > 50.0                      # cost guard: skip string decode
    cols = ["bbox"] if big else ["street", "postcode"]
    tb = _read("addresses", "address", aoi["bbox"], cols)
    if tb is None:
        raise LookupError("no Overture addresses coverage for bbox")
    ctl = _ctrl_fp("addresses", "address")
    _guard(tb, ctl, "address")
    g = _aoi_geom(aoi)
    m = _in_aoi(tb, g)
    n = int(m.sum())
    f = {"ovt_address_n": n,
         "ovt_address_per_km2": _r(n / max(aoi["area_km2"], 1e-6), 2),
         "ovt_address_n_bbox": int(tb.num_rows),
         "ovt_address_present": bool(n > 0),
         "ovt_control_address_rows": _ctrl_rows(ctl)}
    if not big and n:
        s2 = tb.filter(pa.array(m))
        st = [s for s in s2["street"].to_pylist() if s]
        pcs = [s for s in s2["postcode"].to_pylist() if s]
        if st:
            f["ovt_address_n_streets"] = int(len(set(st)))
            f["ovt_address_street_density"] = _r(len(set(st)) / max(aoi["area_km2"], 1e-6), 2)
        if pcs:
            f["ovt_address_dominant_postcode"] = str(collections.Counter(pcs).most_common(1)[0][0])[:12]
            f["ovt_address_n_postcodes"] = int(len(set(pcs)))
    if n == 0:
        # regional context: separates a true local absence from a source coverage gap
        reg = _read("addresses", "address", _pad(aoi["bbox"], 25.0), ["bbox"])
        f["ovt_address_n_within_25km"] = int(0 if reg is None else reg.num_rows)
        f["ovt_address_regional_coverage"] = bool(f["ovt_address_n_within_25km"] > 0)
    return f, {"obs_date": _release()[:10], "res_m": "point",
               "item": f"overture/{_release()}/addresses.address"}


def _a_buildings(aoi):
    """addresses -- Overture buildings/building: built-form count, density, height.

    Registered on the `addresses` slot: both are parcel-level built-form measures
    from the same provider, and the lane's slot vocabulary has no building slot.
    """
    tb = _read("buildings", "building", aoi["bbox"], ["height", "class", "geometry"])
    if tb is None:
        raise LookupError("no Overture buildings coverage for bbox")
    ctl = _ctrl_fp("buildings", "building")
    _guard(tb, ctl, "building")
    g, lat = _aoi_geom(aoi), _lat(aoi)
    m = _in_aoi(tb, g)
    n = int(m.sum())
    a = max(aoi["area_km2"], 1e-6)
    f = {"ovt_building_n": n, "ovt_building_per_km2": _r(n / a, 2),
         "ovt_building_n_bbox": int(tb.num_rows), "ovt_control_building_rows": _ctrl_rows(ctl)}
    if n:
        s2 = tb.filter(pa.array(m))
        h = np.asarray(s2["height"].combine_chunks().to_numpy(zero_copy_only=False), dtype="float64")
        hv = h[np.isfinite(h)]
        if hv.size:
            f["ovt_building_height_m_mean"] = _r(hv.mean(), 2)
            f["ovt_building_height_m_p90"] = _r(np.percentile(hv, 90), 2)
            f["ovt_building_height_m_max"] = _r(hv.max(), 2)
            f["ovt_building_n_with_height"] = int(hv.size)
            f["ovt_building_n_over_20m"] = int((hv > 20).sum())
        cls = [c for c in s2["class"].to_pylist() if c]
        if cls:
            f["ovt_building_dominant_class"] = str(collections.Counter(cls).most_common(1)[0][0])
            f["ovt_building_n_classes"] = int(len(set(cls)))
        if n <= 60000:                     # exact footprint area, bounded decode cost
            geoms, keep = _clip(s2, g)
            ar = _area_km2(geoms, lat)
            f["ovt_building_footprint_km2"] = _r(ar.sum(), 5)
            f["ovt_built_frac"] = _r(min(ar.sum() / a, 1.0), 5)
            f["ovt_building_mean_footprint_m2"] = _r(ar.sum() * 1e6 / n, 1)
        else:
            f["ovt_building_footprint_skipped_n"] = n
    return f, {"obs_date": _release()[:10], "res_m": "vector",
               "item": f"overture/{_release()}/buildings.building"}


def _a_cell_towers(aoi):
    """cell_towers -- Overture base/infrastructure subtype=communication.

    OpenCelliD is the preferred route but both of its open endpoints are
    token-gated (/cell/getInArea -> HTTP 401 {"error":"API Key not known"}; the
    bulk MCC download needs an account token), so per the task this falls back to
    OSM-derived communication masts carried in Overture.
    """
    tb, sel, ctl = _infra(aoi, ("communication",), want_geom=False)
    g = _aoi_geom(aoi)
    f = {"ovt_comm_tower_n": 0, "ovt_mobile_phone_tower_n": 0, "ovt_comm_n_aoi": 0,
         "ovt_comm_tower_present": False, "ovt_control_infra_rows": _ctrl_rows(ctl),
         "ovt_opencellid_status": "auth-blocked-401"}
    if sel.any():
        s2 = tb.filter(pa.array(sel))
        m = _in_aoi(s2, g)
        cls = _col(s2, "class")[m]
        f.update({"ovt_comm_tower_n": int((cls == "communication_tower").sum()),
                  "ovt_mobile_phone_tower_n": int((cls == "mobile_phone_tower").sum()),
                  "ovt_comm_n_aoi": int(len(cls))})
        f["ovt_comm_tower_present"] = bool(f["ovt_comm_n_aoi"] > 0)
    # 10 km context: masts are sparse, so an AOI-only count is often a true zero
    wide = _read("base", "infrastructure", _pad(aoi["bbox"], 10.0), ["subtype", "class"])
    if wide is not None and wide.num_rows:
        ws = _col(wide, "subtype")
        f["ovt_comm_n_within_10km"] = int((ws == "communication").sum())
    return f, {"obs_date": _release()[:10], "res_m": "point",
               "item": f"overture/{_release()}/base.infrastructure[communication]"}


def _a_ground_imagery(aoi):
    """ground_level_imagery -- probes the two open street-level imagery APIs.

    Both require a client token, so this raises with the measured auth response
    rather than embedding a credential.  Kept registered so the dossier records
    the slot as auth-blocked with evidence instead of silence.
    """
    x0, y0, x1, y1 = aoi["bbox"]
    msgs = []
    try:
        r = S.get("https://graph.mapillary.com/images",
                  params={"bbox": f"{x0},{y0},{x1},{y1}", "limit": 1}, headers=UA, timeout=40)
        j = r.json()
        if r.status_code == 200 and "data" in j:
            return ({"mly_imagery_available": bool(j["data"]),
                     "mly_images_probe_n": len(j["data"])},
                    {"obs_date": "live", "res_m": "point", "item": "mapillary_graph_v4"})
        e = j.get("error", {})
        msgs.append(f"mapillary HTTP {r.status_code} code={e.get('code')} "
                    f"{str(e.get('message'))[:50]}")
    except Exception as e:
        msgs.append(f"mapillary {type(e).__name__}: {str(e)[:50]}")
    try:
        r = S.get("https://api.kartaview.org/2.0/photo/",
                  params={"tlLat": y1, "tlLng": x0, "brLat": y0, "brLng": x1},
                  headers=UA, timeout=40)
        st = r.json().get("status", {})
        if st.get("apiCode") in (200, 600):
            d = r.json().get("result", {}).get("data", [])
            return ({"kartaview_imagery_available": bool(d),
                     "kartaview_images_probe_n": len(d)},
                    {"obs_date": "live", "res_m": "point", "item": "kartaview_v2"})
        msgs.append(f"kartaview apiCode={st.get('apiCode')} {str(st.get('apiMessage'))[:30]}")
    except Exception as e:
        msgs.append(f"kartaview {type(e).__name__}: {str(e)[:50]}")
    raise ConnectionError("street-level imagery needs an account token and none is configured; " + "; ".join(msgs))


_WD_TOTAL = """SELECT (COUNT(DISTINCT ?i) AS ?n) (COUNT(DISTINCT ?t) AS ?nt) WHERE {
  SERVICE wikibase:box { ?i wdt:P625 ?c .
    bd:serviceParam wikibase:cornerWest "Point(%.6f %.6f)"^^geo:wktLiteral .
    bd:serviceParam wikibase:cornerEast "Point(%.6f %.6f)"^^geo:wktLiteral }
  OPTIONAL { ?i wdt:P31 ?t } }"""
_WD_TOP = """SELECT ?t ?tLabel (COUNT(DISTINCT ?i) AS ?n) WHERE {
  SERVICE wikibase:box { ?i wdt:P625 ?c .
    bd:serviceParam wikibase:cornerWest "Point(%.6f %.6f)"^^geo:wktLiteral .
    bd:serviceParam wikibase:cornerEast "Point(%.6f %.6f)"^^geo:wktLiteral }
  ?i wdt:P31 ?t .
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en". } }
GROUP BY ?t ?tLabel ORDER BY DESC(?n) LIMIT 5"""


def _sparql(q, bbox):
    x0, y0, x1, y1 = bbox
    r = S.get("https://query.wikidata.org/sparql",
              params={"query": q % (x0, y0, x1, y1), "format": "json"},
              headers=dict(UA, Accept="application/sparql-results+json"), timeout=90)
    if r.status_code != 200:
        raise ConnectionError(f"WDQS HTTP {r.status_code}: {r.text[:90]}")
    return r.json()["results"]["bindings"]


def _a_wikidata(aoi):
    """gazetteer -- Wikidata entity census in the AOI bbox (bbox service around P625)."""
    b = _sparql(_WD_TOTAL, aoi["bbox"])
    if not b:
        raise LookupError("WDQS returned no rows")
    n = int(b[0]["n"]["value"])
    nt = int(b[0].get("nt", {}).get("value", 0))
    def _cf():
        cb = _sparql(_WD_TOTAL, CONTROL_BBOX)
        cn = int(cb[0]["n"]["value"])
        ct = _sparql(_WD_TOP, CONTROL_BBOX) if cn else []
        return (cn, str(ct[0]["t"]["value"].rsplit("/", 1)[-1]) if ct else "")
    ctl = _ctrl("wd_control", _cf)
    f = {"wd_entities_bbox": n, "wd_distinct_classes": nt,
         "wd_entities_per_km2": _r(n / max(aoi["area_km2"], 1e-6), 3),
         "wd_control_entities": _ctrl_rows(ctl)}
    qid = ""
    if n:
        top = _sparql(_WD_TOP, aoi["bbox"])
        if top:
            qid = str(top[0]["t"]["value"].rsplit("/", 1)[-1])
            f["wd_dominant_class_qid"] = qid
            f["wd_dominant_class_label"] = str(top[0].get("tLabel", {}).get("value", ""))[:50]
            f["wd_dominant_class_n"] = int(top[0]["n"]["value"])
            f["wd_top5_class_share"] = _r(min(sum(int(x["n"]["value"]) for x in top) / max(n, 1), 1.0), 4)
    # count equality alone is weak for tiny counts, so the dominant class must match too
    if isinstance(ctl, tuple) and n > 0 and (n, qid) == ctl:
        raise ValueError(f"filter_unverified: AOI (n={n}, class={qid}) == control {ctl}")
    return f, {"obs_date": "live", "res_m": "point", "item": "wikidata_query_service"}


_PLACE_SUB = ["locality", "county", "region", "country", "macrohood", "neighborhood",
              "localadmin", "borough"]


def _a_divisions(aoi):
    """gazetteer -- Overture divisions/division: named places and admin context."""
    cols = ["names", "subtype", "population", "country", "region"]
    for rad in (15.0, 60.0):               # remote AOIs have no named place nearby
        tb = _read("divisions", "division", _pad(aoi["bbox"], rad), cols)
        if tb is None:
            raise LookupError("no Overture divisions coverage for bbox")
        if tb.num_rows:
            break
    ctl = _ctrl_fp("divisions", "division")
    if tb.num_rows == 0:
        raise LookupError(f"no Overture named place within {rad:.0f} km of the AOI")
    _guard(tb, ctl, "division")
    g = _aoi_geom(aoi)
    x, y = _xy(tb)
    cx, cy = aoi["centroid"]
    kx = DEG_LON_M * math.cos(math.radians(cy)) / 1000.0
    dist = np.hypot((x - cx) * kx, (y - cy) * DEG_LAT_M / 1000.0)
    inside = shapely.contains_xy(g, x, y)
    sub = _col(tb, "subtype")
    nm = [(d.get("primary") if isinstance(d, dict) else None) for d in tb["names"].to_pylist()]
    pop = np.asarray(tb["population"].combine_chunks().to_numpy(zero_copy_only=False), dtype="float64")
    f = {"ovt_division_n_in_aoi": int(inside.sum()),
         "ovt_division_n_in_radius": int(tb.num_rows),
         "ovt_division_search_radius_km": _r(rad, 0),
         "ovt_division_n_subtypes": int(len(set(sub.tolist()))),
         "ovt_control_division_rows": _ctrl_rows(ctl)}
    loc = np.isin(sub, _PLACE_SUB)
    cand = np.where(loc)[0] if loc.any() else np.arange(tb.num_rows)
    j = int(cand[np.argmin(dist[cand])])
    f["ovt_nearest_place"] = str(nm[j] or "")[:60]
    f["ovt_nearest_place_subtype"] = str(sub[j])
    f["ovt_nearest_place_km"] = _r(dist[j], 3)
    reg = [v for v in tb["region"].to_pylist() if v]
    cty = [v for v in tb["country"].to_pylist() if v]
    if reg:
        f["ovt_region_code"] = str(collections.Counter(reg).most_common(1)[0][0])[:8]
    if cty:
        f["ovt_country_code"] = str(collections.Counter(cty).most_common(1)[0][0])[:4]
    pv = pop[np.isfinite(pop) & (pop > 0)]
    if pv.size:
        f["ovt_max_place_population_radius"] = int(pv.max())
        f["ovt_n_places_with_population"] = int(pv.size)
    if inside.any():
        inm = [nm[i] for i in np.where(inside)[0] if nm[i]]
        if inm:
            f["ovt_place_names_in_aoi"] = "; ".join(inm[:4])[:120]
    return f, {"obs_date": _release()[:10], "res_m": "admin",
               "item": f"overture/{_release()}/divisions.division"}


# ------------------------------------------------------------------ registry
_ODBL = "ODbL-1.0 (share-alike; OSM via Overture)"
_MIX = "ODbL-1.0 + CDLA-Permissive-2.0 (mixed: OSM / Google-Microsoft open buildings)"

# slot, source_id, family, licence, auth, fn
ADAPTERS = [
 ("railways",              "overture_transportation", "infrastructure", _ODBL, "none", _a_rail),
 ("pipelines",             "overture_utility",        "infrastructure", _ODBL, "none", _a_pipelines),
 ("transmission_lines",    "overture_power",          "infrastructure", _ODBL, "none", _a_transmission),
 ("power_plants",          "overture_power_plant",    "infrastructure", _ODBL, "none", _a_power_plants_ovt),
 ("power_plants",          "wri_gppd",                "infrastructure",
  "CC-BY-4.0 (WRI GPPD v1.3.0)", "none", _a_gppd),
 ("industrial_facilities", "overture_land_use",       "infrastructure", _ODBL, "none", _a_landuse),
 ("industrial_facilities", "epa_echo",                "infrastructure",
  "public-domain (US EPA)", "none", _a_echo),
 ("addresses",             "overture_addresses",      "infrastructure",
  "CDLA-Permissive-2.0 (Overture; per-record OpenAddresses source terms)", "none", _a_addresses),
 ("addresses",             "overture_buildings",      "infrastructure", _MIX, "none", _a_buildings),
 ("cell_towers",           "overture_communication",  "infrastructure", _ODBL, "none", _a_cell_towers),
 ("ground_level_imagery",  "mapillary_kartaview",     "infrastructure",
  "CC-BY-SA-4.0 (Mapillary) / CC-BY-SA (KartaView)", "token-required", _a_ground_imagery),
 ("gazetteer",             "wikidata_sparql",         "infrastructure", "CC0-1.0", "none", _a_wikidata),
 ("gazetteer",             "overture_divisions",      "infrastructure", _MIX, "none", _a_divisions),
]

_THEMES = (("transportation", "segment"), ("base", "infrastructure"), ("base", "land_use"),
           ("addresses", "address"), ("buildings", "building"), ("divisions", "division"))


def warm_cache(themes=_THEMES):
    """Build every Overture manifest and fetch the GPPD CSV.  ~40 s, ~12 MB on disk.

    Call once before a batch run so no individual adapter pays the build cost.
    """
    out = {f"{th}/{tp}": len(_manifest(th, tp)) for th, tp in themes}
    out["gppd_rows"] = len(_gppd())
    out["release"] = _release()
    out["cache_mb"] = round(sum(os.path.getsize(os.path.join(CACHE, f))
                                for f in os.listdir(CACHE)) / 1e6, 2)
    return out

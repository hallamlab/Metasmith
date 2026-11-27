"""Infrastructure and built-form adapters: Overture Maps GeoParquet (range reads, keyless), WRI GPPD, EPA ECHO, Wikidata, street-level imagery probes."""
import os, io, json, time, math, threading, collections

import numpy as np
import pandas as pd
import requests
import shapely
import shapely.affinity
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
_S3NS = "{http://s3.amazonaws.com/doc/2006-03-01/}"

# _MLOCK/_GLOCK may be held across network IO (one-time builds); _CLOCK only guards a dict access.
_MLOCK = threading.RLock()
_GLOCK = threading.RLock()
_CLOCK = threading.Lock()
_REL, _MAN, _GPPD = {}, {}, {}
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
        fp = os.path.join(CACHE, f"ovt_manifest_{_release()}_{theme}_{tp}.json")
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


def _buffer(g, km, lat0):
    """AOI polygon grown by km on the ground (local equirectangular)."""
    f = DEG_LON_M * math.cos(math.radians(lat0)) / DEG_LAT_M
    b = shapely.affinity.scale(shapely.affinity.scale(g, f, 1, origin=(0, 0)).buffer(km * 1000.0 / DEG_LAT_M),
                               1 / f, 1, origin=(0, 0))
    shapely.prepare(b)
    return b


def _bbox_km2(bbox):
    x0, y0, x1, y1 = bbox
    return (x1 - x0) * DEG_LON_M * math.cos(math.radians((y0 + y1) / 2)) * (y1 - y0) * DEG_LAT_M / 1e6


def _by_class(cls, w=None, nd=3):
    """One record per published class value; counts when w is None, else summed w."""
    s = collections.defaultdict(float)
    for c, x in zip(cls, np.ones(len(cls)) if w is None else w):
        if c:
            s[str(c)] += x
    return [{"class": c, "value": int(v) if w is None else _r(v, nd)} for c, v in sorted(s.items())]


def _ovt_meta(theme, tp, res, part=""):
    rel = _release()
    return {"obs_date": rel[:10], "res_m": res, "item": f"overture/{rel}/{theme}.{tp}{part}",
            "release": rel, "url": f"s3://overturemaps-us-west-2/release/{rel}/theme={theme}/type={tp}/"}


def _lat(aoi):
    return aoi["centroid"][1]


def _r(v, n=3):
    return round(float(v), n)


def _col(tb, name):
    return np.asarray(tb[name].combine_chunks(), dtype=object)




# ------------------------------------------------------------------- adapters
def _need(tb, what):
    if tb is None:
        raise LookupError(f"no Overture {what} coverage for bbox")
    return tb


def _a_rail(aoi):
    """railways -- Overture transportation/segment: rail and road network length, total and by class."""
    tb = _need(_read("transportation", "segment", aoi["bbox"], ["subtype", "class", "geometry"]), "transportation")
    if tb.num_rows == 0:
        raise LookupError("no transportation segments in AOI bbox")
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
         "ovt_rail_km_by_class": _by_class(cls[rail], km[rail]),
         "ovt_road_km": _r(km[road].sum(), 3),
         "ovt_road_km_per_km2": _r(km[road].sum() / a, 4),
         "ovt_road_n_segments": int(road.sum()),
         "ovt_road_km_by_class": _by_class(cls[road], km[road])}
    return f, _ovt_meta("transportation", "segment", "vector")


def _infra(aoi, subtype, geom=True, columns=()):
    """Overture base/infrastructure rows of one published subtype."""
    tb = _need(_read("base", "infrastructure", aoi["bbox"],
                     ["subtype", "class"] + (["geometry"] if geom else []) + list(columns)), "base/infrastructure")
    if tb.num_rows == 0:
        return tb
    return tb.filter(pa.array(_col(tb, "subtype") == subtype))


def _a_pipelines(aoi):
    """pipelines -- Overture base/infrastructure subtype=utility."""
    s2 = _infra(aoi, "utility")
    g, lat = _aoi_geom(aoi), _lat(aoi)
    geoms, keep = _clip(s2, g)
    cls = _col(s2, "class")[keep] if s2.num_rows else np.empty(0, object)
    km = _km(geoms, lat)
    pipe = cls == "pipeline"
    f = {"ovt_pipeline_km": _r(km[pipe].sum(), 3),
         "ovt_pipeline_km_per_km2": _r(km[pipe].sum() / max(aoi["area_km2"], 1e-6), 4),
         "ovt_utility_n_by_class": _by_class(cls)}
    return f, _ovt_meta("base", "infrastructure", "vector", "[utility]")


def _a_transmission(aoi):
    """transmission_lines -- Overture base/infrastructure subtype=power: lines and supports by class."""
    s2 = _infra(aoi, "power")
    g, lat = _aoi_geom(aoi), _lat(aoi)
    a = max(aoi["area_km2"], 1e-6)
    geoms, keep = _clip(s2, g)
    cls = _col(s2, "class")[keep] if s2.num_rows else np.empty(0, object)
    line = np.isin(shapely.get_type_id(geoms), [1, 2, 5])
    km = _km(geoms, lat) * line
    f = {"ovt_powerline_km_per_km2": _r(km[cls == "power_line"].sum() / a, 4),
         "ovt_power_tower_per_km2": _r((cls == "power_tower").sum() / a, 3),
         "ovt_power_n_by_class": _by_class(cls),
         "ovt_power_km_by_class": [r for r in _by_class(cls, km) if r["value"] > 0]}
    return f, _ovt_meta("base", "infrastructure", "vector", "[power]")


def _a_power_plants_ovt(aoi):
    """power_plants -- Overture base/infrastructure power plant and generator features."""
    tb = _infra(aoi, "power", columns=["names"])
    g, lat = _aoi_geom(aoi), _lat(aoi)
    f = {"ovt_power_plant_n": 0, "ovt_power_plant_area_km2": 0.0, "ovt_generator_n": 0}
    if tb.num_rows:
        pm = np.isin(_col(tb, "class"), ["plant", "generator"])
        if pm.any():
            s2 = tb.filter(pa.array(pm))
            geoms, keep = _clip(s2, g)
            cls = _col(s2, "class")[keep]
            plant = cls == "plant"
            nm = [(d.get("primary") if isinstance(d, dict) else None)
                  for d in s2.filter(pa.array(keep))["names"].to_pylist()]
            f.update({"ovt_power_plant_n": int(plant.sum()),
                      "ovt_power_plant_area_km2": _r(_area_km2(geoms, lat)[plant].sum(), 5),
                      "ovt_generator_n": int((cls == "generator").sum())})
            names = [n for n, p in zip(nm, plant) if p and n]
            if names:
                f["ovt_power_plant_names"] = "; ".join(names)
    return f, _ovt_meta("base", "infrastructure", "vector", "[power]")


GPPD_SHA = "7a91cfbb2a4e272597acbc00506d61fc1ec73b3d"
GPPD_URL = ("https://raw.githubusercontent.com/wri/global-power-plant-database/"
            f"{GPPD_SHA}/output_database/global_power_plant_database.csv")


def _gppd():
    """WRI Global Power Plant Database v1.3.0 (last commit of the unmaintained repository), cached once."""
    if "df" not in _GPPD:
        with _GLOCK:
            if "df" not in _GPPD:
                fp = os.path.join(CACHE, f"gppd_power_plants_{GPPD_SHA[:7]}.csv")
                if not os.path.exists(fp):
                    r = S.get(GPPD_URL, timeout=180)
                    r.raise_for_status()
                    open(fp, "wb").write(r.content)
                d = pd.read_csv(fp, low_memory=False,
                                usecols=["country", "name", "capacity_mw", "latitude",
                                         "longitude", "primary_fuel", "commissioning_year"])
                _GPPD["df"] = d.dropna(subset=["latitude", "longitude"]).reset_index(drop=True)
    return _GPPD["df"]


def _a_gppd(aoi):
    """power_plants -- WRI GPPD: plants and capacity within 50 km of the AOI centroid, by primary_fuel."""
    d = _gppd()
    lon, lat = d["longitude"].to_numpy(), d["latitude"].to_numpy()
    cx, cy = aoi["centroid"]
    kx = DEG_LON_M * math.cos(math.radians(cy)) / 1000.0
    dist = np.hypot((lon - cx) * kx, (lat - cy) * (DEG_LAT_M / 1000.0))
    near = dist <= 50.0
    inside = shapely.contains_xy(_aoi_geom(aoi), lon, lat)
    s = d.loc[near]
    f = {"gppd_plants_50km": int(near.sum()),
         "gppd_capacity_mw_50km": _r(s["capacity_mw"].sum(), 1),
         "gppd_plants_in_aoi": int(inside.sum()),
         "gppd_nearest_km": _r(dist.min(), 2),
         "gppd_plants_50km_by_fuel": _by_class(s["primary_fuel"].tolist()),
         "gppd_capacity_mw_50km_by_fuel": _by_class(s["primary_fuel"].tolist(), s["capacity_mw"].fillna(0).to_numpy(), 1)}
    if near.any():
        f["gppd_max_capacity_mw_50km"] = _r(s["capacity_mw"].max(), 1)
        cy_ = s["commissioning_year"].dropna()
        if len(cy_):
            f["gppd_median_commission_year_50km"] = int(round(cy_.median()))
    j = int(np.argmin(dist))
    f["gppd_nearest_name"] = str(d.at[j, "name"])
    f["gppd_nearest_fuel"] = str(d.at[j, "primary_fuel"])
    f["gppd_nearest_capacity_mw"] = _r(d.at[j, "capacity_mw"], 1)
    return f, {"obs_date": "2022-01-26", "res_m": "point", "item": f"wri/global-power-plant-database@{GPPD_SHA[:7]}",
               "release": f"v1.3.0 ({GPPD_SHA[:7]})", "url": GPPD_URL}


def _a_landuse(aoi):
    """industrial_facilities -- Overture base/land_use: area by published class, and resource_extraction subtype area."""
    tb = _need(_read("base", "land_use", aoi["bbox"], ["subtype", "class", "geometry"]), "base/land_use")
    g, lat = _aoi_geom(aoi), _lat(aoi)
    f = {"ovt_extractive_area_km2": 0.0, "ovt_landuse_km2_by_class": []}
    if tb.num_rows:
        sub, cls = _col(tb, "subtype"), _col(tb, "class")
        geoms, keep = _clip(tb, g)
        sub, cls = sub[keep], cls[keep]
        ar = _area_km2(geoms, lat)
        f["ovt_extractive_area_km2"] = _r(ar[sub == "resource_extraction"].sum(), 5)
        f["ovt_landuse_km2_by_class"] = _by_class(cls, ar, 5)
    return f, _ovt_meta("base", "land_use", "vector")


_ECHO = {"QueryRows": "echo_facilities_bbox", "CAARows": "echo_caa_facilities_bbox",
         "CWARows": "echo_cwa_facilities_bbox", "RCRRows": "echo_rcra_facilities_bbox",
         "TRIRows": "echo_tri_facilities_bbox", "INSPRows": "echo_inspected_facilities_5yr_bbox",
         "FEARows": "echo_formal_action_facilities_5yr_bbox", "CVRows": "echo_violation_facilities_bbox",
         "SVRows": "echo_significant_violation_facilities_bbox"}
ECHO_URL = "https://echodata.epa.gov/echo/echo_rest_services.get_facilities"


def _echo_rows(bbox):
    x0, y0, x1, y1 = bbox
    r = S.get(ECHO_URL, params={"output": "JSON", "p_c1lon": x0, "p_c1lat": y1,
                                "p_c2lon": x1, "p_c2lat": y0, "responseset": "1"},
              headers=UA, timeout=90)
    if r.status_code != 200:
        raise ConnectionError(f"HTTP {r.status_code}")
    j = r.json().get("Results", {})
    if j.get("Error"):
        raise LookupError(str(j["Error"]))
    return j


def _a_echo(aoi):
    """industrial_facilities -- US EPA ECHO facility counts over the AOI bounding box (the service takes no polygon)."""
    if aoi["country"] != "US":
        raise PermissionError("out of coverage (US-only: EPA ECHO)")
    j = _echo_rows(aoi["bbox"])
    f = {}
    for k, name in _ECHO.items():
        try:
            f[name] = int(str(j[k]).replace(",", ""))
        except (KeyError, ValueError):
            pass
    if "echo_facilities_bbox" not in f:
        raise LookupError("ECHO returned no QueryRows")
    f["echo_facilities_per_bbox_km2"] = _r(f["echo_facilities_bbox"] / max(_bbox_km2(aoi["bbox"]), 1e-6), 3)
    pen = str(j.get("TotalPenalties") or "").replace("$", "").replace(",", "")
    if pen.replace(".", "").isdigit():
        f["echo_total_penalties_5yr_usd_bbox"] = _r(float(pen), 0)
    return f, {"obs_date": "current (ECHO live)", "res_m": "point", "item": "epa_echo_all_programs",
               "release": j.get("Version"), "url": ECHO_URL}


def _a_addresses(aoi):
    """addresses -- Overture addresses/address point census."""
    big = aoi["area_km2"] > 50.0                      # cost guard: skip string decode
    tb = _need(_read("addresses", "address", aoi["bbox"], ["bbox"] if big else ["street", "postcode"]), "addresses")
    g, lat = _aoi_geom(aoi), _lat(aoi)
    m = _in_aoi(tb, g)
    n = int(m.sum())
    a = max(aoi["area_km2"], 1e-6)
    f = {"ovt_address_n": n, "ovt_address_per_km2": _r(n / a, 2)}
    if not big and n:
        s2 = tb.filter(pa.array(m))
        st = {s for s in s2["street"].to_pylist() if s}
        pcs = {s for s in s2["postcode"].to_pylist() if s}
        if st:
            f["ovt_address_n_streets"] = len(st)
            f["ovt_address_street_density"] = _r(len(st) / a, 2)
        if pcs:
            f["ovt_address_n_postcodes"] = len(pcs)
    if n == 0:
        reg = _read("addresses", "address", _pad(aoi["bbox"], 25.0), ["bbox"])
        f["ovt_address_n_within_25km"] = int(_in_aoi(reg, _buffer(g, 25.0, lat)).sum()) if reg is not None else 0
    return f, _ovt_meta("addresses", "address", "point")


def _a_buildings(aoi):
    """addresses -- Overture buildings/building: count, density, height, footprint, count by published class."""
    tb = _need(_read("buildings", "building", aoi["bbox"], ["height", "class", "geometry"]), "buildings")
    g, lat = _aoi_geom(aoi), _lat(aoi)
    m = _in_aoi(tb, g)
    n = int(m.sum())
    a = max(aoi["area_km2"], 1e-6)
    f = {"ovt_building_n": n, "ovt_building_per_km2": _r(n / a, 2)}
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
        f["ovt_building_n_by_class"] = _by_class(s2["class"].to_pylist())
        if n <= 60000:                     # exact footprint area, bounded decode cost
            geoms, _ = _clip(s2, g)
            ar = _area_km2(geoms, lat)
            f["ovt_building_footprint_km2"] = _r(ar.sum(), 5)
            f["ovt_built_frac"] = _r(min(ar.sum() / a, 1.0), 5)
            f["ovt_building_mean_footprint_m2"] = _r(ar.sum() * 1e6 / len(ar), 1)
    return f, _ovt_meta("buildings", "building", "vector")


def _a_cell_towers(aoi):
    """cell_towers -- Overture base/infrastructure subtype=communication (OpenCelliD needs a token)."""
    s2 = _infra(aoi, "communication", geom=False)
    g, lat = _aoi_geom(aoi), _lat(aoi)
    cls = _col(s2, "class")[_in_aoi(s2, g)] if s2.num_rows else np.empty(0, object)
    f = {"ovt_comm_n_by_class": _by_class(cls)}
    wide = _need(_read("base", "infrastructure", _pad(aoi["bbox"], 10.0), ["subtype", "class"]), "base/infrastructure")
    if wide.num_rows:
        near = _in_aoi(wide, _buffer(g, 10.0, lat)) & (_col(wide, "subtype") == "communication")
        f["ovt_comm_n_within_10km"] = int(near.sum())
    return f, _ovt_meta("base", "infrastructure", "point", "[communication]")


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
                    {"obs_date": "live", "res_m": "point", "item": "mapillary_graph_v4",
                     "release": "graph v4", "url": "https://graph.mapillary.com/images"})
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
                    {"obs_date": "live", "res_m": "point", "item": "kartaview_v2",
                     "release": "2.0", "url": "https://api.kartaview.org/2.0/photo/"})
        msgs.append(f"kartaview apiCode={st.get('apiCode')} {str(st.get('apiMessage'))[:30]}")
    except Exception as e:
        msgs.append(f"kartaview {type(e).__name__}: {str(e)[:50]}")
    raise ConnectionError("street-level imagery needs an account token and none is configured; " + "; ".join(msgs))


WD_URL = "https://query.wikidata.org/sparql"
_WD_ITEMS = """SELECT ?i ?c (GROUP_CONCAT(DISTINCT STRAFTER(STR(?t), "entity/"); separator=" ") AS ?ts) WHERE {
  SERVICE wikibase:box { ?i wdt:P625 ?c .
    bd:serviceParam wikibase:cornerWest "Point(%.6f %.6f)"^^geo:wktLiteral .
    bd:serviceParam wikibase:cornerEast "Point(%.6f %.6f)"^^geo:wktLiteral }
  OPTIONAL { ?i wdt:P31 ?t } } GROUP BY ?i ?c"""
_WD_LABELS = """SELECT ?q ?l WHERE { VALUES ?q { %s } ?q rdfs:label ?l . FILTER(LANG(?l) = "en") }"""


def _sparql(q):
    r = S.get(WD_URL, params={"query": q, "format": "json"},
              headers=dict(UA, Accept="application/sparql-results+json"), timeout=90)
    if r.status_code != 200:
        raise ConnectionError(f"WDQS HTTP {r.status_code}: {r.text[:90]}")
    return r.json()["results"]["bindings"]


def _a_wikidata(aoi):
    """gazetteer -- Wikidata items with a P625 coordinate inside the AOI polygon, and their P31 classes."""
    x0, y0, x1, y1 = aoi["bbox"]
    g = _aoi_geom(aoi)
    ents = {}
    for b in _sparql(_WD_ITEMS % (x0, y0, x1, y1)):
        lon, lat = map(float, b["c"]["value"][6:-1].split())
        if shapely.contains_xy(g, lon, lat):
            ents[b["i"]["value"]] = b.get("ts", {}).get("value", "").split()
    n = len(ents)
    cnt = collections.Counter(q for ts in ents.values() for q in ts)
    f = {"wd_entities": n, "wd_entities_per_km2": _r(n / max(aoi["area_km2"], 1e-6), 3),
         "wd_distinct_classes": len(cnt)}
    top = cnt.most_common(5)
    if top:
        lab = {b["q"]["value"].rsplit("/", 1)[-1]: b["l"]["value"]
               for b in _sparql(_WD_LABELS % " ".join(f"wd:{q}" for q, _ in top))}
        names = [lab.get(q, q) for q, _ in top]
        f["wd_entities_by_class"] = [{"class": nm if names.count(nm) == 1 else f"{nm} ({q})", "value": v}
                                     for nm, (q, v) in zip(names, top)]
    return f, {"obs_date": "live", "res_m": "point", "item": "wikidata_query_service",
               "release": time.strftime("%Y-%m-%d"), "url": WD_URL}


_PLACE_SUB = ["locality", "county", "region", "country", "macrohood", "neighborhood",
              "localadmin", "borough"]


def _a_divisions(aoi):
    """gazetteer -- Overture divisions/division: named places and admin context within 15 km, else 60 km, of the AOI."""
    g, lat0 = _aoi_geom(aoi), _lat(aoi)
    cols = ["names", "subtype", "population", "country", "region"]
    for rad in (15.0, 60.0):               # remote AOIs have no named place nearby
        tb = _need(_read("divisions", "division", _pad(aoi["bbox"], rad), cols), "divisions")
        if tb.num_rows:
            x, y = _xy(tb)
            m = shapely.contains_xy(_buffer(g, rad, lat0), x, y)
            if m.any():
                tb = tb.filter(pa.array(m))
                break
    else:
        raise LookupError("no Overture named place within 60 km of the AOI")
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
         "ovt_division_n_by_subtype": _by_class(sub)}
    loc = np.isin(sub, _PLACE_SUB)
    cand = np.where(loc)[0] if loc.any() else np.arange(tb.num_rows)
    j = int(cand[np.argmin(dist[cand])])
    f["ovt_nearest_place"] = str(nm[j] or "")
    f["ovt_nearest_place_subtype"] = str(sub[j])
    f["ovt_nearest_place_km"] = _r(dist[j], 3)
    reg = [v for v in tb["region"].to_pylist() if v]
    cty = [v for v in tb["country"].to_pylist() if v]
    if reg:
        f["ovt_region_code"] = str(collections.Counter(reg).most_common(1)[0][0])
    if cty:
        f["ovt_country_code"] = str(collections.Counter(cty).most_common(1)[0][0])
    pv = pop[np.isfinite(pop) & (pop > 0)]
    if pv.size:
        f["ovt_max_place_population_radius"] = int(pv.max())
        f["ovt_n_places_with_population"] = int(pv.size)
    inm = [nm[i] for i in np.where(inside)[0] if nm[i]]
    if inm:
        f["ovt_place_names_in_aoi"] = "; ".join(inm)
    return f, _ovt_meta("divisions", "division", "admin")


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
    """Build every Overture manifest and fetch the GPPD CSV, so no adapter pays the build cost."""
    out = {f"{th}/{tp}": len(_manifest(th, tp)) for th, tp in themes}
    out["gppd_rows"] = len(_gppd())
    out["release"] = _release()
    out["cache_mb"] = round(sum(os.path.getsize(os.path.join(CACHE, f))
                                for f in os.listdir(CACHE)) / 1e6, 2)
    return out

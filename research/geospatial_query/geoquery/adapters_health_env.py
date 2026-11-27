"""adapters_health_env.py -- health, air quality and emissions lane.

Slots filled: air_quality_monitors, air_quality_modelled, emissions_releases,
disease_surveillance, wastewater_surveillance, contaminated_sites.

Every route is a plain public HTTPS endpoint: no API keys, no registration, no
credentials, no connectors.  Sources that are key-gated (EPA AQS REST API) are
recorded in AUTH_BLOCKED below and deliberately NOT shipped.

Admin-aggregate sources (CDC NNDSS / NWSS, PHAC RVDSS) are county-, province- or
state-level statistics joined to the AOI by point-in-polygon, and `meta['res_m']` says
so (`admin:county` / `admin:state` / `admin:province`).  Point-source distances and
`_<N>km` counts are measured from the AOI polygon (0 inside it), not its centroid.
"""
import os, io, csv, time, math, zipfile, threading
import xml.etree.ElementTree as ET

import numpy as np
import pandas as pd
import requests

from .pipeline import S                      # shared requests.Session

CACHE = os.path.abspath("./cache")
os.makedirs(CACHE, exist_ok=True)

ECCC = "https://data-donnees.az.ec.gc.ca/api/file"
ECCC_LS = "https://data-donnees.az.ec.gc.ca/api/path_contents"
NAPS_DIR = "air/monitor/national-air-pollution-surveillance-naps-program"
NPRI_DIR = ("substances/plansreports/reporting-facilities-pollutant-release-and-transfer-data"
            "/single-year-data-tables-by-facility-releases-transfers-and-disposals")
GHGRP_DIR = ("substances/monitor/greenhouse-gas-reporting-program-ghgrp-facility-"
             "greenhouse-gas-ghg-data")
AQS_BASE = "https://aqs.epa.gov/aqsweb/airdata"
CAMS_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"
FRS = "https://frs-public.epa.gov/ords/frs_public2/frs_rest_services.get_facilities"
ECHO = "https://echodata.epa.gov/echo/echo_rest_services.get_facilities"
CDC = "https://data.cdc.gov/resource"
INFOBASE = "https://health-infobase.canada.ca/src/data"
RVDSS_URL = f"{INFOBASE}/respiratory-virus-detections/RVD_WeeklyData.csv"
PHAC_WW_URL = f"{INFOBASE}/covidLive/wastewater/covid19-wastewater.csv"
PHAC_WW_SITES = os.path.join(os.path.dirname(os.path.abspath(__file__)), "phac_ww_sites.csv")
TIGER_COUNTY = ("https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/"
                "State_County/MapServer/1/query")
NE_ADMIN1 = ("https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/"
             "geojson/ne_10m_admin_1_states_provinces.geojson")
FCSI_ZIP = "https://map-carte.tbs-sct.gc.ca/fcsi-rscf/oddo/fcsi-rscf.zip"
FCSI_AIA = "http://crt.sectigo.com/EntrustOVTLSIssuingRSACA2.crt"

BUF_KM = 25.0            # AOI buffer for point-source joins

# Sources checked and deliberately not shipped because they need a credential.
AUTH_BLOCKED = {
    "epa_aqs_api": ("https://aqs.epa.gov/data/api/* requires a free email+key "
                    "registration (signup/email + key on every call). Not shipped. "
                    "The keyless pre-generated AirData annual files are used instead."),
}

# Sources checked and not shipped for reasons other than authentication.
NOT_SHIPPED = {
    "epa_envirofacts_tri": ("TRI release quantities (data.epa.gov/efservice/...) are not "
                            "reachable from this sandbox, and a sister lane measured the "
                            "service's lat/long range operators as non-filtering: "
                            "byte-identical payloads for a real AOI and the mid-Pacific "
                            "control. Not shipped at any buffer."),
    "eccc_naps_hourly": ("NAPS hourly continuous data is one zip per pollutant per year "
                         "(hundreds of MB/year). The annual-means summary is used instead; "
                         "hourly would blow the lane cache budget."),
}

# --------------------------------------------------------------- infrastructure
_LOCKS, _LOCKS_GUARD, _MEM = {}, threading.Lock(), {}


def _once(key, build):
    """Thread-safe module-level lazy loader (the pipeline runs an 8-thread pool)."""
    if key in _MEM:
        return _MEM[key]
    with _LOCKS_GUARD:
        lk = _LOCKS.setdefault(key, threading.Lock())
    with lk:
        if key not in _MEM:
            _MEM[key] = build()
    return _MEM[key]


def _hav_km(lat0, lon0, lats, lons):
    """Vectorised great-circle distance in km from one point to many."""
    la0, lo0 = math.radians(lat0), math.radians(lon0)
    la = np.radians(np.asarray(lats, dtype="f8"))
    lo = np.radians(np.asarray(lons, dtype="f8"))
    d = (np.sin((la - la0) / 2) ** 2
         + math.cos(la0) * np.cos(la) * np.sin((lo - lo0) / 2) ** 2)
    return 6371.0088 * 2 * np.arcsin(np.sqrt(np.clip(d, 0, 1)))


def _aoi_km(aoi, lats, lons):
    """Distance in km from the AOI polygon (0 inside) to each point, in an azimuthal
    equidistant frame centred on the AOI, so near distances are true to well under 1%."""
    import shapely
    from shapely.geometry import shape
    from shapely.ops import transform
    from pyproj import Transformer
    cx, cy = aoi["centroid"]
    t = Transformer.from_crs("EPSG:4326", f"+proj=aeqd +lat_0={cy} +lon_0={cx} +units=km",
                             always_xy=True)
    g = transform(lambda x, y, z=None: t.transform(x, y), shape(aoi["geom"]))
    x, y = t.transform(np.asarray(lons, dtype="f8"), np.asarray(lats, dtype="f8"))
    return shapely.distance(g, shapely.points(x, y))


def _ranked(aoi, df):
    """Rows with `km` from the AOI, nearest first; points inside the AOI rank by centroid distance."""
    cx, cy = aoi["centroid"]
    return (df.assign(km=_aoi_km(aoi, df["lat"], df["lon"]),
                      ckm=_hav_km(cy, cx, df["lat"], df["lon"]))
            .sort_values(["km", "ckm"], kind="stable"))


def _fold(s):
    """Accent- and case-insensitive key (Natural Earth says 'Québec', PHAC 'Quebec')."""
    import unicodedata
    return "".join(c for c in unicodedata.normalize("NFKD", str(s))
                   if not unicodedata.combining(c)).strip().lower()


def _pad_bbox(bbox, km):
    x0, y0, x1, y1 = bbox
    dy = km / 110.574
    dx = km / max(1e-6, 111.320 * math.cos(math.radians((y0 + y1) / 2)))
    return [x0 - dx, y0 - dy, x1 + dx, y1 + dy]


def _eccc_url(path):
    return requests.Request("GET", ECCC, params={"path": path}).prepare().url


def _eccc_get(path, dest=None, timeout=300):
    """Download one file from the ECCC data catalogue (streamed when cached to disk)."""
    r = S.get(ECCC, params={"path": path}, timeout=timeout, stream=dest is not None)
    r.raise_for_status()
    if dest is None:
        return r.content
    tmp = dest + ".part"
    with open(tmp, "wb") as fh:
        for chunk in r.iter_content(1 << 20):
            fh.write(chunk)
    os.replace(tmp, dest)
    return dest


def _eccc_ls(parent):
    r = S.get(ECCC_LS, params={"path": parent}, timeout=90)
    r.raise_for_status()
    return [e["name"] for e in r.json().get("path_contents", [])]


def _fcsi_ca_bundle():
    """certifi + the Entrust intermediate that map-carte.tbs-sct.gc.ca omits.

    The TBS host serves an incomplete chain (no 'Entrust OV TLS Issuing RSA CA 2'),
    so verification fails for every client.  We fetch that intermediate from the
    leaf's AuthorityInformationAccess caIssuers URL -- standard AIA chasing -- and
    verify against certifi PLUS it.  Verification stays ON; nothing is disabled.
    """
    def build():
        import certifi
        from cryptography import x509
        from cryptography.hazmat.primitives.serialization import Encoding
        out = os.path.join(CACHE, "fcsi_ca_bundle.pem")
        if not os.path.exists(out):
            der = S.get(FCSI_AIA, timeout=60)
            der.raise_for_status()
            pem = x509.load_der_x509_certificate(der.content).public_bytes(Encoding.PEM)
            with open(out + ".part", "wb") as fh:
                fh.write(open(certifi.where(), "rb").read() + b"\n" + pem)
            os.replace(out + ".part", out)
        return out
    return _once("fcsi_ca", build)


# --------------------------------------------------------------- admin joins
def _admin1():
    """Natural Earth 10m admin-1 (states/provinces), public domain. ~12 MB once."""
    def build():
        import geopandas as gpd
        pq = os.path.join(CACHE, "ne_admin1.parquet")
        if not os.path.exists(pq):
            r = S.get(NE_ADMIN1, timeout=300)
            r.raise_for_status()
            g = gpd.read_file(io.BytesIO(r.content))
            g = g[g["iso_a2"].isin(["CA", "US"])][
                ["name", "postal", "iso_3166_2", "iso_a2", "geometry"]].reset_index(drop=True)
            g.to_parquet(pq + ".part")
            os.replace(pq + ".part", pq)
        g = gpd.read_parquet(pq)
        g.sindex  # build spatial index once
        return g
    return _once("admin1", build)


def _admin1_at(lon, lat):
    from shapely.geometry import Point
    g = _admin1()
    hit = g.iloc[list(g.sindex.query(Point(lon, lat), predicate="intersects"))]
    if hit.empty:
        raise LookupError(f"no state/province polygon contains centroid ({lon:.4f},{lat:.4f})")
    r = hit.iloc[0]
    return {"name": str(r["name"]), "postal": str(r["postal"]),
            "iso_3166_2": str(r["iso_3166_2"]), "country": str(r["iso_a2"])}


def _us_county(lon, lat):
    r = S.get(TIGER_COUNTY, params={
        "f": "json", "geometry": f"{lon},{lat}", "geometryType": "esriGeometryPoint",
        "inSR": 4326, "spatialRel": "esriSpatialRelIntersects",
        "outFields": "NAME,STATE,COUNTY,GEOID", "returnGeometry": "false"}, timeout=60)
    r.raise_for_status()
    f = r.json().get("features") or []
    if not f:
        raise LookupError(f"TIGERweb returned no county for ({lon:.4f},{lat:.4f})")
    a = f[0]["attributes"]
    return {"name": a["NAME"], "fips": a["GEOID"], "state_fips": a["STATE"]}


# =============================================================== air quality
def _naps_means():
    """NAPS continuous annual means table (one row per station x pollutant). ~0.3 MB cached."""
    def build():
        pq = os.path.join(CACHE, "naps_means_v2.parquet")
        if os.path.exists(pq):
            return pd.read_parquet(pq)
        year = max(n for n in _eccc_ls(f"{NAPS_DIR}/Data-Donnees") if n.isdigit() and len(n) == 4)
        last_err = None
        for y in (year, str(int(year) - 1), str(int(year) - 2)):
            p = (f"{NAPS_DIR}/Data-Donnees/{y}/ContinuousData-DonneesContinu/"
                 f"AnnualSummaries-SommairesAnnuels/{y}_Means_EN.csv")
            try:
                raw = _eccc_get(p).decode("utf-8-sig", "replace")
            except Exception as e:                       # year present but no means file
                last_err = e
                continue
            lines = raw.splitlines()
            hdr = next(i for i, l in enumerate(lines) if l.startswith("************MEANS"))
            rows = list(csv.reader(lines[hdr + 1:]))
            cols = rows[0]
            df = pd.DataFrame([r[:len(cols)] for r in rows[1:] if len(r) >= len(cols)],
                              columns=cols)
            df = df.rename(columns={"NAPS ID": "naps_id", "Latitude": "lat",
                                    "Longitude": "lon", "Pollutant": "pollutant",
                                    "Average": "annual_mean", "City": "city"})
            for c in ("lat", "lon", "annual_mean"):
                df[c] = pd.to_numeric(df[c], errors="coerce")
            df = df.dropna(subset=["lat", "lon"])
            df["year"] = int(y)
            df["src"] = p
            df = df[["naps_id", "city", "lat", "lon", "pollutant", "annual_mean", "year", "src"]]
            df.to_parquet(pq + ".part")
            os.replace(pq + ".part", pq)
            return df
        raise LookupError(f"no NAPS *_Means_EN.csv found for {year} or two prior years "
                          f"({last_err})")
    return _once("naps", build)


# units as the Means file header states them
NAPS_UNITS = {"SO2": "ppb", "NO2": "ppb", "O3": "ppb", "NO": "ppb", "NOX": "ppb",
              "CO": "ppm", "PM10": "ug/m3", "PM2.5": "ug/m3"}


def _a_naps(aoi):
    if aoi["country"] != "CA":
        raise PermissionError("out of coverage (NAPS is Canada-only)")
    df = _naps_means()
    stn = _ranked(aoi, df)
    uniq = stn.drop_duplicates("naps_id")
    f = {"naps_nearest_continuous_station_km": round(float(uniq["km"].iloc[0]), 2),
         "naps_nearest_continuous_station_id": str(uniq["naps_id"].iloc[0]),
         "naps_nearest_continuous_station_city": str(uniq["city"].iloc[0]),
         "naps_continuous_stations_25km": int((uniq["km"] <= 25).sum()),
         "naps_continuous_stations_50km": int((uniq["km"] <= 50).sum())}
    near = stn[(stn["km"] <= 50) & stn["annual_mean"].notna()].drop_duplicates("pollutant")
    if len(near):
        f["naps_annual_mean"] = [{"class": r.pollutant, "value": round(float(r.annual_mean), 4),
                                  "unit": NAPS_UNITS.get(r.pollutant, "")}
                                 for r in near.itertuples()]
        f["naps_annual_mean_station_km"] = [{"class": r.pollutant, "value": round(float(r.km), 2)}
                                            for r in near.itertuples()]
    y = int(df["year"].iloc[0])
    return f, {"obs_date": str(y), "res_m": "point", "item": f"{y}_Means_EN.csv",
               "release": f"NAPS {y} continuous annual means", "url": _eccc_url(df["src"].iloc[0])}


# EPA parameter code -> unit as AirData's 'Units of Measure' gives it
AQS_PARAMS = {"88101": "ug/m3", "44201": "ppm", "42602": "ppb", "42401": "ppb",
              "42101": "ppm", "81102": "ug/m3", "88502": "ug/m3"}


def _aqs_annual():
    """EPA AirData pre-generated annual concentrations by monitor (keyless). ~4 MB."""
    def build():
        pq = os.path.join(CACHE, "aqs_annual_v2.parquet")
        if os.path.exists(pq):
            return pd.read_parquet(pq)
        this = time.gmtime().tm_year
        for y in (this - 1, this - 2, this):
            u = f"{AQS_BASE}/annual_conc_by_monitor_{y}.zip"
            r = S.get(u, timeout=300)
            if r.status_code != 200 or r.content[:2] != b"PK":
                continue
            z = zipfile.ZipFile(io.BytesIO(r.content))
            df = pd.read_csv(z.open(z.namelist()[0]), low_memory=False,
                             dtype={"Parameter Code": str},
                             usecols=["State Code", "County Code", "Site Num",
                                      "Parameter Code", "Parameter Name", "Sample Duration",
                                      "Latitude", "Longitude", "Year", "Arithmetic Mean",
                                      "Observation Percent", "Local Site Name"])
            df = df[df["Parameter Code"].isin(AQS_PARAMS)]
            df = df.rename(columns={"Latitude": "lat", "Longitude": "lon",
                                    "Parameter Code": "pcode", "Parameter Name": "param",
                                    "Arithmetic Mean": "mean", "Observation Percent": "obs_pct",
                                    "Year": "year", "Local Site Name": "site_name",
                                    "Sample Duration": "duration"})
            df["site_id"] = (df["State Code"].astype(str).str.zfill(2)
                             + df["County Code"].astype(str).str.zfill(3)
                             + df["Site Num"].astype(str).str.zfill(4))
            df["src"] = u
            df = df[["site_id", "site_name", "lat", "lon", "pcode", "param", "duration",
                     "mean", "obs_pct", "year", "src"]].dropna(subset=["lat", "lon"])
            df.to_parquet(pq + ".part")
            os.replace(pq + ".part", pq)
            return df
        raise LookupError("no annual_conc_by_monitor_<year>.zip reachable on aqs.epa.gov")
    return _once("aqs", build)


def _a_aqs(aoi):
    if aoi["country"] != "US":
        raise PermissionError("out of coverage (EPA AQS is US-only)")
    df = _aqs_annual()
    m = _ranked(aoi, df)
    sites = m.drop_duplicates("site_id")
    f = {"aqs_nearest_monitor_km": round(float(sites["km"].iloc[0]), 2),
         "aqs_nearest_site_id": str(sites["site_id"].iloc[0]),
         "aqs_nearest_site_name": str(sites["site_name"].iloc[0]),
         "aqs_sites_25km": int((sites["km"] <= 25).sum()),
         "aqs_sites_50km": int((sites["km"] <= 50).sum())}
    near = m[(m["km"] <= 50) & m["mean"].notna() & (m["obs_pct"] >= 50)].drop_duplicates("pcode")
    if len(near):
        f["aqs_annual_mean"] = [{"class": r.param, "value": round(float(r.mean), 5),
                                 "unit": AQS_PARAMS[r.pcode]} for r in near.itertuples()]
        f["aqs_annual_mean_site_km"] = [{"class": r.param, "value": round(float(r.km), 2)}
                                        for r in near.itertuples()]
    y = int(df["year"].iloc[0])
    return f, {"obs_date": str(y), "res_m": "point", "item": f"annual_conc_by_monitor_{y}",
               "release": f"EPA AirData annual concentration by monitor {y}",
               "url": df["src"].iloc[0]}


CAMS_VARS = ["pm2_5", "pm10", "carbon_monoxide", "nitrogen_dioxide",
             "sulphur_dioxide", "ozone", "aerosol_optical_depth"]
CAMS_DAYS = 30


def _a_cams(aoi):
    cx, cy = aoi["centroid"]
    r = S.get(CAMS_URL, params={"latitude": round(cy, 4), "longitude": round(cx, 4),
                                "hourly": ",".join(CAMS_VARS), "domains": "cams_global",
                                "past_days": CAMS_DAYS, "forecast_days": 0, "timezone": "UTC"},
              timeout=60)
    r.raise_for_status()
    j = r.json()
    h, units = j.get("hourly") or {}, j.get("hourly_units") or {}
    t = h.get("time") or []
    if not t:
        raise LookupError("open-meteo air-quality returned an empty hourly series")
    mean, p95 = [], []
    for v in CAMS_VARS:
        a = np.array([x for x in (h.get(v) or []) if x is not None], dtype="f8")
        if not a.size:
            continue
        unit = (units.get(v) or "1").replace("μg/m³", "ug/m3")
        mean.append({"class": v, "value": round(float(a.mean()), 3), "unit": unit})
        p95.append({"class": v, "value": round(float(np.percentile(a, 95)), 3), "unit": unit})
    if not mean:
        raise LookupError("open-meteo air-quality returned no valid values")
    f = {f"cams_hourly_mean_{CAMS_DAYS}d": mean, f"cams_hourly_p95_{CAMS_DAYS}d": p95,
         "cams_grid_lat": round(float(j.get("latitude")), 3),
         "cams_grid_lon": round(float(j.get("longitude")), 3)}
    return f, {"obs_date": f"{t[0][:10]}/{t[-1][:10]}", "res_m": 45000,
               "item": f"{len(t)} hourly steps", "release": "CAMS global (Open-Meteo domains=cams_global)",
               "url": CAMS_URL}


# =============================================================== emissions
NPRI_COLS = {"Année / Year": "year", "ID INRP / NPRI ID": "npri_id",
             "Nom de l'installation / Facility Name": "facility",
             "Latitude / Latitude": "lat", "Longitude / Longitude": "lon",
             "Nom de secteur SCIAN 4 (Anglais) / NAICS 4 Sector Name (English)": "sector",
             "Nom de substance (Anglais) / Substance Name (English)": "substance",
             "Unités / Units": "units"}
NPRI_MEDIA = {"Émissions à l'air - Totale / Air Emissions - Total": "air",
              "Rejets à l'eau - Totale / Water Releases - Total": "water",
              "Rejets au sol - Totalee / Land Releases - Total": "land"}
# NPRI unit -> (factor, unit written)
NPRI_UNITS = {"tonnes": (1000.0, "kg"), "kg": (1.0, "kg"), "grams": (0.001, "kg"),
              "g TEQ": (1.0, "g TEQ")}


def _npri():
    """NPRI single-year facility x substance release table, newest year. ~39 MB CSV."""
    def build():
        pq = os.path.join(CACHE, "npri_releases_v2.parquet")
        if os.path.exists(pq):
            return pd.read_parquet(pq)
        names = [n for n in _eccc_ls(NPRI_DIR) if n.endswith(".csv") and "DataDonn" in n]
        if not names:
            raise LookupError("no NPRI-INRP_DataDonnees_<year>.csv in the NPRI directory")
        path = f"{NPRI_DIR}/{sorted(names)[-1]}"
        raw = _eccc_get(path, dest=os.path.join(CACHE, "_npri_raw.csv"))
        df = pd.read_csv(raw, encoding="latin-1", low_memory=False,
                         usecols=list(NPRI_COLS) + list(NPRI_MEDIA))
        df = df.rename(columns={**NPRI_COLS, **NPRI_MEDIA}).dropna(subset=["lat", "lon"])
        bad = set(df["units"]) - set(NPRI_UNITS)
        if bad:
            raise LookupError(f"NPRI units not understood: {sorted(bad)}")
        factor = df["units"].map(lambda u: NPRI_UNITS[u][0])
        for c in ("air", "water", "land"):
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0) * factor
        df["unit"] = df["units"].map(lambda u: NPRI_UNITS[u][1])
        df["src"] = path
        df = df[["year", "npri_id", "facility", "lat", "lon", "sector", "substance", "unit",
                 "air", "water", "land", "src"]]
        df.to_parquet(pq + ".part")
        os.replace(pq + ".part", pq)
        os.remove(raw)
        return df
    return _once("npri", build)


def _a_npri(aoi):
    if aoi["country"] != "CA":
        raise PermissionError("out of coverage (NPRI is Canada-only)")
    df = _npri()
    w = _ranked(aoi, df)
    fac = w.drop_duplicates("npri_id")
    f = {"npri_nearest_facility_km": round(float(fac["km"].iloc[0]), 2),
         "npri_nearest_facility_name": str(fac["facility"].iloc[0]),
         "npri_nearest_facility_sector": str(fac["sector"].iloc[0]),
         "npri_facilities_25km": int((fac["km"] <= BUF_KM).sum()),
         "npri_facilities_10km": int((fac["km"] <= 10).sum())}
    near = w[w["km"] <= BUF_KM]
    f["npri_substances_25km"] = int(near["substance"].nunique())
    tot = near.groupby(["substance", "unit"])[["air", "water", "land"]].sum()
    for col in ("air", "water", "land"):
        recs = [{"class": s, "value": float(f"{v:.6g}"), "unit": u}
                for (s, u), v in tot[col].items() if v > 0]
        if recs:
            f[f"npri_{col}_releases_25km"] = recs
    y = int(df["year"].iloc[0])
    return f, {"obs_date": str(y), "res_m": "point", "item": df["src"].iloc[0].rsplit("/", 1)[-1],
               "release": f"NPRI {y} reporting year", "url": _eccc_url(df["src"].iloc[0])}


def _ghgrp():
    """ECCC GHGRP facility greenhouse-gas emissions, newest reference year. ~10 MB CSV."""
    def build():
        pq = os.path.join(CACHE, "ghgrp_v2.parquet")
        if os.path.exists(pq):
            return pd.read_parquet(pq)
        names = [n for n in _eccc_ls(GHGRP_DIR) if n.endswith(".csv") and "GHGEmissionsGES" in n]
        if not names:
            raise LookupError("no PDGES-GHGRP-GHGEmissionsGES*.csv in the GHGRP directory")
        path = f"{GHGRP_DIR}/{sorted(names)[-1]}"
        raw = _eccc_get(path, dest=os.path.join(CACHE, "_ghgrp_raw.csv"))
        hdr = pd.read_csv(raw, encoding="utf-8-sig", nrows=0).columns.tolist()

        def col(sub):
            for c in hdr:
                if sub.lower() in c.lower():
                    return c
            raise LookupError(f"GHGRP column matching {sub!r} not found")
        cmap = {col("Reference Year"): "year", col("Facility Name"): "facility",
                col("Latitude"): "lat", col("Longitude"): "lon",
                col("English Facility NAICS Code Description"): "sector",
                col("excl. CO2bio"): "co2e_t"}
        df = pd.read_csv(raw, encoding="utf-8-sig", low_memory=False,
                         usecols=list(cmap)).rename(columns=cmap)
        for c in ("lat", "lon", "co2e_t", "year"):
            df[c] = pd.to_numeric(df[c], errors="coerce")
        df = df.dropna(subset=["lat", "lon", "year"])
        df["year"] = df["year"].astype(int)
        df = df[df["year"] == df["year"].max()].assign(src=path)
        df.to_parquet(pq + ".part")
        os.replace(pq + ".part", pq)
        os.remove(raw)
        return df
    return _once("ghgrp", build)


def _a_ghgrp(aoi):
    if aoi["country"] != "CA":
        raise PermissionError("out of coverage (ECCC GHGRP is Canada-only)")
    df = _ghgrp()
    w = _ranked(aoi, df)
    near = w[w["km"] <= BUF_KM]
    f = {"ghgrp_nearest_facility_km": round(float(w["km"].iloc[0]), 2),
         "ghgrp_nearest_facility_name": str(w["facility"].iloc[0]),
         "ghgrp_facilities_25km": int(len(near)),
         "ghgrp_co2e_excl_co2bio_kt_25km": round(float(near["co2e_t"].fillna(0).sum()) / 1000.0, 2)}
    if len(near):
        f["ghgrp_max_facility_co2e_excl_co2bio_kt_25km"] = round(float(near["co2e_t"].max()) / 1000.0, 2)
        f["ghgrp_top_sector_25km"] = str(
            near.groupby("sector")["co2e_t"].sum().sort_values(ascending=False).index[0])
    y = int(df["year"].iloc[0])
    return f, {"obs_date": str(y), "res_m": "point", "item": df["src"].iloc[0].rsplit("/", 1)[-1],
               "release": f"GHGRP {y} reference year", "url": _eccc_url(df["src"].iloc[0])}


# Measured throttle on frs-public.epa.gov: the 4th request inside ~10 s returns 429,
# and the budget refills about 10 s later. One sliding-window limiter, 3 per 11 s.
_FRS_GATE, _FRS_HITS = threading.Lock(), []
_FRS_WINDOW_S, _FRS_MAX = 11.0, 3


def _frs_wait():
    while True:
        with _FRS_GATE:
            now = time.time()
            _FRS_HITS[:] = [t for t in _FRS_HITS if now - t < _FRS_WINDOW_S]
            if len(_FRS_HITS) < _FRS_MAX:
                _FRS_HITS.append(now)
                return
            sleep_for = _FRS_WINDOW_S - (now - _FRS_HITS[0]) + 0.1
        time.sleep(max(0.1, min(sleep_for, _FRS_WINDOW_S)))


def _frs(lat, lon, radius_mi, acrnm=None, timeout=90, retries=3):
    """EPA FRS radius search. search_radius is in MILES (verified: 1mi->2, 10mi->20,
    25mi->211 SEMS sites at Bingham Canyon)."""
    p = {"output": "JSON", "latitude83": round(lat, 6), "longitude83": round(lon, 6),
         "search_radius": radius_mi}
    if acrnm:
        p["pgm_sys_acrnm"] = acrnm
    r = None
    for k in range(retries):
        _frs_wait()
        r = S.get(FRS, params=p, timeout=timeout)
        if r.status_code not in (429, 502, 503, 504):
            break
        time.sleep(1.5 * (k + 1))
    r.raise_for_status()
    res = r.json().get("Results") or {}
    if "Error" in res:
        raise ConnectionError("FRS: " + str(res["Error"].get("ErrorMessage"))[:120])
    fac = res.get("FRSFacility") or []
    if isinstance(fac, dict):
        fac = [fac]
    out = []
    for x in fac:
        try:
            out.append({"id": x.get("RegistryId"), "name": x.get("FacilityName") or "",
                        "lat": float(x["Latitude83"]), "lon": float(x["Longitude83"])})
        except (TypeError, ValueError, KeyError):
            continue
    return out


def _echo_box(bbox, timeout=90):
    x0, y0, x1, y1 = bbox
    r = S.get(ECHO, params={"output": "JSON", "p_c1lat": round(y0, 6),
                            "p_c1lon": round(x0, 6), "p_c2lat": round(y1, 6),
                            "p_c2lon": round(x1, 6)}, timeout=timeout)
    r.raise_for_status()
    res = r.json().get("Results") or {}
    if res.get("Message") != "Success":
        raise ConnectionError("ECHO: " + str(res.get("Message"))[:120])
    return res


def _rows(res, key):
    try:
        return int(str(res.get(key, "")).replace(",", ""))
    except ValueError:
        return None


def _a_echo_tri(aoi):
    """US emissions-source presence from ECHO: REPORTER COUNTS in the AOI bbox padded by
    25 km and 10 km, not release quantities (TRI tonnages need Envirofacts, see NOT_SHIPPED)."""
    if aoi["country"] != "US":
        raise PermissionError("out of coverage (EPA ECHO/TRI is US-only)")
    f = {}
    for km in (25, 10):
        res = _echo_box(_pad_bbox(aoi["bbox"], km))
        for k, nm in (("TRIRows", "tri_reporters"), ("CAARows", "caa_air_sources"),
                      ("RCRRows", "rcra_handlers"), ("QueryRows", "regulated_facilities")):
            v = _rows(res, k)
            if v is not None:
                f[f"echotri_{nm}_bbox_pad{km}km"] = v
    if not f:
        raise LookupError("ECHO returned no countable rows at either buffer")
    return f, {"obs_date": "current snapshot", "res_m": "vector",
               "item": "ECHO bbox reporter counts", "release": "current snapshot", "url": ECHO}


# =============================================================== contaminated sites
def _a_frs_sems(aoi):
    if aoi["country"] != "US":
        raise PermissionError("out of coverage (EPA SEMS/Superfund is US-only)")
    cx, cy = aoi["centroid"]
    x0, y0, x1, y1 = aoi["bbox"]
    reach_km = BUF_KM + float(_hav_km(cy, cx, [y0, y0, y1, y1], [x0, x1, x0, x1]).max())
    fac = _frs(cy, cx, math.ceil(reach_km / 1.609344), acrnm="SEMS")
    d = _aoi_km(aoi, [x["lat"] for x in fac], [x["lon"] for x in fac]) if fac else np.zeros(0)
    f = {"sems_sites_25km": int((d <= 25).sum()), "sems_sites_10km": int((d <= 10).sum()),
         "sems_sites_5km": int((d <= 5).sum())}
    if fac:
        i = int(np.argmin(d))
        f["sems_nearest_km"] = round(float(d[i]), 2)
        f["sems_nearest_name"] = str(fac[i]["name"])
    return f, {"obs_date": "current", "res_m": "point", "item": "EPA FRS pgm_sys_acrnm=SEMS",
               "release": "current snapshot", "url": FRS}


def _fcsi():
    """Federal Contaminated Sites Inventory. 8 MB zip -> 287 MB XML, streamed once
    into a ~1 MB parquet of georeferenced sites."""
    def build():
        pq = os.path.join(CACHE, "fcsi_sites_v2.parquet")
        if os.path.exists(pq):
            return pd.read_parquet(pq)
        zp = os.path.join(CACHE, "fcsi.zip")
        if not os.path.exists(zp):
            r = S.get(FCSI_ZIP, timeout=600, verify=_fcsi_ca_bundle(), stream=True)
            r.raise_for_status()
            with open(zp + ".part", "wb") as fh:
                for c in r.iter_content(1 << 20):
                    fh.write(c)
            os.replace(zp + ".part", zp)
        z = zipfile.ZipFile(zp)
        rows = []
        with z.open(z.namelist()[0]) as fh:
            for ev, el in ET.iterparse(fh, events=("end",)):
                if el.tag != "Site":
                    continue
                lat, lon = el.findtext("Location/Latitude"), el.findtext("Location/Longitude")
                if lat and lon:
                    ce = el.find("ContaminationDetails/ContaminationEstimates")
                    try:
                        rows.append({
                            "org": el.get("ReportingOrganization"),
                            "name": el.findtext("Name/EN") or "",
                            "status": el.findtext("SiteStatus/Status/EN") or "",
                            "classification": el.findtext("Classification/Name/EN") or "",
                            "lat": float(lat), "lon": float(lon),
                            "cubic_m": float(ce.findtext("CubicMetres") or 0) if ce is not None else 0.0,
                            "hectares": float(ce.findtext("Hectares") or 0) if ce is not None else 0.0,
                        })
                    except ValueError:
                        pass
                el.clear()
        if not rows:
            raise LookupError("FCSI XML parsed to zero georeferenced sites")
        df = pd.DataFrame(rows)
        df.to_parquet(pq + ".part")
        os.replace(pq + ".part", pq)
        return df
    return _once("fcsi", build)


def _a_fcsi(aoi):
    if aoi["country"] != "CA":
        raise PermissionError("out of coverage (FCSI is Canada-only)")
    df = _fcsi()
    w = _ranked(aoi, df)
    near = w[w["km"] <= BUF_KM]
    f = {"fcsi_nearest_site_km": round(float(w["km"].iloc[0]), 2),
         "fcsi_nearest_site_name": str(w["name"].iloc[0]),
         "fcsi_nearest_site_status": str(w["status"].iloc[0]),
         "fcsi_nearest_site_org": str(w["org"].iloc[0]),
         "fcsi_sites_25km": int(len(near)),
         "fcsi_sites_10km": int((w["km"] <= 10).sum()),
         "fcsi_sites_5km": int((w["km"] <= 5).sum())}
    if len(near):
        for col, nm in (("status", "fcsi_sites_by_status_25km"),
                        ("classification", "fcsi_sites_by_classification_25km")):
            n = near[near[col] != ""][col].value_counts()
            if len(n):
                f[nm] = [{"class": k, "value": int(v)} for k, v in n.items()]
        f["fcsi_contamination_volume_m3_25km"] = round(float(near["cubic_m"].sum()), 1)
        f["fcsi_contaminated_area_ha_25km"] = round(float(near["hectares"].sum()), 3)
        f["fcsi_reporting_orgs_25km"] = int(near["org"].nunique())
    return f, {"obs_date": "current inventory snapshot", "res_m": "point",
               "item": "fcsi-rscf.xml", "release": "current snapshot", "url": FCSI_ZIP}


# =============================================================== disease surveillance
# labels exactly as the weekly table publishes them (Lyme disease is not in it)
NNDSS_WATCH = ["Salmonellosis (excluding Salmonella Typhi infection and Salmonella Paratyphi infection)",
               "Pertussis", "Hepatitis C, acute, Confirmed", "Campylobacteriosis",
               "Legionellosis", "Tuberculosis"]


def _a_nndss(aoi):
    """CDC NNDSS notifiable-disease counts, joined at STATE level (res_m='admin:state')."""
    if aoi["country"] != "US":
        raise PermissionError("out of coverage (CDC NNDSS is US-only)")
    cx, cy = aoi["centroid"]
    st = _admin1_at(cx, cy)["name"]
    u = f"{CDC}/x9gk-5huc.json"
    r = S.get(u, params={"$select": "max(year) as y", "$where": f"states='{st}'"}, timeout=60)
    r.raise_for_status()
    j = r.json()
    if not j or not j[0].get("y"):
        raise LookupError(f"NNDSS has no rows for state '{st}'")
    yr = j[0]["y"]
    r = S.get(u, params={"$select": "max(week) as w",
                         "$where": f"states='{st}' AND year='{yr}'"}, timeout=60)
    r.raise_for_status()
    wk = r.json()[0]["w"]
    r = S.get(u, params={"$where": f"states='{st}' AND year='{yr}' AND week='{wk}'",
                         "$limit": 5000}, timeout=90)
    r.raise_for_status()
    df = pd.DataFrame(r.json())
    if df.empty:
        raise LookupError(f"NNDSS returned no rows for {st} {yr}w{wk}")
    df = df[df["label"].isin(NNDSS_WATCH)]
    f = {"nndss_state": st}
    for col, nm in (("m1", "nndss_cases_current_week"), ("m3", "nndss_cases_ytd"),
                    ("m4", "nndss_cases_prev_year_ytd")):
        v = df.assign(v=pd.to_numeric(df.get(col), errors="coerce")).dropna(subset=["v"])
        if len(v):
            f[nm] = [{"class": r.label, "value": int(r.v)} for r in v.itertuples()]
    return f, {"obs_date": f"{yr}-W{int(wk):02d}", "res_m": "admin:state",
               "item": f"x9gk-5huc / {st}", "release": f"NNDSS weekly tables {yr}",
               "url": u}


def _rvdss():
    def build():
        r = S.get(RVDSS_URL, timeout=120)
        r.raise_for_status()
        df = pd.read_csv(io.BytesIO(r.content))
        for c in ("tests", "detections", "percentpositive", "week", "year"):
            df[c] = pd.to_numeric(df[c], errors="coerce")
        return df.dropna(subset=["tests", "detections", "percentpositive"], how="all")
    return _once("rvdss", build)


def _a_rvdss(aoi):
    """PHAC Respiratory Virus Detection Surveillance: the province's latest week, per virus."""
    if aoi["country"] != "CA":
        raise PermissionError("out of coverage (PHAC RVDSS is Canada-only)")
    cx, cy = aoi["centroid"]
    a = _admin1_at(cx, cy)
    df = _rvdss()
    sub = df[df["province"].astype(str).map(_fold) == _fold(a["name"])]
    if sub.empty:
        raise LookupError(f"PHAC RVDSS has no rows for province '{a['name']}'")
    last = sub.sort_values(["year", "week"]).iloc[-1]
    latest = sub[(sub["year"] == last["year"]) & (sub["week"] == last["week"])]
    f = {"rvdss_jurisdiction": str(last["province"])}
    for col, nm, cast in (("tests", "rvdss_tests", int), ("detections", "rvdss_detections", int),
                          ("percentpositive", "rvdss_percent_positive", float)):
        recs = [{"class": r.virus, "value": cast(getattr(r, col))}
                for r in latest.itertuples() if pd.notna(getattr(r, col))]
        if recs:
            f[nm] = recs
    y0 = int(df["year"].min())
    return f, {"obs_date": str(last["date"]), "res_m": "admin:province",
               "item": f"{int(last['year'])} week {int(last['week'])}",
               "release": f"RVDSS {y0}-{y0 + 1} season", "url": RVDSS_URL}


# =============================================================== wastewater
def _a_nwss(aoi):
    """CDC National Wastewater Surveillance System, joined at COUNTY level
    (res_m='admin:county'). A sewershed statistic, not an AOI measurement."""
    if aoi["country"] != "US":
        raise PermissionError("out of coverage (CDC NWSS is US-only)")
    cx, cy = aoi["centroid"]
    co = _us_county(cx, cy)
    u = f"{CDC}/2ew6-ywp6.json"
    meta = {"res_m": "admin:county", "item": f"2ew6-ywp6 / {co['name']} County ({co['fips']})",
            "release": "current snapshot", "url": u}
    r = S.get(u, params={"$where": f"county_fips like '%{co['fips']}%'",
                         "$order": "date_end DESC", "$limit": 2000}, timeout=90)
    r.raise_for_status()
    rows = r.json()
    if not rows:
        # NWSS covers participating sewersheds only; rural counties often have none.
        return ({"nwss_county": co["name"], "nwss_county_fips": co["fips"],
                 "nwss_sites_county": 0, "nwss_records_county": 0},
                {**meta, "obs_date": "current"})
    df = pd.DataFrame(rows)
    for c in ("percentile", "detect_prop_15d", "population_served"):
        df[c] = pd.to_numeric(df.get(c), errors="coerce")
    latest_end = df["date_end"].max()
    cur = df[df["date_end"] == latest_end]
    withp = df[df["percentile"].notna()]
    f = {"nwss_county": co["name"], "nwss_county_fips": co["fips"],
         "nwss_sites_county": int(df["wwtp_id"].nunique()),
         "nwss_population_served": round(float(
             df.drop_duplicates("wwtp_id")["population_served"].fillna(0).sum()), 0),
         "nwss_latest_period_end": str(latest_end)[:10],
         "nwss_records_county": int(len(df))}
    if len(cur) and cur["percentile"].notna().any():
        f["nwss_percentile_latest"] = round(float(cur["percentile"].mean()), 2)
    if len(cur) and cur["detect_prop_15d"].notna().any():
        f["nwss_detect_prop_15d_latest"] = round(float(cur["detect_prop_15d"].mean()), 2)
    if len(withp):
        f["nwss_percentile_last_reported"] = round(float(
            withp[withp["date_end"] == withp["date_end"].max()]["percentile"].mean()), 2)
        f["nwss_percentile_last_reported_end"] = str(withp["date_end"].max())[:10]
        f["nwss_percentile_max_county"] = round(float(withp["percentile"].max()), 2)
    return f, {**meta, "obs_date": str(latest_end)[:10]}


def _phac_ww():
    def build():
        r = S.get(PHAC_WW_URL, timeout=180)
        r.raise_for_status()
        df = pd.read_csv(io.BytesIO(r.content))
        for c in ("viral_load", "seven_day_rolling_avg"):
            df[c] = pd.to_numeric(df[c], errors="coerce")
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        sites = pd.read_csv(PHAC_WW_SITES)
        missing = set(df["Location"]) - set(sites["Location"])
        if missing:
            raise LookupError(f"phac_ww_sites.csv has no coordinates for {sorted(missing)}")
        return df.dropna(subset=["Date"]).merge(sites[["Location", "lat", "lon"]], on="Location")
    return _once("phac_ww", build)


def _a_phac_ww(aoi):
    """PHAC SARS-CoV-2 wastewater: the monitored plant nearest the AOI, latest sample per fraction."""
    if aoi["country"] != "CA":
        raise PermissionError("out of coverage (PHAC wastewater is Canada-only)")
    df = _phac_ww()
    p = _ranked(aoi, df.drop_duplicates("Location"))
    # a plant's liquid series and its NML solid series are separate Locations at one coordinate
    at = p[(p["lat"] == p["lat"].iloc[0]) & (p["lon"] == p["lon"].iloc[0])]["Location"]
    s = (df[df["Location"].isin(at)].sort_values("Date")
         .groupby("fractionid").tail(1).sort_values("fractionid"))
    f = {"phacww_location": [{"class": r.fractionid, "value": r.Location} for r in s.itertuples()],
         "phacww_location_km": round(float(p["km"].iloc[0]), 2),
         "phacww_date": [{"class": r.fractionid, "value": r.Date.date().isoformat()}
                         for r in s.itertuples()]}
    for c in ("viral_load", "seven_day_rolling_avg"):
        f[f"phacww_{c}"] = [{"class": r.fractionid, "value": round(float(getattr(r, c)), 4),
                             "unit": "copies/mL"} for r in s.itertuples() if pd.notna(getattr(r, c))]
    return f, {"obs_date": s["Date"].max().date().isoformat(), "res_m": "point",
               "item": ", ".join(sorted(set(s["Location"]))),
               "release": f"covid19-wastewater.csv, data to {df['Date'].max().date().isoformat()}",
               "url": PHAC_WW_URL}


# =============================================================== registration
# slot, source_id, family, licence, auth, fn
ADAPTERS = [
 ("air_quality_monitors",   "eccc_naps",      "health_env", "OGL-Canada-2.0",            "none", _a_naps),
 ("air_quality_monitors",   "epa_aqs_airdata","health_env", "public-domain (US EPA)",    "none", _a_aqs),
 ("air_quality_modelled",   "cams_open_meteo","health_env", "CC-BY-4.0 (CAMS/ECMWF)",    "none", _a_cams),
 ("emissions_releases",     "eccc_npri",      "health_env", "OGL-Canada-2.0",            "none", _a_npri),
 ("emissions_releases",     "eccc_ghgrp",     "health_env", "OGL-Canada-2.0",            "none", _a_ghgrp),
 ("emissions_releases",     "epa_echo_tri",   "health_env", "public-domain (US EPA)",    "none", _a_echo_tri),
 ("disease_surveillance",   "cdc_nndss",      "health_env", "public-domain (US CDC)",    "none", _a_nndss),
 ("disease_surveillance",   "phac_rvdss",     "health_env", "OGL-Canada-2.0",            "none", _a_rvdss),
 ("wastewater_surveillance","cdc_nwss",       "health_env", "public-domain (US CDC)",    "none", _a_nwss),
 ("wastewater_surveillance","phac_wastewater","health_env", "OGL-Canada-2.0",            "none", _a_phac_ww),
 ("contaminated_sites",     "epa_frs_sems",   "health_env", "public-domain (US EPA)",    "none", _a_frs_sems),
 ("contaminated_sites",     "tbs_fcsi",       "health_env", "OGL-Canada-2.0",            "none", _a_fcsi),
]

"""adapters_health_env.py -- health, air quality and emissions lane.

Slots filled: air_quality_monitors, air_quality_modelled, emissions_releases,
facility_compliance, disease_surveillance, wastewater_surveillance, contaminated_sites.

Every route is a plain public HTTPS endpoint: no API keys, no registration, no
credentials, no connectors.  Sources that are key-gated (EPA AQS REST API) are
recorded in AUTH_BLOCKED below and deliberately NOT shipped.

Admin-aggregate sources (CDC NNDSS / NWSS, PHAC RVDSS / wastewater) are county-,
province- or state-level statistics joined to the AOI by point-in-polygon.  They
are NOT AOI measurements, and they say so: every such feature name starts with a
jurisdiction prefix, and `meta['res_m']` is `admin:county` / `admin:state` /
`admin:province` rather than a length scale.
"""
import os, io, csv, json, time, math, zipfile, threading
import xml.etree.ElementTree as ET

import numpy as np
import pandas as pd
import requests

from . import pipeline
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
FRS = "https://frs-public.epa.gov/ords/frs_public2/frs_rest_services.get_facilities"
ECHO = "https://echodata.epa.gov/echo/echo_rest_services.get_facilities"
CDC = "https://data.cdc.gov/resource"
INFOBASE = "https://health-infobase.canada.ca/src/data"
TIGER_COUNTY = ("https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/"
                "State_County/MapServer/1/query")
NE_ADMIN1 = ("https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/"
             "geojson/ne_10m_admin_1_states_provinces.geojson")
FCSI_ZIP = "https://map-carte.tbs-sct.gc.ca/fcsi-rscf/oddo/fcsi-rscf.zip"
FCSI_AIA = "http://crt.sectigo.com/EntrustOVTLSIssuingRSACA2.crt"

BUF_KM = 25.0            # AOI buffer for point-source joins
CTRL_BOX = [-140.0, -20.0, -139.0, -19.0]   # mid-Pacific geometry control

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


def _eccc_latest_year(parent, prefix="", suffix=""):
    r = S.get(ECCC_LS, params={"path": parent}, timeout=90)
    r.raise_for_status()
    names = [e["name"] for e in r.json().get("path_contents", [])]
    yrs = sorted(n for n in names if n.isdigit() and len(n) == 4)
    if not yrs:
        raise LookupError(f"no year directories under {parent}")
    return yrs[-1]


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
    import geopandas as gpd
    from shapely.geometry import Point
    g = _admin1()
    hit = g.iloc[list(g.sindex.query(Point(lon, lat), predicate="intersects"))]
    if hit.empty:
        raise LookupError(f"no state/province polygon contains centroid ({lon:.4f},{lat:.4f})")
    r = hit.iloc[0]
    return {"name": str(r["name"]), "postal": str(r["postal"]),
            "iso_3166_2": str(r["iso_3166_2"]), "country": str(r["iso_a2"])}


# pruid (Statistics Canada province/territory SGC code) by province postal abbreviation
PRUID = {"NL": 10, "PE": 11, "NS": 12, "NB": 13, "QC": 24, "ON": 35, "MB": 46,
         "SK": 47, "AB": 48, "BC": 59, "YT": 60, "NT": 61, "NU": 62}
# PHAC RVDSS reporting regions by province
RVDSS_REGION = {"BC": "British Columbia", "AB": "Prairies", "SK": "Prairies",
                "MB": "Prairies", "ON": "Ontario", "QC": "Quebec", "NB": "Atlantic",
                "NS": "Atlantic", "PE": "Atlantic", "NL": "Atlantic",
                "YT": "Territories", "NT": "Territories", "NU": "Territories"}


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
    """NAPS annual means table (one row per station x pollutant). ~0.3 MB cached."""
    def build():
        pq = os.path.join(CACHE, "naps_means.parquet")
        if os.path.exists(pq):
            return pd.read_parquet(pq)
        year = _eccc_latest_year(f"{NAPS_DIR}/Data-Donnees")
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
                                    "Average": "annual_mean", "%Complete": "pct_complete",
                                    "City": "city"})
            for c in ("lat", "lon", "annual_mean", "pct_complete"):
                df[c] = pd.to_numeric(df[c], errors="coerce")
            df = df.dropna(subset=["lat", "lon"])
            df["year"] = int(y)
            df = df[["naps_id", "city", "lat", "lon", "pollutant", "annual_mean",
                     "pct_complete", "year"]]
            df.to_parquet(pq + ".part")
            os.replace(pq + ".part", pq)
            return df
        raise LookupError(f"no NAPS *_Means_EN.csv found for {year} or two prior years "
                          f"({last_err})")
    return _once("naps", build)


NAPS_UNITS = {"NO2": "ppb", "O3": "ppb", "SO2": "ppb", "NO": "ppb", "NOX": "ppb",
              "CO": "ppm", "PM2.5": "ugm3", "PM10": "ugm3"}


def _naps_features(lat, lon, df, pref="naps"):
    d = _hav_km(lat, lon, df["lat"].values, df["lon"].values)
    stn = df.assign(km=d).sort_values("km")
    uniq = stn.drop_duplicates("naps_id")
    f = {f"{pref}_nearest_station_km": round(float(uniq["km"].iloc[0]), 2),
         f"{pref}_nearest_station_id": str(uniq["naps_id"].iloc[0]),
         f"{pref}_nearest_station_city": str(uniq["city"].iloc[0])[:48],
         f"{pref}_stations_25km": int((uniq["km"] <= 25).sum()),
         f"{pref}_stations_50km": int((uniq["km"] <= 50).sum()),
         f"{pref}_year": int(df["year"].iloc[0])}
    near = stn[(stn["km"] <= 50) & stn["annual_mean"].notna()]
    for pol, unit in NAPS_UNITS.items():
        sub = near[near["pollutant"] == pol]
        if sub.empty:
            continue
        key = pol.replace(".", "").lower()
        f[f"{pref}_{key}_{unit}_mean"] = round(float(sub["annual_mean"].iloc[0]), 4)
        f[f"{pref}_{key}_src_km"] = round(float(sub["km"].iloc[0]), 2)
    return f


def _a_naps(aoi):
    if aoi["country"] != "CA":
        raise PermissionError("out of coverage (NAPS is Canada-only)")
    df = _naps_means()
    cx, cy = aoi["centroid"]
    f = _naps_features(cy, cx, df)
    return f, {"obs_date": str(int(df["year"].iloc[0])), "res_m": "point",
               "item": f"NAPS {int(df['year'].iloc[0])} annual means"}


def _aqs_annual():
    """EPA AirData pre-generated annual concentrations by monitor (keyless). ~4 MB."""
    PARAMS = {"88101": ("pm25", "ugm3"), "44201": ("o3", "ppm"), "42602": ("no2", "ppb"),
              "42401": ("so2", "ppb"), "42101": ("co", "ppm"), "81102": ("pm10", "ugm3"),
              "88502": ("pm25_nonfrm", "ugm3")}

    def build():
        pq = os.path.join(CACHE, "aqs_annual.parquet")
        if os.path.exists(pq):
            return pd.read_parquet(pq)
        this = time.gmtime().tm_year
        for y in (this - 1, this - 2, this):
            u = f"{AQS_BASE}/annual_conc_by_monitor_{y}.zip"
            r = S.get(u, timeout=300)
            if r.status_code != 200 or r.content[:2] != b"PK":
                continue
            z = zipfile.ZipFile(io.BytesIO(r.content))
            name = z.namelist()[0]
            df = pd.read_csv(z.open(name), low_memory=False, dtype={"Parameter Code": str},
                             usecols=["State Code", "County Code", "Site Num",
                                      "Parameter Code", "Parameter Name", "POC",
                                      "Sample Duration", "Latitude", "Longitude",
                                      "Year", "Units of Measure", "Arithmetic Mean",
                                      "Observation Percent", "Local Site Name"])
            df = df[df["Parameter Code"].isin(PARAMS)]
            df = df.rename(columns={"Latitude": "lat", "Longitude": "lon",
                                    "Parameter Code": "pcode", "Arithmetic Mean": "mean",
                                    "Observation Percent": "obs_pct", "Year": "year",
                                    "Local Site Name": "site_name",
                                    "Sample Duration": "duration"})
            df["site_id"] = (df["State Code"].astype(str).str.zfill(2)
                             + df["County Code"].astype(str).str.zfill(3)
                             + df["Site Num"].astype(str).str.zfill(4))
            df = df[["site_id", "site_name", "lat", "lon", "pcode", "duration",
                     "mean", "obs_pct", "year"]].dropna(subset=["lat", "lon"])
            df.to_parquet(pq + ".part")
            os.replace(pq + ".part", pq)
            return df
        raise LookupError("no annual_conc_by_monitor_<year>.zip reachable on aqs.epa.gov")
    return _once("aqs", build), PARAMS


def _a_aqs(aoi):
    if aoi["country"] != "US":
        raise PermissionError("out of coverage (EPA AQS is US-only)")
    df, PARAMS = _aqs_annual()
    cx, cy = aoi["centroid"]
    d = _hav_km(cy, cx, df["lat"].values, df["lon"].values)
    m = df.assign(km=d).sort_values("km")
    sites = m.drop_duplicates("site_id")
    f = {"aqs_nearest_monitor_km": round(float(sites["km"].iloc[0]), 2),
         "aqs_nearest_site_id": str(sites["site_id"].iloc[0]),
         "aqs_nearest_site_name": str(sites["site_name"].iloc[0])[:48],
         "aqs_sites_25km": int((sites["km"] <= 25).sum()),
         "aqs_sites_50km": int((sites["km"] <= 50).sum()),
         "aqs_year": int(df["year"].iloc[0])}
    near = m[(m["km"] <= 50) & m["mean"].notna() & (m["obs_pct"] >= 50)]
    for pc, (nm, unit) in PARAMS.items():
        sub = near[near["pcode"] == pc]
        if sub.empty:
            continue
        f[f"aqs_{nm}_{unit}_mean"] = round(float(sub["mean"].iloc[0]), 5)
        f[f"aqs_{nm}_src_km"] = round(float(sub["km"].iloc[0]), 2)
    return f, {"obs_date": str(int(df["year"].iloc[0])), "res_m": "point",
               "item": f"EPA AirData annual_conc_by_monitor_{int(df['year'].iloc[0])}"}


CAMS_VARS = ["pm2_5", "pm10", "carbon_monoxide", "nitrogen_dioxide",
             "sulphur_dioxide", "ozone", "aerosol_optical_depth"]
CAMS_SHORT = {"pm2_5": "pm25_ugm3", "pm10": "pm10_ugm3", "carbon_monoxide": "co_ugm3",
              "nitrogen_dioxide": "no2_ugm3", "sulphur_dioxide": "so2_ugm3",
              "ozone": "o3_ugm3", "aerosol_optical_depth": "aod550"}


def _cams_at(lat, lon, past_days=30):
    r = S.get("https://air-quality-api.open-meteo.com/v1/air-quality",
              params={"latitude": round(lat, 4), "longitude": round(lon, 4),
                      "hourly": ",".join(CAMS_VARS), "domains": "cams_global",
                      "past_days": past_days, "forecast_days": 0, "timezone": "UTC"},
              timeout=60)
    r.raise_for_status()
    return r.json()


def _a_cams(aoi):
    cx, cy = aoi["centroid"]
    j = _cams_at(cy, cx)
    h = j.get("hourly") or {}
    t = h.get("time") or []
    if not t:
        raise LookupError("open-meteo air-quality returned an empty hourly series")
    f = {}
    for v, short in CAMS_SHORT.items():
        a = np.array([x for x in (h.get(v) or []) if x is not None], dtype="f8")
        if not a.size:
            continue
        f[f"cams_{short}_mean"] = round(float(a.mean()), 3)
        f[f"cams_{short}_p95"] = round(float(np.percentile(a, 95)), 3)
    if not f:
        raise LookupError("open-meteo air-quality returned no valid values")
    f["cams_n_hours"] = int(len(t))
    f["cams_grid_lat"] = round(float(j.get("latitude")), 3)
    f["cams_grid_lon"] = round(float(j.get("longitude")), 3)
    return f, {"obs_date": f"{t[0][:10]}/{t[-1][:10]}", "res_m": 44000,
               "item": "CAMS global reanalysis/forecast via open-meteo"}


# =============================================================== emissions
NPRI_COLS = {"Année / Year": "year", "ID INRP / NPRI ID": "npri_id",
             "Nom de l'installation / Facility Name": "facility",
             "Province / Province": "prov",
             "Latitude / Latitude": "lat", "Longitude / Longitude": "lon",
             "Nom de secteur SCIAN 4 (Anglais) / NAICS 4 Sector Name (English)": "sector",
             "No CAS / CAS Number": "cas",
             "Nom de substance (Anglais) / Substance Name (English)": "substance",
             "Unités / Units": "units"}
NPRI_SUMS = {"Émissions à l'air - Totale / Air Emissions - Total": "air",
             "Rejets à l'eau - Totale / Water Releases - Total": "water",
             "Rejets au sol - Totalee / Land Releases - Total": "land"}
_TO_KG = {"tonnes": 1000.0, "kg": 1.0, "grams": 0.001}


def _npri():
    """NPRI single-year facility x substance release table. ~39 MB CSV -> ~6 MB parquet."""
    def build():
        pq = os.path.join(CACHE, "npri_releases.parquet")
        if os.path.exists(pq):
            return pd.read_parquet(pq)
        r = S.get(ECCC_LS, params={"path": NPRI_DIR}, timeout=90)
        r.raise_for_status()
        names = [e["name"] for e in r.json().get("path_contents", [])
                 if e["name"].endswith(".csv") and "DataDonn" in e["name"]]
        if not names:
            raise LookupError("no NPRI-INRP_DataDonnees_<year>.csv in the NPRI directory")
        name = sorted(names)[-1]
        raw = os.path.join(CACHE, "_npri_raw.csv")
        _eccc_get(f"{NPRI_DIR}/{name}", dest=raw)
        want = list(NPRI_COLS) + list(NPRI_SUMS)
        df = pd.read_csv(raw, encoding="latin-1", low_memory=False, usecols=want)
        df = df.rename(columns={**NPRI_COLS, **NPRI_SUMS})
        for c in ("lat", "lon", "air", "water", "land"):
            df[c] = pd.to_numeric(df[c], errors="coerce")
        df = df.dropna(subset=["lat", "lon"])
        u = df["units"].astype(str).str.strip().str.lower()
        df["kg_factor"] = u.map(_TO_KG)
        df["is_teq"] = u.str.contains("teq")
        df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
        df = df[["year", "npri_id", "facility", "prov", "lat", "lon", "sector",
                 "cas", "substance", "units", "kg_factor", "is_teq",
                 "air", "water", "land"]]
        df.to_parquet(pq + ".part")
        os.replace(pq + ".part", pq)
        try:
            os.remove(raw)
        except OSError:
            pass
        return df
    return _once("npri", build)


# the criteria air contaminants, reported by name in NPRI
NPRI_CAC = {"pm25": "PM2.5 - Particulate Matter <= 2.5 Micrometers",
            "pm10": "PM10 - Particulate Matter <= 10 Micrometers",
            "so2": "Sulphur dioxide", "nox": "Nitrogen oxides (expressed as nitrogen dioxide)",
            "voc": "Volatile Organic Compounds (VOCs)", "co": "Carbon monoxide"}


def _npri_features(lat, lon, df, buf_km=BUF_KM):
    d = _hav_km(lat, lon, df["lat"].values, df["lon"].values)
    w = df.assign(km=d)
    fac = w.drop_duplicates("npri_id").sort_values("km")
    yr = int(w["year"].dropna().max()) if w["year"].notna().any() else None
    f = {"npri_nearest_facility_km": round(float(fac["km"].iloc[0]), 2),
         "npri_nearest_facility_name": str(fac["facility"].iloc[0])[:56],
         "npri_nearest_facility_sector": str(fac["sector"].iloc[0])[:48],
         "npri_facilities_25km": int((fac["km"] <= buf_km).sum()),
         "npri_facilities_10km": int((fac["km"] <= 10).sum()),
         "npri_year": yr}
    near = w[w["km"] <= buf_km]
    mass = near[near["kg_factor"].notna() & ~near["is_teq"]].copy()
    if mass.empty:
        f["npri_substances_25km"] = 0
        return f
    for col in ("air", "water", "land"):
        mass[col + "_kg"] = mass[col].fillna(0.0) * mass["kg_factor"]
    f["npri_air_releases_kg_25km"] = round(float(mass["air_kg"].sum()), 1)
    f["npri_water_releases_kg_25km"] = round(float(mass["water_kg"].sum()), 1)
    f["npri_land_releases_kg_25km"] = round(float(mass["land_kg"].sum()), 1)
    f["npri_total_releases_kg_25km"] = round(
        float(mass[["air_kg", "water_kg", "land_kg"]].sum().sum()), 1)
    f["npri_substances_25km"] = int(mass["substance"].nunique())
    tot = (mass.assign(t=mass[["air_kg", "water_kg", "land_kg"]].sum(axis=1))
               .groupby("substance")["t"].sum().sort_values(ascending=False))
    if len(tot):
        f["npri_top_substance"] = str(tot.index[0])[:56]
        f["npri_top_substance_kg_25km"] = round(float(tot.iloc[0]), 1)
    for k, nm in NPRI_CAC.items():
        v = tot.get(nm)
        if v is not None:
            f[f"npri_{k}_kg_25km"] = round(float(v), 1)
    return f


def _a_npri(aoi):
    if aoi["country"] != "CA":
        raise PermissionError("out of coverage (NPRI is Canada-only)")
    df = _npri()
    cx, cy = aoi["centroid"]
    f = _npri_features(cy, cx, df)
    yr = f.get("npri_year")
    return f, {"obs_date": str(yr) if yr else "unknown", "res_m": "point",
               "item": f"NPRI reporting year {yr}"}


def _ghgrp():
    """ECCC GHGRP facility greenhouse-gas emissions, 2004-present. ~10 MB CSV."""
    def build():
        pq = os.path.join(CACHE, "ghgrp.parquet")
        if os.path.exists(pq):
            return pd.read_parquet(pq)
        r = S.get(ECCC_LS, params={"path": GHGRP_DIR}, timeout=90)
        r.raise_for_status()
        names = [e["name"] for e in r.json().get("path_contents", [])
                 if e["name"].endswith(".csv") and "GHGEmissionsGES" in e["name"]]
        if not names:
            raise LookupError("no PDGES-GHGRP-GHGEmissionsGES*.csv in the GHGRP directory")
        raw = _eccc_get(f"{GHGRP_DIR}/{sorted(names)[-1]}",
                        dest=os.path.join(CACHE, "_ghgrp_raw.csv"))
        hdr = pd.read_csv(raw, encoding="latin-1", nrows=0).columns.tolist()

        def col(sub):
            for c in hdr:
                if sub.lower() in c.lower():
                    return c
            raise LookupError(f"GHGRP column matching {sub!r} not found")
        cmap = {col("Reference Year"): "year", col("Facility Name"): "facility",
                col("Latitude"): "lat", col("Longitude"): "lon",
                col("Facility NAICS Code Description"): "sector",
                col("TOTAL EMISSIONS (tonnes CO2e"): "co2e_t"}
        df = pd.read_csv(raw, encoding="latin-1", low_memory=False,
                         usecols=list(cmap)).rename(columns=cmap)
        for c in ("lat", "lon", "co2e_t", "year"):
            df[c] = pd.to_numeric(df[c], errors="coerce")
        df = df.dropna(subset=["lat", "lon", "year"])
        df["year"] = df["year"].astype(int)
        df = df[df["year"] == df["year"].max()]
        df.to_parquet(pq + ".part")
        os.replace(pq + ".part", pq)
        try:
            os.remove(raw)
        except OSError:
            pass
        return df
    return _once("ghgrp", build)


def _ghgrp_features(lat, lon, df, buf_km=BUF_KM):
    d = _hav_km(lat, lon, df["lat"].values, df["lon"].values)
    w = df.assign(km=d).sort_values("km")
    near = w[w["km"] <= buf_km]
    f = {"ghgrp_nearest_facility_km": round(float(w["km"].iloc[0]), 2),
         "ghgrp_nearest_facility_name": str(w["facility"].iloc[0])[:56],
         "ghgrp_facilities_25km": int(len(near)),
         "ghgrp_year": int(df["year"].iloc[0])}
    if len(near):
        f["ghgrp_co2e_kt_25km"] = round(float(near["co2e_t"].fillna(0).sum()) / 1000.0, 2)
        f["ghgrp_max_facility_co2e_kt"] = round(float(near["co2e_t"].max()) / 1000.0, 2)
        f["ghgrp_top_sector_25km"] = str(
            near.groupby("sector")["co2e_t"].sum().sort_values(ascending=False).index[0])[:48]
    else:
        f["ghgrp_co2e_kt_25km"] = 0.0
    return f


def _a_ghgrp(aoi):
    if aoi["country"] != "CA":
        raise PermissionError("out of coverage (ECCC GHGRP is Canada-only)")
    df = _ghgrp()
    cx, cy = aoi["centroid"]
    f = _ghgrp_features(cy, cx, df)
    return f, {"obs_date": str(int(df["year"].iloc[0])), "res_m": "point",
               "item": f"ECCC GHGRP {int(df['year'].iloc[0])}"}


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


def _a_echo_tri(aoi):
    """US emissions-source presence from ECHO, at NPRI-comparable buffers.

    These are REPORTER COUNTS, not release quantities.  TRI release tonnages are
    published only through data.epa.gov (Envirofacts), which is not reachable from
    this sandbox and whose lat/long operators a sister lane measured as
    non-filtering (byte-identical payloads for an AOI and a mid-Pacific control).
    """
    if aoi["country"] != "US":
        raise PermissionError("out of coverage (EPA ECHO/TRI is US-only)")
    f = {}
    for km in (25.0, 10.0):
        res = _echo_box(_pad_bbox(aoi["bbox"], km))
        tag = f"{int(km)}km"
        for k, nm in (("TRIRows", "tri_reporters"), ("CAARows", "caa_air_sources"),
                      ("RCRRows", "rcra_handlers"), ("QueryRows", "regulated_facilities")):
            try:
                f[f"echotri_{nm}_{tag}"] = int(str(res.get(k, "")).replace(",", ""))
            except ValueError:
                continue
    if not f:
        raise LookupError("ECHO returned no countable rows at either buffer")
    if f.get("echotri_regulated_facilities_25km"):
        f["echotri_tri_frac_of_regulated_25km"] = round(
            f.get("echotri_tri_reporters_25km", 0)
            / f["echotri_regulated_facilities_25km"], 5)
    f["echotri_density_per_km2_25km"] = round(
        f.get("echotri_regulated_facilities_25km", 0)
        / max(aoi["area_km2"] + 4 * 25 * math.sqrt(max(aoi["area_km2"], 1)), 1.0), 4)
    return f, {"obs_date": "current snapshot", "res_m": "vector",
               "item": "ECHO bbox reporter counts at 25 km / 10 km buffers"}


# =============================================================== compliance
_ECHO_MAP = {"QueryRows": "echo_facilities", "SVRows": "echo_significant_violations",
             "CVRows": "echo_current_violations", "V3Rows": "echo_violations_3yr",
             "FEARows": "echo_formal_actions_5yr", "InfFEARows": "echo_informal_actions_5yr",
             "INSPRows": "echo_inspections_5yr", "CAARows": "echo_caa_facilities",
             "CWARows": "echo_cwa_facilities", "RCRRows": "echo_rcra_facilities",
             "TRIRows": "echo_tri_facilities", "IndianCountryRows": "echo_tribal_facilities"}


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


def _echo_features(res):
    f = {}
    for k, nm in _ECHO_MAP.items():
        v = res.get(k)
        if v is None:
            continue
        try:
            f[nm] = int(str(v).replace(",", ""))
        except ValueError:
            continue
    pen = str(res.get("TotalPenalties") or "").replace("$", "").replace(",", "")
    try:
        f["echo_penalties_usd_5yr"] = float(pen)
    except ValueError:
        pass
    if f.get("echo_facilities"):
        f["echo_insp_per_facility"] = round(f.get("echo_inspections_5yr", 0)
                                            / f["echo_facilities"], 4)
        f["echo_sig_violation_rate"] = round(f.get("echo_significant_violations", 0)
                                             / f["echo_facilities"], 5)
    return f


def _a_echo(aoi):
    if aoi["country"] != "US":
        raise PermissionError("out of coverage (EPA ECHO is US-only)")
    bbox = _pad_bbox(aoi["bbox"], 2.0)
    res = _echo_box(bbox)
    f = _echo_features(res)
    if not f:
        raise LookupError("ECHO returned no countable rows")
    f["echo_bbox_pad_km"] = 2.0
    return f, {"obs_date": "5-year compliance window, current snapshot",
               "res_m": "vector", "item": "ECHO " + str(res.get("Version"))[:40]}


# =============================================================== contaminated sites
def _a_frs_sems(aoi):
    if aoi["country"] != "US":
        raise PermissionError("out of coverage (EPA SEMS/Superfund is US-only)")
    cx, cy = aoi["centroid"]
    fac = _frs(cy, cx, 16, acrnm="SEMS")       # 16 mi ~ 25 km
    if not fac:
        return ({"sems_sites_25km": 0, "sems_sites_10km": 0, "sems_nearest_km": None},
                {"obs_date": "current", "res_m": "point",
                 "note": "verified absence: FRS returned no SEMS site within 16 mi"})
    d = _hav_km(cy, cx, [x["lat"] for x in fac], [x["lon"] for x in fac])
    o = np.argsort(d)
    return ({"sems_sites_25km": int((d <= 25).sum()),
             "sems_sites_10km": int((d <= 10).sum()),
             "sems_sites_5km": int((d <= 5).sum()),
             "sems_nearest_km": round(float(d[o[0]]), 2),
             "sems_nearest_name": str(fac[o[0]]["name"])[:56]},
            {"obs_date": "current", "res_m": "point", "item": "EPA FRS pgm=SEMS"})


def _fcsi():
    """Federal Contaminated Sites Inventory. 8 MB zip -> 287 MB XML, streamed once
    into a ~1 MB parquet of georeferenced sites."""
    def build():
        pq = os.path.join(CACHE, "fcsi_sites.parquet")
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
                loc = el.find("Location")
                lat = loc.findtext("Latitude") if loc is not None else None
                lon = loc.findtext("Longitude") if loc is not None else None
                if lat and lon:
                    st = el.find("SiteStatus/Status/EN")
                    cl = el.find("Classification")
                    ce = el.find("ContaminationDetails/ContaminationEstimates")
                    conts = {c.findtext("EN") for c in
                             el.findall("ContaminationDetails/ContaminatedMedia/Contamination")}
                    try:
                        rows.append({
                            "site_id": el.get("FederalSiteIdentifier"),
                            "org": el.get("ReportingOrganization"),
                            "name": (el.findtext("Name/EN") or "")[:80],
                            "status": (st.text if st is not None else "") or "",
                            "class_code": (cl.findtext("Code") if cl is not None else "") or "",
                            "lat": float(lat), "lon": float(lon),
                            "prov": (loc.findtext("Province") or ""),
                            "cubic_m": float(ce.findtext("CubicMetres") or 0) if ce is not None else 0.0,
                            "hectares": float(ce.findtext("Hectares") or 0) if ce is not None else 0.0,
                            "n_contaminants": len([c for c in conts if c]),
                            "contaminants": "; ".join(sorted(c for c in conts if c))[:120],
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


def _fcsi_features(lat, lon, df, buf_km=BUF_KM):
    d = _hav_km(lat, lon, df["lat"].values, df["lon"].values)
    w = df.assign(km=d).sort_values("km")
    near = w[w["km"] <= buf_km]
    f = {"fcsi_nearest_site_km": round(float(w["km"].iloc[0]), 2),
         "fcsi_nearest_site_name": str(w["name"].iloc[0])[:56],
         "fcsi_nearest_site_status": str(w["status"].iloc[0])[:24],
         "fcsi_nearest_site_org": str(w["org"].iloc[0])[:8],
         "fcsi_sites_25km": int(len(near)),
         "fcsi_sites_10km": int((w["km"] <= 10).sum()),
         "fcsi_sites_5km": int((w["km"] <= 5).sum())}
    if len(near):
        f["fcsi_active_sites_25km"] = int((near["status"].str.lower() == "active").sum())
        f["fcsi_closed_sites_25km"] = int((near["status"].str.lower() == "closed").sum())
        f["fcsi_high_priority_sites_25km"] = int((near["class_code"] == "1").sum())
        f["fcsi_contaminated_soil_m3_25km"] = round(float(near["cubic_m"].sum()), 1)
        f["fcsi_contaminated_area_ha_25km"] = round(float(near["hectares"].sum()), 3)
        f["fcsi_reporting_orgs_25km"] = int(near["org"].nunique())
    return f


def _a_fcsi(aoi):
    if aoi["country"] != "CA":
        raise PermissionError("out of coverage (FCSI is Canada-only)")
    df = _fcsi()
    cx, cy = aoi["centroid"]
    f = _fcsi_features(cy, cx, df)
    return f, {"obs_date": "current inventory snapshot", "res_m": "point",
               "item": "TBS FCSI fcsi-rscf.xml"}


# =============================================================== disease surveillance
NNDSS_WATCH = ["Lyme disease", "Salmonellosis", "Pertussis", "Hepatitis C, Acute",
               "Campylobacteriosis", "Legionellosis", "Tuberculosis"]


def _a_nndss(aoi):
    """CDC NNDSS notifiable-disease counts, joined at STATE level.

    This is a state-wide statistic, not an AOI measurement: res_m='admin:state'.
    """
    if aoi["country"] != "US":
        raise PermissionError("out of coverage (CDC NNDSS is US-only)")
    cx, cy = aoi["centroid"]
    a = _admin1_at(cx, cy)
    st = a["name"]
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
    rows = r.json()
    if not rows:
        raise LookupError(f"NNDSS returned no rows for {st} {yr}w{wk}")
    df = pd.DataFrame(rows)
    for c in ("m1", "m3", "m4"):
        df[c] = pd.to_numeric(df.get(c), errors="coerce")
    f = {"nndss_state": st, "nndss_year": int(yr), "nndss_week": int(wk),
         "nndss_conditions_tabulated": int(df["label"].nunique()),
         "nndss_conditions_with_cases_ytd": int((df["m3"].fillna(0) > 0).sum()),
         "nndss_total_cases_ytd": round(float(df["m3"].fillna(0).sum()), 1),
         "nndss_total_cases_prev_year_ytd": round(float(df["m4"].fillna(0).sum()), 1),
         "nndss_cases_current_week": round(float(df["m1"].fillna(0).sum()), 1)}
    g = df.groupby("label")["m3"].sum().sort_values(ascending=False)
    if len(g):
        f["nndss_top_condition"] = str(g.index[0])[:56]
        f["nndss_top_condition_cases_ytd"] = round(float(g.iloc[0]), 1)
    for nm in NNDSS_WATCH:
        v = g.get(nm)
        if v is not None and not pd.isna(v):
            f["nndss_" + nm.split(",")[0].lower().replace(" ", "_") + "_ytd"] = round(float(v), 1)
    return f, {"obs_date": f"{yr}-W{wk}", "res_m": "admin:state",
               "item": f"CDC NNDSS x9gk-5huc / {st}"}


def _rvdss():
    def build():
        r = S.get(f"{INFOBASE}/respiratory-virus-detections/RVD_WeeklyData.csv", timeout=120)
        r.raise_for_status()
        df = pd.read_csv(io.BytesIO(r.content))
        for c in ("tests", "detections", "percentpositive", "week", "year"):
            if c in df:
                df[c] = pd.to_numeric(df[c], errors="coerce")
        return df
    return _once("rvdss", build)


def _a_rvdss(aoi):
    """PHAC Respiratory Virus Detection Surveillance, joined at PROVINCE level
    (reported by province; res_m='admin:province'). Not an AOI measurement."""
    if aoi["country"] != "CA":
        raise PermissionError("out of coverage (PHAC RVDSS is Canada-only)")
    cx, cy = aoi["centroid"]
    a = _admin1_at(cx, cy)
    df = _rvdss()
    sub = df[df["province"].astype(str).map(_fold) == _fold(a["name"])]
    if sub.empty:
        reg = RVDSS_REGION.get(a["postal"])
        sub = df[df["region"].astype(str) == reg] if reg else sub
        level = "admin:region"
    else:
        level = "admin:province"
    if sub.empty:
        raise LookupError(f"PHAC RVDSS has no rows for province '{a['name']}'")
    sub = sub.sort_values(["year", "week"])
    last = sub.iloc[-1]
    latest = sub[(sub["year"] == last["year"]) & (sub["week"] == last["week"])]
    f = {"rvdss_jurisdiction": str(last.get("province") or last.get("region"))[:40],
         "rvdss_week": f"{int(last['year'])}-W{int(last['week'])}",
         "rvdss_viruses_reported": int(latest["virus"].nunique()),
         "rvdss_tests_latest_week": round(float(pd.to_numeric(latest["tests"],
                                          errors="coerce").fillna(0).sum()), 0),
         "rvdss_detections_latest_week": round(float(pd.to_numeric(latest["detections"],
                                               errors="coerce").fillna(0).sum()), 0)}
    for v, nm in (("SARS-CoV-2", "sarscov2"), ("Influenza", "influenza"), ("RSV", "rsv")):
        s = latest[latest["virus"].astype(str).str.contains(v, case=False, na=False)]
        if not s.empty:
            f[f"rvdss_{nm}_pct_positive"] = round(float(
                pd.to_numeric(s["percentpositive"], errors="coerce").mean()), 2)
    return f, {"obs_date": f["rvdss_week"], "res_m": level,
               "item": "PHAC RVD_WeeklyData.csv"}


# =============================================================== wastewater
def _a_nwss(aoi):
    """CDC National Wastewater Surveillance System, joined at COUNTY level
    (res_m='admin:county'). A sewershed statistic, not an AOI measurement."""
    if aoi["country"] != "US":
        raise PermissionError("out of coverage (CDC NWSS is US-only)")
    cx, cy = aoi["centroid"]
    co = _us_county(cx, cy)
    u = f"{CDC}/2ew6-ywp6.json"
    r = S.get(u, params={"$where": f"county_fips like '%{co['fips']}%'",
                         "$order": "date_end DESC", "$limit": 2000}, timeout=90)
    r.raise_for_status()
    rows = r.json()
    if not rows:
        # NWSS covers participating sewersheds only; rural counties often have none.
        return ({"nwss_county": co["name"], "nwss_county_fips": co["fips"],
                 "nwss_sites_county": 0, "nwss_records_county": 0},
                {"obs_date": "current", "res_m": "admin:county",
                 "note": "verified absence: no NWSS sewershed reports for this county"})
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
    return f, {"obs_date": str(latest_end)[:10], "res_m": "admin:county",
               "item": f"CDC NWSS 2ew6-ywp6 / {co['name']} County ({co['fips']})"}


def _phac_ww():
    def build():
        r = S.get(f"{INFOBASE}/covidLive/wastewater/covid19-wastewater.csv", timeout=180)
        r.raise_for_status()
        df = pd.read_csv(io.BytesIO(r.content))
        for c in ("viral_load", "seven_day_rolling_avg", "pruid"):
            df[c] = pd.to_numeric(df.get(c), errors="coerce")
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        return df.dropna(subset=["Date", "pruid"])
    return _once("phac_ww", build)


def _a_phac_ww(aoi):
    """PHAC SARS-CoV-2 wastewater monitoring, joined at PROVINCE level
    (res_m='admin:province'). Sites are named, not georeferenced, in the open file."""
    if aoi["country"] != "CA":
        raise PermissionError("out of coverage (PHAC wastewater is Canada-only)")
    cx, cy = aoi["centroid"]
    a = _admin1_at(cx, cy)
    pr = PRUID.get(a["postal"])
    if pr is None:
        raise LookupError(f"no pruid mapping for province postal '{a['postal']}'")
    df = _phac_ww()
    sub = df[df["pruid"] == pr]
    if sub.empty:
        raise LookupError(f"PHAC wastewater has no sites for pruid {pr} ({a['name']})")
    last = sub["Date"].max()
    cur = sub[sub["Date"] == last]
    f = {"phacww_province": a["name"], "phacww_pruid": int(pr),
         "phacww_sites_province": int(sub["Location"].nunique()),
         "phacww_sites_latest_date": int(cur["Location"].nunique()),
         "phacww_latest_date": last.date().isoformat(),
         "phacww_viral_load_latest": round(float(cur["viral_load"].mean()), 4),
         "phacww_7day_avg_latest": round(float(cur["seven_day_rolling_avg"].mean()), 4),
         "phacww_records_province": int(len(sub))}
    hist = sub[sub["Date"] >= last - pd.Timedelta(days=365)]["seven_day_rolling_avg"]
    if hist.notna().any():
        f["phacww_7day_avg_p90_1yr"] = round(float(np.nanpercentile(hist.values, 90)), 4)
        f["phacww_latest_vs_1yr_p90"] = round(
            float(f["phacww_7day_avg_latest"] / max(f["phacww_7day_avg_p90_1yr"], 1e-9)), 3)
    return f, {"obs_date": f["phacww_latest_date"], "res_m": "admin:province",
               "item": "PHAC covid19-wastewater.csv"}


# =============================================================== registration
# Mid-Pacific geometry control, box [-140,-20,-139,-19] / centroid (-139.5,-19.5).
# Measured once per source against a real AOI; every source below filters.
CONTROL_DIFFERENTIALS = {
 "eccc_naps":       "Toronto nearest_station_km=0.71 stations_25km=7 no2=14.0 ppb  |  ctrl 7718.64 / 0 / absent",
 "epa_aqs_airdata": "Detroit nearest_monitor_km=4.06 sites_25km=11 pm25=9.655      |  ctrl 4632.46 / 0 / absent",
 "cams_open_meteo": "Toronto no2=13.674 pm25=6.254 aod=0.120 ug/m3                |  ctrl no2=0.0 pm25=8.282 aod=0.101",
 "eccc_npri":       "Sudbury nearest_facility_km=0.43 fac_25km=19 total=4.84e7 kg  |  ctrl 7720.30 / 0 / absent",
 "eccc_ghgrp":      "Syncrude nearest_km=4.64 fac_25km=5 co2e=22136 kt             |  ctrl 7720.74 / 0 / 0.0",
 "epa_echo_tri":    "Detroit tri_reporters_25km=118 regulated_25km=15182           |  ctrl 0 / 0",
 "epa_echo":        "Detroit facilities=978 inspections=50 penalties=$7.55M        |  ctrl 0 / 0 / $0",
 "cdc_nndss":       "Detroit -> Michigan, total_cases_ytd=47635                    |  ctrl raises LookupError (no admin1 polygon)",
 "phac_rvdss":      "Toronto -> Ontario, tests_latest_week=58173                   |  ctrl raises LookupError (no admin1 polygon)",
 "cdc_nwss":        "Detroit -> Wayne County 26163, sites=2                        |  ctrl raises LookupError (TIGERweb: no county)",
 "phac_wastewater": "Montreal -> Quebec pruid=24, sites=2                          |  ctrl raises LookupError (no admin1 polygon)",
 "epa_frs_sems":    "Bingham nearest_km=0.14 sites_25km=64 sites_5km=3             |  ctrl absent / 0 / absent",
 "tbs_fcsi":        "Montreal nearest_km=0.21 sites_25km=304 active_25km=119       |  ctrl 6699.87 / 0 / absent",
}

# slot, source_id, family, licence, auth, fn
ADAPTERS = [
 ("air_quality_monitors",   "eccc_naps",      "health_env", "OGL-Canada-2.0",            "none", _a_naps),
 ("air_quality_monitors",   "epa_aqs_airdata","health_env", "public-domain (US EPA)",    "none", _a_aqs),
 ("air_quality_modelled",   "cams_open_meteo","health_env", "CC-BY-4.0 (CAMS/ECMWF)",    "none", _a_cams),
 ("emissions_releases",     "eccc_npri",      "health_env", "OGL-Canada-2.0",            "none", _a_npri),
 ("emissions_releases",     "eccc_ghgrp",     "health_env", "OGL-Canada-2.0",            "none", _a_ghgrp),
 ("emissions_releases",     "epa_echo_tri",   "health_env", "public-domain (US EPA)",    "none", _a_echo_tri),
 ("facility_compliance",    "epa_echo",       "health_env", "public-domain (US EPA)",    "none", _a_echo),
 ("disease_surveillance",   "cdc_nndss",      "health_env", "public-domain (US CDC)",    "none", _a_nndss),
 ("disease_surveillance",   "phac_rvdss",     "health_env", "OGL-Canada-2.0",            "none", _a_rvdss),
 ("wastewater_surveillance","cdc_nwss",       "health_env", "public-domain (US CDC)",    "none", _a_nwss),
 ("wastewater_surveillance","phac_wastewater","health_env", "OGL-Canada-2.0",            "none", _a_phac_ww),
 ("contaminated_sites",     "epa_frs_sems",   "health_env", "public-domain (US EPA)",    "none", _a_frs_sems),
 ("contaminated_sites",     "tbs_fcsi",       "health_env", "OGL-Canada-2.0",            "none", _a_fcsi),
]

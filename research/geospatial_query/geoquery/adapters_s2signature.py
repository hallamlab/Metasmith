"""Deterministic spectral-temporal signature — a globally-complete embedding floor.

Why this exists: the `embeddings` slot is the join key for analogue fills, so partial coverage
defeats its purpose. Published foundation-model embedding archives are sparse (Clay v0 publishes
1170 of ~57k land MGRS tiles) or auth-gated (AlphaEarth via Earth Engine). Sentinel-2 L2A, by
contrast, reaches every AOI in the test set.

This adapter compresses one year of Sentinel-2 into a fixed 30-dimensional, L2-normalised vector:
per-band temporal quantiles plus index medians, amplitudes and within-AOI texture. It is NOT a
learned embedding and must never be labelled as one — it carries no SAR, climate or text targets,
and it cannot decode anything. What it does provide is a reproducible, globally-defined coordinate
system in which two AOIs can be compared, which is the minimum the donor-matching design requires.

Fill type stays `observed`: every component is a statistic of measured reflectance.
"""
import os, sys, math, threading, concurrent.futures as cf
import numpy as np

from .pipeline import pc_item, read_window, _dt   # noqa: E402

BANDS = ["B02", "B03", "B04", "B08", "B11", "B12"]
YEAR = "2024"
N_SCENES = 4          # seasonal sample; deterministic pick
MAXPIX = 48           # per-scene decimated window
_LOCK = threading.Lock()

# Reference statistics over 28 Sentinel-2 boxes spread across North American land
# (seed 11, 2 scenes each). Used to z-score components before similarity: cosine on raw
# positive reflectance features is dominated by the shared mean direction and does not
# separate land-cover types (measured: within-class 0.9453 vs between-class 0.9450).
REF_NAMES = ['b02_med', 'b02_p10', 'b02_p90', 'b03_med', 'b03_p10', 'b03_p90', 'b04_med', 'b04_p10', 'b04_p90', 'b08_med', 'b08_p10', 'b08_p90', 'b11_med', 'b11_p10', 'b11_p90', 'b12_med', 'b12_p10', 'b12_p90', 'brightness_med', 'brightness_sd', 'nbr_amp', 'nbr_med', 'ndbi_amp', 'ndbi_med', 'ndvi_amp', 'ndvi_med', 'ndvi_spatial_sd', 'ndwi_amp', 'ndwi_med', 'swir_vis_ratio']
REF_MU = [0.236616, 0.174186, 0.338513, 0.252735, 0.188653, 0.355499, 0.259053, 0.186992, 0.371159, 0.358038, 0.264317, 0.472615, 0.282106, 0.221339, 0.34698, 0.24064, 0.186657, 0.29912, 0.266878, 0.116896, 0.140303, 0.17931, 0.15229, -0.10445, 0.124093, 0.189448, 0.034073, 0.107812, -0.197705, 1.457693]
REF_SD = [0.14315, 0.091632, 0.223625, 0.139546, 0.089518, 0.216129, 0.144786, 0.098729, 0.214129, 0.123777, 0.10074, 0.170365, 0.095237, 0.090107, 0.101513, 0.084068, 0.068592, 0.089927, 0.114397, 0.063529, 0.159974, 0.165551, 0.187674, 0.170242, 0.115453, 0.109136, 0.022132, 0.098327, 0.114041, 0.621895]
REF_N = 28



def _scenes(aoi):
    """Up to N_SCENES least-cloudy scenes, one per season, deterministic."""
    items = pc_item("sentinel-2-l2a", aoi["bbox"], f"{YEAR}-01-01/{YEAR}-12-31",
                    {"eo:cloud_cover": {"lt": 40}}, limit=120)
    if not items:
        items = pc_item("sentinel-2-l2a", aoi["bbox"], f"{int(YEAR)-1}-01-01/{YEAR}-12-31",
                        {"eo:cloud_cover": {"lt": 60}}, limit=120)
    if not items:
        raise LookupError("no Sentinel-2 scene in 2 years under 60% cloud")
    by_season = {}
    for it in items:
        q = (int(_dt(it)[5:7]) - 1) // 3
        cur = by_season.get(q)
        if cur is None or it.properties.get("eo:cloud_cover", 100) < cur.properties.get("eo:cloud_cover", 100):
            by_season[q] = it
    picked = [by_season[q] for q in sorted(by_season)]
    if len(picked) < N_SCENES:                      # backfill with next-cleanest scenes
        rest = sorted((i for i in items if i not in picked),
                      key=lambda i: (i.properties.get("eo:cloud_cover", 100), _dt(i)))
        picked += rest[: N_SCENES - len(picked)]
    return sorted(picked, key=_dt)[:N_SCENES]


def _stack(aoi, item):
    """One scene -> dict band -> 1-D array of reflectance (0-1), AOI-clipped."""
    out = {}
    for b in BANDS:
        if b not in item.assets:
            continue
        a = read_window(item.assets[b].href, aoi["bbox"], aoi["geom"], maxpix=MAXPIX)
        v = a.compressed().astype("float64") / 10000.0
        v = v[(v > 0) & (v < 1.6)]
        if v.size:
            out[b] = v
    return out


def _ix(d, a, b):
    """Normalised difference between two bands, on their common quantile grid."""
    if a not in d or b not in d:
        return None
    n = min(d[a].size, d[b].size)
    if n < 4:
        return None
    qa = np.quantile(d[a], np.linspace(0.05, 0.95, n))
    qb = np.quantile(d[b], np.linspace(0.05, 0.95, n))
    return (qa - qb) / (qa + qb + 1e-9)


def _a_s2_signature(aoi):
    items = _scenes(aoi)
    with cf.ThreadPoolExecutor(4) as ex:
        scenes = [s for s in ex.map(lambda it: _stack(aoi, it), items) if s]
    scenes = [s for s in scenes if len(s) >= 4]
    if not scenes:
        raise LookupError("no usable Sentinel-2 pixels in any sampled scene")

    comp, dims = {}, []
    # per-band temporal median of within-AOI p10/median/p90  (18 dims)
    for b in BANDS:
        per = [s[b] for s in scenes if b in s]
        for qn, q in (("p10", 10), ("med", 50), ("p90", 90)):
            val = float(np.median([np.percentile(v, q) for v in per])) if per else 0.0
            comp[f"{b.lower()}_{qn}"] = val
    # index median + seasonal amplitude  (8 dims)
    for name, (x, y) in {"ndvi": ("B08", "B04"), "ndwi": ("B03", "B08"),
                         "nbr": ("B08", "B12"), "ndbi": ("B11", "B08")}.items():
        meds = [float(np.median(v)) for v in (_ix(s, x, y) for s in scenes) if v is not None]
        comp[f"{name}_med"] = float(np.median(meds)) if meds else 0.0
        comp[f"{name}_amp"] = float(max(meds) - min(meds)) if len(meds) > 1 else 0.0
    # within-AOI texture and brightness  (4 dims)
    nd = [v for v in (_ix(s, "B08", "B04") for s in scenes) if v is not None]
    comp["ndvi_spatial_sd"] = float(np.median([v.std() for v in nd])) if nd else 0.0
    comp["brightness_med"] = float(np.median([np.median(np.concatenate(list(s.values()))) for s in scenes]))
    comp["brightness_sd"] = float(np.median([np.std(np.concatenate(list(s.values()))) for s in scenes]))
    comp["swir_vis_ratio"] = float(comp["b11_med"] / (comp["b02_med"] + 1e-9))

    keys = sorted(comp)
    vec = np.array([comp[k] for k in keys], dtype="float64")
    norm = float(np.linalg.norm(vec))

    # Raw components: interpretable, and what a downstream metric (e.g. an ARD-kernel GP that
    # learns its own per-dimension length-scales) should consume.
    feats = {f"s2sig_raw_{k}": round(float(v), 6) for k, v in zip(keys, vec)}

    # Whitened matching vector: z-score against the baked-in reference population, then unit
    # length, so a dot product between two AOIs is a usable similarity out of the box.
    idx = {n: i for i, n in enumerate(REF_NAMES)}
    z = np.array([(comp[k] - REF_MU[idx[k]]) / REF_SD[idx[k]] if k in idx else 0.0 for k in keys])
    zn = float(np.linalg.norm(z))
    unit = z / (zn + 1e-12)
    feats.update({f"s2sig_w{i:02d}_{k}": round(float(u), 6) for i, (k, u) in enumerate(zip(keys, unit))})
    feats["s2sig_norm"] = round(norm, 6)
    feats["s2sig_whitened_norm"] = round(zn, 6)
    feats["s2sig_dim"] = len(keys)
    feats["s2sig_n_scenes"] = len(scenes)
    feats["s2sig_year"] = int(YEAR)
    feats["s2sig_version"] = "s2sig-v2-whitened"
    feats["s2sig_ref_n"] = REF_N
    return feats, {"obs_date": YEAR, "res_m": 10,
                   "item": ";".join(i.id for i in items)[:200],
                   "note": "deterministic spectral-temporal signature; s2sig_w* is the unit-length whitened matching vector, s2sig_raw_* the interpretable components. Not a learned embedding."}


ADAPTERS = [("embeddings", "s2_signature_v1", "embeddings",
             "CC-BY-4.0 (Copernicus, derived)", "none", _a_s2_signature)]

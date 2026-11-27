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
import concurrent.futures as cf
import numpy as np

from .pipeline import pc_item, read_window, s2_offsets, _dt, PC_SEARCH

BANDS = ["B02", "B03", "B04", "B08", "B11", "B12"]
YEAR = "2024"
N_SCENES = 4          # seasonal sample; deterministic pick
MAXPIX = 48           # per-scene decimated window
VERSION = "s2sig-v3 (sentinel-2-l2a, BOA offset applied)"

# Reference statistics over 28 Sentinel-2 0.1-degree boxes on North American land (random
# centres in -125..-65 E, 30..60 N, seed 11, water-dominated boxes rejected), computed by this
# adapter with the BOA offset applied. Used to z-score components before similarity: cosine on
# raw positive reflectance features is dominated by the shared mean direction and does not
# separate land-cover types.
REF_NAMES = ['b02_med', 'b02_p10', 'b02_p90', 'b03_med', 'b03_p10', 'b03_p90', 'b04_med', 'b04_p10', 'b04_p90', 'b08_med', 'b08_p10', 'b08_p90', 'b11_med', 'b11_p10', 'b11_p90', 'b12_med', 'b12_p10', 'b12_p90', 'brightness_med', 'brightness_sd', 'nbr_amp', 'nbr_med', 'ndbi_amp', 'ndbi_med', 'ndvi_amp', 'ndvi_med', 'ndvi_spatial_sd', 'ndwi_amp', 'ndwi_med', 'swir_vis_ratio']
REF_MU = [0.093812, 0.044219, 0.155501, 0.108798, 0.055919, 0.174412, 0.118163, 0.058025, 0.194747, 0.246054, 0.152217, 0.343451, 0.177972, 0.116832, 0.245571, 0.126762, 0.079828, 0.182169, 0.12081, 0.104012, 0.309446, 0.340025, 0.355288, -0.167864, 0.345799, 0.42934, 0.072685, 0.271853, -0.462918, 3.608376]
REF_SD = [0.120266, 0.041955, 0.176321, 0.107877, 0.039725, 0.161619, 0.112334, 0.048526, 0.163725, 0.081754, 0.052132, 0.120616, 0.081659, 0.07691, 0.09292, 0.075812, 0.064485, 0.084705, 0.068874, 0.044045, 0.212035, 0.262618, 0.275038, 0.242055, 0.254649, 0.224684, 0.051646, 0.233551, 0.186961, 2.05436]



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
    """One scene -> dict band -> 1-D array of BOA reflectance, AOI-clipped."""
    out, off = {}, s2_offsets(item)
    for b in BANDS:
        if b not in item.assets:
            continue
        a = read_window(item.assets[b].href, aoi["bbox"], aoi["geom"], maxpix=MAXPIX)
        v = a.compressed().astype("float64")
        v = (v[v < 16000] + off.get(b, 0)) / 10000.0
        v = v[v > 0]
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
    feats.update({f"s2sig_w_{k}": round(float(u), 6) for k, u in zip(keys, unit)})
    feats["s2sig_norm"] = round(norm, 6)
    feats["s2sig_whitened_norm"] = round(zn, 6)
    feats["s2sig_n_scenes"] = len(scenes)
    return feats, {"obs_date": f"{_dt(items[0])[:10]}/{_dt(items[-1])[:10]}", "res_m": 10,
                   "item": ";".join(i.id for i in items), "release": VERSION, "url": PC_SEARCH}


ADAPTERS = [("embeddings", "s2_signature_v1", "embeddings",
             "CC-BY-4.0 (Copernicus, derived)", "none", _a_s2_signature)]

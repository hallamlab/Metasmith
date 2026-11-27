"""adapters_embeddings.py -- the "embeddings" slot, globally complete.

Lane: embeddings (one source: AlphaEarth Foundations / Google Satellite
Embedding V1, read from the keyless Source Cooperative mirror).

Contract matches pipeline.py:  _a_<name>(aoi) -> (features: dict[str, scalar], meta: dict)
  * flat scalars only, source-prefixed lowercase names
  * PermissionError  -> out of coverage      (recorded not-attempted)
  * any other raise   -> unavailable
  * ADAPTERS is a list of (slot, source_id, family, licence, auth, fn)

WHY THIS SOURCE
---------------
The embeddings slot is the pipeline's join key: analogue fills only work if an
embedding exists for EVERY AOI.  The previously shipped source, Clay v0 on
source.coop, publishes 1170 MGRS tiles -- measured here by listing the bucket
(1170 .gpq objects, 1170 distinct MGRS ids).  Point-sampled against 1000
random global land points that is 5.2 % coverage, and it filled 5 of 15 test
AOIs.

AlphaEarth Foundations (Google DeepMind's Satellite Embedding V1) is mirrored
as plain-HTTPS COGs at data.source.coop/tge-labs/aef.  Measured against 6000
area-weighted random global land points using the published index polygons for
the 2024 epoch: 100.00 % of 5535 non-Antarctic points, 83.4 % of 465 Antarctic
points, 98.72 % overall.  Twelve remote spot locations (Sahara, Amazon,
Siberia, Greenland interior, Tibet, outback Australia, Antarctic coast,
Patagonia, Svalbard, Congo, Mongolia, Papua) were opened as real COG reads and
all twelve returned valid unit-norm embeddings.

ARCHIVE LAYOUT AND THE TWO TRAPS
--------------------------------
v1/annual/<year>/<utm_zone>/<ee_image_id>-<yoff>-<xoff>.tiff
  8192 x 8192 px, 64 bands (A00..A63), int8, nodata -128, 10 m, UTM.
  Years 2017-2025; ~34 150 tiles per year; index at v1/annual/aef_index.parquet
  (77.8 MB, one row per file, WGS84 footprint polygon + bounds).

Trap 1 -- the COGs are BOTTOM-UP: the geotransform has a POSITIVE y-resolution
and its origin is the bottom-left corner, so rasterio's from_bounds() yields an
inverted window and any naive read is vertically mirrored.  The publisher ships
a sidecar .vrt per tile that fixes this, but those VRTs hard-code
/vsis3/us-west-2.opendata.source.coop/... as their SourceDataset, which needs
AWS credentials -- unusable in a keyless pipeline (opening one raises
"GDALWarpOptions.Validate(): hSrcDS is not set").  _read_aef() therefore
derives the north-up affine itself: dst = Affine(a, b, c, 0, -|e|, f + |e|*H),
reads the window in file row order and flips the array.  That reproduces the
sidecar VRT exactly -- for tile 2024/17N/xegut5h9lqo3093nm-0000000000-0000000000
the file's SrcGeoTransform is (336160, 10, 0, 4587520, 0, 10) and the VRT's
DstGeoTransform is (336160, 10, 0, 4669440, 0, -10), and 4587520 + 10*8192 =
4669440.  Validated empirically too: on a 160-row north-south transect across
the Lake Ontario shoreline at -79.54 degE, cosine similarity to an open-water
reference embedding correlates r = +0.985 with ESA WorldCover water fraction in
the as-read orientation and r = -0.510 flipped.

Trap 2 -- pixel values are quantised int8 and must be de-quantised as
(v/127.5)**2 * sign(v); only then is each pixel's 64-vector unit length.  That
unit-norm property is used here as a validity test: a de-quantised nodata pixel
has norm 8.063, so pixels outside [0.9, 1.1] are dropped.  This also removes
overview pixels contaminated by nodata blending on decimated reads.

Only summary scalars are emitted -- never the 64-dim vector.  The feature row
carries a stable re-read reference (tile relative path + north-up pixel window)
so the donor-matching product can fetch the full vector later:

    url  = "https://data.source.coop/tge-labs/aef/" + aef_emb_tile_path
    win  = Window(aef_emb_win_col_off, aef_emb_win_row_off,
                  aef_emb_win_width,   aef_emb_win_height)   # north-up
    # tile is bottom-up: file_row_off = 8192 - (row_off + height); flip rows

The recorded window is the AOI BOUNDING-BOX window inside the primary tile; the
adapter additionally masks to the AOI polygon, so a bbox re-read returns a
slightly larger pixel set.  Verified round-trip on farm_on_essex: re-reading
tile 2024/17N/xegut5h9lqo3093nm-0000000000-0000000000 at window
(1888, 116, 73, 73) gives 5329 valid pixels against the adapter's 4761
polygon-masked pixels, and the mean vector agrees to ~3 decimals
(a00 -0.02969 vs -0.02978, L2 0.79683 vs 0.80187).  Where an AOI spans several
tiles (aef_emb_n_tiles_read > 1) the emitted summary is the area-weighted mosaic;
the reference points at the largest-overlap tile.

Requires, beyond pipeline.py's own imports: geopandas, pyarrow, shapely.
Cold-start cache footprint: 77.8 MB (the index parquet), downloaded once.

Keyless: plain public HTTPS on data.source.coop, no token, no SDK.
Licence: CC-BY-4.0, attribution "The AlphaEarth Foundations Satellite Embedding
dataset is produced by Google and Google DeepMind."
"""
import os, math, time, threading, warnings
import numpy as np

from . import pipeline as P
from .pipeline import S                    # reused proxy-aware session

import rasterio
from rasterio.transform import Affine
from rasterio.transform import from_bounds as aff_from_bounds
from rasterio.windows import Window
from rasterio.warp import transform_bounds
from rasterio.features import geometry_mask
import pyarrow.parquet as pq
import geopandas as gpd
import shapely
from shapely.geometry import shape, box

warnings.filterwarnings("ignore")

CACHE = os.path.abspath("./cache")
os.makedirs(CACHE, exist_ok=True)

AEF_BASE = "https://data.source.coop/tge-labs/aef/"
AEF_INDEX = AEF_BASE + "v1/annual/aef_index.parquet"
AEF_S3_PREFIX = "s3://us-west-2.opendata.source.coop/tge-labs/aef/"
AEF_YEAR = 2024          # fixed epoch: donor matching needs one common year
AEF_YEARS_FALLBACK = (2025, 2023, 2022, 2021, 2020, 2019, 2018, 2017)
AEF_DIM = 64
AEF_TILE_PX = 8192
MAXPIX = 160             # decimated-read cap per side, per tile
MAX_TILES = 4            # cap on tiles mosaicked for one AOI

# Striped multi-band reads over HTTPS need large VSI chunks, as in the
# physical lane; without them each band becomes its own range request.
GENV = dict(P.GDAL_ENV)
GENV.update(GDAL_HTTP_MERGE_CONSECUTIVE_RANGES="YES",
            CPL_VSIL_CURL_CHUNK_SIZE="2097152",
            CPL_VSIL_CURL_CACHE_SIZE="268435456",
            VSI_CACHE_SIZE="134217728",
            GDAL_HTTP_VERSION="2")

_LOCK = threading.Lock()
_MEM = {}


def _once(key, build):
    """Thread-safe memo for the one shared static archive (the tile index)."""
    if key in _MEM:
        return _MEM[key]
    with _LOCK:
        if key not in _MEM:
            _MEM[key] = build()
    return _MEM[key]


def _fetch(url, path):
    if os.path.exists(path) and os.path.getsize(path) > 0:
        return path
    tmp = path + ".part"
    with S.get(url, stream=True, timeout=600) as r:
        r.raise_for_status()
        with open(tmp, "wb") as fh:
            for chunk in r.iter_content(1 << 20):
                fh.write(chunk)
    os.replace(tmp, path)
    return path


def _aef_index():
    """Footprint index for the whole archive, as a GeoDataFrame keyed by year.

    One row per published COG: WGS84 footprint polygon, year, CRS and
    the s3:// path (rewritten to the public HTTPS mirror).
    """
    def build():
        p = _fetch(AEF_INDEX, os.path.join(CACHE, "aef_index.parquet"))
        t = pq.read_table(p, columns=["geom", "year", "path", "crs"])
        d = t.to_pandas()
        d["rel"] = d["path"].str.replace(AEF_S3_PREFIX, "", regex=False)
        g = gpd.GeoDataFrame(d.drop(columns=["geom", "path"]),
                             geometry=shapely.from_wkb(d["geom"].values), crs=4326)
        return {int(y): sub.reset_index(drop=True) for y, sub in g.groupby("year")}
    return _once("aef_index", build)


def _read_aef(rel, bbox, geom=None, maxpix=MAXPIX):
    """Decimated, AOI-clipped read of all 64 bands of one bottom-up AEF COG.

    Returns (E, valid, win) where E is (64, h, w) de-quantised float64 in
    north-up row order, valid is the (h, w) boolean mask of real embedding
    pixels, and win records the north-up pixel window inside the tile.
    """
    url = "/vsicurl/" + AEF_BASE + rel
    with rasterio.Env(**GENV):
        with rasterio.open(url) as src:
            H, Wd, nod, crs = src.height, src.width, src.nodata, src.crs
            t = src.transform
            bottom_up = t.e > 0
            tnu = (Affine(t.a, t.b, t.c, 0.0, -abs(t.e), t.f + abs(t.e) * H)
                   if bottom_up else t)
            b = transform_bounds("EPSG:4326", crs, *bbox, densify_pts=21)
            inv = ~tnu
            c0, r0 = inv * (b[0], b[3])
            c1, r1 = inv * (b[2], b[1])
            co = int(math.floor(min(c0, c1))); ro = int(math.floor(min(r0, r1)))
            w = int(math.ceil(max(c0, c1))) - co
            h = int(math.ceil(max(r0, r1))) - ro
            co = max(0, co); ro = max(0, ro)
            w = min(Wd, co + w) - co; h = min(H, ro + h) - ro
            if w < 1 or h < 1:
                raise LookupError("AOI does not intersect tile pixel array")
            frow = H - (ro + h) if bottom_up else ro
            sc = min(1.0, maxpix / max(h, w))
            oh, ow = max(1, int(h * sc)), max(1, int(w * sc))
            arr = src.read(window=Window(co, frow, w, h), out_shape=(src.count, oh, ow))
            wb_x0, wb_y1 = tnu * (co, ro)
            wb_x1, wb_y0 = tnu * (co + w, ro + h)
    if bottom_up:
        arr = arr[:, ::-1, :]
    E = ((arr.astype("f8") / 127.5) ** 2) * np.sign(arr)
    n = np.linalg.norm(E, axis=0)
    valid = (n > 0.9) & (n < 1.1)              # unit-norm test; nodata scores 8.063
    if nod is not None:
        valid &= (arr[0] != nod)
    if geom is not None and valid.size > 4:
        gb = transform_bounds(crs, "EPSG:4326", wb_x0, wb_y0, wb_x1, wb_y1,
                              densify_pts=21)
        m = geometry_mask([geom], out_shape=(oh, ow),
                          transform=aff_from_bounds(*gb, ow, oh), invert=True)
        if m.any():
            valid &= m
    return E, valid, dict(col_off=co, row_off=ro, width=w, height=h,
                          out_w=ow, out_h=oh, native_px=int(h) * int(w))


def _tile_context_mean(rel):
    """Mean embedding of the whole 81.92 km tile, from the COG's 1x1 top overview.

    The publisher builds every overview pixel as the mean of the full-resolution
    pixels beneath it, re-normalised to unit length, so the 1x1 level is the
    tile mean for one range request.
    """
    with rasterio.Env(**GENV):
        with rasterio.open("/vsicurl/" + AEF_BASE + rel) as src:
            a = src.read(out_shape=(src.count, 1, 1)).reshape(-1).astype("f8")
    v = ((a / 127.5) ** 2) * np.sign(a)
    n = np.linalg.norm(v)
    if not (0.9 < n < 1.1):
        raise LookupError("tile overview is nodata")
    return v / n


def _a_aef_embeddings(aoi):
    """AlphaEarth Foundations / Google Satellite Embedding V1 annual embeddings:
    64-dim unit-length 10 m pixel embeddings, read keyless from the Source
    Cooperative COG mirror.  Summary scalars only, plus a re-read reference."""
    idx = _aef_index()
    aoi_g = shape(aoi["geom"])
    hits, year = None, None
    for y in (AEF_YEAR,) + AEF_YEARS_FALLBACK:
        d = idx.get(y)
        if d is None:
            continue
        sel = d[d.intersects(aoi_g)]
        if len(sel) == 0:
            sel = d[d.intersects(box(*aoi["bbox"]))]
        if len(sel):
            hits, year = sel, y
            break
    if hits is None:
        raise LookupError("no AlphaEarth tile indexed for this AOI in 2017-2025")

    # largest-overlap tile first, so the primary re-read reference is the one
    # holding most of the AOI
    ov = hits.geometry.intersection(aoi_g).area.values
    order = np.argsort(-ov)
    hits = hits.iloc[order[:MAX_TILES]]

    sums = np.zeros(AEF_DIM); wsum = 0.0
    norms_sum, norms_n = 0.0, 0
    primary = None; tried = []
    for _, row in hits.iterrows():
        try:
            E, valid, win = _read_aef(row["rel"], aoi["bbox"], aoi["geom"])
        except Exception as e:
            tried.append(f"{row['rel'].split('/')[-1]}:{type(e).__name__}")
            continue
        k = int(valid.sum())
        if k == 0:
            tried.append(f"{row['rel'].split('/')[-1]}:all-nodata")
            continue
        v = E[:, valid]                                   # (64, k)
        # weight each tile by the native-resolution area it actually contributes,
        # so a coarser-decimated tile does not get a smaller vote
        wt = win["native_px"] * (k / float(win["out_w"] * win["out_h"]))
        sums += v.mean(axis=1) * wt
        wsum += wt
        norms_sum += float(np.linalg.norm(v, axis=0).sum()); norms_n += k
        if primary is None:
            primary = (row, win, v.mean(axis=1), k)
    if primary is None:
        raise LookupError("AlphaEarth tiles indexed but every pixel is nodata "
                          f"over this AOI ({'; '.join(tried) or 'no detail'})")

    m = sums / wsum
    row, win, pmean, pk = primary
    try:                       # AOI distinctiveness against its 81.92 km tile
        ctx = _tile_context_mean(row["rel"])
        cos = round(float(m @ ctx / (np.linalg.norm(m) + 1e-12)), 5)
    except Exception:
        cos = None

    f = {"aef_emb_n_tiles_read": int(len(hits)),
         "aef_emb_px_sampled": int(norms_n),
         "aef_emb_mean_vector_l2_norm": round(float(np.linalg.norm(m)), 5),
         "aef_emb_pixel_l2_norm_mean": round(norms_sum / max(norms_n, 1), 5),
         "aef_emb_cos_to_tile_context": cos,
         # re-read reference for the FULL 64-dim vector; the window is north-up
         "aef_emb_tile_path": str(row["rel"]),
         "aef_emb_tile_crs": str(row["crs"]),
         "aef_emb_win_col_off": int(win["col_off"]),
         "aef_emb_win_row_off": int(win["row_off"]),
         "aef_emb_win_width": int(win["width"]),
         "aef_emb_win_height": int(win["height"])}
    for i in range(len(m)):                       # all 64 components: this vector IS
                                                         # the join key for analogue fills
        f[f"aef_emb_a{i:02d}"] = round(float(m[i]), 6)
    return f, {"obs_date": f"{year}-01-01/{year}-12-31", "res_m": 10, "item": str(row["rel"]),
               "release": f"aef v1 annual {year}", "url": AEF_INDEX}


# slot, source_id, family, licence, auth, fn
ADAPTERS = [
 ("embeddings", "alphaearth_v1_annual", "embeddings",
  "CC-BY-4.0 (Google / Google DeepMind, AlphaEarth Foundations)", "none",
  _a_aef_embeddings),
]

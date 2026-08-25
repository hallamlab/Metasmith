"""Eydallin Fig. 1 -> one row per clone, because the paper publishes no such table.

THE SCREEN'S QUANTITATIVE DATA EXISTS ONLY AS A BAR CHART. Tables 1 and 2 are COG-
classified gene LISTS split excess/deficient, and Supplemental Table 1 is gene -> function
prose; neither carries a number. The only place the 86 measured glycogen contents appear
is Fig. 1, and in the PDF that figure is a 952x374 grayscale JPEG -- not vector, so there
are no drawing operators to read heights off. Measuring pixels is the only route, which is
why this script exists rather than a parser.

WHAT IS MEASURED AND WHAT IS READ. Bar heights are measured mechanically. The bar ORDER is
not -- `LABEL_ORDER` is a human reading of the rotated gene labels at 7x upscale. That
asymmetry is the risk this file has to answer for, so two checks have to pass:

  1. the 86 labels are exactly the 86 genes in the cohort's extraction, as a SET; and
  2. the bars below 100% are exactly the extraction's `glycogen_deficient` genes, and
     those above it exactly its `glycogen_excess` genes.

A one-position slip breaks (2) at the boundary; a misread name breaks (1). Neither is a
warning -- a silently shuffled answer key is worse than no answer key.

HOW A HEIGHT IS MEASURED. Each bar's top is a STROKE CENTRED ON THE DATUM, so the datum is
the stroke's centre and not the first inked row -- reading the first row biases every value
up by half a stroke. `edge()` fits what is actually on the page: two smoothed steps a
stroke-width apart, white above, fill below. Stroke width, stroke level and blur are fitted
GLOBALLY over all 86 bars, because they are properties of the rasteriser rather than of any
bar; only the position is per-bar. Fitting them per-bar is degenerate and was the first
thing tried.

WHY THE ANSWER IS THEN SNAPPED TO A GRID. The 86 fitted heights do not scatter: they lie on
a lattice of 0.6865 current pixels, at a concentration no unquantised set could produce.
The y-axis TICK spacings sit on the same lattice, alternating 49 and 50 units of it, which
is what a 50-unit gap of 49.41 lattice steps has to look like. So the figure in this PDF is
a DOWNSCALED RASTER of a larger one, and its native pixel is what the lattice measures. The
consequence is the honest limit on the whole exercise: the source image quantised these
values before anyone digitised it. Snapping removes the measurement noise and nothing else
-- the value is the centre of a bin about 1.01 points wide, and no amount of subpixel work
narrows that bin.

    mamba run -n figure-net python main/benchmarks/eydallin/digitize_fig1.py [--diagnose]

Needs `data/fabfos/originals/benchmarks/eydallin/` and `data/fabfos/benchmarks/` checked out.
"""
from __future__ import annotations

import argparse
import io
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from PIL import Image
from pypdf import PdfReader
from scipy.optimize import least_squares
from scipy.stats import norm

REPO = Path(__file__).resolve().parents[5]
PDF = REPO / "data/fabfos/originals/benchmarks/eydallin/eydallin2010_fulltext.pdf"
STUDY = REPO / "data/fabfos/benchmarks/eydallin"
OUT = STUDY / "Y" / "measured_glycogen.tsv"

WT_NMOL_GLUCOSE_PER_MG_PROTEIN = 45.0

FIG1_PAGE, FIG1_FORM, FIG1_IMAGE = 3, "/Fm1", "/Im1"

LABEL_ORDER = """
recQ ylcG ptsN clpA glgB gltI xylG nagD cpdB hokA thrB yjcQ rpiB yeaP spoT
pstC galS smg gspD cysI prfB yfdN gntT nagB yfjR metH phr yabI talA serB
yhcE yoeB gpp tnaA ynbD holC csrA napF yfaY yegH cydC yafV cysP ybcV wzc
mlc glgP malP gor exuR csrD ppk ucpA pnp yoaE malT pfs aspP ddg ptsI
ycbJ yciN putP pspE hyuA rbsR ppdB dos yjcC ydcJ rutF yfeD yqjA ssuA yifJ
mdtG ppx yncG glgS ymgC yncC rpoS tdcA erfK glgA glgC
""".split()


def figure_image() -> np.ndarray:
    page = PdfReader(str(PDF)).pages[FIG1_PAGE]
    form = page["/Resources"]["/XObject"][FIG1_FORM].get_object()
    im = form["/Resources"]["/XObject"][FIG1_IMAGE].get_object()
    if im.get("/Filter") != "/DCTDecode":
        raise SystemExit(f"[fig1] {FIG1_IMAGE} is {im.get('/Filter')}, not a JPEG -- "
                         f"everything below assumes this image")
    return np.asarray(Image.open(io.BytesIO(im._data)).convert("L")).astype(float)


def edge(u, t, w, S, sig, W, F):
    return (W + (S - W) * norm.cdf((u - (t - w / 2)) / sig)
              + (F - S) * norm.cdf((u - (t + w / 2)) / sig))


def fit_edges(profiles, t0s):
    t0s = np.asarray(t0s, float)

    def resid(p):
        w, S, sig = p[:3]
        return np.concatenate([edge(u, t, w, S, sig, W, F) - y
                               for (u, y, W, F), t in zip(profiles, p[3:])])

    r = least_squares(resid, np.concatenate([[1.0, 20.0, 0.45], t0s]),
                      bounds=(np.concatenate([[0.2, -200, 0.15], t0s - 3]),
                              np.concatenate([[3.0, 200, 2.0], t0s + 3])),
                      x_scale="jac")
    return r.x[3:], dict(w=r.x[0], level=r.x[1], sigma=r.x[2],
                         rms=float(np.sqrt(np.mean(r.fun ** 2))))


def find_bars(a: np.ndarray, base: int, spine: int):
    ink = a < 200
    top = np.full(a.shape[1], float(base))
    for c in range(spine + 2, a.shape[1]):
        rr = np.where(ink[:base, c])[0]
        if len(rr):
            top[c] = rr.min()
    runs, start = [], None
    for c in range(a.shape[1]):
        if top[c] < base - 2:
            start = c if start is None else start
        elif start is not None:
            runs.append((start, c - 1))
            start = None
    if start is not None:
        runs.append((start, a.shape[1] - 1))
    return [r for r in runs if r[1] - r[0] >= 3], top


def tick_centres(a: np.ndarray, spine: int):
    cols = slice(spine - 6, spine - 1)
    rows = [r for r in range(2, 335) if a[r, cols].mean() < 235]
    groups = []
    for r in rows:
        if groups and r - groups[-1][-1] <= 2:
            groups[-1].append(r)
        else:
            groups.append([r])
    out = []
    for g in groups:
        rr = np.arange(g[0] - 2, g[-1] + 3)
        wgt = np.clip(255 - a[rr, cols].mean(1), 0, None)
        out.append(float((rr * wgt).sum() / wgt.sum()))
    return np.array(out)


def lattice(x: np.ndarray, lo=0.3, hi=2.0, step=2e-5):
    ps = np.arange(lo, hi, step)
    R = np.array([abs(np.exp(2j * np.pi * x / p).sum()) / len(x) for p in ps])
    p = float(ps[R.argmax()])
    phase = float(np.angle(np.exp(2j * np.pi * x / p).sum()) / (2 * np.pi) * p)
    return p, float(R.max()), phase


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", type=Path, default=OUT)
    ap.add_argument("--diagnose", action="store_true",
                    help="print the accuracy evidence: fitted stroke, lattice, residuals")
    ap.add_argument("--no-snap", action="store_true",
                    help="report the raw subpixel fit instead of the lattice it sits on")
    args = ap.parse_args()

    a = figure_image()
    dark = a < 128
    base = int(np.argmax(dark.sum(1)))
    spine = int(np.argmax(dark.sum(0)))
    runs, top = find_bars(a, base, spine)
    if len(runs) != len(LABEL_ORDER):
        raise SystemExit(f"[fig1] measured {len(runs)} bars but read {len(LABEL_ORDER)} "
                         f"labels -- the two have to be the same 86 clones")
    fill = float(np.median([np.median(a[int(top[lo + 2]) + 4:int(top[lo + 2]) + 9, lo + 2:hi - 1])
                            for lo, hi in runs if int(top[lo + 2]) + 9 < base - 2]))

    bots, bpar = fit_edges(
        [(np.arange(base - 4, base + 6, dtype=float),
          np.median(a[np.ix_(np.arange(base - 4, base + 6), list(range(lo + 2, hi - 1)))], axis=1),
          fill, 255.0) for lo, hi in runs],
        [base + 0.5] * len(runs))
    zero = float(bots.mean())

    ticks = tick_centres(a, spine)
    tvals = np.array([500 - 50 * round((t - ticks[0]) / np.median(np.diff(ticks)))
                      for t in ticks])
    anchors = np.concatenate([ticks, [zero]])
    avals = np.concatenate([tvals, [0.0]])
    slope, icept = np.polyfit(anchors, avals, 1)
    cal_resid = np.polyval([slope, icept], anchors) - avals

    profs, t0s = [], []
    for lo, hi in runs:
        r0 = int(top[lo + 2])
        rows = np.arange(r0 - 4, min(r0 + 6, base - 1))
        cols = list(range(lo + 2, hi - 1))
        below = a[r0 + 3:min(r0 + 8, base - 1), lo + 2:hi - 1]
        profs.append((rows.astype(float),
                      np.median(a[np.ix_(rows, cols)], axis=1),
                      255.0, float(np.median(below)) if below.size >= 4 else fill))
        t0s.append(r0 + 0.5)
    tops, tpar = fit_edges(profs, t0s)

    heights = zero - tops
    q, conc, phase = lattice(heights)
    snapped = np.round((heights - phase) / q) * q + phase
    lat_resid = heights - snapped
    use = heights if args.no_snap else snapped
    if conc < 0.5 and not args.no_snap:
        print(f"[fig1] the heights are NOT on a lattice (|R|={conc:.2f}); reporting the "
              f"raw subpixel fit", flush=True)
        use = heights
    values = use * abs(slope)

    df = pd.DataFrame(dict(
        condition_id=[f"eydallin:{g}" for g in LABEL_ORDER],
        element="C",
        gene=LABEL_ORDER,
        pct_wt=np.round(values, 1),
        nmol_glucose_per_mg_protein=np.round(
            values / 100.0 * WT_NMOL_GLUCOSE_PER_MG_PROTEIN, 2),
        rank=np.arange(1, len(runs) + 1),
        source_px=np.round(use / q, 0).astype(int) if not args.no_snap else np.round(use / q, 2),
    ))

    ex = pd.read_csv(STUDY / "extraction.tsv", sep="\t")
    curated = dict(zip(ex["gene_norm"], ex["phenotype"]))
    if set(df["gene"]) != set(curated):
        raise SystemExit(f"[fig1] the labels read off the figure are not the cohort's "
                         f"genes: only in figure {sorted(set(df['gene']) - set(curated))}, "
                         f"only in extraction {sorted(set(curated) - set(df['gene']))}")
    want = np.where(df["pct_wt"] < 100, "glycogen_deficient", "glycogen_excess")
    bad = df[want != df["gene"].map(curated).to_numpy()]
    if len(bad):
        raise SystemExit(f"[fig1] {len(bad)} clones land on the wrong side of WT against "
                         f"the curated phenotype, so the label order is wrong:\n"
                         f"{bad.to_string(index=False)}")

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.unlink(missing_ok=True)
    df.sort_values("gene").to_csv(args.out, sep="\t", index=False)

    print(f"[fig1] {len(df)} clones  {int((df.pct_wt < 100).sum())} deficient / "
          f"{int((df.pct_wt >= 100).sum())} excess  "
          f"{df.pct_wt.min()}% .. {df.pct_wt.max()}%  -> {args.out}", flush=True)
    print(f"[fig1] 1 px = {abs(slope):.4f} points; source raster's own quantum "
          f"{q * abs(slope):.4f} points, so a value is the centre of a bin that wide",
          flush=True)

    if args.diagnose:
        print(f"\n  bar-top stroke     w={tpar['w']:.2f} px  level={tpar['level']:.0f}  "
              f"sigma={tpar['sigma']:.2f}  fit rms={tpar['rms']:.1f}/255")
        print(f"  axis-line stroke   w={bpar['w']:.2f} px  level={bpar['level']:.0f}  "
              f"sigma={bpar['sigma']:.2f}  fit rms={bpar['rms']:.1f}/255")
        print(f"  zero line          row {zero:.3f}, sd over 86 bottoms {bots.std():.3f} px"
              f"  (one JPEG block row: repeatability, not independence)")
        print(f"  calibration        {len(ticks)} ticks {tvals.tolist()} + that zero")
        print(f"                     residual sd {cal_resid.std() / abs(slope):.3f} px, "
              f"the zero anchor's own {np.polyval([slope, icept], zero):+.3f} points")
        print(f"  lattice            {q:.5f} px = {q * abs(slope):.4f} points, "
              f"|R|={conc:.3f} (unquantised 86 values give ~0.11)")
        print(f"                     residual about it: sd {lat_resid.std() / q:.3f} of a "
              f"step, max {np.abs(lat_resid).max() / q:.3f}")
        print(f"  axis span 0..500   {zero - ticks[0]:.2f} px = "
              f"{(zero - ticks[0]) / q:.1f} lattice steps = "
              f"{(zero - ticks[0]) / q / 500:.4f} steps per point")
        print(f"  so the measurement is good to ~{lat_resid.std() * abs(slope):.2f} points "
              f"and the FIGURE is good to ~{q * abs(slope) / 2:.2f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

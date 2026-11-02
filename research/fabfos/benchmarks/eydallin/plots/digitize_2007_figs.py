"""Eydallin 2007 Figs. 1 and 2 -> one row per Keio mutant, because the paper publishes
no such table.

SAME PROBLEM AS THE 2010 SCREEN, DIFFERENT DIFFICULTY. Tables 1 and 2 are COG-classified
gene LISTS and Supplemental Table 1 is gene -> function prose; neither carries a number.
The 65 measured glycogen contents appear only in the two bar charts. But unlike
`digitize_fig1.py`'s 2010 figure -- a 952x374 JPEG whose bar tops had to be recovered by
fitting a smoothed step through the raster -- these two figures are VECTOR. The bars are
`re` drawing operators and their tops are exact page coordinates. There is nothing to fit,
no rasteriser stroke to model, and no quantisation lattice underneath. The accuracy limit
here is the axis calibration, which the tick residuals below report directly.

WHAT IS MEASURED AND WHAT IS READ, and the asymmetry this file has to answer for. Bar
positions are measured. The gene NAMES are read -- they are the rotated labels under each
axis, and this script takes them as PyMuPDF reports them. Three checks stand between that
reading and the output:

  1. the 65 labels are exactly the 65 genes in `extraction.tsv`, as a SET;
  2. Fig. 1's 35 values all land at or above 100% and Fig. 2's 30 all below it, matching
     the extraction's excess/deficient split; and
  3. every label matched a bar within a fraction of the bar pitch, or is named as a
     deliberate zero.

FIVE OF FIG. 2's THIRTY GENES HAVE NO BAR, AND THAT IS THE REASON FOR (3). `glgA`, `glgB`,
`glgC`, `ubiG` and `pgm` are the glycogen-LESS mutants; their bars have zero height and the
PDF emits no rectangle for them at all. So Fig. 2 draws 25 rectangles for 30 labels, and
anything that paired the two by ordinal position would shift 25 of the 30 answers by five
places. Matching is therefore by x-COORDINATE, and a label with no rectangle near it is
recorded as 0.0 and printed by name rather than silently zeroed.

Matching by position would be wrong even with no gap: PyMuPDF returns the labels in the
PDF's text-stream order, which is not the visual left-to-right order on either figure.

    mamba run -n awm python research/fabfos/benchmarks/eydallin/plots/digitize_2007_figs.py [--diagnose]

Needs `data/fabfos/originals/benchmarks/eydallin/` checked out. Runs under `awm` rather
than this tree's usual `msm-fabfos` because it needs PyMuPDF: drawing operators are what
PyMuPDF exposes and pypdf, which the 2010 digitizer uses, does not.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pymupdf

REPO = Path(__file__).resolve().parents[5]
PDF = REPO / "data/fabfos/originals/benchmarks/eydallin/eydallin2007_fulltext.pdf"
STUDY = REPO / "data/fabfos/benchmarks/eydallin_2007"
OUT = STUDY / "Y" / "measured_glycogen.tsv"

# Both figure captions: "Average glycogen content in WT cells was equivalent to 147 nmol
# glucose/mg protein." The 2010 screen's constant is 45.0 -- different paper, different
# medium, different number.
WT_NMOL_GLUCOSE_PER_MG_PROTEIN = 147.0

FIGURES_PAGE = 2

# The gene labels sit at ~70 degrees, not upright and not a clean 90; the axis tick numbers
# and all body text are horizontal. A y-component this large picks out the labels and
# nothing else on the page.
ROTATED_MIN_SIN = 0.5

# A label is paired with the nearest bar centre within this fraction of the bar pitch.
# Observed residuals are ~0.15 of a pitch (the rotated label's bounding box is a slanted
# rectangle, so its x-centre sits slightly left of the bar it names); the five true zeros
# miss by 1.1 pitches or more. Nothing lands in between.
MATCH_MAX_PITCH_FRACTION = 0.45


def filled_bars(page) -> list[tuple[float, float, float]]:
    """Every filled rectangle on the page as (x_centre, y_top, y_bottom).

    Each bar is emitted twice, once filled and once stroked; taking fills only is what
    keeps the count honest. The page background and the plot frames come through here too
    and are separated out by `split_figures` below, on baseline."""
    out = []
    for item in page.get_drawings():
        if item.get("type") != "f":
            continue
        for sub in item["items"]:
            if sub[0] == "re":
                r = sub[1]
                out.append(((r.x0 + r.x1) / 2, r.y0, r.y1))
    return out


def split_figures(bars) -> list[float]:
    """The two baselines, as the two y-bottoms shared by many rectangles.

    A bar chart's rectangles all end on the axis, so a baseline is a mode of the y-bottom
    distribution. The page background and the frames are each one rectangle at their own
    y-bottom and fall out of the count."""
    counts: dict[float, int] = {}
    for _, _, y1 in bars:
        counts[round(y1, 1)] = counts.get(round(y1, 1), 0) + 1
    baselines = sorted(y for y, n in counts.items() if n >= 5)
    if len(baselines) != 2:
        raise SystemExit(f"[fig2007] found {len(baselines)} baselines with >=5 bars "
                         f"({baselines}); expected exactly 2 figures on page "
                         f"{FIGURES_PAGE + 1}")
    return baselines


def spans(page):
    for block in page.get_text("dict")["blocks"]:
        for line in block.get("lines", []):
            for span in line["spans"]:
                yield line, span


def tick_calibration(page, baseline: float, other_baseline: float):
    """Fit page-y -> percent-of-WT on one figure's numeric axis labels.

    Returns the fitted (slope, intercept) and the residuals, in percent, so the caller can
    report how good the calibration actually is instead of asserting that it is good."""
    lo = min(baseline, other_baseline)
    band = (0.0, baseline) if baseline == lo else (other_baseline, baseline)

    ys, vals = [], []
    for line, span in spans(page):
        if abs(line["dir"][1]) > ROTATED_MIN_SIN:
            continue
        text = span["text"].strip()
        if not text.isdigit():
            continue
        x0, y0, x1, y1 = span["bbox"]
        if not (band[0] < (y0 + y1) / 2 <= band[1] + 4):
            continue
        if x1 > 145:                      # the axis numbers sit left of the plot area
            continue
        ys.append((y0 + y1) / 2)
        vals.append(float(text))

    if len(ys) < 3:
        raise SystemExit(f"[fig2007] only {len(ys)} axis ticks found below y={baseline}")

    slope, intercept = np.polyfit(np.asarray(ys), np.asarray(vals), 1)
    resid = np.polyval([slope, intercept], ys) - np.asarray(vals)
    return float(slope), float(intercept), np.asarray(sorted(vals)), resid


def gene_labels(page, baseline: float) -> list[tuple[str, float]]:
    """The rotated gene labels under one figure, as (name, x_centre)."""
    out = []
    for line, span in spans(page):
        if abs(line["dir"][1]) <= ROTATED_MIN_SIN:
            continue
        x0, y0, x1, y1 = span["bbox"]
        if not (baseline < y0 < baseline + 12):
            continue
        out.append((span["text"].strip(), (x0 + x1) / 2))
    return sorted(out, key=lambda t: t[1])


def read_figure(page, baseline: float, other_baseline: float):
    bars = [(cx, y0) for cx, y0, y1 in filled_bars(page) if abs(y1 - baseline) < 0.5]
    bars.sort()
    labels = gene_labels(page, baseline)
    slope, intercept, tick_values, tick_resid = tick_calibration(page, baseline,
                                                                 other_baseline)

    pitch = float(np.median(np.diff([x for _, x in labels])))
    bar_x = np.asarray([cx for cx, _ in bars])

    rows, zeros, offsets = [], [], []
    for name, lx in labels:
        if len(bar_x):
            j = int(np.argmin(np.abs(bar_x - lx)))
            gap = abs(bar_x[j] - lx)
        else:
            j, gap = -1, np.inf
        if gap <= MATCH_MAX_PITCH_FRACTION * pitch:
            offsets.append(bar_x[j] - lx)
            rows.append((name, float(np.polyval([slope, intercept], bars[j][1])),
                         float(bars[j][1])))
        else:
            zeros.append(name)
            rows.append((name, 0.0, float(baseline)))

    matched = len(rows) - len(zeros)
    if matched != len(bars):
        raise SystemExit(f"[fig2007] {len(bars)} bars on baseline {baseline} but "
                         f"{matched} labels matched one -- a bar was claimed twice or "
                         f"none")
    return dict(rows=rows, zeros=zeros, pitch=pitch, n_bars=len(bars),
                slope=slope, intercept=intercept, tick_values=tick_values,
                tick_resid=tick_resid, offsets=np.asarray(offsets))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", type=Path, default=OUT)
    ap.add_argument("--diagnose", action="store_true",
                    help="print the accuracy evidence: tick residuals, label-to-bar "
                         "offsets, and the pitch each was judged against")
    args = ap.parse_args()

    page = pymupdf.open(str(PDF))[FIGURES_PAGE]
    top_baseline, bottom_baseline = split_figures(filled_bars(page))

    fig1 = read_figure(page, top_baseline, bottom_baseline)
    fig2 = read_figure(page, bottom_baseline, top_baseline)

    if len(fig1["rows"]) != 35 or fig1["n_bars"] != 35:
        raise SystemExit(f"[fig2007] Fig. 1 has {len(fig1['rows'])} labels and "
                         f"{fig1['n_bars']} bars; the paper reports 35 glycogen-excess "
                         f"mutants and draws a bar for every one")
    if len(fig2["rows"]) != 30:
        raise SystemExit(f"[fig2007] Fig. 2 has {len(fig2['rows'])} labels; the paper "
                         f"reports 30 glycogen-deficient mutants")

    df = pd.DataFrame(
        [(g, v, "glycogen_excess") for g, v, _ in fig1["rows"]] +
        [(g, v, "glycogen_deficient") for g, v, _ in fig2["rows"]],
        columns=["gene", "pct_wt", "figure_phenotype"])
    df["pct_wt"] = df["pct_wt"].round(1)

    ex = pd.read_csv(STUDY / "extraction.tsv", sep="\t")
    curated = dict(zip(ex["gene"], ex["phenotype"]))
    if set(df["gene"]) != set(curated):
        raise SystemExit(
            f"[fig2007] the labels read off the figures are not the cohort's genes: "
            f"only in figures {sorted(set(df['gene']) - set(curated))}, only in "
            f"extraction {sorted(set(curated) - set(df['gene']))}")

    bad = df[df["figure_phenotype"] != df["gene"].map(curated)]
    if len(bad):
        raise SystemExit(f"[fig2007] {len(bad)} gene(s) appear in the figure that "
                         f"contradicts their curated phenotype:\n{bad.to_string(index=False)}")

    wrong_side = df[((df["figure_phenotype"] == "glycogen_excess") & (df["pct_wt"] < 100)) |
                    ((df["figure_phenotype"] == "glycogen_deficient") & (df["pct_wt"] >= 100))]
    if len(wrong_side):
        raise SystemExit(f"[fig2007] {len(wrong_side)} value(s) land on the wrong side of "
                         f"WT for their own figure, so a calibration or a label order is "
                         f"wrong:\n{wrong_side.to_string(index=False)}")

    out = df.assign(
        condition_id=[f"eydallin2007:{g}" for g in df["gene"]],
        element="C",
        nmol_glucose_per_mg_protein=(df["pct_wt"] / 100.0
                                     * WT_NMOL_GLUCOSE_PER_MG_PROTEIN).round(2),
        source_figure=np.where(df["figure_phenotype"] == "glycogen_excess", "Fig1", "Fig2"),
    )
    out["rank"] = out["pct_wt"].rank(method="first").astype(int)
    out = out[["condition_id", "element", "gene", "pct_wt",
               "nmol_glucose_per_mg_protein", "rank", "source_figure"]]

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.unlink(missing_ok=True)
    out.sort_values("gene").to_csv(args.out, sep="\t", index=False)

    print(f"[fig2007] {len(out)} mutants  {fig1['n_bars']} excess / {fig2['n_bars']} "
          f"deficient bars + {len(fig2['zeros'])} drawn at zero  "
          f"{out.pct_wt.min()}% .. {out.pct_wt.max()}%  -> {args.out}", flush=True)
    print(f"[fig2007] no bar was drawn for {', '.join(fig2['zeros'])} -- the paper's "
          f"glycogen-LESS mutants, recorded as 0.0 rather than dropped", flush=True)

    if args.diagnose:
        for name, fig in (("Fig. 1", fig1), ("Fig. 2", fig2)):
            scale = abs(fig["slope"])
            print(f"\n  {name}")
            print(f"    ticks          {fig['tick_values'].astype(int).tolist()}")
            print(f"    calibration    1 point = {scale:.4f}% ; tick residual sd "
                  f"{fig['tick_resid'].std():.4f}% , max "
                  f"{np.abs(fig['tick_resid']).max():.4f}% "
                  f"(= {np.abs(fig['tick_resid']).max() / scale:.2f} points of position)")
            print(f"    bar pitch      {fig['pitch']:.2f} points")
            print(f"    label offsets  mean {fig['offsets'].mean():+.2f} points, sd "
                  f"{fig['offsets'].std():.2f}, max "
                  f"{np.abs(fig['offsets']).max():.2f} "
                  f"({np.abs(fig['offsets']).max() / fig['pitch']:.2f} of a pitch; "
                  f"the cut is {MATCH_MAX_PITCH_FRACTION})")
    return 0


if __name__ == "__main__":
    sys.exit(main())

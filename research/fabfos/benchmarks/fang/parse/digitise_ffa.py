#!/usr/bin/env python3
"""Read the ASKA/FFA screen's measured response off the figure bars.

Fang et al. (Metab. Eng. 92:13-21, 2025) rebuilt and assayed 60 ASKA clones
individually, which is what makes the study usable as a benchmark: it reports
genes that were built, measured, and did *not* move the phenotype. But the
titers exist only as bar charts. The supplement holds Figures S1-S7 and three
tables (strains, plasmids, NGS primers) and no numeric FFA table, and the data
statement is "available on request", so the bars are the record.

So they are measured, not eyeballed. Each panel is calibrated from its own
y-axis ticks, found in the pixels rather than declared here, and every bar top
is read by walking up its own columns from the baseline. Two things make that
harder than it sounds and both are handled rather than hoped away:

  * the scatter of replicate points and the error bar are drawn *over* the bar,
    so a column's colour is interrupted partway up. The walk therefore hops
    across a gap and keeps going, and takes the tallest height that at least
    three columns independently reach -- occlusion can only make a column look
    shorter, never taller.
  * the grey antialiasing of those same black lines passes through exactly the
    range that says "grey bar", which is why the walk works on maximal runs and
    ignores any run too thin to be bar.

Eight strains have their titer stated in prose and four more have a stated
percentage change against a stated control; those twelve are the anchors. The
published number for an anchored strain is the paper's own, and the digitised
value is kept beside it, so `ffa_anchors.tsv` is a standing measurement of how
far this reader is off where the truth is known. It is the reason to believe
the unanchored bars.

Writes two tables into the acquisitions tier, beside the PDF and the supplement
they were read from:

    data/fabfos/originals/benchmarks/fang/ffa/ffa_response.tsv
    data/fabfos/originals/benchmarks/fang/ffa/ffa_anchors.tsv

A digitisation is a human reading of a figure, the same kind of given as the
curated study extractions -- not something a rebuild recomputes. Run it under
an env with pypdf and Pillow (`figure-net` on this box):

    /home/tony/lib/miniforge3/envs/figure-net/bin/python \\
        research/fabfos/benchmarks/fang/parse/digitise_ffa.py
"""
from __future__ import annotations

import argparse
import csv
import io
import re
import zipfile
from pathlib import Path

import numpy as np
from PIL import Image

def _repo_root(start: Path) -> Path:
    for d in [start, *start.parents]:
        if (d / "data/fabfos").is_dir():
            return d
    raise SystemExit(f"no ancestor of {start} contains data/fabfos")


ROOT = _repo_root(Path(__file__).resolve())
ASKA = ROOT / "data" / "fabfos" / "originals" / "benchmarks" / "fang"
PDF = ASKA / "ffa" / "1-s2.0-S1096717625000989-main.pdf"
DOCX = ASKA / "ffa" / "1-s2.0-S1096717625000989-mmc1.docx"

F0 = 799.6
STATED = {
    ("fig1c", "F0"): (F0, "prose: baseline FFAs production of 799.6 mg/L"),
    ("fig1c", "rfaY"): (2461.3, "prose: highest FFAs production of 2461.3 mg/L"),
    ("fig1d", "F0"): (F0, "prose"),
    ("fig1d", "dRfaY"): (458.0, "prose: produced only 458.0 mg/L"),
    ("fig1d", "dRfaY-comp"): (2091.0, "prose: synthesized 2091.0 mg/L"),
    ("fig3b", "F0"): (F0, "prose"),
    ("fig3b", "lpxK"): (F0 * 1.367, "prose: significant enhancement of 36.7%"),
    ("fig3b", "lpxL"): (F0 * 2.094, "prose: significant enhancement of 109.4%"),
    ("fig3b", "waaF"): (F0 * 1.466, "prose: significant enhancement of 46.6%"),
    ("fig3b", "msbA"): (F0 * 1.648, "prose: significant enhancement of 64.8%"),
    ("fig4c", "RF"): (2240.3, "prose: exhibited 2240.3 mg/L FFAs production"),
    ("fig4c", "yafL"): (3447.6, "prose: reaching 3447.6 mg/L"),
    ("fig4c", "rimM"): (3389.2, "prose: and 3389.2 mg/L"),
    ("fig5a", "rfaY-yafL"): (3447.6, "prose"),
    ("fig5a", "rfaY-yafL-fadR"): (5736.1, "prose: highest FFAs titer of 5736.1 mg/L"),
    ("fig5a", "rfaY-rimM"): (3389.2, "prose"),
    ("figS4", "RF"): (2240.3, "prose"),
    ("figS5", "F0"): (F0, "prose"),
}

def _bands(im):
    r, g, b = im[..., 0], im[..., 1], im[..., 2]
    return {
        "mint": (r > 140) & (r < 225) & (g > 210) & (b > 190) & (b < 250) & ((g - r) > 25),
        "pink": (r > 235) & (g > 185) & (g < 245) & (b > 185) & (b < 245) & ((r - g) > 8),
        "grey": (abs(r - g) < 12) & (abs(g - b) < 12) & (r > 150) & (r < 232),
    }


SUPPORT = 3
MIN_RUN = 6
MAX_GAP = 26
JOINT = 12


def _column_top(mask, x, base, ymin):
    col = mask[ymin:base + 1, x]
    if not col[-1]:
        return None
    runs, y = [], len(col) - 1
    while y >= 0:
        if not col[y]:
            y -= 1
            continue
        end = y
        while y >= 0 and col[y]:
            y -= 1
        runs.append((y + 1, end))
    cur = runs[0]
    for start, end in runs[1:]:
        if cur[0] - end - 1 > MAX_GAP:
            break
        if end - start + 1 >= MIN_RUN:
            cur = (start, end)
    return ymin + cur[0]


def _groups(mask, base, xlo, xhi, minw=10):
    xs = [x for x in range(xlo, xhi) if mask[base, x]]
    if not xs:
        return []
    out, cur = [], [xs[0]]
    for x in xs[1:]:
        if x - cur[-1] <= JOINT:
            cur.append(x)
        else:
            if len(cur) >= minw:
                out.append((cur[0], cur[-1]))
            cur = [x]
    if len(cur) >= minw:
        out.append((cur[0], cur[-1]))
    return out


def find_axes(im, box):
    y0, y1, x0, x1 = box
    dark = im.max(axis=2) < 120
    xaxis = max(range(y0, y1), key=lambda y: dark[y, x0:x1].sum())
    yaxis = max(range(x0, x1), key=lambda x: dark[y0:y1, x].sum())
    return xaxis, yaxis


def find_yticks(im, xaxis, yaxis, ytop, span=14, frac=0.5):
    dark = im.max(axis=2) < 150
    a, b = yaxis - span, yaxis - 2
    rows = [y for y in range(ytop, xaxis + 3) if dark[y, a:b].sum() >= (b - a) * frac]
    if not rows:
        return []
    out, cur = [], [rows[0]]
    for y in rows[1:]:
        if y - cur[-1] <= 3:
            cur.append(y)
        else:
            out.append(cur)
            cur = [y]
    out.append(cur)
    return [float(np.mean(g)) for g in out]


def calibrate(im, box, ytop, vmax):
    xaxis, yaxis = find_axes(im, box)
    ticks = find_yticks(im, xaxis, yaxis, ytop)
    if len(ticks) < 3:
        raise SystemExit(f"only {len(ticks)} y-ticks found near x={yaxis}")
    gaps = np.diff(sorted(ticks))
    if gaps.std() > 1.5:
        raise SystemExit(f"y-ticks unevenly spaced: {ticks}")
    y_zero, y_top = max(ticks), min(ticks)
    return xaxis, y_zero, y_top, vmax / (y_zero - y_top), len(ticks)


def digitise(im, xaxis, y_zero, y_top, scale, xlo, xhi, labels, drop=6):
    bands = _bands(im)
    anyb = bands["mint"] | bands["pink"] | bands["grey"]
    base = xaxis - drop
    found = _groups(anyb, base, xlo, xhi)
    if len(found) != len(labels):
        raise SystemExit(f"found {len(found)} bars, expected {len(labels)}: "
                         f"{[(a, b) for a, b in found]}")
    rows = []
    for (x0, x1), label in zip(found, labels):
        mid = (x0 + x1) // 2
        kind = next((k for k in ("pink", "mint", "grey") if bands[k][base, mid]), "mint")
        ceiling = int(y_top) - 4
        tops = sorted(t for t in (_column_top(bands[kind], x, base, ceiling)
                                  for x in range(x0 + 4, x1 - 3)) if t is not None)
        if len(tops) < SUPPORT:
            raise SystemExit(f"bar {label}: only {len(tops)} readable columns")
        top = tops[SUPPORT - 1]
        rows.append(dict(label=label, colour=kind, top=top,
                         value=round((y_zero - top) * scale, 1)))
    return rows


PANELS = {
    "fig1c": dict(
        source="pdf", page=4, box=(560, 1010, 60, 1340), ytop=555, vmax=3000.0,
        xlo=140, xhi=1300, background="F", control="F0",
        labels=["F0", "setB", "setA", "ydeA", "nepI", "cmr", "bcr", "trpC", "dps",
                "lsrB", "yjcO", "fpr", "yoaE", "mscL", "tesA", "yaaA", "rfaY",
                "yfaO", "yeeS", "norR", "wcaA", "fabZ", "yegL", "ygdD", "sufE"],
        marks={"ydeA": "**", "nepI": "***", "bcr": "ns", "rfaY": "****",
               "wcaA": "ns", "ygdD": "**"},
        note="round 1: 24 ORFs nominated by the genome-scale screen, in strain F"),
    "fig3b": dict(
        source="pdf", page=6, box=(860, 1300, 100, 1200), ytop=880, vmax=2500.0,
        xlo=160, xhi=1180, background="F", control="F0",
        labels=["F0", "lpxH", "lpxK", "lpxL", "lpxM", "waaC", "waaF", "waaP",
                "waaQ", "waaG", "msbA", "lptA", "lptB", "lptD", "lptE"],
        marks={"lpxH": "ns", "lpxK": "***", "lpxL": "**", "waaC": "ns",
               "waaF": "***", "msbA": "***", "lptB": "ns"},
        note="LPS synthesis and transport panel, chosen by pathway not by screen"),
    "fig4c": dict(
        source="pdf", page=7, box=(400, 830, 100, 1500), ytop=420, vmax=4000.0,
        xlo=130, xhi=1620, background="RF", control="RF",
        labels=["RF", "ydeA", "setA", "setB", "rsxG", "yafZ", "nudG", "glxK",
                "yggR", "ytfK", "gpsA", "hydN", "lacI", "pntA", "asd", "gppA",
                "opgD", "yafL", "rimM", "lpxD", "yjdF", "ybeF", "folE", "fabB",
                "tas"],
        marks={"yggR": "ns", "gpsA": "ns", "hydN": "ns", "pntA": "ns",
               "asd": "ns", "yafL": "**", "rimM": "**", "ybeF": "ns"},
        note="round 2: nominated on the RF background, so every strain also "
             "overexpresses rfaY from pRF"),
    "fig5a": dict(
        source="pdf", page=8, box=(20, 460, 60, 560), ytop=30, vmax=6000.0,
        xlo=120, xhi=540, background="RF", control=None,
        labels=["rfaY-yafL", "rfaY-yafL-fadR", "rfaY-rimM", "rfaY-rimM-fadR"],
        marks={"rfaY-yafL-fadR": "****", "rfaY-rimM-fadR": "****"},
        note="fadR added to each round-two winner; each fadR strain is tested "
             "against its own non-fadR parent, not against RF"),
    "figS4": dict(
        source="docx", image="image4", box=(10, 350, 60, 1170), ytop=15,
        vmax=3000.0, xlo=115, xhi=1170, background="RF", control="RF",
        labels=["RF", "rfaY-ydeA", "rfaY-wcaA", "rfaY-nepI", "rfaY-norR",
                "rfaY-lacI", "ygdD-ydeA", "ygdD-wcaA", "ygdD-nepI", "ygdD-rfaY",
                "ygdD-lacI", "norR-ydeA", "norR-wcaA", "norR-nepI", "norR-ygdD",
                "norR-lacI"],
        marks={},
        note="pairwise combinations of the round-one winners; no bar is flagged"),
    "figS5": dict(
        source="docx", image="image5", box=(10, 350, 30, 350), ytop=15,
        vmax=1500.0, xlo=130, xhi=350, background="F", control="F0",
        labels=["F0", "yafL", "rimM"],
        marks={"yafL": "ns", "rimM": "ns"},
        note="the round-two winners overexpressed alone: neither moves without "
             "rfaY, so their effect is epistatic"),
}

FIG1D = [("F0", "", ""), ("dRfaY", "", "rfaY"), ("dRfaY-comp", "rfaY", "rfaY")]
FIG1D_MARKS = {"dRfaY": "***", "dRfaY-comp": "****"}


def clones_of(panel, label):
    if label in ("F0", "RF"):
        return ("rfaY", "") if label == "RF" else ("", "")
    parts = label.split("-")
    if panel == "fig4c":
        return ("|".join(["rfaY"] + parts), "")
    if panel == "fig5a":
        return ("|".join(["rfaY"] + parts[1:]), "")
    if panel == "figS4":
        return ("|".join(parts), "")
    return (label, "")


def panel_image(spec, cache):
    if spec["source"] == "pdf":
        from pypdf import PdfReader
        key = cache / f"pdf_p{spec['page']:02d}.png"
        if not key.exists():
            page = PdfReader(str(PDF)).pages[spec["page"] - 1]
            biggest = max(page.images, key=lambda i: i.image.size[0] * i.image.size[1])
            biggest.image.save(key)
    else:
        key = cache / f"docx_{spec['image']}.png"
        if not key.exists():
            with zipfile.ZipFile(DOCX) as z:
                name = next(n for n in z.namelist()
                            if n.startswith("word/media/") and spec["image"] in n)
                Image.open(io.BytesIO(z.read(name))).convert("RGB").save(key)
    return np.array(Image.open(key).convert("RGB")).astype(int)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", type=Path, default=ASKA / "ffa")
    ap.add_argument("--cache", type=Path, default=Path("/tmp/aska_figs"))
    args = ap.parse_args()
    args.cache.mkdir(parents=True, exist_ok=True)
    args.out.mkdir(parents=True, exist_ok=True)

    response, anchors = [], []
    for panel, spec in PANELS.items():
        im = panel_image(spec, args.cache)
        xaxis, y_zero, y_top, scale, nticks = calibrate(im, spec["box"],
                                                        spec["ytop"], spec["vmax"])
        bars = digitise(im, xaxis, y_zero, y_top, scale, spec["xlo"], spec["xhi"],
                        spec["labels"])
        print(f"{panel}: {len(bars)} bars, {nticks} ticks, "
              f"{scale:.3f} mg/L per pixel")
        for bar in bars:
            label = bar["label"]
            stated = STATED.get((panel, label))
            clones, dels = clones_of(panel, label)
            background = spec["background"]
            if panel == "figS4" and not label.startswith("rfaY"):
                background = "F"
            control = spec["control"]
            if panel == "fig5a":
                control = "rfaY-yafL" if "yafL" in label else "rfaY-rimM"
                if label == control:
                    control = "RF"
            response.append(dict(
                measurement_id=f"{panel}:{label}",
                strain_id=label, figure=panel, background=background,
                clones=clones, deletions=dels, control_strain=control or "",
                ffa_mg_L=round(stated[0], 1) if stated else bar["value"],
                ffa_source="stated" if stated else "digitised",
                ffa_digitised_mg_L=bar["value"],
                significance=spec["marks"].get(label, ""),
                bar_colour=bar["colour"], note=spec["note"]))
            if stated:
                anchors.append(dict(
                    figure=panel, strain_id=label,
                    stated_mg_L=round(stated[0], 1),
                    digitised_mg_L=bar["value"],
                    residual_mg_L=round(bar["value"] - stated[0], 1),
                    residual_pct=round(100 * (bar["value"] - stated[0]) / stated[0], 2),
                    basis=stated[1]))

    for label, clones, dels in FIG1D:
        stated = STATED[("fig1d", label)]
        response.append(dict(
            measurement_id=f"fig1d:{label}",
            strain_id=label, figure="fig1d", background="F", clones=clones,
            deletions=dels, control_strain="F0", ffa_mg_L=round(stated[0], 1),
            ffa_source="stated", ffa_digitised_mg_L="",
            significance=FIG1D_MARKS.get(label, ""), bar_colour="",
            note="rfaY deletion and complementation; axis is broken between "
                 "1000 and 1500 so the bars are not digitised"))

    for rows, name in ((response, "ffa_response.tsv"), (anchors, "ffa_anchors.tsv")):
        path = args.out / name
        with path.open("w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=list(rows[0]), delimiter="\t")
            w.writeheader()
            w.writerows(rows)
        print(f"wrote {path} ({len(rows)} rows)")

    res = np.array([abs(a["residual_mg_L"]) for a in anchors])
    pct = np.array([abs(a["residual_pct"]) for a in anchors])
    print(f"anchor residual: median {np.median(res):.1f} mg/L "
          f"({np.median(pct):.2f}%), max {res.max():.1f} mg/L ({pct.max():.2f}%)")


if __name__ == "__main__":
    main()

"""Does the input predict the assembly? Read QC against assembly QC, 2x2.

SPEC
  One point per (library, assembler): a read metric on x, an assembly metric on
  y, coloured by assembler.

      x = mean Phred            x = total reads (Gbp)
  y = total assembly length     [0,0]              [0,1]
  y = contig N50                [1,0]              [1,1]

  The two assemblers of a library share an x -- the read metric is per library --
  so their points are joined by a thin rule and read as one sample. 35 libraries,
  70 points.

READS ARE POST-DEPLETION
  The x axis is the reads that actually fed the assembler: host-filtered, which
  is the only read QC this run recorded. There is no raw-versus-filtered pair
  here and no trimming report, so this figure cannot and does not say what QC
  removed -- it says what the assembler was given and what it returned.

INPUT   data/fabfos/runs/scadc_fosmids/assembly/assembly_stats/assembly_summary.tsv
        data/fabfos/runs/scadc_fosmids/assembly/assembly_stats/read_qc_stats/*.json
ENV     mamba run -n figure-net python main/figures/inserts/readqc_vs_assembly.py
OUT     cache/readqc_vs_assembly_grid.{png,svg}
        cache/readqc_vs_assembly_legend.{png,svg}
"""
import argparse
import json

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt          # noqa: E402
import numpy as np                       # noqa: E402
import pandas as pd                      # noqa: E402
from matplotlib.colors import to_rgba    # noqa: E402
from matplotlib.lines import Line2D      # noqa: E402
from matplotlib.ticker import FuncFormatter, LogLocator, NullFormatter  # noqa: E402

from _common import (ASM_COLOR, ASM_LABEL, ASSEMBLERS, ASSEMBLY, INK, save)

SUMMARY = ASSEMBLY / "assembly_stats" / "assembly_summary.tsv"
READ_QC = ASSEMBLY / "assembly_stats" / "read_qc_stats"


def build():
    df = pd.read_csv(SUMMARY, sep="\t")
    qual = {}
    for p in sorted(READ_QC.glob("*.json")):
        qual[p.stem] = json.loads(p.read_text())["mean_quality"]
    missing = sorted(set(df["pool"]) - set(qual))
    if missing:
        raise SystemExit(f"no read QC for {len(missing)} libraries, e.g. {missing[:3]}")

    df["avg_qual"] = df["pool"].map(qual)
    df["bases_gbp"] = df["bases"] / 1e9
    df["len_kb"] = df["length"] / 1e3
    df["n50_kb"] = df["N50"] / 1e3
    return df


def _kb_label(v, _pos=None):
    if v <= 0:
        return ""
    return f"{v / 1000:g}Mb" if v >= 1000 else f"{v:g}kb"


def _plain(v, _pos=None):
    return f"{v:g}"


def _panel(ax, df, xkey, xscale, ykey, yscale):
    for _pool, pts in df.groupby("pool"):
        if len(pts) >= 2:
            ax.plot(pts[xkey], pts[ykey], color=INK, lw=0.43, zorder=1)
    for asm in ASSEMBLERS:
        sub = df[df["assembler"] == asm]
        ax.scatter(sub[xkey], sub[ykey], s=48, linewidths=0.43,
                   facecolors=to_rgba(ASM_COLOR[asm], 0.6), edgecolors=INK, zorder=3)
    ax.set_xscale(xscale)
    ax.set_yscale(yscale)
    ax.grid(False)
    ax.tick_params(labelsize=9)
    for sp in ("top", "right"):
        ax.spines[sp].set_visible(False)


def generate():
    df = build()

    fig = plt.figure(figsize=(8.4, 7.2), dpi=300)
    gs = fig.add_gridspec(2, 2, hspace=0.12, wspace=0.12)
    a00 = fig.add_subplot(gs[0, 0])
    a01 = fig.add_subplot(gs[0, 1], sharey=a00)
    a10 = fig.add_subplot(gs[1, 0], sharex=a00)
    a11 = fig.add_subplot(gs[1, 1], sharex=a01, sharey=a10)

    _panel(a00, df, "avg_qual", "linear", "len_kb", "log")
    _panel(a01, df, "bases_gbp", "log", "len_kb", "log")
    _panel(a10, df, "avg_qual", "linear", "n50_kb", "log")
    _panel(a11, df, "bases_gbp", "log", "n50_kb", "log")

    for ax in (a00, a10):
        ax.yaxis.set_major_locator(LogLocator(base=10, subs=(1.0,)))
        ax.yaxis.set_major_formatter(FuncFormatter(_kb_label))
        ax.yaxis.set_minor_locator(LogLocator(base=10, subs=tuple(range(2, 10))))
        ax.yaxis.set_minor_formatter(NullFormatter())
    for ax in (a01, a11):
        ax.xaxis.set_major_locator(LogLocator(base=10, subs=(1.0, 2.0, 5.0)))
        ax.xaxis.set_major_formatter(FuncFormatter(_plain))
        ax.xaxis.set_minor_locator(
            LogLocator(base=10, subs=(3.0, 4.0, 6.0, 7.0, 8.0, 9.0)))
        ax.xaxis.set_minor_formatter(NullFormatter())

    for ax in (a00, a01):
        ax.tick_params(axis="x", which="both", labelbottom=False, length=0)
    for ax in (a01, a11):
        ax.tick_params(axis="y", which="both", labelleft=False, length=0)
    a00.spines["bottom"].set_visible(False)
    a01.spines["bottom"].set_visible(False)
    a01.spines["left"].set_visible(False)
    a11.spines["left"].set_visible(False)

    a10.set_xlabel("Mean phred (host-filtered reads)", fontsize=10.5)
    a11.set_xlabel("Total reads (Gbp)", fontsize=10.5)
    a00.set_ylabel("Total assembly length", fontsize=10.5)
    a10.set_ylabel("Contig N50", fontsize=10.5)
    fig.align_ylabels((a00, a10))

    save(fig, "readqc_vs_assembly_grid")
    plt.close(fig)

    n_per = {a: int((df["assembler"] == a).sum()) for a in ASSEMBLERS}
    handles = [Line2D([], [], marker="o", ls="", markersize=9,
                      markerfacecolor=to_rgba(ASM_COLOR[a], 0.6),
                      markeredgecolor=INK, markeredgewidth=0.43,
                      label=f"{ASM_LABEL[a]}  (n={n_per[a]})")
               for a in ASSEMBLERS]
    legfig = plt.figure(figsize=(2.4, 1.0), dpi=300)
    legfig.legend(handles=handles, loc="center", frameon=False,
                  fontsize=11, labelspacing=1.4, handletextpad=0.5)
    save(legfig, "readqc_vs_assembly_legend")
    plt.close(legfig)

    q, b = df["avg_qual"], df["bases_gbp"]
    L, n = df["len_kb"], df["n50_kb"]
    print(f"{len(df)} points across {df['pool'].nunique()} libraries; "
          f"phred {q.min():.1f}-{q.max():.1f}, reads {b.min():.2f}-{b.max():.2f} Gbp, "
          f"len {L.min():.0f}-{L.max():.0f} kb, N50 {n.min():.1f}-{n.max():.1f} kb")
    for asm in ASSEMBLERS:
        s = df[df["assembler"] == asm]
        print(f"  {asm:8s} median length {np.median(s['len_kb']):.0f} kb, "
              f"median N50 {np.median(s['n50_kb']):.1f} kb, "
              f"median mapped {s['fraction_reads_mapped'].median():.3f}")


def main():
    argparse.ArgumentParser(description=__doc__.splitlines()[0]).parse_args()
    generate()


if __name__ == "__main__":
    main()

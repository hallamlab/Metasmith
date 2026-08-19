import csv
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent
CACHE = HERE / "cache"
TSV = CACHE / "recall_sweep.tsv"

C_SIL = "#636EFA"
C_CLONE = "#00CC96"
C_READ = "#AB63FA"
C_MARK = "#212121"
C_FAINT = "#9E9E9E"

REF = "recall_0.99"
TARGET = 0.99
LIB_DEFAULT = 0.99


def load():
    if not TSV.exists():
        raise SystemExit(f"no {TSV} -- run gen_recall_sweep.py first")
    with open(TSV) as fh:
        rows = [{k: float(v) for k, v in r.items()}
                for r in csv.DictReader(fh, delimiter="\t")]
    rows.sort(key=lambda r: r["k"])
    return rows


def main():
    rows = load()
    has_read = "read_recall" in rows[0]
    k = np.array([r["k"] for r in rows])
    sil = np.array([r["silhouette"] for r in rows])
    ident = np.array([r["identity"] for r in rows])
    clone = np.array([r[REF] for r in rows])

    peak = max(rows, key=lambda r: r["silhouette"])
    ok = [r for r in rows if r[REF] >= TARGET]
    front = max(ok, key=lambda r: r["silhouette"]) if ok else None
    lib = min(rows, key=lambda r: abs(r["identity"] - LIB_DEFAULT))

    marks = [m["k"] for m in (peak, front, lib) if m]
    lo, hi = max(2, min(marks) - 45), max(marks) + 45
    win = (k >= lo) & (k <= hi)

    fig, ax = plt.subplots(figsize=(8.2, 5.2))
    ax.plot(k[win], sil[win], "-", color=C_SIL, lw=1.4, label="silhouette")
    ax.set_xlabel("N Clusters", fontsize=12)
    ax.set_ylabel("Silhouette", color=C_SIL, fontsize=12)
    ax.tick_params(axis="y", labelcolor=C_SIL, color=C_SIL, labelsize=10)
    ax.tick_params(axis="x", labelsize=10)
    ax.spines["left"].set_color(C_SIL)
    ax.spines["top"].set_visible(False)
    ax.set_xlim(lo, hi)
    s_lo = np.floor((sil[win].min() - 0.02) * 20) / 20
    s_hi = np.ceil((sil[win].max() + 0.02) * 20) / 20
    ax.set_ylim(s_lo, s_hi)

    ax2 = ax.twinx()
    ax2.plot(k[win], clone[win], "-", color=C_CLONE, lw=1.8,
             label=f"clone recall (R={REF.split('_')[1]})")
    if has_read:
        rr = np.array([r["read_recall"] for r in rows])
        ax2.plot(k[win], rr[win], "-", color=C_READ, lw=1.4, label="read recall (pairs)")
    ax2.axhline(TARGET, color=C_FAINT, ls="--", lw=1.0)
    ax2.set_ylabel("Recall", color=C_CLONE, fontsize=12)
    ax2.tick_params(axis="y", labelcolor=C_CLONE, color=C_CLONE, labelsize=10)
    ax2.spines["right"].set_color(C_CLONE)
    ax2.spines["left"].set_color(C_SIL)
    ax2.spines["top"].set_visible(False)
    r_lo = min(0.98 if has_read else 1.0, float(clone[win].min()))
    ax2.set_ylim(np.floor((r_lo - 0.02) * 20) / 20, 1.005)

    axt = ax.twiny()
    axt.set_xlim(lo, hi)
    ticks = [t for t in ax.get_xticks() if lo <= t <= hi]
    axt.set_xticks(ticks)
    axt.set_xticklabels([f"{np.interp(t, k, ident):.3f}" for t in ticks], fontsize=9)
    axt.set_xlabel("Between-cluster identity", fontsize=11, labelpad=6)
    axt.spines["left"].set_color(C_SIL)
    axt.spines["right"].set_color(C_CLONE)

    cuts = [(peak, "--", C_MARK, "silhouette peak\n(ships today)"),
            (front, "-", C_CLONE, f"recall >= {TARGET:g}"),
            (lib, ":", C_FAINT, f"library default\nidentity {LIB_DEFAULT:g}")]
    seen = set()
    for m, style, colour, _tag in cuts:
        if m is None or m["k"] in seen:
            continue
        seen.add(m["k"])
        ax.axvline(m["k"], color=colour, ls=style, lw=1.3)

    h1, l1 = ax.get_legend_handles_labels()
    h2, l2 = ax2.get_legend_handles_labels()
    ax.legend(h1 + h2, l1 + l2, loc="lower left", fontsize=9, frameon=True,
              framealpha=0.9, edgecolor="none")
    ax.set_title("Day-6 pool: clustering quality against clone and read recall",
                 fontsize=12, pad=32)

    fig.tight_layout(rect=(0, 0.13, 1, 1))
    for i, (m, _s, colour, tag) in enumerate(cuts):
        if m is None:
            continue
        line = (f"{tag.replace(chr(10), ' '):32s}N={int(m['k']):3d}   "
                f"identity={m['identity']:.3f}   silhouette={m['silhouette']:.3f}   "
                f"clone recall={m[REF]:.3f}")
        if has_read:
            line += f"   read recall={m['read_recall']:.3f}"
        fig.text(0.055, 0.085 - i * 0.031, line, fontsize=8, family="monospace",
                 color=colour, va="top", ha="left")

    for ext in ("png", "svg"):
        out = CACHE / f"recall_sweep.{ext}"
        fig.savefig(out, dpi=200, bbox_inches="tight")
        print(f"  wrote {out.relative_to(REPO)}")
    plt.close(fig)

    print(f"  silhouette peak  N={int(peak['k'])} id={peak['identity']:.4f} "
          f"sil={peak['silhouette']:.4f} clone={peak[REF]:.4f}"
          + (f" read={peak['read_recall']:.4f}" if has_read else ""))
    if front:
        print(f"  recall frontier  N={int(front['k'])} id={front['identity']:.4f} "
              f"sil={front['silhouette']:.4f} clone={front[REF]:.4f}"
              + (f" read={front['read_recall']:.4f}" if has_read else ""))
    print(f"  library default  N={int(lib['k'])} id={lib['identity']:.4f} "
          f"sil={lib['silhouette']:.4f} clone={lib[REF]:.4f}"
          + (f" read={lib['read_recall']:.4f}" if has_read else ""))


if __name__ == "__main__":
    sys.exit(main())

import argparse
import os
import subprocess
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from scipy.cluster.hierarchy import fcluster, linkage
from scipy.spatial.distance import squareform
from sklearn.metrics import silhouette_score

HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent
CACHE = HERE / "cache"

sys.path.insert(0, str(REPO / "main" / "figures" / "inserts"))
import pieces as piece_lib      # noqa: E402  -- the shared piece rebuild

BLAST_BIN = Path(os.environ.get(
    "FABFOS_BLAST_BIN", "/home/tony/lib/miniforge3/envs/fabfos-bio/bin"))
COLS = "qseqid sseqid sstart send pident length nident qlen slen".split()

MIN_LEN = 10_000
CUT_ID = 0.99
K_MARGIN = 60


C_SIL = "#636EFA"
C_THR = "#EF553B"

METRICS = [
    ("qlen", "legacy   nident / qlen"),
    ("max",  "symmetric   nident / max(qlen, slen)"),
]


def input_pieces():
    if piece_lib.MIN_LEN != MIN_LEN:
        raise SystemExit(f"the shared rebuild floors at {piece_lib.MIN_LEN:,} bp, "
                         f"this sweep at {MIN_LEN:,}")
    seqs, _actions = piece_lib.by_piece_id()
    print(f"  {len(seqs):,} pieces >= {MIN_LEN:,} bp")
    return seqs


def blast_table(seqs, tag="pieces_ge10kb"):
    CACHE.mkdir(parents=True, exist_ok=True)
    tsv = CACHE / f"{tag}.tsv"
    if tsv.exists():
        print(f"  blast {tag}: cached ({len(seqs):,} seqs)")
        return tsv
    ws = CACHE / tag
    ws.mkdir(exist_ok=True)
    fa = ws / "seqs.fna"
    with open(fa, "w") as f:
        for k, v in seqs.items():
            f.write(f">{k}\n{v}\n")
    subprocess.run([str(BLAST_BIN / "makeblastdb"), "-dbtype", "nucl",
                    "-in", str(fa), "-out", str(ws / "db")],
                   check=True, capture_output=True)
    subprocess.run([str(BLAST_BIN / "blastn"), "-num_threads", "8",
                    "-evalue", "1000", "-perc_identity", "50",
                    "-query", str(fa), "-db", str(ws / "db"),
                    "-outfmt", "6 " + " ".join(COLS), "-out", str(tsv)],
                   check=True, capture_output=True)
    print(f"  blast {tag}: {len(seqs):,} seqs -> {tsv.name}")
    return tsv


def identity_matrix(tsv, labels, norm):
    idx = {k: i for i, k in enumerate(labels)}
    n = len(labels)
    sim = np.zeros((n, n))
    with open(tsv) as fh:
        for line in fh:
            f = line.rstrip("\n").split("\t")
            if len(f) != len(COLS):
                continue
            r = dict(zip(COLS, f))
            q, s = r["qseqid"], r["sseqid"]
            if q not in idx or s not in idx:
                continue
            qlen, slen = int(r["qlen"]), int(r["slen"])
            denom = qlen if norm == "qlen" else max(qlen, slen)
            if not denom:
                continue
            v = int(r["nident"]) / denom
            i, j = idx[q], idx[s]
            if v > sim[i, j]:
                sim[i, j] = v
    sim = np.maximum(sim, sim.T)
    np.fill_diagonal(sim, 1.0)
    return sim


def sweep(sim):
    n = sim.shape[0]
    dist = 1.0 - sim
    np.fill_diagonal(dist, 0.0)
    Z = linkage(squareform(dist, checks=False), method="complete")
    ks, sils, thr = [], [], []
    for h in np.unique(Z[:, 2]):
        assignment = fcluster(Z, h, criterion="distance")
        k = len(set(assignment))
        if k < 2 or k >= n:
            continue
        ks.append(k)
        sils.append(silhouette_score(dist, assignment, metric="precomputed"))
        thr.append(1.0 - h)
    ks, sils, thr = np.array(ks), np.array(sils), np.array(thr)
    order = np.argsort(ks)
    ks, sils, thr = ks[order], sils[order], thr[order]
    peak = int(np.argmax(sils))
    k_cut = len(set(fcluster(Z, 1.0 - CUT_ID, criterion="distance")))
    return dict(ks=ks, sils=sils, thr=thr, k_cut=k_cut,
                k_best=int(ks[peak]), id_best=float(thr[peak]),
                sil_best=float(sils[peak]))


def draw_panel(ax, res, title, xlim, ylim, show_ylabel, show_y2label):
    ks, sils, thr, k_cut = res["ks"], res["sils"], res["thr"], res["k_cut"]
    lo, hi = ylim

    ax.plot(ks, sils, "-", color=C_SIL, lw=1.0)
    ax.set_xlabel("N Clusters", fontsize=12)
    if show_ylabel:
        ax.set_ylabel("Silhouette", color=C_SIL, fontsize=12)
    ax.tick_params(axis="x", labelsize=10)
    ax.tick_params(axis="y", labelsize=10, labelcolor=C_SIL, color=C_SIL)
    ax.spines["left"].set_color(C_SIL)
    ax.set_ylim(lo, hi)
    ax.set_yticks([lo, (lo + hi) / 2, hi])
    ax.set_xlim(*xlim)
    step = max(20, int(round((xlim[1] - xlim[0]) / 4 / 20)) * 20)
    ax.set_xticks(np.arange(xlim[0], xlim[1] + 1, step))
    ax.set_title(title, fontsize=11, pad=8)

    ax2 = ax.twinx()
    ax2.plot(ks, thr, "-", color=C_THR, lw=1.0)
    if show_y2label:
        ax2.set_ylabel("Between Cluster Identity", color=C_THR, fontsize=12)
    ax2.tick_params(axis="y", labelsize=10, labelcolor=C_THR, color=C_THR)
    ax2.spines["right"].set_color(C_THR)
    ax2.spines["left"].set_color(C_SIL)
    ax2.set_ylim(0.0, 1.0)

    ax.axvline(res["k_best"], color="#212121", ls="--", lw=1.4)
    ax.text(res["k_best"] - (xlim[1] - xlim[0]) * 0.015, lo + (hi - lo) * 0.40,
            f"N = {res['k_best']}\nIdentity = {res['id_best']:.3f}\n"
            f"Silhouette = {res['sil_best']:.3f}",
            fontsize=10, color="#212121", va="top", ha="right")
    ax.axvline(k_cut, color="#9E9E9E", ls=":", lw=1.0)
    ax.text(k_cut + (xlim[1] - xlim[0]) * 0.012, lo + (hi - lo) * 0.06,
            f"identity {CUT_ID:g}\nN = {k_cut}",
            fontsize=8, color="#9E9E9E", va="bottom", ha="left")
    for a in (ax, ax2):
        a.spines["top"].set_visible(False)

    print(f"    {title}: peak silhouette {res['sil_best']:.4f} at N={res['k_best']} "
          f"(identity {res['id_best']:.4f});  fixed {CUT_ID:g} cut would give N={k_cut}")


def main():
    global CUT_ID
    ap = argparse.ArgumentParser(description="cluster-quality sweep, one panel per metric")
    ap.add_argument("--identity", type=float, default=CUT_ID,
                    help="the identity cut to mark (default 0.99)")
    a = ap.parse_args()
    CUT_ID = a.identity
    stem = "clustering_sweep" if CUT_ID == 0.99 else f"clustering_sweep.id{CUT_ID:g}".replace("0.", "")

    print(f"clustering sweep (marking identity {CUT_ID:g})")
    seqs = input_pieces()
    labels = sorted(seqs)
    tsv = blast_table(seqs)

    sims = {norm: identity_matrix(tsv, labels, norm) for norm, _t in METRICS}
    res = {norm: sweep(sims[norm]) for norm, _t in METRICS}
    marks = [r[key] for r in res.values() for key in ("k_best", "k_cut")]
    k_lo = max(2, min(marks) - K_MARGIN)
    k_hi = max(marks) + K_MARGIN
    print(f"  markers at {sorted(marks)} -> N window {k_lo}-{k_hi}")
    inwin = {n: (r["ks"] >= k_lo) & (r["ks"] <= k_hi) for n, r in res.items()}
    smin = min(r["sils"][inwin[n]].min() for n, r in res.items())
    smax = max(r["sils"][inwin[n]].max() for n, r in res.items())
    pad = max(0.02, (smax - smin) * 0.15)
    ylim = (np.floor((smin - pad) * 20) / 20, np.ceil((smax + pad) * 20) / 20)

    fig, axes = plt.subplots(1, len(METRICS), figsize=(12.5, 5.0))
    for i, (norm, title) in enumerate(METRICS):
        draw_panel(axes[i], res[norm], title, (k_lo, k_hi), ylim,
                   show_ylabel=(i == 0), show_y2label=(i == len(METRICS) - 1))
    fig.tight_layout()
    CACHE.mkdir(parents=True, exist_ok=True)
    for ext in ("png", "svg"):
        out = CACHE / f"{stem}.{ext}"
        fig.savefig(out, dpi=200, bbox_inches="tight")
        print(f"  wrote {out.relative_to(REPO)}")
    plt.close(fig)


if __name__ == "__main__":
    sys.exit(main())

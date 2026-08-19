"""Cluster quality against granularity, and where the pipeline's cut lands.

SPEC
  x is the NUMBER OF CLUSTERS k, not an identity knob. The complete-linkage tree
  gives the exact identity threshold that produces each k for free -- cutting it
  to k clusters fixes the between-cluster identity at `1 - Z[n-k, 2]` -- so no
  parameter sweep is needed; the tree already encodes the monotone k <-> identity
  map. Silhouette (left y, blue) is evaluated at each k by cutting the tree; the
  back-calculated identity threshold (right y, red) is read off the merge height.
  The dashed line marks the cut the pipeline actually made, identity 0.99.

  Silhouette is mean-based and so robust. The naive min-within / max-between gap
  is degenerate under a coverage-blind identity, which reads a short piece
  contained in a longer one as 100% identical and pins max between-cluster
  identity at 1.0 whatever the cut. The metric drawn here is the pipeline's own,
  `nident / max(qlen, slen)`, which is symmetric and penalises length
  disagreement, so containment is not read as identity.

THE AXIS WINDOW IS DERIVED
  Both windows come from where the cut and the silhouette actually land. The
  scadc original hard-codes k 160-240 and silhouette 0.8-1.0 because it draws one
  known dataset; hard-coding them for a different piece set clipped a 0.72
  silhouette clean off the axis and showed an empty panel -- a failure that looks
  like a result.

INPUT   identity.py -> pieces.py (which read only ./data)
ENV     mamba run -n figure-net python main/figures/inserts/cluster_quality.py
OUT     cache/cluster_quality.{png,svg}
"""
import argparse

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt      # noqa: E402
import numpy as np                   # noqa: E402
from scipy.cluster.hierarchy import fcluster, linkage   # noqa: E402
from scipy.spatial.distance import squareform           # noqa: E402
from sklearn.metrics import silhouette_score            # noqa: E402

import identity
from _common import C_A, C_B, INK, save

CUT_ID = 0.99
K_MARGIN = 60


def tree():
    labels, sym, _cont = identity.matrices()
    dist = 1.0 - sym
    np.fill_diagonal(dist, 0.0)
    return labels, dist, linkage(squareform(dist, checks=False), method="complete")


def sweep():
    labels, dist, Z = tree()
    n = len(labels)
    k_cut = len(set(fcluster(Z, 1.0 - CUT_ID, criterion="distance")))

    ks, sils = [], []
    for k in range(2, min(n - 1, k_cut + K_MARGIN) + 1):
        assignment = fcluster(Z, k, criterion="maxclust")
        if len(set(assignment)) != k:
            continue
        ks.append(k)
        sils.append(silhouette_score(dist, assignment, metric="precomputed"))
    ks = np.array(ks)
    thr = 1.0 - Z[n - ks - 1, 2]
    return ks, np.array(sils), thr, k_cut, n


def generate():
    ks, sils, thr, k_cut, n = sweep()

    peak = int(ks[int(np.argmax(sils))])
    x_lo = max(int(ks[0]), min(peak, k_cut) - 40)
    x_hi = int(ks[-1])
    win = (ks >= x_lo) & (ks <= x_hi)

    fig, ax = plt.subplots(figsize=(6.6, 5.0), dpi=300)
    ax.plot(ks, sils, "-", color=C_A, lw=1.0)
    ax.set_xlabel("N clusters", fontsize=12)
    ax.set_ylabel("Silhouette", color=C_A, fontsize=12)
    ax.tick_params(axis="x", labelsize=10)
    ax.tick_params(axis="y", labelsize=10, labelcolor=C_A, color=C_A)
    ax.spines["left"].set_color(C_A)
    ax.set_xlim(x_lo, x_hi)

    lo = np.floor(sils[win].min() * 20) / 20
    hi = min(1.0, np.ceil(sils[win].max() * 20) / 20)
    ax.set_ylim(lo, hi)
    ax.set_yticks(np.round(np.linspace(lo, hi, 3), 2))
    ax.set_xticks(np.linspace(x_lo, x_hi, 5).round().astype(int))

    ax2 = ax.twinx()
    ax2.plot(ks, thr, "-", color=C_B, lw=1.0)
    ax2.set_ylabel("Between cluster identity", color=C_B, fontsize=12)
    ax2.tick_params(axis="y", labelsize=10, labelcolor=C_B, color=C_B)
    ax2.spines["right"].set_color(C_B)
    ax2.spines["left"].set_color(C_A)
    ax2.set_ylim(0.0, 1.0)

    ax.axvline(k_cut, color=INK, ls="--", lw=1.2)
    ax.text(k_cut - (x_hi - x_lo) * 0.02, lo + (hi - lo) * 0.25,
            f"N = {k_cut}\nIdentity = {CUT_ID:g}",
            fontsize=10, color=INK, va="top", ha="right")
    for a in (ax, ax2):
        a.spines["top"].set_visible(False)

    save(fig, "cluster_quality")
    plt.close(fig)

    best = int(np.argmax(sils))
    print(f"{n} pieces; the {CUT_ID:g} cut is {k_cut} clusters "
          f"(silhouette {sils[ks == k_cut][0]:.3f})")
    print(f"silhouette peaks at k={ks[best]} ({sils[best]:.3f}, "
          f"between-cluster identity {thr[best]:.3f})")


def main():
    argparse.ArgumentParser(description=__doc__.splitlines()[0]).parse_args()
    generate()


if __name__ == "__main__":
    main()

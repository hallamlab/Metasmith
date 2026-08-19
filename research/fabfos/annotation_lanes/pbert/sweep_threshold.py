"""THE QUESTION -- what does the pbert lane need to see before it is allowed to speak?

The lane's only knob today is PBERT_FLOOR, and it thresholds a vote share that was
normalised within the top-K -- a scale-free quantity that cannot say "this ORF has no
good neighbour". The quantity that can is the top-1 similarity, which lane_embed
computes and discards. This sweeps both axes together and names the operating point.

SCOPE   DH10B, the 1,288 adjudicable ORFs, against the repaired pool (see
        repair_pool_index.py -- on the shipped pool every number here is noise).
        Three leakage conditions as in sweep_metrics.py.
INPUT   research/fabfos/annotation_lanes/pbert/cache/knn_<metric>.npz
METHOD  The distance axis is driven off the observed quantile grid of top-1
        similarity, because each metric's similarity has its own scale. The vote is
        computed once per condition and then filtered, so the surface is exact rather
        than sampled. Scored ORF-level in EC space; see _common.py.
ENV     PYTHONPATH="$PWD/src" mamba run -n msm python \
            research/fabfos/annotation_lanes/pbert/sweep_threshold.py --metric cosine
OUT     research/fabfos/annotation_lanes/pbert/threshold_<metric>_dh10b.tsv  (committed)
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import _knn  # noqa: E402
from _common import (CACHE, HERE, load_cohort, load_pool, mnxr_to_ec,  # noqa: E402
                     score_label_level, score_orf_level)

FLOORS = [0.0, 0.05, 0.10, 0.15, 0.20, 0.25, 0.30, 0.35, 0.40, 0.45,
          0.50, 0.55, 0.60, 0.65, 0.70, 0.75, 0.80, 0.85, 0.90]
QUANTILES = [0.0, 0.05, 0.10, 0.15, 0.20, 0.25, 0.30, 0.35, 0.40, 0.45, 0.50,
             0.55, 0.60, 0.65, 0.70, 0.75, 0.80, 0.85, 0.90, 0.95]
CONDITIONS = [("pool", None, None), ("self", "drop", None), ("twin", "drop", 0.99)]
PRECISION_TARGET = 0.88


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--metric", default="cosine")
    a = ap.parse_args()

    z = np.load(CACHE / f"knn_{a.metric}.npz", allow_pickle=True)
    idx, val, cos, orf, own = z["idx"], z["val"], z["cos"], z["orf"], z["own"]
    _, _, labels = load_pool()
    coh = load_cohort()
    truth_ec = {o: t for o, t in zip(coh["orf"], coh["ec"]) if o in set(orf)}
    truth_mx = {o: t for o, t in zip(coh["orf"], coh["mnxr"]) if o in set(orf)}
    m2e = mnxr_to_ec()

    rows = []
    for cond, drop, twin in CONDITIONS:
        I, Cc = _knn.refine(idx, cos, drop_col=(own if drop else None), twin_cut=twin)
        # The gate reads the RETRIEVAL metric's own top-1 value, which survives the
        # condition's exclusions -- a hidden twin must not count as a good neighbour.
        nn = _first_surviving(val, idx, I)
        full = _knn.vote(I, Cc, labels, 0.0)
        grid = [float(np.quantile(nn[np.isfinite(nn)], q)) for q in QUANTILES]
        for qi, thr in zip(QUANTILES, grid):
            gated = nn >= thr
            for fl in FLOORS:
                pm = {o: {k for k, v in d.items() if v >= fl} if g else set()
                      for o, d, g in zip(orf, full, gated)}
                pe = {o: {e for m in s for e in m2e.get(m, ())} for o, s in pm.items()}
                r = dict(metric=a.metric, condition=cond, nn_quantile=qi,
                         nn_threshold=round(thr, 4), floor=fl,
                         **score_orf_level(pe, truth_ec),
                         **{f"mnxr_{k}": v for k, v in score_label_level(pm, truth_mx).items()})
                rows.append(r)
    out = pd.DataFrame(rows)
    p = HERE / f"threshold_{a.metric}_dh10b.tsv"
    out.to_csv(p, sep="\t", index=False)

    for cond in ("pool", "self", "twin"):
        s = out[out["condition"] == cond]
        b = s.loc[s["f1"].idxmax()]
        print(f"[{cond}] best F1 {b['f1']:.4f}  P={b['precision']:.4f} R={b['recall']:.4f} "
              f"cov={b['coverage']:.3f}  at nn>={b['nn_threshold']:.4f} (q{b['nn_quantile']:.2f}) "
              f"floor={b['floor']}")
        ok = s[s["precision"] >= PRECISION_TARGET]
        if len(ok):
            t = ok.loc[ok["recall"].idxmax()]
            print(f"        P>={PRECISION_TARGET}: P={t['precision']:.4f} R={t['recall']:.4f} "
                  f"F1={t['f1']:.4f} cov={t['coverage']:.3f} at nn>={t['nn_threshold']:.4f} "
                  f"floor={t['floor']}")
        else:
            print(f"        no cell reaches precision {PRECISION_TARGET}; "
                  f"max precision {s['precision'].max():.4f}")
    print(f"wrote {p}")


def _first_surviving(val, idx_wide, I):
    """The retrieval value of each query's best surviving neighbour."""
    pos = {}
    out = np.full(len(I), -np.inf, np.float32)
    for i in range(len(I)):
        if I[i, 0] < 0:
            continue
        w = np.where(idx_wide[i] == I[i, 0])[0]
        if len(w):
            out[i] = val[i, w[0]]
    return out


if __name__ == "__main__":
    main()

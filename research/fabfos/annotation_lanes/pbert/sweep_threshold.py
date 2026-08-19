"""THE QUESTION -- what does the pbert lane need to see before it is allowed to speak?

The lane's only knob used to be PBERT_FLOOR, which thresholds a vote share normalised
within the retrieved top-K. That is a scale-free quantity: it measures whether the
neighbours AGREE, not whether any of them is close, so thirty neighbours at cosine
0.15 that agree score 1.0 and "this ORF has no good neighbour" is inexpressible.

Three knobs now, and this sweeps them together:

  nn_min   an ORF whose nearest landmark is below this gets NO call at all
  tau      a neighbour votes only if it is also within this fraction of that best
           cosine -- so a dense neighbourhood votes with many and a thin one with one
  k_max    how many candidates are retrieved before either cut applies
  floor    the incumbent label-level cut on the vote share, re-tuned rather than kept

NN_MIN IS SWEPT IN ABSOLUTE COSINE, NOT IN QUANTILES. The previous grid drove the
gate off observed quantiles of the top-1 similarity and went blind above q0.15: the
landmark set contains exact sequence twins, so the quantiles saturate at 1.0 and
every cell from q0.20 up is the same cell.

SCOPE   DH10B, the 1,288 adjudicable ORFs, against ref::label_transfer_landmarks
        (222,019 references). Three leakage conditions as in sweep_metrics.py; the
        `twin` row is the honest read for an ORF with no close relative.
INPUT   research/fabfos/annotation_lanes/pbert/cache/knn_<metric>.npz
METHOD  The admitted set does not depend on `floor`, so the vote is computed once per
        (nn_min, tau, k_max) and then filtered -- the floor axis is exact, not
        sampled. Scored ORF-level in EC space AND label-level in MNXR space, because
        the two figures this lane gets quoted against were measured on different
        axes; see _common.py. Call counts are reported beside every cell.
        `check_vote_matches_lane.py` proves this vote IS gpr_4lane.py's.
ENV     PYTHONPATH="$PWD/src" mamba run -n msm python \
            research/fabfos/annotation_lanes/pbert/sweep_threshold.py [--metric cosine]
OUT     research/fabfos/annotation_lanes/pbert/threshold_<metric>_dh10b.tsv (committed)
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

# COARSE ON PURPOSE. Four knobs against 1,288 adjudicable ORFs overfits on a fine
# grid; the operating point has to survive the `twin` condition, not win a cell.
NN_MINS = [0.00, 0.60, 0.65, 0.70, 0.75, 0.7716, 0.80, 0.85, 0.90]
TAUS = [0.00, 0.90, 0.95, 0.98, 0.995]
K_MAXES = [5, 10, 30]
FLOORS = [0.00, 0.05, 0.10, 0.20, 0.30, 0.50]
CONDITIONS = [("pool", None, None), ("self", "drop", None), ("twin", "drop", 0.99)]
PRECISION_TARGET = 0.88
# The pre-quota rule, as a row in its own panel.
INCUMBENT = (0.00, 0.00, 30, 0.20)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--metric", default="cosine")
    a = ap.parse_args()

    z = np.load(CACHE / f"knn_{a.metric}.npz", allow_pickle=True)
    idx, cos, orf, own = z["idx"], z["cos"], z["orf"], z["own"]
    _, acc, labels = load_pool()
    if idx.max() >= len(acc):
        raise SystemExit(
            f"[threshold] the cached neighbours index up to row {idx.max():,} of a "
            f"pool with {len(acc):,} references. The cache was built against a "
            f"different landmark set, so every neighbour it names is another "
            f"protein -- re-run sweep_metrics.py")
    coh = load_cohort()
    truth_ec = {o: t for o, t in zip(coh["orf"], coh["ec"]) if o in set(orf)}
    truth_mx = {o: t for o, t in zip(coh["orf"], coh["mnxr"]) if o in set(orf)}
    m2e = mnxr_to_ec()
    kwide = max(K_MAXES)

    rows = []
    for cond, drop, twin in CONDITIONS:
        I, Cc = _knn.refine(idx, cos, k=kwide,
                            drop_col=(own if drop else None), twin_cut=twin)
        for nn_min in NN_MINS:
            for tau in TAUS:
                for k_max in K_MAXES:
                    admitted = np.zeros(len(I), np.int32)
                    full = _knn.vote(I, Cc, labels, 0.0, nn_min, tau, k_max,
                                     admitted=admitted)
                    fired = admitted > 0
                    for fl in FLOORS:
                        pm = {o: {k for k, v in d.items() if v >= fl}
                              for o, d in zip(orf, full)}
                        pe = {o: {e for m in s for e in m2e.get(m, ())}
                              for o, s in pm.items()}
                        n_calls = sum(len(s) for s in pm.values())
                        speaking = sum(1 for s in pm.values() if s)
                        rows.append(dict(
                            metric=a.metric, condition=cond,
                            nn_min=nn_min, tau=tau, k_max=k_max, floor=fl,
                            n_abstained=int((~fired).sum()),
                            mean_admitted=round(float(admitted[fired].mean()), 2)
                            if fired.any() else 0.0,
                            n_calls=n_calls,
                            calls_per_speaking_orf=round(n_calls / speaking, 3)
                            if speaking else 0.0,
                            **score_orf_level(pe, truth_ec),
                            **{f"mnxr_{k}": v for k, v in
                               score_label_level(pm, truth_mx).items()}))
    out = pd.DataFrame(rows)
    p = HERE / f"threshold_{a.metric}_dh10b.tsv"
    out.to_csv(p, sep="\t", index=False)

    def show(tag, r):
        print(f"{tag:26s} nn_min={r['nn_min']:<6} tau={r['tau']:<5} k={r['k_max']:<3} "
              f"floor={r['floor']:<4}  P={r['precision']:.4f} R={r['recall']:.4f} "
              f"F1={r['f1']:.4f} cov={r['coverage']:.3f}  "
              f"calls={r['n_calls']} ({r['calls_per_speaking_orf']}/ORF, "
              f"{r['mean_admitted']} nbrs)")

    key = ["nn_min", "tau", "k_max", "floor"]
    for cond, _, _ in CONDITIONS:
        s = out[out["condition"] == cond]
        print(f"\n--- {cond}")
        inc = s[(s[key] == pd.Series(INCUMBENT, index=key)).all(axis=1)].iloc[0]
        show("incumbent (pre-quota)", inc)
        show("best F1", s.loc[s["f1"].idxmax()])
        ok = s[s["precision"] >= PRECISION_TARGET]
        if len(ok):
            show(f"best R at P>={PRECISION_TARGET}", ok.loc[ok["recall"].idxmax()])
        else:
            print(f"  no cell reaches precision {PRECISION_TARGET}; "
                  f"max {s['precision'].max():.4f}")
    print(f"\nwrote {p}  ({len(out):,} cells)")


if __name__ == "__main__":
    main()

"""THE QUESTION -- which distance should the pbert lane retrieve its K neighbours with?

The lane inherited cosine from the scadc study, which only ever ran cosine. This
scores a panel of retrieval metrics on one axis, holding everything else at the
shipped configuration (K = 30, cosine-weighted vote, floor 0.20), so the incumbent
is a row in its own panel.

SCOPE   DH10B, the 1,288 adjudicable ORFs from build_cohort.py. Three leakage
        conditions per metric: `pool` (as deployed), `self` (the ORF's own
        Swiss-Prot accession hidden), `twin` (every neighbour at cosine >= 0.99
        hidden -- the cheap stand-in for the source study's DIAMOND cluster removal).
INPUT   data/fabfos/processed/label_transfer_landmarks/landmarks/landmarks.parquet
        data/fabfos/benchmarks/lane_dh10b/annotations/dh10b.pbert.{parquet,index.csv}
        research/fabfos/annotation_lanes/pbert/cohort_dh10b.tsv
METHOD  see _knn.py -- the panel varies retrieval only; the vote weighting stays cosine.
        Scored ORF-level in EC space (the seven-lane table's axis) and label-level in
        MNXR space (the embed-transfer report's axis); see _common.py.
ENV     PYTHONPATH="$PWD/src" mamba run -n msm python \
            research/fabfos/annotation_lanes/pbert/sweep_metrics.py [--with-l1]
OUT     research/fabfos/annotation_lanes/pbert/metrics_dh10b.tsv   (committed)
        research/fabfos/annotation_lanes/pbert/cache/knn_<metric>.npz  (gitignored)
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import _knn  # noqa: E402
from _common import (CACHE, HERE, K, PBERT_FLOOR, PBERT_NN_MIN,  # noqa: E402
                     PBERT_TAU, load_cohort, load_pool, load_query, mnxr_to_ec,
                     orf_to_accession, score_label_level, score_orf_level)

SHIPPED = dict(nn_min=PBERT_NN_MIN, tau=PBERT_TAU, k_max=K)
PRE_QUOTA = dict(nn_min=0.0, tau=0.0, k_max=30)
PRE_QUOTA_FLOOR = 0.20

CONDITIONS = [("pool", None, None), ("self", "drop", None), ("twin", "drop", 0.99)]
PANEL = ["cosine", "dot", "euclidean", "correlation", "zscore", "whiten"]


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--with-l1", action="store_true",
                    help="also measure L1 on a query subsample (no BLAS form; slow)")
    ap.add_argument("--l1-queries", type=int, default=200)
    ap.add_argument("--shipped-pool", action="store_true",
                    help="use the pool index as shipped (misaligned) -- the 'before' row")
    ap.add_argument("--vote", choices=("shipped", "pre-quota"), default="shipped",
                    help="'shipped' reads the quota and floor from fabfos_evidence; "
                         "'pre-quota' is the rule the lane ran before this work -- "
                         "top-30, no quota, floor 0.20. The 'before' rows need it, "
                         "because a before/after that changed the vote AND the pool "
                         "at once attributes the whole difference to whichever one "
                         "the reader already believed in.")
    a = ap.parse_args()
    CACHE.mkdir(exist_ok=True)
    quota = PRE_QUOTA if a.vote == "pre-quota" else SHIPPED
    floor = PRE_QUOTA_FLOOR if a.vote == "pre-quota" else PBERT_FLOOR
    print(f"[metrics] vote: {a.vote} {quota} floor={floor}", flush=True)

    coh = load_cohort()
    truth_ec = dict(zip(coh["orf"], coh["ec"]))
    truth_mnxr = dict(zip(coh["orf"], coh["mnxr"]))
    pool, acc, labels = load_pool(repaired=not a.shipped_pool)
    qemb, qid = load_query()
    keep = np.array([i for i, o in enumerate(qid) if o in truth_ec])
    qemb, qid = qemb[keep], qid[keep]
    print(f"[metrics] pool {pool.shape}  cohort queries {len(qid)}", flush=True)

    pos = {a_: i for i, a_ in enumerate(acc)}
    o2a = orf_to_accession()
    own = np.array([pos.get(o2a.get(o, ""), -1) for o in qid], np.int32)
    m2e = mnxr_to_ec()
    cos_ref = _knn._norm(pool)
    cos_q = _knn._norm(qemb)

    metrics = _knn.build_metrics(pool, qemb, names=PANEL)
    rows = []
    for name in PANEL + (["l1"] if a.with_l1 else []):
        t0 = time.time()
        if name == "l1":
            idx, val = _knn.topk_l1(pool, qemb[:a.l1_queries])
            sub = slice(0, a.l1_queries)
        else:
            idx, val = _knn.topk(metrics[name])
            sub = slice(None)
        # cosine of each retrieved neighbour -- the weighting stays cosine whatever
        # metric retrieved them, and the twin cut is defined on it too.
        cos = np.einsum("nd,nkd->nk", cos_q[sub], cos_ref[idx]).astype(np.float32)
        np.savez_compressed(CACHE / f"knn_{name}.npz", idx=idx, val=val, cos=cos,
                            orf=qid[sub], own=own[sub])
        for cond, drop, twin in CONDITIONS:
            I, Cc = _knn.refine(idx, cos, drop_col=(own[sub] if drop else None), twin_cut=twin)
            admitted = np.zeros(len(I), np.int32)
            votes = _knn.vote(I, Cc, labels, floor, admitted=admitted, **quota)
            pred_mnxr = {o: set(v) for o, v in zip(qid[sub], votes)}
            n_calls = sum(len(v) for v in pred_mnxr.values())
            speaking = sum(1 for v in pred_mnxr.values() if v)
            pred_ec = {o: {e for m in v for e in m2e.get(m, ())} for o, v in pred_mnxr.items()}
            t_ec = {o: truth_ec[o] for o in qid[sub]}
            t_mx = {o: truth_mnxr[o] for o in qid[sub]}
            top1 = float(np.mean([bool(pred_mnxr[o] & t_mx[o]) for o in qid[sub]]))
            rows.append(dict(metric=name, condition=cond, vote=a.vote,
                             pool="shipped" if a.shipped_pool else "repaired",
                             n_query=len(I), n_calls=n_calls,
                             calls_per_speaking_orf=round(n_calls / speaking, 3)
                             if speaking else 0.0,
                             n_abstained=int((admitted == 0).sum()),
                             median_nn=round(float(np.median(np.where(np.isfinite(val[:, 0]), val[:, 0], np.nan))), 4),
                             **score_orf_level(pred_ec, t_ec),
                             **{f"mnxr_{k}": v for k, v in score_label_level(pred_mnxr, t_mx).items()},
                             any_mnxr_hit=round(top1, 4)))
            r = rows[-1]
            print(f"[metrics] {name:11s} {cond:4s} EC P={r['precision']:.3f} R={r['recall']:.3f} "
                  f"F1={r['f1']:.3f} cov={r['coverage']:.3f}  MNXR F1={r['mnxr_macro_f1']:.3f}",
                  flush=True)
        print(f"[metrics] {name} done in {time.time()-t0:.1f}s", flush=True)

    out = pd.DataFrame(rows)
    if a.with_l1:
        # L1 is measured on the first --l1-queries ORFs only, so every metric is
        # re-scored on that same slice; comparing L1 to a full-cohort row would be
        # comparing two different denominators.
        sub = []
        for name_ in PANEL + ["l1"]:
            z = np.load(CACHE / f"knn_{name_}.npz", allow_pickle=True)
            n_ = a.l1_queries
            for cond, drop, twin in CONDITIONS:
                I, Cc = _knn.refine(z["idx"][:n_], z["cos"][:n_],
                                    drop_col=(z["own"][:n_] if drop else None), twin_cut=twin)
                votes = _knn.vote(I, Cc, labels, PBERT_FLOOR, **SHIPPED)
                o_ = z["orf"][:n_]
                pm = {o: set(v) for o, v in zip(o_, votes)}
                pe = {o: {e for m in v for e in m2e.get(m, ())} for o, v in pm.items()}
                sub.append(dict(metric=name_, condition=cond, n_query=len(I),
                                **score_orf_level(pe, {o: truth_ec[o] for o in o_}),
                                **{f"mnxr_{k}": v for k, v in
                                   score_label_level(pm, {o: truth_mnxr[o] for o in o_}).items()}))
        pd.DataFrame(sub).to_csv(HERE / "metrics_dh10b_l1subset.tsv", sep="\t", index=False)
        print(f"wrote {HERE/'metrics_dh10b_l1subset.tsv'}")

    name = "metrics_dh10b_shippedpool.tsv" if a.shipped_pool else "metrics_dh10b.tsv"
    out.to_csv(HERE / name, sep="\t", index=False)
    print(f"wrote {HERE/name}")


if __name__ == "__main__":
    main()

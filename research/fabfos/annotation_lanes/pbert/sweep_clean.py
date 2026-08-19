"""THE QUESTION -- where should CLEAN abstain?

CLEAN emits a full level-4 EC for ~99% of ORFs and never declines, so its coverage
is an artifact of not abstaining rather than of reach. Its score is discriminative,
so a threshold is available; the scadc ablation set it at 0.01 on the EPI300 curated
cohort. This re-derives it on DH10B, the chassis the lane figures are quoted on.

SCOPE   DH10B, the 1,288 adjudicable ORFs from build_cohort.py. No re-run of the
        annotator: the copied dh10b.clean.tsv carries a score per predicted EC.
INPUT   data/fabfos/benchmarks/lane_dh10b/annotations/dh10b.clean.tsv
        research/fabfos/annotation_lanes/pbert/cohort_dh10b.tsv
METHOD  Level-4 ECs only, as the source study filters, then a threshold sweep.
        Scored on the same two axes as the pbert sweeps, in EC space, so the CLEAN
        and pbert rows sit on one axis; see _common.py.
ENV     PYTHONPATH="$PWD/src" mamba run -n msm python \
            research/fabfos/annotation_lanes/pbert/sweep_clean.py
OUT     research/fabfos/annotation_lanes/pbert/clean_abstain_dh10b.tsv   (committed)
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import ANN, HERE, L4, load_cohort, score_label_level, score_orf_level  # noqa: E402

SCADC_THRESHOLD = 0.01
THRESHOLDS = [0.0, 0.001, 0.005, 0.01, 0.02, 0.05, 0.1, 0.15, 0.2, 0.3, 0.4,
              0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.99, 0.999]
PRECISION_TARGET = 0.88


def main():
    coh = load_cohort()
    truth = dict(zip(coh["orf"], coh["ec"]))

    d = pd.read_csv(ANN / "dh10b.clean.tsv", sep="\t")
    d.columns = [c.strip() for c in d.columns]
    d = d.rename(columns={"Query ID": "orf", "Predicted EC number": "ec",
                          "clean_score": "score"})
    d["ec"] = d["ec"].astype(str).str.replace(r"^EC:", "", regex=True).str.strip()
    d["score"] = pd.to_numeric(d["score"], errors="coerce").fillna(0.0)
    d = d[d["ec"].str.match(L4, na=False)]
    d = d[d["orf"].isin(truth)]
    print(f"[clean] {len(d):,} level-4 calls over {d['orf'].nunique():,} adjudicable ORFs")

    rows = []
    for t in THRESHOLDS:
        sub = d[d["score"] >= t]
        pred = {o: set(g) for o, g in sub.groupby("orf")["ec"]}
        rows.append(dict(threshold=t, n_calls=len(sub),
                         **score_orf_level(pred, truth),
                         **{f"ec_{k}": v for k, v in score_label_level(pred, truth).items()}))
    out = pd.DataFrame(rows)
    out.to_csv(HERE / "clean_abstain_dh10b.tsv", sep="\t", index=False)

    b = out.loc[out["f1"].idxmax()]
    z = out[out["threshold"] == 0.0].iloc[0]
    s = out[out["threshold"] == SCADC_THRESHOLD].iloc[0]
    print(f"[clean] no abstention   P={z['precision']:.4f} R={z['recall']:.4f} "
          f"F1={z['f1']:.4f} cov={z['coverage']:.4f}")
    print(f"[clean] best F1 @{b['threshold']}  P={b['precision']:.4f} R={b['recall']:.4f} "
          f"F1={b['f1']:.4f} cov={b['coverage']:.4f}  "
          f"(recall cost vs no abstention: {z['recall']-b['recall']:+.4f})")
    print(f"[clean] scadc's {SCADC_THRESHOLD}  P={s['precision']:.4f} R={s['recall']:.4f} "
          f"F1={s['f1']:.4f} cov={s['coverage']:.4f}")
    ok = out[out["precision"] >= PRECISION_TARGET]
    if len(ok):
        t = ok.loc[ok["recall"].idxmax()]
        print(f"[clean] P>={PRECISION_TARGET} @{t['threshold']}  P={t['precision']:.4f} "
              f"R={t['recall']:.4f} F1={t['f1']:.4f} cov={t['coverage']:.4f}")
    else:
        print(f"[clean] no threshold reaches precision {PRECISION_TARGET}; "
              f"max {out['precision'].max():.4f} @{out.loc[out['precision'].idxmax(),'threshold']}")
    print(f"wrote {HERE/'clean_abstain_dh10b.tsv'}")


if __name__ == "__main__":
    main()

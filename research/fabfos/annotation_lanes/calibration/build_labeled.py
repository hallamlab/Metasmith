# THE QUESTION -- for each of the four chosen lanes, how often is a call at a given
# score wrong, measured on ORFs that curated Swiss-Prot can adjudicate?
#
# SCOPE   DH10B only, the one host with a curated truth set. This writes the labelled
#         join; the fit is `fit_calibration.py`.
# INPUT   data/fabfos/runs/e_coli_dh10b/annotations/lanes/*   (production lane output)
#         data/fabfos/originals/genomes/e_coli_dh10b/genome/NC_010473.1.faa
#         data/fabfos/benchmarks/lane_dh10b/generators/sprot_k12.tsv
#         data/fabfos/processed/{mnxr_lookup,label_transfer_landmarks}
# METHOD  See _cal.py. One row per (call, EC); `correct` is set when the call's EC set
#         meets the ORF's curated EC set.
# ENV     mamba run -n msm python research/fabfos/annotation_lanes/calibration/build_labeled.py
# OUT     research/fabfos/annotation_lanes/calibration/out/labeled_<lane>.tsv
#         research/fabfos/annotation_lanes/calibration/out/labeled_summary.tsv
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import _cal as C  # noqa: E402


def label(calls: pd.DataFrame, truth: pd.DataFrame) -> pd.DataFrame:
    """Collapse (call, EC) rows back to one row per call, carrying `correct`.

    Only calls on adjudicable ORFs survive. A call on an ORF with no curated entry is
    not a negative -- nothing is known about it."""
    t = dict(zip(truth["orf"], truth["truth_ec"]))
    calls = calls[calls["orf"].isin(t)].copy()
    calls["hit"] = [ec in t[o] for o, ec in zip(calls["orf"], calls["ec"])]
    g = (calls.groupby(["lane", "orf", "intermediate_id"], sort=False)
              .agg(score=("score", "max"), n_ec=("ec", "nunique"), correct=("hit", "any"))
              .reset_index())
    g["correct"] = g["correct"].astype(int)
    # The ORF's curated EC family, to the third level. It is a HOLDOUT KEY, not evidence:
    # an ORF-grouped fold still lets a fold train on a paralogue of what it is scored on,
    # and a lane that has memorised one enzyme family would read as a lane that generalises.
    fam = {o: sorted(ecs)[0].rsplit(".", 1)[0] for o, ecs in t.items()}
    g["ec_family"] = g["orf"].map(fam)
    return g


def main() -> int:
    C.OUT.mkdir(parents=True, exist_ok=True)
    truth = C.cohort()
    print(f"adjudicable ORFs      {len(truth)}  of "
          f"{sum(1 for _ in C.iter_fasta(C.GENOME))} run ORFs", flush=True)
    print(f"mean curated EC/ORF   {truth['truth_ec'].map(len).mean():.2f}", flush=True)

    m2e = C.mnxr_to_ec()
    print(f"MNXR carrying a level-4 EC  {len(m2e)}", flush=True)

    raw = {}
    raw["kofam"] = C.lane_kofam()
    raw["clean"] = C.lane_clean()
    raw["uniref50"] = C.lane_uniref50(m2e)
    print("[cal] pbert: voting against 222,019 landmarks ...", flush=True)
    raw["pbert"] = C.lane_pbert(m2e)

    rows = []
    for lane in C.LANE_ORDER:
        calls = raw[lane]
        lab = label(calls, truth)
        lab.to_csv(C.OUT / f"labeled_{lane}.tsv", sep="\t", index=False)
        n_err = int((lab["correct"] == 0).sum())
        rows.append(dict(
            lane=lane,
            calls_parsed=calls.attrs["n_calls_in"],
            calls_with_ec=calls.attrs["n_calls_kept"],
            calls_adjudicable=len(lab),
            orfs_adjudicable=lab["orf"].nunique(),
            errors=n_err,
            precision=round(1 - n_err / len(lab), 4) if len(lab) else 0.0,
            score_min=round(float(lab["score"].min()), 4) if len(lab) else None,
            score_p05=round(float(lab["score"].quantile(0.05)), 4) if len(lab) else None,
            score_p95=round(float(lab["score"].quantile(0.95)), 4) if len(lab) else None,
            score_max=round(float(lab["score"].max()), 4) if len(lab) else None,
        ))
    s = pd.DataFrame(rows)
    s.to_csv(C.OUT / "labeled_summary.tsv", sep="\t", index=False)
    print()
    print(s.to_string(index=False))
    print("\nThe error count, not the call count, is what limits each fit.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

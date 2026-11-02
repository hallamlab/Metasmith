# THE QUESTION -- what does the mapper need in order to turn a lane score into nats,
# and can a reader tell where that number came from?
#
# SCOPE   Packaging only. The fit is `fit_calibration.py`; this writes it out.
# INPUT   out/calibration.tsv, out/reliability.tsv
# METHOD  One JSON carrying the parameters, the apply rule, the prior, the fitted score
#         range and the digest of every input the fit saw. `source.txt` beside it, as
#         `ref::label_transfer_landmarks` does.
# ENV     mamba run -n msm python research/fabfos/annotation_lanes/calibration/emit_reference.py
# OUT     data/fabfos/processed/lane_calibration/{lane_calibration.json,source.txt}
#
# WHY THE DIGESTS ARE IN THE ARTIFACT. A calibration is a claim about a benchmark, and
# the claim is only checkable if the benchmark is named. `fabfos_evidence.load_calibration`
# does not verify them -- it cannot, a run does not have the truth set -- so they are
# here for a person reading the file and for a rebuild that wants to know it is a rebuild.
from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import _cal as C  # noqa: E402
import fit_calibration as F  # noqa: E402

DEST = C.REPO / "data/fabfos/processed/lane_calibration"

# The rule a consumer applies, stated in the artifact so it cannot drift from the fit.
# The clamp is not cosmetic: outside [score_min, score_max] the curve is extrapolation,
# and a lane that saturates would otherwise run its LLR off to wherever the transform
# goes. The floor is the project's decision that these lanes do not disconfirm -- a
# negative LLR would say the call is evidence a reaction is ABSENT, which none of them
# means. Against `reference_lam0` it does not fire on this cohort at all; against the
# benchmark's own call prevalence it would fire on 58% of two lanes, which is why the
# reference is the pooling's prior and not the benchmark's.
APPLY = ("llr = max(0, A * transform(clip(score, score_min, score_max)) + B "
         "- reference_lam0)")


def digest(path: Path) -> str:
    h = hashlib.md5()
    with open(path, "rb") as fh:
        for blk in iter(lambda: fh.read(1 << 20), b""):
            h.update(blk)
    return h.hexdigest()


def main() -> int:
    cal = pd.read_csv(C.OUT / "calibration.tsv", sep="\t")
    rel = pd.read_csv(C.OUT / "reliability.tsv", sep="\t")
    DEST.mkdir(parents=True, exist_ok=True)

    inputs = {
        "truth_sprot_k12": C.SPROT,
        "query_orfs": C.GENOME,
        "kofam": C.LANES / "kofamscan.csv",
        "kofam_descriptions": C.LANES / "kofamscan_descriptions.csv",
        "clean": C.LANES / "clean.tsv",
        "uniref50": C.LANES / "diamond_uniref50.tsv",
        "pbert_embeddings": C.LANES / "proteinbert_embeddings.parquet",
        "landmarks": C.LANDMARKS,
        "reac_prop": C.REAC_PROP,
    }

    lanes = {}
    for _, r in cal.iterrows():
        lanes[r["lane"]] = dict(
            model=r["model"], transform=r["transform"], eps=F.EPS,
            A=float(r["A"]), B=float(r["B"]),
            score_min=float(r["score_min"]), score_max=float(r["score_max"]),
            llr_at_min=float(r["llr_at_min"]), llr_at_max=float(r["llr_at_max"]),
            llr_at_median=float(r["llr_at_median"]),
            p_correct_at_median=float(r["p_correct_at_median"]),
            n_calls=int(r["n"]), n_errors=int(r["errors"]),
            cv_logloss=float(r["cv_logloss"]), cv_brier=float(r["cv_brier"]),
            slope_gain_z=float(r["slope_gain_z"]),
        )

    doc = dict(
        schema="lane_calibration/v1",
        apply=APPLY,
        units="nats",
        prior=dict(
            reference_lam0=float(cal["lam0"].iloc[0]),
            pi_fit=float(cal["pi_fit"].iloc[0]),
            n_calls=int(cal["n"].sum()), n_correct=int((cal["n"] - cal["errors"]).sum()),
            note=("`reference_lam0` is the prior llr is measured against AND the one "
                  "`pool_logodds` adds back, so a reaction with one assertion comes out "
                  "at that assertion's own calibrated accuracy. `pi_fit` is the share "
                  "of correct calls among calls the lanes MADE on adjudicable ORFs -- "
                  "what the fitted intercept B is relative to, reported so sigmoid(B) "
                  "reads as a lane's precision. Subtracting pi_fit instead would floor "
                  "every below-average call to zero and pin the pooled belief into "
                  "[pi_fit, 1]."),
        ),
        truth_set=dict(
            host="e_coli_dh10b", accession="NC_010473.1",
            method=("Swiss-Prot K-12 entries carrying a level-4 EC, keyed by the md5 of "
                    "their sequence; a run ORF is adjudicable when its own sequence md5 "
                    "hits that key"),
            adjudicable_orfs=1300,
            level="EC",
            level_note=("Reaction-level would measure the same fan-out dilution the "
                        "mapper's 1/F_n divisor already applies. Moving to it means "
                        "removing that divisor in the same change."),
        ),
        caveats=[
            "kofam and uniref50 ship as constants. Their score did not beat its own "
            "intercept on ORF-grouped held-out folds (z = 0.62 and 0.23), so the fitted "
            "slope was dropped rather than shipped as noise.",
            "CLEAN's abstain point was tuned against this same truth family, so its "
            "curve is not fully independent of it. The curve is conditioned on the "
            "score, so the tuning moves WHICH calls exist rather than the score-to-error "
            "relation, but the fit is not a clean holdout for that lane.",
            "The ProteinBERT lane retrieves against Swiss-Prot and the truth set is "
            "Swiss-Prot, so most of these ORFs find themselves. That populates the top "
            "of the score range and leaves the bottom thin; it does not bias the curve, "
            "which is conditioned on the score.",
            "Over half of ProteinBERT's calls sit at exactly vote = 1.0, because an ORF "
            "admitting a single neighbour votes 1.0 whatever that neighbour is worth. "
            "The lane's score cannot separate unanimous-among-many from only-one-voter.",
            "score_min/score_max bound where each curve has support. Outside them the "
            "apply rule clamps rather than continuing the trend.",
        ],
        lanes=lanes,
        reliability=rel.to_dict(orient="records"),
        inputs={k: dict(path=str(p.relative_to(C.REPO)), md5=digest(p))
                for k, p in inputs.items()},
    )
    (DEST / "lane_calibration.json").write_text(json.dumps(doc, indent=2) + "\n")

    try:
        rev = subprocess.run(["git", "-C", str(C.REPO), "rev-parse", "HEAD"],
                             capture_output=True, text=True, check=True).stdout.strip()
    except Exception:
        rev = "unknown"
    (DEST / "source.txt").write_text(
        "ref::lane_calibration -- per-lane score-to-log-likelihood-ratio calibration\n"
        "\n"
        "built by  research/fabfos/annotation_lanes/calibration/"
        "{build_labeled,fit_calibration,emit_reference}.py\n"
        f"at commit {rev}\n"
        "\n"
        "Read `lane_calibration.json`. `apply` states the rule; `caveats` states what\n"
        "the fit does and does not support. `fabfos_evidence.load_calibration` is the\n"
        "only consumer and is where the rule is implemented once.\n")

    print(json.dumps({k: dict(model=v["model"], A=v["A"], B=v["B"],
                              llr_at_median=v["llr_at_median"])
                      for k, v in lanes.items()}, indent=2))
    print(f"\nwrote {DEST}/lane_calibration.json  "
          f"({(DEST / 'lane_calibration.json').stat().st_size:,} bytes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

# THE QUESTION -- what did the two lanes score before, and what do they score now?
#
# Assembles the rows the run exists to produce from the sweep tables. NOTHING IS
# COMPUTED HERE; every number is looked up, so this file cannot disagree with the
# sweeps that produced it.
#
# The pbert lane had two independent defects and fixing them is not one step, so it
# carries three states rather than two: the landmark set's index did not describe its
# embedding stack (rebuild_landmarks.py), and the lane had no way to refuse an ORF
# (sweep_threshold.py). The middle row is the landmark repair alone, at the vote rule
# the lane ran before this work -- without it the whole difference could be attributed
# to whichever of the two the reader already believed in.
#
# EVERY ROW IS REPORTED UNDER ALL THREE LEAKAGE CONDITIONS. `pool` is the deployed
# arrangement and it leaks: an ORF's own Swiss-Prot entry is a landmark. `twin` hides
# every neighbour at cosine >= 0.99 and is the honest read for an ORF with no close
# relative -- which is the case that motivated this work.
#
# CALL COUNTS SIT BESIDE THE SCORES, not behind them: a lane can raise ORF-level
# precision by emitting MORE labels per ORF, because that makes an intersection with
# the truth set easier. `n_calls` and the label-level columns are what make that
# visible.
#
# ENV     PYTHONPATH="$PWD/src" mamba run -n msm python             research/fabfos/annotation_lanes/pbert/report_before_after.py
# OUT     research/fabfos/annotation_lanes/pbert/before_after_dh10b.tsv   (committed)
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import HERE, K, PBERT_FLOOR, PBERT_NN_MIN, PBERT_TAU  # noqa: E402

CONDITIONS = ["pool", "self", "twin"]
COLS = ["n_fired", "n_correct", "precision", "recall", "f1", "coverage",
        "mnxr_macro_precision", "mnxr_macro_recall", "mnxr_macro_f1",
        "n_calls", "calls_per_speaking_orf"]
PRE_QUOTA = dict(nn_min=0.0, tau=0.0, k_max=30, floor=0.20)


def rows(label, config, src, **sel):
    d = pd.read_csv(HERE / src, sep="\t")
    for k, v in sel.items():
        d = d[d[k] == v]
    out = []
    for cond in CONDITIONS:
        c = d[d["condition"] == cond]
        if len(c) != 1:
            raise SystemExit(f"{src} {sel} condition={cond} matched {len(c)} rows")
        r = c.iloc[0]
        out.append(dict(lane=label, config=config, condition=cond,
                        **{col: r.get(col, float("nan")) for col in COLS}))
    return out


def main():
    tuned = f"nn_min {PBERT_NN_MIN}, tau {PBERT_TAU}, k_max {K}, floor {PBERT_FLOOR}"
    out = pd.DataFrame(
        rows("pbert", "as shipped: misaligned landmarks, top-30, floor 0.20",
             "metrics_dh10b_shippedpool.tsv", metric="cosine")
        + rows("pbert", "landmarks repaired only: top-30, floor 0.20",
               "threshold_cosine_dh10b.tsv", **PRE_QUOTA)
        + rows("pbert", f"repaired + quota (SHIPPED): {tuned}",
               "threshold_cosine_dh10b.tsv", nn_min=PBERT_NN_MIN, tau=PBERT_TAU,
               k_max=K, floor=PBERT_FLOOR)
    )

    cl = pd.read_csv(HERE / "clean_abstain_dh10b.tsv", sep="\t")
    for cfg, thr in (("as shipped: no abstention", 0.0), ("abstain below 0.02", 0.02)):
        r = cl[cl["threshold"] == thr].iloc[0]
        out.loc[len(out)] = dict(lane="clean", config=cfg, condition="n/a",
                                 **{c: r.get(c, float("nan")) for c in COLS})

    out.to_csv(HERE / "before_after_dh10b.tsv", sep="\t", index=False)
    with pd.option_context("display.width", 250, "display.max_columns", 40):
        print(out.to_string(index=False))
    print(f"\nwrote {HERE/'before_after_dh10b.tsv'}")


if __name__ == "__main__":
    main()

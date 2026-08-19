"""THE QUESTION -- what did the two lanes score before, and what do they score now?

Assembles the four rows the run exists to produce from the sweep tables. Nothing is
computed here; every number is looked up, so this file cannot disagree with the
sweeps that produced them.

The pbert rows carry three states, not two, because the shipped lane has two
independent defects and fixing them is not one step: the pool index misdescribes the
stack (repair_pool_index.py), and the lane has no absolute-quality gate
(sweep_threshold.py). The middle row is the pool repair alone.

ENV     PYTHONPATH="$PWD/src" mamba run -n msm python \
            research/fabfos/annotation_lanes/pbert/report_before_after.py
OUT     research/fabfos/annotation_lanes/pbert/before_after_dh10b.tsv   (committed)
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import HERE  # noqa: E402

COND = "pool"
COLS = ["precision", "recall", "f1", "coverage", "n_fired", "n_correct"]


def row(label, config, src, **sel):
    d = pd.read_csv(HERE / src, sep="\t")
    for k, v in sel.items():
        d = d[d[k] == v]
    if len(d) != 1:
        raise SystemExit(f"{src} {sel} matched {len(d)} rows")
    r = d.iloc[0]
    return dict(lane=label, config=config, **{c: r[c] for c in COLS})


def main():
    rows = [
        row("pbert", "as shipped: misaligned pool, cosine, floor 0.20, no gate",
            "metrics_dh10b_shippedpool.tsv", metric="cosine", condition=COND),
        row("pbert", "pool repaired only: cosine, floor 0.20, no gate",
            "metrics_dh10b.tsv", metric="cosine", condition=COND),
        row("pbert", "pool repaired + nn_similarity >= 0.7716, floor 0.00",
            "threshold_cosine_dh10b.tsv", condition=COND, nn_quantile=0.15, floor=0.0),
        row("pbert", "pool repaired + nn_similarity >= 0.7716, floor 0.20",
            "threshold_cosine_dh10b.tsv", condition=COND, nn_quantile=0.15, floor=0.20),
        row("clean", "as shipped: no abstention", "clean_abstain_dh10b.tsv", threshold=0.0),
        row("clean", "abstain below 0.02", "clean_abstain_dh10b.tsv", threshold=0.02),
    ]
    out = pd.DataFrame(rows)
    out.to_csv(HERE / "before_after_dh10b.tsv", sep="\t", index=False)
    print(out.to_string(index=False))
    print(f"\nwrote {HERE/'before_after_dh10b.tsv'}")


if __name__ == "__main__":
    main()

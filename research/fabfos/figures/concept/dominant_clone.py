"""Rank the fosmid inserts by how many pools they dominate.

The insert coverage matrix is depth per (insert, pool). Depth x length is bases attributable
to that insert, so an insert's *share* of a pool is its bases over the pool's total -- which is
the quantity "this clone is essentially the whole pool" actually names. An insert dominates a
pool when its share clears the threshold, and the ranking is by how many pools it dominates.

This exists so the choice of clone is a measurement someone can re-run and disagree with,
rather than a name carried forward in prose. The ranking is stable from a 0.3 threshold to a
0.9 one, so the threshold is not what picks the winner.
"""
import argparse
from pathlib import Path

import numpy as np
import pandas as pd

MATRIX = Path("/home/tony/agentic_workspace/projects/fabfos/figure/data/fabfos/"
              "scadc_fosmids/sequences/insert_coverage/insert_coverage_matrix.tsv")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--matrix", type=Path, default=MATRIX)
    ap.add_argument("--top", type=int, default=10)
    ap.add_argument("--thresholds", default="0.3,0.5,0.7,0.9")
    args = ap.parse_args()

    d = pd.read_csv(args.matrix, sep="\t")
    depth = d.drop(columns=["insert_id", "length"]).to_numpy(float)
    bases = depth * d["length"].to_numpy(float)[:, None]
    share = bases / np.where(bases.sum(0) > 0, bases.sum(0), 1.0)

    ths = [float(t) for t in args.thresholds.split(",")]
    out = pd.DataFrame({"insert_id": d.insert_id, "length": d.length,
                        "max_share": share.max(1)})
    for t in ths:
        out[f"pools>{t:g}"] = (share > t).sum(1)
    key = f"pools>{ths[len(ths) // 2]:g}"
    out = out.sort_values([key, "max_share"], ascending=False).reset_index(drop=True)
    print(f"{len(d)} inserts over {share.shape[1]} pools; ranked by {key}")
    print(out.head(args.top).to_string(index=False))


if __name__ == "__main__":
    main()

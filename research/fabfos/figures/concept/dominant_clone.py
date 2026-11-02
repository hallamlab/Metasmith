import argparse
from pathlib import Path

import numpy as np
import pandas as pd

MATRIX = (Path(__file__).resolve().parents[4]
          / "data/fabfos/runs/scadc_fosmids/sequences/insert_coverage"
          / "insert_coverage_matrix.tsv")


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

#!/usr/bin/env python3
"""Count every sample's reads at each stage, from the raw reads to the clean ASV counts.

A paired sample is counted in pairs until the merge, so every column is on one scale and
each stage can only lose reads. `table_filtered` is what survived the depth and prevalence
cut: the clean counts plus the counts curation removed.
"""

import argparse
import json
from pathlib import Path

import pandas as pd

REASONS = ["contaminant", "mitochondrial", "abundance", "taxonomy"]


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--sample", nargs=3, action="append", required=True,
                    metavar=("ID", "FASTP_JSON", "READ_COUNTS"))
    ap.add_argument("--raw", type=Path, required=True, help="the denoised ASV table")
    ap.add_argument("--clean", type=Path, required=True)
    ap.add_argument("--removed", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()

    raw = pd.read_csv(args.raw, sep="\t", index_col=0)
    clean = pd.read_csv(args.clean, sep="\t", index_col=0)
    removed = pd.read_csv(args.removed, sep="\t", index_col=0)

    rows = []
    for sid, fastp_json, counts_tsv in args.sample:
        counts = pd.read_csv(counts_tsv, sep="\t").iloc[0]
        assert str(counts["sample"]) == sid, f"[{counts_tsv}] counts sample {counts['sample']}, not {sid}"
        per = 2 if counts["parity"] == "paired" else 1
        summary = json.loads(Path(fastp_json).read_text())["summary"]
        row = {
            "sample": sid,
            "parity": counts["parity"],
            "raw": summary["before_filtering"]["total_reads"] // per,
            "qc": summary["after_filtering"]["total_reads"] // per,
            "merged": int(counts["merged"]),
            "filtered": int(counts["filtered"]),
            "mapped": int(raw[sid].sum()) if sid in raw else 0,
            "kept": int(clean[sid].sum()) if sid in clean else 0,
        }
        for reason in REASONS:
            part = removed.loc[removed["reason"] == reason]
            row[f"removed_{reason}"] = int(part[sid].sum()) if sid in part else 0
        row["table_filtered"] = row["kept"] + sum(row[f"removed_{r}"] for r in REASONS)
        rows.append(row)

    order = ["sample", "parity", "raw", "qc", "merged", "filtered", "mapped", "table_filtered",
             "kept"] + [f"removed_{r}" for r in REASONS]
    pd.DataFrame(rows)[order].to_csv(args.out, sep="\t", index=False)


if __name__ == "__main__":
    main()

import argparse
import csv

import fabfos_evidence as fe
import pandas as pd

COLUMNS = ["orf_id", "kind", "value", "score", "confidence"]


def orf_ids(path):
    with open(path) as fh:
        return [line[1:].split()[0] for line in fh if line.startswith(">")]


def clean_rows(path, floor, keep):
    rows = []
    with open(path) as fh:
        for r in csv.DictReader(fh, delimiter="\t"):
            orf, score = r["Query ID"].strip(), float(r["clean_score"])
            ec = r["Predicted EC number"].strip()
            ec = ec[3:] if ec.upper().startswith("EC:") else ec
            if orf in keep and score >= floor and ec.count(".") == 3:
                rows.append({"orf_id": orf, "kind": "EC", "value": ec, "score": score, "confidence": "clean"})
    return rows


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--orfs", required=True)
    p.add_argument("--clean", required=True)
    p.add_argument("--floor", type=float, default=fe.CLEAN_MIN_SCORE)
    p.add_argument("--out", required=True)
    a = p.parse_args()

    rows = clean_rows(a.clean, a.floor, set(orf_ids(a.orfs)))
    df = pd.DataFrame(rows, columns=COLUMNS).astype({"score": "float64"})
    fe.write_parquet(df, a.out)
    print(f"[ptools_gather] {len(rows):,} CLEAN EC calls at score >= {a.floor}", flush=True)


if __name__ == "__main__":
    main()

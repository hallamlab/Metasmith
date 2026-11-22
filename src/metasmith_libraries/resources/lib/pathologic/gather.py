import argparse
import csv
import re

import fabfos_evidence as fe
import pandas as pd

COLUMNS = ["orf_id", "kind", "value", "score", "confidence"]
UNINFORMATIVE = re.compile(r"hypothetical|uncharacteri[sz]ed|unknown function|^\s*$", re.I)
# A UniRef title is "<cluster id> <name> n=<members> Tax=<taxon> TaxID=<id> RepID=<id>".
UNIREF_TITLE = re.compile(r"^UniRef50_\S+\s+|\s+n=\d+\s+Tax=.*$")


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


# PathoLogic matches a FUNCTION line to reactions by name, as MetaPathways' input does; an ORF's name
# is its best-scoring UniRef50 hit that names something.
def uniref50_names(hits, descriptions, keep):
    titles = fe.read_uniref50_titles(descriptions)
    df = pd.read_csv(hits, sep="\t", dtype={"qseqid": str, "sseqid": str}, usecols=["qseqid", "sseqid", "evalue", "bitscore"])
    df = df[df["qseqid"].isin(keep)].sort_values(["qseqid", "evalue", "bitscore"], ascending=[True, True, False])
    df["name"] = df["sseqid"].map(titles).fillna("").str.replace(UNIREF_TITLE, "", regex=True)
    df = df[~df["name"].str.contains(UNINFORMATIVE)].drop_duplicates("qseqid")
    return [{"orf_id": o, "kind": "FUNCTION", "value": n, "score": b, "confidence": "uniref50"}
            for o, n, b in zip(df["qseqid"], df["name"], df["bitscore"])]


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--orfs", required=True)
    p.add_argument("--clean", required=True)
    p.add_argument("--uniref50-hits", required=True)
    p.add_argument("--uniref50-descriptions", required=True)
    p.add_argument("--floor", type=float, default=fe.CLEAN_MIN_SCORE)
    p.add_argument("--out", required=True)
    a = p.parse_args()

    keep = set(orf_ids(a.orfs))
    ecs = clean_rows(a.clean, a.floor, keep)
    names = uniref50_names(a.uniref50_hits, a.uniref50_descriptions, keep)
    df = pd.DataFrame(ecs + names, columns=COLUMNS).astype({"score": "float64"})
    fe.write_parquet(df, a.out)
    print(f"[ptools_gather] {len(ecs):,} CLEAN EC calls at score >= {a.floor}, {len(names):,} UniRef50 names", flush=True)


if __name__ == "__main__":
    main()

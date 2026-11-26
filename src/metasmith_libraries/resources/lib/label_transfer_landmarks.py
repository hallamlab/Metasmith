import argparse
import gzip
import re
import shutil
from pathlib import Path

import pandas as pd

POOL_ID_SOURCE = "uniprot"
POOL_EVIDENCE = "reviewed"
FASTA_FILE = "uniprot_sprot.fasta.gz"
RELDATE_FILE = "reldate.txt"
TABLE_NAME = "landmarks.parquet"
SOURCE_NAME = "source.txt"

# Must equal POOL_ALPHABET in transforms/functionalAnnotation/proteinbert.py: the query and
# the pool are only comparable if the same function produced both. ProteinBERT's encoder
# indexes past its lookup array on ordinal 90 ('Z'), and lowercase trips the same bug, so
# everything outside its tokens becomes 'X', which is what it does with 'B' and '*' anyway.
POOL_ALPHABET = set("ACDEFGHIKLMNPQRSTUVWXY")


def tokenisable(seq):
    return "".join(c if c in POOL_ALPHABET else "X" for c in seq.upper())


def select(a):
    sp = Path(a.swissprot)
    b = pd.read_parquet(a.bridge, columns=["id", "id_source", "mnxr", "evidence_quality"])
    sel = b[(b["id_source"] == POOL_ID_SOURCE) & (b["evidence_quality"] == POOL_EVIDENCE)]
    labels = (sel.groupby("id")["mnxr"].apply(lambda s: ";".join(sorted(set(s))))
                 .rename("mnxr_list").reset_index().rename(columns={"id": "accession"}))
    print(f"[pool] bridge slice: {len(sel):,} rows -> {len(labels):,} labelled accessions",
          flush=True)
    if labels.empty:
        raise SystemExit("[pool] the bridge carries no reviewed uniprot rows -- the cut that "
                         "defines the pool selected nothing")
    wanted = dict(zip(labels["accession"], labels["mnxr_list"]))

    # `>sp|P12345|NAME_ORGANISM ...`; rhea2uniprot writes bare accessions, so the two join.
    written = recoded = 0
    seen = set()
    keep = False
    with gzip.open(sp / FASTA_FILE, "rt") as fh, open(a.fasta, "w") as out:
        for line in fh:
            if line.startswith(">"):
                parts = line[1:].split("|")
                acc = parts[1] if len(parts) >= 3 else line[1:].split(None, 1)[0]
                keep = acc in wanted and acc not in seen
                if keep:
                    seen.add(acc)
                    # The embedder echoes the header into its index; it is the join key.
                    out.write(">" + acc + "\n")
                    written += 1
            elif keep:
                s = line.strip()
                t = tokenisable(s)
                if t != s:
                    recoded += 1
                out.write(t + "\n")
    print(f"[pool] {recoded:,} sequence lines carried a residue outside the embedder's "
          f"alphabet and were recoded to X", flush=True)

    missing = len(wanted) - written
    print(f"[pool] {written:,} of {len(wanted):,} labelled accessions have a Swiss-Prot "
          f"sequence; {missing:,} do not", flush=True)
    if written == 0:
        raise SystemExit("[pool] no pool sequences found -- the bridge's uniprot ids and "
                         "Swiss-Prot's `sp|ACC|` field are one id space, so zero overlap "
                         "is a parse bug, not a coverage fact")
    # `reviewed` means rhea2uniprot.tsv, i.e. Swiss-Prot, so the cut is meant to BE this
    # release. A gap past 2% means Rhea and UniProt have drifted and the pool would be a
    # silent subset of what it claims.
    if missing > 0.02 * len(wanted):
        raise SystemExit(f"[pool] {missing:,} of {len(wanted):,} reviewed accessions "
                         f"({100.0*missing/len(wanted):.1f}%) are absent from this Swiss-Prot "
                         f"release; Rhea and UniProt are too far apart")

    labels[labels["accession"].isin(seen)].to_parquet(a.labels, index=False)

    with open(a.source, "w") as fh:
        fh.write("sequences\tswissprot " + (sp / "release").read_text().strip() + "\n")
        fh.write(f"labels\tmnxr_lookup id_source={POOL_ID_SOURCE} "
                 f"evidence_quality={POOL_EVIDENCE}\n")
        fh.write("embedder\tproteinbert (weights baked into env::proteinbert.env)\n")
        fh.write("sequences_written\t" + str(written) + "\n")
        fh.write("labelled_accessions\t" + str(len(wanted)) + "\n")
        reldate = sp / RELDATE_FILE
        if reldate.exists():
            fh.write("reldate\t" + reldate.read_text().strip().replace("\n", " | ") + "\n")


# `pbert` writes `<stem>.<k>.embedding.npy` with k unpadded, so a lexicographic sort puts
# chunk 10 before chunk 2 while the index stays in FASTA order. That once labelled every
# landmark with another protein's reactions and raised nothing.
def chunk_no(path):
    m = re.search(r"\.(\d+)(?:\.embedding)?$", path.stem)
    if m is None:
        raise SystemExit(f"[pool] {path.name} does not end in a chunk number, so the order "
                         f"the embedder wrote its chunks in is not recoverable")
    return int(m.group(1))


def assemble(a):
    import numpy as np

    shards = Path(a.shards)
    pool = Path(a.out)
    pool.mkdir(parents=True, exist_ok=True)

    npys = sorted(shards.glob("*.npy"), key=chunk_no)
    csvs = sorted(shards.glob("*.csv"))
    if len(csvs) > 1:
        csvs = sorted(csvs, key=chunk_no)
    if not npys or not csvs:
        raise SystemExit(f"[pool] embedder produced no output under {shards}")

    # Narrow and downcast each chunk before stacking: the other order peaks at every chunk
    # at full width plus vstack's copy, tens of GB on ~222k sequences.
    stack = np.vstack([np.load(f)[:, -512:].astype(np.float32) for f in npys])
    idx = pd.concat([pd.read_csv(f) for f in csvs], ignore_index=True)
    for cand in ("sequence_id", "id"):
        if cand in idx.columns:
            accession = idx[cand].astype(str).to_numpy()
            break
    else:
        raise SystemExit(f"[pool] the embedder index has no id column: {list(idx.columns)}")
    if len(accession) != len(stack):
        raise SystemExit(f"[pool] the embedder index has {len(accession)} rows and its "
                         f"chunks stack to {len(stack)}")

    want = [ln[1:].split()[0] for ln in Path(a.fasta).read_text().splitlines()
            if ln.startswith(">")]
    if list(accession) != want:
        bad = next((i for i, (x, y) in enumerate(zip(accession, want)) if x != y),
                   min(len(accession), len(want)))
        raise SystemExit(
            f"[pool] the embedder index is not in the order of the FASTA it was given: "
            f"row {bad} is {list(accession[bad:bad+1])}, the FASTA has {want[bad:bad+1]}")

    labels = pd.read_parquet(a.labels)
    table = pd.DataFrame({"accession": accession}).merge(labels, on="accession", how="left")
    n_unlabelled = int(table["mnxr_list"].isna().sum())
    if n_unlabelled:
        raise SystemExit(f"[pool] {n_unlabelled:,} embedded sequences carry no label, but "
                         f"each was selected for having one -- the embedder's index ids do "
                         f"not match the FASTA headers")

    table = pd.concat([table, pd.DataFrame(
        stack, columns=[f"dim_{i}" for i in range(stack.shape[1])])], axis=1)
    table.to_parquet(pool / TABLE_NAME, index=False)
    shutil.copy(a.source, pool / SOURCE_NAME)
    print(f"[pool] {len(table):,} landmarks, "
          f"{table['mnxr_list'].str.split(';').explode().nunique():,} distinct MNXR, "
          f"{stack.shape[1]} dims", flush=True)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="step", required=True)
    s = sub.add_parser("select")
    s.add_argument("--bridge", required=True)
    s.add_argument("--swissprot", required=True)
    s.add_argument("--fasta", required=True)
    s.add_argument("--labels", required=True)
    s.add_argument("--source", required=True)
    s.set_defaults(func=select)
    s = sub.add_parser("assemble")
    s.add_argument("--shards", required=True)
    s.add_argument("--fasta", required=True)
    s.add_argument("--labels", required=True)
    s.add_argument("--source", required=True)
    s.add_argument("--out", required=True)
    s.set_defaults(func=assemble)
    a = p.parse_args()
    a.func(a)

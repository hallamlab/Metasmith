import argparse as _argparse
import ast as _ast

_p = _argparse.ArgumentParser()
_p.add_argument("--bridge", required=True)
_p.add_argument("--evidence", required=True)
_p.add_argument("--fasta-file", required=True)
_p.add_argument("--id-source", required=True)
_p.add_argument("--reldate-file", required=True)
_p.add_argument("--swissprot", required=True)
A = _p.parse_args()

import gzip
from pathlib import Path
import pandas as pd

BRIDGE = A.bridge
SP_ROOT = A.swissprot
FASTA_OUT = "_pool.faa"
LABELS_OUT = "_pool_labels.parquet"
SOURCE_OUT = "_pool_source.txt"

subs = sorted(p for p in Path(SP_ROOT).iterdir() if p.is_dir())
if len(subs) != 1:
    raise SystemExit(f"[pool-esmc] expected exactly one Swiss-Prot release under "
                     f"{SP_ROOT}, found {len(subs)} ({[p.name for p in subs]}) -- "
                     f"which release the pool was built from is not recoverable from "
                     f"the embeddings")
SP = subs[0]
FASTA = str(SP / A.fasta_file)
print(f"[pool-esmc] Swiss-Prot release {SP.name}", flush=True)

b = pd.read_parquet(BRIDGE, columns=["id", "id_source", "mnxr", "evidence_quality"])
sel = b[(b["id_source"] == A.id_source) & (b["evidence_quality"] == A.evidence)]
labels = (sel.groupby("id")["mnxr"].apply(lambda s: ";".join(sorted(set(s))))
             .rename("mnxr_list").reset_index().rename(columns={"id": "accession"}))
print(f"[pool-esmc] bridge slice: {len(sel):,} rows -> {len(labels):,} labelled "
      f"accessions", flush=True)
if labels.empty:
    raise SystemExit("[pool-esmc] the bridge carries no reviewed uniprot rows -- the "
                     "cut that defines the pool selected nothing")

wanted = dict(zip(labels["accession"], labels["mnxr_list"]))

written = 0
seen = set()
keep = False
with gzip.open(FASTA, "rt") as fh, open(FASTA_OUT, "w") as out:
    for line in fh:
        if line.startswith(">"):
            parts = line[1:].split("|")
            acc = parts[1] if len(parts) >= 3 else line[1:].split(None, 1)[0]
            keep = acc in wanted and acc not in seen
            if keep:
                seen.add(acc)
                # Bare accession: the embedder echoes the first header token into its
                # index, and that is the key the labels are re-joined on.
                out.write(">" + acc + "\n")
                written += 1
        elif keep:
            out.write(line.strip().upper() + "\n")

missing = len(wanted) - written
print(f"[pool-esmc] {written:,} of {len(wanted):,} labelled accessions have a "
      f"Swiss-Prot sequence; {missing:,} do not", flush=True)
if written == 0:
    raise SystemExit("[pool-esmc] no pool sequences found -- the accession join broke. "
                     "The bridge's uniprot ids and Swiss-Prot's `sp|ACC|` field are the "
                     "same id space, so zero overlap is a parse bug, not a coverage fact")
if missing > 0.02 * len(wanted):
    raise SystemExit(f"[pool-esmc] {missing:,} of {len(wanted):,} reviewed accessions "
                     f"({100.0*missing/len(wanted):.1f}%) are absent from Swiss-Prot "
                     f"{SP.name}. The reviewed cut is meant to BE this release")

labels[labels["accession"].isin(seen)].to_parquet(LABELS_OUT, index=False)

reldate = SP / A.reldate_file
with open(SOURCE_OUT, "w") as fh:
    fh.write("sequences\tswissprot " + SP.name + "\n")
    fh.write("labels\tmnxr_lookup id_source=" + str(A.id_source) + " evidence_quality=" + str(A.evidence) + "\n")
    fh.write("embedder\tesmc_600m (ref::esm_c_600m_weights)\n")
    fh.write("sequences_written\t" + str(written) + "\n")
    fh.write("labelled_accessions\t" + str(len(wanted)) + "\n")
    if reldate.exists():
        fh.write("reldate\t" + reldate.read_text().strip().replace("\n", " | ") + "\n")

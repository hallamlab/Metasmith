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

import gzip, os
from pathlib import Path
import pandas as pd

BRIDGE = A.bridge
SP_ROOT = A.swissprot
FASTA_OUT = "_pool.faa"
LABELS_OUT = "_pool_labels.parquet"
SOURCE_OUT = "_pool_source.txt"

subs = sorted(p for p in Path(SP_ROOT).iterdir() if p.is_dir())
if len(subs) != 1:
    raise SystemExit(f"[pool] expected exactly one Swiss-Prot release under {SP_ROOT}, "
                     f"found {len(subs)} ({[p.name for p in subs]}) -- which release the "
                     f"pool was built from is not recoverable from the embeddings")
SP = subs[0]
FASTA = str(SP / A.fasta_file)
print(f"[pool] Swiss-Prot release {SP.name}", flush=True)

b = pd.read_parquet(BRIDGE, columns=["id", "id_source", "mnxr", "evidence_quality"])
sel = b[(b["id_source"] == A.id_source) & (b["evidence_quality"] == A.evidence)]
labels = (sel.groupby("id")["mnxr"].apply(lambda s: ";".join(sorted(set(s))))
             .rename("mnxr_list").reset_index().rename(columns={"id": "accession"}))
print(f"[pool] bridge slice: {len(sel):,} rows -> {len(labels):,} labelled accessions",
      flush=True)
if labels.empty:
    raise SystemExit("[pool] the bridge carries no reviewed uniprot rows -- the cut that "
                     "defines the pool selected nothing")

wanted = dict(zip(labels["accession"], labels["mnxr_list"]))

# THE ALPHABET IS NARROWED TO WHAT THE EMBEDDER CAN TOKENISE, and this is not
# cosmetic. ProteinBERT's `aa_to_token_index` covers exactly ACDEFGHIKLMNPQRSTUVWXY;
# the image's encoder builds a lookup array sized to the largest of those ordinals
# ('Y', 89) and guards it with `if c > len(arrayed_map)` -- an off-by-one, so a
# residue whose ordinal is exactly 90 falls into the else branch and indexes past
# the end. 'Z' (Glx) is ordinal 90, Swiss-Prot uses it, and the whole run dies with
# `IndexError: getitem out of range` after the model has loaded. Measured on 2,000
# reviewed sequences: one of them carried a 'Z' and that was enough.
#
# So every residue outside the tokenisable set becomes 'X' -- which is what the
# encoder does with 'B' and '*' anyway (they map to the OTHER token), just done here
# where it cannot crash. Lowercase is upper-cased for the same reason: 'a' is
# ordinal 97 and would trip the identical bug.
POOL_ALPHABET = set("ACDEFGHIKLMNPQRSTUVWXY")


def tokenisable(seq):
    return "".join(c if c in POOL_ALPHABET else "X" for c in seq.upper())


# Swiss-Prot headers are `>sp|P12345|NAME_ORGANISM Description OS=...`, so the accession
# is the second pipe-delimited field. The bridge's uniprot ids come from rhea2uniprot,
# which writes bare accessions, so the two join directly.
written = 0
recoded = 0
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
                # Header is the bare accession: the embedder echoes it into its index,
                # and that is the key the labels are re-joined on.
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
    raise SystemExit("[pool] no pool sequences found -- the accession join broke. The "
                     "bridge's uniprot ids and Swiss-Prot's `sp|ACC|` field are the same "
                     "id space, so zero overlap is a parse bug, not a coverage fact")
# The cut IS Swiss-Prot: `reviewed` means the accession came from rhea2uniprot.tsv rather
# than the trembl file. So a labelled accession with no sequence here is an accession Rhea
# still lists that this Swiss-Prot release has demerged or deleted -- a handful, and a
# real finding about release skew between Rhea and UniProt if it is ever more than that.
if missing > 0.02 * len(wanted):
    raise SystemExit(f"[pool] {missing:,} of {len(wanted):,} reviewed accessions "
                     f"({100.0*missing/len(wanted):.1f}%) are absent from Swiss-Prot "
                     f"{SP.name}. The reviewed cut is meant to BE this release; a gap "
                     f"this size means Rhea and UniProt are far enough apart that the "
                     f"pool would silently be a subset of what it claims")

labels[labels["accession"].isin(seen)].to_parquet(LABELS_OUT, index=False)

reldate = SP / A.reldate_file
with open(SOURCE_OUT, "w") as fh:
    fh.write("sequences\tswissprot " + SP.name + "\n")
    fh.write("labels\tmnxr_lookup id_source=" + str(A.id_source) + " evidence_quality=" + str(A.evidence) + "\n")
    fh.write("embedder\tproteinbert (weights baked into env::proteinbert.env)\n")
    fh.write("sequences_written\t" + str(written) + "\n")
    fh.write("labelled_accessions\t" + str(len(wanted)) + "\n")
    if reldate.exists():
        fh.write("reldate\t" + reldate.read_text().strip().replace("\n", " | ") + "\n")

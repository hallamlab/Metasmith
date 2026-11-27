#!/usr/bin/env python3
"""Join Pfam, Foldseek and AlphaFold DB evidence for the CB2A island and top-30 proteins into one table.

    python consolidate.py WORKDIR

WORKDIR holds targets.tsv, pfam.tsv, afdb.json, esm/<gene_id>.pdb for each ESMFold model of a protein
AlphaFold DB lacks, and foldseek/<gene_id>.json for each searched model. An optional
short_labels.tsv (gene_id, short_label) adds the hand-checked figure label beside the automatic call.
"""

import csv
import json
import re
import sys
from pathlib import Path

W = Path(sys.argv[1])
DBS = ("pdb100", "afdb-swissprot", "afdb50")
# Foldseek's prob estimates P(same SCOP superfamily); 0.9 is a confident family call, 0.5 a lead.
CONFIDENT, LEAD = 0.9, 0.5
UNINFORMATIVE = re.compile(r"uncharacteri[sz]ed|hypothetical|unknown function|DUF\d|^\s*$|^protein$", re.I)


def target_name(t):
    name = re.sub(r"^AF-\S+-F\d+-model_v\d+\s*", "", t)
    name = re.sub(r"^\S+\.(?:cif|pdb)\.gz_\S+\s*", "", name)
    return name.strip()


def hits(gid):
    p = W / "foldseek" / f"{gid}.json"
    if not p.exists():
        return None
    out = {}
    for r in json.load(open(p)).get("results", []):
        al = r.get("alignments") or []
        al = al[0] if al and isinstance(al[0], list) else al
        out[r["db"]] = al
    return out


def best(al, db):
    keep = [a for a in al if not (db == "afdb50" and a["seqId"] >= 90)]
    named = [a for a in keep if not UNINFORMATIVE.search(target_name(a["target"]))]
    return (named or keep or [None])[0]


def esmfold_plddt(pdb):
    ca = [float(l[60:66]) for l in open(pdb) if l.startswith("ATOM") and l[12:16].strip() == "CA"]
    # ESMFold writes pLDDT to the B-factor on a 0-1 scale; AlphaFold DB uses 0-100.
    return 100 * sum(ca) / len(ca)


def main():
    targets = list(csv.DictReader(open(W / "targets.tsv"), delimiter="\t"))
    pfam = {r["gene_id"]: r for r in csv.DictReader(open(W / "pfam.tsv"), delimiter="\t")}
    afdb = {r["gene_id"]: r for r in json.load(open(W / "afdb.json"))}
    labels_tsv = W / "short_labels.tsv"
    labels = ({r["gene_id"]: r["short_label"] for r in csv.DictReader(open(labels_tsv), delimiter="\t")}
              if labels_tsv.exists() else {})
    rows = []
    for t in targets:
        g = t["gene_id"]
        h = hits(g)
        row = {k: t[k] for k in ("gene_id", "protein_id", "start", "end", "strand", "product", "len",
                                 "in_island", "in_top30", "TPM_cb2a_cu3")}
        row["pfam"] = pfam[g]["pfam"] if g in pfam else "scan failed (server error)"
        row["pfam_below_GA"] = pfam[g]["pfam_below_GA"] if g in pfam else ""
        row["afdb_model"] = afdb[g]["afdb_acc"]
        row["plddt"] = afdb[g]["plddt"]
        row["structure_source"] = "AlphaFold DB" if afdb[g]["afdb_acc"] else ""
        esm = W / "esm" / f"{g}.pdb"
        if not afdb[g]["afdb_acc"] and esm.exists():
            row["structure_source"] = "ESMFold"
            row["plddt"] = f"{esmfold_plddt(esm):.1f}"
        for db in DBS:
            b = best(h.get(db, []), db) if h else None
            row[f"{db}_hit"] = target_name(b["target"]) if b else ""
            row[f"{db}_taxon"] = b["taxName"] if b else ""
            row[f"{db}_prob"] = f"{b['prob']:.2f}" if b else ""
            row[f"{db}_evalue"] = f"{b['eval']:.1e}" if b else ""
            row[f"{db}_seqid"] = f"{b['seqId']:.0f}" if b else ""
        row["foldseek_searched"] = "yes" if h else "no"
        row["family_call"], row["evidence"] = call(row)
        row["figure_label"] = labels.get(g, "")
        rows.append(row)
    with open(W / "annotations.tsv", "w") as o:
        w = csv.DictWriter(o, fieldnames=list(rows[0]), delimiter="\t")
        w.writeheader()
        w.writerows(rows)
    for r in rows:
        if r["product"] and UNINFORMATIVE.search(r["product"]) or "family" in r["product"]:
            print(f"{r['gene_id'][7:]:<8} {r['product'][:34]:<34} -> {r['family_call'][:70]:<70} [{r['evidence']}]")


def call(r):
    for db in ("pdb100", "afdb-swissprot"):
        p = float(r[f"{db}_prob"] or 0)
        if p >= CONFIDENT and r[f"{db}_hit"] and not UNINFORMATIVE.search(r[f"{db}_hit"]):
            return r[f"{db}_hit"], f"Foldseek {db} prob {p:.2f}"
    if r["pfam"] and not r["pfam"].startswith("scan failed"):
        return r["pfam"].split(" (")[0], "Pfam"
    for db in ("pdb100", "afdb-swissprot"):
        p = float(r[f"{db}_prob"] or 0)
        if p >= LEAD and r[f"{db}_hit"] and not UNINFORMATIVE.search(r[f"{db}_hit"]):
            return r[f"{db}_hit"] + "?", f"Foldseek {db} prob {p:.2f} (tentative)"
    if r["foldseek_searched"] == "no":
        return "", "no structure yet"
    return "", "no family found"


if __name__ == "__main__":
    main()

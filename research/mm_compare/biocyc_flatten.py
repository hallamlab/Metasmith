#!/usr/bin/env python3
"""Flatten BioCyc 26 SQLite PGDBs into the reaction, reaction-gene, gene and pathway tables pgdb_extract.py writes.

    biocyc_flatten.py <out dir> <strain>=<pgdb file> ...

A BioCyc 26 .pgdb is a SQLite file holding each flat-file frame as JSON. Pathways are every
pathway frame the PGDB holds, super-pathways included, since it carries no pwy-evidence-list.
Genes are keyed by the PGDB's gene frame id, which is the RefSeq locus tag of the genome it was
built from. Writes <strain>.{reactions,reaction_genes,genes,pathways}.tsv.gz and <strain>.info.tsv.
"""
import csv
import gzip
import json
import sqlite3
import sys
from pathlib import Path


def table(c, name):
    for key, js in c.execute(f"select key, json from {name}"):
        yield key, json.loads(js)


def write(path, header, rows):
    with gzip.open(path, "wt") as f:
        w = csv.writer(f, delimiter="\t", lineterminator="\n")
        w.writerow(header)
        w.writerows(rows)


def flatten(strain, pgdb, out):
    c = sqlite3.connect(pgdb)
    prot = {k: (d.get("GENE", []), d.get("COMPONENTS", [])) for k, d in table(c, "proteins_dat")}

    def genes(pid, depth=0):
        g, comps = prot.get(pid, ([], []))
        found = list(g)
        if depth == 0:
            for comp in comps:
                found += genes(comp if isinstance(comp, str) else comp[0], 1)
        return found

    write(out / f"{strain}.reactions.tsv.gz", ["pgdb", "reaction", "ec"],
          [(strain, k, "|".join(d.get("EC-NUMBER", []))) for k, d in table(c, "reactions_dat")])
    write(out / f"{strain}.reaction_genes.tsv.gz", ["pgdb", "reaction", "gene"],
          sorted({(strain, r, g) for _, d in table(c, "enzrxns_dat")
                  for r in d.get("REACTION", []) for g in genes(d.get("ENZYME", [""])[0])}))
    write(out / f"{strain}.genes.tsv.gz", ["pgdb", "gene", "name", "accession_2"],
          [(strain, k, "|".join(d.get("COMMON-NAME", [])), "|".join(d.get("ACCESSION-2", [])))
           for k, d in table(c, "genes_dat")])
    write(out / f"{strain}.pathways.tsv.gz", ["pgdb", "pathway", "evidenced_reactions"],
          [(strain, k, "|".join(d.get("REACTION-LIST", []))) for k, d in table(c, "pathways_dat")])
    keep = ("ORGANISM", "VERSION", "RELEASE-DATE", "GENOME", "GENOME-UPDATE-DATE", "PGDB-TIER", "COMMENT")
    write(out / f"{strain}.info.tsv.gz", ["key", "value"],
          [(k, v) for k, v in c.execute("select key, val from info") if k in keep])


def main():
    out = Path(sys.argv[1])
    out.mkdir(parents=True, exist_ok=True)
    for arg in sys.argv[2:]:
        strain, pgdb = arg.split("=", 1)
        flatten(strain, pgdb, out)
        print(strain, "flattened", file=sys.stderr)


if __name__ == "__main__":
    main()

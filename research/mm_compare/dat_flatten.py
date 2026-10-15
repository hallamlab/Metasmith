#!/usr/bin/env python3
"""Flatten a Pathway Tools attribute-value dump (a directory of .dat files) into the tables biocyc_flatten.py writes.

    dat_flatten.py <out dir> <strain>=<dat dir> ...

Reads reactions, enzrxns, proteins, genes and pathways .dat files from <dat dir>. Pathways come
from ../reports/pwy-evidence-list.dat when PathoLogic wrote one, the source pgdb_extract.py reads,
and otherwise from every pathway frame with its REACTION-LIST, as for a curated PGDB. Genes carry
ACCESSION-1, the locus tag (a b-number in EcoCyc).
"""
import csv
import gzip
import re
import sys
from pathlib import Path


def frames(path):
    rec = {}
    for line in path.read_text(encoding="latin-1").splitlines():
        if line.startswith("#"):
            continue
        if line.startswith("//"):
            if rec:
                yield rec
            rec = {}
            continue
        if " - " in line and not line.startswith("/"):
            k, v = line.split(" - ", 1)
            rec.setdefault(k, []).append(v)
    if rec:
        yield rec


def write(path, header, rows):
    with gzip.open(path, "wt") as f:
        w = csv.writer(f, delimiter="\t", lineterminator="\n")
        w.writerow(header)
        w.writerows(rows)


def flatten(strain, d, out):
    prot = {p["UNIQUE-ID"][0]: (p.get("GENE", []), [c.split()[0] for c in p.get("COMPONENTS", [])])
            for p in frames(d / "proteins.dat")}

    def genes(pid, depth=0):
        g, comps = prot.get(pid, ([], []))
        found = list(g)
        if depth == 0:
            for c in comps:
                found += genes(c, 1)
        return found

    write(out / f"{strain}.reactions.tsv.gz", ["pgdb", "reaction", "ec"],
          [(strain, r["UNIQUE-ID"][0], "|".join(r.get("EC-NUMBER", []))) for r in frames(d / "reactions.dat")])
    write(out / f"{strain}.reaction_genes.tsv.gz", ["pgdb", "reaction", "gene"],
          sorted({(strain, r, g) for e in frames(d / "enzrxns.dat")
                  for r in e.get("REACTION", []) for g in genes(e.get("ENZYME", [""])[0])}))
    write(out / f"{strain}.genes.tsv.gz", ["pgdb", "gene", "name", "accession_1"],
          [(strain, g["UNIQUE-ID"][0], "|".join(g.get("COMMON-NAME", [])), "|".join(g.get("ACCESSION-1", [])))
           for g in frames(d / "genes.dat")])
    evidence = d.parent / "reports" / "pwy-evidence-list.dat"
    if evidence.exists():
        pwys = []
        for line in evidence.read_text(encoding="latin-1").splitlines():
            if line.startswith("("):
                ids = re.findall(r'"([^"]+)"', line)
                if ids:
                    pwys.append((strain, ids[0], "|".join(ids[1:])))
    else:
        pwys = [(strain, p["UNIQUE-ID"][0], "|".join(p.get("REACTION-LIST", []))) for p in frames(d / "pathways.dat")]
    write(out / f"{strain}.pathways.tsv.gz", ["pgdb", "pathway", "evidenced_reactions"], pwys)


def main():
    out = Path(sys.argv[1])
    out.mkdir(parents=True, exist_ok=True)
    for arg in sys.argv[2:]:
        strain, d = arg.split("=", 1)
        flatten(strain, Path(d), out)
        print(strain, "flattened", file=sys.stderr)


if __name__ == "__main__":
    main()

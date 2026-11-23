#!/usr/bin/env python3
"""Flatten one sample's MetaPathways PGDB tarballs into reaction, reaction-gene and pathway tables.

    pgdb_extract.py <sample dir> <out dir> [--procs N]

Reads <sample dir>/pgdb/{community,MAGs/*}/*cyc.tar.bz2 and writes three gzipped TSVs, one row per
PGDB entity, keyed by `pgdb` (community, or the CAMI_OTU genome name): reactions (id, EC numbers),
reaction_genes (reaction, gene via enzrxns -> protein -> gene, one complex level resolved) and
pathways (the pathways PathoLogic inferred, from pwy-evidence-list.dat, with their evidenced reactions).
"""
import argparse
import gzip
import re
import sys
import tarfile
from multiprocessing import Pool
from pathlib import Path

WANT = ("data/reactions.dat", "data/enzrxns.dat", "data/proteins.dat", "reports/pwy-evidence-list.dat")


def frames(text):
    rec = {}
    for line in text.splitlines():
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


def one(path):
    name = path.parent.name
    files = {}
    with tarfile.open(path, "r:bz2") as t:
        for m in t:
            for w in WANT:
                if m.name.endswith(w):
                    files[w] = t.extractfile(m).read().decode("latin-1")
    rxns = [(r["UNIQUE-ID"][0], "|".join(r.get("EC-NUMBER", [])))
            for r in frames(files.get(WANT[0], ""))]
    prot = {}
    for p in frames(files.get(WANT[2], "")):
        prot[p["UNIQUE-ID"][0]] = (p.get("GENE", []), [c.split()[0] for c in p.get("COMPONENTS", [])])

    def genes(pid, depth=0):
        g, comps = prot.get(pid, ([], []))
        out = list(g)
        if depth == 0:
            for c in comps:
                out += genes(c, 1)
        return out

    rg = sorted({(r, g) for e in frames(files.get(WANT[1], ""))
                 for r in e.get("REACTION", []) for g in genes(e.get("ENZYME", [""])[0])})
    pwys = []
    for line in files.get(WANT[3], "").splitlines():
        if line.startswith("("):
            ids = re.findall(r'"([^"]+)"', line)
            if ids:
                pwys.append((ids[0], "|".join(ids[1:])))
    return name, rxns, rg, pwys, sorted(files)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("sample_dir", type=Path)
    ap.add_argument("out_dir", type=Path)
    ap.add_argument("--procs", type=int, default=8)
    a = ap.parse_args()
    tars = sorted(a.sample_dir.glob("pgdb/community/*cyc.tar.bz2")) + sorted(a.sample_dir.glob("pgdb/MAGs/*/*cyc.tar.bz2"))
    a.out_dir.mkdir(parents=True, exist_ok=True)
    s = a.sample_dir.name
    outs = {k: gzip.open(a.out_dir / f"{s}.{k}.tsv.gz", "wt") for k in ("reactions", "reaction_genes", "pathways")}
    outs["reactions"].write("pgdb\treaction\tec\n")
    outs["reaction_genes"].write("pgdb\treaction\tgene\n")
    outs["pathways"].write("pgdb\tpathway\tevidenced_reactions\n")
    short = 0
    with Pool(a.procs) as pool:
        for name, rxns, rg, pwys, got in pool.imap_unordered(one, tars, chunksize=4):
            short += len(got) < len(WANT)
            outs["reactions"].writelines(f"{name}\t{r}\t{e}\n" for r, e in rxns)
            outs["reaction_genes"].writelines(f"{name}\t{r}\t{g}\n" for r, g in rg)
            outs["pathways"].writelines(f"{name}\t{p}\t{r}\n" for p, r in pwys)
    for f in outs.values():
        f.close()
    print(f"{s}: {len(tars)} PGDBs, {short} missing a wanted file", file=sys.stderr)


if __name__ == "__main__":
    main()

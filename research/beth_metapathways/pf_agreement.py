#!/usr/bin/env python3
"""How closely the port's PathoLogic input matches MetaPathways 4's, gene by gene.

    pf_agreement.py MP4_PF PORT_PF ORFS_FAA MP4_MAPPING

Matches genes by contig and coordinates (MP4_MAPPING is MetaPathways' preprocessed
<sample>.mapping.txt; the port's coordinates come from the Prodigal headers of ORFS_FAA) and prints
one TSV row: genes in each .pf, genes in both, and among those the share with the same FUNCTION,
the same EC set and the same METACYC set.
"""
import sys
from collections import defaultdict


def records(pf):
    out, rec = {}, defaultdict(list)
    with open(pf) as fh:
        for line in fh:
            line = line.rstrip("\n")
            if line == "//":
                out[rec["ID"][0]] = rec
                rec = defaultdict(list)
            elif "\t" in line:
                k, v = line.split("\t", 1)
                rec[k].append(v)
    return out


def main():
    mp4_pf, port_pf, faa, mapping = sys.argv[1:5]
    contig = {}
    with open(mapping) as fh:
        for line in fh:
            f = line.rstrip("\n").split("\t")
            contig[f[0].rsplit("-", 1)[1]] = f[1]
    coords = {}
    with open(faa) as fh:
        for line in fh:
            if line.startswith(">"):
                f = line[1:].split(" # ")
                orf = f[0].split()[0]
                coords[orf] = (orf.rsplit("_", 1)[0], min(int(f[1]), int(f[2])), max(int(f[1]), int(f[2])))
    mp4 = {}
    for gid, r in records(mp4_pf).items():
        a, b = int(r["STARTBASE"][0]), int(r["ENDBASE"][0])
        mp4[(contig[gid.split("-")[0]], min(a, b), max(a, b))] = r
    port = {coords[r["NAME"][0]]: r for r in records(port_pf).values()}
    both = set(mp4) & set(port)
    same = {k: sum(set(mp4[g].get(k, [])) == set(port[g].get(k, [])) for g in both) for k in ("FUNCTION", "EC", "METACYC")}
    print("mp4_genes\tport_genes\tboth\tsame_function\tsame_ec\tsame_metacyc")
    n = len(both) or 1
    print(f"{len(mp4)}\t{len(port)}\t{len(both)}\t{same['FUNCTION'] / n:.3f}\t{same['EC'] / n:.3f}\t{same['METACYC'] / n:.3f}")


if __name__ == "__main__":
    sys.exit(main())

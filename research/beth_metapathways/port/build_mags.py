#!/usr/bin/env python3
"""Write each eligible UniteM bin of Beth's assemblies as a FASTA cut from its own assembly.

Records are copied verbatim, so a MAG's contig names are the assembly's and the bin PGDB can take
the assembly's ORF annotations. Prints the manifest (sample, bin_id, path, contigs, bases) to stdout.
Run on a fir login node: stdlib only.
"""

import argparse
import csv
import gzip
import sys
from collections import defaultdict
from pathlib import Path

ASSEMBLER = {"hybridspades": "hybrid_metaspades", "operams": "operams", "shortspades": "short_metaspades"}


def eligible_unitem(table):
    bins = defaultdict(set)
    with open(table) as fh:
        for r in csv.DictReader(fh, delimiter="\t"):
            if r["binner"] == "unitem" and r["eligible"] == "True":
                bins[(r["sample"], r["assembler"])].add(r["bin_id"])
    return bins


def contig_bins(contig_map, wanted):
    owner = {}
    with gzip.open(contig_map, "rt") as fh:
        for r in csv.DictReader(fh, delimiter="\t"):
            if r["new_bin_id"] in wanted:
                owner[r["original_contig_id"]] = r["new_bin_id"]
    return owner


def split(assembly, owner, out_dir):
    out_dir.mkdir(parents=True, exist_ok=True)
    handles, stats, sink = {}, defaultdict(lambda: [0, 0]), None
    with open(assembly) as fh:
        for line in fh:
            if line.startswith(">"):
                b = owner.get(line[1:].split()[0])
                if b is not None and b not in handles:
                    handles[b] = open(out_dir / f"{b}.fasta", "w")
                sink = handles.get(b) if b else None
                if sink:
                    stats[b][0] += 1
            elif sink:
                stats[b][1] += len(line.strip())
            if sink:
                sink.write(line)
    for h in handles.values():
        h.close()
    return stats


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--root", type=Path, default=Path("/scratch/phyberos/beth_mp"))
    ap.add_argument("--out", type=Path, default=Path("/scratch/phyberos/beth_mp/port/mags"))
    ap.add_argument("--sample", action="append", help="an assembly stem; default every one")
    a = ap.parse_args()

    bins = eligible_unitem(a.root / "meta" / "eligible_bins.tsv")
    stems = a.sample or sorted(p.stem for p in (a.root / "inputs").glob("*.fasta"))
    w = csv.writer(sys.stdout, delimiter="\t", lineterminator="\n")
    w.writerow(["sample", "bin_id", "path", "contigs", "bases"])
    for stem in stems:
        method, sample = stem.split("_", 1)
        wanted = bins.get((sample, ASSEMBLER[method]), set())
        owner = contig_bins(a.root / "meta" / "contig_maps" / f"{stem}.unitem.tsv.gz", wanted)
        stats = split(a.root / "inputs" / f"{stem}.fasta", owner, a.out / stem)
        missing = wanted - set(stats)
        if missing:
            sys.exit(f"{stem}: no contigs written for {sorted(missing)}")
        for b in sorted(stats):
            w.writerow([stem, b, a.out / stem / f"{b}.fasta", *stats[b]])


if __name__ == "__main__":
    main()

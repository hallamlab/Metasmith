#!/usr/bin/env python3
"""Write MetaPathways analysis_wf inputs for Beth's TCAG assemblies.

    python3 make_mp_inputs.py BETH_MP_ROOT

Reads BETH_MP_ROOT/inputs/<sample>.fasta, meta/eligible_bins.tsv and
meta/contig_maps/<sample>.unitem.tsv.gz. Writes mp4/mag_maps/<sample>.tsv (headerless
contig<TAB>bin, eligible UniteM bins only) and mp4/manifest.tsv with the parent assemblies last.
"""

import csv
import gzip
import sys
from collections import defaultdict
from pathlib import Path

PREFIX = {"hybrid_metaspades": "hybridspades", "operams": "operams", "short_metaspades": "shortspades"}


def main():
    root = Path(sys.argv[1])
    out = root / "mp4"
    (out / "mag_maps").mkdir(parents=True, exist_ok=True)

    eligible = defaultdict(set)
    with open(root / "meta" / "eligible_bins.tsv") as fh:
        for r in csv.DictReader(fh, delimiter="\t"):
            if r["binner"] == "unitem" and r["eligible"] == "True":
                eligible[f"{PREFIX[r['assembler']]}_{r['sample']}"].add(r["bin_id"])

    samples = sorted(p.stem for p in (root / "inputs").glob("*.fasta"))
    samples.sort(key=lambda s: s.endswith("_cpe3_parent"))
    rows = []
    for s in samples:
        contigs = {line[1:].split()[0] for line in open(root / "inputs" / f"{s}.fasta") if line.startswith(">")}
        seen, lines = {}, []
        with gzip.open(root / "meta" / "contig_maps" / f"{s}.unitem.tsv.gz", "rt") as fh:
            for r in csv.DictReader(fh, delimiter="\t"):
                b, c = r["new_bin_id"], r["original_contig_id"]
                if b not in eligible[s]:
                    continue
                if c not in contigs:
                    sys.exit(f"{s}: {c} of {b} is not in the assembly")
                if c in seen and seen[c] != b:
                    sys.exit(f"{s}: {c} is in both {seen[c]} and {b}")
                seen[c] = b
                lines.append(f"{c}\t{b}\n")
        found = set(seen.values())
        if found != eligible[s]:
            sys.exit(f"{s}: map lacks bins {sorted(eligible[s] - found)}")
        (out / "mag_maps" / f"{s}.tsv").write_text("".join(lines))
        rows.append([s, str(root / "inputs" / f"{s}.fasta"), "none", "", "", str(out / "mag_maps" / f"{s}.tsv")])
        print(f"{s}\t{len(found)} bins\t{len(lines)} contigs")

    with open(out / "manifest.tsv", "w") as fh:
        fh.write("sample_id\tassembly\tread_layout\treads_1\treads_2\tmag_map\n")
        fh.writelines("\t".join(r) + "\n" for r in rows)
    print(f"{len(rows)} samples, {sum(len(eligible[s]) for s in samples)} bins")


if __name__ == "__main__":
    main()

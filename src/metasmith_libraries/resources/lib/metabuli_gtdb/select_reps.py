"""Select the GTDB species representatives a Metabuli database is built from.

The R214.1 rule: a species representative whose NCBI assembly level is Complete Genome or
Chromosome, with CheckM2 completeness above 90 and contamination below 5. Writes
selected.tsv (one row per genome) and members.txt (each genome's path inside the GTDB
representative genome archive, for `tar -x -T`).
"""

import argparse
import csv
import gzip
import sys
from pathlib import Path

LEVELS = {"Complete Genome", "Chromosome"}


def selected(metadata: Path):
    with gzip.open(metadata, "rt", newline="") as fh:
        for row in csv.DictReader(fh, delimiter="\t"):
            if row["gtdb_representative"] != "t" or row["ncbi_assembly_level"] not in LEVELS:
                continue
            if float(row["checkm2_completeness"]) > 90 and float(row["checkm2_contamination"]) < 5:
                yield row


def member(accession: str, root: str) -> str:
    acc = accession.removeprefix("RS_").removeprefix("GB_")
    digits = acc[4:13]
    return f"{root}/database/{acc[:3]}/{digits[0:3]}/{digits[3:6]}/{digits[6:9]}/{acc}_genomic.fna.gz"


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--metadata", nargs="+", required=True, help="GTDB <domain>_metadata_<release>.tsv.gz tables")
    p.add_argument("--archive-root", required=True, help="top directory of the representative genome archive")
    p.add_argument("--out", required=True, type=Path)
    a = p.parse_args()

    a.out.mkdir(parents=True, exist_ok=True)
    n, bp = 0, 0
    with open(a.out / "selected.tsv", "w") as sel, open(a.out / "members.txt", "w") as mem:
        sel.write("accession\tgenome_size\tgtdb_taxonomy\n")
        for table in a.metadata:
            for row in selected(Path(table)):
                sel.write(f"{row['accession']}\t{row['genome_size']}\t{row['gtdb_taxonomy']}\n")
                mem.write(member(row["accession"], a.archive_root) + "\n")
                n += 1
                bp += int(row["genome_size"])
    print(f"selected {n:,} representatives, {bp / 1e9:.1f} Gbp", file=sys.stderr)
    if n == 0:
        sys.exit("no representative passed the selection")


if __name__ == "__main__":
    main()

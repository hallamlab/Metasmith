#!/usr/bin/env python3
"""Turn the metasmith pathologic run's pgdb_csv_tables into the per-strain tables the other PGDB sources use.

    pathologic_tables.py <repo root>

Reads data/scratch/mm_compare_pathologic/results/annotation-pgdb_csv_tables/*.tgz. Each PGDB names
its strain only through its gene ids, which carry the RefSeq accession of the proteome, so that
accession picks the strain. Writes data/mm_compare/ecoli_pgdb/metasmith_pathologic/<strain>.*.tsv.gz.
"""
import csv
import gzip
import io
import sys
import tarfile
from pathlib import Path

STRAINS = ("k12", "w3110", "bw25113", "dh1", "dh10b", "epi300")


def main(repo: Path):
    accessions = {next((repo / "data/fabfos/originals/genomes" / f"e_coli_{s}" / "genome").glob("*.faa")).stem
                  .upper(): s for s in STRAINS}
    src = repo / "data/scratch/mm_compare_pathologic/results/annotation-pgdb_csv_tables"
    out = repo / "data/mm_compare/ecoli_pgdb/metasmith_pathologic"
    out.mkdir(parents=True, exist_ok=True)
    for t in sorted(src.glob("*.tgz")):
        with tarfile.open(t) as tf:
            tabs = {Path(m.name).stem: list(csv.reader(io.TextIOWrapper(tf.extractfile(m))))
                    for m in tf if m.name.endswith(".csv")}
        gene = tabs["genes"][1][0]
        strain = next(s for a, s in accessions.items() if a in gene)
        for name, rows in tabs.items():
            path = out / f"{strain}.{name}.tsv.gz"
            path.unlink(missing_ok=True)
            with gzip.open(path, "wt") as f:
                w = csv.writer(f, delimiter="\t", lineterminator="\n")
                w.writerow(["pgdb"] + rows[0])
                w.writerows([strain] + r for r in rows[1:])
        print(strain, "reactions", len(tabs["reactions"]) - 1,
              "with gene", len({r[0] for r in tabs["reaction_genes"][1:]}),
              "pathways", len(tabs["pathways"]) - 1, file=sys.stderr)


if __name__ == "__main__":
    main(Path(sys.argv[1]))

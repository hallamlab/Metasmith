#!/usr/bin/env python3
"""Per-PGDB counts from the flattened MetaPathways PGDB tables, in Pathway Tools' own terms.

    pgdb_inventory.py <data/mm_compare>

Reads ryan_pgdb_tables/<sample>.{reactions,reaction_genes,pathways}.tsv.gz (from pgdb_extract.py),
ryan_pgdb_pwy/<sample>/pgdb/**/*_pwy.tsv and ecoli_pgdb/<route>/<strain>.*.tsv.gz, and writes
inventory/pgdb_models.tsv, one row per PGDB. `route` names the pipeline that built it.
"""
import csv
import gzip
import sys
from collections import defaultdict
from pathlib import Path


def read(path):
    with gzip.open(path, "rt") as f:
        yield from csv.DictReader(f, delimiter="\t")


def counts(rx, rg, pw):
    c = defaultdict(lambda: defaultdict(set))
    for r in read(rx):
        c[r["pgdb"]]["reactions"].add(r["reaction"])
        if r["ec"]:
            c[r["pgdb"]]["reactions_with_ec"].add(r["reaction"])
    for r in read(rg):
        c[r["pgdb"]]["reactions_with_gene"].add(r["reaction"])
        c[r["pgdb"]]["genes_with_reaction"].add(r["gene"])
    for r in read(pw):
        c[r["pgdb"]]["pathways"].add(r["pathway"])
    return c


COUNTED = ("reactions", "reactions_with_gene", "reactions_with_ec", "genes_with_reaction", "pathways")


def main(root: Path):
    rows = []
    for rx in sorted((root / "ryan_pgdb_tables").glob("*.reactions.tsv.gz")):
        sample = rx.name.split(".")[0]
        c = counts(rx, rx.with_name(f"{sample}.reaction_genes.tsv.gz"), rx.with_name(f"{sample}.pathways.tsv.gz"))
        scores = {}
        for pwy in (root / "ryan_pgdb_pwy" / sample / "pgdb").rglob("*_pwy.tsv"):
            with open(pwy) as f:
                s = [float(r["PWY_SCORE"]) for r in csv.DictReader(f, delimiter="\t") if r["PWY_SCORE"]]
            scores[pwy.parent.name] = (len(s), sum(s) / len(s) if s else "")
        for pgdb, k in sorted(c.items()):
            n_pwy_tsv, mean_score = scores.get(pgdb, ("", ""))
            rows.append(["cami_hmp", "ryan_metapathways", sample, pgdb,
                         "community" if pgdb == "community" else "genome",
                         *(len(k[x]) for x in COUNTED), n_pwy_tsv, mean_score])
    for route in sorted(d for d in (root / "ecoli_pgdb").iterdir() if d.is_dir()):
        for rx in sorted(route.glob("*.reactions.tsv.gz")):
            strain = rx.name.split(".")[0]
            c = counts(rx, rx.with_name(f"{strain}.reaction_genes.tsv.gz"), rx.with_name(f"{strain}.pathways.tsv.gz"))
            rows.append(["ecoli", route.name, strain, strain, "genome", *(len(c[strain][x]) for x in COUNTED), "", ""])
    out = root / "inventory"
    out.mkdir(exist_ok=True)
    (out / "pgdb_models.tsv").unlink(missing_ok=True)
    with open(out / "pgdb_models.tsv", "w") as f:
        w = csv.writer(f, delimiter="\t", lineterminator="\n")
        w.writerow(["dataset", "route", "sample", "subject", "level", "reactions", "reactions_with_gene",
                    "reactions_with_ec", "genes_with_reaction", "pathways", "pwy_tsv_rows", "pwy_score_mean"])
        w.writerows(rows)
    print(len(rows), "PGDBs", file=sys.stderr)


if __name__ == "__main__":
    main(Path(sys.argv[1]))

"""Partition a ppanggolin matrix into core / accessory / unique families.

    ppanggolin_summary.py <matrix.csv> <kofamscan_descriptions.csv> <out.tsv>

The matrix is ppanggolin's roary-format `matrix.csv`: a fixed block of metadata
columns followed by one column per genome, each holding that genome's copies of
the family or an empty cell. A family present in every genome is core, in exactly
one is unique, and anything between is accessory -- and the accessory fraction is
where a lifestyle difference between strains shows.

The function column is a best-effort join. ppanggolin names a family by its
representative gene and carries the annotation text it inherited; there is no KO
column, so the only key the two tables share is a KO accession appearing inside
that text. Families whose annotation carries none get an empty function rather
than a wrong one.
"""
import re
import sys

import polars as pl

# The roary/ppanggolin metadata block. Everything from here on is a genome.
GENOME_COL_START = 14
KO = re.compile(r"\bK\d{5}\b")


def load_ko_definitions(path: str) -> dict[str, str]:
    frame = pl.read_csv(path, infer_schema_length=0)
    if frame.height == 0 or frame.width < 2:
        return {}
    key, value = frame.columns[0], frame.columns[-1]
    return dict(zip(frame[key].to_list(), frame[value].to_list()))


def main(argv: list[str]) -> int:
    matrix_path, kofam_path, out_path = argv
    matrix = pl.read_csv(matrix_path, infer_schema_length=0)
    genomes = matrix.columns[GENOME_COL_START:]
    assert genomes, (
        f"[{matrix_path}] has {matrix.width} columns, so nothing past the "
        f"{GENOME_COL_START}-column metadata block is a genome"
    )
    definitions = load_ko_definitions(kofam_path)

    name_col = matrix.columns[0]
    annot_col = matrix.columns[2] if matrix.width > 2 else name_col

    rows = []
    for record in matrix.iter_rows(named=True):
        present = sum(
            1 for g in genomes
            if (record.get(g) or "").strip() not in ("", "-")
        )
        if present >= len(genomes):
            partition = "core"
        elif present <= 1:
            partition = "unique"
        else:
            partition = "accessory"
        annotation = record.get(annot_col) or ""
        hit = KO.search(annotation)
        ko = hit.group(0) if hit else ""
        rows.append({
            "family": record.get(name_col) or "",
            "partition": partition,
            "n_genomes": present,
            "fraction": round(present / len(genomes), 4),
            "KO": ko,
            "function": definitions.get(ko, ""),
            "annotation": annotation,
        })

    pl.DataFrame(rows).write_csv(out_path, separator="\t")
    counts = {p: sum(1 for r in rows if r["partition"] == p) for p in ("core", "accessory", "unique")}
    print(f"{len(rows)} families over {len(genomes)} genomes: {counts} -> {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

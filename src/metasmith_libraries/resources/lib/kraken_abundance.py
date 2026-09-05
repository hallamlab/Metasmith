"""Pivot per-sample kraken2 reports into one sample-by-taxon count matrix.

    kraken_abundance.py <out.tsv> <rank> <name>=<report> [<name>=<report> ...]

`rank` is a kraken rank code (S, G, F, ...). Rows are samples, columns are taxa,
counts are the reads assigned at or below the taxon -- the fourth kreport column.
"""
import sys

import polars as pl


def read_report(path: str, rank: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    with open(path, errors="replace") as fh:
        for line in fh:
            if not line.strip():
                continue
            fields = line.rstrip("\n").split("\t")
            if len(fields) < 6:
                continue
            # pct, reads_clade, reads_taxon, rank_code, taxid, indented_name
            if fields[3].strip() != rank:
                continue
            name = fields[5].strip()
            try:
                counts[name] = counts.get(name, 0) + int(fields[1])
            except ValueError:
                continue
    return counts


def main(argv: list[str]) -> int:
    out_path, rank, *pairs = argv
    per_sample: dict[str, dict[str, int]] = {}
    for spec in pairs:
        name, _, path = spec.partition("=")
        assert path, f"expected <name>=<report>, got [{spec}]"
        per_sample[name] = read_report(path, rank)

    taxa = sorted({t for c in per_sample.values() for t in c})
    rows = [
        {"sample": name, **{t: counts.get(t, 0) for t in taxa}}
        for name, counts in sorted(per_sample.items())
    ]
    # An empty schema still has to carry the sample column, or a survey where no
    # sample reached the rank writes a headerless file that reads as corrupt.
    frame = pl.DataFrame(rows, schema=["sample"] + taxa) if rows else pl.DataFrame(schema={"sample": pl.Utf8})
    frame.write_csv(out_path, separator="\t")
    print(f"{len(rows)} samples x {len(taxa)} taxa at rank [{rank}] -> {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

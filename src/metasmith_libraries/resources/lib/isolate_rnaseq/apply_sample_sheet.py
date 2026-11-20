"""Rename a raw count table's library columns to their copper dose, in dose order.

    apply_sample_sheet.py <raw_count_table.tsv> <sample_sheet.tsv> <gene_count_table.csv>

The sheet has columns `library` and `copper_ppm`. The output is the bare count matrix, so
any count consumer reads it.
"""
import sys

import pandas as pd

from sheet import read_sheet


def main(raw: str, sheet: str, out: str) -> None:
    table = pd.read_csv(raw, sep="\t", index_col="gene_id")
    doses = read_sheet(sheet)
    missing = set(doses.index) - set(table.columns)
    assert not missing, f"sample sheet names libraries absent from the counts: {sorted(missing)}"
    counts = table[list(doses.index)]
    counts.columns = list(doses.label)
    counts.to_csv(out)
    print(f"{len(counts)} genes x {len(doses)} libraries: " + ", ".join(f"{i}={l}" for i, l in doses.label.items()))


if __name__ == "__main__":
    main(*sys.argv[1:4])

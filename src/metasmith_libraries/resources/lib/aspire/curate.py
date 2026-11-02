#!/usr/bin/env python3
"""Apply the non-target call to an ASV table: the counts kept, and the counts removed with a reason.

The four filters and their order are upstream's FILTER_COUNTS, called through
filter_nontarget.py's own functions so the kept table is the one upstream writes. Upstream
then partitioned the rest into three more count tables; here each removed ASV names the
first filter that dropped it.
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import filter_nontarget as fn  # noqa: E402

MITO_COLS = ["MITOMASTER", "BLAST_mito"]


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--counts", type=Path, required=True)
    ap.add_argument("--taxonomy", type=Path, required=True)
    ap.add_argument("--master", type=Path, required=True, help="mito_checker.py's master table")
    ap.add_argument("--abundance-threshold", type=float, required=True)
    ap.add_argument("--min-consensus", type=float, required=True)
    ap.add_argument("--exclude-taxon", action="append", default=[])
    ap.add_argument("--clean", type=Path, required=True)
    ap.add_argument("--removed", type=Path, required=True)
    args = ap.parse_args()

    counts = fn.load_table(args.counts)
    master = fn.clean_index(fn.load_table(args.master))
    tax = fn.clean_index(fn.load_table(args.taxonomy))

    decon, micro, mito = fn.filter_nontarget_asvs(counts, master, "BioFactorial", MITO_COLS)
    abundant = fn.filter_by_abundance(micro, args.abundance_threshold)
    clean = fn.filter_by_taxonomy(abundant, tax, "Taxon", "Consensus", args.min_consensus,
                                  fn.parse_exclude_taxa(args.exclude_taxon))

    reason = {}
    for asv in counts.index:
        if asv not in decon.index:
            reason[asv] = "contaminant"
        elif asv in mito.index:
            reason[asv] = "mitochondrial"
        elif asv not in abundant.index:
            reason[asv] = "abundance"
        elif asv not in clean.index:
            reason[asv] = "taxonomy"

    removed = counts.loc[[a for a in counts.index if a in reason]].copy()
    removed.insert(0, "reason", [reason[a] for a in removed.index])
    removed.index.name = counts.index.name
    clean.index.name = counts.index.name
    fn.save_output(clean, args.clean, "clean counts")
    fn.save_output(removed, args.removed, "removed counts")
    assert len(clean) + len(removed) == len(counts)


if __name__ == "__main__":
    main()

"""Add DRAM-v 1.5's own auxiliary scores and AMG flags to rendered annotations.

The step that ends `DRAM-v.py annotate`, `add_dramv_scores_and_flags`, called on
dramv_annotations.py's output instead of DRAM-v's own searches. Runs where mag_annotator
imports: env::dram.env.
"""
import argparse
import logging

import pandas as pd
from mag_annotator import annotate_vgfs as av

# A representative id is no VirSorter name, so neither of DRAM-v's name parsers fits.
# dramv_annotations.py names each affi entry the way the VirSorter2 parser maps a scaffold.
av.get_virsorter_affi_contigs_name = av.get_virsorter2_affi_contigs_name


class Sheets:
    """The one thing add_dramv_scores_and_flags reads off DRAM's DatabaseHandler."""

    def __init__(self, sheets):
        self.config = {"dram_sheets": {
            s: sheets + "/" + s + ".tsv" for s in ("amg_database", "genome_summary_form")}}


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--annotations", required=True)
    p.add_argument("--affi", required=True)
    p.add_argument("--fasta", required=True)
    p.add_argument("--sheets", required=True, help="ref::dram_sheets")
    p.add_argument("--out", required=True)
    a = p.parse_args()

    logger = logging.getLogger("dramv")
    annotations = pd.read_csv(a.annotations, sep="\t", index_col=0)
    hits = av.get_virsorter_hits(a.affi)
    annotations = av.add_dramv_scores_and_flags(annotations, Sheets(a.sheets), logger, hits, a.fasta)
    annotations.to_csv(a.out, sep="\t")
    amg = annotations["amg_flags"].str.contains("M") & (annotations["auxiliary_score"] <= 3)
    print("[dramv] " + str(len(annotations)) + " genes; auxiliary score "
          + repr(annotations["auxiliary_score"].value_counts().sort_index().to_dict())
          + "; " + str(int(amg.sum())) + " metabolic at score <= 3", flush=True)


if __name__ == "__main__":
    main()

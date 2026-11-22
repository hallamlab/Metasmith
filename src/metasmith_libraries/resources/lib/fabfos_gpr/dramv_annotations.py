"""Render a study's vOTU representatives as the three inputs of DRAM-v 1.5's scoring.

`DRAM-v.py annotate` searches its own gene calls and then scores them against VirSorter2's
affi-contigs table. Here the genes are the frozen set's own ORFs on each representative,
annotated from the annotation table built on them, and the gene categories are the
VirSorter2 genes inside the representative's interval, from the per-sample runs, moved into
its frame. Writes the annotations (dram_annotations' columns plus DRAM-v's viral ones), the
affi table and the representatives' FASTA; dramv_flags.py adds DRAM-v's scores and flags.
"""
import argparse
import os
import re
import sys
from collections import defaultdict

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from dram_annotations import DRAMV_COLS, fasta_records, gene_data, render  # noqa: E402

# VirSorter2 names an affi entry `<contig>||<suffix>` with `|` made `_`.
_VS2_SUFFIX = re.compile(r"__(full|\d+_partial|lt2gene)$")
AFFI_FIELDS = 12


def affi_name(rep):
    """The affi entry name DRAM-v's VirSorter2 parser maps a scaffold to."""
    return rep.replace("|", "_")


def representatives(votu, provenance):
    reps = pd.read_csv(votu, sep="\t", header=None, usecols=[0], dtype=str)[0].unique()
    prov = pd.read_csv(provenance, sep="\t", dtype={"source_contig": str})
    prov = prov[prov["frozen_id"].isin(reps)]
    missing = set(reps) - set(prov["frozen_id"])
    if missing:
        raise SystemExit("[dramv] " + str(len(missing)) + " vOTU representatives have no "
                         "provenance row, e.g. " + sorted(missing)[0] + "; the cluster table "
                         "and the provenance table come from different frozen sets")
    return prov


def read_affi(paths):
    """VirSorter2 genes per source contig, as (start, end, row fields); start and end are
    on the contig VirSorter2 read, since its gene calls are made before any trim."""
    genes = defaultdict(dict)
    for path in paths:
        contig = None
        with open(path) as fh:
            for line in fh:
                line = line.rstrip("\n")
                if not line:
                    continue
                if line.startswith(">"):
                    contig = _VS2_SUFFIX.sub("", line[1:].split("|")[0])
                    continue
                f = line.split("|")
                if len(f) != AFFI_FIELDS:
                    raise SystemExit("[dramv] " + path + " has a " + str(len(f)) + "-field gene "
                                     "row; VirSorter2's affi-contigs table has " + str(AFFI_FIELDS))
                genes[contig][(int(f[1]), int(f[2]))] = f
    return genes


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--ev-lib", required=True)
    p.add_argument("--frozen", required=True, help="the frozen candidate viral contigs")
    p.add_argument("--provenance", required=True)
    p.add_argument("--votu", required=True, help="the vOTU cluster table, representative first")
    p.add_argument("--orfs", required=True, help="the frozen set's ORF FASTA")
    p.add_argument("--annotation-table", nargs="+", required=True, help="lane tables built on --orfs")
    p.add_argument("--affi", nargs="*", default=[], help="VirSorter2 affi-contigs tables")
    p.add_argument("--name", required=True, help="DRAM-v's `fasta` column")
    p.add_argument("--out-annotations", required=True)
    p.add_argument("--out-affi", required=True)
    p.add_argument("--out-fasta", required=True)
    a = p.parse_args()

    sys.path.insert(0, os.path.dirname(a.ev_lib))
    import fabfos_evidence as ev

    prov = representatives(a.votu, a.provenance)
    genes = gene_data(a.orfs, set(prov["frozen_id"]))
    if genes.empty:
        raise SystemExit("[dramv] no ORF of " + a.orfs + " lies on any of " + str(len(prov))
                         + " vOTU representatives; the ORFs are not of this frozen set")
    table = ev.read_lane_tables(a.annotation_table, filters=[("orf", "in", list(genes.index))])
    out = render(table, genes, a.name, DRAMV_COLS)
    out.to_csv(a.out_annotations, sep="\t")

    vs2 = read_affi(a.affi)
    n_scored = 0
    with open(a.out_affi, "w") as fh:
        for r in prov.itertuples():
            inside = sorted((s - r.start + 1, e - r.start + 1, f)
                            for (s, e), f in vs2.get(r.source_contig, {}).items()
                            if s >= r.start and e <= r.end)
            if not inside:
                continue
            n_scored += 1
            name = affi_name(r.frozen_id)
            fh.write(">" + name + "|" + str(len(inside)) + "|l\n")
            for i, (s, e, f) in enumerate(inside, 1):
                fh.write("|".join([name + "__" + str(i), str(s), str(e), *f[3:]]) + "\n")

    keep = set(prov["frozen_id"])
    with open(a.out_fasta, "w") as fh:
        for rid, _, seq in fasta_records(a.frozen):
            if rid in keep:
                fh.write(">" + rid + "\n" + seq + "\n")

    print("[dramv] " + str(len(prov)) + " vOTU representatives: "
          + str(out["scaffold"].nunique()) + " with genes (" + str(len(out)) + " genes), "
          + str(n_scored) + " with VirSorter2 genes to score against", flush=True)


if __name__ == "__main__":
    main()

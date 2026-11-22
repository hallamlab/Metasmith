"""Render one MAG's rows of an annotation table as DRAM 1.5's annotations.tsv.

The file `DRAM.py distill` reads, built from our own lanes in place of `DRAM.py annotate`:
kofam for KEGG, UniRef50 for DRAM's UniRef90, InterProScan's Pfam calls for DRAM's
mmseqs Pfam search, and the dbcan, merops and vogdb lanes for DRAM's own three. Each
lane is cut the way DRAM cuts its own search, since the annotation table keeps every call.
ORF ids stay as the ORF FASTA names them; DRAM would prefix each with the genome name.
"""
import argparse
import os
import re
import sys

import pandas as pd

# The columns DRAM 1.5 writes for these databases, in its order. No `*_RBH`, `kegg_RBH`
# or `uniref_RBH`: nothing here searches back, so the rank is C, D or E, never A or B.
DRAM_COLS = [
    "fasta", "scaffold", "gene_position", "start_position", "end_position", "strandedness",
    "rank", "ko_id", "kegg_hit", "uniref_id", "uniref_hit", "uniref_eVal", "pfam_hits",
    "cazy_ids", "cazy_hits", "cazy_best_hit", "peptidase_id", "peptidase_family",
    "peptidase_hit", "peptidase_bitScore", "peptidase_eVal", "vogdb_id", "vogdb_hits",
    "vogdb_categories", "heme_regulatory_motif_count",
]
# DRAM-v adds its RefSeq viral search to the same set.
VIRAL_COLS = ["viral_id", "viral_hit", "viral_bitScore", "viral_eVal"]
DRAMV_COLS = DRAM_COLS[:12] + VIRAL_COLS + DRAM_COLS[12:]

# DRAM's own cut-offs (mag_annotator 1.5.0): an HMM hit counts at model coverage >= 0.35
# and a full-sequence E-value at or under 1e-18 for dbCAN, 1e-15 otherwise; a blast-style
# hit is the top hit at bitscore >= 60.
HMM_MIN_COVERAGE = 0.35
DBCAN_MAX_EVALUE = 1e-18
VOGDB_MAX_EVALUE = 1e-15
MIN_BITSCORE = 60.0

_PRODIGAL = re.compile(r"#\s*(\d+)\s*#\s*(\d+)\s*#\s*(-?1)\s*#")
_PEPTIDASE_FAMILY = re.compile(r"#\w*.#")


def fasta_records(path):
    rid, header, seq = None, "", []
    with open(path) as fh:
        for line in fh:
            if line.startswith(">"):
                if rid is not None:
                    yield rid, header, "".join(seq)
                header = line[1:].rstrip("\n")
                rid, seq = header.split()[0], []
            else:
                seq.append(line.strip())
    if rid is not None:
        yield rid, header, "".join(seq)


# A Prodigal id is `<contig>_<n>`, its header `# start # end # strand # ...`; DRAM's
# get_gene_data reads both the same way.
def gene_data(orfs, contigs):
    rows = []
    for oid, header, seq in fasta_records(orfs):
        scaffold, _, n = oid.rpartition("_")
        if scaffold not in contigs:
            continue
        m = _PRODIGAL.search(header)
        start, end, strand = (int(g) for g in m.groups()) if m else (None, None, None)
        rows.append((oid, scaffold, int(n) if n.isdigit() else None, start, end, strand,
                     len(re.findall("C..CH", seq))))
    df = pd.DataFrame(rows, columns=[
        "orf", "scaffold", "gene_position", "start_position", "end_position",
        "strandedness", "heme_regulatory_motif_count"]).set_index("orf")
    coords = ["gene_position", "start_position", "end_position", "strandedness"]
    df[coords] = df[coords].astype("Int64")
    return df


def _lane(table, lane, namespace=None):
    df = table[table["lane"] == lane]
    return df if namespace is None else df[df["namespace"] == namespace]


def _top(df, by, ascending):
    return df.sort_values(by, ascending=ascending, kind="stable").drop_duplicates("orf").set_index("orf")


def _joined(df, col):
    return df.groupby("orf", sort=False)[col].agg(
        lambda v: "; ".join(dict.fromkeys(x for x in v if isinstance(x, str) and x)))


def render(table, genes, name, cols=DRAM_COLS):
    out = genes.copy()
    out.insert(0, "fasta", name)

    # kofamscan keeps a KO at or over its own threshold; one with no threshold has no
    # ratio, and DRAM skips those too. Then the best E-value, as DRAM picks.
    ko = _lane(table, "kofam", "ko")
    ko = _top(ko[ko["score"] >= 1.0], "evalue", True)
    out["ko_id"], out["kegg_hit"] = ko["term"], ko["description"]

    # The lane's score is the BLAST score ratio, so the top hit is the highest ratio and
    # no bitscore cut applies.
    ur = _top(_lane(table, "uniref50"), ["score", "evalue"], [False, True])
    out["uniref_id"], out["uniref_hit"], out["uniref_eVal"] = ur["term"], ur["description"], ur["evalue"]

    # InterProScan does not report the Pfam version. DRAM's readers of `pfam_hits` match
    # `\[PF\d{5}.\d*\]` and drop what follows the dot, so `[PF00001.]` reads as PF00001
    # to both distill and DRAM-v's transposon test.
    pf = _lane(table, "interproscan", "pfam").sort_values(["orf", "start"], kind="stable")
    out["pfam_hits"] = _joined(pf.assign(hit=pf["description"].fillna("") + " [" + pf["term"] + ".]"), "hit")

    vr = _lane(table, "refseq_viral")
    vr = _top(vr[vr["score"] >= MIN_BITSCORE], "score", False)
    out["viral_id"], out["viral_hit"] = vr["term"], vr["description"]
    out["viral_bitScore"], out["viral_eVal"] = vr["score"], vr["evalue"]

    cz = _lane(table, "dbcan")
    cz = cz[(cz["score"] >= HMM_MIN_COVERAGE) & (cz["evalue"] <= DBCAN_MAX_EVALUE)]
    out["cazy_ids"], out["cazy_hits"] = _joined(cz, "term"), _joined(cz, "description")
    out["cazy_best_hit"] = _top(cz, "evalue", True)["term"]

    pep = _lane(table, "merops")
    pep = _top(pep[pep["score"] >= MIN_BITSCORE], "score", False)
    out["peptidase_id"], out["peptidase_hit"] = pep["term"], pep["description"]
    out["peptidase_family"] = pep["description"].map(
        lambda h: m.group()[1:-1] if isinstance(h, str) and (m := _PEPTIDASE_FAMILY.search(h)) else None)
    out["peptidase_bitScore"], out["peptidase_eVal"] = pep["score"], pep["evalue"]

    vog = _lane(table, "vogdb")
    vog = _top(vog[(vog["score"] >= HMM_MIN_COVERAGE) & (vog["evalue"] <= VOGDB_MAX_EVALUE)],
               "evalue", True)
    out["vogdb_id"], out["vogdb_hits"] = vog["term"], vog["description"]
    # The category is the last `; ` field of DRAM's description string, two letters a code.
    out["vogdb_categories"] = vog["description"].map(
        lambda d: ";".join(sorted({c[i:i + 2] for c in [d.split("; ")[-1]]
                                   for i in range(0, len(c), 2)})) if isinstance(d, str) else None)

    best = out["ko_id"].notna() | out["uniref_id"].notna()
    some = out["pfam_hits"].notna() | out["cazy_ids"].notna() | out["peptidase_id"].notna()
    out["rank"] = "E"
    out.loc[some, "rank"] = "D"
    out.loc[best, "rank"] = "C"
    out.index.name = None
    return out[cols]


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--ev-lib", required=True)
    p.add_argument("--annotation-table", required=True)
    p.add_argument("--orfs", required=True, help="the ORF FASTA the table was built on")
    p.add_argument("--mag", required=True, help="the MAG's contig FASTA")
    p.add_argument("--name", required=True, help="DRAM's `fasta` column: the genome name")
    p.add_argument("--out", required=True)
    a = p.parse_args()

    sys.path.insert(0, os.path.dirname(a.ev_lib))
    import fabfos_evidence as ev

    contigs = {rid for rid, _, _ in fasta_records(a.mag)}
    genes = gene_data(a.orfs, contigs)
    if genes.empty:
        raise SystemExit(
            "[dram] no ORF of " + a.orfs + " lies on a contig of " + a.mag + " ("
            + str(len(contigs)) + " contigs). The MAG and the ORF set come from different "
            "assemblies, or the ORF ids are not `<contig>_<n>`")
    table = ev.read_parquet(a.annotation_table)
    table = table[table["orf"].isin(genes.index)]
    out = render(table, genes, a.name)
    out.to_csv(a.out, sep="\t")
    print("[dram] " + a.name + ": " + str(len(out)) + " genes on " + str(len(contigs))
          + " contigs; rank " + out["rank"].value_counts().sort_index().to_dict().__repr__(),
          flush=True)


if __name__ == "__main__":
    main()

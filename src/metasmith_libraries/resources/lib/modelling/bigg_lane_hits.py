"""One MAG's rows of the bigg lane table, written back as the DIAMOND outfmt 6 `carve --diamond` reads.

CarveMe keeps query, subject and bitscore from that file (reconstruction/diamond.py,
load_diamond_results), so the columns the lane table does not carry are written as 0.

    bigg_lane_hits.py --ev-lib PATH --lane-table PATH --mag MAG.fna --out hits.tsv
"""
import argparse
import os
import sys


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--ev-lib", required=True)
    p.add_argument("--lane-table", required=True)
    p.add_argument("--mag", required=True)
    p.add_argument("--out", required=True)
    a = p.parse_args()
    sys.path.insert(0, os.path.dirname(a.ev_lib))
    import fabfos_evidence as ev

    with open(a.mag) as fh:
        contigs = {line[1:].split()[0] for line in fh if line.startswith(">")}
    t = ev.read_parquet(a.lane_table, columns=["orf", "term", "start", "end", "evalue", "score",
                                               "bigg_pident", "bigg_length"])
    # Prodigal names each protein <contig>_<n>.
    t = t[t["orf"].str.rpartition("_")[0].isin(contigs)]
    if t.empty:
        raise SystemExit("[carveme] no bigg hit lies on a contig of " + a.mag + " ("
                         + str(len(contigs)) + " contigs); the lane and the MAG come from "
                         "different assemblies, or the ORF ids are not `<contig>_<n>`")
    out = t.assign(mismatch=0, gapopen=0, sstart=0, send=0)[
        ["orf", "term", "bigg_pident", "bigg_length", "mismatch", "gapopen", "start", "end",
         "sstart", "send", "evalue", "score"]]
    for c in ("bigg_length", "start", "end"):
        out[c] = out[c].astype(int)
    out.to_csv(a.out, sep="\t", header=False, index=False)
    print("[carveme] " + str(len(out)) + " bigg hits on " + str(out["orf"].nunique())
          + " ORFs of " + a.mag, flush=True)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Score the R3 OPERA-MS polishing test from skani's tables, one row per sample and lane, plus pooled rows.

    python3 pratama_polish_score.py <skani dir> <pub dir> <out tsv>

<skani dir> holds <sample>.<lane>.tsv from `skani dist --qi -q <published vOTUs> --ri -r <assembly>
--small-genomes --min-af 15`, and <pub dir> the published hybrid vOTUs per sample as <sample>.fna.
Each vOTU's best contig is the pair with the highest ANI x query aligned fraction. "Recovered" is
ANI >= 95 over >= 85% of the vOTU. The query side is the share of the vOTU the contig covers; the
reference side is the share of that contig the vOTU covers, the direction E3's E5-to-published check read.
"""

import csv
import statistics
import sys
from collections import defaultdict
from pathlib import Path


def vOTUs(fna):
    return [line[1:].split()[0] for line in open(fna) if line.startswith(">")]


def best_hits(table):
    best = {}
    with open(table) as fh:
        for r in csv.DictReader(fh, delimiter="\t"):
            q = r["Query_name"].split()[0]
            ani, afq, afr = float(r["ANI"]), float(r["Align_fraction_query"]), float(r["Align_fraction_ref"])
            if q not in best or ani * afq > best[q][0] * best[q][1]:
                best[q] = (ani, afq, afr)
    return best


def row(sample, lane, names, best):
    hits = [best[n] for n in names if n in best]
    return {
        "sample": sample, "lane": lane, "votus": len(names), "with_pair": len(hits),
        "no_pair_share": round(1 - len(hits) / len(names), 4),
        "ani95_share": round(sum(h[0] >= 95 for h in hits) / len(names), 4),
        "recovered_share": round(sum(h[0] >= 95 and h[1] >= 85 for h in hits) / len(names), 4),
        "median_af_votu": round(statistics.median(h[1] for h in hits), 2) if hits else "",
        "median_af_contig": round(statistics.median(h[2] for h in hits), 2) if hits else "",
        "median_ani": round(statistics.median(h[0] for h in hits), 2) if hits else "",
    }


def main(skani_dir, pub_dir, out):
    rows, pooled = [], defaultdict(lambda: ([], {}))
    for fna in sorted(Path(pub_dir).glob("*.fna")):
        sample = fna.stem
        names = vOTUs(fna)
        for lane in ("unpolished", "polished"):
            table = Path(skani_dir) / f"{sample}.{lane}.tsv"
            if not table.exists():
                print(f"missing {table}", file=sys.stderr)
                continue
            best = best_hits(table)
            rows.append(row(sample, lane, names, best))
            pooled[lane][0].extend(names)
            pooled[lane][1].update(best)
    for lane, (names, best) in pooled.items():
        rows.append(row("all", lane, names, best))
    with open(out, "w", newline="") as fh:
        w = csv.DictWriter(fh, list(rows[0]), delimiter="\t", lineterminator="\n")
        w.writeheader()
        w.writerows(rows)


if __name__ == "__main__":
    main(*sys.argv[1:])

import argparse
import csv
import gzip
import sys
from collections import Counter, defaultdict
from pathlib import Path

from lcastar import parse_gtdb, to_text

csv.field_size_limit(sys.maxsize)
ROOT = 1


def read_parents(nodes_dmp):
    parent = {}
    with open(nodes_dmp) as fh:
        for line in fh:
            f = line.split("\t|\t", 2)
            parent[int(f[0])] = int(f[1])
    return parent


def read_merged(merged_dmp):
    merged = {}
    if merged_dmp and Path(merged_dmp).exists():
        with open(merged_dmp) as fh:
            for line in fh:
                f = line.split("\t|", 2)
                merged[int(f[0])] = int(f[1].strip())
    return merged


def ancestors(taxid, parent):
    seen = []
    while taxid in parent and taxid not in seen:
        seen.append(taxid)
        if taxid == ROOT:
            break
        taxid = parent[taxid]
    return seen


def read_genomes(metadata_paths, merged):
    pairs = Counter()
    for path in metadata_paths:
        with gzip.open(path, "rt") as fh:
            for row in csv.DictReader(fh, delimiter="\t"):
                raw = row.get("ncbi_taxid", "")
                if not raw.isdigit():
                    continue
                lineage = parse_gtdb(row["gtdb_taxonomy"])
                if lineage:
                    taxid = int(raw)
                    pairs[(merged.get(taxid, taxid), lineage)] += 1
    return pairs


def deepest_prefix(lineage_counts, threshold):
    total = sum(lineage_counts.values())
    best, best_frac = (), 1.0
    for depth in range(1, 8):
        prefix_counts = Counter()
        for lineage, n in lineage_counts.items():
            if len(lineage) >= depth:
                prefix_counts[lineage[:depth]] += n
        if not prefix_counts:
            break
        prefix, n = prefix_counts.most_common(1)[0]
        frac = n / total
        if not (frac > threshold or frac == 1.0):
            break
        best, best_frac = prefix, frac
    return best, best_frac


def build(parent, merged, pairs, threshold):
    under = defaultdict(Counter)
    for (taxid, lineage), n in pairs.items():
        for anc in ancestors(taxid, parent):
            under[anc][lineage] += n

    voted = {t: deepest_prefix(c, threshold) for t, c in under.items()}
    unanimous = {t: deepest_prefix(c, 1.0)[0] for t, c in under.items()}

    rows = {}
    for taxid, (lineage, frac) in voted.items():
        if lineage:
            rows[taxid] = (lineage, frac, "genomes")
    # A taxon with no GTDB genome under it inherits only what its nearest
    # ancestor's genomes agree on unanimously: a eukaryote must not inherit
    # the bacterial majority of "cellular organisms".
    for taxid in parent:
        if taxid in under:
            continue
        anc, hops = parent[taxid], 0
        while anc not in under and anc != ROOT and hops < 100:
            anc, hops = parent.get(anc, ROOT), hops + 1
        if anc in under and unanimous[anc]:
            rows[taxid] = (unanimous[anc], 1.0, f"ancestor:{anc}")
    for old, new in merged.items():
        if new in rows and old not in rows:
            rows[old] = rows[new]
    return rows


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--taxdump", required=True)
    p.add_argument("--metadata", required=True, nargs="+")
    p.add_argument("--out", required=True)
    p.add_argument("--threshold", type=float, default=0.5)
    a = p.parse_args()

    taxdump = Path(a.taxdump)
    parent = read_parents(taxdump / "nodes.dmp")
    merged = read_merged(taxdump / "merged.dmp")
    pairs = read_genomes(a.metadata, merged)
    print(f"[ncbi_to_gtdb] {sum(pairs.values()):,} genomes over {len(pairs):,} taxid-lineage pairs", flush=True)
    rows = build(parent, merged, pairs, a.threshold)

    with gzip.open(a.out, "wt") as fh:
        fh.write("taxid\tlineage\tsupport\tvia\n")
        for taxid in sorted(rows):
            lineage, frac, via = rows[taxid]
            fh.write(f"{taxid}\t{to_text(lineage)}\t{frac:.4f}\t{via}\n")
    print(f"[ncbi_to_gtdb] {len(rows):,} NCBI taxids mapped", flush=True)


if __name__ == "__main__":
    main()

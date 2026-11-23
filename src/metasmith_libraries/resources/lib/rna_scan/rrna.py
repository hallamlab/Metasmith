import argparse
import re

from gff import Feature, read_fasta, revcomp, write_gff
# lib::lcastar is staged as its own directory and reaches this through PYTHONPATH.
from lcastar import gtdb_rank, lca, parse_gtdb, to_text

_LINEAGE = re.compile(r"(d__[^\[]*?)\s*(?:\[|$)")

# Minimum SSU identity per rank (Yarza et al. 2014, Nat Rev Microbiol 12:635).
# A best hit below the threshold of a rank cannot support that rank.
_YARZA = {"species": 98.7, "genus": 94.5, "family": 86.5, "order": 82.0,
          "class": 78.5, "phylum": 75.0, "domain": 0.0}


def read_barrnap(paths):
    feats = []
    for path in paths:
        with open(path) as fh:
            for line in fh:
                if line.startswith("#") or not line.strip():
                    continue
                f = line.rstrip("\n").split("\t")
                attrs = dict(kv.split("=", 1) for kv in f[8].split(";") if "=" in kv)
                product = attrs.get("product") or attrs.get("Name", "").replace("_", " ")
                evalue = float(f[5]) if f[5] not in (".", "") else 1.0
                feats.append(Feature(f[0], int(f[3]), int(f[4]), f[6], "rRNA", product, evalue, f[1]))
    return feats


def best_of_overlaps(feats):
    kept = []
    for feat in sorted(feats, key=lambda x: (x.score, x.contig, x.start)):
        if not any(k.contig == feat.contig and k.strand == feat.strand and k.overlaps(feat) for k in kept):
            kept.append(feat)
    return sorted(kept, key=lambda x: (x.contig, x.start))


def cmd_merge(a):
    feats = best_of_overlaps(read_barrnap(a.gff))
    for i, feat in enumerate(feats, 1):
        feat.id = f"rrna_{i}"
    write_gff(feats, a.out_gff)
    seqs = read_fasta(a.assembly)
    n = 0
    with open(a.out_ssu, "w") as fh:
        for feat in feats:
            if "16S" in feat.product or "18S" in feat.product:
                s = seqs[feat.contig][feat.start - 1:feat.end]
                fh.write(f">{feat.id}\n{revcomp(s) if feat.strand == '-' else s}\n")
                n += 1
    print(f"[rrna] {len(feats)} rRNA genes, {n} SSU", flush=True)


def cmd_classify(a):
    feats = {}
    with open(a.gff) as fh:
        for line in fh:
            if line.startswith("#") or not line.strip():
                continue
            f = line.rstrip("\n").split("\t")
            attrs = dict(kv.split("=", 1) for kv in f[8].split(";") if "=" in kv)
            feats[attrs["ID"]] = (f[0], attrs.get("product", ""))

    hits = {}
    with open(a.blast) as fh:
        for line in fh:
            q, _, pident, length, evalue, bits, title = line.rstrip("\n").split("\t", 6)
            pident, bits, evalue = float(pident), float(bits), float(evalue)
            if evalue > a.evalue or pident < a.min_identity or bits < a.min_bitscore:
                continue
            m = _LINEAGE.search(title)
            if m:
                hits.setdefault(q, []).append((bits, pident, parse_gtdb(m.group(1))))

    with open(a.out, "w") as fh:
        fh.write("gene\tcontig\tproduct\tlineage\trank\tbest_identity\tbest_bitscore\tn_hits\n")
        for gene, gene_hits in sorted(hits.items()):
            best_bits = max(h[0] for h in gene_hits)
            top = [h for h in gene_hits if h[0] >= best_bits * (1 - a.top_percent / 100)]
            best_pident = max(h[1] for h in top)
            lineage = lca(h[2] for h in top)
            while lineage and best_pident < _YARZA[gtdb_rank(lineage)]:
                lineage = lineage[:-1]
            contig, product = feats.get(gene, ("", ""))
            fh.write(f"{gene}\t{contig}\t{product}\t{to_text(lineage)}\t{gtdb_rank(lineage)}"
                     f"\t{best_pident:.2f}\t{best_bits:.1f}\t{len(top)}\n")
    print(f"[rrna] {len(hits)} SSU genes classified", flush=True)


def main():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)
    m = sub.add_parser("merge")
    m.add_argument("--gff", nargs="+", required=True)
    m.add_argument("--assembly", required=True)
    m.add_argument("--out-gff", required=True)
    m.add_argument("--out-ssu", required=True)
    c = sub.add_parser("classify")
    c.add_argument("--gff", required=True)
    c.add_argument("--blast", required=True)
    c.add_argument("--out", required=True)
    c.add_argument("--evalue", type=float, default=1e-6)
    c.add_argument("--min-identity", type=float, default=20.0)
    c.add_argument("--min-bitscore", type=float, default=50.0)
    c.add_argument("--top-percent", type=float, default=1.0)
    a = p.parse_args()
    {"merge": cmd_merge, "classify": cmd_classify}[a.cmd](a)


if __name__ == "__main__":
    main()

"""DIAMOND hits in the shape diamond_uniref50 writes: a headered BLAST6 table with a BSR column, and
a table of subject titles keyed on subject id.

    diamond_bsr.py split --raw RAW --query FAA --hits OUT --descriptions OUT
    diamond_bsr.py merge --hits OUT --descriptions OUT --hit-chunks H [H ...] --description-chunks D [D ...]

`split` reads diamond's outfmt 6 with `stitle` as a 13th column. `merge` concatenates the chunks
of one search, describing each subject once.
"""
import argparse
import math

# BSR is bitscore over the query's self-alignment bitscore, estimated as the BLOSUM62 diagonal sum
# under DIAMOND's gapped lambda and K, as diamond_uniref50 does.
BLOSUM62_DIAG = {
    "A": 4, "R": 5, "N": 6, "D": 6, "C": 9, "Q": 5, "E": 5, "G": 6, "H": 8, "I": 4,
    "L": 4, "K": 5, "M": 5, "F": 6, "P": 7, "S": 4, "T": 5, "W": 11, "Y": 7, "V": 4,
    "B": 4, "Z": 4, "J": 3,
}
LAMBDA, K = 0.267, 0.041
HITS_HEADER = "\t".join([
    "qseqid", "sseqid", "pident", "length", "mismatch", "gapopen",
    "qstart", "qend", "sstart", "send", "evalue", "bitscore", "bsr",
]) + "\n"
DESCRIPTIONS_HEADER = "sseqid\tdescription\n"
STITLE_AT = 12


def self_bitscores(fasta):
    raw, sid = {}, None
    with open(fasta) as fh:
        for line in fh:
            if line.startswith(">"):
                sid = line[1:].split()[0]
                raw[sid] = 0
            elif sid is not None:
                raw[sid] += sum(BLOSUM62_DIAG.get(c, 0) for c in line.strip().upper())
    return {s: max(0.0, (LAMBDA * r - math.log(K)) / math.log(2)) for s, r in raw.items()}


def split(a):
    self_bs = self_bitscores(a.query)
    titles = {}
    n = 0
    with open(a.raw) as fin, open(a.hits, "w") as fout:
        fout.write(HITS_HEADER)
        for line in fin:
            if not line.strip():
                continue
            f = line.rstrip("\n").split("\t", STITLE_AT)
            sb = self_bs.get(f[0], 0.0)
            bsr = float(f[11]) / sb if sb > 0 else 0.0
            if len(f) > STITLE_AT:
                # stitle repeats the subject id as its first word.
                title = f[STITLE_AT].split(None, 1)
                titles.setdefault(f[1], " ".join(title[1].split()) if len(title) > 1 else "")
            fout.write("\t".join(f[:STITLE_AT]) + f"\t{bsr:.4f}\n")
            n += 1
    with open(a.descriptions, "w") as fout:
        fout.write(DESCRIPTIONS_HEADER)
        fout.writelines(f"{s}\t{titles[s]}\n" for s in sorted(titles))
    print(f"[diamond_bsr] {n:,} hits over {len(titles):,} subjects", flush=True)


def merge(a):
    seen = set()
    with open(a.hits, "w") as hits, open(a.descriptions, "w") as descs:
        hits.write(HITS_HEADER)
        descs.write(DESCRIPTIONS_HEADER)
        for h in a.hit_chunks:
            with open(h) as fh:
                hits.writelines(line for line in fh if line != HITS_HEADER)
        for d in a.description_chunks:
            with open(d) as fh:
                for line in fh:
                    key = line.split("\t", 1)[0]
                    if line != DESCRIPTIONS_HEADER and key not in seen:
                        seen.add(key)
                        descs.write(line)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("split")
    s.add_argument("--raw", required=True)
    s.add_argument("--query", required=True)
    s.add_argument("--hits", required=True)
    s.add_argument("--descriptions", required=True)
    s.set_defaults(fn=split)
    m = sub.add_parser("merge")
    m.add_argument("--hits", required=True)
    m.add_argument("--descriptions", required=True)
    m.add_argument("--hit-chunks", nargs="+", required=True)
    m.add_argument("--description-chunks", nargs="+", required=True)
    m.set_defaults(fn=merge)
    a = p.parse_args()
    a.fn(a)


if __name__ == "__main__":
    main()

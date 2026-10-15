"""Check a study fetched through the AWS .sra route against what ENA publishes about it.

The manifest's md5 column is NOT the gate here and this script does not pretend otherwise.
That md5 is over the fastq ENA generates; these files came out of NCBI's run-level .sra, and
the two differ in the read header alone (ENA `.../1`, fasterq-dump ` length=82`) -- so the
sequence and quality streams are byte-identical while the gzip is not, and the products run
about 6% larger than the manifest's bytes.

What is checkable is what ENA publishes about the run rather than about the file: read_count
and base_count. Both are exact here, so a run whose mates sum to them carries exactly the
reads ENA serves. Presence and non-emptiness are checked per manifest row; the counts are
checked per run, over the mates together, because base_count spans them.

Writes one row per problem; an empty report is the pass condition.
"""

import argparse
import csv
import gzip
import sys
from pathlib import Path


def count(path: Path) -> tuple[int, int]:
    reads = bases = 0
    with gzip.open(path, "rb") as f:
        for i, line in enumerate(f):
            if i % 4 == 1:
                reads += 1
                bases += len(line) - 1
    return reads, bases


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--table", type=Path, required=True)
    ap.add_argument("--root", type=Path, required=True)
    ap.add_argument("--dataset", required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--shard", type=int, default=0)
    ap.add_argument("--shards", type=int, default=1)
    ap.add_argument("--size-only", action="store_true",
                    help="presence and non-emptiness only; skips the count, which decompresses")
    args = ap.parse_args()

    with args.table.open() as f:
        rows = list(csv.DictReader(f, delimiter="\t"))
    rows = [r for i, r in enumerate(rows) if i % args.shards == args.shard]

    args.out.parent.mkdir(parents=True, exist_ok=True)
    problems = ok = 0
    with args.out.open("w") as out:
        out.write("problem\tdataset\trun\texpected\tactual\n")
        for r in rows:
            rel = r["relpaths"].split(";")
            paths = [args.root/args.dataset/p for p in rel]
            bad = False
            for p, name in zip(paths, rel):
                if not p.is_file():
                    out.write(f"missing\t{args.dataset}\t{name}\t-\t-\n"); bad = True
                elif p.stat().st_size == 0:
                    out.write(f"empty\t{args.dataset}\t{name}\t-\t0\n"); bad = True
            if bad:
                problems += 1
                continue
            if args.size_only:
                ok += 1
                continue
            reads = bases = 0
            per = []
            for p in paths:
                n, b = count(p)
                per.append(n)
                bases += b
            # Mates carry the same read count, so spots is any one of them; base_count spans
            # the pair. A pair whose mates disagree is a truncation the sum would hide.
            if len(set(per)) != 1:
                out.write(f"mate-skew\t{args.dataset}\t{r['run']}\tequal\t{per}\n")
                problems += 1
                continue
            reads = per[0]
            if r["read_count"] and str(reads) != r["read_count"]:
                out.write(f"read_count\t{args.dataset}\t{r['run']}\t{r['read_count']}\t{reads}\n")
                problems += 1
                continue
            if r["base_count"] and str(bases) != r["base_count"]:
                out.write(f"base_count\t{args.dataset}\t{r['run']}\t{r['base_count']}\t{bases}\n")
                problems += 1
                continue
            ok += 1
        out.flush()

    sys.stderr.write(f"{args.dataset} shard {args.shard}/{args.shards}: {ok} runs verified, "
                     f"{problems} problems -> {args.out}\n")


if __name__ == "__main__":
    main()

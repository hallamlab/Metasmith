"""Check a corpus fetched from ENA against the manifest: size always, MD5 where published.

Pairs with fetch_array.sbatch, which is the ENA path -- so it is the FALLBACK verifier here.
It is the right one only for files pulled from ENA's own vol1/fastq, whose md5 the manifest
carries. Anything that came through the AWS .sra mirror will fail every row: that md5 is over
a file ENA generates and the .sra route reproduces its reads but not its bytes. Use
verify_sra.py for those.

Shardable, because a terabyte of hashing belongs in the queue rather than on a login node.
Writes one TSV row per problem; an empty report is the pass condition. An empty md5 column is
a fact about one object rather than about its source, and is reported.
"""

import argparse
import csv
import hashlib
import sys
from pathlib import Path


def md5sum(path: Path, chunk: int = 8 << 20) -> str:
    h = hashlib.md5()
    with path.open("rb") as f:
        while block := f.read(chunk):
            h.update(block)
    return h.hexdigest()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--manifest", type=Path, required=True)
    ap.add_argument("--root", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--shard", type=int, default=0)
    ap.add_argument("--shards", type=int, default=1)
    ap.add_argument("--size-only", action="store_true", help="skip hashing entirely")
    ap.add_argument("--dataset", nargs="*", default=None, help="limit to these datasets")
    args = ap.parse_args()

    with args.manifest.open() as f:
        rows = list(csv.DictReader(f, delimiter="\t"))
    if args.dataset:
        rows = [r for r in rows if r["dataset"] in set(args.dataset)]
    rows = [r for i, r in enumerate(rows) if i % args.shards == args.shard]

    args.out.parent.mkdir(parents=True, exist_ok=True)
    problems = checked = hashed = 0
    with args.out.open("w") as out:
        out.write("problem\tdataset\trelpath\texpected\tactual\n")
        for r in rows:
            path = args.root / r["dataset"] / r["relpath"]
            want = int(r["bytes"])
            if not path.exists():
                out.write(f"missing\t{r['dataset']}\t{r['relpath']}\t{want}\t-\n")
                problems += 1
                continue
            got = path.stat().st_size
            if want > 0 and got != want:
                out.write(f"size\t{r['dataset']}\t{r['relpath']}\t{want}\t{got}\n")
                problems += 1
                continue
            checked += 1
            if args.size_only:
                continue
            if not r["md5"]:
                out.write(f"no-checksum\t{r['dataset']}\t{r['relpath']}\t-\t-\n")
                problems += 1
                continue
            actual = md5sum(path)
            hashed += 1
            if actual != r["md5"]:
                out.write(f"md5\t{r['dataset']}\t{r['relpath']}\t{r['md5']}\t{actual}\n")
                problems += 1
        out.flush()

    sys.stderr.write(f"shard {args.shard}/{args.shards}: {checked} present and sized, "
                     f"{hashed} hashed, {problems} problems -> {args.out}\n")


if __name__ == "__main__":
    main()

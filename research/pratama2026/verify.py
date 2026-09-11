"""Check a fetched Pratama 2026 corpus against the manifest: size always, MD5 where published.

Shardable, because 765 GiB of hashing belongs in the queue rather than on a login node.
Writes one TSV row per problem; an empty report is the pass condition.

Unlike the CAMI corpus this has no unverifiable objects: ENA publishes a plain MD5 per fastq and
Zenodo an algorithm-qualified one that build_manifest.py keeps only when it is an MD5, so there is
no multipart-ETag case to exempt. An empty md5 column here is a fact about one object rather than
about its source, and is reported.
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

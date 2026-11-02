"""Check a fetched CAMI corpus against the manifest: size always, MD5 where published.

Shardable, because the full corpus is terabytes of reading and belongs in the queue.
Writes one TSV row per problem; an empty report is the pass condition.
"""

import argparse
import csv
import hashlib
import sys
from pathlib import Path

# The mouse gut set and CAMI I publish no checksums, so size is all they can be held to.
NO_CHECKSUM_DATASETS = {"cami2_toy_mousegut", "cami1"}


def is_multipart_etag(checksum: str) -> bool:
    """A Swift/S3 hash like '7c4ff473...-1' is an ETag over concatenated part hashes.

    It is not the file's MD5 and cannot be reproduced without the uploader's part
    size, so comparing it to one reports every object as corrupt. 167 of CAMI III's
    173 checksums are this shape.
    """
    return "-" in checksum


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
    args = ap.parse_args()

    with args.manifest.open() as f:
        rows = [r for i, r in enumerate(csv.DictReader(f, delimiter="\t"))
                if i % args.shards == args.shard]

    args.out.parent.mkdir(parents=True, exist_ok=True)
    problems = checked = unverifiable = 0
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
            if args.size_only or not r["md5"] or r["dataset"] in NO_CHECKSUM_DATASETS:
                continue
            if is_multipart_etag(r["md5"]):
                unverifiable += 1
                continue
            actual = md5sum(path)
            if actual != r["md5"]:
                out.write(f"md5\t{r['dataset']}\t{r['relpath']}\t{r['md5']}\t{actual}\n")
                problems += 1
        out.flush()

    sys.stderr.write(f"shard {args.shard}/{args.shards}: {checked} ok, {problems} problems,"
                     f" {unverifiable} size-only (multipart etag) -> {args.out}\n")


if __name__ == "__main__":
    main()

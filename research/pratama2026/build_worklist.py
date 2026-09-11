"""List the manifest rows that are not yet on disk at their published byte count.

One row per array task for fetch_array.sbatch, in the manifest's own column order minus
the drop flag. An empty worklist is the finished condition.
"""

import argparse
import csv
from pathlib import Path


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--manifest", type=Path, required=True)
    ap.add_argument("--root", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--dataset", nargs="*", default=None)
    args = ap.parse_args()

    with args.manifest.open() as f:
        rows = list(csv.DictReader(f, delimiter="\t"))
    if args.dataset:
        rows = [r for r in rows if r["dataset"] in set(args.dataset)]

    todo = []
    for r in rows:
        dest = args.root/r["dataset"]/r["relpath"]
        if dest.exists() and dest.stat().st_size == int(r["bytes"]):
            continue
        todo.append(r)

    with args.out.open("w") as f:
        for r in todo:
            f.write("\t".join([r["dataset"], r["relpath"], r["url"], r["bytes"], r["md5"]]) + "\n")
    print(f"{len(todo)} of {len(rows)} objects outstanding -> {args.out}")


if __name__ == "__main__":
    main()

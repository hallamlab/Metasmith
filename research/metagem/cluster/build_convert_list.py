"""List the runs whose staged .sra is ready to convert and whose fastq is not yet on disk.

One row per RUN, because the S3 object is run-level and both mates come out of one
fasterq-dump. A run is outstanding when its .sra is staged at the published byte count and
at least one of its manifest relpaths is missing or empty. An empty list is the finished
condition for that study.
"""

import argparse
import csv
from pathlib import Path


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--table", type=Path, required=True)
    ap.add_argument("--root", type=Path, required=True)
    ap.add_argument("--dataset", required=True)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()

    with args.table.open() as f:
        rows = list(csv.DictReader(f, delimiter="\t"))

    todo = []
    unstaged = 0
    for r in rows:
        rel = r["relpaths"].split(";")
        if all((args.root/args.dataset/p).is_file() and (args.root/args.dataset/p).stat().st_size > 0
               for p in rel):
            continue
        sra = args.root/".sra"/args.dataset/f"{r['run']}.sra"
        if not sra.is_file() or sra.stat().st_size != int(r["sra_bytes"]):
            unstaged += 1
            continue
        todo.append(r)

    with args.out.open("w") as f:
        for r in todo:
            f.write("\t".join([r["run"], r["sra_bytes"], r["sra_md5"], r["read_count"],
                               r["base_count"], r["relpaths"]]) + "\n")
    print(f"{args.dataset}: {len(todo)} of {len(rows)} runs ready to convert, "
          f"{unstaged} still awaiting their .sra -> {args.out}")


if __name__ == "__main__":
    main()

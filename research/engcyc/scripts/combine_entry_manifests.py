"""Write one manifest.csv for a project entry from the manifest.csv of each delivery in it.

Each input is named <delivery>.csv, with file paths relative to that delivery. The output prefixes
each file path with <delivery>/ and notes a file whose name, bytes and md5 recur in another delivery.
"""
import argparse
import csv
import sys
from collections import defaultdict
from pathlib import Path

from manifest_jgi import COLUMNS


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("deliveries", type=Path, nargs="+", help="per-delivery manifest tables, named <delivery>.csv")
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()
    rows = []
    for path in sorted(args.deliveries):
        with open(path) as fh:
            for row in csv.DictReader(fh):
                if row["file"] == "manifest.csv":
                    continue
                row["file"] = f"{path.stem}/{row['file']}"
                rows.append(row)
    seen = defaultdict(list)
    for row in rows:
        if row["md5"]:
            seen[(row["file"].rsplit("/", 1)[-1], row["bytes"], row["md5"])].append(row["file"])
    for row in rows:
        twins = [f for f in seen.get((row["file"].rsplit("/", 1)[-1], row["bytes"], row["md5"]), [])
                 if f.split("/", 1)[0] != row["file"].split("/", 1)[0]]
        if twins:
            row["notes"] = "; ".join(n for n in (row["notes"], "identical copy at " + ", ".join(twins)) if n)
    with open(args.out, "w", newline="") as out:
        w = csv.DictWriter(out, COLUMNS)
        w.writeheader()
        w.writerows(rows)
    print(f"{len(rows)} rows from {len(args.deliveries)} deliveries -> {args.out}", file=sys.stderr)


if __name__ == "__main__":
    main()

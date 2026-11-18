"""Land verified moves into the repo tables: append each work folder's manifest pieces to their entry manifests,
and point manifest.tsv and worklist.tsv at the delivery folders.

A work folder holds plan_final.tsv (the chinook path of every moved file) and manifest_piece/*.csv, whose file
paths are relative to their delivery folder. Each piece row is placed by finding the planned chinook path that
ends with its file, so a piece may span several deliveries. Rows already in the entry manifest are replaced.
"""
import argparse
import csv
import re
from collections import defaultdict
from pathlib import Path

DELIVERY = re.compile(r"^\d{4}-\d{2}-\d{2}_")


def read_csv(path, delimiter=","):
    with open(path, newline="") as fh:
        r = csv.DictReader(fh, delimiter=delimiter)
        return r.fieldnames, list(r)


def write_csv(path, cols, rows, delimiter=","):
    with open(path, "w", newline="") as fh:
        w = csv.DictWriter(fh, cols, delimiter=delimiter, lineterminator="\n")
        w.writeheader()
        w.writerows(rows)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("work", nargs="+", type=Path, help="work folders, one per dataset")
    ap.add_argument("--data", type=Path, default=Path(__file__).resolve().parents[3] / "data/engcyc")
    ap.add_argument("--status", default="archived", help="manifest.tsv status for each landed dataset")
    args = ap.parse_args()

    fm = args.data / "file_manifests"
    _, locs = read_csv(fm / "LOCATIONS.tsv", "\t")
    roots = sorted(((l["chinook_folder"].rstrip("/") + "/", l["table"]) for l in locs), key=lambda x: -len(x[0]))

    added = defaultdict(list)
    deliveries = defaultdict(set)
    for work in args.work:
        _, plan = read_csv(work / "plan_final.tsv", "\t")
        dests = [p["dest_path"] for p in plan]
        for piece in sorted((work / "manifest_piece").glob("*.csv")):
            _, rows = read_csv(piece)
            for row in rows:
                hits = [d for d in dests if d.endswith("/" + row["file"])]
                if len(hits) != 1:
                    raise SystemExit(f"{piece.name}: {row['file']} matches {len(hits)} planned paths")
                root, table = next((r, t) for r, t in roots if hits[0].startswith(r))
                row["file"] = hits[0][len(root):]
                added[table].append(row)
                parts = Path(row["file"]).parts
                end = next(i for i, p in enumerate(parts) if DELIVERY.match(p))
                deliveries[row["dataset_id"]].add(root + "/".join(parts[:end + 1]))

    for table, rows in added.items():
        path = fm / table
        cols, old = read_csv(path) if path.exists() else (list(rows[0]), [])
        new_files = {r["file"] for r in rows}
        kept = [r for r in old if r["file"] not in new_files]
        write_csv(path, cols, kept + [{c: r.get(c, "") for c in cols} for r in rows])
        print(f"{table}: {len(rows)} rows landed, {len(old) - len(kept)} replaced, {len(kept) + len(rows)} total")

    cols, rows = read_csv(args.data / "manifest.tsv", "\t")
    for r in rows:
        if r["dataset_id"] in deliveries:
            new = [f"chinook:{d}" for d in sorted(deliveries[r["dataset_id"]])]
            r["locations"] = "; ".join(new + [l for l in filter(None, r["locations"].split("; ")) if l not in new])
            r["status"] = args.status
    write_csv(args.data / "manifest.tsv", cols, rows, "\t")

    wcols, work = read_csv(args.data / "worklist.tsv", "\t")
    for w in work:
        found = sorted(deliveries.get(w["dataset_id"], ()))
        if found:
            w["final_folder"] = "; ".join(f + "/" for f in found)
            w["date"] = "; ".join(sorted({Path(f).name.split("_")[0] for f in found}))
            w["state"] = "manifested"
    write_csv(args.data / "worklist.tsv", wcols, work, "\t")
    missing = set(deliveries) - {r["dataset_id"] for r in rows}
    if missing:
        print("not in manifest.tsv:", ", ".join(sorted(missing)))


if __name__ == "__main__":
    main()

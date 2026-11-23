"""Combine every transfer plan and ledger under the engcyc work folder into one moves table.

One row per file moved to chinook: where it came from, where it is now, whether it was compressed on the way,
its bytes and md5 on chinook (joined from file_manifests by path) and the Globus tasks of its transfer.
A file staged through altair keeps its first leg's source.
Rerun after each batch; the table is rebuilt from the plans, so it never drifts from them.
"""
import argparse
import csv
import re
from pathlib import Path

ENDPOINTS = {
    "329fd2aa-d985-11f1-a025-0effda64b40f": "backup",
    "57b23332-9048-11f1-ad24-02ce27bde401": "altair",
    "8dec4129-9ab4-451d-a45f-5b4b8471f7a3": "fir",
    "bae74145-f2ef-41e6-ae73-38d06110819d": "chinook",
}
UUID = re.compile(r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b")
COLS = ["wave", "transfer", "dataset_id", "source", "source_path", "chinook_path", "action",
        "source_bytes", "bytes", "md5", "in_manifest", "globus_tasks"]


def read_tsv(path):
    with open(path, newline="") as fh:
        return list(csv.DictReader(fh, delimiter="\t"))


def manifest_index(data):
    index = {}
    for loc in read_tsv(data / "file_manifests/LOCATIONS.tsv"):
        table = data / "file_manifests" / loc["table"]
        if not table.exists():
            continue
        with open(table, newline="") as fh:
            for row in csv.DictReader(fh):
                index[loc["chinook_folder"].rstrip("/") + "/" + row["file"]] = row
    return index


def ledger_tasks(work):
    tasks = {}
    for ledger in work.rglob("ledger.tsv"):
        for row in read_tsv(ledger):
            tasks.setdefault(row["dataset_id"], []).append(row["task_id"])
    for idfile in work.rglob("task_*.id"):
        tasks.setdefault(idfile.parent.name, []).extend(UUID.findall(idfile.read_text()))
    return tasks


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--work", type=Path, default=Path.home() / "engcyc_meta")
    ap.add_argument("--data", type=Path, default=Path(__file__).resolve().parents[3] / "data/engcyc")
    ap.add_argument("--in-flight", nargs="*", default=[], help="transfer folders whose files have not landed yet")
    args = ap.parse_args()

    index = manifest_index(args.data)
    tasks = ledger_tasks(args.work)
    rows = []
    for plan in sorted(args.work.rglob("plan_final.tsv")):
        rel = plan.relative_to(args.work).parts
        transfer = plan.parent.name
        if rel[0] == "survey" or transfer in args.in_flight:
            continue
        for p in read_tsv(plan):
            hit = index.get(p["dest_path"], {})
            rows.append({
                "wave": rel[0], "transfer": transfer, "dataset_id": hit.get("dataset_id", transfer),
                "source": ENDPOINTS.get(p["source_endpoint"], p["source_endpoint"]),
                "source_path": p["source_path"], "chinook_path": p["dest_path"], "action": p["action"],
                "source_bytes": p["bytes"], "bytes": hit.get("bytes", ""), "md5": hit.get("md5", ""),
                "in_manifest": "yes" if hit else "no", "globus_tasks": ";".join(tasks.get(transfer, [])),
            })
    for date, src, dest in (l.rstrip("\n").split("\t") for l in open(args.data / "sources/chinook/external_moves.tsv")):
        hit = index.get(dest, {})
        rows.append({"wave": "w1", "transfer": f"rename_{date}", "dataset_id": hit.get("dataset_id", ""),
                     "source": "chinook", "source_path": src, "chinook_path": dest, "action": "renamed",
                     "source_bytes": hit.get("bytes", ""), "bytes": hit.get("bytes", ""), "md5": hit.get("md5", ""),
                     "in_manifest": "yes" if hit else "no", "globus_tasks": ""})
    by_dest = {}
    for r in rows:
        kept = by_dest.setdefault(r["chinook_path"], r)
        if kept is not r:
            kept["globus_tasks"] = ";".join(filter(None, [kept["globus_tasks"], r["globus_tasks"]]))
    rows = list(by_dest.values())
    out = args.data / "moves.tsv"
    with open(out, "w", newline="") as fh:
        w = csv.DictWriter(fh, COLS, delimiter="\t", lineterminator="\n")
        w.writeheader()
        w.writerows(rows)
    print(f"{len(rows)} rows, {sum(r['in_manifest'] == 'yes' for r in rows)} matched a manifest row -> {out}")


if __name__ == "__main__":
    main()

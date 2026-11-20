"""Move planned raw-data files onto chinook with Globus, then verify them and write file listings.

A plan is a TSV of source_endpoint, source_path, dest_path, bytes, action, one row per file, named
<workdir>/<dataset_id>/plan_final.tsv. Stages, each safe to rerun:
  check     list every destination folder and count files missing, present, or present with another size
  transfer  submit one checksum-verified Globus batch per dataset for the files not yet present
  status    show the Globus status of every submitted task
  verify    list again; once complete write <dataset_id>/listings/<delivery>.tsv (bytes, md5, file relative to
            the delivery folder), the input of manifest_listing.py
"""
import argparse
import csv
import json
import subprocess
import sys
from collections import defaultdict
from pathlib import Path, PurePosixPath

GLOBUS = "/home/tony/lib/miniforge3/envs/globus/bin/globus"
CHINOOK = "bae74145-f2ef-41e6-ae73-38d06110819d"
LEDGER_COLUMNS = ["dataset_id", "task_id", "files", "bytes"]


def globus(*args, timeout=600):
    out = subprocess.run([GLOBUS, *args], capture_output=True, text=True, timeout=timeout)
    if out.returncode:
        raise RuntimeError(f"globus {' '.join(args)}: {out.stderr.strip()}")
    return out.stdout


def read_plan(path):
    with open(path) as fh:
        return list(csv.DictReader(fh, delimiter="\t"))


def entry_root(dest):
    """The folder a manifest.csv sits in: a Received_raw_data entry or an external dataset folder."""
    parts = PurePosixPath(dest).parts
    depth = 4 if parts[2] == "Received_raw_data" else 6
    return str(PurePosixPath(*parts[:depth]))


def delivery(dest):
    parts = PurePosixPath(dest).parts
    depth = 5 if parts[2] == "Received_raw_data" else 6
    return str(PurePosixPath(*parts[:depth]))


class Chinook:
    def __init__(self):
        self.sizes = {}
        self.listed = set()

    def list(self, folder):
        if folder in self.listed:
            return
        self.listed.add(folder)
        try:
            data = json.loads(globus("ls", "-r", "--recursive-depth-limit", "12", "--format", "json", f"{CHINOOK}:{folder}/"))
        except RuntimeError as err:
            if "404" in str(err) or "ClientError.NotFound" in str(err):
                return
            raise
        for item in data["DATA"]:
            if item["type"] == "file":
                self.sizes[f"{folder}/{item['name']}"] = item["size"]

    def classify(self, rows):
        for folder in sorted({delivery(r["dest_path"]) for r in rows}):
            self.list(folder)
        state = defaultdict(list)
        for r in rows:
            size = self.sizes.get(r["dest_path"])
            state["missing" if size is None else "present" if size == int(r["bytes"]) else "wrong_size"].append(r)
        return state


def load_ledger(path):
    if not path.exists():
        return []
    with open(path) as fh:
        return list(csv.DictReader(fh, delimiter="\t"))


def append_ledger(path, row):
    new = not path.exists()
    with open(path, "a", newline="") as fh:
        w = csv.DictWriter(fh, LEDGER_COLUMNS, delimiter="\t")
        if new:
            w.writeheader()
        w.writerow(row)


def submit(dataset_id, rows, dry_run):
    by_source = defaultdict(list)
    for r in rows:
        by_source[r["source_endpoint"]].append(r)
    tasks = []
    for source, group in by_source.items():
        batch = "".join(f'"{r["source_path"]}" "{r["dest_path"]}"\n' for r in group)
        if dry_run:
            print(f"  would submit {len(group)} files {sum(int(r['bytes']) for r in group) / 1e9:.1f} GB from {source}")
            continue
        out = subprocess.run(
            [GLOBUS, "transfer", source, CHINOOK, "--batch", "-", "--sync-level", "checksum", "--verify-checksum",
             "--label", f"engcyc {dataset_id}", "--format", "json"],
            input=batch, capture_output=True, text=True, timeout=600)
        if out.returncode:
            raise RuntimeError(f"submit {dataset_id}: {out.stderr.strip()}")
        tasks.append((json.loads(out.stdout)["task_id"], group))
    return tasks


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("stage", choices=("check", "transfer", "status", "verify"))
    ap.add_argument("plans", type=Path, nargs="+", help="<workdir>/<dataset_id>/plan_final.tsv files")
    ap.add_argument("--ledger", type=Path, required=True, help="TSV of submitted Globus tasks")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--skip-units", type=Path, help="compress_reads.py units.tsv: leave its input files to that script")
    args = ap.parse_args()

    if args.stage == "status":
        for row in load_ledger(args.ledger):
            task = json.loads(globus("task", "show", row["task_id"], "--format", "json"))
            print(f"{row['dataset_id']:32} {task['status']:10} {task['files_transferred']}/{row['files']} files "
                  f"{task['bytes_transferred'] / 1e9:.1f}/{int(row['bytes']) / 1e9:.1f} GB  {row['task_id']}")
        return

    skip = set()
    if args.skip_units:
        with open(args.skip_units) as fh:
            skip = {i[2] for u in csv.DictReader(fh, delimiter="\t") for i in json.loads(u["inputs"])}
    chinook = Chinook()
    failed = False
    for plan in args.plans:
        dataset_id = plan.parent.name
        rows = [r for r in read_plan(plan) if r["dest_path"] not in skip]
        state = chinook.classify(rows)
        counts = " ".join(f"{k}={len(state[k])}" for k in ("missing", "present", "wrong_size"))
        print(f"{dataset_id:32} {counts}")
        for r in state["wrong_size"]:
            print(f"  wrong size: {r['dest_path']}", file=sys.stderr)
            failed = True
        if args.stage == "transfer" and state["missing"] and not state["wrong_size"]:
            for task_id, group in submit(dataset_id, state["missing"], args.dry_run):
                append_ledger(args.ledger, {"dataset_id": dataset_id, "task_id": task_id, "files": len(group),
                                            "bytes": sum(int(r["bytes"]) for r in group)})
                print(f"  submitted {task_id}")
        if args.stage == "verify":
            if state["missing"] or state["wrong_size"]:
                failed = True
                continue
            by_delivery = defaultdict(list)
            for r in rows:
                by_delivery[delivery(r["dest_path"])].append(r)
            (plan.parent / "listings").mkdir(exist_ok=True)
            for folder, group in by_delivery.items():
                with open(plan.parent / "listings" / f"{PurePosixPath(folder).name}.tsv", "w", newline="") as fh:
                    w = csv.writer(fh, delimiter="\t")
                    w.writerow(["bytes", "md5", "file"])
                    for r in sorted(group, key=lambda r: r["dest_path"]):
                        w.writerow([r["bytes"], "", str(PurePosixPath(r["dest_path"]).relative_to(folder))])
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()

"""Fetch the CAMI corpus from a manifest. Resumable, size-checked, login-node safe.

A file is promoted out of staging only once its byte count matches the manifest, so an
interrupted run never leaves a truncated file that looks complete.
"""

import argparse
import concurrent.futures as cf
import csv
import os
import subprocess
import sys
import threading
from pathlib import Path

_lock = threading.Lock()


def log(msg: str) -> None:
    with _lock:
        sys.stderr.write(msg + "\n")
        sys.stderr.flush()


def read_manifest(path: Path, exclude_drop: set[int]) -> list[dict]:
    with path.open() as f:
        rows = list(csv.DictReader(f, delimiter="\t"))
    return [r for r in rows if int(r["drop"]) not in exclude_drop]


def fetch_one(row: dict, root: Path, staging: Path, retries: int = 3) -> tuple[str, str]:
    want = int(row["bytes"])
    dest = root / row["dataset"] / row["relpath"]
    if dest.exists() and (want <= 0 or dest.stat().st_size == want):
        return ("have", str(dest))

    part = staging / (row["dataset"] + "__" + row["relpath"].replace("/", "__"))
    part.parent.mkdir(parents=True, exist_ok=True)
    dest.parent.mkdir(parents=True, exist_ok=True)

    for attempt in range(retries):
        # -C - resumes a partial .part across restarts and across retries.
        rc = subprocess.run(
            ["curl", "-sS", "--fail", "--location", "--retry", "3",
             "--connect-timeout", "60", "-C", "-", "-o", str(part), row["url"]],
            capture_output=True, text=True,
        )
        got = part.stat().st_size if part.exists() else 0
        if rc.returncode == 0 and (want <= 0 or got == want):
            os.replace(part, dest)
            return ("got", str(dest))
        # curl exits 33 when the server refuses a range on an already-complete file.
        if rc.returncode == 33 and want > 0 and got == want:
            os.replace(part, dest)
            return ("got", str(dest))
        # Overshoot means the range was ignored and the body was appended onto the
        # partial. Resuming again appends again, so the partial has to go.
        if want > 0 and got > want:
            part.unlink(missing_ok=True)
            continue
        if attempt == retries - 1:
            return ("FAIL", f"{row['url']} rc={rc.returncode} got={got} want={want} {rc.stderr.strip()[:200]}")
    return ("FAIL", row["url"])


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--manifest", type=Path, required=True)
    ap.add_argument("--root", type=Path, required=True, help="corpus landing directory")
    ap.add_argument("--staging", type=Path, default=None, help="default <root>/.staging")
    ap.add_argument("--workers", type=int, default=4,
                    help="keep modest: this runs on a shared login node")
    ap.add_argument("--exclude-drop", type=int, nargs="*", default=[],
                    help="drop ranks to skip, e.g. 1 for the per-genome BAM trees")
    ap.add_argument("--dataset", nargs="*", default=None, help="limit to these datasets")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    staging = args.staging or (args.root / ".staging")
    rows = read_manifest(args.manifest, set(args.exclude_drop))
    if args.dataset:
        rows = [r for r in rows if r["dataset"] in set(args.dataset)]
    total = sum(int(r["bytes"]) for r in rows if int(r["bytes"]) > 0)
    log(f"{len(rows)} objects, {total / 1024**4:.2f} TiB -> {args.root}")
    if args.dry_run:
        return

    args.root.mkdir(parents=True, exist_ok=True)
    staging.mkdir(parents=True, exist_ok=True)

    counts = {"have": 0, "got": 0, "FAIL": 0}
    failures = []
    with cf.ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(fetch_one, r, args.root, staging): r for r in rows}
        for i, fut in enumerate(cf.as_completed(futures), 1):
            kind, detail = fut.result()
            counts[kind] += 1
            if kind == "FAIL":
                failures.append(detail)
                log(f"[{i}/{len(rows)}] FAIL {detail}")
            elif i % 25 == 0 or kind == "got":
                log(f"[{i}/{len(rows)}] {kind} {counts['got']} fetched, "
                    f"{counts['have']} already present, {counts['FAIL']} failed")

    log(f"\ndone: {counts['got']} fetched, {counts['have']} already present, "
        f"{counts['FAIL']} failed")
    for f in failures:
        log("  " + f)
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()

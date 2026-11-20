#!/usr/bin/env python3
"""Merge the photobioreactor's exported logs into one table of sensor rows.

    python research/aspire/campaigns/ab48_r1/merge_reactor_logs.py ReactorLogs/ OUT.csv

Takes the reactor's ReactorLogs folder as exported (Log*.csv at the top for the current run,
Archive/<export>/Log*.csv for earlier ones) and writes every sensor row once, oldest first, with
the run's culture name and the file it came from.
"""

import argparse
import csv
from pathlib import Path


def read_log(path: Path, root: Path):
    with open(path, encoding="utf-8-sig", newline="") as f:
        lines = list(csv.reader(f))
    culture = next(r[1] for r in lines[:3] if r and r[0] == "Culture Name:")
    header_at = next(i for i, r in enumerate(lines) if r and r[0] == "Date")
    header = lines[header_at]
    rows = [r[:len(header)] for r in lines[header_at + 1:]
            if r and r[0] != "Event:" and len(r) >= len(header)]
    return header, culture, str(path.relative_to(root)), rows


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("logs", type=Path)
    ap.add_argument("out", type=Path)
    args = ap.parse_args()

    header, by_time = None, {}
    for path in sorted(args.logs.rglob("Log*.csv")):
        cols, culture, source, rows = read_log(path, args.logs)
        assert header is None or cols == header, f"{path}: columns differ from the first log's"
        header = cols
        for r in rows:
            by_time[r[0]] = [*r, culture, source]
    with open(args.out, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow([*header, "run", "source_file"])
        w.writerows(by_time[t] for t in sorted(by_time))
    print(f"{args.out}: {len(by_time)} rows, {min(by_time)} to {max(by_time)}")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Draw E1's run-to-run control subset and write its nf-core/mag input sheets.

A stratified 10% of every CAMI dataset in E1 (nearest integer, at least one), drawn with a
fixed seed. For each arm it writes the subset's rows of E1's read sheet and an
--assembly_input sheet pointing at E1's own assemblies, taken from e1_close's sheet.
"""

import argparse
import csv
import math
import random
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
ASSEMBLER = {"short": "MEGAHIT", "long": "Flye"}


def draw(rows, fraction, seed):
    by_dataset = defaultdict(list)
    for r in rows:
        by_dataset[(r["arm"], r["dataset"])].append(r)
    rng = random.Random(seed)
    picked = []
    for key in sorted(by_dataset):
        members = sorted(by_dataset[key], key=lambda r: int(r["idx"]))
        k = max(1, math.floor(fraction * len(members) + 0.5))
        chosen = rng.sample(members, k)
        picked.extend(sorted(chosen, key=lambda r: int(r["idx"])))
        print(f"{key[0]}\t{key[1]}\t{k}/{len(members)}\t" + " ".join(r["sample"] for r in chosen))
    return picked


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("sheet", help="e1_close sheet.tsv")
    ap.add_argument("--fraction", type=float, default=0.1)
    ap.add_argument("--seed", type=int, default=20260922)
    ap.add_argument("--out", type=Path, default=HERE)
    args = ap.parse_args()

    with open(args.sheet, newline="") as fh:
        rows = list(csv.DictReader(fh, delimiter="\t"))
    picked = draw(rows, args.fraction, args.seed)

    for arm in ("short", "long"):
        subset = [r for r in picked if r["arm"] == arm]
        names = {r["sample"] for r in subset}
        with open(HERE / f"samplesheet.{arm}.csv", newline="") as fh:
            reader = csv.DictReader(fh)
            reads = [r for r in reader if r["sample"] in names]
            header = reader.fieldnames
        assert len(reads) == len(subset), f"{arm}: {len(reads)} read rows for {len(subset)} samples"
        with open(args.out / f"samplesheet.{arm}.ctl.csv", "w", newline="") as fh:
            w = csv.DictWriter(fh, header, lineterminator="\n")
            w.writeheader()
            w.writerows(reads)
        group = {r["sample"]: r["group"] for r in reads}
        with open(args.out / f"assembly_input.{arm}.ctl.csv", "w", newline="") as fh:
            w = csv.writer(fh, lineterminator="\n")
            w.writerow(["id", "group", "assembler", "fasta"])
            for r in subset:
                w.writerow([r["sample"], group[r["sample"]], ASSEMBLER[arm], r["assembly"]])
        print(f"{arm}: {len(subset)} samples")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""E4's ablation ladder over a stratified 10% subset of its bins.

pick: sample 10% of each study's bins that have a metaGEM GEM and a model in both archived lanes,
and write them to e4_ablation_bins.tsv.
"""

import argparse
import csv
import math
import random
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
TALLY = HERE.parent / "results/e4/tally.tsv"
BINS = HERE / "e4_ablation_bins.tsv"
SEED = 20260928
FRACTION = 0.10


def read_bins(path=BINS):
    with open(path) as f:
        return [(r["study"], r["bin"]) for r in csv.DictReader(f, delimiter="\t")]


def cmd_pick(args):
    eligible = defaultdict(list)
    with open(TALLY) as f:
        for r in csv.DictReader(f, delimiter="\t"):
            if r["metagem_gem"] == r["repro_model"] == r["modern_model"] == "1":
                eligible[r["study"]].append(r["bin"])
    rng = random.Random(SEED)
    picked = []
    for study in sorted(eligible):
        pool = sorted(eligible[study])
        n = math.floor(len(pool) * FRACTION + 0.5)
        picked += [(study, b) for b in sorted(rng.sample(pool, n))]
        print(f"{study}\t{len(pool)}\t{n}")
    print(f"total\t{sum(map(len, eligible.values()))}\t{len(picked)}")
    with open(args.out, "w") as f:
        w = csv.writer(f, delimiter="\t", lineterminator="\n")
        w.writerow(["study", "bin"])
        w.writerows(picked)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    pick = sub.add_parser("pick")
    pick.add_argument("--out", type=Path, default=BINS)
    pick.set_defaults(func=cmd_pick)
    args = p.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()

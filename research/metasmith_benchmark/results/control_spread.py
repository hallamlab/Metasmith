"""Print the run-to-run spread on the 25 control samples: each pair's MAG match percentages,
one row per arm and binner, from the summary.tsv that compare_arms.py wrote for that pair.

Each cell is "first run / second run", the share of that run's MAGs with a reciprocal partner.
E1ctl vs E1ctl2 is the seed-only pair. The pairs against E1 also carry E1's trimmed-read depths.
"""
import argparse
import csv
from pathlib import Path

from reproduction import markdown

HERE = Path(__file__).resolve().parent
PAIRS = (
    ("E1ctl vs E1ctl2", HERE / "compare_arms_ctl_ctl2" / "e1ctl_e1ctl2"),
    ("E1 vs E1ctl", HERE / "compare_arms_ctl" / "e1_e1ctl"),
    ("E1 vs E1ctl2", HERE / "compare_arms_ctl2" / "e1_e1ctl2"),
    ("E1 vs E2", HERE / "compare_arms_ctl" / "e1_e2"),
)


def main():
    argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter).parse_args()
    cells = {}
    for label, d in PAIRS:
        with open(d / "summary.tsv", newline="") as f:
            for r in csv.DictReader(f, delimiter="\t"):
                cells.setdefault((r["arm"], r["binner"]), {})[label] = f"{r['E1_matched_pct']} / {r['E2_matched_pct']}"
    head = ["arm", "binner"] + [label for label, _ in PAIRS]
    print(markdown(head, [[arm, binner] + [c[label] for label, _ in PAIRS] for (arm, binner), c in cells.items()]))


if __name__ == "__main__":
    main()

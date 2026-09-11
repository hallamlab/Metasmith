#!/usr/bin/env python3
"""Round 2 of curation: normalise the KBase universe into a proposed ontology.

Round 1 was mechanical -- it reduced 3,965 narratives to 1,633 distinct workflow
shapes and judged none of them. This round proposes the vocabulary a curated
library would be built from, and checks that the proposal reconciles with the
port's own denominators. It proposes. It does not build.

Runs the four stages in order, then the gate:

    assign.py       493 apps          -> 54 core task verbs
    map_types.py    121 workspace types + 67 unions -> 35 entities on a property grid
    map_params.py   2,758 parameters  -> one disposition each, and a config surface
    catalogue.py    1,633 shapes      -> canonical workflows over the verbs
    check.py        every table joined back to catalog/ and curation/r1/

    curate.py [--skip-check]
"""

from __future__ import annotations

import argparse
import functools
import subprocess
import sys
from pathlib import Path

print = functools.partial(print, flush=True)

HERE = Path(__file__).resolve().parent
STAGES = ["assign.py", "map_types.py", "map_params.py", "catalogue.py"]


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--skip-check", action="store_true",
                    help="write the tables without reconciling them")
    args = ap.parse_args()

    for stage in STAGES:
        print(f"\n=== {stage} " + "=" * (60 - len(stage)))
        subprocess.run([sys.executable, str(HERE / stage)], check=True)
    if args.skip_check:
        return
    print("\n=== check.py " + "=" * 52)
    subprocess.run([sys.executable, str(HERE / "check.py")], check=True)


if __name__ == "__main__":
    main()

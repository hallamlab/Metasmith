#!/usr/bin/env python3
"""Build a masked view of the generated KBase transform library.

The whole library hands the planner every producer of every type at once, and
130 apps produce a Genome. Worse, 46 apps are pure sources -- uploaders and
importers that require nothing -- so they are the cheapest producer of anything
and an unmasked solve answers every target with "import it from staging".

A mask is a directory of the stubs a solve is allowed to use, compiled on its
own. Call `materialise` with the app ids to keep.
"""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent.parent
CATALOG = REPO / "research" / "kbase" / "catalog"


def catalog_rows() -> dict:
    return {a["app_id"]: a for a in
            (json.loads(l) for l in (CATALOG / "apps.jsonl").read_text().splitlines() if l)}


def ledger_rows() -> dict:
    return {r["app_id"]: r for r in
            (json.loads(l) for l in (CATALOG / "ledger.jsonl").read_text().splitlines() if l)
            if r["disposition"] == "converted"}


def pure_sources() -> set[str]:
    apps = catalog_rows()
    return {aid for aid, r in ledger_rows().items() if not apps[aid]["inputs"]}


def materialise(dest: Path, keep: set[str], *, compile: bool = True) -> Path:
    """Write a transform library holding only `keep`, and compile its metadata."""
    led = ledger_rows()
    dest = Path(dest)
    if dest.exists(): shutil.rmtree(dest)
    dest.mkdir(parents=True)
    n = 0
    for aid in sorted(keep):
        row = led.get(aid)
        if not row: continue
        shutil.copy2(HERE / "transforms" / "kbase" / row["transform"], dest / row["transform"])
        n += 1
    if compile:
        subprocess.run(
            [sys.executable, "-m", "metasmith", "build", "transforms", "--types",
             str(HERE / "data_types"), "--transforms", str(dest)],
            check=True, capture_output=True,
        )
    return dest


if __name__ == "__main__":
    print(f"{len(ledger_rows())} converted, {len(pure_sources())} of them pure sources")

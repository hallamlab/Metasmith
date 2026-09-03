#!/usr/bin/env python3
"""Print a deterministic fingerprint of every shipped template's solved plan.

`dev/libraries.sh -b` asserts that each template still solves and prints its step
count. That is enough to catch a break and not enough to catch a re-route: widening
a requirement can leave the count identical while a target starts resolving through
a different transform. This prints step count plus the transform behind every step,
sorted, so two runs diff cleanly.

Wall clock on this host is not comparable between runs; the names and counts are.

    python research/metasmith_libraries/template_fingerprint.py [name ...]
"""

from __future__ import annotations

import sys
import traceback
from pathlib import Path

MLIB = Path(__file__).resolve().parents[2] / "src" / "metasmith_libraries"
sys.path.insert(0, str(MLIB))

import build_templates as B  # noqa: E402


def fingerprint(module) -> list[str]:
    task = module.build_spec(rebuild=False).Solve()
    if not task.ok:
        return [f"  DROPPED {sorted(task.plan.dropped_targets)}"]
    rows = []
    for step in task.plan.steps:
        produced = sorted({i.dtype_name for g in step.produces for i in g})
        rows.append(f"  {Path(step.transform._path).stem:<32} -> {', '.join(produced)}")
    return sorted(rows)


def main() -> int:
    names = sys.argv[1:]
    modules = [m for m in B.authors() if not names or m.NAME in names]
    for module in modules:
        print(f"\n[{module.NAME}]")
        try:
            rows = fingerprint(module)
        except Exception:
            traceback.print_exc()
            print("  FAILED")
            continue
        print(f"  steps={len(rows)}")
        for row in rows:
            print(row)
    return 0


if __name__ == "__main__":
    sys.exit(main())

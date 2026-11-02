#!/usr/bin/env python3
"""Render the solved plans the report puts side by side, in both themes.

Two drivers, one picture each: the metagenomics template the library already
ships, and the viromics survey this scope adds. They are drawn from the same
renderer with the same settings so the two shapes can be compared honestly --
same node vocabulary, same colour scheme, same scale.

    PYTHONPATH=src python research/viromics/reports/mkplandag.py

Each driver's own `--dag` writes one light render with a painted background,
which is right for opening in a viewer and wrong for a web page: the report
sits on its own ground and follows the reader's theme. This solves once per
driver and renders twice with `background=False`, so each file composites onto
whatever is behind it.

Step order is on: the numbers are the plan's execution order, which is what
lets a reader match a node to a row in the report's ledger.

Keep the basename free of dots -- `render()` reads a suffix as the output
format, so `plan-dag.light` asks graphviz for a format called "light".
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
sys.path.insert(0, str(ROOT / "src" / "metasmith_libraries"))
sys.path.insert(0, str(HERE.parent))

import metagenomics_from_paired_reads as METAGENOMICS       # noqa: E402
import viromics_survey_from_paired_reads as VIROMICS        # noqa: E402

DRIVERS = {"metagenomics": METAGENOMICS, "viromics": VIROMICS}


def main() -> int:
    for name, driver in DRIVERS.items():
        task = driver.build_spec().Solve(max_iter=1024, max_refine=256, seed=42)
        plan = task.plan
        if not task.ok or plan.dropped_targets:
            print(f"{name}: refusing to render an incomplete plan: ok={task.ok} "
                  f"dropped={sorted(plan.dropped_targets)}")
            return 1
        print(f"{name}: {len(plan.steps)} steps, {len(driver.TARGETS)} targets")
        for theme in ("light", "dark"):
            out = plan.RenderDAG(str(HERE / f"plan-dag-{name}-{theme}"), format="svg",
                                 theme=theme, background=False, show_step_order=True)
            dims = re.search(r'width="(\d+)" height="(\d+)"',
                             Path(out).read_text()[:400])
            print(f"  {theme}: {dims.group(1)}x{dims.group(2)} -> {Path(out).name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

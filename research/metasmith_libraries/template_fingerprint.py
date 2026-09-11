#!/usr/bin/env python3
"""Print a deterministic fingerprint of every shipped template's solved plan.

`dev/libraries.sh -b` asserts that each template still solves and prints its step
count. That is enough to catch a break and not enough to catch a re-route: widening
a requirement can leave the count identical while a target starts resolving through
a different transform. This prints step count plus the transform behind every step,
sorted, so two runs diff cleanly.

Wall clock on this host is not comparable between runs; the names and counts are.

**What an identical fingerprint does NOT mean.** It says the plan SHAPE is unchanged.
It does not say the library is. A transform whose name and product types stay put can
be rewritten entirely -- different tool, different flags, different science -- and this
prints byte-identical output, because nothing here reaches inside a transform. That is
the more common kind of change and it is the kind that retires cache shards, since a
transform's identity hashes its whole source file.

The worked example, 2026-09-11: `transforms/assembly/spades.py` went from a hand-rolled
metaSPAdes call to the DOE JGI Metagenome Workflow -- bbcms error correction added,
kmers fixed, a 200 bp contig floor imposed. Every template naming `spades_assembly`
changed meaning, `ecspr_survey_from_pooled_reads` included, and this tool reported no
difference at all. Correctly: no step re-routed.

So use it for what it is. A clean diff here plus a changed transform file means the
change landed without disturbing anything else, which is the useful thing to know. A
clean diff on its own is not evidence that nothing changed -- for that, read the diff.

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

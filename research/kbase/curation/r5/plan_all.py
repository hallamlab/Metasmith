#!/usr/bin/env python3
"""Solve every shipped template and print each plan, step by step.

    plan_all.py > _plans_r5.txt

The comparison a round makes against its predecessor is step-for-step, never by
step count: a plan that swaps one producer for another and stays the same length
is exactly the regression a count would miss. Each template prints its solved
steps' source paths, sorted, under a header carrying the name and the count.

Run from a checkout whose `_metadata/` has been rebuilt. Against an older commit,
extract it with `git archive` first, so both the engine and the library on that
side are the committed ones.

The solver logs to stdout, so the plan lines carry a marker and everything else is
filtered out. Both sides of a comparison must be filtered the same way, which is
why the filter lives here rather than in whoever calls this.
"""
from __future__ import annotations

import sys
import traceback
from pathlib import Path

HERE = Path(__file__).resolve()
REPO = HERE.parents[4]
MLIB = REPO / "src" / "metasmith_libraries"
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(MLIB))

import build_templates as B                                        # noqa: E402


def step_source(step) -> str:
    """The transform's path relative to its library root.

    `_path` is where the instance was loaded from; the library root is its
    `transforms/<group>` directory, and the two-segment tail is what r4's
    comparison printed and what makes the two sides of a diff line up.
    """
    transform = step.transform
    path = Path(getattr(transform, "_path", "") or transform.name or "?")
    if path.is_absolute():
        parts = path.parts
        if "transforms" in parts:
            i = parts.index("transforms")
            return str(Path(*parts[i + 1:]))
        return str(path)
    # `_path` is relative to its own library root, so two libraries both holding a
    # `checkm.py` would print the same line. The root's directory name is the group.
    root = Path(str(getattr(step.transform_library, "location", "") or "?")).name
    return str(Path(root) / path)


def main() -> int:
    out: list[str] = []
    for module in B.authors():
        try:
            spec = module.build_spec(rebuild=False)
            task = spec.Solve()
        except Exception:
            out.append(f"### {module.NAME} ok=False steps=0")
            traceback.print_exc(file=sys.stderr)
            continue
        steps = sorted(step_source(s) for s in task.plan.steps)
        out.append(f"### {module.NAME} ok={task.ok} steps={len(steps)}")
        out.extend(f"    {s}" for s in steps)
    for name, why in sorted(B.BLOCKED.items()):
        out.append(f"### {name} BLOCKED {why}")
    print("\n".join(out))
    return 0


if __name__ == "__main__":
    sys.exit(main())

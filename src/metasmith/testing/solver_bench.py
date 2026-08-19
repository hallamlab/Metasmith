"""Benchmark runner for the solver: wall time plus a fingerprint, per case.

Every performance change to the solver reports through this. The contract is
one line: **the fingerprints must match and the time must drop, or the change
is reverted.** Anything that cannot show a wall-time win on this corpus is not
paying for itself, however good the idea was.

Usage::

    python -m metasmith.testing.solver_bench --out after.json
    python -m metasmith.testing.solver_bench --out after.json --baseline before.json

The corpus is the four shipped templates (real transform libraries, real
search) plus a fixed slice of generated instances covering the pressures the
harness dials expose. Template cases are skipped, not failed, when the standard
library has not been compiled — the generated slice always runs.
"""

from __future__ import annotations

import argparse
import json
import os
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any

from .solver_verification import (
    GeneratorDials,
    check_plan,
    generate_problem,
    plan_fingerprint,
    problem_of_plan,
)

__all__ = ["CORPUS", "run_generated", "run_templates", "run_all", "diff"]


CORPUS: list[tuple[str, int, GeneratorDials]] = [
    ("chain-6", 1, GeneratorDials(n_types=6, n_extra_transforms=3)),
    ("chain-10", 2, GeneratorDials(n_types=10, n_extra_transforms=6)),
    (
        "cyclic",
        3,
        GeneratorDials(n_types=8, n_extra_transforms=6, cycle_density=0.6),
    ),
    (
        "lineage",
        4,
        GeneratorDials(
            n_types=8, n_extra_transforms=6, lineage_density=0.8, target_lineage=1.0
        ),
    ),
    (
        "duplicates",
        5,
        GeneratorDials(n_types=8, n_extra_transforms=4, n_duplicate_transforms=4),
    ),
    (
        "product-groups",
        6,
        GeneratorDials(n_types=8, n_extra_transforms=6, product_group_density=0.7),
    ),
    (
        "multi-given",
        7,
        GeneratorDials(n_types=8, n_given=2, n_given_groups=3, n_extra_transforms=5),
    ),
    (
        "kitchen-sink",
        8,
        GeneratorDials(
            n_types=10,
            n_given=2,
            n_given_groups=2,
            n_extra_transforms=8,
            cycle_density=0.4,
            lineage_density=0.7,
            n_duplicate_transforms=3,
            product_group_density=0.5,
            target_lineage=1.0,
        ),
    ),
]


STRESS_CORPUS: list[tuple[str, int, GeneratorDials]] = [
    (
        "wide-search",
        816,
        GeneratorDials(
            n_types=8,
            n_extra_transforms=16,
            product_group_density=0.5,
            lineage_density=0.5,
            target_lineage=0.5,
            max_requirements=3,
        ),
    ),
    (
        "deep-search",
        1216,
        GeneratorDials(
            n_types=12,
            n_extra_transforms=16,
            product_group_density=0.5,
            lineage_density=0.5,
            target_lineage=0.5,
            max_requirements=3,
        ),
    ),
    (
        "dense-groups",
        1616,
        GeneratorDials(
            n_types=16,
            n_extra_transforms=16,
            product_group_density=0.9,
            lineage_density=0.5,
            target_lineage=0.5,
            max_requirements=3,
        ),
    ),
]


def _libraries_root() -> Path | None:
    env = os.environ.get("METASMITH_LIBRARIES_ROOT")
    if env:
        p = Path(env).expanduser().resolve()
        return p if p.exists() else None
    root = Path(__file__).resolve().parents[2] / "metasmith_libraries"
    compiled = root / "transforms" / "logistics" / "_metadata" / "index.yml"
    return root if compiled.exists() else None


def run_generated(cases: list[tuple[str, int, GeneratorDials]] | None = None) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for name, seed, dials in cases if cases is not None else CORPUS:
        problem = generate_problem(seed, dials, name=name)
        t0 = time.perf_counter()
        solution = problem.solve()
        elapsed = time.perf_counter() - t0
        verdict = check_plan(problem, solution)
        out[f"gen/{name}"] = {
            "seconds": round(elapsed, 4),
            "fingerprint": plan_fingerprint(solution),
            "steps": len(solution.dependency_plan),
            "ok": verdict.ok,
            "violations": verdict.violations,
            "iterations": solution._iterations,
            "refiner_iterations": sum(t for _, t in solution._refiner_iterations),
            "dials": asdict(dials),
        }
    return out


def run_templates(root: Path | None = None) -> dict[str, Any]:
    root = root or _libraries_root()
    if root is None:
        return {}
    from ..agents import Template

    out: dict[str, Any] = {}
    for template in Template.Discover(root):
        t0 = time.perf_counter()
        task = template.spec.Solve()
        elapsed = time.perf_counter() - t0
        result = getattr(task.plan, "_solver_result", None)
        problem = problem_of_plan(task.plan, name=template.name)
        if problem is None or result is None:
            verdict_ok, violations = False, [
                "plan carries no solver inputs, so the checker cannot see it"
            ]
        else:
            verdict = check_plan(problem, result)
            verdict_ok, violations = verdict.ok, verdict.violations
        out[f"template/{template.name}"] = {
            "seconds": round(elapsed, 4),
            "fingerprint": plan_fingerprint(result) if result is not None else None,
            "steps": len(task.plan.steps),
            "ok": bool(task.ok) and verdict_ok,
            "violations": violations,
            "iterations": getattr(result, "_iterations", None),
            "refiner_iterations": (
                sum(t for _, t in result._refiner_iterations)
                if result is not None
                else None
            ),
        }
    return out


def run_all(*, templates: bool = True, root: Path | None = None) -> dict[str, Any]:
    cases = run_generated()
    if templates:
        cases.update(run_templates(root))
    return {
        "cases": cases,
        "total_seconds": round(sum(c["seconds"] for c in cases.values()), 4),
    }


def diff(baseline: dict[str, Any], current: dict[str, Any]) -> list[str]:
    lines: list[str] = []
    b, c = baseline["cases"], current["cases"]
    changed = [k for k in sorted(set(b) & set(c)) if b[k]["fingerprint"] != c[k]["fingerprint"]]
    if changed:
        lines.append(f"!! FINGERPRINT CHANGED on {len(changed)} case(s): {', '.join(changed)}")
    for k in sorted(set(b) | set(c)):
        if k not in b:
            lines.append(f"   + {k}: new, {c[k]['seconds']:.3f}s")
            continue
        if k not in c:
            lines.append(f"   - {k}: dropped")
            continue
        bs, cs = b[k]["seconds"], c[k]["seconds"]
        pct = ((cs - bs) / bs * 100) if bs else 0.0
        flag = "  " if b[k]["fingerprint"] == c[k]["fingerprint"] else "!!"
        lines.append(f"{flag} {k}: {bs:.3f}s -> {cs:.3f}s ({pct:+.1f}%)")
    bt, ct = baseline["total_seconds"], current["total_seconds"]
    lines.append(f"   TOTAL: {bt:.3f}s -> {ct:.3f}s ({(ct - bt) / bt * 100:+.1f}%)")
    return lines


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", type=Path, help="write results JSON here")
    ap.add_argument(
        "--pin",
        type=Path,
        help="write the stable fields only (no timings) -- the checked-in gate",
    )
    ap.add_argument(
        "--stress",
        action="store_true",
        help="run STRESS_CORPUS instead of CORPUS (slow, mcts-bound)",
    )
    ap.add_argument("--baseline", type=Path, help="compare against this results JSON")
    ap.add_argument("--no-templates", action="store_true")
    ap.add_argument("--templates-root", type=Path, default=None)
    args = ap.parse_args(argv)

    if args.stress:
        cases = run_generated(STRESS_CORPUS)
        current = {
            "cases": cases,
            "total_seconds": round(sum(c["seconds"] for c in cases.values()), 4),
        }
    else:
        current = run_all(templates=not args.no_templates, root=args.templates_root)
    if args.out:
        args.out.write_text(json.dumps(current, indent=2), encoding="utf-8")
    if args.pin:
        pin = {
            "cases": {
                k: {f: v[f] for f in ("fingerprint", "steps", "ok")}
                for k, v in current["cases"].items()
            }
        }
        args.pin.write_text(json.dumps(pin, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    for name, case in sorted(current["cases"].items()):
        status = "ok " if case["ok"] else "BAD"
        print(f"{status} {name:38s} {case['seconds']:8.3f}s  {case['fingerprint']}")
    print(f"    {'TOTAL':38s} {current['total_seconds']:8.3f}s")
    if args.baseline:
        baseline = json.loads(args.baseline.read_text(encoding="utf-8"))
        print()
        for line in diff(baseline, current):
            print(line)
    bad = [k for k, v in current["cases"].items() if not v["ok"]]
    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Does an underspecified workflow run the same tool twice, and what removes it?

    python research/metasmith/witness_sweep/duplicate_work.py --arm unpinned

`metagenomics_from_paired_reads` carries a lineage pin on every assembly-derived
target -- `{"type": ..., "parents": [0]}` -- and a comment saying what happens
without one: the planner answers different targets from different assemblers and
runs both legs. That is the duplicated-work case, in the shipped library, with
the remedy already applied. Stripping the pins is the reproduction; putting them
back one at a time is the measurement of which one was doing the work.

Nothing here edits the template. The target list is rebuilt in memory and handed
to `Spec.SolveViews`, so the library on disk is untouched.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

HERE = Path(__file__).resolve()
ROOT = HERE.parents[3]
MLIB = ROOT / "src" / "metasmith_libraries"
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(MLIB))

from metasmith.agents.spec import Spec  # noqa: E402
from metasmith.models.solver_backend import Backend  # noqa: E402
from metasmith.testing.solver_spec import check_spec  # noqa: E402
from metasmith.testing.solver_verification import (  # noqa: E402
    check_plan, plan_fingerprint, problem_of_plan,
)
from metasmith.testing.witness_check import solve_and_check  # noqa: E402

import metagenomics_from_paired_reads as TEMPLATE  # noqa: E402


def targets(arm: str) -> list:
    """The shipped target list, or a variant of it.

    Only the pins to target 0 -- the megahit assembly -- are variable. The
    per-binner pins are not a choice: three `taxonomy::checkm_stats` targets are
    distinguishable only by the binner each descends from, and `TargetBuilder`
    refuses a second target of one type with identical parents. So the
    underspecified workflow is the one whose assembly-derived targets name no
    assembly, which is exactly the ambiguity the template's comment describes.
    """
    shipped = TEMPLATE.TARGETS
    pinned_to_assembly = {
        i for i, t in enumerate(shipped)
        if isinstance(t, dict) and list(t.get("parents") or ()) == [0]
    }

    if arm == "shipped":
        keep = pinned_to_assembly
    elif arm == "unpinned":
        keep = set()
    elif arm == "orfs_only":
        keep = {i for i in pinned_to_assembly if shipped[i]["type"] == "sequences::orfs"}
    elif arm == "assembly_only":
        # Only the targets that are themselves a product of the assembler.
        products = {"sequences::orfs", "sequences::assembly_stats",
                    "sequences::assembly_per_contig_coverage",
                    "sequences::assembly_per_bp_coverage"}
        keep = {i for i in pinned_to_assembly if shipped[i]["type"] in products}
    elif arm == "minimal":
        # A short target set that still spans two assemblers. The full unpinned
        # list does reproduce the duplication, but its search does not fit in
        # 8 GB, and a case nobody can re-run is not a reproduction.
        return ["sequences::orfs",
                "annotation::kofamscan_results",
                "annotation::diamond_uniref50_results"]
    elif arm == "minimal_pinned":
        return [{"type": "sequences::orfs"},
                {"type": "annotation::kofamscan_results", "parents": [0]},
                {"type": "annotation::diamond_uniref50_results", "parents": [0]}]
    elif arm == "minimal_one_pin":
        # The narrowest pin: one of the two consumers anchored, the other free.
        # Whether that is enough is the practical question a workflow author has
        # -- it says whether the pin has to be repeated on every downstream
        # target or only on the ones that would otherwise diverge.
        return ["sequences::orfs",
                {"type": "annotation::kofamscan_results", "parents": [0]},
                "annotation::diamond_uniref50_results"]
    elif arm == "bins_only":
        keep = {i for i in pinned_to_assembly if "bin_fasta" in shipped[i]["type"]}
    else:
        raise SystemExit(f"unknown arm [{arm}]")

    out = []
    for i, t in enumerate(shipped):
        if i in pinned_to_assembly and i not in keep:
            out.append(t["type"])
        else:
            out.append(t)
    return out


def solve(arm: str, *, max_refine: int = 256):
    spec = TEMPLATE.build_spec()
    spec.target_types = targets(arm)
    data_lib = spec.input_library
    from metasmith.agents.spec import _as_data_lib, _as_transform_lib
    data_lib = _as_data_lib(data_lib)
    samples = list(data_lib.AsSamples(spec.sample_type))
    resources = [_as_data_lib(x) for x in spec.resource_libraries]
    return Spec.SolveViews(
        samples=samples,
        resources=resources,
        transforms=[_as_transform_lib(x) for x in spec.transform_libraries],
        targets=list(spec.target_types),
        max_refine=max_refine,
    )


def describe(task) -> dict:
    """What ran twice, and which downstream steps each copy fed."""
    steps = sorted(task.plan.steps, key=lambda s: s.order)
    names = {id(s): Path(s.transform._path).stem for s in steps}
    counts = Counter(names[id(s)] for s in steps)
    duplicated = {n: c for n, c in counts.items() if c > 1}

    producer_of: dict[int, object] = {}
    for s in steps:
        for g in s.produces:
            for inst in g:
                producer_of[id(inst)] = s
    consumers: dict[int, list] = defaultdict(list)
    for s in steps:
        for inst in s.uses:
            src = producer_of.get(id(inst))
            if src is not None:
                consumers[id(src)].append(names[id(s)])

    detail = {}
    for name in sorted(duplicated):
        copies = [s for s in steps if names[id(s)] == name]
        detail[name] = [
            {
                "order": s.order,
                "consumers": sorted(set(consumers[id(s)])),
                "inputs": sorted({i.dtype_name for i in s.uses}),
            }
            for s in copies
        ]
    return {
        "ok": bool(task.ok),
        "steps": len(steps),
        "dropped_targets": sorted(task.plan.dropped_targets),
        "duplicated": duplicated,
        "duplicate_detail": detail,
        "transform_counts": dict(sorted(counts.items())),
    }


def adjudicate(task, name: str) -> dict:
    result = getattr(task.plan, "_solver_result", None)
    problem = problem_of_plan(task.plan, name=name)
    if problem is None or result is None:
        return {"error": "the plan carries no solver inputs"}
    request, reply, verdict = solve_and_check(problem)
    reference = check_spec(request, reply)
    objects = check_plan(problem, result)
    return {
        "fingerprint": plan_fingerprint(result),
        "witness_ok": verdict.ok,
        "witness_complete": verdict.complete,
        "witness_clauses": verdict.clauses,
        "reference_ok": reference.ok,
        "reference_violations": list(reference.violations),
        "check_plan_ok": objects.ok,
        "check_plan_violations": list(objects.violations),
    }


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--arm", default="unpinned",
                    choices=["shipped", "unpinned", "orfs_only", "assembly_only", "bins_only",
                             "minimal", "minimal_pinned", "minimal_one_pin"])
    ap.add_argument("--refiner", action="store_true",
                    help="also solve at max_refine=0 and compare fingerprints")
    ap.add_argument("--out", type=Path, default=None)
    args = ap.parse_args()

    assert Backend("solve") == "rust", f"backend is {Backend('solve')}"
    task = solve(args.arm)
    doc = {"arm": args.arm, "backend": Backend("solve")}
    doc.update(describe(task))
    doc["adjudication"] = adjudicate(task, f"metagenomics/{args.arm}")

    if args.refiner:
        bare = solve(args.arm, max_refine=0)
        doc["refiner"] = {
            "max_refine_256": doc["adjudication"].get("fingerprint"),
            "max_refine_0": plan_fingerprint(bare.plan._solver_result),
            "steps_at_0": len(bare.plan.steps),
        }
        doc["refiner"]["same_plan"] = (
            doc["refiner"]["max_refine_0"] == doc["refiner"]["max_refine_256"]
        )

    text = json.dumps(doc, indent=2, sort_keys=True)
    if args.out is not None:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(text)
    print(text)


if __name__ == "__main__":
    main()

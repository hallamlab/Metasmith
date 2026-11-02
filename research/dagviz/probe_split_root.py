#!/usr/bin/env python3
"""Does a two-case E2 solve work once the two arms stop sharing a root?

Both arms declare their sample metadata as `e2::read_metadata`, and an Endpoint
is its property set, so the short and the long case intern to the SAME metadata
node -- and to the same `read_truth` node under it. Every lineage anchor that is
supposed to say "this sample" then says "either arm". This runs the solver twice
over the same two cases: once as the arms are declared, once with one property
added to the long arm's metadata so the two roots are distinct.
"""

import os
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, "/home/tony/agentic_workspace/projects/metasmith/engine/bench-run/research/metasmith_benchmark/drivers")
os.environ.setdefault("E2_CACHE_DIR", str(HERE / "e2cache"))

import _common as c  # noqa: E402
import e2_cami as e2  # noqa: E402
from metasmith.models.solver import Endpoint, Transform, solve_by_mcts  # noqa: E402
from metasmith.models.libraries.instances import DataInstanceLibraryView  # noqa: E402
from metasmith.models.workflow.plan import CollectSolverInputs  # noqa: E402
from metasmith.python_api import DataInstanceLibrary, TransformInstanceLibrary  # noqa: E402

ARM_TAG = '{"arm":"long"}'
TARGETS = os.environ.get("TARGETS", "e2::assembly").split(",")


def cases_and_transforms():
    arms = e2.enumerate_arms()
    cache = Path(os.environ["E2_CACHE_DIR"])
    groups = []
    for arm in ("short", "long"):
        smith = c.agent_for("cami", False, cache / f"dryrun_home_{arm}")
        inputs, globals_lib = e2.declare_givens(smith, arm, arms[arm][:1], ensure=False)
        res = [
            DataInstanceLibraryView(lib) for lib in (
                DataInstanceLibrary.Load(c.LIBRARY / "resources" / "e2"),
                DataInstanceLibrary.Load(c.MLIB / "resources" / "lib"),
                globals_lib,
            )
        ]
        groups += [[v] + res for v in inputs.AsSamples("e2::read_metadata")]
    trlib = TransformInstanceLibrary.Load(c.LIBRARY / "transforms" / "e2")
    _, cases, transform2inst, _ = CollectSolverInputs(groups, [trlib])
    return cases, list(transform2inst.keys()), trlib


def retag(case: set[Endpoint]) -> set[Endpoint]:
    """The same case with one property added to its root, and every endpoint
    under that root rebuilt so it hangs off the new one."""
    root = next(e for e in case if any("read_metadata" in p for p in e.properties))
    new_root = Endpoint(set(root.properties) | {ARM_TAG})
    out = {new_root}
    for e in case:
        if e is root:
            continue
        if root in e.parents:
            out.add(Endpoint(set(e.properties), parents=(e.parents - {root}) | {new_root}))
        else:
            out.add(e)
    return out


def target_model(trlib) -> Transform:
    t = Transform()
    for name in TARGETS:
        t.AddRequirement(example=trlib.GetType(name))
    return t


def attempt(label, cases, transforms, target):
    print(f"--- {label}: {len(cases)} cases, roots shared: "
          f"{len(cases[0] & cases[1]) if len(cases) == 2 else 'n/a'} ---")
    try:
        sol = solve_by_mcts(given=cases, target=target, transforms=transforms)
    except Exception as e:
        print("    REFUSED:")
        for line in str(e).splitlines()[-6:]:
            print("      ", line)
        return
    print(f"    complete={sol.complete}, {len(sol.dependency_plan)} applications")
    for app in sol.dependency_plan:
        print("      ", getattr(app.transform, "_name", None) or app.transform)


def main():
    cases, transforms, trlib = cases_and_transforms()
    target = target_model(trlib)
    assert len(cases) == 2, f"expected two cases, got {len(cases)}"
    attempt("as declared", cases, transforms, target)
    attempt("long arm retagged", [cases[0], retag(cases[1])], transforms, target)
    attempt("two short cases", [cases[0], retag(cases[0])], transforms, target)
    # A short sample with no CAMISIM truth is a real second case, not a
    # synthetic one: the long arm declares truth only where has_truth says so.
    no_truth = {e for e in cases[0] if not any("read_truth" in p for p in e.properties)}
    attempt("short, with and without truth", [cases[0], no_truth], transforms, target)
    attempt("short without truth alone", [no_truth], transforms, target)
    attempt("short alone", [cases[0]], transforms, target)
    attempt("long alone", [cases[1]], transforms, target)


if __name__ == "__main__":
    sys.exit(main())

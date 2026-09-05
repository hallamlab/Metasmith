#!/usr/bin/env python3
"""Run the plan witness over the real workflows this repository can reach.

    python research/metasmith/witness_sweep/run_sweep.py templates --out out.json

Each arm collects `(name, SolverProblem)` pairs from somewhere real, hands them
to `metasmith.testing.witness_sweep`, and writes one JSON document. The arms are
separate subcommands because their costs differ by two orders of magnitude and
because a failure in one should not cost the others.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve()
ROOT = HERE.parents[3]
sys.path.insert(0, str(ROOT / "src"))

from metasmith.testing.solver_verification import plan_fingerprint, problem_of_plan  # noqa: E402
from metasmith.testing.witness_sweep import (  # noqa: E402
    CaseResult, summarise, sweep_problem, templates_corpus,
)

MLIB = ROOT / "src" / "metasmith_libraries"


def _emit(results: list[CaseResult], out: Path | None) -> None:
    doc = summarise(results)
    text = json.dumps(doc, indent=2, sort_keys=True)
    if out is not None:
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(text)
    for r in results:
        line = f"  {r.verdict:>10}  {r.name}  steps={r.steps} {r.seconds}s"
        if r.error: line += f"  error={r.error}"
        if r.note: line += f"  note={r.note}"
        if r.clauses: line += f"  clauses={r.clauses}"
        print(line)
    print(json.dumps({k: v for k, v in doc.items() if k != "results"}, indent=2, sort_keys=True))


def arm_templates(args) -> list[CaseResult]:
    results = []
    for name, problem, fingerprint in templates_corpus(MLIB):
        print(f"[templates] {name}", flush=True)
        results.append(sweep_problem(name, problem, expect_fingerprint=fingerprint))
    return results


def _sweep_task(name: str, task, results: list[CaseResult]) -> None:
    """A driver hands back a WorkflowTask; the sweep wants the problem behind it."""
    result = getattr(task.plan, "_solver_result", None)
    problem = problem_of_plan(task.plan, name=name)
    if problem is None or result is None:
        r = CaseResult(name=name)
        r.error = "the plan carries no solver inputs, so the checker cannot see it"
        results.append(r)
        return
    results.append(sweep_problem(name, problem, expect_fingerprint=plan_fingerprint(result)))


def arm_fabfos(args) -> list[CaseResult]:
    """The three shipped fabfos pipelines, set up as their driver tests set them up."""
    import tempfile

    from metasmith.python_api import Runtime
    from fabfos.pipelines import annotation, assembly, ecspr

    results: list[CaseResult] = []
    with tempfile.TemporaryDirectory(prefix="msm-sweep-fabfos-") as td:
        work = Path(td)

        reads, host, pcc1 = work / "pool_0.fq.gz", work / "host.fna", work / "pcc1.fna"
        for f in (reads, host, pcc1): f.touch()
        print("[fabfos] assembly", flush=True)
        _, task, _ = assembly.generate_workflow(
            work / "assembly", experiment="sweep", reads=[reads], parity="paired",
            host=host, pcc1=pcc1, recovery_lib=None, runtime=Runtime.APPTAINER,
        )
        _sweep_task("fabfos/assembly", task, results)

        orfs = work / "orfs.faa"; orfs.touch()
        print("[fabfos] annotation", flush=True)
        _, task, _ = annotation.generate_workflow(
            work / "annotation", orfs=orfs, kofam_profiles=None, kofam_ko_list=None,
            uniref50_db=None, mnxr_lookup=None, landmarks=None, runtime=Runtime.APPTAINER,
        )
        _sweep_task("fabfos/annotation", task, results)

        units = []
        for nm in ("pool_a", "pool_b"):
            gpr, cond = work / f"gpr_{nm}.parquet", work / f"cond_{nm}.parquet"
            gpr.touch(); cond.touch()
            units.append(ecspr.Unit(name=nm, gpr_table=gpr, conditions=cond))
        print("[fabfos] ecspr", flush=True)
        _, task, _ = ecspr.generate_workflow(
            work / "ecspr", units=units, atom_pairs=None, direction_ratios=None,
            runtime=Runtime.APPTAINER,
        )
        _sweep_task("fabfos/ecspr", task, results)
    return results


def arm_blocked(args) -> list[CaseResult]:
    """The negative control: a driver that is not expected to solve at all.

    `dl_embeddings_from_orfs` is listed BLOCKED in build_templates.py because the
    only producer of `sequences::orfs_shard` is disabled. Failing to solve is a
    different outcome from a plan being refused, and the sweep has to be able to
    say which one it saw.
    """
    sys.path.insert(0, str(MLIB))
    import dl_embeddings_from_orfs as mod  # noqa: E402

    results: list[CaseResult] = []
    print("[blocked] dl_embeddings_from_orfs", flush=True)
    task = mod.build_spec().Solve()
    if task.ok:
        _sweep_task("blocked/dl_embeddings_from_orfs", task, results)
        return results
    r = CaseResult(name="blocked/dl_embeddings_from_orfs")
    r.solved = False
    r.steps = len(task.plan.steps)
    r.note = f"did not solve, as expected: dropped {sorted(task.plan.dropped_targets)}"
    results.append(r)
    return results


def arm_planbench(args) -> list[CaseResult]:
    """PlanBench Blocksworld, the one external problem set in the tree.

    122 instances the DVC pin materialises. They matter here because they are
    shaped by another planning community rather than by this project's own
    library, so an assumption baked into both the solver and the witness would
    have to be a very general one to survive them.
    """
    import signal

    bench = ROOT / "research" / "metasmith" / "solver_benchmarking"
    sys.path.insert(0, str(bench))
    from compare import SUBSETS, resolve_instance_dir  # noqa: E402
    from graph_expand import StateGraphTooLarge, build_solver_problem, expand_state_graph  # noqa: E402
    from pddl_ground import ground_instance  # noqa: E402

    instance_dir = resolve_instance_dir(
        ROOT / "data" / "metasmith" / "solver_benchmarks" / "blocksworld"
    )
    domain = instance_dir / "generated_domain.pddl"

    # The state graphs are cyclic, and the backward distance walk's runtime on a
    # cyclic graph depends on iteration order -- the same instance has run in
    # 0.08 s in one process and hung in another. A timeout is a first-class
    # outcome here, not defensive boilerplate.
    def _alarm(signum, frame): raise TimeoutError("solve exceeded the per-instance budget")
    signal.signal(signal.SIGALRM, _alarm)

    results: list[CaseResult] = []
    for label, subdir, _n in SUBSETS:
        files = sorted((instance_dir / subdir).glob("instance-*.pddl"),
                       key=lambda p: int(p.stem.split("-")[1]))
        if args.limit: files = files[:args.limit]
        for pf in files:
            name = f"planbench/{label}/{pf.stem}"
            try:
                graph = expand_state_graph(ground_instance(domain, pf))
                problem = build_solver_problem(graph)
            except StateGraphTooLarge as e:
                r = CaseResult(name=name); r.note = f"state graph too large: {e}"
                results.append(r); continue
            signal.alarm(args.budget)
            try:
                results.append(sweep_problem(name, problem))
            except TimeoutError as e:
                r = CaseResult(name=name); r.error = str(e); results.append(r)
            finally:
                signal.alarm(0)
        print(f"[planbench] {label}: {len(files)} instances", flush=True)
    return results


def arm_driver(args) -> list[CaseResult]:
    """Any author driver that exposes `NAME` and `build_spec()`.

    The one arm that reaches outside this branch. A driver on another branch is
    staged read-only into a scratch tree -- library and all -- and imported from
    there, so nothing here is ever checked out over the worktree.
    """
    import importlib.util

    path = Path(args.driver).resolve()
    assert path.exists(), f"no driver at {path}"
    sys.path.insert(0, str(path.parent))
    spec_ = importlib.util.spec_from_file_location(path.stem, path)
    mod = importlib.util.module_from_spec(spec_)
    spec_.loader.exec_module(mod)

    name = f"driver/{getattr(mod, 'NAME', path.stem)}"
    print(f"[driver] {name}", flush=True)
    results: list[CaseResult] = []
    task = mod.build_spec().Solve()
    if not task.ok:
        r = CaseResult(name=name)
        r.steps = len(task.plan.steps)
        r.note = f"did not solve: dropped {sorted(task.plan.dropped_targets)}"
        results.append(r)
        return results
    _sweep_task(name, task, results)
    return results


def arm_aspire(args) -> list[CaseResult]:
    """The ported ASPIRE amplicon pipeline, all seven of its declared cases.

    Plan-only, no cluster, and the only real driver in the tree that fans a
    fixed transform library out over a selectable target set -- which makes it
    seven real workflows for the price of one library load.
    """
    import importlib.util

    path = ROOT / "research" / "aspire" / "aspire_asv_pipeline.py"
    sys.path.insert(0, str(path.parent))
    spec_ = importlib.util.spec_from_file_location("aspire_asv_pipeline", path)
    mod = importlib.util.module_from_spec(spec_)
    spec_.loader.exec_module(mod)

    from metasmith.python_api import Spec

    results: list[CaseResult] = []
    on = dict(mod.DEFAULT_ON)
    for case in sorted(mod.CASES):
        name = f"aspire/{case}"
        print(f"[aspire] {case}", flush=True)
        targets = mod.targets_for(case, on)
        inputs = mod.build_inputs(mod.CACHE / f"aspire_{case}.xgdb", 3, on, False)
        task = Spec(
            input_library=inputs, target_types=targets,
            transform_libraries=mod.TRANSFORMS,
            resource_libraries=[mod.MLIB / "resources" / "env"],
            sample_type=None,
        ).Solve(max_iter=1024, max_refine=256, seed=42)
        if not task.ok:
            r = CaseResult(name=name)
            r.steps = len(task.plan.steps)
            r.note = f"did not solve: dropped {sorted(task.plan.dropped_targets)}"
            results.append(r)
            continue
        _sweep_task(name, task, results)
    return results


ARMS = {
    "templates": arm_templates, "fabfos": arm_fabfos, "aspire": arm_aspire,
    "blocked": arm_blocked, "planbench": arm_planbench, "driver": arm_driver,
}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("arm", choices=sorted(ARMS))
    ap.add_argument("--out", type=Path, default=None)
    ap.add_argument("--limit", type=int, default=None, help="first N instances per subset")
    ap.add_argument("--budget", type=int, default=60, help="per-case seconds before a timeout is recorded")
    ap.add_argument("--driver", type=str, default=None, help="the driver arm: path to a module with NAME and build_spec()")
    args = ap.parse_args()
    _emit(ARMS[args.arm](args), args.out)


if __name__ == "__main__":
    main()

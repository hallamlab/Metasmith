#!/usr/bin/env python3
"""Solve a subset of the viromics driver's targets and report what came back.

The survey is the largest target set in the repository, and its solve time grows
sharply rather than smoothly with the number of targets -- so "does it solve" is a
question you end up asking about a *subset*, repeatedly, while changing types. The
author driver only ever solves the whole set, which is the slowest way to learn
anything. This is the fast way.

    python research/viromics/probe_targets.py                 # the whole set
    python research/viromics/probe_targets.py --first 28      # a prefix
    python research/viromics/probe_targets.py --one-per-app   # see below
    python research/viromics/probe_targets.py --bisect        # find the wall

`--one-per-app` drops every target whose producing transform already answers an
earlier target. Sibling products come out of the same task either way -- the run
that writes checkv_contamination writes checkv_quality_summary -- so naming both
adds no coverage, and it does add a pair of slots the planner has to find one
consistent application for. This target set has eight such groups.

Run it with the rust solver staged (`src/workflow_solver/dev.sh --stage`); on the
python fallback every number here is ~15x larger and the wall arrives sooner.
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
MLIB = HERE.parents[1] / "src" / "metasmith_libraries"
sys.path.insert(0, str(MLIB))
sys.path.insert(0, str(HERE))

import viromics_survey_from_paired_reads as V                     # noqa: E402

# One representative target per producing transform. The rest are siblings of
# these, produced by the same task.
ONE_PER_APP = {
    "taxonomy::genomad_virus_summary", "taxonomy::genomad_taxonomy",
    "viromics::vibrant_amgs", "viromics::vibrant_genome_quality",
    "annotation::dramv_annotations", "viromics::checkv_contamination",
    "viromics::vcontact3_network", "annotation::kofamscan_descriptions",
    "viromics::host_prediction_genome", "viromics::cas_operons",
    "viromics::candidate_call_provenance",
}


def name_of(t) -> str:
    return t if isinstance(t, str) else t["type"]


def spec_for(targets):
    # The driver's own spec, with the target list swapped out -- so a probe
    # cannot drift from what the driver actually solves. `build_spec` reads
    # the module global when it is called, which is what makes this work.
    V.TARGETS = list(targets)
    return V.build_spec()


def solve(targets, max_iter, max_refine, seed):
    t0 = time.time()
    task = spec_for(targets).Solve(max_iter=max_iter, max_refine=max_refine, seed=seed)
    return task, time.time() - t0


def report(task, elapsed, targets):
    dropped = sorted(set(task.plan.dropped_targets))
    print(f"  {len(targets)} targets -> ok={task.ok} steps={len(task.plan.steps)} "
          f"dropped={len(dropped)} in {elapsed:.0f}s")
    if task.ok:
        for step in task.plan.steps:
            produced = sorted({i.dtype_name for g in step.produces for i in g})
            print(f"    {step.order:>2}. {Path(step.transform._path).stem:<28}"
                  f" -> {', '.join(produced)}")
    elif dropped:
        print(f"    dropped: {', '.join(dropped)}")


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--first", type=int, default=None, help="only the first N targets")
    p.add_argument("--one-per-app", action="store_true",
                   help="drop sibling products of an already-targeted transform")
    p.add_argument("--bisect", action="store_true",
                   help="grow the prefix one target at a time and stop at the first failure")
    p.add_argument("--from", dest="start", type=int, default=19,
                   help="--bisect starts here (default 19, the metagenomics half)")
    p.add_argument("--max-iter", type=int, default=1024)
    p.add_argument("--max-refine", type=int, default=256)
    p.add_argument("--seed", type=int, default=42)
    args = p.parse_args()

    targets = list(V.TARGETS)
    if args.one_per_app:
        targets = [t for t in targets if name_of(t) not in ONE_PER_APP]
    if args.first is not None:
        targets = targets[:args.first]

    if args.bisect:
        for n in range(args.start, len(targets) + 1):
            task, elapsed = solve(targets[:n], args.max_iter, args.max_refine, args.seed)
            print(f"{n:>3} (+{name_of(targets[n-1]):<44}) ok={task.ok} "
                  f"steps={len(task.plan.steps):<3} {elapsed:.0f}s", flush=True)
            if not task.ok:
                return 1
        return 0

    task, elapsed = solve(targets, args.max_iter, args.max_refine, args.seed)
    report(task, elapsed, targets)
    return 0 if task.ok else 1


if __name__ == "__main__":
    sys.exit(main())

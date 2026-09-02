#!/usr/bin/env python3
"""Author driver for the viromics survey, and the coverage report behind it.

Antonio's manual pipeline is 40 steps (`pipeline_steps.yml`); the library can
plan 14 of them today, and the 26 it cannot are what this file exists to make
visible. Run it with no arguments for the best-effort survey: solve what is
reachable, render the DAG, print the gap.

The module also carries the standard author interface -- NAME, DESCRIPTION,
build_spec -- so that the day the missing transforms land it moves to
src/metasmith_libraries/ and into build_templates.AUTHORS unchanged. It lives
here rather than there because A.author asserts a complete solve, and this
target set is deliberately the incomplete one.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import yaml

HERE = Path(__file__).resolve().parent
MLIB = HERE.parents[1] / "src" / "metasmith_libraries"
sys.path.insert(0, str(MLIB))

import _authoring as A                                       # noqa: E402
from metasmith.python_api import DEFERRED, Spec              # noqa: E402

NAME = "viromics_survey_from_paired_reads"
DESCRIPTION = """
The reachable half of Antonio's vOTU-centric viromics pipeline, from raw paired
short reads: QC and trimming, both assemblers, geNomad and VirSorter2 viral
calls, DRAM-v auxiliary metabolic genes, per-contig coverage, and crAssphage as
a faecal-source marker. vOTU clustering, CheckV and VIBRANT are not in it --
see pipeline_steps.yml.
"""

# Every viral target is pinned to the metaSPAdes assembly (index 0). Both
# assemblers extend sequences::assembly, so an unpinned target is satisfiable
# from either and the planner answers different targets from different
# assemblers -- running both legs, and paying for the ambiguity in search time.
# metaSPAdes is the pin because it is the pipeline's step 3; megahit stays a
# target of its own so step 4 is still in the plan, as the pipeline runs both.
_ASM = 0
TARGETS = [
    "sequences::spades_assembly",                                   # step 3
    "sequences::megahit_assembly",                                  # step 4
    "sequences::read_qc_stats",                                     # step 1
    {"type": "taxonomy::genomad_virus_summary",   "parents": [_ASM]},   # 7, 19
    {"type": "taxonomy::genomad_plasmid_summary", "parents": [_ASM]},
    {"type": "annotation::virsorter2_viral_sequences", "parents": [_ASM]},
    {"type": "annotation::virsorter2_scores",          "parents": [_ASM]},
    {"type": "annotation::dramv_distill",         "parents": [_ASM]},   # step 24
    {"type": "annotation::dramv_annotations",     "parents": [_ASM]},
    {"type": "sequences::assembly_per_contig_coverage", "parents": [_ASM]},  # 16
    {"type": "sequences::assembly_stats",         "parents": [_ASM]},
    "annotation::crassphage_coverage",
]


def build_spec(rebuild: bool = False) -> Spec:
    def inputs(lib):
        for tl in ("sequences.yml", "alignment.yml", "ref.yml",
                   "annotation.yml", "taxonomy.yml"):
            lib.AddTypeLibrary(A.TYPES / tl)
        meta = lib.AddValue("reads_metadata.json",
                            {"parity": "paired", "length_class": "short"},
                            "sequences::read_metadata")
        pair = lib.AddValue("read_pair.txt", "sample_1", "sequences::read_pair",
                            parents={meta})
        lib.AddItem(DEFERRED, "sequences::zipped_forward_short_reads", parents={pair})
        lib.AddItem(DEFERRED, "sequences::zipped_reverse_short_reads", parents={pair})

    return Spec(
        input_library=A.deferred_inputs(NAME, inputs, rebuild=rebuild),
        sample_type="sequences::read_metadata",
        target_types=TARGETS,
        transform_libraries=A.transforms(
            "logistics", "assembly", "metagenomics", "functionalAnnotation"),
        resource_libraries=[A.envs()],
    )


def coverage_report() -> str:
    steps = yaml.safe_load((HERE / "pipeline_steps.yml").read_text())["steps"]
    by_status = {"available": [], "substitute": [], "missing": []}
    lines, group = [], None
    for s in steps:
        by_status[s["status"]].append(s)
        if s["group"] != group:
            group = s["group"]
            lines.append(f"\n  {group}")
        mark = {"available": "++", "substitute": " ~", "missing": " ."}[s["status"]]
        lines.append(f"    {mark} {s['n']:>2}. {s['tool']:<46}"
                     f" {s.get('transform', '')}")
    counts = "  ".join(f"{k}={len(v)}" for k, v in by_status.items())
    header = f"Antonio's pipeline against the library ({len(steps)} steps): {counts}"
    legend = "  ++ same tool   ~ different tool, same contract   . nothing"
    return "\n".join([header] + lines + ["", legend, ""])


def survey(max_iter: int, max_refine: int, seed: int, render: bool) -> int:
    print(coverage_report())

    spec = build_spec()
    task = spec.Solve(max_iter=max_iter, max_refine=max_refine, seed=seed)
    plan = task.plan
    dropped = sorted(plan.dropped_targets)

    print(f"plan: {len(plan.steps)} steps, {len(TARGETS) - len(dropped)}"
          f"/{len(TARGETS)} targets reached")
    for step in plan.steps:
        produced = sorted({i.dtype_name for g in step.produces for i in g})
        print(f"  {step.order:>2}. {Path(step.transform._path).stem:<26}"
              f" -> {', '.join(produced)}")
    if dropped:
        print(f"\ndropped: {', '.join(dropped)}")
        for hint in plan.hints or []:
            print(f"  hint: {hint}")

    if render and plan.steps:
        out = HERE / "results" / NAME
        out.parent.mkdir(parents=True, exist_ok=True)
        for fmt in ("svg", "png"):
            print(f"dag: {plan.RenderDAG(str(out), format=fmt)}")
    return 0 if plan.steps else 1


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--author", action="store_true",
                   help="ship this as a template instead (asserts a complete solve)")
    p.add_argument("--rebuild", action="store_true",
                   help="discard and re-mint the deferred input library")
    p.add_argument("--no-dag", action="store_true", help="skip rendering")
    p.add_argument("--max-iter", type=int, default=1024)
    p.add_argument("--max-refine", type=int, default=256)
    p.add_argument("--seed", type=int, default=42)
    args = p.parse_args()

    if args.author:
        A.author(sys.modules[__name__], rebuild=args.rebuild, dag=not args.no_dag)
        sys.exit(0)
    sys.exit(survey(args.max_iter, args.max_refine, args.seed, not args.no_dag))

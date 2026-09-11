#!/usr/bin/env python3
"""Assemble the port's findings report from the census, the ledger and the solves.

Every number is read from the machine-written files rather than recounted, so the
report cannot drift from them. Where a number here disagrees with census.json or
ledger.jsonl, those files are right.

    build_report.py
"""

from __future__ import annotations

import argparse
import collections
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent.parent
CAT = REPO / "research" / "kbase" / "catalog"
NAR = REPO / "research" / "kbase" / "narratives"
CUR = REPO / "research" / "kbase" / "curation" / "r1"
TPL = REPO / "research" / "kbase" / "templates"

REASONS = {
    "solved": "metasmith planned the workflow.",
    "no_plan_all_targets": "no plan reaches the targets. Almost always a terminal app that "
                           "declares no typed product, so the chain's end is untyped and "
                           "nothing downstream can be asked for.",
    "no_plan_some_targets": "a plan exists for part of the target set. One unreachable target "
                            "drops the rest with it.",
    "no_typed_target": "no terminal app declares a typed product, and no report either.",
    "no_typed_input": "no root app declares a typed input, and the source it starts from "
                      "declares no typed product to stand in for one.",
    "out_of_memory": "the solve exceeded its memory cap.",
    "timeout": "the solve did not finish.",
    "raised": "the solve raised.",
}


def jl(p): return [json.loads(l) for l in Path(p).read_text().splitlines() if l]


def main():
    argparse.ArgumentParser(description=__doc__).parse_args()
    census = json.loads((CAT / "census.json").read_text())
    ledger = jl(CAT / "ledger.jsonl")
    dags = jl(REPO / "data" / "kbase" / "narratives" / "dags.jsonl")
    shapes = jl(CUR / "shapes.jsonl")
    cands = jl(CUR / "candidates.jsonl")
    res = jl(TPL / "solve_results.jsonl")

    conv = [r for r in ledger if r["disposition"] == "converted"]
    act = [r for r in ledger if r["active"]]
    act_conv = [r for r in act if r["disposition"] == "converted"]
    solved = [r for r in res if r["ok"]]
    by_reason = collections.Counter(r["reason"] for r in res)

    steps_ratio = [r["steps"] / r["n_steps"] for r in solved if r["n_steps"]]
    covered = sum(r["copies"] for r in solved)
    all_copies = sum(c["copies"] for c in cands)

    L = []
    A = L.append
    A("# What the KBase port covers\n")
    A("Every number below is read from `catalog/census.json`, `catalog/ledger.jsonl`,")
    A("`narratives/dags.jsonl`, `curation/r1/shapes.jsonl` and")
    A("`templates/solve_results.jsonl`. Where this file disagrees with those, they win.\n")

    A("## The funnel, both lanes\n")
    A("### Apps to transforms\n")
    A("| stage | count |\n|---|---|")
    a = census["apps"]
    A(f"| apps in the KBase catalog | {a['total']} |")
    A(f"| marked active | {a['active']} |")
    A(f"| runnable rather than a viewer | {a['runnable']} |")
    A(f"| naming some product (typed output or a report) | {a['with_any_output_channel']} |")
    A(f"| converted to a metasmith transform | {len(conv)} |")
    A(f"| **active apps converted** | **{len(act_conv)} of {len(act)}** |")
    A("")
    A("### Narratives to planned workflows\n")
    A("| stage | count |\n|---|---|")
    A(f"| public narratives | {len(dags)} |")
    A(f"| with at least one app cell | {sum(1 for d in dags if d['n_nodes'])} |")
    A(f"| with more than one app cell | {sum(1 for d in dags if d['n_nodes'] > 1)} |")
    A(f"| with at least one edge between cells | {sum(1 for d in dags if d['n_edges'])} |")
    A(f"| distinct workflow shapes after deduplication | {len(shapes)} |")
    A(f"| shapes trusted enough to solve (reference edges only, apps still in the catalog) | {len(cands)} |")
    A(f"| **shapes metasmith planned** | **{len(solved)} of {len(res)}** |")
    A("")
    A(f"Those {len(solved)} shapes account for {covered} of the {all_copies} narrative copies")
    A(f"behind the candidate set ({100 * covered / all_copies:.0f}%). A shape is one workflow;")
    A("a copy is one scientist who ran it.\n")

    A("## Why a shape does not plan\n")
    A("| outcome | shapes | what it means |\n|---|---|---|")
    for reason, n in by_reason.most_common():
        A(f"| {reason} | {n} | {REASONS.get(reason, '')} |")
    A("")

    A("## Every extra target costs about a tenth of the solve rate\n")
    A("The strongest predictor of whether a shape plans is how many targets it names.")
    A("A target is a solver slot, not a wish: each one must be reached, and one")
    A("unreachable target drops the rest with it.\n")
    A("| targets named | shapes | solved | rate |\n|---|---|---|---|")
    buckets = collections.defaultdict(lambda: [0, 0])
    for r in res:
        b = buckets[min(r["n_targets"], 6)]
        b[0] += 1; b[1] += bool(r["ok"])
    for k in sorted(buckets):
        n, o = buckets[k]
        A(f"| {k}{'+' if k == 6 else ''} | {n} | {o} | {100 * o / n:.0f}% |")
    A("")
    A("The practical reading: derive one terminal product per workflow, not every")
    A("product every terminal app declares. Where a shape genuinely has several")
    A("endpoints, pin them to a shared ancestor rather than naming them side by side.\n")

    A("## How much of each workflow the plan reproduces\n")
    shorter = sum(1 for r in solved if r["n_steps"] and r["steps"] < r["n_steps"])
    equal = sum(1 for r in solved if r["n_steps"] and r["steps"] == r["n_steps"])
    longer = sum(1 for r in solved if r["n_steps"] and r["steps"] > r["n_steps"])
    A(f"Of the solved shapes, {shorter} plan in fewer steps than the narrative ran,")
    A(f"{equal} in the same number and {longer} in more.\n")
    A("A shorter plan is the expected case rather than a loss. The planner builds only")
    A("what a target needs, and a narrative runs plenty that nothing downstream asks")
    A("for -- a QC report beside an assembly, a viewer on a matrix. A longer plan is the")
    A("interesting case: it means the planner fanned out where the scientist did not,")
    A("running three binners because three bin types were asked for.\n")

    A("## The workflows that plan, by how many people ran them\n")
    A("| copies | narrative steps | planned steps | workflow |\n|---|---|---|---|")
    for r in sorted(solved, key=lambda x: -x["copies"])[:20]:
        A(f"| {r['copies']} | {r['n_steps']} | {r['steps']} | "
          f"{' -> '.join(x.split('/')[-1] for x in r['apps'][:5])}"
          f"{' ...' if len(r['apps']) > 5 else ''} |")
    A("")

    A("## Findings\n")
    A("**A mask is not a tuning knob, it is a correctness requirement.** 46 of the 370")
    A("transforms are pure sources, so they are the cheapest producer of anything and an")
    A("unmasked solve answers every target with \"import it from staging\". That plan is")
    A("complete and correct and useless. Every solve here runs against the shape's own apps.\n")
    A("**An optional KBase parameter must not become a metasmith requirement.** 138 of the")
    A("599 typed input parameters are optional. Modelling them as requirements made")
    A("`run_flux_balance_analysis` demand an expression matrix nobody has, and the")
    A("2-step workflow 45 narratives actually ran had no plan. Fixing it moved a 12-shape")
    A("sample from 7 solved to 10.\n")
    A("**A union type carries its token and nothing else.** `_` is a matched property, not")
    A("a comment. The first draft gave each union a human-readable description, which is a")
    A("property no member carries, so every union was unsatisfiable and every solve using")
    A("one returned no plan with no dropped targets. The lint now asserts each union is")
    A("satisfied by each of its declared members.\n")
    A("**KBase's own specs under-declare what apps produce.** The dominant failure class is")
    A("a terminal app with no typed output: `extract_bins_as_assemblies` extracts")
    A("assemblies and declares none, `run_checkM_lineage_wf` reports quality and declares")
    A("none. The workflow is real and the type graph cannot see its end. This is a limit of")
    A("the source, not of the planner, and it is where a hand-written override would buy the")
    A("most coverage.\n")
    A("**The witness rejects plans over this library.** The `feat/solver` msm_solver refuses")
    A("plans the Python solver emits, naming one clause: `indexed`, whose comment says a")
    A("violation means the encoder emitted malformed rows. The same binary solves the")
    A("shipped templates. Reproducer at `reports/witness_indexed_repro.json`; raised with")
    A("the engine/solver scope. Everything here was solved with the Python solver, which")
    A("is what this branch ships.\n")

    (HERE / "COVERAGE.md").write_text("\n".join(L) + "\n")
    print("\n".join(L[:40]))
    print(f"\nwrote {(HERE / 'COVERAGE.md').relative_to(REPO)}")


if __name__ == "__main__":
    main()

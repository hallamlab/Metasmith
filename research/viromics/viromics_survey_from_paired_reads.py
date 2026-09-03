#!/usr/bin/env python3
"""Author driver for the viromics survey, and the coverage report behind it.

Antonio's manual pipeline is 40 steps (`pipeline_steps.yml`). This driver is an
offshoot of `metagenomics_from_paired_reads`, not a second pipeline: steps 1-4
and 26-40 are that template, and what is added here is the viral lane.

The lane's shape is one decision. The three callers -- geNomad, VirSorter2 and
VIBRANT -- each reduce their own output to a normalised interval table; those are
merged into ONE dereplicated candidate set, and that set is frozen. After it,
nothing writes a FASTA: lengths, clusters, CheckV trim suggestions, taxonomy,
function and host prediction are all tables keyed on its contig ids, and
Antonio's four filter scripts become a join over them. That is what makes a
different threshold or a different vOTU slice cost a groupby rather than a rerun.

Every viromics transform is currently a MOCK -- the model is real, the protocol
touches its outputs. This proves the types line up and the planner reaches every
target. It proves nothing about whether CheckV likes the input.

Run it with no arguments for the coverage report, the solve and the DAG. Run it
with --author to ship it as a template. It lives here rather than in the package
because `A.author` asserts a complete solve.
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
Antonio's vOTU-centric viromics survey from raw paired short reads, as one
template over both catalogues. The metagenomics half is QC, trimming, megahit,
mapping, three binners, dereplication, CheckM2 and GTDB-Tk. The viral half runs
geNomad, VirSorter2 and VIBRANT as callers, merges their calls into one frozen
candidate set, and then emits only tables over it: lengths, two MMseqs2
clusterings, CheckV, geNomad taxonomy and genes, DRAM-v, KOfam, vConTACT3, iPHoP
and CRISPR spacer matches. Filtering and OTU slicing happen after the DAG.
"""

# The target list is deliberately SHORT, and that is the least obvious thing in
# this file. A target is not a request for a file to exist -- it is a slot the
# planner has to satisfy consistently with every other slot, and this pipeline's
# outputs mostly arrive as dependencies of one another. Naming all forty of
# Antonio's outputs made the solve take minutes and then stop finding any plan;
# naming the nine that nothing else pulls in solves the same graph in a second.
# So: add a target only after checking that `probe_targets.py` does not already
# reach it, and re-run that probe after adding one.
#
# Two pins, for two different reasons, and one rule about which applies.
#
# `_ASM` is the pin metagenomics_from_paired_reads already carries: an unpinned
# `sequences::assembly` target is satisfiable from any assembler, so the planner
# answers different targets from different ones and runs both legs.
#
# `_FROZEN` names the frozen candidate set. Which pin a viral target takes is the
# design rule, not a preference:
#
#   cross-sample tools run on the frozen set, because pooling is the point of
#   them -- MMseqs2 *defines* a vOTU by clustering across samples, and CheckV,
#   vConTACT3, iPHoP and the spacer BLAST all want one catalogue.
#
#   per-contig annotators run once per sample and join to the frozen set through
#   viromics::candidate_call_provenance -- geNomad, VirSorter2, VIBRANT, DRAM-v.
#   Nothing about them is cross-sample: they report per contig, and a frozen
#   contig is an interval of a contig they already annotated.
#
# That split is also what makes this template solvable, and the reason is
# invisible from any one file. Lineage constraints are ANCESTRAL: the frozen set
# descends from every contig batch that fed the merge, so any requirement the
# frozen set also satisfies makes a caller's calls eligible to feed the merge
# that produced it. Measured: with geNomad reachable on the frozen set the solve
# took 43s; with VIBRANT reachable there too it stopped finding a plan at all.
# Hence the callers require sequences::contig_batch, and every consumer of the
# frozen set names viromics::dereplicated_candidate_virus outright.
_ASM = 0
_FROZEN = 1

TARGETS = [
    # The assembly is named so the per-sample targets below have something to pin
    # to; everything from reads to contig batches arrives as its dependency.
    "sequences::megahit_assembly",                                          # 0
    # The frozen set. Its own three candidate_virus slots put all three callers
    # in the DAG, and the merge drags the whole read -> assembly -> batch prefix
    # in behind them, so none of that needs naming.
    "viromics::dereplicated_candidate_virus",                               # 1

    # -- cross-sample tools, on the frozen set (steps 5, 6, 9, 10, 14, 20, 21, 23, 25)
    {"type": "viromics::contig_length_table", "parents": [_FROZEN]},        # 2
    {"type": "viromics::precluster_table", "parents": [_FROZEN]},           # 3
    {"type": "viromics::votu_cluster_table", "parents": [_FROZEN]},         # 4
    {"type": "viromics::checkv_contamination", "parents": [_FROZEN]},       # 5
    {"type": "viromics::vcontact3_network", "parents": [_FROZEN]},          # 6
    {"type": "annotation::kofamscan_descriptions", "parents": [_FROZEN]},   # 7
    # host_prediction pulls the whole MAG lane in behind it -- three binners,
    # the aggregator, skANI, CheckM2, GTDB-Tk and iphop_add_to_db -- and the
    # spacer BLAST adds CCTyper. That is why none of those are named here.
    {"type": "viromics::host_prediction_genome", "parents": [_FROZEN]},     # 8
    {"type": "viromics::spacer_host_links", "parents": [_FROZEN]},          # 9

    # -- per-sample work that nothing above reaches (steps 24, 40, contig taxonomy)
    {"type": "annotation::dramv_distill", "parents": [_ASM]},               # 10
    {"type": "taxonomy::metabuli", "parents": [_ASM]},                      # 11
    {"type": "annotation::dram_annotations", "parents": [_ASM]},            # 12
]


def build_spec(rebuild: bool = False) -> Spec:
    def inputs(lib):
        for tl in ("sequences.yml", "alignment.yml", "ref.yml", "annotation.yml",
                   "taxonomy.yml", "binning.yml", "binning_local.yml",
                   "viromics.yml"):
            lib.AddTypeLibrary(A.TYPES / tl)
        # The contentless root, modelled on pangenome::pangenome: it is what the
        # per-sample viral calls are grouped under, and declaring it shared is
        # what stops it fanning out per sample.
        study = lib.AddValue("contig_study.json", {"logistics": "contig study"},
                             "viromics::contig_study")
        meta = lib.AddValue("reads_metadata.json",
                            {"parity": "paired", "length_class": "short"},
                            "sequences::read_metadata", parents={study})
        pair = lib.AddValue("read_pair.txt", "sample_1", "sequences::read_pair",
                            parents={meta})
        lib.AddItem(DEFERRED, "sequences::zipped_forward_short_reads", parents={pair})
        lib.AddItem(DEFERRED, "sequences::zipped_reverse_short_reads", parents={pair})

    return Spec(
        input_library=A.deferred_inputs(NAME, inputs, rebuild=rebuild),
        sample_type="sequences::read_metadata",
        shared_input_paths=["contig_study.json"],
        target_types=TARGETS,
        transform_libraries=A.transforms(
            "logistics", "assembly", "metagenomics", "functionalAnnotation",
            "viromics"),
        resource_libraries=[A.envs()],
    )


def coverage_report() -> str:
    steps = yaml.safe_load((HERE / "pipeline_steps.yml").read_text())["steps"]
    by_status = {}
    lines, group = [], None
    for s in steps:
        by_status.setdefault(s["status"], []).append(s)
        if s["group"] != group:
            group = s["group"]
            lines.append(f"\n  {group}")
        mark = {"available": "++", "substitute": " ~", "mock": " M",
                "dropped": " x", "missing": " ."}.get(s["status"], " ?")
        lines.append(f"    {mark} {s['n']:>2}. {s['tool']:<46}"
                     f" {s.get('transform', '')}")
    counts = "  ".join(f"{k}={len(v)}" for k, v in sorted(by_status.items()))
    header = f"Antonio's pipeline against the library ({len(steps)} steps): {counts}"
    legend = ("  ++ same tool   ~ different tool, same contract"
              "   M mocked model, no tool yet   x not planned   . nothing")
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
    return 0 if not dropped and plan.steps else 1


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

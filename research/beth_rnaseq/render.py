#!/usr/bin/env python3
"""Solve the proposed isolate RNA-seq pipeline for Beth's TCAG run DAV36692 and render its DAG.

Real transforms come from the metasmith standard library; the gaps are the `isolate_rnaseq`
stubs under `mocklib/`. Every input is DEFERRED or a small value, so nothing here touches
reads: the plan is the deliverable.

`build.sh` compiles the mock library and then runs this.
"""

import os
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
SRC = Path(os.environ.get("MSM_SRC", HERE.parents[1] / "src"))
sys.path.insert(0, str(SRC))

from metasmith.models.dag_draw import Label
from metasmith.python_api import DEFERRED, DataInstanceLibrary, Spec, record_library

MLIB = SRC / "metasmith_libraries"
MOCK = HERE / "mocklib"

# TCAG run DAV36692 (NovaSeq X lane 5, 2026-08-19): library names as delivered. cu1..cu6 are
# copper doses (0, 1, 10, 50, 100, 300 ppm) with one library each. cb2a_cu1 and cu2 turned out
# to be Bacillus and are dropped, which leaves CB2A one library and nothing to compare, so it
# gets its own plan.

# The transcriptomics root also holds the eukaryotic STAR -> StringTie route to a
# gene_count_table. Pinning the count-derived targets under the sample-sheet table (index 0)
# keeps the planner from answering any of them through that route instead.
PLANS = {
    "epi300": dict(
        organism="Escherichia coli EPI300",
        samples=[f"epi300_cu{i}" for i in range(1, 7)],
        sample_sheet=True,
        targets=[
            "transcriptomics::gene_count_table",
            "isolate_rnaseq::qc_report",
            {"type": "isolate_rnaseq::dose_trend_results", "parents": [0]},
            {"type": "transcriptomics::expression_clusters", "parents": [0]},
            "isolate_rnaseq::metal_gene_panel",
        ],
    ),
    "cb2a": dict(
        organism="Caulobacter vibrioides CB2A JS4038",
        samples=["cb2a_cu3"],
        sample_sheet=False,
        targets=[
            "isolate_rnaseq::qc_report",
            "isolate_rnaseq::tpm_table",
            "annotation::bacmet_diamond_results",
        ],
    ),
}


def inputs(plan: dict) -> DataInstanceLibrary:
    lib = DataInstanceLibrary(Path(tempfile.mkdtemp(prefix="beth-rnaseq-")))
    for tl in ("sequences.yml", "transcriptomics.yml", "annotation.yml", "alignment.yml"):
        lib.AddTypeLibrary(MLIB / "data_types" / tl)
    lib.AddTypeLibrary(MOCK / "data_types" / "isolate_rnaseq.yml")

    exp = lib.AddValue("experiment.txt", f"{plan['organism']} RNA-seq", "transcriptomics::experiment")
    lib.AddItem(DEFERRED, "sequences::isolate_assembly", parents={exp})
    if plan["sample_sheet"]:
        lib.AddItem(DEFERRED, "isolate_rnaseq::sample_sheet", parents={exp})
        lib.AddValue("clustering_params.json", {"method": "hierarchical", "k": None},
                     "transcriptomics::clustering_params")
    for s in plan["samples"]:
        meta = lib.AddValue(f"read_metadata_{s}.json",
                            {"sample": s, "parity": "paired", "length_class": "short"},
                            "sequences::read_metadata", parents={exp})
        pair = lib.AddValue(f"read_pair_{s}.txt", s, "sequences::read_pair", parents={meta})
        lib.AddItem(DEFERRED, "sequences::zipped_forward_short_reads", parents={pair})
        lib.AddItem(DEFERRED, "sequences::zipped_reverse_short_reads", parents={pair})
    return record_library(lib)


def solve_and_render(plan: dict, out: Path) -> None:
    spec = Spec(
        input_library=inputs(plan),
        target_types=plan["targets"],
        transform_libraries=[MLIB / "transforms" / n
                             for n in ("logistics", "kbase", "transcriptomics", "functionalAnnotation", "metagenomics")]
                            + [MOCK / "transforms" / "isolate_rnaseq"],
        resource_libraries=[MLIB / "resources" / "env", MLIB / "resources" / "lib"],
    )
    task = spec.Solve()
    print(f"{out.stem}: {len(task.plan.steps)} steps, ok={task.ok}, dropped={sorted(task.plan.dropped_targets)}")
    for step in task.plan.steps:
        uses = sorted({u.dtype_name for u in step.uses if not u.dtype_name.startswith(("env::", "lib::"))})
        print(f"  {step.transform.name:<32} <- {', '.join(uses)}")
    dag = task.plan.BuildDAG(given_root=False, target_sink=False)
    # Tag each transform with the library it came from, so real and mock steps read apart.
    # BuildDAG names a transform node "<order> <name>"; DagRenderer has no public relabel.
    for step in task.plan.steps:
        root = Path(step.transform_library.location).name
        dag._labels[f"{step.order} {step.transform.name}"] = Label(
            name=step.transform.name, namespace="MOCK" if root == "isolate_rnaseq" else root)
    out.parent.mkdir(parents=True, exist_ok=True)
    # render() reads a dotted basename's suffix as the format, so pass the stem.
    for fmt in ("svg", "png"):
        print(dag.render(out, fmt))


def main(results: Path) -> None:
    for code, plan in PLANS.items():
        solve_and_render(plan, results / f"beth_rnaseq_dag_{code}")


if __name__ == "__main__":
    main(Path(sys.argv[1]) if len(sys.argv) > 1 else HERE / "results")

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

# TCAG run DAV36692 (NovaSeq X lane 5, 2026-08-19): library names as delivered. Each organism is
# its own experiment, and its genome assembly is a given under it, so its reads only ever meet
# its own genome.
ORGANISMS = {
    "EPI300": ("Escherichia coli EPI300", [f"epi300_cu{i}" for i in range(1, 7)]),
    "CB2A": ("Caulobacter vibrioides CB2A", [f"cb2a_cu{i}" for i in range(1, 4)]),
}

# The transcriptomics root also holds the eukaryotic STAR -> StringTie route to a
# gene_count_table. Pinning the count-derived targets under the sample-sheet table (index 0)
# keeps the planner from answering any of them through that route instead.
TARGETS = [
    "transcriptomics::gene_count_table",
    "isolate_rnaseq::qc_report",
    {"type": "transcriptomics::volcano_plot", "parents": [0]},
    {"type": "transcriptomics::expression_clusters", "parents": [0]},
    "isolate_rnaseq::metal_gene_panel",
]


def inputs() -> DataInstanceLibrary:
    lib = DataInstanceLibrary(Path(tempfile.mkdtemp(prefix="beth-rnaseq-")))
    for tl in ("sequences.yml", "transcriptomics.yml", "annotation.yml", "alignment.yml"):
        lib.AddTypeLibrary(MLIB / "data_types" / tl)
    lib.AddTypeLibrary(MOCK / "data_types" / "isolate_rnaseq.yml")

    for code, (name, samples) in ORGANISMS.items():
        exp = lib.AddValue(f"experiment_{code}.txt", f"{name} RNA-seq", "transcriptomics::experiment")
        lib.AddItem(DEFERRED, "sequences::isolate_assembly", parents={exp})
        lib.AddItem(DEFERRED, "isolate_rnaseq::sample_sheet", parents={exp})
        for s in samples:
            meta = lib.AddValue(f"read_metadata_{s}.json",
                                {"sample": s, "parity": "paired", "length_class": "short"},
                                "sequences::read_metadata", parents={exp})
            pair = lib.AddValue(f"read_pair_{s}.txt", s, "sequences::read_pair", parents={meta})
            lib.AddItem(DEFERRED, "sequences::zipped_forward_short_reads", parents={pair})
            lib.AddItem(DEFERRED, "sequences::zipped_reverse_short_reads", parents={pair})

    lib.AddValue("clustering_params.json", {"method": "hierarchical", "k": None},
                 "transcriptomics::clustering_params")
    return record_library(lib)


def main(out: Path) -> None:
    spec = Spec(
        input_library=inputs(),
        target_types=TARGETS,
        transform_libraries=[MLIB / "transforms" / n
                             for n in ("logistics", "kbase", "transcriptomics", "functionalAnnotation", "metagenomics")]
                            + [MOCK / "transforms" / "isolate_rnaseq"],
        resource_libraries=[MLIB / "resources" / "env", MLIB / "resources" / "lib"],
    )
    task = spec.Solve()
    print(f"{len(task.plan.steps)} steps, ok={task.ok}, dropped={sorted(task.plan.dropped_targets)}")
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
        print(dag.render(out.with_suffix(""), fmt))


if __name__ == "__main__":
    main(Path(sys.argv[1]) if len(sys.argv) > 1 else HERE / "results" / "beth_rnaseq_dag.svg")

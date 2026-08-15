#!/usr/bin/env python3
"""Regenerate `stress_dag.json` from the spanish-lakes metagenomics recipe.

HISTORICAL, as of the monorepo migration, and left as written on purpose. The
two paths below name branches of the `metasmith-libraries` project — a
`spanish-lakes-metagenomics` transform library and a `phyloflash` read set —
that were never ported here and exist only in that archived repository and its
bundle under `data/archive/repo-bundles/`. Neither path resolves on this
machine any more, so this script does not run as-is.

That is the honest state, and better than the alternative: repointing MLIB at
this repo's `src/metasmith_libraries` would still execute, and would quietly
regenerate a DIFFERENT DAG than the committed `stress_dag.json` — a fixture
whose whole value is being a fixed, large, real plan. To regenerate it, restore
those two branches from the bundle first.

Run by hand, never by the suite: planning this needs four transform libraries,
a resource library and a solver run, none of which belong on the test path. The
fixture it writes is just node/edge lists, so everything downstream reads plain
JSON and touches no disk beyond it.

    python tests/metasmith/fixtures/generate_stress_dag.py \
        tests/metasmith/fixtures/stress_dag.json

It writes to a path rather than stdout because metasmith's logger prints the
planner's resolution trace there.

The recipe is mirrored here rather than imported: upstream it pins `sys.path`
at a different metasmith worktree and renders at import time.
"""
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve()
sys.path.insert(0, str(HERE.parents[3] / "src"))

MLIB = Path(
    "/home/tony/agentic_workspace/projects/metasmith-libraries"
    "/spanish-lakes-metagenomics"
)
READS = Path(
    "/home/tony/agentic_workspace/projects/metasmith-libraries"
    "/phyloflash/tests/test_data"
)
OUT_DIR = Path("/tmp/metasmith_stress_dag")

from metasmith.python_api import (  # noqa: E402
    Agent, Runtime, DataInstanceLibrary, Source, TargetBuilder,
    TransformInstanceLibrary,
)


def main() -> int:
    inputs = DataInstanceLibrary(OUT_DIR.resolve() / "inputs.xgdb")
    for tl in ["sequences.yml", "alignment.yml", "ref.yml", "annotation.yml",
               "taxonomy.yml", "binning.yml", "binning_local.yml"]:
        inputs.AddTypeLibrary(MLIB / "data_types" / tl)

    meta = inputs.AddValue("reads_metadata.json",
                           {"parity": "paired", "length_class": "short"},
                           "sequences::read_metadata")
    pair = inputs.AddValue("read_pair.txt", "sample_1", "sequences::read_pair",
                           parents={meta})
    inputs.AddItem((READS / "small_reads_R1.fq.gz").resolve(),
                   "sequences::zipped_forward_short_reads", parents={pair})
    inputs.AddItem((READS / "small_reads_R2.fq.gz").resolve(),
                   "sequences::zipped_reverse_short_reads", parents={pair})
    inputs.Save()

    smith = Agent(home=Source.FromLocal(OUT_DIR.resolve() / "msm_home"),
                  runtime=Runtime.DOCKER)

    targets = TargetBuilder()
    for t in ["sequences::read_qc_stats", "sequences::orfs",
              "sequences::assembly_stats",
              "sequences::assembly_per_contig_coverage",
              "sequences::assembly_per_bp_coverage",
              "annotation::diamond_uniref50_results",
              "annotation::kofamscan_results", "taxonomy::metabuli",
              "taxonomy::phyloflash_summary", "binning_local::cluster_table"]:
        targets.Add(t)

    # distinct parents are what force a separate checkm + gtdbtk per binner
    # instead of the planner satisfying all three from one of them
    bins = [targets.Add(f"sequences::{b}_bin_fasta")
            for b in ("metabat2", "semibin2", "comebin")]
    for parent in bins:
        targets.Add("taxonomy::checkm_stats", parents={parent})
        targets.Add("taxonomy::gtdbtk", parents={parent})
    for b in ("metabat2", "semibin2", "comebin"):
        targets.Add(f"binning::{b}_contig_to_bin_table")

    task = smith.GenerateWorkflow(
        samples=list(inputs.AsSamples("sequences::read_metadata")),
        resources=[DataInstanceLibrary.Load(MLIB / "resources" / "containers"),
                   inputs],
        transforms=[
            TransformInstanceLibrary.Load(MLIB / "transforms" / lib)
            for lib in ("logistics", "assembly", "metagenomics",
                        "functionalAnnotation")
        ],
        targets=targets,
    )
    # never print the task or its plan: the repr carries the whole MCTS tree
    if not task.ok:
        print("planning failed", file=sys.stderr)
        return 1

    r = task.plan.BuildDAG()
    labels = r.labels
    out = Path(sys.argv[1]) if len(sys.argv) > 1 else HERE.parent / "stress_dag.json"
    out.write_text(json.dumps({
        "source": "spanish-lakes-metagenomics/main/metag_workflow_from_reads.py",
        "nodes": [
            {"id": n, "kind": k.name,
             "label": {"name": labels[n].name,
                       "namespace": labels[n].namespace,
                       "full": labels[n].full}}
            for n, k in r._nodes.items()
        ],
        "edges": [list(e) for e in r._edges],
    }, indent=1) + "\n")
    print(f"wrote {out}: {len(r._nodes)} nodes, {len(r._edges)} edges",
          file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

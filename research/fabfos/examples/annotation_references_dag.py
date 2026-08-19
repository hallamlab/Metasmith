from __future__ import annotations

import sys
import tempfile
from pathlib import Path

from metasmith.python_api import (
    Agent,
    Source,
    DataInstanceLibrary,
    TransformInstanceLibrary,
    TargetBuilder,
    Runtime,
)

REPO = Path(__file__).resolve().parents[3]
MLIB = REPO / "src" / "metasmith_libraries"
BREF = REPO / "src" / "fabfos" / "build_references"
ARTIFACTS = REPO / "tests" / "fabfos" / "artifacts"

TARGETS = [
    ("R3", "ref::kofamscan_profiles"),
    ("R3", "ref::kofamscan_ko_list"),
    ("R4", "ref::uniref50_diamond_db"),
    ("R5", "ref::mnxr_lookup"),
    ("R7", "ref::reference_label_pool"),
    ("R8", "ref::esm_c_600m_weights"),
    ("R10", "ref::reference_label_pool_esmc"),
]

DUPLICATE_PRODUCERS = ("downloadKofamscanDB", "downloadUniref50", "downloadEsmC")

EXPECTED = {
    "metanetx", "kegg", "rhea", "kofam", "uniref", "swissprot", "esm_c",
    "kofam_ref", "uniref50_dmnd", "mnxr_lookup", "reference_label_pool",
    "esm_c_weights", "reference_label_pool_esmc",
}


def plan(work: Path):
    inputs = DataInstanceLibrary(work / "inputs.xgdb")
    for tl in ("ncbi.yml", "sequences.yml", "annotation.yml", "ref.yml", "lib.yml"):
        inputs.AddTypeLibrary(MLIB / "data_types" / tl)
    for tl in ("fabfos_data.yml", "raw.yml", "interm.yml", "bench.yml", "buildlib.yml"):
        inputs.AddTypeLibrary(BREF / "data_types" / tl)
    inputs.Save()

    resources = [
        DataInstanceLibrary.Load(MLIB / "resources" / "env"),
        DataInstanceLibrary.Load(MLIB / "resources" / "lib"),
        DataInstanceLibrary.Load(BREF / "resources" / "buildlib"),
        inputs,
    ]
    transforms = [
        TransformInstanceLibrary.Load(BREF / "transforms" / "acquire"),
        TransformInstanceLibrary.Load(BREF / "transforms" / "compile"),
    ]

    targets = TargetBuilder()
    for _id, dtype in TARGETS:
        targets.Add(dtype)

    agent = Agent(home=Source.FromLocal(work / "agent_home"),
                  runtime=Runtime.APPTAINER)
    return agent.GenerateWorkflow(
        samples=[inputs],
        resources=resources,
        transforms=transforms,
        targets=targets,
    )


def main() -> int:
    with tempfile.TemporaryDirectory() as td:
        task = plan(Path(td))
        if not task.ok:
            print(f"FAILED to plan:\n{task.plan}")
            return 1

        used = {Path(s.transform._path).stem for s in task.plan.steps}
        print(f"Plan OK -- {len(task.plan.steps)} steps, {len(used)} distinct transforms\n")
        for step in sorted(task.plan.steps, key=lambda s: s.order):
            prods = [i.dtype_name for g in step.produces for i in g]
            print(f"  {step.order:>3}  {Path(step.transform._path).stem:<24} -> {prods}")

        dups = set(DUPLICATE_PRODUCERS) & used
        if dups:
            print(f"\nDUPLICATE REFERENCE PRODUCER(S) in the plan: {sorted(dups)}. "
                  f"logistics/ produces the same ref:: types compile/ does, so which "
                  f"one built a reference would be a planner tiebreak.")
        missing = (EXPECTED - used) | dups
        extra = used - EXPECTED
        print()
        if missing:
            print(f"MISSING expected transforms: {sorted(missing)}")
        if extra:
            print(f"note -- transforms used but not in EXPECTED: {sorted(extra)}")
        if not missing:
            print("every expected transform is in the plan")

        ARTIFACTS.mkdir(parents=True, exist_ok=True)
        svg = ARTIFACTS / "annotation_references_dag.svg"
        task.plan.RenderDAG(svg, blacklist_namespaces={"lib", "env", "buildlib"})
        print(f"\nDAG -> {svg}")
        return 1 if missing else 0


if __name__ == "__main__":
    sys.exit(main())

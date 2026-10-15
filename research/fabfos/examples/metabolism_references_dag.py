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
from metasmith.python_api import record_library

REPO = Path(__file__).resolve().parents[3]
MLIB = REPO / "src" / "metasmith_libraries"
BREF = REPO / "src" / "fabfos" / "build_references"
ARTIFACTS = REPO / "tests" / "fabfos" / "artifacts"

GIVEN_TYPE = "fabfos_data::metacyc"
GIVEN_AT = REPO / "data" / "fabfos" / "originals" / "metacyc"

RUN_GIVENS = {
    "fabfos_data::prior_bake_logs":
        REPO / "data" / "fabfos" / "processed" / "metabolism_bake" / "logs",
    "fabfos_data::aam_cache":
        REPO / "data" / "fabfos" / "temp" / "aam_cache",
}

TARGETS = [
    ("R6", "ref::atom_pairs"),
    ("R6", "ref::metabolism_vocab"),
    ("R6", "ref::direction_ratios"),
]

DUPLICATE_PRODUCERS = ("downloadKofamscanDB", "downloadUniref50", "downloadEsmC")

EXPECTED = {
    "metanetx", "equilibrator", "chebi", "modelseed",
    "mnx_lookups",
    "aam_worklist",
    "aam_recount", "aam_blockers", "aam_nametwin",
    "aam_rescue",
    "aam_algebra", "aam_forecast", "aam_partial", "aam_universe",
    "rxnmapper", "localmapper", "indigo",
    "aam_stack", "aam_redox", "aam_reference",
    "dgbyg", "direction_ensemble", "direction_bake",
}

RUNTIME = Runtime.APPTAINER
AGENT_ENV = "msm-fabfos"
AGENT_IMAGE = "docker://quay.io/hallamlab/metasmith:0.15.1"


def plan(work: Path):
    inputs = DataInstanceLibrary(work / "inputs.xgdb")
    for tl in ("ncbi.yml", "sequences.yml", "annotation.yml", "ref.yml", "lib.yml"):
        inputs.AddTypeLibrary(MLIB / "data_types" / tl)
    for tl in ("fabfos_data.yml", "raw.yml", "interm.yml", "bench.yml", "buildlib.yml",
               "lookup.yml", "evidence.yml"):
        inputs.AddTypeLibrary(BREF / "data_types" / tl)

    given = GIVEN_AT
    if not given.exists():
        given = work / "metacyc_standin"
        given.mkdir(parents=True, exist_ok=True)
        print(f"NOTE: no MetaCyc drop-in at {GIVEN_AT}; standing in an empty directory "
              f"so the plan can resolve. A run refuses where the .dat is read.\n")
    inputs.AddItem(given, GIVEN_TYPE)
    for dtype, at in RUN_GIVENS.items():
        if not at.exists():
            at = work / dtype.replace("::", "_")
            at.mkdir(parents=True, exist_ok=True)
        inputs.AddItem(at, dtype)
    # Synthetic placeholders, authored here and used nowhere else, so
    # writing them down is the whole of their record.
    inputs = record_library(inputs)

    resources = [
        DataInstanceLibrary.Load(MLIB / "resources" / "env"),
        DataInstanceLibrary.Load(MLIB / "resources" / "lib"),
        DataInstanceLibrary.Load(BREF / "resources" / "buildlib"),
        inputs,
    ]
    transforms = [
        TransformInstanceLibrary.Load(BREF / "transforms" / "acquire"),
        TransformInstanceLibrary.Load(BREF / "transforms" / "compile"),
        TransformInstanceLibrary.Load(BREF / "transforms" / "bake"),
    ]

    targets = TargetBuilder()
    for _id, dtype in TARGETS:
        targets.Add(dtype)

    agent = Agent(home=Source.FromLocal(work / "agent_home"),
                  runtime=RUNTIME,
                  container=AGENT_ENV if RUNTIME == Runtime.MAMBA else AGENT_IMAGE)
    return inputs, agent.GenerateWorkflow(
        samples=list(inputs.AsSamples(GIVEN_TYPE)),
        resources=resources,
        transforms=transforms,
        targets=targets,
    )


def main() -> int:
    with tempfile.TemporaryDirectory() as td:
        inputs, task = plan(Path(td))
        if not task.ok:
            print(f"FAILED to plan:\n{task.plan}")
            return 1

        used = {Path(s.transform._path).stem for s in task.plan.steps}
        print(f"Plan OK -- {len(task.plan.steps)} steps, {len(used)} distinct transforms\n")
        for step in sorted(task.plan.steps, key=lambda s: s.order):
            prods = [i.dtype_name for g in step.produces for i in g]
            print(f"  {step.order:>3}  {Path(step.transform._path).stem:<20} -> {prods}")

        staged = [(path, name) for path, name, _ in inputs.Iterate()]
        print(f"\ngiven: {len(staged)} staged input(s) -- "
              f"{sorted({n for _, n in staged})}")

        problems = []
        upstream = [n for _, n in staged if n not in RUN_GIVENS]
        if upstream != [GIVEN_TYPE]:
            problems.append(
                f"expected exactly one UPSTREAM given, found {sorted(upstream)}. "
                f"Everything but the licensed MetaCyc drop-in is fetchable, so a second "
                f"one means something with a transform behind it is being handed in "
                f"instead. The run-state givens ({sorted(RUN_GIVENS)}) are counted apart "
                f"-- neither is upstream data and neither can have a producer.")
        dups = set(DUPLICATE_PRODUCERS) & used
        if dups:
            print(f"\nDUPLICATE REFERENCE PRODUCER(S) in the plan: {sorted(dups)}. "
                  f"logistics/ produces the same ref:: types compile/ does, so which "
                  f"one built a reference would be a planner tiebreak.")
        missing = (EXPECTED - used) | dups
        if missing:
            problems.append(f"MISSING expected transforms: {sorted(missing)}")
        extra = used - EXPECTED
        if extra:
            print(f"note -- transforms used but not in EXPECTED: {sorted(extra)}")

        print()
        for p in problems:
            print(f"FAIL: {p}")
        if not problems:
            print("every expected transform is in the plan, and MetaCyc is the only "
                  "upstream given")

        ARTIFACTS.mkdir(parents=True, exist_ok=True)
        svg = ARTIFACTS / "metabolism_references_dag.svg"
        task.plan.RenderDAG(svg, blacklist_namespaces={"lib", "env", "buildlib"})
        print(f"\nDAG -> {svg}")
        return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())

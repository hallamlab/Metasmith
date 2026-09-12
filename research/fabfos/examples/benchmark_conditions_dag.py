from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "src"))

from metasmith.python_api import (                                    # noqa: E402
    Agent,
    Source,
    DataInstanceLibrary,
    TransformInstanceLibrary,
    TargetBuilder,
    Runtime,
)
from metasmith.python_api import record_library

MLIB = REPO / "src" / "metasmith_libraries"
BREF = REPO / "src" / "fabfos" / "build_references"
DATA = REPO / "data" / "fabfos"
ARTIFACTS = REPO / "tests" / "fabfos" / "artifacts"
WORK = DATA / "scratch" / "benchmark_conditions_dag"

AGENT_ENV = "msm-fabfos"

STAGED = {
    "bench::study_extractions": DATA / "benchmarks" / "_extractions",
    "raw::het_screen_records":  DATA / "originals" / "benchmarks" / "het_screen",
    "ref::mnxr_lookup":         DATA / "processed" / "mnxr_lookup" / "mnxr_lookup.parquet",
    "raw::laser_records":       DATA / "originals" / "benchmarks" / "laser",
    "fabfos_data::metanetx":    DATA / "originals" / "metanetx",
    "ref::gpr_table_gem":       DATA / "benchmarks",
}

EXPECTED = ("condition_gpr", "conditions")


def main() -> int:
    WORK.mkdir(parents=True, exist_ok=True)
    inputs = DataInstanceLibrary(WORK / "inputs.xgdb")
    for tl in ("ncbi.yml", "sequences.yml", "annotation.yml", "ref.yml", "lib.yml"):
        inputs.AddTypeLibrary(MLIB / "data_types" / tl)
    for tl in ("fabfos_data.yml", "raw.yml", "interm.yml", "bench.yml", "buildlib.yml",
               "lookup.yml", "evidence.yml"):
        inputs.AddTypeLibrary(BREF / "data_types" / tl)

    missing = []
    for dtype, at in STAGED.items():
        if not at.exists():
            missing.append((dtype, at))
            at = WORK / dtype.replace("::", "_")
            if Path(STAGED[dtype]).suffix:
                at.parent.mkdir(parents=True, exist_ok=True)
                at.touch()
            else:
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
    transforms = [TransformInstanceLibrary.Load(BREF / "transforms" / "benchmark")]

    targets = TargetBuilder()
    targets.Add("bench::conditions")

    agent = Agent(home=Source.FromLocal(WORK / "agent_home"),
                  runtime=Runtime.MAMBA, container=AGENT_ENV)
    task = agent.GenerateWorkflow(
        samples=[inputs],
        resources=resources, transforms=transforms, targets=targets)

    if not task.ok:
        print(f"FAILED to plan:\n{task.plan}")
        return 1

    used = {Path(s.transform._path).stem for s in task.plan.steps}
    print(f"Plan OK -- {len(task.plan.steps)} step(s)\n")
    for step in sorted(task.plan.steps, key=lambda s: s.order):
        prods = [i.dtype_name for g in step.produces for i in g]
        print(f"  {step.order:>3}  {Path(step.transform._path).stem:<20} -> {prods}")

    if missing:
        print("\nstaged from a stand-in (the real bytes are not on this machine):")
        for dtype, at in missing:
            print(f"  {dtype:<28} {at.relative_to(REPO)}")

    absent = [t for t in EXPECTED if t not in used]
    print()
    if absent:
        print(f"MISSING from the plan: {absent}")
        return 1
    print("both conditions steps are in the plan")

    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    dag = ARTIFACTS / "benchmark_conditions_dag.svg"
    task.plan.RenderDAG(dag, blacklist_namespaces={"lib", "env", "buildlib"})
    print(f"\nDAG -> {dag}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

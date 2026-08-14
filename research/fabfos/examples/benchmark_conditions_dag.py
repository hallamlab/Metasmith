"""The plan-only gate for the conditions tier -- B3 (`condition_gpr`) and B4 (`conditions`).

    PATH="/home/tony/lib/miniforge3/envs/msm/bin:$PATH" \\
        python examples/benchmark_conditions_dag.py

Two steps, both of which read the per-study extractions and turn them into the
per-condition GPR rows the study half composes onto a host. This exists because
`raw::het_screen_records` had no artifact behind it for as long as the two steps have
declared it: the type resolved, the plan did not, and nothing said so. A gate that only
plans is enough to catch that, and it needs none of the bytes.

It does not catch everything a run would. `bench_cohorts` refused by name on filenames
carried over from the sibling project (`gof_observations.tsv`) that exist nowhere here,
which no amount of planning would surface — a plan resolves types, and that was a
filename inside one. Both steps now read `bench::study_extractions`.

WHAT THIS IS NOT. It does not run. `conditions` and `condition_gpr` read cohort tables
whose extraction is human judgement; running them here would produce a benchmark from
whatever happened to be on this disk. The executing driver is the study tier's, and it
takes the conditions as a given for the same reason.

`raw::het_screen_records` IS A LOOKUP, not a fifth cohort -- LASER's heterologous labels
resolved to accessions, keyed on the `(label, source organism)` pair LASER writes.
`build_references/curate_het_screen.py` produces it, and says there why that key
and not the label.
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
# The pinned engine ahead of whatever this machine has installed. See AGENTS.md.
sys.path.insert(0, str(REPO / "src"))

from metasmith.python_api import (                                    # noqa: E402
    Agent,
    Source,
    DataInstanceLibrary,
    TransformInstanceLibrary,
    TargetBuilder,
    Runtime,
)

MLIB = REPO / "src" / "metasmith_libraries"
BREF = REPO / "src" / "fabfos" / "build_references"
DATA = REPO / "data" / "fabfos"
ARTIFACTS = REPO / "tests" / "fabfos" / "artifacts"
WORK = DATA / "scratch" / "benchmark_conditions_dag"

AGENT_ENV = "msm-fabfos"

# Every given the two steps read. All four literature products are pinned bytes plus one
# curated table; there is no transform behind any of them, which is the whole reason this
# gate is plan-only.
STAGED = {
    # The per-study extractions, one folder and one schema. NOT the downloads under
    # `originals/benchmarks/` -- those are the upstream repository, the Keio supplement
    # and a PDF, and the cohort loaders read none of them.
    "bench::study_extractions": DATA / "benchmarks" / "_extractions",
    "raw::het_screen_records":  DATA / "originals" / "benchmarks" / "het_screen",
    "ref::mnxr_lookup":         DATA / "processed" / "mnxr_lookup" / "mnxr_lookup.parquet",
    # The upstream LASER checkout, for `inputs/Gene-Reaction Pairings.txt` -- the MetaCyc
    # route's whole source, and the one route here that is neither a curator's assignment
    # nor a projection through an annotation id space.
    "raw::laser_records":       DATA / "originals" / "benchmarks" / "laser",
    # The reaction universe an emitted edge has to be a member of, plus the transport
    # flag. Read from MetaNetX's own reac_prop.tsv rather than off the bridge: the bridge
    # holds only reactions carrying an EC, KO, MetaCyc or UniProt key, so membership
    # against it fails 151 curated ids MetaNetX defines perfectly well.
    "fabfos_data::metanetx":    DATA / "originals" / "metanetx",
    # The host's own curated model. Without it the native, LOF and screen arms carry no
    # reaction at all -- which, since Y is derived by walking an entry's reactions, is the
    # same state as having no answer key.
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
            # Planning never opens an input, so a stand-in resolves the type exactly as
            # the real bytes do. Reported anyway: a gate that passed on a tree holding
            # none of the products would say nothing about whether they are there.
            missing.append((dtype, at))
            at = WORK / dtype.replace("::", "_")
            if Path(STAGED[dtype]).suffix:
                at.parent.mkdir(parents=True, exist_ok=True)
                at.touch()
            else:
                at.mkdir(parents=True, exist_ok=True)
        inputs.AddItem(at, dtype)
    inputs.Save()

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
    # ONE SAMPLE, THE WHOLE LIBRARY. Both steps group on their env rather than on a
    # given -- there is one conditions table over all four cohorts, not one per cohort --
    # so sampling by a literature product asks the planner to satisfy a group_by that no
    # given carries, and the target silently drops out of the plan. Same shape as the
    # annotation gate, which takes no given at all.
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

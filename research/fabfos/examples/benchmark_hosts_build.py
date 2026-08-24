from __future__ import annotations

import argparse
import json
import shutil
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

MLIB = REPO / "src" / "metasmith_libraries"
BREF = REPO / "src" / "fabfos" / "build_references"
DATA = REPO / "data" / "fabfos"
ARTIFACTS = REPO / "tests" / "fabfos" / "artifacts"
SCRATCH = DATA / "scratch"
PUBLISH_AT = DATA / "runs"

AGENT_ENV = "msm-fabfos"

STAGED = {
    "fabfos_data::genomes":  DATA / "originals" / "genomes",
    "fabfos_data::metanetx": DATA / "originals" / "metanetx",
    "ref::mnxr_lookup":      DATA / "processed" / "mnxr_lookup" / "mnxr_lookup.parquet",
    "ref::metabolism_vocab": DATA / "processed" / "metabolism_bake" / "vocab.parquet",
    "ref::atom_pairs":       DATA / "processed" / "metabolism_bake" / "atom_pairs.parquet",
    "ref::direction_ratios": DATA / "processed" / "metabolism_bake" / "direction.parquet",
    "bench::study_extractions": DATA / "benchmarks" / "_extractions",
}

FORBIDDEN = ("aam_reference", "direction_bake", "mnx_lookups", "genomes", "metanetx")


def plan(work: Path, *, hosts_only: bool = False):
    inputs = DataInstanceLibrary(work / "inputs.xgdb")
    for tl in ("ncbi.yml", "sequences.yml", "annotation.yml", "ref.yml", "lib.yml"):
        inputs.AddTypeLibrary(MLIB / "data_types" / tl)
    for tl in ("fabfos_data.yml", "raw.yml", "interm.yml", "bench.yml", "buildlib.yml",
               "lookup.yml", "evidence.yml"):
        inputs.AddTypeLibrary(BREF / "data_types" / tl)

    missing = []
    for dtype, at in STAGED.items():
        if not at.exists():
            missing.append((dtype, at))
            at = work / dtype.replace("::", "_")
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
    targets.Add("ref::gpr_table_gem")
    if not hosts_only:
        targets.Add("bench::study_benchmark")

    agent = Agent(home=Source.FromLocal(work / "agent_home"),
                  runtime=Runtime.MAMBA,
                  container=AGENT_ENV)
    return missing, inputs, agent, agent.GenerateWorkflow(
        samples=list(inputs.AsSamples("fabfos_data::genomes")),
        resources=resources,
        transforms=transforms,
        targets=targets,
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", action="store_true",
                    help="execute; without it this plans, renders and stops")
    ap.add_argument("--work", type=Path, default=None)
    ap.add_argument("--publish", action="store_true",
                    help=f"copy the tables into {PUBLISH_AT.relative_to(REPO)}")
    ap.add_argument("--hosts-only", action="store_true",
                    help="build and publish the host tables alone, leaving the study "
                         "folders as they are -- see plan()")
    a = ap.parse_args()

    work = a.work or (SCRATCH / "benchmark_hosts")
    if work.exists() and not a.work:
        shutil.rmtree(work)
    work.mkdir(parents=True, exist_ok=True)

    missing, inputs, agent, task = plan(work, hosts_only=a.hosts_only)
    if not task.ok:
        print(f"FAILED to plan:\n{task.plan}")
        return 1

    used = {Path(s.transform._path).stem for s in task.plan.steps}
    print(f"Plan OK -- {len(task.plan.steps)} step(s): {sorted(used)}")
    for step in sorted(task.plan.steps, key=lambda s: s.order):
        prods = [i.dtype_name for g in step.produces for i in g]
        print(f"  {step.order:>3}  {Path(step.transform._path).stem:<20} -> {prods}")

    problems = []
    forbidden = set(FORBIDDEN) & used
    if forbidden:
        problems.append(
            f"a producer of a STAGED type is in the plan: {sorted(forbidden)}. Asking "
            f"for a host GPR table must not rebuild the references it reads.")
    want_steps = 1 if a.hosts_only else 2
    if len(task.plan.steps) != want_steps:
        problems.append(f"expected {want_steps} step(s)"
                        + ("" if a.hosts_only else " (hosts, studies)")
                        + f", got {len(task.plan.steps)}")

    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    svg = ARTIFACTS / "benchmark_hosts_dag.svg"
    task.plan.RenderDAG(svg, blacklist_namespaces={"lib", "env", "buildlib"})
    print(f"\nDAG -> {svg}")

    for p in problems:
        print(f"FAIL: {p}")
    if problems:
        return 1

    if missing:
        for dtype, at in missing:
            print(f"NOTE: no bytes for {dtype} at {at}; an empty stand-in resolved the "
                  f"type so the plan could be checked.")
        if a.run:
            print("\nrefusing --run with stand-ins: this would build a reference out of "
                  "empty files. `dvc checkout` the pins first.")
            return 1

    if not a.run:
        print("\nplan only. Re-run with --run to execute.")
        return 0

    print(f"\nexecuting under {Runtime.MAMBA} in {work} ...", flush=True)
    agent.Deploy()
    agent.StageWorkflow(task, on_exist="update")
    agent.RunWorkflow(task, config_file=agent.GetNxfConfigPresets()["local"])
    result = agent.WaitForWorkflow(task, timeout_s=3600, poll_s=10)
    print(f"\nstatus: {result['status']} after {result['elapsed_s']:.0f}s")
    for line in result["tail"]:
        print(f"    {line}")
    if result["status"] != "completed":
        return 2

    results = Path(agent.GetResultSource(task).GetPath())
    made = sorted(results.glob("ref-gpr_table_gem/*/hosts/*/gpr_gem.parquet"))
    studies = sorted(results.glob("bench-study_benchmark/*/*/gpr_manual.parquet"))
    print(f"\n{len(made)} host table(s), {len(studies)} study folder(s) under {results}")
    for st in studies:
        print(f"    study {st.parent.name}")
    for m in made:
        print(f"    {m.parent.name:<16} {m.stat().st_size:,} B")
    if not made:
        print("FAIL: the run completed and produced no host table at all")
        return 3
    want = json.loads((made[0].parent.parent.parent / "BUILD.json").read_text())
    want_hosts = sorted(want["gem_source"])
    if sorted(p.parent.name for p in made) != want_hosts:
        print(f"FAIL: built {sorted(p.parent.name for p in made)}, "
              f"BUILD.json declares {want_hosts}")
        return 3

    src = made[0].parent.parent
    if a.publish:
        def replace(src_file: Path, dst_file: Path) -> None:
            dst_file.parent.mkdir(parents=True, exist_ok=True)
            if dst_file.exists() or dst_file.is_symlink():
                dst_file.unlink()
            shutil.copy2(src_file, dst_file)

        # `hosts/<host>/gpr_gem.parquet` is the transform's own layout inside one output
        # directory; the run tier keys on the run, so this fans out per host rather than
        # dropping the directory. BUILD.json describes the build, not a host, so one copy
        # sits at the tier root.
        dest = PUBLISH_AT
        dest.mkdir(parents=True, exist_ok=True)
        for host_dir in sorted(src.glob("*")):
            replace(host_dir / "gpr_gem.parquet",
                    dest / host_dir.name / "gpr" / "gpr_gem.parquet")
        replace(src.parent / "BUILD.json", dest / "BUILD_gem.json")
        sroot = studies[0].parent.parent if studies else None
        for study_dir in sorted(p for p in sroot.glob("*") if p.is_dir()) if sroot else []:
            d = DATA / "benchmarks" / study_dir.name
            if d.exists():
                shutil.rmtree(d)
            shutil.copytree(study_dir, d)
        if sroot:
            replace(sroot / "BUILD.json", DATA / "benchmarks" / "BUILD_studies.json")
        print(f"\npublished -> {dest}"
              + (f" and {DATA / 'benchmarks'}/<study>/" if sroot else
                 " (host tables only; the study folders were left alone)"))
    else:
        print(f"\nnot published. Re-run with --publish to copy into {PUBLISH_AT}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""Build the curated-GEM half of the host benchmark (B1), here, under MAMBA.

    PATH="/home/tony/lib/miniforge3/envs/msm/bin:$PATH" \\
        python examples/benchmark_hosts_build.py            # plan only
    PATH="..." python examples/benchmark_hosts_build.py --run

Two steps: the three host tables, and the seven per-study folders that read them.
`hosts/<host>/gpr_gem.parquet` and `<study>/{extraction.tsv, gpr_manual.parquet,
conditions.tsv, README.md}`. It is small -- two model JSONs,
a crosswalk and a join -- which is why it runs here rather than on a cluster; the
de-novo half is the one that needs a GPU allocation, and that is
`examples/benchmark_hosts_denovo_on_hpc.py`.

MAMBA, not a container. `cobra.env` carries a `conda:` key and no image, and
`Agent.runtime` is one global setting, so this graph has exactly one runtime it can
satisfy. `build-refs-cobra` must already exist; `bash envs/setup_agent_env.sh` builds
it along with the agent env.

TWO GIVENS, and both are declared rather than tolerated:

  * `fabfos_data::genomes`      -- DVC-pinned; the acquisition exists, and re-fetching
                                   would build the reference off a fresh pull rather
                                   than off the pins this build describes
  * the metabolism bake         -- R6's product, DVC-pinned at
                                   data/processed/metabolism_bake/ for the same reason as
                                   the genomes: this build reads the pin the benchmark was
                                   measured against, not whatever a fresh R6 run would
                                   mint. R6 makes it -- see REFERENCES.md § R6 for
                                   what makes it differ from the tier-4 agreement
                                   target

`ref::mnxr_lookup` is likewise staged rather than recompiled, for the first reason.

THE BAKE IS PART OF THE RESULT, not scaffolding around it. `in_atom_universe` is only
meaningful relative to one bake, so the transform writes the identity block into its own
BUILD.json; when R6 lands, rebuilding is a visible diff rather than a silent change of
meaning.
"""
from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
# The pinned engine, ahead of whatever this machine has installed: only it resolves a
# `conda:` env declaration by runtime. See AGENTS.md.
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
# Where the benchmark's host half lives. Hosts are not studies, so they sit beside the
# per-study folders rather than inside one.
PUBLISH_AT = DATA / "benchmarks" / "hosts"

AGENT_ENV = "msm-fabfos"

# type -> where it is staged from. Everything here is a GIVEN: pinned bytes handed in,
# not fetched. A run that re-acquired would not be building the reference these pins
# describe.
STAGED = {
    "fabfos_data::genomes":  DATA / "originals" / "genomes",
    "fabfos_data::metanetx": DATA / "originals" / "metanetx",
    "ref::mnxr_lookup":      DATA / "processed" / "mnxr_lookup" / "mnxr_lookup.parquet",
    "ref::metabolism_vocab": DATA / "processed" / "metabolism_bake" / "vocab.parquet",
    "ref::atom_pairs":       DATA / "processed" / "metabolism_bake" / "atom_pairs.parquet",
    "ref::direction_ratios": DATA / "processed" / "metabolism_bake" / "direction.parquet",
    # The per-paper extractions. A GIVEN: there is no transform that turns a PDF
    # supplement into rows, and one that only copied a file would dress a judgement
    # call up as a computation.
    "bench::study_extractions": DATA / "benchmarks" / "_extractions",
}

# The producers of the staged types, asserted ABSENT from the plan by name. Loading
# `bake/` would let the planner decide to rebuild R6 as a side effect of asking for a
# host GPR table -- a 40-hour graph reached by a tiebreak.
FORBIDDEN = ("aam_ensemble", "direction_ensemble", "mnx_lookups", "genomes", "metanetx")


def plan(work: Path):
    inputs = DataInstanceLibrary(work / "inputs.xgdb")
    for tl in ("ncbi.yml", "sequences.yml", "annotation.yml", "ref.yml", "lib.yml"):
        inputs.AddTypeLibrary(MLIB / "data_types" / tl)
    for tl in ("fabfos_data.yml", "raw.yml", "interm.yml", "bench.yml", "buildlib.yml",
               "lookup.yml", "evidence.yml"):
        inputs.AddTypeLibrary(BREF / "data_types" / tl)

    missing = []
    for dtype, at in STAGED.items():
        if not at.exists():
            # Planning never opens an input, so a stand-in resolves the type exactly as
            # the real bytes do -- which is what lets this render on a machine holding
            # none of them. A RUN is what needs the real one.
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
    a = ap.parse_args()

    work = a.work or (SCRATCH / "benchmark_hosts")
    if work.exists() and not a.work:
        shutil.rmtree(work)
    work.mkdir(parents=True, exist_ok=True)

    missing, inputs, agent, task = plan(work)
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
    if len(task.plan.steps) != 2:
        problems.append(f"expected two steps (hosts, studies), got "
                        f"{len(task.plan.steps)}")

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

    # A COMPLETED RUN IS NOT A SUCCEEDED ONE. The verdict comes from the products, and
    # the products are behind a published symlink whose name is a content hash -- so
    # this resolves the link rather than globbing for a path the run does not use.
    results = Path(agent.GetResultSource(task).GetPath())
    made = sorted(results.glob("ref-gpr_table_gem/*/hosts/*/gpr_gem.parquet"))
    studies = sorted(results.glob("bench-study_benchmark/*/*/gpr_manual.parquet"))
    print(f"\n{len(made)} host table(s), {len(studies)} study folder(s) under {results}")
    for st in studies:
        print(f"    study {st.parent.name}")
    for m in made:
        print(f"    {m.parent.name:<16} {m.stat().st_size:,} B")
    if len(made) != 3:
        return 3

    src = made[0].parent.parent          # .../hosts
    if a.publish:
        # UNLINK BEFORE COPYING, ALWAYS. Anything already published under data/ is a
        # read-only HARDLINK into a DVC cache that several worktrees share, so opening
        # one for writing is both a PermissionError and -- if the mode ever allowed it --
        # a way to mutate every other worktree's copy of that file through the shared
        # inode. Replacing the link is the only safe write.
        def replace(src_file: Path, dst_file: Path) -> None:
            dst_file.parent.mkdir(parents=True, exist_ok=True)
            if dst_file.exists() or dst_file.is_symlink():
                dst_file.unlink()
            shutil.copy2(src_file, dst_file)

        dest = PUBLISH_AT
        dest.mkdir(parents=True, exist_ok=True)
        for host_dir in sorted(src.glob("*")):
            replace(host_dir / "gpr_gem.parquet", dest / host_dir.name / "gpr_gem.parquet")
        replace(src.parent / "BUILD.json", dest / "BUILD_gem.json")
        sroot = studies[0].parent.parent
        for study_dir in sorted(p for p in sroot.glob("*") if p.is_dir()):
            d = DATA / "benchmarks" / study_dir.name
            if d.exists():
                shutil.rmtree(d)
            shutil.copytree(study_dir, d)
        replace(sroot / "BUILD.json", DATA / "benchmarks" / "BUILD_studies.json")
        print(f"\npublished -> {dest} and {DATA / 'benchmarks'}/<study>/")
    else:
        print(f"\nnot published. Re-run with --publish to copy into {PUBLISH_AT}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

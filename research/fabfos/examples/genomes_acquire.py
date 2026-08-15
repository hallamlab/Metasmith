#!/usr/bin/env python3
"""Re-run the host-genome acquisition, here, under MAMBA.

    PATH="/home/tony/lib/miniforge3/envs/msm-fabfos/bin:$PATH" \\
        python examples/genomes_acquire.py                 # plan only
    PATH="..." python examples/genomes_acquire.py --run --publish

`fabfos_data::genomes` is a GIVEN to every other driver in this directory -- B1 stages
it, B2 stages it, and both say in as many words that re-acquiring would build the
reference off a fresh pull rather than off the pin. That is right for them and it left
the acquisition itself with no way to be run: the folder on disk was produced once,
before this tree, and the transform that produces it had no driver at all.

It needs one whenever the HOST SET changes, which is a change to `acquire/genomes.py`
and nothing else -- adding DH1 and W3110 for the ASKA cohorts is what asked for this.
So this is deliberately the smallest driver that exists: one target, one transform
directory, no staged inputs, and a publish step that replaces the pinned folder.

MAMBA, not a container, for the same reason `benchmark_hosts_build.py` is: the two envs
this graph needs (`ncbi-datasets`, `python_for_data_science`) both carry a `conda:` key,
`Agent.runtime` is one global setting, and `envs/setup_agent_env.sh` already creates
both by name. Nothing here is heavy -- five `datasets download` calls and three wgets.

PUBLISHING IS ADDITIVE, AND THAT IS THE WHOLE CARE THIS DRIVER TAKES. Upstream has
re-released every one of the three original hosts since they were pinned -- measured
2026-08-14, all nine of their files differ from the pin, k12's translated CDS set by 18
records -- under unchanged assembly accessions. Overwriting them would move the
reference that B1, B2 and every benchmark table were measured against, silently and
without any of those tables changing. So a host already in the chunk keeps its pinned
bytes and only a NEW host is copied in; `--replace-existing` is there for the day
someone decides to take the newer snapshot deliberately, which is a re-run of
everything downstream and not a publish step.

The consequence is worth stating rather than hiding: after this, the chunk holds two
snapshots -- the older one for k12/dh10b/epi300 and today's for whatever was added.
They are the same three files per host from the same FTP paths, so nothing is
heterogeneous in FORM; what differs is when it was fetched.

WRITING REPLACES A FILE, NEVER OPENS ONE. `data/fabfos/originals/genomes/` is a materialised
DVC chunk whose files are read-only hardlinks into a cache several worktrees share, so
an in-place write is both a PermissionError and, if the mode allowed it, a way to
corrupt that chunk for every worktree at once. `dvc add data/fabfos/originals/genomes`
afterwards is what makes the change a pin rather than a local edit.
"""
from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
# The pinned engine, ahead of whatever this machine has installed: only it resolves a
# `conda:` env declaration by runtime. See AGENTS.md.
sys.path.insert(0, str(REPO / "src" / "metasmith" / "src"))

from metasmith.python_api import (                                    # noqa: E402
    Agent,
    Source,
    DataInstanceLibrary,
    TransformInstanceLibrary,
    TargetBuilder,
    Runtime,
)

MLIB = REPO / "src" / "metasmith_libraries"
BREF = REPO / "build_references"
DATA = REPO / "data"
SCRATCH = DATA / "scratch"
PUBLISH_AT = DATA / "originals" / "genomes"

AGENT_ENV = "msm-fabfos"


def plan(work: Path):
    inputs = DataInstanceLibrary(work / "inputs.xgdb")
    for tl in ("ncbi.yml", "sequences.yml", "annotation.yml", "ref.yml", "lib.yml"):
        inputs.AddTypeLibrary(MLIB / "data_types" / tl)
    for tl in ("fabfos_data.yml", "raw.yml", "interm.yml", "bench.yml", "buildlib.yml",
               "lookup.yml", "evidence.yml"):
        inputs.AddTypeLibrary(BREF / "data_types" / tl)
    inputs.Save()

    resources = [
        DataInstanceLibrary.Load(MLIB / "resources" / "env"),
        DataInstanceLibrary.Load(MLIB / "resources" / "lib"),
        DataInstanceLibrary.Load(BREF / "resources" / "buildlib"),
        inputs,
    ]
    transforms = [TransformInstanceLibrary.Load(BREF / "transforms" / "acquire")]

    targets = TargetBuilder()
    targets.Add("fabfos_data::genomes")

    agent = Agent(home=Source.FromLocal(work / "agent_home"),
                  runtime=Runtime.MAMBA,
                  container=AGENT_ENV)
    # The solver roots its search in samples and refuses an empty set, so the EMPTY
    # inputs library is the one sample -- it registers the type libraries and hands over
    # no data. An acquisition has no samples in the sense the fosmid pipeline does.
    return agent, agent.GenerateWorkflow(
        samples=[inputs],
        resources=resources,
        transforms=transforms,
        targets=targets,
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", action="store_true",
                    help="execute; without it this plans and stops")
    ap.add_argument("--publish", action="store_true",
                    help=f"copy hosts NOT already in {PUBLISH_AT.relative_to(REPO)} "
                         f"into it")
    ap.add_argument("--replace-existing", action="store_true",
                    help="also overwrite hosts already pinned there. This moves the "
                         "reference every built table was measured against -- see the "
                         "module docstring before using it")
    ap.add_argument("--work", type=Path, default=None)
    a = ap.parse_args()

    work = a.work or (SCRATCH / "genomes_acquire")
    if work.exists() and not a.work:
        shutil.rmtree(work)
    work.mkdir(parents=True, exist_ok=True)

    agent, task = plan(work)
    if not task.ok:
        print(f"FAILED to plan:\n{task.plan}")
        return 1

    used = {Path(s.transform._path).stem for s in task.plan.steps}
    print(f"Plan OK -- {len(task.plan.steps)} step(s): {sorted(used)}")
    if used != {"genomes"}:
        print(f"FAIL: expected exactly the `genomes` step, got {sorted(used)}")
        return 1

    if not a.run:
        print("\nplan only. Re-run with --run to execute.")
        return 0

    print(f"\nexecuting under {Runtime.MAMBA} in {work} ...", flush=True)
    agent.Deploy()
    agent.StageWorkflow(task, on_exist="update")
    agent.RunWorkflow(task, config_file=agent.GetNxfConfigPresets()["local"])
    result = agent.WaitForWorkflow(task, timeout_s=7200, poll_s=10)
    print(f"\nstatus: {result['status']} after {result['elapsed_s']:.0f}s")
    for line in result["tail"]:
        print(f"    {line}")
    if result["status"] != "completed":
        return 2

    # A COMPLETED RUN IS NOT A SUCCEEDED ONE -- the verdict comes from the products.
    results = Path(agent.GetResultSource(task).GetPath())
    made = sorted(results.glob("fabfos_data-genomes/*/*/genome"))
    hosts = sorted({p.parent.name for p in made})
    print(f"\n{len(hosts)} host genome(s) under {results}: {hosts}")
    if not hosts:
        return 3
    for h in hosts:
        d = made[0].parent.parent / h
        files = sorted(p.name for p in (d / "genome").glob("*"))
        gem = sorted(p.name for p in (d / "GEM").glob("*")) if (d / "GEM").is_dir() else []
        print(f"    {h:<16} genome {files}  GEM {gem}")

    if a.publish:
        src = made[0].parent.parent
        PUBLISH_AT.mkdir(parents=True, exist_ok=True)
        added, kept = [], []
        for host_dir in sorted(p for p in src.glob("*") if p.is_dir()):
            dest = PUBLISH_AT / host_dir.name
            if dest.exists() and not a.replace_existing:
                kept.append(host_dir.name)
                continue
            if dest.exists():
                shutil.rmtree(dest)
            shutil.copytree(host_dir, dest)
            added.append(host_dir.name)
        print(f"\npublished -> {PUBLISH_AT}")
        if added:
            print(f"    written: {added}")
        if kept:
            print(f"    left at their pinned bytes: {kept} "
                  f"(--replace-existing to take today's snapshot instead)")
        print(f"    now: dvc add {PUBLISH_AT.relative_to(REPO)} && git add the pin")
    else:
        print(f"\nnot published. Re-run with --publish to add new hosts to "
              f"{PUBLISH_AT}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

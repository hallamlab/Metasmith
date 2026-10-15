#!/usr/bin/env python3
"""The metasmith PathoLogic route over the six E. coli RefSeq proteomes, on fir.

    PATH="$HOME/lib/miniforge3/envs/msm/bin:$PATH" python research/mm_compare/ecoli_pathologic.py   # plan only
    ... --preflight | --run | --retrieve

DeepEC, KOfam and UniRef50 annotate each proteome, ptools_annotation_gather folds them into a
PathoLogic input, and pathologic builds one PGDB per strain. The pathologic env names a private
image: seed fir's apptainer cache with it (a link named for the env's URI) before --run.
"""
from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(REPO / "research" / "fabfos" / "examples"))

from metasmith.python_api import (                                      # noqa: E402
    DataInstanceLibrary, TargetBuilder, TransformInstanceLibrary,
)
from _driver import (                                                   # noqa: E402
    FIR_ACCOUNT, FIR_HOST, check_schedulable, check_staged_executor, check_tasks, check_walltimes,
    envs_from_plan, fir_agent, landed_products, pin_external_leaf_ids, preflight,
    provision_dev_overlay_remote, retrieve,
)
import benchmark_hosts_denovo_on_hpc as B2                              # noqa: E402

MLIB = REPO / "src" / "metasmith_libraries"
STRAINS = ("k12", "w3110", "bw25113", "dh1", "dh10b", "epi300")
GENOMES = REPO / "data" / "fabfos" / "originals" / "genomes"
WORK = REPO / "data" / "scratch" / "mm_compare_pathologic"
TARGETS = ("annotation::pgdb_csv_tables", "annotation::pgdb_archive")
EXPECTED = {"chunkOrfsForAnnotation", "deepec", "merge_deepec", "kofamscan", "merge_kofamscan",
            "diamond_uniref50", "merge_diamond_uniref50", "ptools_annotation_gather", "pathologic"}

AGENT_HOME = "/scratch/phyberos/bench/e4_mm/agent_home"
PROCESSED = "/scratch/phyberos/fabfos_refs/processed"
CONTAINER = "docker://quay.io/hallamlab/metasmith:0.22.1"
REFS = {k: v for k, v in B2.REFS_4.items() if k.startswith(("ref::kofamscan", "ref::uniref50"))}
QUEUE_SIZE = 50


def plan(agent):
    xgdb = WORK / "inputs.xgdb"
    if xgdb.exists():
        shutil.rmtree(xgdb)
    inputs = DataInstanceLibrary(xgdb)
    for tl in B2.TYPE_LIBRARIES:
        inputs.AddTypeLibrary(tl)
    for s in STRAINS:
        shutil.copy(next((GENOMES / f"e_coli_{s}" / "genome").glob("*.faa")), xgdb / f"{s}.faa")
        inputs.AddItem(f"{s}.faa", "sequences::orfs")
    for dtype, rel in REFS.items():
        inputs.AddItem(f"{PROCESSED}/{rel}", dtype)
    pin_external_leaf_ids(inputs)
    inputs.Save()

    resources = [DataInstanceLibrary.Load(MLIB / "resources" / "env"),
                 DataInstanceLibrary.Load(MLIB / "resources" / "lib"), inputs]
    transforms = [TransformInstanceLibrary.Load(MLIB / "transforms" / t)
                  for t in ("functionalAnnotation", "logistics")]
    tb = TargetBuilder()
    for t in TARGETS:
        tb.Add(t)
    return agent.GenerateWorkflow(samples=list(inputs.AsSamples("sequences::orfs")),
                                  resources=resources, transforms=transforms, targets=tb)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--run", action="store_true")
    ap.add_argument("--preflight", action="store_true")
    ap.add_argument("--retrieve", action="store_true")
    ap.add_argument("--timeout-hours", type=float, default=48.0)
    a = ap.parse_args()
    WORK.mkdir(parents=True, exist_ok=True)

    agent = fir_agent(host=FIR_HOST, agent_home=AGENT_HOME, container=CONTAINER)
    task = plan(agent)
    if not task.ok:
        print(f"PLAN DID NOT RESOLVE:\n{getattr(task.plan, 'hints', task.plan)}", file=sys.stderr)
        return 3
    steps = sorted(task.plan.steps, key=lambda s: s.order)
    used = {Path(s.transform._path).stem for s in steps}
    print(f"plan: {len(steps)} steps, transforms {sorted(used)}")
    if used != EXPECTED:
        print(f"expected exactly {sorted(EXPECTED)}", file=sys.stderr)
        return 3
    key = task.GetKey()
    print(f"task key: {key}", flush=True)
    (WORK / "RUN_KEY").write_text(key)

    if a.preflight:
        return preflight(FIR_HOST, AGENT_HOME, CONTAINER, envs_from_plan(task), mlib=MLIB)
    if a.retrieve:
        out = WORK / "results"
        retrieve(FIR_HOST, agent.GetResultSource(task).GetPath(), out)
        check_tasks(FIR_HOST, AGENT_HOME, key)
        return 0 if set(TARGETS) <= set(landed_products(out, list(TARGETS))) else 2
    if not a.run:
        print("plan only. Add --preflight or --run.")
        return 0

    provision_dev_overlay_remote(FIR_HOST, AGENT_HOME)
    if preflight(FIR_HOST, AGENT_HOME, CONTAINER, envs_from_plan(task), mlib=MLIB):
        return 4
    agent.StageWorkflow(task, on_exist="update")
    if (check_staged_executor(FIR_HOST, AGENT_HOME, key) or check_walltimes(FIR_HOST, B2.RESOURCE_OVERRIDES)
            or check_schedulable(FIR_HOST, FIR_ACCOUNT, B2.RESOURCE_OVERRIDES, workdir=AGENT_HOME)):
        return 4
    agent.RunWorkflow(task, config_file=agent.GetNxfConfigPresets()["slurm"],
                      params={"slurmAccount": FIR_ACCOUNT, "executor": {"queueSize": QUEUE_SIZE}},
                      resource_overrides=B2.RESOURCE_OVERRIDES)
    result = agent.WaitForWorkflow(task, timeout_s=a.timeout_hours * 3600, poll_s=300)
    print(f"status: {result['status']} after {result['elapsed_s'] / 3600:.2f} h", flush=True)
    for line in result["tail"]:
        print(f"    {line}")
    return 0 if result["status"] == "completed" else 2


if __name__ == "__main__":
    sys.exit(main())

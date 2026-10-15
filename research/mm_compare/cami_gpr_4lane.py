#!/usr/bin/env python3
"""The fabfos 4-lane GPR mapper over E5's CAMI II HMP MAG proteins, one ORF set per sample, on fir.

    PATH="$HOME/lib/miniforge3/envs/msm/bin:$PATH" python research/mm_compare/cami_gpr_4lane.py           # plan only
    PATH="..." python research/mm_compare/cami_gpr_4lane.py --preflight
    PATH="..." python research/mm_compare/cami_gpr_4lane.py --run
    PATH="..." python research/mm_compare/cami_gpr_4lane.py --retrieve

`clone_gpr_on_hpc.py` with many ORF sets in one plan: the 49 pooled sample sets under
`data/mm_compare/e5_hmp_orfs/` plus `e5_hmp_poolcheck.faa`, one MAG's ORFs alone, which tests that
pooling a sample does not change a MAG's rows. `e5_hmp_orf_to_mag.tsv` splits the tables back to MAGs.
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
    DataInstanceLibrary, Duration, Gpu, Resources, Size, TargetBuilder, TransformInstanceLibrary,
)
from _driver import (                                                   # noqa: E402
    FIR_ACCOUNT, FIR_HOST, check_schedulable, check_staged_executor, check_tasks, check_walltimes,
    envs_from_plan, fir_agent, landed_products, pin_external_leaf_ids, preflight,
    provision_dev_overlay_remote, retrieve,
)
import benchmark_hosts_denovo_on_hpc as B2                              # noqa: E402
from clone_gpr_on_hpc import EXPECTED, TARGET                           # noqa: E402

MLIB = REPO / "src" / "metasmith_libraries"
DATA = REPO / "data" / "mm_compare"
ORF_SETS = sorted((DATA / "e5_hmp_orfs").glob("*.faa")) + [DATA / "e5_hmp_poolcheck.faa"]
WORK = REPO / "data" / "scratch" / "mm_compare_gpr"

AGENT_HOME = "/scratch/phyberos/bench/e4_mm/agent_home"
PROCESSED = "/scratch/phyberos/fabfos_refs/processed"
CONTAINER = "docker://quay.io/hallamlab/metasmith:0.22.1"
GPU_ACCOUNT = "def-shallam_gpu"
# CLEAN declares 16 GB of device memory, so a 2g.20gb MIG slice fits and backfills ahead of a whole card.
GPU = Gpu(memory=Size.GB(20), type="nvidia_h100_80gb_hbm3_2g.20gb", flag="--gres=gpu:")
# E5 shares the account's 1,000-job queue on fir.
QUEUE_SIZE = 200

# A sample pools up to 89k ORFs, where the host driver's mapper override was sized for one ~5k proteome.
OVERRIDES = dict(B2.RESOURCE_OVERRIDES,
                 gpr_4lane=Resources(cpus=4, memory=Size.GB(64), duration=Duration(hours=3)))


def plan(agent):
    xgdb = WORK / "inputs.xgdb"
    if xgdb.exists():
        shutil.rmtree(xgdb)
    inputs = DataInstanceLibrary(xgdb)
    for tl in B2.TYPE_LIBRARIES:
        inputs.AddTypeLibrary(tl)
    for orfs in ORF_SETS:
        shutil.copy(orfs, xgdb / orfs.name)
        inputs.AddItem(orfs.name, "sequences::orfs")
    for dtype, rel in B2.REFS_4.items():
        inputs.AddItem(f"{PROCESSED}/{rel}", dtype)
    pin_external_leaf_ids(inputs)
    inputs.Save()
    print(f"    {len(ORF_SETS)} ORF sets")

    resources = [DataInstanceLibrary.Load(MLIB / "resources" / "env"),
                 DataInstanceLibrary.Load(MLIB / "resources" / "lib"), inputs]
    transforms = [TransformInstanceLibrary.Load(MLIB / "transforms" / t)
                  for t in ("functionalAnnotation", "fabfos", "logistics")]
    tb = TargetBuilder()
    tb.Add(TARGET)
    return agent.GenerateWorkflow(samples=list(inputs.AsSamples("sequences::orfs")),
                                  resources=resources, transforms=transforms, targets=tb)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--run", action="store_true")
    ap.add_argument("--preflight", action="store_true")
    ap.add_argument("--retrieve", action="store_true")
    ap.add_argument("--deploy", action="store_true",
                    help="deploy the agent home first. Then delete setup_commands from its lib/agent.yml "
                         "before --run, or every task dies 'relay MISSING'")
    ap.add_argument("--timeout-hours", type=float, default=72.0)
    a = ap.parse_args()
    WORK.mkdir(parents=True, exist_ok=True)

    agent = fir_agent(host=FIR_HOST, agent_home=AGENT_HOME, container=CONTAINER)
    B2.check_refs(FIR_HOST, PROCESSED, 4)
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
        return 0 if TARGET in landed_products(out, [TARGET]) else 2
    if a.deploy:
        agent.Deploy()
        print("deployed: strip setup_commands from lib/agent.yml, then --run")
        return 0
    if not a.run:
        print("plan only. Add --preflight or --run.")
        return 0

    provision_dev_overlay_remote(FIR_HOST, AGENT_HOME)
    if preflight(FIR_HOST, AGENT_HOME, CONTAINER, envs_from_plan(task), mlib=MLIB):
        return 4
    agent.StageWorkflow(task, on_exist="update")
    if (check_staged_executor(FIR_HOST, AGENT_HOME, key) or check_walltimes(FIR_HOST, OVERRIDES)
            or check_schedulable(FIR_HOST, FIR_ACCOUNT, OVERRIDES, workdir=AGENT_HOME)):
        return 4
    agent.RunWorkflow(task, config_file=agent.GetNxfConfigPresets()["slurm"],
                      params={"slurmAccount": FIR_ACCOUNT, "slurmGpuAccount": GPU_ACCOUNT,
                              "executor": {"queueSize": QUEUE_SIZE}},
                      resource_overrides=OVERRIDES, gpus=GPU)
    result = agent.WaitForWorkflow(task, timeout_s=a.timeout_hours * 3600, poll_s=300)
    print(f"status: {result['status']} after {result['elapsed_s'] / 3600:.2f} h", flush=True)
    for line in result["tail"]:
        print(f"    {line}")
    return 0 if result["status"] == "completed" else 2


if __name__ == "__main__":
    sys.exit(main())

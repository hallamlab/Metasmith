#!/usr/bin/env python3
"""One ORF set through the four canonical lanes and the mapper, on a cluster.

    PATH="/home/tony/lib/miniforge3/envs/msm/bin:$PATH" \\
        python examples/clone_gpr_on_hpc.py --site sockeye --preflight
    PATH="..." python examples/clone_gpr_on_hpc.py --site sockeye --run
    PATH="..." python examples/clone_gpr_on_hpc.py --site sockeye --publish

The de-novo half of a STUDY, where its siblings do the de-novo half of a HOST. The
eydallin cohort's 86 clones are an ORF set like any other -- `main/benchmarks/eydallin/
build_clone_orfs.py` writes them out of W3110 -- and this runs the shipped mapper over
them exactly as `benchmark_hosts_denovo_on_hpc.py` runs it over a proteome. That is the
whole point: the two lines of evidence for a clone (a curated model's assertion, and
what the lanes infer from its sequence) have to come from the same mapper the host half
uses, or the comparison measures the mapper rather than the clone.

WHY NOT `benchmark_hosts_denovo_lanes.py --orfs`. That driver's job is the host set: it
stages every proteome, attributes each lane output back to a host by lineage, and hands
the result to a collector that REFUSES a table it cannot place. An ORF set that is not
a host has no place in that, and bolting one on would make the refusal conditional --
which is the one property that collector has.

NO SCATTER AND NO COLLECTOR, therefore: one ORF set in, one `annotation::gpr_table` out.
The graph is four lanes plus `gpr_4lane`, and every lane is pinned to the one ORF set by
the mapper's own `parents={orfs}`.

Everything site-shaped -- the reference paths, the GPU declaration, the SLURM accounts,
the preflight, the walltime check -- is imported from the host driver rather than
restated, so the two cannot drift into annotating one ORF set two ways.
"""
from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]

_ENGINE = REPO / "src"
if (_ENGINE / "metasmith").is_dir():
    sys.path.insert(0, str(_ENGINE))

from metasmith.python_api import (                                      # noqa: E402
    DataInstanceLibrary, TargetBuilder, TransformInstanceLibrary,
)

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _driver import (                                                   # noqa: E402
    check_schedulable, check_staged_executor, check_tasks, check_walltimes,
    envs_from_plan, landed_products, preflight, provision_dev_overlay_remote,
    publish_by_type, retrieve,
)
import benchmark_hosts_denovo_on_hpc as B2                              # noqa: E402

MLIB = REPO / "src" / "metasmith_libraries"
DATA = REPO / "data"
SCRATCH = DATA / "scratch"
ARTIFACTS = REPO / "tests" / "artifacts"

CLONES = DATA / "fabfos" / "runs" / "eydallin_clones" / "annotations" / "eydallin_clones.faa"
PUBLISH_INTO = DATA / "fabfos" / "runs" / "eydallin_clones"

TARGET = "annotation::gpr_table"

# A run publishes its declared targets and nothing else, so the merged lane tables are
# targets in their own right rather than fallout of the mapper's. Declaring them is also
# what keeps the chunk files out: `publish_intermediates` would land every one.
LANES = [
    "annotation::kofamscan_results",
    "annotation::kofamscan_descriptions",
    "annotation::clean_predictions",
    "annotation::diamond_uniref50_results",
    "annotation::diamond_uniref50_descriptions",
    "annotation::proteinbert_embeddings",
]

PUBLISH_AT = {
    "annotation::gpr_table": "gpr/gpr_denovo_mapper.parquet",
    "annotation::kofamscan_results": "annotations/lanes/kofamscan.csv",
    "annotation::kofamscan_descriptions": "annotations/lanes/kofamscan_descriptions.csv",
    "annotation::clean_predictions": "annotations/lanes/clean.tsv",
    "annotation::diamond_uniref50_results": "annotations/lanes/diamond_uniref50.tsv",
    "annotation::diamond_uniref50_descriptions":
        "annotations/lanes/diamond_uniref50_descriptions.tsv",
    "annotation::proteinbert_embeddings": "annotations/lanes/proteinbert_embeddings.parquet",
}

EXPECTED = {"chunkOrfsForAnnotation",
            "kofamscan", "clean", "diamond_uniref50", "proteinbert",
            "merge_kofamscan", "merge_diamond_uniref50", "merge_proteinbert",
            "gpr_4lane"}


def plan(work: Path, agent, orfs: Path, remote_processed: str):
    xgdb = work / "inputs.xgdb"
    if xgdb.exists():
        shutil.rmtree(xgdb)
    inputs = DataInstanceLibrary(xgdb)
    for tl in B2.TYPE_LIBRARIES:
        inputs.AddTypeLibrary(tl)

    n = sum(1 for line in orfs.open() if line.startswith(">"))
    print(f"    sequences::orfs                  {orfs.name}  ({n} records)")
    shutil.copy(orfs, xgdb / orfs.name)
    inputs.AddItem(orfs.name, "sequences::orfs")

    for dtype, rel in B2.REFS_4.items():
        remote = f"{remote_processed}/{rel}"
        print(f"    {dtype:32s} {remote}")
        inputs.AddItem(remote, dtype)
    inputs.Save()

    resources = [
        DataInstanceLibrary.Load(MLIB / "resources" / "env"),
        DataInstanceLibrary.Load(MLIB / "resources" / "lib"),
        inputs,
    ]
    transforms = [
        TransformInstanceLibrary.Load(MLIB / "transforms" / "functionalAnnotation"),
        TransformInstanceLibrary.Load(MLIB / "transforms" / "fabfos"),
        TransformInstanceLibrary.Load(MLIB / "transforms" / "logistics"),
    ]
    tb = TargetBuilder()
    tb.Add(TARGET)
    for lane in LANES:
        tb.Add(lane)
    return agent.GenerateWorkflow(
        samples=list(inputs.AsSamples("sequences::orfs")),
        resources=resources,
        transforms=transforms,
        targets=tb,
    )


def check_plan(task) -> int:
    steps = sorted(task.plan.steps, key=lambda s: s.order)
    used = {Path(s.transform._path).stem for s in steps}
    print(f"\nPlan OK -- {len(steps)} steps\n")
    for step in steps:
        prods = [i.dtype_name for g in step.produces for i in g]
        print(f"  {step.order:>3}  {Path(step.transform._path).stem:<24} -> {prods}")
    bad = 0
    if used != EXPECTED:
        print(f"\nexpected exactly {sorted(EXPECTED)}, got {sorted(used)}",
              file=sys.stderr)
        bad = 1
    if "prodigal" in used:
        print("\nprodigal is in the plan: the clones are already protein.",
              file=sys.stderr)
        bad = 1
    if not bad:
        print("\nfour lanes and the mapper over one given ORF set")
    return bad


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--orfs", type=Path, default=CLONES)
    ap.add_argument("--into", type=Path, default=PUBLISH_INTO,
                    help="where --publish lands the table and the lane outputs. Any ORF "
                         "set works here, not just the clones: one FASTA in, one mapper "
                         "table out, which is what a host needs too when the fan-out "
                         "route's lineage cannot be trusted")
    ap.add_argument("--site", choices=sorted(B2.SITES), default="sockeye")
    ap.add_argument("--run", action="store_true")
    ap.add_argument("--preflight", action="store_true")
    ap.add_argument("--retrieve", action="store_true")
    ap.add_argument("--publish", action="store_true")
    ap.add_argument("--publish-dry-run", action="store_true")
    ap.add_argument("--timeout-hours", type=float, default=8.0)
    ap.add_argument("--poll-s", type=float, default=60.0)
    ap.add_argument("--work", default=None)
    a = ap.parse_args()

    site = B2.SITES[a.site]
    work = (Path(a.work).resolve() if a.work
            else SCRATCH / f"clone_gpr_{a.site}_{a.orfs.stem}")
    work.mkdir(parents=True, exist_ok=True)
    local_results = work / "results"

    if a.publish or a.publish_dry_run:
        return publish_by_type(local_results, PUBLISH_AT, a.into,
                               dry_run=a.publish_dry_run, repo=REPO)

    if not a.orfs.exists():
        raise SystemExit(f"no ORF set at {a.orfs}.\n  Build it: `mamba run -n "
                         f"figure-net python main/benchmarks/eydallin/"
                         f"build_clone_orfs.py --publish`")

    agent = site["agent"](host=site["host"], agent_home=site["agent_home"],
                          container=site["container"])
    print(f"=== site: {a.site} ({site['host']}) ===")
    B2.check_refs(site["host"], site["processed"], 4)

    print("=== staging ===")
    task = plan(work, agent, a.orfs, site["processed"])
    if not task.ok:
        print(f"\nPLAN DID NOT RESOLVE:\n{getattr(task.plan, 'hints', task.plan)}",
              file=sys.stderr)
        return 3
    if check_plan(task):
        return 3
    print(f"\n=== task key: {task.GetKey()} ===", flush=True)
    (work / "RUN_KEY").write_text(task.GetKey())

    if a.preflight:
        return preflight(site["host"], site["agent_home"], site["container"],
                         envs_from_plan(task), mlib=MLIB,
                         image_store=site.get("image_store"))
    if a.retrieve:
        retrieve(site["host"], agent.GetResultSource(task).GetPath(), local_results)
        check_tasks(site["host"], site["agent_home"], task.GetKey())
        return verify(local_results)
    if not a.run:
        print("\nplan only: nothing staged, nothing run. Add --run.")
        return 0

    print("=== Deploy() ===", flush=True)
    agent.Deploy()
    provision_dev_overlay_remote(site["host"], site["agent_home"])
    if preflight(site["host"], site["agent_home"], site["container"],
                 envs_from_plan(task), mlib=MLIB,
                 image_store=site.get("image_store")):
        print("\nrefusing to run: a compute node has no outbound network.",
              file=sys.stderr)
        return 4

    # "update" would keep a run directory whose workflow.nf and Orchestrator.groovy were
    # compiled against a different identity space; every re-stage here starts clean.
    agent.StageWorkflow(task, on_exist="clear")
    if check_staged_executor(site["host"], site["agent_home"], task.GetKey()):
        return 4
    if check_walltimes(site["host"], B2.RESOURCE_OVERRIDES):
        return 4
    if check_schedulable(site["host"], site["account"], B2.RESOURCE_OVERRIDES,
                         workdir=site["agent_home"]):
        return 4

    agent.RunWorkflow(
        task,
        config_file=agent.GetNxfConfigPresets()["slurm"],
        params={"slurmAccount": site["account"],
                "slurmGpuAccount": site["gpu_account"]},
        resource_overrides=B2.RESOURCE_OVERRIDES,
        gpus=site["gpu"],
    )
    print(f"=== waiting (timeout {a.timeout_hours:.1f}h) ===", flush=True)
    result = agent.WaitForWorkflow(task, timeout_s=a.timeout_hours * 3600,
                                   poll_s=a.poll_s)
    print(f"=== status: {result['status']} after "
          f"{result['elapsed_s'] / 3600:.2f} h ===", flush=True)
    for line in result["tail"]:
        print(f"    {line}")
    if result["status"] != "completed":
        return 2
    check_tasks(site["host"], site["agent_home"], task.GetKey())
    retrieve(site["host"], agent.GetResultSource(task).GetPath(), local_results)
    return verify(local_results)


def verify(results: Path) -> int:
    wanted = [TARGET] + LANES
    landed = landed_products(results, wanted)
    missing = [t for t in wanted if t not in landed]
    if missing:
        print(f"\nTHE RUN IS GREEN BUT {len(missing)} TARGET(S) ARE ABSENT: "
              f"{missing}. Nextflow ignores a process that exhausted its retries; "
              f"read _metasmith/logs.*/main.log.",
              file=sys.stderr)
        return 2
    print(f"{TARGET} and {len(LANES)} lane table(s) present. Now: --publish.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

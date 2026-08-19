"""Build the DE-NOVO half of the host benchmark (B2) on an HPC site.

    PATH="/home/tony/lib/miniforge3/envs/msm/bin:$PATH" \\
        python examples/benchmark_hosts_denovo_on_hpc.py                 # plan only
    PATH="..." python examples/benchmark_hosts_denovo_on_hpc.py --site sockeye --run

TWO SITES, AND THE SECOND IS NOT A LUXURY. fir goes into whole-cluster cooling
maintenance for a day and a half at a time; during the window its login nodes and
storage stay up while the scheduler refuses every job, so the work moves rather than
waits. `--site` selects the whole bundle -- host, agent home, module incantation, image
store, both SLURM accounts and what a GPU is -- because those are not independently
choosable: mixing fir's H100 declaration into a sockeye submission yields
`requested_gpus 0` and a CLEAN lane that runs on no card at all.

What does NOT change with the site is the plan. The graph, the targets, the lane set and
the resource overrides are the method; the site is where it runs. That split is why this
is one driver with a switch rather than two files that drift.

The sibling of `examples/scadc_gpr.py`, pointed at three host proteomes instead of the
recovered inserts. THAT IS THE POINT OF WIRING IT THIS WAY: the benchmark's de-novo
evidence comes out of the SHIPPED mapper running exactly as it runs on a fosmid ORF set,
so the benchmark tests the method rather than a file someone once produced.

    genomes -> host_proteomes -> {kofam, clean, uniref50, proteinbert} -> gpr_4lane
                                                                      -> host_gpr_denovo

`host_proteomes` is the scatter, and it is in the graph rather than in this driver for a
lineage reason: `host_gpr_denovo` pins the mapper's output to `parents={genomes}`, and
three proteomes staged as unrelated givens give that pin nothing to bind to. The planner
would then satisfy the mapper from whatever ORF set is cheapest to reach and the host
attribution would land on a table built from something else.

THE LANE SET IS CHECKED, NOT REPORTED. The fourth lane needs `ref::reference_label_pool`;
`check_refs` probes it on the site before anything is staged, and B2's collector refuses a
table whose channels are not the declared four. An absent reference stops the run here --
it never yields a shorter table.

Everything else -- the reference staging rule, the GPU declaration, the preflight, the
walltime check, the task-table verdict -- is `examples/scadc_gpr.py`'s, imported rather
than copied, because a second copy of it is a second thing to keep true.
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
    DataInstanceLibrary, Duration, Gpus, Resources, Size,
    TargetBuilder, TransformInstanceLibrary,
)

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _driver import (                                                   # noqa: E402
    FIR_ACCOUNT, FIR_AGENT_HOME, FIR_CONTAINER, FIR_GPU, FIR_GPU_ACCOUNT, FIR_HOST,
    SOCKEYE_ACCOUNT, SOCKEYE_AGENT_HOME, SOCKEYE_CONTAINER, SOCKEYE_GPU,
    SOCKEYE_GPU_ACCOUNT, SOCKEYE_HOST, SOCKEYE_IMAGE_STORE,
    check_schedulable, check_staged_executor, check_tasks, check_walltimes,
    envs_from_plan, fir_agent, landed_products, preflight,
    provision_dev_overlay_remote, publish_by_type, retrieve, sockeye_agent, ssh_once,
)

SITES = {
    "fir": dict(
        host=FIR_HOST, agent_home=FIR_AGENT_HOME, agent=fir_agent, gpu=FIR_GPU,
        account=FIR_ACCOUNT, gpu_account=FIR_GPU_ACCOUNT, container=FIR_CONTAINER,
        processed="/scratch/phyberos/fabfos_refs/processed",
    ),
    "sockeye": dict(
        host=SOCKEYE_HOST, agent_home=SOCKEYE_AGENT_HOME, agent=sockeye_agent,
        gpu=SOCKEYE_GPU, account=SOCKEYE_ACCOUNT, gpu_account=SOCKEYE_GPU_ACCOUNT,
        image_store=SOCKEYE_IMAGE_STORE, container=SOCKEYE_CONTAINER,
        processed="/arc/project/st-shallam-1/fabfos_refs/processed",
    ),
}

BREF = REPO / "src" / "fabfos" / "build_references"
MLIB = REPO / "src" / "metasmith_libraries"
DATA = REPO / "data" / "fabfos"
PROCESSED = DATA / "processed"
SCADC = DATA / "runs" / "scadc_fosmids"
SCRATCH = DATA / "scratch"
ARTIFACTS = REPO / "tests" / "fabfos" / "artifacts"

INSERTS = SCADC / "sequences" / "inserts" / "inserts.fna"

REFS_4 = {
    "ref::kofamscan_profiles": "kofam_ref/profiles",
    "ref::kofamscan_ko_list": "kofam_ref/ko_list.tsv",
    "ref::uniref50_diamond_db": "uniref50_dmnd/uniref50.dmnd",
    "ref::mnxr_lookup": "mnxr_lookup/mnxr_lookup.parquet",
    "ref::reference_label_pool": "reference_label_pool/pool",
}
REFS_7 = dict(REFS_4, **{
    "ref::esm_c_600m_weights": "esm_c_weights/esmc_600m.tgz",
    "ref::reference_label_pool_esmc": "reference_label_pool_esmc/pool",
    "ref::ezpred_model": "ezpred_model/EZpred",
})

GENOMES = REPO / "data" / "fabfos" / "originals" / "genomes"

TARGET_4 = "annotation::gpr_table"
TARGET_7 = "annotation::gpr_table_7lane"
TARGET_DENOVO = "ref::gpr_table_denovo"

PUBLISH_AT = {
    "annotation::gpr_table": "gpr_4lane.parquet",
    "annotation::gpr_table_7lane": "gpr_7lane.parquet",
    "ref::gpr_table_denovo": "hosts",
    "sequences::orfs": "orfs.faa",
    "sequences::gff": "orfs.gff",
    "annotation::kofamscan_results": "lanes/kofamscan.csv",
    "annotation::clean_predictions": "lanes/clean.tsv",
    "annotation::diamond_uniref50_results": "lanes/diamond_uniref50.tsv",
    "annotation::proteinbert_embeddings": "lanes/proteinbert_embeddings.parquet",
    "annotation::proteinbert_index": "lanes/proteinbert_index.csv",
    "annotation::deepec_predictions": "lanes/deepec.tsv",
    "annotation::ezpred_predictions": "lanes/ezpred.csv",
    "annotation::esm_c_embeddings": "lanes/esm_c_embeddings.parquet",
    "annotation::esm_c_index": "lanes/esm_c_index.csv",
}

TYPE_LIBRARIES = ([MLIB / "data_types" / f for f in
                   ("sequences.yml", "annotation.yml", "ref.yml", "lib.yml",
                    "fabfos.yml", "ncbi.yml")]
                  + [BREF / "data_types" / f for f in
                     ("fabfos_data.yml", "raw.yml", "interm.yml", "bench.yml",
                      "buildlib.yml", "lookup.yml", "evidence.yml")])

RESOURCE_OVERRIDES = {
    "kofamscan": Resources(cpus=16, memory=Size.GB(32), duration=Duration(hours=4)),
    "diamond_uniref50": Resources(cpus=16, memory=Size.GB(64), duration=Duration(hours=3)),
    "proteinbert": Resources(cpus=8, memory=Size.GB(32), duration=Duration(hours=2)),
    "clean": Resources(cpus=4, memory=Size.GB(32), duration=Duration(hours=2)),
    "deepec": Resources(cpus=8, memory=Size.GB(32), duration=Duration(hours=1)),
    "esm_c": Resources(cpus=4, memory=Size.GB(32), duration=Duration(hours=1),
                       gpus=Gpus.REQUIRED, gpu_memory=Size.GB(24)),
    "ezpred": Resources(cpus=4, memory=Size.GB(32), duration=Duration(hours=1)),
    "gpr_4lane": Resources(cpus=4, memory=Size.GB(48), duration=Duration(hours=1)),
    "gpr_7lane": Resources(cpus=4, memory=Size.GB(64), duration=Duration(hours=1)),
}


def expected_transforms(lanes: int) -> set[str]:
    sys.path.insert(0, str(REPO / "tests"))
    from test_annotation_driver import EXPECTED_TRANSFORMS  # noqa: E402
    four = set(EXPECTED_TRANSFORMS) | {"host_proteomes", "host_gpr_denovo"}
    if lanes == 7:
        return four | {"deepec", "ezpred", "esm_c", "gpr_7lane"}
    return four


def check_refs(host: str, remote_processed: str, lanes: int) -> None:
    refs = REFS_7 if lanes == 7 else REFS_4
    probe = "; ".join(f'[ -e "{remote_processed}/{rel}" ] || echo "MISSING {d} {rel}"'
                      for d, rel in refs.items())
    out = ssh_once(host, probe).strip()
    if out:
        raise SystemExit(
            f"references absent from {host}:{remote_processed}:\n"
            + "\n".join(f"    {ln}" for ln in out.splitlines())
            + f"\n  Build and place them: "
              f"`python examples/annotation_references_build.py --run` then "
              f"`--publish-remote`.")


def build_inputs(work: Path, lanes: int, remote_processed: str) -> DataInstanceLibrary:
    xgdb = work / "inputs.xgdb"
    if xgdb.exists():
        shutil.rmtree(xgdb)
    inputs = DataInstanceLibrary(xgdb)
    for tl in TYPE_LIBRARIES:
        inputs.AddTypeLibrary(tl)

    if not GENOMES.exists():
        raise SystemExit(
            f"the host set is not at {GENOMES.relative_to(REPO)}.\n"
            f"  Materialise the pin: `dvc checkout data/fabfos/originals/genomes.dvc`")
    n = sum(1 for _ in GENOMES.glob("*/genome/*.faa"))
    print(f"    fabfos_data::genomes         {GENOMES.relative_to(REPO)}  "
          f"({n} proteomes)")
    if n < 1:
        raise SystemExit(f"no proteomes under {GENOMES}/*/genome/*.faa")
    shutil.copytree(GENOMES, xgdb / GENOMES.name, dirs_exist_ok=True)
    inputs.AddItem(GENOMES.name, "fabfos_data::genomes")

    for dtype, rel in (REFS_7 if lanes == 7 else REFS_4).items():
        remote = f"{remote_processed}/{rel}"
        print(f"    {dtype:32s} {remote}")
        inputs.AddItem(remote, dtype)
    inputs.Save()
    return inputs


def plan(work: Path, agent, lanes: int, remote_processed: str):
    inputs = build_inputs(work, lanes, remote_processed)
    resources = [
        DataInstanceLibrary.Load(MLIB / "resources" / "env"),
        DataInstanceLibrary.Load(MLIB / "resources" / "lib"),
        inputs,
    ]
    transforms = [
        TransformInstanceLibrary.Load(MLIB / "transforms" / "metagenomics"),
        TransformInstanceLibrary.Load(MLIB / "transforms" / "functionalAnnotation"),
        TransformInstanceLibrary.Load(MLIB / "transforms" / "fabfos"),
        TransformInstanceLibrary.Load(BREF / "transforms" / "benchmark"),
    ]
    tb = TargetBuilder()
    tb.Add(TARGET_7 if lanes == 7 else TARGET_4)
    if lanes == 7:
        tb.Add(TARGET_4)
    tb.Add(TARGET_DENOVO)

    return agent.GenerateWorkflow(
        samples=list(inputs.AsSamples("fabfos_data::genomes")),
        resources=resources,
        transforms=transforms,
        targets=tb,
    )


def check_plan(task, lanes: int) -> int:
    used = {Path(s.transform._path).stem for s in task.plan.steps}
    print(f"\nPlan OK -- {len(task.plan.steps)} steps, {len(used)} distinct transforms\n")
    for step in sorted(task.plan.steps, key=lambda s: s.order):
        prods = [i.dtype_name for g in step.produces for i in g]
        print(f"  {step.order:>3}  {Path(step.transform._path).stem:<24} -> {prods}")

    bad = 0
    missing = expected_transforms(lanes) - used
    if missing:
        print(f"\nMISSING expected transforms: {sorted(missing)}", file=sys.stderr)
        bad = 1
    for dup in ("downloadKofamscanDB", "downloadUniref50", "downloadEsmC"):
        if dup in used:
            print(f"\nlogistics/{dup} is in the plan -- duplicate reference producer.",
                  file=sys.stderr)
            bad = 1
    n_prodigal = sum(1 for s in task.plan.steps
                     if Path(s.transform._path).stem in ("prodigal", "host_proteomes"))
    if n_prodigal != 1:
        print(f"\n{n_prodigal} ORF-set steps. Every lane must annotate ONE ORF set or "
              f"the mapper folds annotations of one onto another.", file=sys.stderr)
        bad = 1
    if not bad:
        print("\none ORF set, every expected transform, no duplicate producer")
    return bad


def publish(results: Path, *, dry_run: bool, lanes: int = 4) -> int:
    rc = publish_by_type(results, PUBLISH_AT, REPO / "data" / "fabfos" / "benchmarks" / "denovo",
                         dry_run=dry_run, repo=REPO)
    if rc == 0 and not dry_run:
        print("Pin the chunk:  dvc add data/fabfos/runs/scadc_fosmids/annotations")
    return rc


def verify(results: Path, lanes: int) -> int:
    targets = [TARGET_7, TARGET_4] if lanes == 7 else [TARGET_4]
    landed = landed_products(results, targets)
    absent = [t for t in targets if t not in landed]
    if absent:
        print(f"\nTHE RUN IS GREEN BUT {absent} IS ABSENT. Nextflow ignores a process "
              f"that exhausted its retries; read _metasmith/logs.*/main.log for the "
              f"step that died.", file=sys.stderr)
        return 2
    print(f"{', '.join(targets)} present. "
          f"Now: --publish (then `dvc add data/fabfos/runs/scadc_fosmids/annotations`).")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--lanes", type=int, choices=(4, 7), default=4)
    ap.add_argument("--run", action="store_true",
                    help="execute on the host; without it this plans, renders the "
                         "DAG and stops")
    ap.add_argument("--preflight", action="store_true")
    ap.add_argument("--retrieve", action="store_true",
                    help="pull an already-finished run's results down and verify "
                         "them, without re-running")
    ap.add_argument("--publish", action="store_true")
    ap.add_argument("--publish-dry-run", action="store_true")
    ap.add_argument("--site", choices=sorted(SITES), default="fir",
                    help="which cluster to run on. Selects host, agent home, image "
                         "store, both SLURM accounts and the GPU declaration together "
                         "-- they are not independently choosable.")
    ap.add_argument("--host", default=None)
    ap.add_argument("--agent-home", default=None)
    ap.add_argument("--container", default=None,
                    help="the AGENT image. SITE-SPECIFIC: fir carries a hand-tagged "
                         "0.19.0-fabfos, sockeye the engine-derived tag. Pointing one "
                         "site at the other's is a MISSING agent in preflight.")
    ap.add_argument("--remote-processed", default=None,
                    help="the host-side mirror of data/fabfos/processed/, where the "
                         "references are read from. Placed there by "
                         "annotation_references_build.py --publish-remote, or moved "
                         "between clusters with globus.")
    ap.add_argument("--slurm-account", default=None)
    ap.add_argument("--slurm-gpu-account", default=None,
                    help="CLEAN declares a GPU, and both sites charge GPU work to a "
                         "separate allocation: rrg-shallam-ab has no GPU allocation on "
                         "fir (def-shallam does), and sockeye splits st-shallam-1 from "
                         "st-shallam-1-gpu.")
    ap.add_argument("--timeout-hours", type=float, default=24.0)
    ap.add_argument("--poll-s", type=float, default=60.0)
    ap.add_argument("--work", default=None)
    a = ap.parse_args()

    site = SITES[a.site]
    for flag, key in (("host", "host"), ("agent_home", "agent_home"),
                      ("container", "container"),
                      ("remote_processed", "processed"),
                      ("slurm_account", "account"),
                      ("slurm_gpu_account", "gpu_account")):
        if getattr(a, flag) is None:
            setattr(a, flag, site[key])
    print(f"=== site: {a.site} ({a.host}) ===")

    work = (Path(a.work).resolve() if a.work
            else SCRATCH / f"hosts_denovo_{a.site}_{a.lanes}lane")
    work.mkdir(parents=True, exist_ok=True)
    local_results = work / "results"

    if a.publish or a.publish_dry_run:
        return publish(local_results, dry_run=a.publish_dry_run, lanes=a.lanes)
    agent = site["agent"](host=a.host, agent_home=a.agent_home, container=a.container)

    print(f"=== checking the references on {a.host} ===")
    check_refs(a.host, a.remote_processed, a.lanes)

    print(f"=== staging ({a.lanes} lanes) ===")
    task = plan(work, agent, a.lanes, a.remote_processed)
    if not task.ok:
        print(f"\nPLAN DID NOT RESOLVE:\n{getattr(task.plan, 'hints', task.plan)}",
              file=sys.stderr)
        return 3
    if check_plan(task, a.lanes):
        return 3

    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    svg = ARTIFACTS / f"scadc_gpr_{a.lanes}lane.svg"
    task.plan.RenderDAG(svg, blacklist_namespaces={"lib", "env"})
    print(f"\nDAG -> {svg}")
    print(f"\n=== task key: {task.GetKey()} ===", flush=True)

    if a.preflight:
        return preflight(a.host, a.agent_home, a.container,
                         envs_from_plan(task), mlib=MLIB,
                         image_store=site.get("image_store"))

    (work / "RUN_KEY").write_text(task.GetKey())

    if a.retrieve:
        retrieve(a.host, agent.GetResultSource(task).GetPath(), local_results)
        check_tasks(a.host, a.agent_home, task.GetKey())
        return verify(local_results, a.lanes)

    if not a.run:
        print("\nplan only: nothing staged, nothing run. Add --run.")
        return 0

    print("=== Deploy() ===", flush=True)
    agent.Deploy()
    provision_dev_overlay_remote(a.host, a.agent_home)

    if preflight(a.host, a.agent_home, a.container, envs_from_plan(task), mlib=MLIB,
                 image_store=site.get("image_store")):
        print("\nrefusing to run: a compute node has no outbound network, so an "
              "image absent from the store cannot be pulled once a task starts.",
              file=sys.stderr)
        return 4

    agent.StageWorkflow(task, on_exist="update")
    if check_staged_executor(a.host, a.agent_home, task.GetKey()):
        return 4
    if check_walltimes(a.host, RESOURCE_OVERRIDES):
        return 4
    if check_schedulable(a.host, a.slurm_account, RESOURCE_OVERRIDES,
                         workdir=a.agent_home):
        return 4

    print(f"=== executor: slurm, account {a.slurm_account} "
          f"(gpu: {a.slurm_gpu_account}) ===", flush=True)
    agent.RunWorkflow(
        task,
        config_file=agent.GetNxfConfigPresets()["slurm"],
        params={"slurmAccount": a.slurm_account,
                "slurmGpuAccount": a.slurm_gpu_account},
        resource_overrides=RESOURCE_OVERRIDES,
        gpus=site["gpu"],
    )

    print(f"=== waiting (timeout {a.timeout_hours:.1f}h, poll {a.poll_s:.0f}s) ===",
          flush=True)
    result = agent.WaitForWorkflow(task, timeout_s=a.timeout_hours * 3600,
                                   poll_s=a.poll_s)
    print(f"=== status: {result['status']} after "
          f"{result['elapsed_s'] / 3600:.2f} h ===", flush=True)
    for line in result["tail"]:
        print(f"    {line}")
    if result["status"] != "completed":
        return 2

    check_tasks(a.host, a.agent_home, task.GetKey())
    retrieve(a.host, agent.GetResultSource(task).GetPath(), local_results)
    return verify(local_results, a.lanes)


if __name__ == "__main__":
    sys.exit(main())

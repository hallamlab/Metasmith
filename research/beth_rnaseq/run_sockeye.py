#!/usr/bin/env python3
"""Beth's isolate RNA-seq (TCAG run DAV36692) from raw reads on sockeye, one plan per organism.

    python research/beth_rnaseq/run_sockeye.py dag [epi300|cb2a]
    python research/beth_rnaseq/run_sockeye.py images
    python research/beth_rnaseq/run_sockeye.py run [epi300|cb2a] [--plan-only]
    python research/beth_rnaseq/run_sockeye.py status [epi300|cb2a]
    python research/beth_rnaseq/run_sockeye.py retrieve [epi300|cb2a]

The reads are copies of Beth's under ROOT/reads, made by ROOT/stage_reads.sbatch: her own
folders are read only to us, and a transform that unlinks an input must never reach them.
Everything this writes on sockeye stays under ROOT. Connect to sockeye through the awm ssh
domain first; every ssh here rides that connection.

`build.sh` compiles the library this needs before any subcommand.
"""

import argparse
import os
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
sys.path.insert(0, str(REPO / "research" / "fabfos" / "examples"))

from _driver import (  # noqa: E402
    SOCKEYE_ACCOUNT, check_schedulable, check_tasks, envs_from_plan, pin_external_leaf_ids,
    provision_dev_overlay_remote, retrieve, sockeye_agent, ssh_once,
)
from metasmith.models.dag_draw import Label  # noqa: E402
from metasmith.python_api import (  # noqa: E402
    DataInstanceLibrary, Duration, Resources, Size, TransformInstanceLibrary,
)

MLIB = REPO / "src" / "metasmith_libraries"
LIBRARY = HERE / "library"
HOST = "sockeye"
ROOT = os.environ.get("BETH_RNASEQ_ROOT", "/scratch/st-shallam-1/txyliu/beth_rnaseq_B")
AGENT_HOME = f"{ROOT}/agent_home"
READS = f"{ROOT}/reads"
IMAGE_STORE = f"{ROOT}/images"
CONTAINER = "docker://quay.io/hallamlab/metasmith:0.24.2"
WORK = REPO / "cache" / "beth_rnaseq"
RESULTS = HERE / "results"

# cu1..cu6 are 0, 1, 10, 50, 100 and 300 ppm CuSO4, one library each. cb2a_cu1 and cu2 turned
# out to be Bacillus and are left out, which leaves CB2A one library at 10 ppm and nothing to
# compare, so it gets its own plan.
PLANS = {
    "epi300": dict(
        organism="Escherichia coli EPI300",
        accession="GCF_052692645.1",
        doses={"epi300_cu1": 0, "epi300_cu2": 1, "epi300_cu3": 10, "epi300_cu4": 50,
               "epi300_cu5": 100, "epi300_cu6": 300},
        # Pinning the count-derived targets under the sample-sheet table (index 0) keeps the
        # planner from answering them through some other route to a gene_count_table.
        targets=[
            "transcriptomics::gene_count_table",
            {"type": "isolate_rnaseq::dose_trend_results", "parents": [0]},
            {"type": "isolate_rnaseq::dose_trend_figures", "parents": [0]},
            "isolate_rnaseq::dose_clusters",
            "isolate_rnaseq::metal_gene_panel",
            "isolate_rnaseq::tpm_table",
            "isolate_rnaseq::qc_report",
        ],
    ),
    "cb2a": dict(
        organism="Caulobacter vibrioides CB2A JS4038",
        accession="GCF_052446975.1",
        doses={"cb2a_cu3": 10},
        targets=[
            "isolate_rnaseq::tpm_table",
            "isolate_rnaseq::qc_report",
            "annotation::bacmet_diamond_results",
        ],
    ),
}

TRANSFORMS = [MLIB / "transforms" / n for n in ("logistics", "kbase", "functionalAnnotation")] \
    + [LIBRARY / "transforms" / "isolate_rnaseq"]

RESOURCE_OVERRIDES = {
    "fastp": Resources(cpus=8, memory=Size.GB(16), duration=Duration(hours=4)),
    "bowtie2_align": Resources(cpus=16, memory=Size.GB(32), duration=Duration(hours=6)),
}


def agent():
    return sockeye_agent(host=HOST, agent_home=AGENT_HOME, container=CONTAINER, image_store=IMAGE_STORE)


def build_inputs(code: str) -> DataInstanceLibrary:
    plan = PLANS[code]
    work = WORK / code
    work.mkdir(parents=True, exist_ok=True)
    lib = DataInstanceLibrary(work / "inputs.xgdb")
    lib.Purge()
    for t in ("sequences.yml", "ncbi.yml", "transcriptomics.yml", "annotation.yml", "alignment.yml"):
        lib.AddTypeLibrary(MLIB / "data_types" / t)
    lib.AddTypeLibrary(LIBRARY / "data_types" / "isolate_rnaseq.yml")

    exp = lib.AddValue("experiment.txt", f"{plan['organism']} RNA-seq", "transcriptomics::experiment")
    # The genome name sits under the experiment, so each organism's reads meet only its own
    # genome, and the planner fetches the RefSeq assembly instead of building one from reads.
    name = lib.AddValue("genome_name.txt", plan["organism"], "ncbi::genome_name", parents={exp})
    lib.AddValue("accession.txt", plan["accession"], "ncbi::assembly_accession", parents={name})
    sheet = "library\tcopper_ppm\n" + "".join(f"{s}\t{p}\n" for s, p in plan["doses"].items())
    lib.AddValue("sample_sheet.tsv", sheet, "isolate_rnaseq::sample_sheet", parents={exp})
    for s in plan["doses"]:
        meta = lib.AddValue(f"{s}.read_metadata.json",
                            {"sample": s, "parity": "paired", "length_class": "short"},
                            "sequences::read_metadata", parents={exp})
        pair = lib.AddValue(f"{s}.read_pair.txt", s, "sequences::read_pair", parents={meta})
        lib.AddItem(f"{READS}/{s}_R1.fastq.gz", "sequences::zipped_forward_short_reads", parents={pair})
        lib.AddItem(f"{READS}/{s}_R2.fastq.gz", "sequences::zipped_reverse_short_reads", parents={pair})
    pin_external_leaf_ids(lib)
    lib.Save()
    return lib


def plan(smith, code: str):
    inputs = build_inputs(code)
    task = smith.GenerateWorkflow(
        samples=list(inputs.AsSamples("transcriptomics::experiment")),
        resources=[DataInstanceLibrary.Load(MLIB / "resources" / n) for n in ("env", "lib")],
        transforms=[TransformInstanceLibrary.Load(p) for p in TRANSFORMS],
        targets=PLANS[code]["targets"],
    )
    if not task.ok:
        sys.exit(f"{code} did not solve: dropped {sorted(task.plan.dropped_targets)}")
    print(f"{code}: {len(task.plan.steps)} steps, key={task.GetKey()}")
    for step in task.plan.steps:
        uses = sorted({u.dtype_name for u in step.uses if not u.dtype_name.startswith(("env::", "lib::"))})
        print(f"  {step.order:>3} {step.transform.name:<28} <- {', '.join(uses)}")
    return task


def codes(args) -> list[str]:
    return [args.organism] if args.organism else list(PLANS)


def sif_name(uri: str) -> str:
    return uri.replace("://", "..").replace(":", "..").replace("/", "_") + ".sif"


def cmd_images(args):
    smith = agent()
    uris = {CONTAINER}
    for code in PLANS:
        for name in envs_from_plan(plan(smith, code)):
            for line in (MLIB / "resources" / "env" / name).read_text().splitlines():
                if line.startswith("container:"):
                    uris.add(line.split(":", 1)[1].strip())
    out = ssh_once(HOST, "; ".join(
        f'[ -e {IMAGE_STORE}/{sif_name(u)}.verified ] && echo "OK      {u}" || echo "MISSING {u}"'
        for u in sorted(uris)))
    print(out.rstrip())
    return 1 if "MISSING" in out else 0


def cmd_dag(args):
    smith = agent()
    RESULTS.mkdir(exist_ok=True)
    for code in codes(args):
        task = plan(smith, code)
        dag = task.plan.BuildDAG(given_root=False, target_sink=False)
        # BuildDAG names a transform node "<order> <name>"; DagRenderer has no public relabel.
        for step in task.plan.steps:
            root = Path(step.transform_library.location).name
            dag._labels[f"{step.order} {step.transform.name}"] = Label(name=step.transform.name, namespace=root)
        for fmt in ("svg", "png"):
            print(dag.render(RESULTS / f"beth_rnaseq_dag_{code}", fmt))
    return 0


def cmd_run(args):
    smith = agent()
    tasks = {code: plan(smith, code) for code in codes(args)}
    if args.plan_only:
        return 0
    missing = ssh_once(HOST, "; ".join(f'[ -s {READS}/{s}_{m}.fastq.gz ] || echo "MISSING {s}_{m}"'
                                       for code in tasks for s in PLANS[code]["doses"] for m in ("R1", "R2")))
    if missing.strip():
        print(missing, file=sys.stderr)
        return 1
    if cmd_images(args):
        return 2
    smith.Deploy()
    provision_dev_overlay_remote(HOST, AGENT_HOME)
    if check_schedulable(HOST, SOCKEYE_ACCOUNT, RESOURCE_OVERRIDES, workdir=AGENT_HOME):
        return 4
    for code, task in tasks.items():
        smith.StageWorkflow(task, on_exist="update")
        report = smith.MaterialiseImages(task)
        print(f"{code} images: {report['fetched']} fetched, {report['already_present']} present")
        smith.RunWorkflow(
            task,
            config_file=smith.GetNxfConfigPresets()["slurm"],
            params={"slurmAccount": SOCKEYE_ACCOUNT},
            resource_overrides=RESOURCE_OVERRIDES,
        )
        print(f"launched {code} {task.GetKey()}")
    return 0


def cmd_status(args):
    smith = agent()
    failed = 0
    for code in codes(args):
        task = plan(smith, code)
        result = smith.WaitForWorkflow(task, timeout_s=1, poll_s=1)
        print(f"{code} status: {result['status']}")
        for line in result["tail"]:
            print(f"    {line}")
        failed += check_tasks(HOST, AGENT_HOME, task.GetKey(), attempt="latest")
    return 1 if failed else 0


def cmd_retrieve(args):
    smith = agent()
    for code in codes(args):
        task = plan(smith, code)
        retrieve(HOST, smith.GetResultSource(task).GetPath(), WORK / code / "results")
    return 0


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("images")
    for name in ("dag", "run", "status", "retrieve"):
        p = sub.add_parser(name)
        p.add_argument("organism", nargs="?", choices=list(PLANS))
        if name == "run":
            p.add_argument("--plan-only", action="store_true")
    args = ap.parse_args()
    return {"images": cmd_images, "dag": cmd_dag, "run": cmd_run, "status": cmd_status,
            "retrieve": cmd_retrieve}[args.cmd](args)


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""ASPIRE over PRJNA801777 on sockeye: 18 V4 amplicon libraries from Anabaena and Microcystis cultures.

    python research/aspire/campaigns/cyano_r1/run_cyano.py list
    python research/aspire/campaigns/cyano_r1/run_cyano.py stage-reads
    python research/aspire/campaigns/cyano_r1/run_cyano.py stage-refs
    python research/aspire/campaigns/cyano_r1/run_cyano.py side-load-images
    python research/aspire/campaigns/cyano_r1/run_cyano.py check-refs
    python research/aspire/campaigns/cyano_r1/run_cyano.py run [--plan-only]
    python research/aspire/campaigns/cyano_r1/run_cyano.py status
    python research/aspire/campaigns/cyano_r1/run_cyano.py retrieve
    python research/aspire/campaigns/cyano_r1/run_cyano.py dag

Every download runs on the login node, since compute nodes have no internet. Connect to
sockeye through the awm ssh domain first; every ssh here rides that connection.

`stage-refs` needs the mock's mitochondrial and contaminant FASTAs locally (ASPIRE_MOCK_REFS),
and `side-load-images` needs the aspire image built locally (docker/aspire/dev.sh --build).
"""

import argparse
import csv
import os
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[3]
sys.path.insert(0, str(REPO / "research" / "fabfos" / "examples"))
sys.path.insert(0, str(REPO / "research" / "aspire"))

from _driver import (  # noqa: E402
    SOCKEYE_ACCOUNT, SOCKEYE_HOST, SOCKEYE_IMAGE_STORE, check_schedulable,
    check_staged_executor, check_tasks, envs_from_plan, pin_external_leaf_ids,
    provision_dev_overlay_remote, retrieve, sockeye_agent, ssh_once,
)
from aspire_asv_pipeline import read_metadata  # noqa: E402
from metasmith.models.dag_renderer import DagMode  # noqa: E402
from metasmith.python_api import (  # noqa: E402
    DataInstanceLibrary, Duration, Resources, Size, TransformInstanceLibrary,
)

MLIB = REPO / "src" / "metasmith_libraries"
HOST = os.environ.get("ASPIRE_SOCKEYE_HOST", SOCKEYE_HOST)
ROOT = os.environ.get("ASPIRE_CYANO_ROOT", "/scratch/st-shallam-1/txyliu/aspire_cyano")
AGENT_HOME = f"{ROOT}/agent_home"
READS = f"{ROOT}/reads"
REFS = os.environ.get("ASPIRE_REFS", "/arc/project/st-shallam-1/aspire_refs")
SILVA = f"{REFS}/silva_138_2"
IMAGE_STORE = os.environ.get("ASPIRE_IMAGE_STORE", SOCKEYE_IMAGE_STORE)
CONTAINER = os.environ.get("ASPIRE_AGENT_CONTAINER", "docker://quay.io/hallamlab/metasmith:0.23.0")
ASPIRE_IMAGE = "quay.io/hallamlab/aspire:0.1.0"
MOCK_REFS = Path(os.environ.get("ASPIRE_MOCK_REFS", REPO / "data" / "aspire" / "mock_references"))
LOCAL_RESULTS = REPO / "data" / "aspire" / "cyano_r1"
WORK = REPO / "cache" / "aspire" / "cyano_r1"

TARGETS = ["aspire::counts_clean", "aspire::read_fate", "amplicon::asv_taxonomy",
           "aspire::sankey_outputs", "aspire::analysis_metadata", "aspire::indicspecies_results"]
SILVA_FILES = {
    "silva.arb.gz": "https://www.arb-silva.de/fileadmin/silva_databases/release_138_2/ARB_files/SILVA_138.2_SSURef_NR99_03_07_24_opt.arb.gz",
    "silva_seqs.qza": "https://data.qiime2.org/2024.10/common/silva-138-99-seqs.qza",
    "silva_tax.qza": "https://data.qiime2.org/2024.10/common/silva-138-99-tax.qza",
    "silva_nb_classifier.qza": "https://data.qiime2.org/classifiers/sklearn-1.4.2/silva/silva-138-99-nb-classifier.qza",
}
RESOURCE_OVERRIDES = {
    "sina_trim": Resources(cpus=16, memory=Size.GB(48), duration=Duration(hours=6)),
    "taxonomy": Resources(cpus=8, memory=Size.GB(32), duration=Duration(hours=6)),
    "indicspecies": Resources(cpus=2, memory=Size.GB(8), duration=Duration(hours=4)),
}


def samples():
    with open(HERE / "samples.tsv") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def agent():
    return sockeye_agent(host=HOST, agent_home=AGENT_HOME, container=CONTAINER,
                         image_store=IMAGE_STORE)


def cmd_list(_):
    for s in samples():
        print(f"{s['sample']:<6}{s['run_accession']:<14}{s['read_count']:>8} pairs")
    return 0


def cmd_stage_reads(_):
    lines = ["set -euo pipefail", f"mkdir -p {READS}", f"cd {READS}"]
    for s in samples():
        for mate, url, md5 in zip(("R1", "R2"), s["fastq_ftp"].split(";"), s["fastq_md5"].split(";")):
            out = f"{s['sample']}_{mate}.fastq.gz"
            lines.append(f'[ "$(md5sum < {out} 2>/dev/null | cut -d" " -f1)" = {md5} ] '
                         f'|| {{ curl -sSfL -o {out} https://{url}; '
                         f'[ "$(md5sum < {out} | cut -d" " -f1)" = {md5} ]; }}')
    lines.append("ls | wc -l")
    print(ssh_once(HOST, "\n".join(lines)).strip(), "files staged in", READS)
    return 0


def cmd_stage_refs(_):
    fetch = " && ".join(f"{{ [ -e {n.removesuffix('.gz')} ] || curl -sSfL -o {n} {u}; }}"
                        for n, u in SILVA_FILES.items())
    ssh_once(HOST, f"set -e; mkdir -p {SILVA}; cd {SILVA}; {fetch}; "
                   f"[ -e silva.arb ] || gunzip silva.arb.gz; ls -la")
    for name in ("mitochondria.fasta", "contaminants.fasta"):
        subprocess.run(["scp", "-q", str(MOCK_REFS / name), f"{HOST}:{REFS}/{name}"], check=True)
    return cmd_check_refs(_)


def sif_name(uri: str) -> str:
    return uri.replace("://", "..").replace(":", "..").replace("/", "_") + ".sif"


def plan_images(task) -> list[str]:
    uris = [CONTAINER]
    for name in envs_from_plan(task):
        for line in (MLIB / "resources" / "env" / name).read_text().splitlines():
            if line.startswith("container:"):
                uris.append(line.split(":", 1)[1].strip())
    return uris


def cmd_side_load_images(_):
    # Built here, not on the login node: unpacking layers onto sockeye's GPFS scratch takes most
    # of an hour per image, and a dropped ssh session kills the build with it.
    uris = plan_images(plan(agent()))
    present = ssh_once(HOST, "; ".join(f"[ -e {IMAGE_STORE}/{sif_name(u)}.verified ] && echo {u}"
                                       for u in uris) + "; true").split()
    tmp = WORK / "apptainer_tmp"
    tmp.mkdir(parents=True, exist_ok=True)
    env = {**os.environ, "APPTAINER_TMPDIR": str(tmp)}
    for uri in uris:
        if uri in present:
            print(f"present  {uri}")
            continue
        sif = WORK / sif_name(uri)
        if not sif.exists():
            source, tar = uri, WORK / "aspire.tar"
            if uri == f"docker://{ASPIRE_IMAGE}":
                subprocess.run(["docker", "save", "-o", str(tar), ASPIRE_IMAGE], check=True)
                source = f"docker-archive://{tar}"
            partial = sif.with_suffix(".partial")
            subprocess.run(["apptainer", "build", "--force", str(partial), source], check=True, env=env)
            partial.rename(sif)
            tar.unlink(missing_ok=True)
        remote = f"{IMAGE_STORE}/{sif.name}"
        subprocess.run(["rsync", "-a", "--partial", "-e", "ssh -o BatchMode=yes", str(sif),
                        f"{HOST}:{remote}"], check=True)
        ssh_once(HOST, f"module load gcc/9.4.0 apptainer/1.3.1 && "
                       f"apptainer exec --no-home --cleanenv {remote} true && : > {remote}.verified")
        sif.unlink()
        print(f"loaded   {uri}")
    return 0


def cmd_check_refs(_):
    wanted = [f"{SILVA}/{n.removesuffix('.gz')}" for n in SILVA_FILES] + \
             [f"{REFS}/mitochondria.fasta", f"{REFS}/contaminants.fasta"] + \
             [f"{READS}/{s['sample']}_{m}.fastq.gz" for s in samples() for m in ("R1", "R2")]
    out = ssh_once(HOST, "; ".join(f'[ -s {p} ] || echo "MISSING {p}"' for p in wanted))
    missing = [ln for ln in out.splitlines() if ln.startswith("MISSING")]
    print("\n".join(missing) if missing else f"all {len(wanted)} inputs present on {HOST}")
    return 1 if missing else 0


def build_inputs():
    WORK.mkdir(parents=True, exist_ok=True)
    lib = DataInstanceLibrary(WORK / "inputs.xgdb")
    lib.Purge()
    for t in ("aspire.yml", "amplicon.yml", "sequences.yml"):
        lib.AddTypeLibrary(MLIB / "data_types" / t)
    study = lib.AddValue("study_metadata.tsv", (HERE / "study_metadata.tsv").read_text(),
                         "aspire::study_metadata")
    lib.AddValue("params.yml", (HERE / "params.yml").read_text(), "aspire::params", parents={study})
    for s in samples():
        sid = s["sample"]
        meta = lib.AddValue(f"{sid}.read_metadata.json", read_metadata(sid, "paired"),
                            "sequences::read_metadata", parents={study})
        pair = lib.AddValue(f"{sid}.read_pair.txt", sid, "sequences::read_pair", parents={meta})
        for mate, dtype in (("R1", "zipped_forward_short_reads"), ("R2", "zipped_reverse_short_reads")):
            lib.AddItem(f"{READS}/{sid}_{mate}.fastq.gz", f"sequences::{dtype}", parents={pair})
    pin_external_leaf_ids(lib)
    lib.Save()
    return lib


def build_references():
    lib = DataInstanceLibrary(WORK / "references.xgdb")
    lib.Purge()
    for t in ("aspire.yml", "amplicon.yml"):
        lib.AddTypeLibrary(MLIB / "data_types" / t)
    lib.AddItem(SILVA, "amplicon::silva_db")
    lib.AddItem(f"{REFS}/mitochondria.fasta", "aspire::mito_reference_source")
    lib.AddItem(f"{REFS}/contaminants.fasta", "aspire::contaminant_reference_source")
    pin_external_leaf_ids(lib)
    lib.Save()
    return lib


def plan(smith):
    inputs = build_inputs()
    task = smith.GenerateWorkflow(
        samples=list(inputs.AsSamples("aspire::study_metadata")),
        resources=[DataInstanceLibrary.Load(MLIB / "resources" / n) for n in ("env", "lib")]
                  + [build_references()],
        transforms=[TransformInstanceLibrary.Load(MLIB / "transforms" / n) for n in ("aspire", "logistics")],
        targets=TARGETS,
    )
    if not task.ok:
        sys.exit(f"did not solve: dropped {sorted(task.plan.dropped_targets)}")
    n = len(samples())
    got = {}
    for g in task.plan.given:
        got[g.dtype_name] = got.get(g.dtype_name, 0) + 1
    for dtype in ("sequences::read_metadata", "sequences::zipped_forward_short_reads",
                  "sequences::zipped_reverse_short_reads"):
        if got.get(dtype) != n:
            sys.exit(f"{dtype}: the plan carries {got.get(dtype, 0)}, samples.tsv has {n}")
    print(f"plan: {len(task.plan.steps)} steps over {n} samples, key={task.GetKey()}")
    return task


def cmd_run(args):
    smith = agent()
    task = plan(smith)
    if args.plan_only:
        return 0
    if cmd_check_refs(args):
        return 1
    smith.Deploy()
    provision_dev_overlay_remote(HOST, AGENT_HOME)
    smith.StageWorkflow(task, on_exist="update")
    if check_staged_executor(HOST, AGENT_HOME, task.GetKey()):
        return 3
    report = smith.MaterialiseImages(task)
    print(f"images: {report['fetched']} fetched, {report['already_present']} present")
    if check_schedulable(HOST, SOCKEYE_ACCOUNT, RESOURCE_OVERRIDES, workdir=AGENT_HOME):
        return 4
    smith.RunWorkflow(
        task,
        config_file=smith.GetNxfConfigPresets()["slurm"],
        params={"slurmAccount": SOCKEYE_ACCOUNT},
        resource_overrides=RESOURCE_OVERRIDES,
    )
    print(f"launched {task.GetKey()}; poll with `status`")
    return 0


def cmd_status(_):
    smith = agent()
    task = plan(smith)
    result = smith.WaitForWorkflow(task, timeout_s=1, poll_s=1)
    print(f"status: {result['status']}")
    for line in result["tail"]:
        print(f"    {line}")
    return check_tasks(HOST, AGENT_HOME, task.GetKey(), attempt="latest")


def cmd_retrieve(_):
    smith = agent()
    task = plan(smith)
    if check_tasks(HOST, AGENT_HOME, task.GetKey(), attempt="latest"):
        return 1
    retrieve(HOST, smith.GetResultSource(task).GetPath(), LOCAL_RESULTS)
    return 0


def cmd_dag(_):
    task = plan(agent())
    reports = HERE / "reports"
    reports.mkdir(exist_ok=True)
    # RenderDAG reads a dotted basename's suffix as the format, so the stem carries no dot.
    for mode, suffix in ((DagMode.PLAIN, ""), (DagMode.STEPS, "_steps"), (DagMode.LEGEND, "_legend")):
        print(task.plan.RenderDAG(str(reports / f"cyano_r1{suffix}"), format="svg",
                                  show_step_order=True, mode=mode))
    return 0


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("list", "stage-reads", "stage-refs", "side-load-images", "check-refs", "status", "retrieve", "dag"):
        sub.add_parser(name)
    run = sub.add_parser("run")
    run.add_argument("--plan-only", action="store_true")
    args = ap.parse_args()
    return {
        "list": cmd_list, "stage-reads": cmd_stage_reads, "stage-refs": cmd_stage_refs,
        "side-load-images": cmd_side_load_images, "check-refs": cmd_check_refs, "run": cmd_run,
        "status": cmd_status, "retrieve": cmd_retrieve, "dag": cmd_dag,
    }[args.cmd](args)


if __name__ == "__main__":
    raise SystemExit(main())

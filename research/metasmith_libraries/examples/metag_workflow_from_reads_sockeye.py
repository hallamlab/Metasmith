#!/usr/bin/env python3
import os
import sys
from pathlib import Path

if os.environ.get("MSM_SRC"):
    sys.path.insert(0, os.environ["MSM_SRC"])
from metasmith.python_api import (
    Agent, SshSource, Runtime,
    DataInstanceLibrary, TransformInstanceLibrary,
    TargetBuilder,
)

HPC_HOST      = os.environ.get("MSM_HPC_HOST", "sockeye")
SLURM_ACCOUNT = os.environ.get("MSM_SLURM_ACCOUNT", "<slurm-allocation>")
SETUP_COMMANDS = ["module load gcc/9.4.0", "module load apptainer"]

REF = Path(os.environ.get("MSM_REF_DB_DIR", "<ref-db-dir-on-cluster>"))
REMOTE_UNIREF50_DMND   = REF / "diamond"    / "uniref50.dmnd"
REMOTE_KOFAM_PROFILES  = REF / "kofamscan"  / "profiles.tgz"
REMOTE_KOFAM_KO_LIST   = REF / "kofamscan"  / "ko_list.tsv"
REMOTE_METABULI_REF    = REF / "metabuli"   / "gtdb"
REMOTE_GTDB            = REF / "gtdb"        / "release226"
REMOTE_PHYLOFLASH_DB   = REF / "phyloflash" / "138.2"

R1 = Path(os.environ.get("MSM_READS_R1", "<reads-R1.fq.gz>"))
R2 = Path(os.environ.get("MSM_READS_R2", "<reads-R2.fq.gz>"))
OUT_DIR = Path("results/metag_workflow_sockeye")

MLIB = Path(__file__).resolve().parents[3] / "src" / "metasmith_libraries"

SUBMIT = "--run" in sys.argv


def require_configured():
    unset = [k for k, v in {
        "MSM_SLURM_ACCOUNT": SLURM_ACCOUNT,
        "MSM_REF_DB_DIR":    str(REF),
        "MSM_READS_R1":      str(R1),
        "MSM_READS_R2":      str(R2),
    }.items() if "<" in str(v)]
    if unset:
        print("configure these first (env var, or edit the default in this file):",
              file=sys.stderr)
        for k in unset:
            print(f"  {k}", file=sys.stderr)
        raise SystemExit(2)


def ssh_capture(cmd: str) -> str:
    import subprocess
    return subprocess.run(
        ["ssh", HPC_HOST, cmd], capture_output=True, text=True, check=True
    ).stdout.strip()


def main():
    require_configured()
    user = ssh_capture("echo $USER")
    scratch = f"/scratch/{SLURM_ACCOUNT}/{user}"
    agent_home = f"{scratch}/metasmith"
    remote_dir = f"{scratch}/metag_workflow/inputs"
    remote_r1 = f"{remote_dir}/{R1.name}"
    remote_r2 = f"{remote_dir}/{R2.name}"

    out = OUT_DIR.resolve()
    out.mkdir(parents=True, exist_ok=True)

    inputs = DataInstanceLibrary(out / "inputs.xgdb")
    for tl in ["sequences.yml", "alignment.yml", "ref.yml", "annotation.yml",
               "taxonomy.yml", "binning.yml", "binning_local.yml"]:
        inputs.AddTypeLibrary(MLIB / "data_types" / tl)

    meta = inputs.AddValue("reads_metadata.json", {"parity": "paired", "length_class": "short"}, "sequences::read_metadata")
    pair = inputs.AddValue("read_pair.txt", "sample_1", "sequences::read_pair", parents={meta})
    inputs.AddItem(Path(remote_r1), "sequences::zipped_forward_short_reads", parents={pair})
    inputs.AddItem(Path(remote_r2), "sequences::zipped_reverse_short_reads", parents={pair})

    inputs.AddItem(REMOTE_UNIREF50_DMND,  "ref::uniref50_diamond_db")
    inputs.AddItem(REMOTE_KOFAM_PROFILES, "ref::kofamscan_profiles")
    inputs.AddItem(REMOTE_KOFAM_KO_LIST,  "ref::kofamscan_ko_list")
    inputs.AddItem(REMOTE_METABULI_REF,   "ref::metabuli_ref")
    inputs.AddItem(REMOTE_GTDB,           "ref::gtdb")
    inputs.AddItem(REMOTE_PHYLOFLASH_DB,  "ref::phyloflash_db")
    inputs.Save()

    smith = Agent(
        home=SshSource(host=HPC_HOST, path=agent_home).AsSource(),
        runtime=Runtime.APPTAINER,
        setup_commands=SETUP_COMMANDS,
    )

    targets = TargetBuilder()
    targets.Add("sequences::read_qc_stats")
    targets.Add("sequences::orfs")
    targets.Add("sequences::assembly_stats")
    targets.Add("sequences::assembly_per_contig_coverage")
    targets.Add("sequences::assembly_per_bp_coverage")
    targets.Add("annotation::diamond_uniref50_results")
    targets.Add("annotation::kofamscan_results")
    targets.Add("taxonomy::metabuli")
    targets.Add("taxonomy::phyloflash_summary")
    targets.Add("binning_local::cluster_table")

    mb_bin = targets.Add("sequences::metabat2_bin_fasta")
    sb_bin = targets.Add("sequences::semibin2_bin_fasta")
    cb_bin = targets.Add("sequences::comebin_bin_fasta")
    for parent in (mb_bin, sb_bin, cb_bin):
        targets.Add("taxonomy::checkm_stats", parents={parent})
        targets.Add("taxonomy::gtdbtk",       parents={parent})
    targets.Add("binning::metabat2_contig_to_bin_table")
    targets.Add("binning::semibin2_contig_to_bin_table")
    targets.Add("binning::comebin_contig_to_bin_table")

    print("==> GenerateWorkflow()", flush=True)
    task = smith.GenerateWorkflow(
        samples=list(inputs.AsSamples("sequences::read_metadata")),
        resources=[DataInstanceLibrary.Load(MLIB / "resources" / "env"), inputs],
        transforms=[
            TransformInstanceLibrary.Load(MLIB / "transforms" / "logistics"),
            TransformInstanceLibrary.Load(MLIB / "transforms" / "assembly"),
            TransformInstanceLibrary.Load(MLIB / "transforms" / "metagenomics"),
            TransformInstanceLibrary.Load(MLIB / "transforms" / "functionalAnnotation"),
        ],
        targets=targets,
    )
    if not task.ok or len(task.plan.steps) == 0:
        print(f"!! plan failed: hints={list(task.plan.hints)}", file=sys.stderr)
        return 3
    task.plan.RenderDAG(str(out / "dag"), format="svg")
    print(f"==> task key: {task.GetKey()}  steps={len(task.plan.steps)}", flush=True)

    if not SUBMIT:
        print("render-only; run metag_setup_sockeye.py --run first, then pass --run here to submit")
        return 0

    print("==> StageWorkflow(on_exist=clear)", flush=True)
    smith.StageWorkflow(task, on_exist="clear")

    print(f"==> RunWorkflow (SLURM, account={SLURM_ACCOUNT})", flush=True)
    smith.RunWorkflow(
        task,
        config_file=smith.GetNxfConfigPresets()["slurm"],
        params=dict(slurmAccount=SLURM_ACCOUNT),
    )

    print("==> WaitForWorkflow", flush=True)
    result = smith.WaitForWorkflow(task, timeout_s=43200.0, poll_s=30.0)
    print(f"==> status: {result['status']} after {result['elapsed_s']:.1f}s", flush=True)
    for line in result["tail"]:
        print(f"    {line}")
    if result["status"] != "completed":
        return 2

    src = smith.GetResultSource(task)
    print(f"==> result source: {src.GetPath()}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

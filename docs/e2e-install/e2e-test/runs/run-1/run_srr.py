"""metagenomics_from_paired_reads (no metabuli) on SRA run SRR9658108, on nibi.

Usage:
  python work/run_srr.py plan      # build the task and render the DAG only
  python work/run_srr.py setup     # SetupWorkflow (downloads/DB builds), wait
  python work/run_srr.py run       # StageWorkflow + RunWorkflow + WaitForWorkflow
"""
import json
import sys
from pathlib import Path

from metasmith.python_api import (
    Agent, DataInstanceLibrary, Runtime, Source, TargetBuilder,
    TransformInstanceLibrary,
)

ACCOUNT = "def-shallam_cpu"
AGENT_HOME = "/scratch/phyberos/msm_e2e/run-1"
ACC_FILE_REMOTE = f"{AGENT_HOME}/inputs/SRR9658108.txt"   # created by ssh step
LIB = Path("/home/tony/msm_e2e/run-1/sandbox/MetasmithLibraries")
OUT = Path("/home/tony/msm_e2e/run-1/sandbox/work/run_srr")
TASK_JSON = OUT / "task.txt"
PARAMS = {"slurmAccount": ACCOUNT}

smith = Agent(
    home=Source.FromSsh(host="nibi", path=AGENT_HOME),
    runtime=Runtime.APPTAINER,
    setup_commands=[
        "module load apptainer/1.3.5",
        f"export TMPDIR={AGENT_HOME}/tmp",
        "mkdir -p $TMPDIR",
        "export APPTAINER_TMPDIR=$TMPDIR",
    ],
)


def build_task():
    OUT.mkdir(parents=True, exist_ok=True)
    inputs = DataInstanceLibrary(OUT / "inputs.xgdb")
    for tl in ["sequences.yml", "alignment.yml", "ref.yml", "annotation.yml",
               "taxonomy.yml", "binning.yml", "binning_local.yml", "ncbi.yml"]:
        inputs.AddTypeLibrary(LIB / "data_types" / tl)
    # Same metadata lineage as the template, but reads come from SRA.
    meta = inputs.AddValue("reads_metadata.json",
                           {"parity": "paired", "length_class": "short"},
                           "sequences::read_metadata")
    pair = inputs.AddValue("read_pair.txt", "sample_1", "sequences::read_pair",
                           parents={meta})
    inputs.AddItem(Path(ACC_FILE_REMOTE), "ncbi::sra_accession", parents={meta})
    inputs.Save()

    # Template targets minus taxonomy::metabuli (index 7).
    targets = TargetBuilder()
    for t in ["sequences::read_qc_stats", "sequences::orfs",
              "sequences::assembly_stats", "sequences::assembly_per_contig_coverage",
              "sequences::assembly_per_bp_coverage",
              "annotation::diamond_uniref50_results", "annotation::kofamscan_results",
              "taxonomy::phyloflash_summary", "binning_local::cluster_table"]:
        targets.Add(t)
    mb = targets.Add("sequences::metabat2_bin_fasta")
    sb = targets.Add("sequences::semibin2_bin_fasta")
    cb = targets.Add("sequences::comebin_bin_fasta")
    for parent in (mb, sb, cb):
        targets.Add("taxonomy::checkm_stats", parents={parent})
        targets.Add("taxonomy::gtdbtk", parents={parent})
    for t in ["binning::metabat2_contig_to_bin_table",
              "binning::semibin2_contig_to_bin_table",
              "binning::comebin_contig_to_bin_table"]:
        targets.Add(t)

    task = smith.GenerateWorkflow(
        samples=list(inputs.AsSamples("sequences::read_metadata")),
        resources=[DataInstanceLibrary.Load(LIB / "resources" / "env"), inputs],
        transforms=[TransformInstanceLibrary.Load(LIB / "transforms" / n)
                    for n in ("logistics", "assembly", "metagenomics",
                              "functionalAnnotation")],
        targets=targets,
    )
    if not task.ok or len(task.plan.steps) == 0:
        print(f"!! plan failed: hints={list(task.plan.hints)}", file=sys.stderr)
        raise SystemExit(3)
    task.plan.RenderDAG(str(OUT / "dag"), format="svg")
    print(f"task key: {task.GetKey()} steps={len(task.plan.steps)}", flush=True)
    return task


def main(stage):
    if stage == "plan":
        build_task()
        return
    task = build_task()
    if stage == "setup":
        res = smith.SetupWorkflow(task, config_file=smith.GetNxfConfigPresets()["slurm"],
                                  params=PARAMS)
        print("setup:", json.dumps(res, default=str)[:2000], flush=True)
        return
    if stage == "run":
        smith.StageWorkflow(task, on_exist="update")
        smith.RunWorkflow(task, config_file=smith.GetNxfConfigPresets()["slurm"],
                          params=PARAMS)
        result = smith.WaitForWorkflow(task, timeout_s=43200.0, poll_s=60.0)
        print(f"status: {result['status']} after {result['elapsed_s']:.1f}s", flush=True)
        for line in result["tail"]:
            print(f"    {line}")
        print("result source:", smith.GetResultSource(task).GetPath(), flush=True)
        return
    raise SystemExit(f"unknown stage {stage}")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "plan")

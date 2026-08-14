#!/usr/bin/env python
"""Run eukaryotic transcriptomics pipeline (STAR + StringTie) on sockeye via SLURM."""
import sys
import time
sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

from pathlib import Path
from metasmith.python_api import (
    Agent, Runtime, Source, SshSource,
    DataInstanceLibrary, TransformInstanceLibrary,
    TargetBuilder, Resources, Size,
)

MLIB = Path(__file__).resolve().parent.parent.parent
SOCKEYE_STAGING = Path("/scratch/st-shallam-1/pwy_group/staging/porphyridium")
PORPHYRIDIUM_ACCESSION = "GCA_008690995.1"

SAMPLES = {
    "POR-0-1": ("POR-0-1-090325_S59_L001_R1_001.fastq.gz", "POR-0-1-090325_S59_L001_R2_001.fastq.gz"),
    "POR-0-2": ("POR-0-2-090325_S60_L001_R1_001.fastq.gz", "POR-0-2-090325_S60_L001_R2_001.fastq.gz"),
    "POR-0-3": ("POR-0-3-090325_S61_L001_R1_001.fastq.gz", "POR-0-3-090325_S61_L001_R2_001.fastq.gz"),
    "POR-S-1": ("POR-S-1-090325_S62_L001_R1_001.fastq.gz", "POR-S-1-090325_S62_L001_R2_001.fastq.gz"),
    "POR-S-2": ("POR-S-2-090325_S67_L001_R1_001.fastq.gz", "POR-S-2-090325_S67_L001_R2_001.fastq.gz"),
    "POR-S-3": ("POR-S-3-090325_S68_L001_R1_001.fastq.gz", "POR-S-3-090325_S68_L001_R2_001.fastq.gz"),
    "POR-8-1": ("POR-8-1-090325_S66_L001_R1_001.fastq.gz", "POR-8-1-090325_S66_L001_R2_001.fastq.gz"),
    "POR-8-2": ("POR-8-2-090325_S69_L001_R1_001.fastq.gz", "POR-8-2-090325_S69_L001_R2_001.fastq.gz"),
    "POR-8-3": ("POR-8-3-090325_S70_L001_R1_001.fastq.gz", "POR-8-3-090325_S70_L001_R2_001.fastq.gz"),
}


def main():
    print("=== Setting up sockeye agent ===")
    agent_home = SshSource(
        host="sockeye",
        path=Path("/scratch/st-shallam-1/pwy_group/metasmith"),
    ).AsSource()
    smith = Agent(
        home=agent_home,
        runtime=Runtime.APPTAINER,
        setup_commands=[
            'module load gcc/9.4.0',
            'module load apptainer/1.3.1',
        ],
    )
    print("Agent ready")

    print("\n=== Loading resources & transforms ===")
    base_res = [DataInstanceLibrary.Load(MLIB / "resources/env")]
    t_transforms = [
        TransformInstanceLibrary.Load(MLIB / "transforms/transcriptomics"),
        TransformInstanceLibrary.Load(MLIB / "transforms/logistics"),
    ]

    print("\n=== Creating input library ===")
    import tempfile
    tmp = Path(tempfile.mkdtemp())
    inputs_dir = tmp / "inputs.xgdb"
    inputs = DataInstanceLibrary(inputs_dir)
    for tl in ["sequences.yml", "ncbi.yml", "transcriptomics.yml"]:
        inputs.AddTypeLibrary(MLIB / "data_types" / tl)

    experiment = inputs.AddValue(
        "porphyridium_experiment.txt",
        "porphyridium_transcriptomics",
        "transcriptomics::experiment",
    )
    inputs.AddValue(
        "porphyridium_accession.txt",
        PORPHYRIDIUM_ACCESSION,
        "ncbi::assembly_accession",
        parents={experiment},
    )

    for sample_name, (r1_file, r2_file) in SAMPLES.items():
        r1_path = SOCKEYE_STAGING / r1_file
        r2_path = SOCKEYE_STAGING / r2_file
        pair = inputs.AddValue(
            f"{sample_name}_pair.txt", sample_name,
            "sequences::read_pair", parents={experiment},
        )
        inputs.AddItem(r1_path, "sequences::zipped_forward_short_reads", parents={pair})
        inputs.AddItem(r2_path, "sequences::zipped_reverse_short_reads", parents={pair})

    inputs.Save()

    print("\n=== Generating workflow ===")
    targets = TargetBuilder()
    targets.Add("transcriptomics::gene_count_table")
    task = smith.GenerateWorkflow(
        samples=list(inputs.AsSamples("transcriptomics::experiment")),
        resources=base_res + [inputs],
        transforms=t_transforms,
        targets=targets,
    )
    if not task.ok:
        print(f"FAILED: {task}")
        sys.exit(1)
    print(f"Plan has {len(task.plan.steps)} steps")

    print("\n=== Staging workflow ===")
    smith.StageWorkflow(task, on_exist="update", verify_external_paths=False)
    print("Staged")

    print("\n=== Running workflow (SLURM) ===")
    with open(MLIB / "secrets/slurm_account_sockeye") as f:
        SLURM_ACCOUNT = f.readline().strip()

    smith.RunWorkflow(
        task,
        config_file=smith.GetNxfConfigPresets()["slurm"],
        params=dict(slurmAccount=SLURM_ACCOUNT),
    )
    print("Workflow submitted to SLURM")

    print("\n=== Waiting for completion ===")
    results_path = smith.GetResultSource(task).GetPath()
    t0 = time.time()
    timeout = 86400  # 24h
    last_print = 0
    while not (results_path / "_metadata").exists():
        elapsed = time.time() - t0
        if elapsed > timeout:
            print(f"TIMEOUT after {timeout}s")
            sys.exit(1)
        if elapsed - last_print >= 300:
            print(f"  waiting... {elapsed/60:.0f}min elapsed")
            last_print = elapsed
        time.sleep(10)

    smith.CheckWorkflow(task)
    results = DataInstanceLibrary.Load(results_path)
    print(f"\nWorkflow completed in {(time.time()-t0)/60:.0f}min")

    print("\n=== Results ===")
    for path, type_name, endpoint in results.Iterate():
        full_path = path if path.is_absolute() else results_path / path
        print(f"  {type_name}: {full_path} (exists={full_path.exists()})")

    for path, type_name, endpoint in results.Iterate():
        if "gene_count_table" in type_name:
            full_path = path if path.is_absolute() else results_path / path
            if full_path.exists():
                with open(full_path) as f:
                    lines = f.readlines()
                print(f"\nGene count table: {len(lines)} lines")
                print(f"Header: {lines[0].strip()}")
                if len(lines) > 1:
                    print(f"First data row: {lines[1].strip()[:200]}")
                if len(lines) > 2:
                    print(f"Last data row: {lines[-1].strip()[:200]}")

    print("\nDone!")


if __name__ == "__main__":
    main()

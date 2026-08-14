#!/usr/bin/env python
"""Run Braker3 gene prediction + BUSCO completeness on Sockeye via SLURM.

Uses STAR BAMs + assembly from previous transcriptomics run (eguEpdhP)
and pprodigal ORFs from annotation run (l16XOO97).
"""
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

# Previous runs on Sockeye
PREV_RUN = Path("/scratch/st-shallam-1/pwy_group/metasmith/runs/eguEpdhP")
ANNOT_RUN = Path("/scratch/st-shallam-1/pwy_group/metasmith/runs/l16XOO97")

# Assembly from eguEpdhP
PREV_ASSEMBLY = PREV_RUN / "nxf_work/66/3de2dd0280f3835edc3892d31c4a03/1-1-1.f1CMorcneUoLGMna-O4PhHAkd.fna"

# 9 STAR BAMs from eguEpdhP (one per sample)
PREV_BAMS = [
    PREV_RUN / "nxf_work/c7/27b072e68fe94432f4bd5aeff7853a/1-1-1.g3ah0QAiGjmgmOQv-9mrjFffM.bam",
    PREV_RUN / "nxf_work/96/bdea4f2c6ee0a1f9cdd87aab997b6a/1-1-1.SZV4oNiEOp2dIAHQ-9mrjFffM.bam",
    PREV_RUN / "nxf_work/9f/74f4b648ffa56609b3594c02d491e8/1-1-1.iCaeK0EUwqlMC7ZR-9mrjFffM.bam",
    PREV_RUN / "nxf_work/b9/a898a97688fe08a07bb1b1ed98a308/1-1-1.SYqoyBxlvuPEILkU-9mrjFffM.bam",
    PREV_RUN / "nxf_work/5e/40e1c46ccdb86558659ffb71a1a691/1-1-1.L8rJHsIV6bp7TLk8-9mrjFffM.bam",
    PREV_RUN / "nxf_work/06/3995f70094ed2c82651a7bfaf0303a/1-1-1.UMK0uBW7MDfv9lR2-9mrjFffM.bam",
    PREV_RUN / "nxf_work/9d/cd2ddf9a908c2da567de42cd6fc146/1-1-1.4Ih5b6oUlLPBmjQj-9mrjFffM.bam",
    PREV_RUN / "nxf_work/20/ea324c10296ba3207878b381493b77/1-1-1.ZjUpCov8SiawjbWC-9mrjFffM.bam",
    PREV_RUN / "nxf_work/08/ccf1ede8ced5db4d626104e6a32976/1-1-1.Grq6HkY9LZzPhIwC-9mrjFffM.bam",
]

# ORFs from pprodigal (l16XOO97)
PREV_ORFS = ANNOT_RUN / "nxf_work/96/db239ab848b9bd6158e47a822a4cd6/1-1-1.SS41y8j3VqT9mmpP-28NRtNMg.faa"


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
        TransformInstanceLibrary.Load(MLIB / "transforms/functionalAnnotation"),
        TransformInstanceLibrary.Load(MLIB / "transforms/logistics"),
    ]

    print("\n=== Creating input library ===")
    import tempfile
    tmp = Path(tempfile.mkdtemp())
    inputs_dir = tmp / "inputs.xgdb"
    inputs = DataInstanceLibrary(inputs_dir)
    for tl in ["sequences.yml", "transcriptomics.yml", "annotation.yml"]:
        inputs.AddTypeLibrary(MLIB / "data_types" / tl)

    # Experiment grouping node
    experiment = inputs.AddValue(
        "porphyridium_experiment.txt",
        "porphyridium_transcriptomics",
        "transcriptomics::experiment",
    )

    # Assembly
    inputs.AddItem(PREV_ASSEMBLY, "sequences::assembly", parents={experiment})

    # STAR BAMs (for merge_bams → braker3)
    for bam in PREV_BAMS:
        inputs.AddItem(bam, "transcriptomics::star_bam", parents={experiment})

    # ORFs (for BUSCO protein mode)
    inputs.AddItem(PREV_ORFS, "sequences::orfs", parents={experiment})

    # BUSCO lineage download trigger
    inputs.AddValue(
        "busco_source.txt",
        "eukaryota_odb10",
        "annotation::busco_source",
    )

    inputs.Save()

    print("\n=== Generating workflow ===")
    targets = TargetBuilder()
    targets.Add("transcriptomics::braker3_gff")
    targets.Add("annotation::busco_results")
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
    for i, step in enumerate(task.plan.steps):
        print(f"  Step {i+1}: {step}")

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
    timeout = 259200  # 72h (Braker3 can be slow)
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

    print("\nDone!")


if __name__ == "__main__":
    main()

#!/usr/bin/env python
"""Submit BRaKER3-only workflow using cached all-9 merged BAM on Sockeye."""
import sys
from pathlib import Path

sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

from metasmith.python_api import (
    Agent,
    Runtime,
    DataInstanceLibrary,
    SshSource,
    TargetBuilder,
    TransformInstanceLibrary,
)

MLIB = Path(__file__).resolve().parent.parent.parent
ASSEMBLY = Path(
    "/arc/project/st-shallam-1/pwy_group/data/porphyridium_purpureum/"
    "eguEpdhP-intermediates/assembly/1-1-1.f1CMorcneUoLGMna-O4PhHAkd.fna"
)
CACHED_BAM = Path(
    "/scratch/st-shallam-1/pwy_group/metasmith/cache/merged_bams/"
    "porphyridium_all9_iNlpm1XR.bam"
)


def main() -> None:
    print("=== Setting up sockeye agent ===")
    agent_home = SshSource(
        host="sockeye",
        path=Path("/scratch/st-shallam-1/pwy_group/metasmith"),
    ).AsSource()
    smith = Agent(
        home=agent_home,
        runtime=Runtime.APPTAINER,
        setup_commands=[
            "module load gcc/9.4.0",
            "module load apptainer/1.3.1",
        ],
    )

    print("=== Loading resources & transforms ===")
    base_res = [DataInstanceLibrary.Load(MLIB / "resources/env")]
    t_transforms = [TransformInstanceLibrary.Load(MLIB / "transforms/transcriptomics")]

    print("=== Creating input library ===")
    import tempfile

    tmp = Path(tempfile.mkdtemp())
    inputs_dir = tmp / "inputs.xgdb"
    inputs = DataInstanceLibrary(inputs_dir)
    for tl in ["sequences.yml", "transcriptomics.yml"]:
        inputs.AddTypeLibrary(MLIB / "data_types" / tl)

    experiment = inputs.AddValue(
        "porphyridium_experiment.txt",
        "porphyridium_transcriptomics",
        "transcriptomics::experiment",
    )
    inputs.AddItem(ASSEMBLY, "sequences::assembly", parents={experiment})
    inputs.AddItem(CACHED_BAM, "transcriptomics::merged_bam", parents={experiment})
    inputs.Save()

    print("=== Generating workflow ===")
    targets = TargetBuilder()
    targets.Add("transcriptomics::braker3_gff")
    task = smith.GenerateWorkflow(
        samples=list(inputs.AsSamples("transcriptomics::experiment")),
        resources=base_res + [inputs],
        transforms=t_transforms,
        targets=targets,
    )
    if not task.ok:
        raise SystemExit(f"FAILED to generate workflow: {task}")

    print(f"Plan has {len(task.plan.steps)} steps")
    for step in task.plan.steps:
        name = Path(step.transform._path).stem
        prods = [i.dtype_name for g in step.produces for i in g]
        print(f"  Step {step.order}: {name} -> {prods}")

    print("=== Staging workflow ===")
    smith.StageWorkflow(task, on_exist="update", verify_external_paths=False)

    print("=== Submitting workflow (SLURM) ===")
    with open(MLIB / "secrets/slurm_account_sockeye") as f:
        slurm_account = f.readline().strip()

    smith.RunWorkflow(
        task,
        config_file=smith.GetNxfConfigPresets()["slurm"],
        params={"slurmAccount": slurm_account},
    )

    result_path = smith.GetResultSource(task).GetPath()
    print("Submitted BRaKER3 workflow")
    print(f"Result path: {result_path}")
    print("Monitor: ssh sockeye 'squeue -u txyliu'")


if __name__ == "__main__":
    main()

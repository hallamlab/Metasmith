#!/usr/bin/env python
"""Submit merge_bams-only workflow on Sockeye and exit after submission.

This isolates batching/merge behavior from downstream BRaKER3 steps.
"""
from pathlib import Path
from metasmith.python_api import (
    Agent,
    Runtime,
    SshSource,
    DataInstanceLibrary,
    TransformInstanceLibrary,
    TargetBuilder,
)

MLIB = Path(__file__).resolve().parent.parent.parent
ARC_STAR_BAMS = Path("/arc/project/st-shallam-1/pwy_group/data/porphyridium_purpureum/star_bams")

# 9 STAR BAMs staged on arc (same set used in run 62Tq8B53)
PREV_BAMS = [
    ARC_STAR_BAMS / "1-1-1.g3ah0QAiGjmgmOQv-9mrjFffM.bam",
    ARC_STAR_BAMS / "1-1-1.iCaeK0EUwqlMC7ZR-9mrjFffM.bam",
    ARC_STAR_BAMS / "1-1-1.SYqoyBxlvuPEILkU-9mrjFffM.bam",
    ARC_STAR_BAMS / "1-1-1.Grq6HkY9LZzPhIwC-9mrjFffM.bam",
    ARC_STAR_BAMS / "1-1-1.UMK0uBW7MDfv9lR2-9mrjFffM.bam",
    ARC_STAR_BAMS / "1-1-1.L8rJHsIV6bp7TLk8-9mrjFffM.bam",
    ARC_STAR_BAMS / "1-1-1.4Ih5b6oUlLPBmjQj-9mrjFffM.bam",
    ARC_STAR_BAMS / "1-1-1.ZjUpCov8SiawjbWC-9mrjFffM.bam",
    ARC_STAR_BAMS / "1-1-1.SZV4oNiEOp2dIAHQ-9mrjFffM.bam",
]


def main() -> None:
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

    base_res = [DataInstanceLibrary.Load(MLIB / "resources/env")]
    t_transforms = [TransformInstanceLibrary.Load(MLIB / "transforms/transcriptomics")]

    import tempfile

    tmp = Path(tempfile.mkdtemp())
    inputs_dir = tmp / "inputs.xgdb"
    inputs = DataInstanceLibrary(inputs_dir)
    inputs.AddTypeLibrary(MLIB / "data_types" / "transcriptomics.yml")

    experiment = inputs.AddValue(
        "porphyridium_experiment.txt",
        "porphyridium_transcriptomics",
        "transcriptomics::experiment",
    )
    for bam in PREV_BAMS:
        inputs.AddItem(bam, "transcriptomics::star_bam", parents={experiment})
    inputs.Save()

    targets = TargetBuilder()
    targets.Add("transcriptomics::merged_bam")

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

    smith.StageWorkflow(task, on_exist="update", verify_external_paths=False)

    with open(MLIB / "secrets/slurm_account_sockeye") as f:
        slurm_account = f.readline().strip()

    smith.RunWorkflow(
        task,
        config_file=smith.GetNxfConfigPresets()["slurm"],
        params={"slurmAccount": slurm_account},
    )

    result_path = smith.GetResultSource(task).GetPath()
    print("Submitted merge_bams workflow")
    print(f"Result path: {result_path}")
    print("Monitor: ssh sockeye 'squeue -u txyliu'")


if __name__ == "__main__":
    main()

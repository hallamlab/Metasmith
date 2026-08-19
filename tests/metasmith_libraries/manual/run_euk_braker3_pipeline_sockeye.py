#!/usr/bin/env python
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

ARC_DATA = Path("/arc/project/st-shallam-1/pwy_group/data/porphyridium_purpureum")
ARC_INTER = ARC_DATA / "eguEpdhP-intermediates"
PREV_ASSEMBLY = ARC_INTER / "assembly/1-1-1.f1CMorcneUoLGMna-O4PhHAkd.fna"

PREV_MERGED_BAMS = [
    Path("/scratch/st-shallam-1/pwy_group/metasmith/cache/merged_bams/porphyridium_all9_iNlpm1XR.bam"),
]

PREV_BAMS = [
    ARC_DATA / "star_bams/1-1-1.g3ah0QAiGjmgmOQv-9mrjFffM.bam",
    ARC_DATA / "star_bams/1-1-1.SZV4oNiEOp2dIAHQ-9mrjFffM.bam",
    ARC_DATA / "star_bams/1-1-1.iCaeK0EUwqlMC7ZR-9mrjFffM.bam",
    ARC_DATA / "star_bams/1-1-1.SYqoyBxlvuPEILkU-9mrjFffM.bam",
    ARC_DATA / "star_bams/1-1-1.L8rJHsIV6bp7TLk8-9mrjFffM.bam",
    ARC_DATA / "star_bams/1-1-1.UMK0uBW7MDfv9lR2-9mrjFffM.bam",
    ARC_DATA / "star_bams/1-1-1.4Ih5b6oUlLPBmjQj-9mrjFffM.bam",
    ARC_DATA / "star_bams/1-1-1.ZjUpCov8SiawjbWC-9mrjFffM.bam",
    ARC_DATA / "star_bams/1-1-1.Grq6HkY9LZzPhIwC-9mrjFffM.bam",
]


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
    ]

    print("\n=== Creating input library ===")
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

    inputs.AddItem(PREV_ASSEMBLY, "sequences::assembly", parents={experiment})

    for bam in PREV_MERGED_BAMS:
        inputs.AddItem(bam, "transcriptomics::merged_bam", parents={experiment})

    for bam in PREV_BAMS:
        inputs.AddItem(bam, "transcriptomics::star_bam", parents={experiment})

    inputs.Save()

    print("\n=== Generating workflow ===")
    targets = TargetBuilder()
    targets.Add("transcriptomics::braker3_gff")
    targets.Add("transcriptomics::braker3_proteins")
    targets.Add("transcriptomics::stringtie_gtf")
    targets.Add("transcriptomics::merged_gtf")
    targets.Add("transcriptomics::stringtie_quant_gtf")
    targets.Add("transcriptomics::gene_count_table")
    targets.Add("transcriptomics::diff_count_table")
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
    for step in task.plan.steps:
        name = Path(step.transform._path).stem
        prods = [i.dtype_name for g in step.produces for i in g]
        print(f"  Step {step.order}: {name} -> {prods}")

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
    timeout = 259200
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

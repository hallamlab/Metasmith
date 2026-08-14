#!/usr/bin/env python
"""Submit BRaKER3+downstream pipeline on Sockeye and exit after submission."""
import sys
import time
sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

from pathlib import Path
from metasmith.python_api import (
    Agent, Runtime, SshSource,
    DataInstanceLibrary, TransformInstanceLibrary,
    TargetBuilder,
)

MLIB = Path(__file__).resolve().parent.parent.parent

# Archived data on Sockeye/arc
ARC_DATA = Path("/arc/project/st-shallam-1/pwy_group/data/porphyridium_purpureum")
ARC_INTER = ARC_DATA / "eguEpdhP-intermediates"
ARC_LIB = Path("/arc/project/st-shallam-1/pwy_group/lib/annotation-dbs")

EGGNOG_DB = ARC_LIB / "eggnog"
PREV_ASSEMBLY = ARC_INTER / "assembly/1-1-1.f1CMorcneUoLGMna-O4PhHAkd.fna"

# Single all-9 BAM cached on Sockeye scratch
PREV_MERGED_BAMS = [
    Path("/scratch/st-shallam-1/pwy_group/metasmith/cache/merged_bams/porphyridium_all9_iNlpm1XR.bam"),
]

# 9 StringTie GTFs
PREV_GTFS = [
    ARC_INTER / "stringtie_gtfs/1-1-1.mIUVIDA5mZok47aj-pivRePzg.gtf",
    ARC_INTER / "stringtie_gtfs/1-1-1.BRMMpolIq2S9C2Pi-pivRePzg.gtf",
    ARC_INTER / "stringtie_gtfs/1-1-1.tQz1gW9ro4DfYSeH-pivRePzg.gtf",
    ARC_INTER / "stringtie_gtfs/1-1-1.pM4PRELz0cbDrCY0-pivRePzg.gtf",
    ARC_INTER / "stringtie_gtfs/1-1-1.MGgyTz89CDosELue-pivRePzg.gtf",
    ARC_INTER / "stringtie_gtfs/1-1-1.uXmlYWFLzxrEndox-pivRePzg.gtf",
    ARC_INTER / "stringtie_gtfs/1-1-1.LmDhfwQht1RjTcs2-pivRePzg.gtf",
    ARC_INTER / "stringtie_gtfs/1-1-1.w10NAtUononrGb7Z-pivRePzg.gtf",
    ARC_INTER / "stringtie_gtfs/1-1-1.SnuOuFlyA1RpXocm-pivRePzg.gtf",
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

    print("=== Loading resources & transforms ===")
    base_res = [DataInstanceLibrary.Load(MLIB / "resources/env")]
    t_transforms = [
        TransformInstanceLibrary.Load(MLIB / "transforms/transcriptomics"),
        TransformInstanceLibrary.Load(MLIB / "transforms/functionalAnnotation"),
        TransformInstanceLibrary.Load(MLIB / "transforms/logistics"),
    ]

    print("=== Creating input library ===")
    import tempfile
    tmp = Path(tempfile.mkdtemp())
    inputs_dir = tmp / "inputs.xgdb"
    inputs = DataInstanceLibrary(inputs_dir)
    for tl in ["sequences.yml", "transcriptomics.yml", "annotation.yml"]:
        inputs.AddTypeLibrary(MLIB / "data_types" / tl)

    experiment = inputs.AddValue(
        "porphyridium_experiment.txt",
        f"porphyridium_transcriptomics_{int(time.time())}",
        "transcriptomics::experiment",
    )
    inputs.AddItem(PREV_ASSEMBLY, "sequences::assembly", parents={experiment})

    for bam in PREV_MERGED_BAMS:
        inputs.AddItem(bam, "transcriptomics::merged_bam", parents={experiment})

    for gtf in PREV_GTFS:
        inputs.AddItem(gtf, "transcriptomics::stringtie_gtf", parents={experiment})

    inputs.AddItem(EGGNOG_DB, "annotation::eggnog_data")
    inputs.AddValue("busco_source.txt", "eukaryota_odb10", "annotation::busco_source")
    inputs.Save()

    print("=== Generating workflow ===")
    targets = TargetBuilder()
    targets.Add("transcriptomics::gene_count_table")
    targets.Add("transcriptomics::diff_count_table")
    targets.Add("transcriptomics::braker3_gff")
    targets.Add("annotation::eggnog_results")
    targets.Add("annotation::busco_results")

    task = smith.GenerateWorkflow(
        samples=list(inputs.AsSamples("transcriptomics::experiment")),
        resources=base_res + [inputs],
        transforms=t_transforms,
        targets=targets,
    )
    if not task.ok:
        raise SystemExit(f"FAILED: {task}")

    print(f"Plan has {len(task.plan.steps)} steps")
    for step in task.plan.steps:
        name = Path(step.transform._path).stem
        prods = [i.dtype_name for g in step.produces for i in g]
        print(f"  Step {step.order}: {name} -> {prods}")

    print("=== Staging workflow ===")
    smith.StageWorkflow(task, on_exist="update", verify_external_paths=False)

    with open(MLIB / "secrets/slurm_account_sockeye") as f:
        slurm_account = f.readline().strip()

    print("=== Submitting workflow (SLURM) ===")
    smith.RunWorkflow(
        task,
        config_file=smith.GetNxfConfigPresets()["slurm"],
        params=dict(slurmAccount=slurm_account),
    )

    result_path = smith.GetResultSource(task).GetPath()
    print("Workflow submitted to SLURM")
    print(f"Result path: {result_path}")
    print("Monitor: ssh sockeye 'squeue -u txyliu'")


if __name__ == "__main__":
    main()

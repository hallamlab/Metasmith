#!/usr/bin/env python
"""Wait for merge_bams output, then submit BRaKER3-only workflow on Sockeye.

Uses:
- merged BAM from run msrLr7rq (single experiment-level BAM)
- cached assembly from arc
"""
from __future__ import annotations

import subprocess
import time
from pathlib import Path

from metasmith.python_api import (
    Agent,
    Runtime,
    DataInstanceLibrary,
    SshSource,
    TargetBuilder,
    TransformInstanceLibrary,
)

MLIB = Path(__file__).resolve().parent.parent.parent
MERGE_RUN_RESULTS = Path("/scratch/st-shallam-1/pwy_group/metasmith/runs/msrLr7rq/results")
ASSEMBLY = Path(
    "/arc/project/st-shallam-1/pwy_group/data/porphyridium_purpureum/"
    "eguEpdhP-intermediates/assembly/1-1-1.f1CMorcneUoLGMna-O4PhHAkd.fna"
)


def find_merged_bam() -> Path | None:
    cmd = (
        "ssh sockeye "
        "\"find /scratch/st-shallam-1/pwy_group/metasmith/runs/msrLr7rq/results "
        "-type f -name '*-iNlpm1XR.bam' | head -n1\""
    )
    out = subprocess.check_output(cmd, shell=True, text=True).strip()
    return Path(out) if out else None


def wait_for_merged_bam(timeout_seconds: int = 6 * 3600, poll_seconds: int = 30) -> Path:
    t0 = time.time()
    while True:
        bam = find_merged_bam()
        if bam is not None:
            return bam
        elapsed = int(time.time() - t0)
        if elapsed > timeout_seconds:
            raise TimeoutError("Timed out waiting for merged BAM in msrLr7rq results")
        if elapsed % 300 < poll_seconds:
            print(f"Waiting for merged BAM... {elapsed // 60} min elapsed")
        time.sleep(poll_seconds)


def main() -> None:
    print("=== Waiting for merge output ===")
    merged_bam = wait_for_merged_bam()
    print(f"Found merged BAM: {merged_bam}")

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
    inputs.AddItem(merged_bam, "transcriptomics::merged_bam", parents={experiment})
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

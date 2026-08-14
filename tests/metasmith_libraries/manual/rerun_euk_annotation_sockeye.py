#!/usr/bin/env python
"""Re-run eggNOG annotation using previously downloaded DB + pprodigal ORFs.

The downloadEggnogDB step exited with code 1 despite the DB being fully
downloaded/decompressed. This script provides the DB and ORFs directly
as inputs so we only need to run eggnog_mapper.
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

# Pre-existing data on Sockeye from previous runs
PREV_RUN = Path("/scratch/st-shallam-1/pwy_group/metasmith/runs/eguEpdhP")
ANNOT_RUN = Path("/scratch/st-shallam-1/pwy_group/metasmith/runs/l16XOO97")

# eggNOG DB (downloaded in l16XOO97, all files present despite exit code 1)
EGGNOG_DB = ANNOT_RUN / "nxf_work/da/524b2239d32eb1515de4c2ed2f01a2/1-1-1.nO6YGyJx7w2CFYPZ-KIapn0ju"
# ORFs from pprodigal (completed in l16XOO97)
ORFS = ANNOT_RUN / "nxf_work/96/db239ab848b9bd6158e47a822a4cd6/1-1-1.SS41y8j3VqT9mmpP-28NRtNMg.faa"


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
        TransformInstanceLibrary.Load(MLIB / "transforms/functionalAnnotation"),
    ]

    print("\n=== Creating input library ===")
    import tempfile
    tmp = Path(tempfile.mkdtemp())
    inputs_dir = tmp / "inputs.xgdb"
    inputs = DataInstanceLibrary(inputs_dir)
    for tl in ["sequences.yml", "annotation.yml"]:
        inputs.AddTypeLibrary(MLIB / "data_types" / tl)

    # Provide pre-computed inputs directly
    inputs.AddItem(ORFS, "sequences::orfs")
    inputs.AddItem(EGGNOG_DB, "annotation::eggnog_data")

    inputs.Save()

    print("\n=== Generating workflow ===")
    targets = TargetBuilder()
    targets.Add("annotation::eggnog_results")
    task = smith.GenerateWorkflow(
        samples=list(inputs.AsSamples("sequences::orfs")),
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
    timeout = 172800  # 48h
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

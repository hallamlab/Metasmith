#!/usr/bin/env python
"""Debug download transforms on Sockeye using local executor.

Runs downloadBuscoLineage and downloadEggnogDB one at a time with verbose output.
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


def run_single_target(target_name, target_type):
    print(f"\n{'='*60}")
    print(f"=== Testing {target_name} ===")
    print(f"{'='*60}")

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

    base_res = [DataInstanceLibrary.Load(MLIB / "resources/env")]
    t_transforms = [
        TransformInstanceLibrary.Load(MLIB / "transforms/logistics"),
    ]

    import tempfile
    tmp = Path(tempfile.mkdtemp())
    inputs_dir = tmp / "inputs.xgdb"
    inputs = DataInstanceLibrary(inputs_dir)
    inputs.AddTypeLibrary(MLIB / "data_types" / "annotation.yml")

    inputs.AddValue("eggnog_source.txt", "eggnog", "annotation::eggnog_source")
    inputs.AddValue("busco_source.txt", "eukaryota_odb10", "annotation::busco_source")
    inputs.Save()

    targets = TargetBuilder()
    targets.Add(target_type)
    task = smith.GenerateWorkflow(
        samples=list(inputs.AsSamples("annotation::eggnog_source")),
        resources=base_res + [inputs],
        transforms=t_transforms,
        targets=targets,
    )
    if not task.ok:
        print(f"FAILED to generate workflow: {task}")
        return False

    print(f"Plan has {len(task.plan.steps)} steps")
    for step in task.plan.steps:
        name = Path(step.transform._path).stem
        prods = [i.dtype_name for g in step.produces for i in g]
        print(f"  Step {step.order}: {name} -> {prods}")

    smith.StageWorkflow(task, on_exist="update", verify_external_paths=False)
    print("Staged")

    # Use local executor (no SLURM) for debugging
    with open(MLIB / "secrets/slurm_account_sockeye") as f:
        SLURM_ACCOUNT = f.readline().strip()

    smith.RunWorkflow(
        task,
        config_file=smith.GetNxfConfigPresets()["local"],
        params=dict(slurmAccount=SLURM_ACCOUNT),
    )
    print("Workflow running (local executor)")

    results_path = smith.GetResultSource(task).GetPath()
    t0 = time.time()
    timeout = 7200  # 2h
    last_print = 0
    while not (results_path / "_metadata").exists():
        elapsed = time.time() - t0
        if elapsed > timeout:
            print(f"TIMEOUT after {timeout}s")
            return False
        if elapsed - last_print >= 60:
            print(f"  waiting... {elapsed/60:.0f}min elapsed")
            last_print = elapsed
        time.sleep(5)

    smith.CheckWorkflow(task)
    results = DataInstanceLibrary.Load(results_path)
    print(f"\nCompleted in {(time.time()-t0)/60:.0f}min")

    print("\n=== Results ===")
    count = 0
    for path, type_name, endpoint in results.Iterate():
        full_path = path if path.is_absolute() else results_path / path
        print(f"  {type_name}: {full_path} (exists={full_path.exists()})")
        count += 1

    if count == 0:
        print("  NO RESULTS - transform likely failed")
        return False
    return True


def main():
    # Test eggnog (BUSCO already confirmed working)
    ok_eggnog = run_single_target("downloadEggnogDB", "annotation::eggnog_data")

    if not ok_eggnog:
        print("\n*** EggNOG download failed. ***")
        sys.exit(1)

    print("\n=== EggNOG download succeeded! ===")


if __name__ == "__main__":
    main()

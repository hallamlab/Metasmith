"""
Run ora2fastq workflow on sockeye HPC cluster via Metasmith.

Usage:
    conda run -n msm_env python tests/run_ora2fastq_sockeye.py
"""
import subprocess
from pathlib import Path
from metasmith.python_api import (
    Agent,
    Runtime,
    Source,
    DataInstanceLibrary,
    TransformInstanceLibrary,
    TargetBuilder,
    Resources,
    Size,
    Duration,
)
import time

MLIB = Path(__file__).parent.parent
SOCKEYE_AGENT_PATH = "/scratch/st-shallam-1/pwy_group/metasmith"
SOCKEYE_DATA_PATH = "/scratch/st-shallam-1/pwy_group/metasmith/data/ora2fastq_test"
# oradata extracted from orad container (needed for Apptainer — Docker has it internally)
SOCKEYE_ORADATA_PATH = "/scratch/st-shallam-1/pwy_group/metasmith/data/orad_data/oradata"
# Input library lives locally; item paths point to sockeye (already transferred)
LOCAL_INPUTS_PATH = Path(__file__).parent / "test_msm_home" / "ora2fastq_inputs.xgdb"
SLURM_ACCOUNT = "st-shallam-1"

agent = Agent(
    home=Source.FromSsh("sockeye", SOCKEYE_AGENT_PATH),
    runtime=Runtime.APPTAINER,
    setup_commands=[
        "module load gcc/9.4.0",
        "module load apptainer/1.3.1",
    ],
)

# Build input library locally; item paths are absolute paths on sockeye
# (ORA files already rsync'd there — StageWorkflow won't re-copy absolute paths)
LOCAL_INPUTS_PATH.parent.mkdir(parents=True, exist_ok=True)
inputs = DataInstanceLibrary(LOCAL_INPUTS_PATH)
inputs.AddTypeLibrary(MLIB / "data_types/sequences.yml")

meta = inputs.AddValue(
    "reads_metadata.json",
    {"parity": "paired", "length_class": "short"},
    "sequences::read_metadata",
)
pair = inputs.AddValue(
    "read_pair.json",
    {"atomic": "a pair of read files"},
    "sequences::read_pair",
    parents={meta},
)
inputs.AddItem(
    Path(SOCKEYE_DATA_PATH) / "S485_S64_L006_R1_001.fastq.ora",
    "sequences::forward_ora_reads",
    parents={pair},
)
inputs.AddItem(
    Path(SOCKEYE_DATA_PATH) / "S485_S64_L006_R2_001.fastq.ora",
    "sequences::reverse_ora_reads",
    parents={pair},
)
inputs.Save()

resources = [
    DataInstanceLibrary.Load(MLIB / "resources/env"),
    inputs,
]

transforms = [
    TransformInstanceLibrary.Load(MLIB / "transforms/logistics"),
]

targets = TargetBuilder()
targets.Add("sequences::short_reads")

print("Generating workflow...")
task = agent.GenerateWorkflow(
    samples=list(inputs.AsSamples("sequences::read_metadata")),
    resources=resources,
    transforms=transforms,
    targets=targets,
)
assert task.ok, f"Workflow generation failed: {task}"
print(f"Plan: {len(task.plan.steps)} step(s), key={task.GetKey()}")

print("Staging workflow...")
agent.StageWorkflow(task, on_exist="clear")

# Apptainer on HPC can't read /app/oradata inside the container (root-owned).
# Patch the Nextflow config after staging to bind mount the pre-extracted oradata.
run_key = task.GetKey()
nxf_config_path = f"/scratch/st-shallam-1/pwy_group/metasmith/runs/{run_key}/workflow.config.nf"
patch = (
    "\n// Apptainer bind mount for orad reference genome (HPC only)\n"
    "process {\n"
    "    withName: 'p01__ora2fastq' {\n"
    f"        containerOptions = '--bind {SOCKEYE_ORADATA_PATH}:/app/oradata'\n"
    "    }\n"
    "}\n"
)
subprocess.run(
    ["ssh", "sockeye", f"cat >> {nxf_config_path}"],
    input=patch.encode(),
    check=True,
)
print(f"Patched workflow.config.nf for run [{run_key}]")

print("Running workflow on sockeye SLURM...")
agent.RunWorkflow(
    task,
    config_file=agent.GetNxfConfigPresets()["slurm"],
    params=dict(
        slurmAccount=SLURM_ACCOUNT,
        executor=dict(queueSize=10),
        process=dict(tries=1, cpus=4, memory="16 GB", time="3hours"),
    ),
)

# Poll for completion
print("Waiting for workflow to complete...")
results_source = agent.GetResultSource(task)
results_path = results_source.GetPath()
start = time.time()
timeout = 7200  # 2 hours

while not (results_path / "_metadata").exists():
    elapsed = int(time.time() - start)
    print(f"  waiting... ({elapsed}s)")
    if elapsed > timeout:
        raise TimeoutError("Workflow did not complete within 2 hours")
    time.sleep(30)

agent.CheckWorkflow(task)
results = DataInstanceLibrary.Load(results_path)

print("\nResults:")
found_reads = False
for path, type_name, endpoint in results.Iterate():
    if "short_reads" in type_name:
        found_reads = True
        full_path = path if path.is_absolute() else results_path / path
        size = full_path.stat().st_size if full_path.exists() else -1
        print(f"  {type_name}: {full_path} ({size:,} bytes)")

if found_reads:
    print("\nSUCCESS: ora2fastq workflow completed on sockeye.")
else:
    print("\nFAILED: no short_reads found in results.")

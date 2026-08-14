"""Pull transcriptomics-related containers on sockeye (login node, local executor)."""
import sys
from pathlib import Path

from metasmith.python_api import (
    Agent, Runtime, Source, SshSource,
    DataInstanceLibrary, TransformInstanceLibrary, TargetBuilder,
)

MLIB = Path(__file__).resolve().parent.parent.parent

agent_home = SshSource(host="sockeye", path=Path("/scratch/st-shallam-1/pwy_group/metasmith")).AsSource()
smith = Agent(
    home=agent_home,
    runtime=Runtime.APPTAINER,
    setup_commands=[
        'module load gcc/9.4.0',
        'module load apptainer/1.3.1',
    ],
)

containers = DataInstanceLibrary.Load(MLIB / "resources/env")
logistics = TransformInstanceLibrary.Load(MLIB / "transforms/logistics")

targets = TargetBuilder()
targets.Add("env::pulled_container")

WHITELIST = {Path(f"{n}.env") for n in [
    "star",
    "stringtie",
    "seqkit",
    "salmon",
    "python_for_data_science",
    "ncbi-datasets",
    "samtools",
    "gffread",
    "eggnog-mapper",
    "braker3",
    "busco",
    "pydeseq2",
]}

samples = [
    x for x in containers.AsSamples("env::env")
    if len(x._mask.intersection(WHITELIST)) > 0
]
print(f"Pulling {len(samples)} containers: {[str(p) for s in samples for p in s._mask]}")

task = smith.GenerateWorkflow(
    samples=samples,
    resources=[],
    transforms=[logistics],
    targets=targets,
)
print(f"Plan OK={task.ok}, steps={len(task.plan.steps)}")

smith.StageWorkflow(task, on_exist="update")

params = dict(
    executor=dict(
        cpus=4,
        memory='8 GB',
        queueSize=len(samples),
    ),
)
smith.RunWorkflow(
    task,
    config_file=smith.GetNxfConfigPresets()["local"],
    params=params,
)

import os, sys
from pathlib import Path
from metasmith.python_api import Agent, TargetBuilder, Source, SshSource, DataInstanceLibrary, TransformInstanceLibrary, DataTypeLibrary
from metasmith.python_api import Resources, Size, Duration
from metasmith.python_api import Runtime

MLIB = Path("/home/tony/workspace/tools/MetasmithLibraries")

base_dir = Path("./cache")

agent_home = Source.FromLocal((base_dir/"local_home").resolve())
smith = Agent(
    home = agent_home,
    runtime=Runtime.APPTAINER,
)


notebook_name = Path(__file__).stem
in_dir = base_dir/f"{notebook_name}/inputs.xgdb"

containers = DataInstanceLibrary.Load(MLIB/"resources/env")
logistics = TransformInstanceLibrary.Load(MLIB/f"transforms/logistics")

targets = TargetBuilder()
targets.Add("env::pulled_container")

task = smith.GenerateWorkflow(
    samples=containers.AsSamples("env::env"),
    resources=[],
    transforms=[logistics],
    targets=targets,
)
task.plan.RenderDAG("./cache/pull_dag.svg", blacklist_namespaces=set())
print(task.ok, len(task.plan.steps))

smith.StageWorkflow(task, on_exist="update")

params = dict(
    executor=dict(
        cpus=15,
        memory='20 GB',
        queueSize=5,
    ),
)
smith.RunWorkflow(
    task,
    config_file=smith.GetNxfConfigPresets()["local"],
    params=params,
    resource_overrides={
        "all": Resources(
            memory=Size.GB(2),
            cpus=2,
        )   
    }
)

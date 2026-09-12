import os, sys
from pathlib import Path
from metasmith.python_api import Agent, Source, SshSource, DataInstanceLibrary, TransformInstanceLibrary, DataTypeLibrary
from metasmith.python_api import Resources, Size, Duration, TargetBuilder
from metasmith.python_api import Runtime
from metasmith.hashing import KeyGenerator

base_dir = Path("./cache")

agent_home = Source.FromLocal((base_dir/"local_home").resolve())
smith = Agent(
    home = agent_home,
    runtime=Runtime.DOCKER,
)


notebook_name = Path(__file__).stem
test="prodigal"
local = Path("./cache/example_assemblies").absolute()
assert local.exists()
in_dir = base_dir/f"{notebook_name}/inputs.{test}.xgdb"
try:
    inputs = DataInstanceLibrary.Load(in_dir)
except:
    inputs = DataInstanceLibrary(in_dir)
    inputs.Purge()
    inputs.AddTypeLibrary(namespace="sequences", lib=DataTypeLibrary.Load("../data_types/sequences.yml"))
    inputs.AddTypeLibrary(namespace="ncbi", lib=DataTypeLibrary.Load("../data_types/ncbi.yml"))
    inputs.AddItem(local/"Ana_PS.fna", "sequences::assembly")
    inputs.Save()


resources = [
    DataInstanceLibrary.Load(f"../resources/{n}")
    for n in [
        "env",
    ]
]

transforms = [
    TransformInstanceLibrary.Load(f"../transforms/{n}")
    for n in [
        "logistics",
        "metagenomics",
    ]
]

targets = TargetBuilder()
targets.Add("taxonomy::genomad_virus_summary")
targets.Add("taxonomy::genomad_plasmid_summary")

task = smith.GenerateWorkflow(
    samples=inputs.AsSamples("sequences::assembly"),
    resources=resources,
    transforms=transforms,
    targets=targets,
)
p = task.plan.RenderDAG(base_dir/f"{notebook_name}/dag")
print(task.ok, len(task.plan.steps))
print(p)
print(f"task: {task.GetKey()}, input {in_dir}")

smith.StageWorkflow(task, on_exist="clear", verify_external_paths=False)

with open("../secrets/slurm_account_sockeye") as f:
    SLURM_ACCOUNT = f.readline()
params = dict(
    slurmAccount=SLURM_ACCOUNT,
    executor=dict(
        cpus=15,
        memory='6 GB',
        queueSize=3,
    ),
)
smith.RunWorkflow(
    task=task,
    config_file=smith.GetNxfConfigPresets()["local"],
    params=params,
    resource_overrides={
        "all": Resources(
            memory=Size.MB(1),
            cpus=15,
        ),
    }
)

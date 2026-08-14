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
    # runtime=Runtime.APPTAINER,
    runtime=Runtime.DOCKER,
)

# agent_home = SshSource(host="sockeye", path=Path("/scratch/st-shallam-1/pwy_group/metasmith")).AsSource()
# smith = Agent(
#     home = agent_home,
#     runtime=Runtime.APPTAINER,
#     setup_commands=[
#         'module load gcc/9.4.0',
#         'module load apptainer/1.3.1',
#     ]
# )
# smith.Deploy(assertive=True)

# import ipynbname
# notebook_name = ipynbname.name()
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

# inputs = DataInstanceLibrary.Load(in_dir)

resources = [
    DataInstanceLibrary.Load(f"../resources/{n}")
    for n in [
        "env",
        # "lib",
    ]
]

transforms = [
    TransformInstanceLibrary.Load(f"../transforms/{n}")
    for n in [
        "logistics",
        # "assembly",
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
# task.SaveAs(Source.FromLocal(Path("./cache/test.task").absolute()))
# p = task.plan._solver_result.RenderDAG(base_dir/f"{notebook_name}/dag_raw")
p = task.plan.RenderDAG(base_dir/f"{notebook_name}/dag")
print(task.ok, len(task.plan.steps))
print(p)
print(f"task: {task.GetKey()}, input {in_dir}")

# smith.StageWorkflow(task, on_exist="update", verify_external_paths=True)
smith.StageWorkflow(task, on_exist="clear", verify_external_paths=False)

# with open("../secrets/slurm_account_fir") as f:
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
    # config_file=smith.GetNxfConfigPresets()["slurm"],
    config_file=smith.GetNxfConfigPresets()["local"],
    params=params,
    # stub_delay=1,
    resource_overrides={
        "all": Resources(
            memory=Size.MB(1),
            cpus=15,
        ),
        # transforms[0]["getNcbiSra.py"]: Resources(
        #     memory=Size.GB(2),
        #     cpus=5,
        # ),
    }
)

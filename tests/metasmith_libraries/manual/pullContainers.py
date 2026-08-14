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
    # runtime=Runtime.DOCKER,
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

# agent_home = SshSource(host="fir", path=Path("/scratch/phyberos/metasmith")).AsSource()
# smith = Agent(
#     home = agent_home,
#     runtime=Runtime.APPTAINER,
#     setup_commands=[
#         'module load gcc/9.4.0',
#         'module load apptainer/1.3.1',
#     ]
# )

# smith.Deploy(assertive=True)
# sys.exit(0)

notebook_name = Path(__file__).stem
in_dir = base_dir/f"{notebook_name}/inputs.xgdb"

containers = DataInstanceLibrary.Load(MLIB/"resources/env")
logistics = TransformInstanceLibrary.Load(MLIB/f"transforms/logistics")

targets = TargetBuilder()
targets.Add("env::pulled_container")

# WL = {Path(f"{n}.env") for n in [
#     # "gtdbtk",
#     "fastani",
# ]}
# samples = [x for x in containers.AsSamples("env::env") if len(x._mask.intersection(WL))>0]
task = smith.GenerateWorkflow(
    # samples=samples,
    samples=containers.AsSamples("env::env"),
    resources=[],
    transforms=[logistics],
    # targets=[inputs.GetType("sequences::gbk")]
    targets=targets,
)
task.plan.RenderDAG("./cache/pull_dag.svg", blacklist_namespaces=set())
print(task.ok, len(task.plan.steps))

# smith.StageWorkflow(task, on_exist="clear")
smith.StageWorkflow(task, on_exist="update")
# smith.StageWorkflow(task, on_exist="update_workflow")

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

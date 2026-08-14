import os, sys
from pathlib import Path
from metasmith.python_api import Agent, Source, SshSource, DataInstanceLibrary, TransformInstanceLibrary, DataTypeLibrary
from metasmith.python_api import Resources, Size, Duration, TargetBuilder
from metasmith.python_api import Runtime

base_dir = Path("./cache")

# agent_home = Source.FromLocal((base_dir/"local_home").resolve())
agent_home = Source.FromLocal((base_dir/"local_home").absolute())
smith = Agent(
    home = agent_home,
    # runtime=Runtime.APPTAINER,
    runtime=Runtime.DOCKER,
)

# agent_home = SshSource(host="chamois", path=Path("/home/tliu/metasmith")).AsSource()
# smith = Agent(
#     home = agent_home,
#     runtime=Runtime.APPTAINER,
#     setup_commands=[
#         'PATH=/home/tliu/miniforge3/envs/def/bin/:$PATH',
#         'export TMPDIR="/home/$USER/tmp"',
#         'mkdir -p $TMPDIR',
#         'export APPTAINER_CACHEDIR="$TMPDIR"',
#         'export APPTAINER_TMPDIR="$TMPDIR"',
#     ]
# )

# agent_home = SshSource(host="sockeye", path=Path("/scratch/st-shallam-1/pwy_group/metasmith")).AsSource()
# smith = Agent(
#     home = agent_home,
#     runtime=Runtime.APPTAINER,
#     setup_commands=[
#         'module load gcc/9.4.0',
#         'module load apptainer/1.3.1',
#         'export APPTAINER_CACHEDIR="/scratch/st-shallam-1/pwy_group/apptainer_cache"',
#     ]
# )

# agent_home = SshSource(host="fir", path=Path("/scratch/phyberos/metasmith")).AsSource()
# smith = Agent(
#     home = agent_home,
#     runtime=Runtime.APPTAINER,
#     setup_commands=[
#         "module load StdEnv/2023",
#         "module load apptainer/1.3.5",
#     ]
# )

smith.Deploy()

# import ipynbname
# notebook_name = ipynbname.name()
notebook_name = Path(__file__).stem
in_dir = base_dir/f"{notebook_name}/inputs.xgdb"

try:
    inputs = DataInstanceLibrary.Load(in_dir)
except:
    inputs = DataInstanceLibrary(in_dir)
    inputs.Purge()
    inputs.AddTypeLibrary(namespace="ncbi", lib=DataTypeLibrary.Load("../data_types/ncbi.yml"))
    inputs.AddTypeLibrary(namespace="sequences", lib=DataTypeLibrary.Load("../data_types/sequences.yml"))
    inputs.AddTypeLibrary(namespace="pangenome", lib=DataTypeLibrary.Load("../data_types/pangenome.yml"))

    group = inputs.AddValue("pangenome", "e coli", "pangenome::pangenome")
    _DH10b_name = inputs.AddValue("DH10b.name", "DH10b", "ncbi::genome_name", parents={group})
    inputs.AddValue("DH10b", "GCF_000019425.1", "ncbi::assembly_accession", parents={_DH10b_name})
    _K12_name = inputs.AddValue("K12.name", "K12", "ncbi::genome_name", parents={group})
    inputs.AddValue("K12", "GCF_000005845.2", "ncbi::assembly_accession", parents={_K12_name})
    _EPI300_name = inputs.AddValue("EPI300.name", "EPI300", "ncbi::genome_name", parents={group})
    inputs.AddValue("EPI300", "GCA_052692645.1", "ncbi::assembly_accession", parents={_EPI300_name})
    # inputs.AddItem((base_dir/f"{notebook_name}/epi300.gbk").resolve(), "sequences::gbk", parents={group})
    # inputs.LocalizeContents()
    inputs.Save()

# inputs = DataInstanceLibrary.Load(in_dir)

resources = [
    DataInstanceLibrary.Load(f"../resources/{n}")
    for n in ["env", "lib"]
]

transforms = [
    TransformInstanceLibrary.Load(f"../transforms/{n}")
    for n in ["logistics", "pangenome"]
]

targets = TargetBuilder()
targets.Add("pangenome::heatmap")
# targets.Add("sequences::orfs")

task = smith.GenerateWorkflow(
    samples=inputs.AsSamples("ncbi::assembly_accession"),
    resources=resources,
    transforms=transforms,
    targets=targets,
)
task.plan.RenderDAG(base_dir/f"{notebook_name}/dag")
print(task.ok, len(task.plan.steps))

# # smith.StageWorkflow(task, on_exist="clear")
smith.StageWorkflow(task, on_exist="update")
params = dict(
    executor=dict(
        cpus=14,
        queueSize=3,
    ),
    process=dict(
        tries=1,
    ),
)
smith.RunWorkflow(
    task=task,
    config_file=smith.GetNxfConfigPresets()["local"],
    params=params,
    resource_overrides={
        "*": Resources(
            memory=Size.GB(1),
            cpus=14,
        ),
        "getNcbiAssembly": Resources(
            memory=Size.GB(1),
            cpus=2,
        )
    },
)

# with open("../secrets/slurm_account_fir") as f:
#     SLURM_ACCOUNT = f.readline()
# smith.RunWorkflow(
#     task,
#     config_file=smith.GetNxfConfigPresets()["slurm"],
#     params=dict(
#         slurmAccount=SLURM_ACCOUNT,
#     )
# )

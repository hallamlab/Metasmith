import os, sys
from pathlib import Path
from metasmith.python_api import Agent, Source, SshSource, DataInstanceLibrary, TransformInstanceLibrary, DataTypeLibrary
from metasmith.python_api import Resources, Size, Duration, TargetBuilder
from metasmith.python_api import Runtime

base_dir = Path("./cache")

agent_home = Source.FromLocal((base_dir/"local_home").absolute())
smith = Agent(
    home = agent_home,
    runtime=Runtime.DOCKER,
)


smith.Deploy()

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
    inputs.Save()


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

task = smith.GenerateWorkflow(
    samples=inputs.AsSamples("ncbi::assembly_accession"),
    resources=resources,
    transforms=transforms,
    targets=targets,
)
task.plan.RenderDAG(base_dir/f"{notebook_name}/dag")
print(task.ok, len(task.plan.steps))

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

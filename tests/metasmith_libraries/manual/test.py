from pathlib import Path
from metasmith.python_api import Agent, Runtime
from metasmith.python_api import DataTypeLibrary, DataInstanceLibrary, TransformInstanceLibrary
from metasmith.python_api import Source, Logistics
from metasmith.python_api import TargetBuilder, Resources, Size, Duration
from metasmith.python_api import ipynbButtonLink

WORKSPACE = Path("/home/tony/workspace/tools/Metasmith/scratch/test_ws")
MLIB = WORKSPACE/"MetasmithLibraries"

ani_types_path = WORKSPACE/"ani_types.yml"
ani_types = DataTypeLibrary.Load(ani_types_path)
for name, model in ani_types:
    print(name, model)


ani_transforms_path = WORKSPACE/"ani_transforms"
ani_transforms = TransformInstanceLibrary(ani_transforms_path)
ani_transforms.AddTypeLibrary(lib=ani_types, namespace="ani")
ani_transforms.AddTypeLibrary(MLIB/"data_types/sequences.yml")
ani_transforms.AddTypeLibrary(MLIB/"data_types/pangenome.yml")
ani_transforms.AddStub("fastani")
ani_transforms.Save()

inputs_path = WORKSPACE/"ani_test_inputs"
try:
    inputs = DataInstanceLibrary.Load(inputs_path)
except:
    inputs = DataInstanceLibrary(inputs_path)
    inputs.AddTypeLibrary(MLIB/"data_types/pangenome.yml")
    inputs.AddTypeLibrary(MLIB/"data_types/ncbi.yml")
    inputs.AddTypeLibrary(ani_types, namespace="ani")

    group = inputs.AddValue("pangenome", "e coli", "pangenome::pangenome")
    _DH10b_name = inputs.AddValue("DH10b.name", "DH10b", "ncbi::genome_name", parents={group})
    inputs.AddValue("DH10b", "GCF_000019425.1", "ncbi::assembly_accession", parents={_DH10b_name})
    _K12_name = inputs.AddValue("K12.name", "K12", "ncbi::genome_name", parents={group})
    inputs.AddValue("K12", "GCF_000005845.2", "ncbi::assembly_accession", parents={_K12_name})
    _EPI300_name = inputs.AddValue("EPI300.name", "EPI300", "ncbi::genome_name", parents={group})
    inputs.AddValue("EPI300", "GCF_049667475.1", "ncbi::assembly_accession", parents={_EPI300_name})
    inputs.AddValue("fastani.oci", "docker://staphb/fastani:1.34", "ani::fastani.oci")
    inputs.Save()

resources = [
    DataInstanceLibrary.Load(MLIB/f"resources/{n}")
    for n in ["env", "lib"]
] + [
    view
    for view in inputs.AsSamples("ani::fastani.oci")
]

transforms = [
    TransformInstanceLibrary.Load(MLIB/f"transforms/{n}")
    for n in ["logistics", "pangenome"]
] + [
    ani_transforms
]

agent_home = Source.FromLocal(Path("/home/tony/workspace/tools/MetasmithLibraries/tests/cache/local_home"))
smith = Agent(
    home = agent_home,
    runtime=Runtime.DOCKER,
)


targets = TargetBuilder()
targets.Add("ani::table")


task = smith.GenerateWorkflow(
    samples=inputs.AsSamples("ncbi::assembly_accession"),
    resources=resources,
    transforms=transforms,
    targets=targets,
)

print(len(task.plan.steps))
task.plan.RenderDAG("./dag.svg")

smith.StageWorkflow(task, on_exist="update")

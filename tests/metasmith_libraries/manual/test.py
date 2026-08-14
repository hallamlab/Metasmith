from pathlib import Path
from metasmith.python_api import Agent, Runtime
from metasmith.python_api import DataTypeLibrary, DataInstanceLibrary, TransformInstanceLibrary
from metasmith.python_api import Source, Logistics
from metasmith.python_api import TargetBuilder, Resources, Size, Duration
from metasmith.python_api import ipynbButtonLink

# WORKSPACE = Path("../../").resolve() # back twice since we are in example_resources/tutorials
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
    # assert False
    inputs = DataInstanceLibrary.Load(inputs_path)
except:
    inputs = DataInstanceLibrary(inputs_path)
    # add data types
    inputs.AddTypeLibrary(MLIB/"data_types/pangenome.yml")
    inputs.AddTypeLibrary(MLIB/"data_types/ncbi.yml")
    inputs.AddTypeLibrary(ani_types, namespace="ani")

    # register inputs
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

# agent_home = Source.FromLocal(WORKSPACE/"msm_home")
agent_home = Source.FromLocal(Path("/home/tony/workspace/tools/MetasmithLibraries/tests/cache/local_home"))
smith = Agent(
    home = agent_home,
    runtime=Runtime.DOCKER,
)

# smith.Deploy(assertive=True)

targets = TargetBuilder()
# targets.Add("pangenome::heatmap")
targets.Add("ani::table")
# targets.Add("sequences::gbk")
# targets.Add("ncbi::accession")

# for x in inputs.AsSamples("ncbi::accession"):
# for x in resources:
#     print(x.manifest)

task = smith.GenerateWorkflow(
    samples=inputs.AsSamples("ncbi::assembly_accession"),
    resources=resources,
    transforms=transforms,
    targets=targets,
)

print(len(task.plan.steps))
task.plan.RenderDAG("./dag.svg")

smith.StageWorkflow(task, on_exist="update")

# smith.RunWorkflow(
#     task,
#     config_file=smith.GetNxfConfigPresets()["local"],
#     params= dict(
#         executor=dict(
#             cpus=15,
#             queueSize=3,
#         ),
#         process=dict(
#             tries=1,
#         ),
#     ),
#     resource_overrides={
#         "*": Resources(
#             memory=Size.GB(1),
#         ),
#         "fastani": Resources(
#             cpus=14,
#         )
#     },
#     stub_delay=
# )

# smith.GetResultSource(task)

# dag = task.plan.RenderDAG(WORKSPACE/"ani_dag.svg")
# ipynbButtonLink(dag)


# smith.Deploy()
# WORKSPACE

# sys.path = ["/home/tony/workspace/tools/MetasmithLibraries/tests/cache/local_home/runs/r1PAGuVP/_metasmith/task/transforms/bTPFEBhcO5JE/isolate"]+sys.path
# __import__("miniasm")


# hex = md5("/msm_home/runs/jNnaM8ZN/_metasmith/task/data/A7ZRjTx9d2kZ/SRR21655586.json".encode()).hexdigest()
# hex = hex[:15]
# print(hex)

# # 4. Two's complement wrap:
# # If the 63rd bit is set, it's negative in a signed 64-bit system
# val = int(hex, 16)
# # if val >= 0x8000000000000000:
# #     val -= 0x10000000000000000
# print(val)
# # print(2**31-1)

# print(bool("False"))
# print([1, 2][-3:])
# print(Path("asdf.x").suffix)
# print(int(float("2.7")))

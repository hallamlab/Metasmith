import os, sys
from pathlib import Path
from metasmith.python_api import Agent, Source, SshSource, TargetBuilder
from metasmith.python_api import DataInstanceLibrary, TransformInstanceLibrary, DataTypeLibrary
from metasmith.python_api import Resources, Size, Duration
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

input_raw = [
    # ("SRR5585544", "ncbi::sra_accession", dict(parity="single", length_class="short")),
    # ("SRR21655585", "ncbi::sra_accession", dict(parity="paired", length_class="short")), # 128 M
    # ("SRR3926590", "ncbi::sra_accession", dict(parity="paired", length_class="short")),
    # ("ERR391747", "ncbi::sra_accession", dict(parity="single", length_class="short")), # 92 M
    # ("SRR9430068", "ncbi::sra_accession", dict(parity="single", length_class="long")), # 126 M
    # ("ERR391746", "ncbi::sra_accession", dict(parity="single", length_class="long")), # 76 M
    # ("ERR6134066", "ncbi::sra_accession", dict(parity="single", length_class="long")), # 68 M
    # ("ERR6134064", "ncbi::sra_accession", dict(parity="paired", length_class="short")), # 32 M
    # ("SRR17798920", "ncbi::sra_accession", dict(parity="single", length_class="short")), # 73 M
    # ("SRR039686", "ncbi::sra_accession", dict(parity="single", length_class="long")), # 148 M

    ("SRR17798920", "ncbi::sra_accession", dict(parity="single", length_class="short")), # 73 M
    ("SRR039686", "ncbi::sra_accession", dict(parity="single", length_class="long")), # 148 M
    ("SRR21655586", "ncbi::sra_accession", dict(parity="paired", length_class="short")), # 135 M
]
_, _hash = KeyGenerator.FromStr("".join(str(p) for p, t, m in input_raw))
in_dir = base_dir/f"{notebook_name}/inputs.{_hash}.xgdb"
print(in_dir)
todo = {}
for p, t, m in input_raw:
    if isinstance(p, Path):
        meta = Path(f"{p.name}.json")
        reads = p
    else:
        k = p
        meta = Path(f"{p}.json")
        reads = Path(f"{p}.acc")
    todo[p] = {meta, reads}

if in_dir.exists():
    inputs = DataInstanceLibrary.Load(in_dir)
else:
    inputs = DataInstanceLibrary(in_dir)
    inputs.Purge()
    inputs.AddTypeLibrary("sequences", DataTypeLibrary.Load("../data_types/sequences.yml"))
    inputs.AddTypeLibrary("ncbi", DataTypeLibrary.Load("../data_types/ncbi.yml"))
    for p, t, m in input_raw:
        if isinstance(p, Path):
            m["acc"] = p.name.split(".")[0].split("_")[0]
            meta = inputs.AddValue(f"{p.name}.json", m, "sequences::read_metadata")
            reads = inputs.AddItem(p, t, parents={meta})
        else:
            k = p
            meta = inputs.AddValue(f"{p}.json", m, "sequences::read_metadata")
            reads = inputs.AddValue(f"{p}.acc", p, t, parents={meta})
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
        "assembly",
    ]
]

targets = TargetBuilder()
for n, p in [
        # "sequences::miniasm_gfa",
        ("sequences::reads",                        set()),
        # ("sequences::read_qc_stats",                {"sequences::reads"}),
        # ("sequences::discarded_reads",              set()),
        ("sequences::assembly",                     {"sequences::reads"}),
        ("sequences::assembly_stats",               {"sequences::assembly", "sequences::reads"}),
        ("sequences::assembly_per_bp_coverage",     {"sequences::assembly"}),
        ("sequences::assembly_per_contig_coverage", {"sequences::assembly"}),

        # ("sequences::flye_raw_assembly",                set()),
        # ("sequences::assembly_stats",               {"sequences::flye_raw_assembly"}),
        # ("sequences::assembly_per_bp_coverage",     {"sequences::flye_raw_assembly"}),
        # ("sequences::assembly_per_contig_coverage", {"sequences::flye_raw_assembly"}),
    ]:
    targets.Add(n, p)

task = smith.GenerateWorkflow(
    samples=[inputs.AsView(mask=v) for k, v in todo.items()],
    resources=resources,
    transforms=transforms,
    # targets=["sequences::read_qc_stats"],
    targets=targets,
)
# task.SaveAs(Source.FromLocal(Path("./cache/test.task").absolute()))
# p = task.plan._solver_result.RenderDAG(base_dir/f"{notebook_name}/dag_raw")
p = task.plan.RenderDAG(base_dir/f"{notebook_name}/dag")
print(task.ok, len(task.plan.steps))
print(p)
print(f"task: {task.GetKey()}, input {in_dir}")

# smith.StageWorkflow(task, on_exist="update_all", verify_external_paths=True)
# # smith.StageWorkflow(task, on_exist="clear", verify_external_paths=False)

# with open("../secrets/slurm_account_fir") as f:
# # with open("../secrets/slurm_account_sockeye") as f:
#     SLURM_ACCOUNT = f.readline()
# params = dict(
#     slurmAccount=SLURM_ACCOUNT,
#     executor=dict(
#         cpus=15,
#         memory='6 GB',
#         queueSize=10,
#     ),
# )
# smith.RunWorkflow(
#     task=task,
#     # stub_delay=15,
#     # config_file=smith.GetNxfConfigPresets()["slurm"],
#     config_file=smith.GetNxfConfigPresets()["local"],
#     params=params,
#     resource_overrides={
#         "all": Resources(
#             memory=Size.MB(100),
#             cpus=1,
#         ),
#     }
# )

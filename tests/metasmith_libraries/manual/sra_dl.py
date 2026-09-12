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
    runtime=Runtime.DOCKER,
)


notebook_name = Path(__file__).stem

input_raw = [

    ("SRR17798920", "ncbi::sra_accession", dict(parity="single", length_class="short")),
    ("SRR039686", "ncbi::sra_accession", dict(parity="single", length_class="long")),
    ("SRR21655586", "ncbi::sra_accession", dict(parity="paired", length_class="short")),
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
        "assembly",
    ]
]

targets = TargetBuilder()
for n, p in [
        ("sequences::reads",                        set()),
        ("sequences::assembly",                     {"sequences::reads"}),
        ("sequences::assembly_stats",               {"sequences::assembly", "sequences::reads"}),
        ("sequences::assembly_per_bp_coverage",     {"sequences::assembly"}),
        ("sequences::assembly_per_contig_coverage", {"sequences::assembly"}),

    ]:
    targets.Add(n, p)

task = smith.GenerateWorkflow(
    samples=[inputs.AsView(mask=v) for k, v in todo.items()],
    resources=resources,
    transforms=transforms,
    targets=targets,
)
p = task.plan.RenderDAG(base_dir/f"{notebook_name}/dag")
print(task.ok, len(task.plan.steps))
print(p)
print(f"task: {task.GetKey()}, input {in_dir}")

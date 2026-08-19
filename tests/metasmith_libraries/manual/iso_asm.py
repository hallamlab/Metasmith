import os, sys
from pathlib import Path
from metasmith.python_api import Agent, Source, SshSource, DataInstanceLibrary, TransformInstanceLibrary, DataTypeLibrary
from metasmith.python_api import Resources, Size, Duration
from metasmith.python_api import Runtime
from metasmith.python_api import TargetBuilder
from metasmith.hashing import KeyGenerator

base_dir = Path("./cache")


agent_home = SshSource(host="sockeye", path=Path("/scratch/st-shallam-1/pwy_group/metasmith")).AsSource()
smith = Agent(
    home = agent_home,
    runtime=Runtime.APPTAINER,
    setup_commands=[
        'module load gcc/9.4.0',
        'module load apptainer/1.3.1',
    ]
)
smith.Deploy(assertive=True)

notebook_name = Path(__file__).stem

input_raw = [


    (Path(f"/arc/project/st-shallam-1/pwy_group/data/model_strains/Ana_PS.fastq.gz"), "sequences::long_reads", dict(parity="single", length_class="long")),
    (Path(f"/arc/project/st-shallam-1/pwy_group/data/model_strains/Nos_PS.fastq.gz"), "sequences::long_reads", dict(parity="single", length_class="long")),
    (Path(f"/arc/project/st-shallam-1/pwy_group/data/model_strains/SynC_PS.fastq.gz"), "sequences::long_reads", dict(parity="single", length_class="long")),
    (Path(f"/arc/project/st-shallam-1/pwy_group/data/model_strains/SynT_PS.fastq.gz"), "sequences::long_reads", dict(parity="single", length_class="long")),

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
        ("sequences::100x_long_reads",              set()),
        ("sequences::read_qc_stats",                set()),
        ("sequences::isolate_assembly",             set()),
        ("sequences::assembly_stats",               {"sequences::isolate_assembly"}),
        ("sequences::assembly_per_bp_coverage",     {"sequences::isolate_assembly"}),
        ("sequences::assembly_per_contig_coverage", {"sequences::isolate_assembly"}),
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

smith.StageWorkflow(task, on_exist="clear", verify_external_paths=False)

with open("../secrets/slurm_account_sockeye") as f:
    SLURM_ACCOUNT = f.readline()
params = dict(
    slurmAccount=SLURM_ACCOUNT,
    executor=dict(
        queueSize=500,
    ),
    process=dict(
        array=2,
        tries=2,
    )
)
smith.RunWorkflow(
    task=task,
    config_file=smith.GetNxfConfigPresets()["slurm"],
    params=params,
    resource_overrides={
        "all": Resources(
            memory=Size.MB(100),
            cpus=1,
        ),
    }
)

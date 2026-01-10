# from pathlib import Path
# import os, sys
# from tqdm import tqdm
# # _src = "/home/tony/workspace/tools/Metasmith/src"
# # if _src not in sys.path: sys.path = [_src]+list(sys.path)
# from pathlib import Path
# from metasmith.python_api import Agent, Source, SshSource, DataInstanceLibrary, TransformInstanceLibrary, DataTypeLibrary
# from metasmith.python_api import Resources, Size, Duration
# from metasmith.python_api import ContainerRuntime

# remote_reads = Path("./cache/sample_reads").absolute()
# agent_home = Source.FromLocal(Path("./cache/local_home").absolute())
# smith = Agent(
#     home = agent_home,
#     setup_commands=[
#     ],
#     runtime=ContainerRuntime.APPTAINER,
# )
# # smith.Deploy(assertive=True)

# reads = {}
# with open("/home/tony/workspace/projects/Cyanoverse/data/generated/tree") as f:
#     for l in f:
#         if not l.startswith("./reads/"): continue
#         l = l[:-1]
#         toks = l.split("/")
#         if len(toks)<4: continue
#         k = toks[2]
#         r = toks[3]
#         reads[k] = reads.get(k, [])+[r]

# pe_reads = {k:v for k, v in reads.items() if len(v)>1}
# for k, v in pe_reads.items():
#     assert len(v)==2, v

# dtypelibs = Path("/home/tony/workspace/tools/MetasmithLibraries/data_types")
# def _get_type(f):
#     if "_1.fastq" in f:
#         return "sequences::zipped_forward_short_reads"
#     if "_2.fastq" in f:
#         return "sequences::zipped_reverse_short_reads"
#     assert False, f
# inputs = DataInstanceLibrary("./cache/to_interleave.xgdb")
# inputs.Purge()
# inputs.AddTypeLibrary("sequences", DataTypeLibrary.Load(dtypelibs/"sequences.yml"))
# todo = {}
# # _lst = list(pe_reads.items())[:256]
# # _lst = list(pe_reads.items())[:128]
# # _lst = list(pe_reads.items())[:10]
# _lst = list(pe_reads.items())[:3]
# # _lst = list(pe_reads.items())
# for k, pair in _lst:
#     acc = inputs.AddValue(f"{k}.acc", k, "sequences::read_pair")
#     todo[k] = [acc]
#     for f in pair:
#         p = remote_reads/f"{k}/{f}"
#         todo[k] = todo.get(k, [])+[p]
#         inputs.AddItem(p, _get_type(f), parents={acc})
# inputs.Save()

# resources = [
#     DataInstanceLibrary.Load(dtypelibs/f"../resources/{n}")
#     for n in [
#         "containers",
#         # "lib",
#     ]
# ]

# transforms = [
#     TransformInstanceLibrary.Load(dtypelibs/f"../transforms/{n}")
#     for n in [
#         "logistics",
#         # "assembly",
#     ]
# ]

# dtypes = DataTypeLibrary.Load(dtypelibs/f"sequences.yml")
# task = smith.GenerateWorkflow(
#     samples=[inputs.AsView(set(pair)) for i, pair in enumerate(todo.values())],
#     resources=resources,
#     transforms=transforms,
#     targets=[dtypes["short_reads"]]
# )
# p = task.plan.RenderDAG(f"cache/try2/dag")
# print(task.ok, len(task.plan.steps))
# print(task.GetKey())

# smith.StageWorkflow(task, on_exist="update_workflow",  verify_external_paths=False)

# params = dict(
#     process = dict(
#         tries=2,
#     ),
# )
# smith.RunWorkflow(
#     task=task,
#     config_file=smith.GetNxfConfigPresets()["local"],
#     params=params,
#     resource_overrides={
#         transforms[0]["interleave_zipped_short_reads"]: Resources(
#             memory=Size.GB(4),
#             cpus=2,
#             duration=Duration(hours=4),
#         )
#     }
# )

from pathlib import Path
import os
from metasmith.python_api import DataInstanceLibrary

res_path= Path("./cache/local_home/runs/nlq8TEBZ/results")
reslib = DataInstanceLibrary.Load(res_path)
to_rename = {}
for path, name, type in reslib.Iterate():
    if path.is_absolute(): continue
    print(name, path)
    for parent in reslib.parents[path]:
        if parent.name != "sequences::read_pair": continue
        print(" ", parent.name)
        print(" ", parent.path)
        print()
        to_rename[path] = path.parent/f"{parent.path.stem}.fq.gz"
        break

for a, b in to_rename.items():
    if b in reslib: continue
    print(f"{a} -> {b}")
    reslib.Rename(a, b, _save=False)
reslib.Save() # important!
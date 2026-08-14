#!/usr/bin/env python3
"""Resume the cyano copper panel into a DIAMOND all-vs-all BSR-distance histogram.

Reuses the six proteomes already fetched by the prior PPanGGOLiN run (run key
uOJnvxQz) -- no NCBI refetch. Each .faa is copied to `<genus-species-PCC>.faa`
(name parsed from the matching .gbk ORGANISM line), so the planner skips
getNcbiAssembly and goes straight to diamond_all_vs_all -> bsr_histogram.

Run with the `msm` env python:
    PY=/home/tony/lib/miniforge3/envs/msm/bin/python
    $PY main/pangenome_cyano_copper_bsr.py        # plan-only: render DAG
    $PY main/pangenome_cyano_copper_bsr.py run     # stage + run locally (Docker)
"""
import sys
import time
import shutil
from pathlib import Path

from metasmith.python_api import (
    Agent, Source, Runtime,
    DataInstanceLibrary, TransformInstanceLibrary,
    TargetBuilder, Resources, Size,
)

MLIB = Path(__file__).resolve().parent.parent
BASE = MLIB / "main" / "cache" / "pangenome_cyano_copper_bsr"
PRIOR = MLIB / "main" / "cache" / "pangenome_cyano_copper" / "msm_home" / "runs" / "uOJnvxQz" / "results"
ORFS_DIR = PRIOR / "1_sequences-orfs"
GBK_DIR = PRIOR / "1_sequences-gbk"
RUN = len(sys.argv) > 1 and sys.argv[1] == "run"
TIMEOUT = 1800


def lineage_token(p: Path) -> str:
    # filenames look like 1-1-1.<HASH>-<endpoint>.<ext>; <HASH> is shared between
    # a genome's orfs and gbk products.
    parts = p.name.split(".")
    if len(parts) >= 2 and "-" in parts[1]:
        return parts[1].split("-")[0]
    return p.stem


def organism_name(gbk_path: Path) -> str:
    name = None
    with open(gbk_path) as g:
        for i, l in enumerate(g):
            if i > 15:
                break
            if "ORGANISM" not in l:
                continue
            name = l.replace("ORGANISM", "").strip()
            name = name.replace("[", "").replace("]", "")
            for x in ",.'\"":
                name = name.replace(x, "")
            name = "-".join(name.split())
            break
    return name or gbk_path.stem


agent_home = Source.FromLocal((BASE / "msm_home").absolute())
smith = Agent(home=agent_home, runtime=Runtime.DOCKER)
smith.Deploy()

# genome name per lineage token, from the gbk ORGANISM lines
name_by_token = {lineage_token(g): organism_name(g) for g in GBK_DIR.glob("*.gbk")}

in_dir = BASE / "resume_inputs.xgdb"
try:
    inputs = DataInstanceLibrary.Load(in_dir)
except Exception:
    inputs = DataInstanceLibrary(in_dir)
    inputs.Purge()
    inputs.AddTypeLibrary(MLIB / "data_types/sequences.yml")
    inputs.AddTypeLibrary(MLIB / "data_types/pangenome.yml")

    group = inputs.AddValue("pangenome", "cyano_copper_panel", "pangenome::pangenome")

    named_dir = BASE / "orfs_named"
    named_dir.mkdir(parents=True, exist_ok=True)
    for faa in sorted(ORFS_DIR.glob("*.faa")):
        genome = name_by_token.get(lineage_token(faa), faa.stem)
        dest = named_dir / f"{genome}.faa"
        shutil.copy(faa.resolve(), dest)
        inputs.AddItem(dest.resolve(), "sequences::orfs", parents={group})
        print(f"  staged {genome}.faa")
    inputs.Save()

resources = [
    DataInstanceLibrary.Load(MLIB / f"resources/{n}")
    for n in ["env", "lib"]
]
transforms = [
    TransformInstanceLibrary.Load(MLIB / f"transforms/{n}")
    for n in ["logistics", "pangenome"]
]

targets = TargetBuilder()
targets.Add("pangenome::bsr_histogram")

task = smith.GenerateWorkflow(
    samples=inputs.AsSamples("pangenome::pangenome"),
    resources=resources,
    transforms=transforms,
    targets=targets,
)

try:
    task.plan.RenderDAG(str(BASE / "dag"))
except Exception as e:
    print(f"[warn] RenderDAG skipped: {e}")

print(f"task.ok = {task.ok}")
print(f"steps   = {len(task.plan.steps)}")
for i, step in enumerate(task.plan.steps):
    label = None
    for attr in ("transform", "model"):
        obj = getattr(step, attr, None)
        if obj is None:
            continue
        label = getattr(getattr(obj, "model", obj), "name", None) or getattr(obj, "name", None)
        if label:
            break
    print(f"  [{i}] {label or repr(step)}")

if not RUN:
    print(f"\nplan-only; DAG -> {BASE / 'dag'} | pass 'run' to execute")
    sys.exit(0 if task.ok else 1)

assert task.ok, "workflow planning failed"
smith.StageWorkflow(task, on_exist="clear")
smith.RunWorkflow(
    task=task,
    config_file=smith.GetNxfConfigPresets()["local"],
    params=dict(executor=dict(cpus=8, queueSize=3), process=dict(tries=1)),
    resource_overrides={"*": Resources(memory=Size.GB(8), cpus=8)},
)

results_path = smith.GetResultSource(task).GetPath()
start = time.time()
while not (results_path / "_metadata").exists():
    if time.time() - start > TIMEOUT:
        raise TimeoutError(f"workflow did not complete within {TIMEOUT}s")
    time.sleep(10)

smith.CheckWorkflow(task)
results = DataInstanceLibrary.Load(results_path)
print("\n=== outputs ===")
for path, type_name, endpoint in results.Iterate():
    if path.is_absolute():
        continue
    print(f"{type_name}: {results_path / path}")

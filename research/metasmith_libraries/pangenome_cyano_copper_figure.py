#!/usr/bin/env python3
"""Regenerate the cyano copper-panel PPanGGOLiN heatmap from existing .gbk.

Resumes from the six RefSeq .gbk already fetched by the original run (run key
uOJnvxQz) -- no NCBI refetch -- and re-clusters with data-driven thresholds
(--identity 0.3 --coverage 0.8, read off the BSR + identity/coverage
histograms) plus the ORGANISM-line genome naming fix. Targets the heatmap.

Run with the `msm` env python:
    PY=/home/tony/lib/miniforge3/envs/msm/bin/python
    $PY main/pangenome_cyano_copper_figure.py        # plan-only: render DAG
    $PY main/pangenome_cyano_copper_figure.py run     # stage + run locally (Docker)
"""
import sys
import time
from pathlib import Path

from metasmith.python_api import (
    Agent, Source, Runtime,
    DataInstanceLibrary, TransformInstanceLibrary,
    TargetBuilder, Resources, Size,
)

MLIB = Path(__file__).resolve().parent.parent
BASE = MLIB / "main" / "cache" / "pangenome_cyano_copper_figure"
PRIOR = MLIB / "main" / "cache" / "pangenome_cyano_copper" / "msm_home" / "runs" / "uOJnvxQz" / "results"
GBK_DIR = PRIOR / "1_sequences-gbk"
RUN = len(sys.argv) > 1 and sys.argv[1] == "run"
TIMEOUT = 2400

agent_home = Source.FromLocal((BASE / "msm_home").absolute())
smith = Agent(home=agent_home, runtime=Runtime.DOCKER)
if RUN:
    smith.Deploy()  # deploys the relay binary via a container; needs Docker
else:
    print("[plan-only] skipping Deploy() (no container needed to plan + render DAG)")

in_dir = BASE / "resume_inputs.xgdb"
try:
    inputs = DataInstanceLibrary.Load(in_dir)
except Exception:
    inputs = DataInstanceLibrary(in_dir)
    inputs.Purge()
    inputs.AddTypeLibrary(MLIB / "data_types/ncbi.yml")
    inputs.AddTypeLibrary(MLIB / "data_types/sequences.yml")
    inputs.AddTypeLibrary(MLIB / "data_types/pangenome.yml")

    group = inputs.AddValue("pangenome", "cyano_copper_panel", "pangenome::pangenome")
    for gbk in sorted(GBK_DIR.glob("*.gbk")):
        # Resuming from files on disk rather than from accessions, so the name
        # comes from the filename -- but it is still declared, because that is
        # what ppanggolin reads. Its stem is the strain here (PCC_7002.gbk).
        label = gbk.stem
        nm = inputs.AddValue(f"{label}.name", label, "ncbi::genome_name", parents={group})
        inputs.AddItem(gbk.resolve(), "sequences::gbk", parents={nm})
        print(f"  staged {gbk.name} as [{label}]")
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
targets.Add("pangenome::heatmap")
targets.Add("pangenome::ppanggolin_matrix")

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

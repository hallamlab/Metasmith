#!/usr/bin/env python3
"""Plan + run a PPanGGOLiN cyanobacterial pangenome heatmap via Metasmith.

Six-strain copper-study cyano panel (five PCC strains + AB48), all fetched from
NCBI as assembly accessions. getNcbiAssembly pulls each RefSeq .gbff (NCBI-PGAP
annotated), ppanggolin builds the pangenome, heatmap renders it.

AB48 = Phormidium/Sodalinema yuhuli strain AB48 = GCF_023983615.1 (complete
genome, UBC deposit, NCBI-PGAP v6.10) -- same fetch path and annotation
provenance as the five PCC strains, so all six are apples-to-apples.

Run with the `msm` env python:
    PY=/home/tony/lib/miniforge3/envs/msm/bin/python
    $PY main/pangenome_cyano_copper.py        # plan-only: generate + render DAG
    $PY main/pangenome_cyano_copper.py run     # also stage + run locally (Docker)
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
BASE = MLIB / "main" / "cache" / "pangenome_cyano_copper"
RUN = len(sys.argv) > 1 and sys.argv[1] == "run"
TIMEOUT = 2400  # seconds to wait for the async run (6 NCBI fetches + ppanggolin)

# Six-strain cyano copper panel: name -> NCBI assembly accession
PANEL = {
    "PCC_7002": "GCF_000019485.1",  # Picosynechococcus sp. PCC 7002
    "PCC_7418": "GCF_000317635.1",  # Halothece sp. PCC 7418
    "PCC_7376": "GCF_000316605.1",  # [Leptolyngbya] sp. PCC 7376
    "PCC_7116": "GCF_000316665.1",  # Rivularia sp. PCC 7116
    "PCC_7420": "GCF_000155555.1",  # Coleofasciculus chthonoplastes PCC 7420 (draft scaffold)
    "AB48":     "GCF_023983615.1",  # Phormidium/Sodalinema yuhuli AB48 (complete, NCBI-PGAP)
}

agent_home = Source.FromLocal((BASE / "msm_home").absolute())
smith = Agent(home=agent_home, runtime=Runtime.DOCKER)
smith.Deploy()

in_dir = BASE / "inputs.xgdb"
try:
    inputs = DataInstanceLibrary.Load(in_dir)
except Exception:
    inputs = DataInstanceLibrary(in_dir)
    inputs.Purge()
    inputs.AddTypeLibrary(MLIB / "data_types/ncbi.yml")
    inputs.AddTypeLibrary(MLIB / "data_types/sequences.yml")
    inputs.AddTypeLibrary(MLIB / "data_types/pangenome.yml")

    group = inputs.AddValue("pangenome", "cyano_copper_panel", "pangenome::pangenome")
    for name, acc in PANEL.items():
        # The name is a declared input now, not just the library path it used to
        # be thrown away into. It sits between the pangenome and the accession,
        # so everything downloaded inherits it and ppanggolin labels each genome
        # with it rather than guessing from the GenBank header.
        nm = inputs.AddValue(f"{name}.name", name, "ncbi::genome_name", parents={group})
        inputs.AddValue(name, acc, "ncbi::assembly_accession", parents={nm})
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
    samples=inputs.AsSamples("ncbi::assembly_accession"),
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
    resource_overrides={"*": Resources(memory=Size.GB(4), cpus=8)},
)

# RunWorkflow is fire-and-forget; poll for the results _metadata sentinel.
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

#!/usr/bin/env python
import sys
import tempfile
from pathlib import Path

MLIB = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, "/home/tony/workspace/msm/Metasmith/src")

from metasmith.python_api import (
    Agent, Runtime, Source,
    DataInstanceLibrary, TransformInstanceLibrary,
    TargetBuilder,
)

tmp = Path(tempfile.mkdtemp())
agent_home = Source.FromLocal(tmp)
smith = Agent(home=agent_home, runtime=Runtime.APPTAINER)

transforms = [
    TransformInstanceLibrary.Load(MLIB / "transforms/functionalAnnotation"),
    TransformInstanceLibrary.Load(MLIB / "transforms/logistics"),
]

base_res = [DataInstanceLibrary.Load(MLIB / "resources/env")]

inputs_dir = tmp / "inputs.xgdb"
inputs = DataInstanceLibrary(inputs_dir)
for tl in ["sequences.yml", "transcriptomics.yml", "annotation.yml"]:
    inputs.AddTypeLibrary(MLIB / "data_types" / tl)

def mock(name, is_dir=False):
    p = tmp / name
    if is_dir:
        p.mkdir(exist_ok=True)
    else:
        p.touch()
    return p

experiment = inputs.AddValue(
    "porphyridium_experiment.txt",
    "porphyridium_transcriptomics",
    "transcriptomics::experiment",
)

inputs.AddItem(mock("braker3.faa"), "sequences::orfs", parents={experiment})

inputs.AddItem(mock("eggnog", is_dir=True), "annotation::eggnog_data")

inputs.AddValue("busco_source.txt", "eukaryota_odb10", "annotation::busco_source")

inputs.Save()

targets = TargetBuilder()
targets.Add("annotation::busco_results")
targets.Add("annotation::eggnog_results")

print("Generating annotation workflow plan...", flush=True)
task = smith.GenerateWorkflow(
    samples=list(inputs.AsSamples("transcriptomics::experiment")),
    resources=base_res + [inputs],
    transforms=transforms,
    targets=targets,
)

if not task.ok:
    print(f"FAILED: {task.plan}")
    sys.exit(1)

steps = task.plan.steps
print(f"\nPlan OK — {len(steps)} steps\n")
for step in steps:
    name = Path(step.transform._path).stem
    prods = [i.dtype_name for g in step.produces for i in g]
    print(f"  Step {step.order}: {name} -> {prods}")

import os
_env_bin = Path(sys.executable).parent
os.environ["PATH"] = f"{_env_bin}:{os.environ.get('PATH', '')}"

out_png = MLIB / "results/annotation_dag.png"
task.plan.RenderDAG(out_png, blacklist_namespaces={"lib", "env"})
print(f"\nDAG written to: {out_png}")

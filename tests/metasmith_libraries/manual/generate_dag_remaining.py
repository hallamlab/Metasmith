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
    TransformInstanceLibrary.Load(MLIB / "transforms/transcriptomics"),
]

containers = DataInstanceLibrary(tmp / "containers.xgdb")
containers.AddTypeLibrary(MLIB / "data_types" / "env.yml")
for name in ["stringtie.env", "pydeseq2.env", "python_for_data_science.env"]:
    containers.AddItem(MLIB / "resources/env" / name, f"env::{name}")
containers.Save()

inputs_dir = tmp / "inputs.xgdb"
inputs = DataInstanceLibrary(inputs_dir)
for tl in ["sequences.yml", "transcriptomics.yml"]:
    inputs.AddTypeLibrary(MLIB / "data_types" / tl)

def mock(name):
    p = tmp / name
    p.touch()
    return p

experiment = inputs.AddValue("experiment.txt", "porphyridium", "transcriptomics::experiment")

inputs.AddItem(mock("braker3.gff3"), "transcriptomics::braker3_gff", parents={experiment})

for i in range(9):
    inputs.AddItem(mock(f"stringtie_{i}.gtf"), "transcriptomics::stringtie_gtf", parents={experiment})

for i in range(9):
    inputs.AddItem(mock(f"star_{i}.bam"), "transcriptomics::star_bam", parents={experiment})

inputs.Save()

targets = TargetBuilder()
targets.Add("transcriptomics::merged_gtf")
targets.Add("transcriptomics::stringtie_quant_gtf")
targets.Add("transcriptomics::gene_count_table")
targets.Add("transcriptomics::diff_count_table")

print("Generating workflow plan...", flush=True)
task = smith.GenerateWorkflow(
    samples=list(inputs.AsSamples("transcriptomics::experiment")),
    resources=[containers, inputs],
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

out_png = MLIB / "results/remaining_dag.png"
task.plan.RenderDAG(out_png, blacklist_namespaces={"lib", "env"})
print(f"\nDAG written to: {out_png}")

#!/usr/bin/env python
"""Generate DAG without BUSCO (BUSCO failed in this run).

Renders SVG and PNG to results/report_package01/.
"""
import sys
import os
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
    TransformInstanceLibrary.Load(MLIB / "transforms/functionalAnnotation"),
    TransformInstanceLibrary.Load(MLIB / "transforms/logistics"),
]

# Eukaryotic containers — BUSCO excluded
EUKARYOTIC_CONTAINERS = [
    "star.env",
    "samtools.env",
    "stringtie.env",
    "gffread.env",
    "python_for_data_science.env",
    "eggnog-mapper.env",
    "braker3.env",
    "pydeseq2.env",
]
euk_containers = DataInstanceLibrary(tmp / "containers_euk.xgdb")
euk_containers.AddTypeLibrary(MLIB / "data_types" / "env.yml")
for name in EUKARYOTIC_CONTAINERS:
    euk_containers.AddItem(MLIB / "resources/env" / name, f"env::{name}")
euk_containers.Save()

inputs_dir = tmp / "inputs.xgdb"
inputs = DataInstanceLibrary(inputs_dir)
for tl in ["sequences.yml", "transcriptomics.yml", "annotation.yml", "env.yml"]:
    inputs.AddTypeLibrary(MLIB / "data_types" / tl)

def mock(name, is_dir=False):
    p = tmp / name
    if is_dir:
        p.mkdir(exist_ok=True)
    else:
        p.touch()
    return p

experiment = inputs.AddValue("experiment.txt", "porphyridium", "transcriptomics::experiment")
inputs.AddItem(mock("genome.fna"), "sequences::assembly", parents={experiment})

for i in range(3):
    pair = inputs.AddValue(f"pair_{i}.txt", f"pair_{i}", "sequences::read_pair")
    inputs.AddItem(mock(f"r1_{i}.fastq.gz"), "sequences::zipped_forward_short_reads", parents={pair, experiment})
    inputs.AddItem(mock(f"r2_{i}.fastq.gz"), "sequences::zipped_reverse_short_reads", parents={pair, experiment})

inputs.AddValue("eggnog_source.txt", "eggnog", "annotation::eggnog_source")
inputs.Save()

# Targets — no busco_results
targets = TargetBuilder()
targets.Add("transcriptomics::gene_count_table")
targets.Add("transcriptomics::diff_count_table")
targets.Add("annotation::eggnog_results")
targets.Add("transcriptomics::braker3_gff")
targets.Add("sequences::orfs")

print("Generating workflow plan (no BUSCO)...", flush=True)
task = smith.GenerateWorkflow(
    samples=list(inputs.AsSamples("transcriptomics::experiment")),
    resources=[euk_containers, inputs],
    transforms=transforms,
    targets=targets,
)

if not task.ok:
    print(f"FAILED to generate plan: {task.plan}")
    sys.exit(1)

steps = task.plan.steps
print(f"\nPlan OK — {len(steps)} steps\n")
for step in steps:
    name = Path(step.transform._path).stem
    prods = [i.dtype_name for g in step.produces for i in g]
    print(f"  Step {step.order}: {name} -> {prods}")

# Render
_env_bin = Path(sys.executable).parent
os.environ["PATH"] = f"{_env_bin}:{os.environ.get('PATH', '')}"

out_dir = MLIB / "results/report_package01"
for ext in ["svg", "png"]:
    out = out_dir / f"workflow_dag.{ext}"
    task.plan.RenderDAG(out, blacklist_namespaces={"lib", "env"})
    print(f"DAG written to: {out}")

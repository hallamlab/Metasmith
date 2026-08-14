#!/usr/bin/env python
"""Generate and visualize the Metasmith eukaryotic workflow DAG locally.

Starting inputs: genome assembly + paired RNA-seq reads
Targets: gene_count_table, diff_count_table, eggnog_results, busco_results, braker3_gff

Run without staging or executing — planning only.
"""
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

# ── Local agent (GenerateWorkflow does not require SSH) ───────────────────────
tmp = Path(tempfile.mkdtemp())
agent_home = Source.FromLocal(tmp)
smith = Agent(home=agent_home, runtime=Runtime.APPTAINER)

# ── Transform libraries ───────────────────────────────────────────────────────
transforms = [
    TransformInstanceLibrary.Load(MLIB / "transforms/transcriptomics"),
    TransformInstanceLibrary.Load(MLIB / "transforms/functionalAnnotation"),
    TransformInstanceLibrary.Load(MLIB / "transforms/logistics"),
]

# ── Eukaryotic container library (excludes prokaryotic tools) ────────────────
# pprodigal is excluded: it provides a shortcut to sequences::orfs that
# conflicts with the correct eukaryotic path via gffread_proteins, and it
# provides sequences::gff which we instead supply as a reference annotation.
EUKARYOTIC_CONTAINERS = [
    "star.env",
    "samtools.env",
    "stringtie.env",
    "gffread.env",
    "python_for_data_science.env",
    "eggnog-mapper.env",
    "braker3.env",
    "busco.env",
    "pydeseq2.env",
]
euk_containers = DataInstanceLibrary(tmp / "containers_euk.xgdb")
euk_containers.AddTypeLibrary(MLIB / "data_types" / "env.yml")
for name in EUKARYOTIC_CONTAINERS:
    euk_containers.AddItem(MLIB / "resources/env" / name, f"env::{name}")
euk_containers.Save()

# ── Input library ─────────────────────────────────────────────────────────────
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

# Genome assembly (starting input)
inputs.AddItem(mock("genome.fna"), "sequences::assembly", parents={experiment})

# Paired RNA-seq reads (3 samples sufficient for planning)
for i in range(3):
    pair = inputs.AddValue(f"pair_{i}.txt", f"pair_{i}", "sequences::read_pair")
    inputs.AddItem(mock(f"r1_{i}.fastq.gz"), "sequences::zipped_forward_short_reads", parents={pair, experiment})
    inputs.AddItem(mock(f"r2_{i}.fastq.gz"), "sequences::zipped_reverse_short_reads", parents={pair, experiment})

# Download triggers — let planner derive eggnog_data and busco_lineage
inputs.AddValue("eggnog_source.txt", "eggnog", "annotation::eggnog_source")
inputs.AddValue("busco_source.txt", "eukaryota_odb10", "annotation::busco_source")

inputs.Save()

# ── Generate workflow plan ────────────────────────────────────────────────────
targets = TargetBuilder()
targets.Add("transcriptomics::gene_count_table")
targets.Add("transcriptomics::diff_count_table")
targets.Add("annotation::eggnog_results")
targets.Add("annotation::busco_results")
targets.Add("transcriptomics::braker3_gff")

print("Generating workflow plan...", flush=True)
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

# ── Render via Metasmith's built-in DAG renderer ──────────────────────────────
import os
# Ensure the conda env bin (graphviz dot) is on PATH for the renderer
_env_bin = Path(sys.executable).parent
os.environ["PATH"] = f"{_env_bin}:{os.environ.get('PATH', '')}"

out = MLIB / "results/workflow_dag.svg"
task.plan.RenderDAG(out, blacklist_namespaces={"lib", "env"})
print(f"\nDAG written to: {out}")

out_png = MLIB / "results/workflow_dag.png"
task.plan.RenderDAG(out_png, blacklist_namespaces={"lib", "env"})
print(f"DAG written to: {out_png}")

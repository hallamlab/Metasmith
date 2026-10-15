#!/usr/bin/env python3
import os
import sys
import tempfile
from pathlib import Path

from metasmith.python_api import (
    Agent, Runtime, Source,
    DataInstanceLibrary, TransformInstanceLibrary,
    TargetBuilder,
)
from metasmith.python_api import record_library

MLIB = Path(__file__).resolve().parent.parent
OUT_DIR = MLIB / "results"
OUT_DIR.mkdir(parents=True, exist_ok=True)

tmp = Path(tempfile.mkdtemp(prefix="resistome_dag_"))
smith = Agent(home=Source.FromLocal(tmp), runtime=Runtime.APPTAINER)

containers = DataInstanceLibrary.Load(MLIB / "resources/env")
transforms = [
    TransformInstanceLibrary.Load(MLIB / "transforms/functionalAnnotation"),
    TransformInstanceLibrary.Load(MLIB / "transforms/metagenomics"),
    TransformInstanceLibrary.Load(MLIB / "transforms/logistics"),
]

TYPE_LIBS = ["sequences.yml", "alignment.yml", "binning.yml", "taxonomy.yml", "annotation.yml", "ref.yml"]


def new_inputs(name: str) -> DataInstanceLibrary:
    lib = DataInstanceLibrary(tmp / f"{name}.xgdb")
    for tl in TYPE_LIBS:
        lib.AddTypeLibrary(MLIB / "data_types" / tl)
    return lib


def mock(name: str) -> Path:
    p = tmp / name
    p.touch()
    return p


def plan_with_sweep(label, inputs, sample_type, target_list, configs, seeds):
    tb = TargetBuilder()
    for t in target_list:
        tb.Add(t)
    samples = list(inputs.AsSamples(sample_type))
    for max_iter, max_refine in configs:
        for seed in seeds:
            print(f"[{label}] plan: mi={max_iter} mr={max_refine} seed={seed} ...", flush=True)
            t = smith.GenerateWorkflow(
                samples=samples, resources=[containers, inputs],
                transforms=transforms, targets=tb,
                max_iter=max_iter, max_refine=max_refine, seed=seed,
            )
            dropped = list(t.plan.dropped_targets)
            print(f"  -> ok={t.ok} steps={len(t.plan.steps)} dropped={len(dropped)} {dropped}", flush=True)
            if t.ok:
                return t
    print(f"[{label}] NO COMPLETE PLAN found in sweep.")
    hints = getattr(t.plan, "hints", None)
    if hints:
        print(f"[{label}] hints: {hints}")
    return None


def render(task, stem):
    steps = task.plan.steps
    print(f"\nPlan OK — {len(steps)} steps:")
    for step in steps:
        name = Path(step.transform._path).stem
        prods = [i.dtype_name for g in step.produces for i in g]
        print(f"  Step {step.order}: {name} -> {prods}")
    for ext in ("svg", "png"):
        out = OUT_DIR / f"{stem}.{ext}"
        task.plan.RenderDAG(out, blacklist_namespaces={"lib", "env"})
        print(f"DAG written to: {out.resolve()}")


os.environ["PATH"] = f"{Path(sys.executable).parent}:{os.environ.get('PATH', '')}"

main = new_inputs("main")
asm = main.AddItem(mock("sample.fna"), "sequences::assembly")
main.AddItem(mock("sample.bam"), "alignment::bam", parents={asm})
main.AddItem(mock("sample.fq.gz"), "sequences::short_reads", parents={asm})
main.AddItem(mock("sample.orfs.faa"), "sequences::orfs", parents={asm})
main.AddItem(mock("bin.1.fa"), "sequences::comebin_bin_fasta", parents={asm})
main.AddItem(mock("contig_to_bin.tsv"), "binning::comebin_contig_to_bin_table", parents={asm})
main.AddItem(mock("metaphlan_profile.tsv"), "taxonomy::metaphlan_profile", parents={asm})
main.AddItem(mock("sample.clean.fq.gz"), "sequences::clean_short_reads", parents={asm})
main.AddItem(mock("mag_ref"), "binning::derep_mag_ref")
# Synthetic placeholders, authored here and used nowhere else, so
# writing them down is the whole of their record.
main = record_library(main)

MAIN_TARGETS = [
    "annotation::rgi_results",
    "annotation::amrfinderplus_results",
    "annotation::megares_diamond_results",
    "annotation::bacmet_diamond_results",
    "annotation::vfdb_diamond_results",
    "annotation::pathofact_amr",
    "annotation::integronfinder_summary",
    "annotation::mobileelementfinder_results",
    "annotation::virsorter2_viral_sequences",
    "taxonomy::genomad_virus_summary",
    "annotation::feast_proportions",
    "annotation::instrain_profile",
]
main_task = plan_with_sweep(
    "main", main, "sequences::assembly", MAIN_TARGETS,
    configs=[(1024, 256), (2048, 512), (4096, 1024)],
    seeds=[1, 7, 13, 42, 99],
)
if main_task is None:
    sys.exit(1)
render(main_task, "resistome_dag")

up = new_inputs("upstream")
asm2 = up.AddItem(mock("u_sample.fna"), "sequences::assembly")
up.AddItem(mock("u_sample.bam"), "alignment::bam", parents={asm2})
up.AddItem(mock("u_sample.fq.gz"), "sequences::short_reads", parents={asm2})
# Synthetic placeholders, authored here and used nowhere else, so
# writing them down is the whole of their record.
up = record_library(up)

UPSTREAM_TARGETS = [
    "sequences::orfs",
    "sequences::comebin_bin_fasta",
    "binning::comebin_contig_to_bin_table",
    "taxonomy::metaphlan_profile",
]
up_task = plan_with_sweep(
    "upstream", up, "sequences::assembly", UPSTREAM_TARGETS,
    configs=[(1024, 256), (2048, 512)],
    seeds=[1, 7, 13, 42, 99],
)
if up_task is None:
    print("WARNING: upstream DAG did not converge; main DAG still written.")
    sys.exit(0)
render(up_task, "resistome_upstream_dag")

print("\nBoth DAGs rendered under results/.")

#!/usr/bin/env python3
"""Render the resistome-pipeline DAG locally (planning only).

A single joint MCTS solve over all ~13 terminal resistome branches at full chain
length is intractable (the search does not converge within any practical budget
— diverse branches from assembly / bins / reads explode the joint search space).
So we split the picture into two complementary renders that *tile* the pipeline:

  1. results/resistome_dag.{svg,png} — the MAIN resistome (annotation) view.
     Seeds the four coupling-heavy intermediates (ORFs, COMEBin bins,
     contig->bin table, MetaPhlAn profile) as givens, leaving everything else
     derivable. Shows all 13 terminal resistome tools wired up: ORF chunking ->
     the five ARG/DIAMOND tools (RGI, AMRFinderPlus, MEGARes, BacMet, VFDB) +
     their merges, plus PathoFact, IntegronFinder, MobileElementFinder,
     VirSorter2, geNomad, RGI-on-MAGs, inStrain, and the MetaPhlAn->FEAST
     source-tracking leg, plus every reference-DB downloader leg.

  2. results/resistome_upstream_dag.{svg,png} — the companion preprocessing view:
     assembly + bam + short_reads -> Prodigal (ORFs), COMEBin (bins + table),
     MetaPhlAn (profile). These are the nodes seeded-away in render 1.

Two inputs in render 1 are STUBS (no real data yet, see plan PG3/PG4):
  - annotation::pathofact_db   <- transforms/logistics/downloadPathofactDB.py
  - annotation::feast_sources  <- transforms/logistics/downloadFeastSources.py
They let the graph close for visualisation; they are not runnable.

Run with the msm env on PATH (has metasmith + graphviz `dot`):
  /home/tony/lib/miniforge3/envs/msm/bin/python main/render_resistome_dag.py
"""
import os
import sys
import tempfile
from pathlib import Path

from metasmith.python_api import (
    Agent, Runtime, Source,
    DataInstanceLibrary, TransformInstanceLibrary,
    TargetBuilder,
)

MLIB = Path(__file__).resolve().parent.parent
OUT_DIR = MLIB / "results"
OUT_DIR.mkdir(parents=True, exist_ok=True)

tmp = Path(tempfile.mkdtemp(prefix="resistome_dag_"))
smith = Agent(home=Source.FromLocal(tmp), runtime=Runtime.APPTAINER)

# Resources: full container library + all relevant transform domains.
containers = DataInstanceLibrary.Load(MLIB / "resources/env")
transforms = [
    TransformInstanceLibrary.Load(MLIB / "transforms/functionalAnnotation"),
    TransformInstanceLibrary.Load(MLIB / "transforms/metagenomics"),  # incl. binning/ + taxonomy/
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
    """Sweep (budget, seed) until a COMPLETE plan (0 dropped). Never print
    task.plan raw — its repr embeds the whole MCTS tree (hundreds of MB)."""
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


# Ensure graphviz `dot` (in the env bin) is on PATH for the renderer.
os.environ["PATH"] = f"{Path(sys.executable).parent}:{os.environ.get('PATH', '')}"

# ── Render 1: MAIN resistome DAG ──────────────────────────────────────────────
# Seed assembly + bam + short_reads + the four coupling-heavy intermediates
# (ORFs, COMEBin bins, contig->bin table, MetaPhlAn profile). Seeding all four
# keeps the 13-target joint solve tractable at the default budget (~20s, fully
# deterministic at seed=1); the ORF-chunking + DIAMOND fan-out/merge machinery is
# still derived (orf_chunk is NOT seeded), so it appears in the graph. The three
# producers seeded-away here (Prodigal, COMEBin, MetaPhlAn) are shown in render 2.
main = new_inputs("main")
asm = main.AddItem(mock("sample.fna"), "sequences::assembly")
main.AddItem(mock("sample.bam"), "alignment::bam", parents={asm})
main.AddItem(mock("sample.fq.gz"), "sequences::short_reads", parents={asm})
main.AddItem(mock("sample.orfs.faa"), "sequences::orfs", parents={asm})
main.AddItem(mock("bin.1.fa"), "sequences::comebin_bin_fasta", parents={asm})
main.AddItem(mock("contig_to_bin.tsv"), "binning::comebin_contig_to_bin_table", parents={asm})
main.AddItem(mock("metaphlan_profile.tsv"), "taxonomy::metaphlan_profile", parents={asm})
main.Save()

# One target per terminal resistome transform (multi-output transforms — pathofact,
# virsorter2, genomad, integronfinder, instrain — still render all their products).
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

# ── Render 2: companion UPSTREAM (preprocessing) DAG ──────────────────────────
# assembly + bam + short_reads -> the three intermediates seeded-away above.
up = new_inputs("upstream")
asm2 = up.AddItem(mock("u_sample.fna"), "sequences::assembly")
up.AddItem(mock("u_sample.bam"), "alignment::bam", parents={asm2})
up.AddItem(mock("u_sample.fq.gz"), "sequences::short_reads", parents={asm2})
up.Save()

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

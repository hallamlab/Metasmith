#!/usr/bin/env python3
"""Render the Spanish Lakes viromics DAG locally (planning only).

Nothing here is runnable and nothing is opened: inputs are empty placeholder
files, and the point is to see which transforms the solver reaches for and how
they wire together.

  <script dir>/cache/viromics_dag.{svg,png} — assembly + clean short reads -> the four
  viral legs:
    * splitContigsForAmr -> contig_batch, feeding the two contig-level viral
      callers, VirSorter2 and geNomad (virus + plasmid summaries);
    * DRAM-v on the VirSorter2 viral contigs + affi-contigs (AMG distillation);
    * BBMap crAssphage coverage off the clean reads (human faecal marker);
    plus every reference-DB downloader leg (VirSorter2 DB, DRAM DB, geNomad DB,
    crAssphage reference).

Reads are seeded as `sequences::clean_short_reads` rather than raw reads so the
graph stops at the viromics boundary — seeding raw reads instead would pull
bbduk and the whole assembly leg back in, which is a different picture.

Run with the msm env on PATH (has metasmith + graphviz `dot`):
  mamba run -n msm python main/render_viromics_dag.py
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

def _find_mlib() -> Path:
    """Walk up to the library root — the dir holding data_types/ + transforms/ —
    so this script keeps working wherever under the repo it is filed."""
    for d in Path(__file__).resolve().parents:
        if (d / "data_types").is_dir() and (d / "transforms").is_dir():
            return d
    raise RuntimeError(f"no MetasmithLibraries root above {__file__}")


MLIB = _find_mlib()
OUT_DIR = Path(__file__).resolve().parent / "cache"
OUT_DIR.mkdir(parents=True, exist_ok=True)

tmp = Path(tempfile.mkdtemp(prefix="viromics_dag_"))
smith = Agent(home=Source.FromLocal(tmp), runtime=Runtime.APPTAINER)

containers = DataInstanceLibrary.Load(MLIB / "resources/env")
transforms = [
    TransformInstanceLibrary.Load(MLIB / "transforms/functionalAnnotation"),
    TransformInstanceLibrary.Load(MLIB / "transforms/metagenomics"),  # incl. taxonomy/
    TransformInstanceLibrary.Load(MLIB / "transforms/logistics"),
]

TYPE_LIBS = ["sequences.yml", "annotation.yml", "taxonomy.yml", "ref.yml"]


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
    t = None
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
    hints = getattr(getattr(t, "plan", None), "hints", None)
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

viromics = new_inputs("viromics")
asm = viromics.AddItem(mock("sample.fna"), "sequences::assembly")
viromics.AddItem(mock("sample.clean.fq.gz"), "sequences::clean_short_reads", parents={asm})
viromics.Save()

# One target per terminal viromics transform. geNomad's two summaries are listed
# separately so the plasmid leg is visible even though one step produces both.
TARGETS = [
    "annotation::virsorter2_viral_sequences",
    "taxonomy::genomad_virus_summary",
    "taxonomy::genomad_plasmid_summary",
    "annotation::dramv_distill",
    "annotation::crassphage_coverage",
]

task = plan_with_sweep(
    "viromics", viromics, "sequences::assembly", TARGETS,
    configs=[(1024, 256), (2048, 512), (4096, 1024)],
    seeds=[1, 7, 13, 42, 99],
)
if task is None:
    sys.exit(1)
render(task, "viromics_dag")

print(f"\nDAG rendered under {OUT_DIR}.")

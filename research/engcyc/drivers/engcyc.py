#!/usr/bin/env python3
"""Engcyc: one pipeline over every metagenome in data/engcyc/manifest.tsv, merged from bench-E5,
the Spanish Lakes metagenomics and viromics runs, and the Cyanoverse annotation layer.

A plan takes one read shape, so each shape solves on its own:

  short_pe        paired short reads as two gzipped files: bbduk, MEGAHIT
  short_se        single-end short reads: bbduk, MEGAHIT
  long_hifi       PacBio HiFi: hifiasm-meta
  long_noisy      Nanopore or PacBio CLR: Filtlong, Flye --meta
  virome_pe       a VLP virome as paired short reads: the front end and the viral lane, no MAG lane
  given_assembly  reads plus an existing assembly (E5's hybrids, Spanish Lakes, Cyanoverse)

After the front end every shape runs the same lanes on its assembly: the MAG lane (three binners,
the CheckM2 filter, skani, GTDB-Tk, CarveMe and MEMOTE), the annotation layer (the 4-lane GPR and
Cyanoverse's six annotators), and the viral lane, whose callers run per sample and whose catalogue
tools run once per dataset on the merged, frozen candidate set.

`solve` builds every input as a deferred placeholder, solves and runs nothing. --dag renders each plan
twice under page/dags/: <shape>_steps.svg with the tools only, leaving out the steps that fetch and build
reference databases, and <shape>_full.svg with every step and every data intermediate.
"""

import argparse
import dataclasses
import json
import sys
import tempfile
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]
MLIB = REPO / "src" / "metasmith_libraries"
DAG_DIR = HERE.parent / "page" / "dags"
sys.path.insert(0, str(MLIB))

import _authoring as A  # noqa: E402
from metasmith.python_api import DEFERRED, DataInstanceLibrary, Spec, TransformInstanceLibrary, record_library  # noqa: E402
from metasmith.models.dag_renderer import DagMode  # noqa: E402

TYPE_LIBS = ("sequences.yml", "alignment.yml", "ref.yml", "annotation.yml", "taxonomy.yml",
             "binning.yml", "binning_local.yml", "viromics.yml", "modelling.yml")

# shape -> (read layout, assembler's product, lanes)
SHAPES = {
    "short_pe": ("split", "sequences::megahit_assembly", {"mags", "annotation", "viral"}),
    "short_se": ("se", "sequences::megahit_assembly", {"mags", "annotation", "viral"}),
    "long_hifi": ("hifi", "sequences::hifiasm_meta_assembly", {"mags", "annotation", "viral"}),
    "long_noisy": ("noisy", "sequences::flye_assembly", {"mags", "annotation", "viral"}),
    "virome_pe": ("split", "sequences::megahit_assembly", {"viral"}),
    "given_assembly": ("given", "sequences::assembly", {"mags", "annotation", "viral"}),
}
# Each shape's assembler, by the file its transform lives in. metaSPAdes answers sequences::assembly too,
# and Flye and hifiasm-meta both take long reads, so every other assembler leaves the search.
ASSEMBLERS = {"spades.py", "megahit.py", "flye.py", "flye_raw.py", "hifi/hifiasm_meta.py", "hifi/hifiasm.py",
              "hifi/miniasm.py"}
KEEP = {"split": {"megahit.py"}, "se": {"megahit.py"}, "hifi": {"hifi/hifiasm_meta.py"},
        "noisy": {"flye.py"}, "given": set()}
# Transforms whose quality-MAG copies stand in for them. Left in, DAS Tool's bins answer the de novo tree's
# putative_genome, and iPHoP then adds quality MAGs that the tree does not describe.
REPLACED = {"binning/das_tool.py", "taxonomy/gtdbtk_de_novo.py"}
GEM_LANE = {"prodigal_from_quality_bin.py", "carveme_from_orfs.py", "memote_score.py"}


def declare_inputs(shape):
    layout = SHAPES[shape][0]
    lib = DataInstanceLibrary(Path(tempfile.mkdtemp(prefix=f"engcyc-{shape}-")))
    for tl in TYPE_LIBS:
        lib.AddTypeLibrary(A.TYPES / tl)
    # The dataset root. The viral merge groups a dataset's per-sample calls under it, so it is shared rather
    # than per sample.
    study = lib.AddValue("contig_study.json", {"logistics": "engcyc dataset"}, "viromics::contig_study")
    meta = lib.AddValue("reads_metadata.json", {
        "split": {"parity": "paired", "length_class": "short"},
        "se": {"parity": "single", "length_class": "short"},
        "hifi": {"parity": "single", "length_class": "long"},
        "noisy": {"parity": "single", "length_class": "long"},
        "given": {"parity": "paired", "length_class": "short"},
    }[layout], "sequences::read_metadata", parents={study})
    # merge_candidate_calls finds a sample's assembly through its read_pair, so every shape names one.
    pair = lib.AddValue("read_pair.txt", "sample_1", "sequences::read_pair", parents={meta})
    if layout in ("split", "given"):
        lib.AddItem(DEFERRED, "sequences::zipped_forward_short_reads", parents={pair})
        lib.AddItem(DEFERRED, "sequences::zipped_reverse_short_reads", parents={pair})
    elif layout == "se":
        lib.AddItem(DEFERRED, "sequences::short_reads_se", parents={pair})
    else:
        lib.AddItem(DEFERRED, "sequences::long_reads", parents={pair})
    if layout == "given":
        lib.AddItem(DEFERRED, "sequences::assembly", parents={pair})
    # Study-wide inputs that no transform produces.
    lib.AddItem(DEFERRED, "ref::mnxr_lookup")
    lib.AddItem(DEFERRED, "ref::label_transfer_landmarks")
    lib.AddItem(DEFERRED, "modelling::media")
    lib.AddValue("medium_name.txt", "M8", "modelling::medium_name")
    lib.AddValue("busco_source.marker", "bacteria_odb10", "annotation::busco_source")
    library = record_library(lib)
    shared = ["contig_study.json", "medium_name.txt", "busco_source.marker"] + sorted(
        str(path) for path, dtype in getattr(library, "manifest", {}).items()
        if dtype in ("ref::mnxr_lookup", "ref::label_transfer_landmarks", "modelling::media"))
    return library, shared


class Targets:
    def __init__(self):
        self.items = []

    def add(self, dtype, *parents):
        parents = [p for p in parents if p is not None]
        self.items.append({"type": dtype, "parents": parents} if parents else dtype)
        return len(self.items) - 1


def build_targets(shape):
    _, assembly, lanes = SHAPES[shape]
    t = Targets()
    # A given cannot be a target, so a given assembly pins nothing and its targets go unparented.
    given = shape == "given_assembly"
    asm = None if given else t.add(assembly)
    if not given:
        t.add("sequences::read_qc_stats")
    # Every annotator pins to this slot, so each reads the one ORF set the layout publishes.
    orfs = t.add("sequences::orfs", asm)
    for dtype in ("sequences::gff", "sequences::assembly_stats", "sequences::assembly_per_contig_coverage",
                  "taxonomy::metabuli"):
        t.add(dtype, asm)
    if SHAPES[shape][0] in ("split", "se", "given"):
        t.add("taxonomy::phyloflash_summary")

    if "mags" in lanes:
        binners = [t.add(f"sequences::{b}_bin_fasta", asm) for b in ("metabat2", "semibin2", "comebin")]
        for b in ("metabat2", "semibin2", "comebin"):
            t.add(f"binning::{b}_contig_to_bin_table", asm)
        for b in binners:
            t.add("taxonomy::checkm_stats", b)
        mags = t.add("binning_local::quality_bin_fasta", asm)
        t.add("binning_local::cluster_table", asm)
        t.add("taxonomy::gtdbtk", mags)
        gem = t.add("modelling::carveme_model", t.add("sequences::bin_orfs", mags))
        t.add("modelling::memote_score", gem)

    if "annotation" in lanes:
        for dtype in ("annotation::gpr_table", "annotation::kofamscan_results", "annotation::clean_predictions",
                      "annotation::diamond_uniref50_results", "annotation::proteinbert_embeddings",
                      "annotation::interproscan_results", "annotation::deepec_predictions",
                      "annotation::busco_results", "annotation::predictf_results"):
            t.add(dtype, orfs)
        for dtype in ("annotation::bakta_tsv", "annotation::antismash_json"):
            t.add(dtype, asm)

    if "viral" in lanes:
        # Pooled tools name the frozen set outright, and per-contig tools pin to the assembly: a per-contig
        # tool that can reach the frozen set lets its calls feed the merge that made it, and the solve cycles.
        frozen = t.add("viromics::dereplicated_candidate_virus", asm)
        for dtype in ("viromics::contig_length_table", "viromics::precluster_table", "viromics::votu_cluster_table",
                      "viromics::checkv_contamination", "viromics::checkv_quality_summary",
                      "viromics::vcontact3_network"):
            t.add(dtype, frozen)
        # Viral KOs are the whole-assembly KOfam calls, joined to the frozen set through its provenance table.
        t.add("annotation::kofamscan_descriptions", orfs)
        if "mags" in lanes:
            t.add("viromics::host_prediction_genome", frozen)
            t.add("viromics::spacer_host_links", frozen)
        t.add("annotation::dramv_distill", asm)
        t.add("taxonomy::genomad_taxonomy", asm)
    return t.items


def build_transforms(shape):
    layout = SHAPES[shape][0]
    masked = ASSEMBLERS - KEEP[layout]
    assembly = TransformInstanceLibrary.Load((MLIB / "transforms" / "assembly").resolve()).AsView(
        {Path(p) for p in masked}, invert=True)
    # prodigal-gv's ORFs on the frozen set descend from the assembly, so they answer the whole-assembly ORF
    # slot as well as prodigal's do, and the solver picks either. Without it the slot has one producer.
    viromics = TransformInstanceLibrary.Load((MLIB / "transforms" / "viromics").resolve()).AsView(
        {Path("prodigal_gv.py")}, invert=True)
    # MEMOTE scores any metabolic_model, and four other modelling transforms make one, so the GEM lane keeps
    # only its own three. Its prodigal reads the quality MAG pool, as gtdbtk_quality does.
    modelling = TransformInstanceLibrary.Load((MLIB / "transforms" / "metabolicModelling").resolve()).AsView(
        {Path(p) for p in GEM_LANE})
    # Order decides which library answers a namespace, and metagenomics is the only one carrying binning
    # and binning_local.
    metagenomics = TransformInstanceLibrary.Load((MLIB / "transforms" / "metagenomics").resolve()).AsView(
        {Path(p) for p in REPLACED}, invert=True)
    return [metagenomics, assembly, *A.transforms(
        "logistics", "functionalAnnotation", "fabfos"), viromics, modelling]


def is_reference_step(step):
    return Path(step.transform._path).stem.startswith(("download", "build", "ref_"))


def solve(shape, args):
    inputs, shared = declare_inputs(shape)
    targets = build_targets(shape)
    spec = Spec(
        input_library=inputs,
        sample_type="sequences::read_metadata",
        shared_input_paths=shared,
        target_types=targets,
        transform_libraries=build_transforms(shape),
        resource_libraries=[A.envs(), MLIB / "resources" / "lib"],
    )
    t0 = time.time()
    task = spec.Solve(max_iter=args.max_iter, max_refine=args.max_refine, seed=args.seed)
    plan = task.plan
    dropped = sorted(plan.dropped_targets)
    print(f"\n=== {shape}: {len(plan.steps)} steps, {len(targets) - len(dropped)}/{len(targets)} targets, "
          f"solved in {time.time() - t0:.1f}s", flush=True)
    for step in plan.steps:
        produced = sorted({i.dtype_name for g in step.produces for i in g})
        print(f"  {step.order:>3}. {Path(step.transform._path).stem:<28} -> {', '.join(produced)}")
    orf_producers = {Path(t.producing_step.transform._path).stem for t in plan.targets
                     if t.instance.dtype_name == "sequences::orfs"}
    if plan.steps and orf_producers != {"prodigal"}:
        print(f"the whole-assembly ORFs come from {sorted(orf_producers)}, not prodigal")
        return False
    if dropped:
        print(f"dropped: {', '.join(dropped)}")
        for hint in plan.hints or []:
            print(f"  hint: {hint}")
    if args.dag and plan.steps:
        DAG_DIR.mkdir(parents=True, exist_ok=True)
        tools = dataclasses.replace(plan, steps=[s for s in plan.steps if not is_reference_step(s)])
        for drawn, mode, suffix in ((tools, DagMode.STEPS, "steps"), (plan, DagMode.PLAIN, "full")):
            out = drawn.RenderDAG(str(DAG_DIR / f"{shape}_{suffix}"), format="svg", mode=mode)
            print(f"dag: {out}")
        (DAG_DIR / f"{shape}.json").write_text(json.dumps({
            "steps": len(plan.steps), "tools": len(tools.steps), "targets": len(targets),
            "transforms": sorted({Path(s.transform._path).stem for s in tools.steps}),
        }, indent=1))
    return not dropped and bool(plan.steps)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("solve", help="solve one plan per shape on deferred inputs; runs nothing")
    p.add_argument("--shape", choices=list(SHAPES), action="append", help="repeatable; default every shape")
    p.add_argument("--dag", action="store_true", help="render each plan to page/dags/<shape>_{steps,full}.svg")
    p.add_argument("--max-iter", type=int, default=1024)
    p.add_argument("--max-refine", type=int, default=256)
    p.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()
    failed = [s for s in (args.shape or SHAPES) if not solve(s, args)]
    if failed:
        print(f"FAILED: {', '.join(failed)}", file=sys.stderr)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""Plan nine real analyses through round 4's new transforms and record what solved.

Round 3 claimed parity against a table of KBase task verbs. A verb table is not a
plan: nothing had ever asked the solver whether the chains those transforms sit in
actually connect. This is that question, asked once per analysis a researcher would
recognise -- named for what it answers, given the inputs a lab would have, and
carrying one or two terminal products rather than a reachability probe's single hop.

Each analysis is one `Spec.Solve()` and one JSON record in `kbase_parity.jsonl`:
whether it solved, how many steps, which transform files the planner picked, which
targets it dropped, and -- when it did not solve -- a failure CLASS rather than a
count. A red row is a result here. It is the first time these chains have been
planned at all, so a named failure is the deliverable it produces.

Two conventions carried from the shipped templates, both load-bearing:

  * A target is a solver slot. Every analysis names one terminal product and hangs
    the rest off it with `parents=[i]`, because an unpinned target downstream of an
    ambiguous type (`sequences::assembly` has nine producers, `read_qc_stats` three)
    is free to be answered from a different producer than its sibling -- the planner
    then runs both and splits the work.
  * The transform libraries are named per analysis rather than passed wholesale.
    A solve over every group is not a harder test, it is a meaningless one.

    PYTHONPATH=src python research/metasmith_libraries/probe_kbase_parity.py [id ...]
"""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
import time
import traceback
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent
MLIB = REPO / "src" / "metasmith_libraries"
TYPES = MLIB / "data_types"
OUT = HERE / "kbase_parity.jsonl"

sys.path.insert(0, str(MLIB))

from metasmith.python_api import (  # noqa: E402
    DEFERRED, DataInstanceLibrary, Spec, TransformInstanceLibrary,
)


def _lib(build) -> DataInstanceLibrary:
    library = DataInstanceLibrary(Path(tempfile.mkdtemp(prefix="msm-parity-")))
    build(library)
    return library


def _types(lib, *names):
    for n in names:
        lib.AddTypeLibrary(TYPES / f"{n}.yml")


# ----------------------------------------------------------------------------
# The analyses. One function per question; it returns the Spec to solve.
# ----------------------------------------------------------------------------

def a1_model_from_isolate_reads():
    """Draft a metabolic model for a new isolate, from its raw reads."""
    def inputs(lib):
        _types(lib, "sequences", "ref", "annotation", "modelling", "ecspr", "alignment")
        meta = lib.AddValue("read_metadata.json",
                            {"parity": "paired", "length_class": "short"},
                            "sequences::read_metadata")
        pair = lib.AddValue("read_pair.txt", "isolate_1", "sequences::read_pair",
                            parents={meta})
        lib.AddItem(DEFERRED, "sequences::zipped_forward_short_reads", parents={pair})
        lib.AddItem(DEFERRED, "sequences::zipped_reverse_short_reads", parents={pair})
        lib.AddItem(DEFERRED, "annotation::bakta_db")
        lib.AddItem(DEFERRED, "ref::mnxr_lookup")
        lib.AddItem(DEFERRED, "ref::label_transfer_landmarks")
        lib.AddItem(DEFERRED, "ref::metabolism_vocab")
        # Round 5 wrote the modelling bodies and found the lane had an id->MNXR
        # bridge and no stoichiometry: reac_prop carries the equations, chem_prop
        # the formulas, chem_xref the BiGG bridge MetBridge needs.
        lib.AddItem(DEFERRED, "ref::mnx_reac_prop")
        lib.AddItem(DEFERRED, "ref::mnx_chem_prop")
        lib.AddItem(DEFERRED, "ref::mnx_chem_xref")
        lib.AddItem(DEFERRED, "modelling::media")
        lib.AddItem(DEFERRED, "ecspr::conditions")
    return Spec(
        input_library=_lib(inputs),
        sample_type=None,
        target_types=[
            "sequences::megahit_assembly",
            {"type": "annotation::bakta_annotated_proteins", "parents": [0]},
            {"type": "annotation::gpr_table", "parents": [0]},
            {"type": "modelling::gapfilled_model", "parents": [2]},
            {"type": "modelling::fba_solution", "parents": [3]},
        ],
        transform_libraries=[MLIB / "transforms" / g for g in (
            "logistics", "assembly", "metagenomics", "functionalAnnotation",
            "fabfos", "kbase")],
        resource_libraries=[MLIB / "resources" / "env", MLIB / "resources" / "lib"],
    )


def a2_model_from_bigg():
    """The curated control: the same flux question, from a published model."""
    def inputs(lib):
        _types(lib, "modelling", "ecspr")
        lib.AddValue("bigg_id.txt", "iML1515", "modelling::bigg_model_id")
        lib.AddItem(DEFERRED, "modelling::media")
        lib.AddItem(DEFERRED, "ecspr::conditions")
    return Spec(
        input_library=_lib(inputs),
        sample_type=None,
        target_types=["modelling::fba_solution"],
        transform_libraries=[MLIB / "transforms" / "kbase"],
        resource_libraries=[MLIB / "resources" / "env", MLIB / "resources" / "lib"],
    )


def a3_finish_long_read_isolate():
    """Finish a long-read isolate genome and hand back an annotated record."""
    def inputs(lib):
        _types(lib, "sequences", "ref", "annotation")
        # ONE read_metadata node for the isolate, parenting BOTH read sets. Polishing
        # joins a long-read assembly to a short-read library, and what says "same
        # sample" is a shared ancestor. Two metadata nodes, one per read set, leaves
        # the assembly and the reads with no common ancestor and polypolish with no
        # candidate pair.
        #
        # That ancestor is now `sequences::sample_name`, above the metadata rather than
        # the metadata itself: round 5 recorded that the isolate was saying "same
        # sample" by sharing a node that was never named for the job, and a name is
        # what the job actually has.
        name = lib.AddValue("sample_name.txt", "isolate_1", "sequences::sample_name")
        meta = lib.AddValue("read_metadata.json",
                            {"parity": "paired", "length_class": "hybrid"},
                            "sequences::read_metadata", parents={name})
        lib.AddItem(DEFERRED, "sequences::long_reads", parents={meta})
        pair = lib.AddValue("read_pair.txt", "isolate_1", "sequences::read_pair",
                            parents={meta})
        lib.AddItem(DEFERRED, "sequences::zipped_forward_short_reads", parents={pair})
        lib.AddItem(DEFERRED, "sequences::zipped_reverse_short_reads", parents={pair})
        lib.AddItem(DEFERRED, "annotation::bakta_db")
    return Spec(
        input_library=_lib(inputs),
        sample_type=None,
        target_types=[
            "sequences::polished_assembly",
            {"type": "sequences::gbk", "parents": [0]},
            {"type": "annotation::bakta_summary", "parents": [0]},
            "sequences::nanoplot_html_report",
        ],
        transform_libraries=[MLIB / "transforms" / g for g in (
            "logistics", "assembly", "metagenomics", "functionalAnnotation", "kbase")],
        resource_libraries=[MLIB / "resources" / "env", MLIB / "resources" / "lib"],
    )


def a4_mags_from_metagenome():
    """Recover MAGs from a metagenome, with the length filter KBase users apply."""
    def inputs(lib):
        _types(lib, "sequences", "ref", "annotation", "taxonomy", "binning", "alignment")
        meta = lib.AddValue("read_metadata.json",
                            {"parity": "paired", "length_class": "short"},
                            "sequences::read_metadata")
        pair = lib.AddValue("read_pair.txt", "sample_1", "sequences::read_pair",
                            parents={meta})
        lib.AddItem(DEFERRED, "sequences::zipped_forward_short_reads", parents={pair})
        lib.AddItem(DEFERRED, "sequences::zipped_reverse_short_reads", parents={pair})
        lib.AddValue("min_contig_length.txt", "2500", "sequences::min_contig_length")
        lib.AddItem(DEFERRED, "ref::gtdb")
    return Spec(
        input_library=_lib(inputs),
        sample_type=None,
        target_types=[
            "sequences::megahit_assembly",
            {"type": "sequences::filtered_assembly", "parents": [0]},
            {"type": "sequences::metabat2_bin_fasta", "parents": [1]},
            {"type": "taxonomy::checkm_stats", "parents": [2]},
            {"type": "taxonomy::gtdbtk", "parents": [2]},
            "sequences::fastqc_html_report",
            "sequences::fastp_json_report",
        ],
        transform_libraries=[MLIB / "transforms" / g for g in (
            "logistics", "assembly", "metagenomics", "kbase")],
        resource_libraries=[MLIB / "resources" / "env", MLIB / "resources" / "lib"],
    )


def _survey_inputs(lib):
    _types(lib, "sequences", "ref", "taxonomy", "amplicon", "aspire")
    survey = lib.AddValue("survey.json", {"logistics": "survey"}, "amplicon::survey")
    # The sample's own name, above its reads, so `kraken_abundance` can label a row
    # by what the study calls the sample rather than by a staged file's stem.
    # Deferred like `ncbi::genome_name`: nothing produces a name.
    name = lib.AddValue("sample_name.txt", "sample_1", "sequences::sample_name",
                        parents={survey})
    meta = lib.AddValue("read_metadata.json",
                        {"parity": "paired", "length_class": "short"},
                        "sequences::read_metadata", parents={name})
    pair = lib.AddValue("read_pair.txt", "sample_1", "sequences::read_pair",
                        parents={meta})
    lib.AddItem(DEFERRED, "sequences::zipped_forward_short_reads", parents={pair})
    lib.AddItem(DEFERRED, "sequences::zipped_reverse_short_reads", parents={pair})
    lib.AddItem(DEFERRED, "ref::kraken2_db")
    # The study's own sample sheet. Every producer of these two is still inside the
    # ASPIRE pipeline and still gated on `aspire::run`, so outside a run they are
    # what round 3 calls a deferred input: a table the researcher already has.
    lib.AddItem(DEFERRED, "aspire::analysis_metadata", parents={survey})
    lib.AddItem(DEFERRED, "aspire::analysis_asv_meta", parents={survey})
    return survey


def a5_community_structure():
    """Who is there, and how does the community separate -- from shotgun reads."""
    return Spec(
        input_library=_lib(_survey_inputs),
        sample_type=None,
        target_types=[
            "amplicon::asv_table",
            {"type": "aspire::diversity_outputs", "parents": [0]},
            {"type": "aspire::umap_plots", "parents": [0]},
            {"type": "aspire::paired_group_contrast_outputs", "parents": [0]},
            {"type": "aspire::measurement_association_outputs", "parents": [0]},
        ],
        transform_libraries=[MLIB / "transforms" / g for g in (
            "logistics", "metagenomics", "kbase", "aspire")],
        resource_libraries=[MLIB / "resources" / "env", MLIB / "resources" / "lib"],
    )


def a5b_cooccurrence_network():
    """The same community, asked for a co-occurrence network instead."""
    def inputs(lib):
        survey = _survey_inputs(lib)
        # Feature taxonomy is an import here: the ASPIRE lane gets it from SINA against
        # SILVA, and a kraken-based survey has no producer for it.
        lib.AddItem(DEFERRED, "amplicon::asv_taxonomy", parents={survey})
        # The ASPIRE policy tokens are inputs, not configuration -- each is a pair of
        # mutually exclusive types and registering one arm is how a stage is selected.
        for token in ("spieceasi_on", "indicspecies_on", "network_modules_on",
                      "asv_mag_link_off", "graph_network_on"):
            lib.AddValue(f"policy_{token}.txt", token, f"aspire::{token}",
                         parents={survey})
    return Spec(
        input_library=_lib(inputs),
        sample_type=None,
        target_types=[
            "amplicon::asv_table",
            {"type": "aspire::network_graph_all", "parents": [0]},
            {"type": "aspire::network_outputs", "parents": [0]},
        ],
        transform_libraries=[MLIB / "transforms" / g for g in (
            "logistics", "metagenomics", "kbase", "aspire")],
        resource_libraries=[MLIB / "resources" / "env", MLIB / "resources" / "lib"],
    )


def _rnaseq_inputs(lib):
    _types(lib, "sequences", "ref", "annotation", "transcriptomics", "alignment")
    exp = lib.AddValue("experiment.txt", "rnaseq_study", "transcriptomics::experiment")
    meta = lib.AddValue("read_metadata.json",
                        {"parity": "paired", "length_class": "short"},
                        "sequences::read_metadata", parents={exp})
    pair = lib.AddValue("read_pair.txt", "library_1", "sequences::read_pair",
                        parents={meta})
    lib.AddItem(DEFERRED, "sequences::zipped_forward_short_reads", parents={pair})
    lib.AddItem(DEFERRED, "sequences::zipped_reverse_short_reads", parents={pair})
    lib.AddItem(DEFERRED, "sequences::assembly", parents={exp})
    lib.AddItem(DEFERRED, "annotation::bakta_db")
    # KBase ships four clustering apps here and round 5 made them one transform with
    # a method knob, which is an input like any other -- the language has no optional
    # requirement, so the knob is a one-line file a template defers.
    lib.AddItem(DEFERRED, "transcriptomics::clustering_params")


def a6_expression_response():
    """What changed in the transcriptome, and which genes move together."""
    return Spec(
        input_library=_lib(_rnaseq_inputs),
        sample_type=None,
        target_types=[
            "transcriptomics::bowtie2_bam",
            {"type": "transcriptomics::stringtie_quant_gtf", "parents": [0]},
            {"type": "transcriptomics::gene_count_table", "parents": [1]},
            {"type": "transcriptomics::expression_clusters", "parents": [2]},
            {"type": "transcriptomics::diff_count_table", "parents": [1]},
        ],
        transform_libraries=[MLIB / "transforms" / g for g in (
            "logistics", "assembly", "metagenomics", "functionalAnnotation",
            "transcriptomics", "kbase")],
        resource_libraries=[MLIB / "resources" / "env", MLIB / "resources" / "lib"],
    )


def a6b_functional_enrichment():
    """Which functions are over-represented in a gene set the researcher brings."""
    def inputs(lib):
        _types(lib, "sequences", "ref", "annotation")
        lib.AddItem(DEFERRED, "sequences::assembly")
        lib.AddItem(DEFERRED, "annotation::gene_set")
        lib.AddItem(DEFERRED, "ref::interproscan_data")
    return Spec(
        input_library=_lib(inputs),
        sample_type=None,
        target_types=["annotation::go_overrepresentation"],
        transform_libraries=[MLIB / "transforms" / g for g in (
            "logistics", "metagenomics", "functionalAnnotation", "kbase")],
        resource_libraries=[MLIB / "resources" / "env", MLIB / "resources" / "lib"],
    )


def a7_compare_strains():
    """Compare a set of strains: what is core, what is accessory, how related."""
    def inputs(lib):
        _types(lib, "ncbi", "pangenome", "sequences", "annotation", "ref", "taxonomy")
        pan = lib.AddValue("pangenome.json", {"logistics": "pangenome"},
                           "pangenome::pangenome")
        nm = lib.AddItem(DEFERRED, "ncbi::genome_name", parents={pan})
        lib.AddItem(DEFERRED, "ncbi::assembly_accession", parents={nm})
        lib.AddItem(DEFERRED, "ref::gtdb")
    return Spec(
        input_library=_lib(inputs),
        sample_type=None,
        target_types=[
            "pangenome::ppanggolin_matrix",
            {"type": "pangenome::ppanggolin_summary", "parents": [0]},
            "taxonomy::gtdbtk_tree",
        ],
        transform_libraries=[MLIB / "transforms" / g for g in (
            "logistics", "metagenomics", "functionalAnnotation", "pangenome", "kbase")],
        resource_libraries=[MLIB / "resources" / "env", MLIB / "resources" / "lib"],
    )


def a8_gene_presence():
    """Is this gene family in these genomes, and what do the copies look like."""
    def inputs(lib):
        _types(lib, "sequences", "annotation", "comparative", "ref")
        lib.AddItem(DEFERRED, "sequences::assembly")
    return Spec(
        input_library=_lib(inputs),
        sample_type=None,
        target_types=[
            "annotation::homolog_hits",
            "comparative::msa",
        ],
        transform_libraries=[MLIB / "transforms" / g for g in (
            "logistics", "metagenomics", "kbase")],
        resource_libraries=[MLIB / "resources" / "env", MLIB / "resources" / "lib"],
    )


def a9_strain_variants():
    """Which variants separate this isolate from the reference it was mapped to."""
    def inputs(lib):
        _types(lib, "sequences", "alignment", "ref")
        lib.AddItem(DEFERRED, "sequences::assembly")
        meta = lib.AddValue("read_metadata.json",
                            {"parity": "paired", "length_class": "short"},
                            "sequences::read_metadata")
        pair = lib.AddValue("read_pair.txt", "isolate_1", "sequences::read_pair",
                            parents={meta})
        lib.AddItem(DEFERRED, "sequences::zipped_forward_short_reads", parents={pair})
        lib.AddItem(DEFERRED, "sequences::zipped_reverse_short_reads", parents={pair})
    return Spec(
        input_library=_lib(inputs),
        sample_type=None,
        target_types=[
            "alignment::bam",
            {"type": "alignment::alignment_stats", "parents": [0]},
            {"type": "sequences::variant_calls", "parents": [0]},
        ],
        transform_libraries=[MLIB / "transforms" / g for g in (
            "logistics", "assembly", "metagenomics", "kbase")],
        resource_libraries=[MLIB / "resources" / "env", MLIB / "resources" / "lib"],
    )


ANALYSES = [
    ("a1_model_from_isolate_reads", a1_model_from_isolate_reads,
     "Can I draft a metabolic model for an isolate I just sequenced?",
     "Paired short reads from one bacterial isolate, plus a defined medium.",
     "An assembly, a bakta protein annotation, a GPR evidence table, a gap-filled "
     "genome-scale model and a flux solution on that medium."),
    ("a2_model_from_bigg", a2_model_from_bigg,
     "What does the same flux question look like on a curated published model?",
     "A BiGG accession and the same medium.",
     "A flux solution, directly comparable to the drafted one."),
    ("a3_finish_long_read_isolate", a3_finish_long_read_isolate,
     "Can I finish and annotate a long-read isolate genome?",
     "Nanopore long reads and a short-read library from the same isolate.",
     "Read QC, a short-read-polished assembly and a GenBank record."),
    ("a4_mags_from_metagenome", a4_mags_from_metagenome,
     "Which genomes can I recover from this metagenome, and who are they?",
     "Paired short reads from one environmental sample and a minimum contig length.",
     "Read QC, a length-filtered assembly, bins, completeness and GTDB taxonomy."),
    ("a5_community_structure", a5_community_structure,
     "Who is in these samples and how does the community separate?",
     "Paired short reads per sample, a kraken2 database and the study's sample sheet.",
     "A count table, diversity, an ordination and two group contrasts."),
    ("a5b_cooccurrence_network", a5b_cooccurrence_network,
     "...and can I get a co-occurrence network out of the same table?",
     "The same inputs as a5.",
     "An inferred network and the rendered overlay."),
    ("a6_expression_response", a6_expression_response,
     "What changed in the transcriptome, and which genes move together?",
     "A reference assembly, its annotation and paired RNA-seq libraries.",
     "Alignments, per-gene counts, expression clusters and a differential table."),
    ("a6b_functional_enrichment", a6b_functional_enrichment,
     "Which functions are over-represented in the genes that changed?",
     "The gene set from a6 and the assembly it came from.",
     "A GO over-representation table."),
    ("a7_compare_strains", a7_compare_strains,
     "What is core and what is accessory across this strain collection?",
     "NCBI assembly accessions for the strains, and the GTDB reference.",
     "A pangenome matrix, a functional core/accessory summary and a marker phylogeny."),
    ("a8_gene_presence", a8_gene_presence,
     "Is this gene family present in these genomes, and how do the copies align?",
     "Genome assemblies and the query protein set they are searched with.",
     "A homology hit table and a multiple sequence alignment."),
    ("a9_strain_variants", a9_strain_variants,
     "Which variants separate this isolate from its reference?",
     "Paired short reads and the reference assembly they map to.",
     "An alignment, its quality summary and a VCF."),
]


def classify(task, err: str) -> str:
    if task is not None and not task.ok:
        if not task.plan.steps:
            return "no_plan_at_all"
        return "no_plan_some_targets"
    e = err.lower()
    if "memory" in e or "killed" in e: return "out_of_memory"
    if "not found in" in e or "no such type" in e: return "missing_type"
    if "no samples of type" in e: return "no_samples"
    return "raised"


def _index(spec) -> dict[str, str]:
    out = {}
    for p in spec.transform_libraries:
        lib = TransformInstanceLibrary.Load(Path(str(p)))
        for key, tr in lib.IterateTransforms():
            out[str(tr.model.key)] = f"{Path(str(p)).name}/{key}"
    return out


def run(name, build, question, supplies, products) -> dict:
    rec = {"id": name, "question": question, "supplies": supplies,
           "products": products}
    t0 = time.time()
    try:
        spec = build()
        rec["targets"] = [t if isinstance(t, str) else t["type"]
                          for t in spec.target_types]
        rec["libraries"] = [Path(str(p)).name for p in spec.transform_libraries]
        task = spec.Solve()
        index = _index(spec)
        rec |= {
            "ok": bool(task.ok),
            "steps": len(task.plan.steps),
            # In plan order, not sorted: the artifact draws these as a chain, and a
            # sorted list would draw a chain that never ran.
            "picked": [index.get(str(s.transform.model.key), "?")
                       for s in sorted(task.plan.steps, key=lambda x: x.order)],
            "dropped": sorted(str(x) for x in task.plan.dropped_targets),
            "reason": "solved" if task.ok else classify(task, ""),
            "seconds": round(time.time() - t0, 2),
        }
    except Exception as e:
        rec |= {"ok": False, "steps": 0, "picked": [], "dropped": [],
                "reason": classify(None, f"{e}\n{traceback.format_exc()}"),
                "error": repr(e)[:400], "seconds": round(time.time() - t0, 2)}
    return rec


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("ids", nargs="*", help="only these analyses (default: all)")
    ap.add_argument("--out", default=str(OUT))
    a = ap.parse_args()

    chosen = [x for x in ANALYSES if not a.ids or x[0] in a.ids]
    unknown = set(a.ids) - {x[0] for x in ANALYSES}
    if unknown:
        print(f"no such analysis: {', '.join(sorted(unknown))}", file=sys.stderr)
        return 2

    records = []
    for name, build, question, supplies, products in chosen:
        print(f"[{name}]", flush=True)
        rec = run(name, build, question, supplies, products)
        records.append(rec)
        mark = "OK " if rec["ok"] else "XX "
        print(f"  {mark} {rec['steps']:3d} steps  {rec['seconds']:6.2f}s  {rec['reason']}"
              + (f"  dropped={rec['dropped']}" if rec.get("dropped") else ""), flush=True)

    out = Path(a.out)
    mode = "w" if not a.ids or not out.exists() else "r+"
    if mode == "w":
        out.write_text("".join(json.dumps(r) + "\n" for r in records))
    else:
        kept = [json.loads(l) for l in out.read_text().splitlines() if l]
        kept = [r for r in kept if r["id"] not in {x["id"] for x in records}]
        order = {x[0]: i for i, x in enumerate(ANALYSES)}
        merged = sorted(kept + records, key=lambda r: order.get(r["id"], 99))
        out.write_text("".join(json.dumps(r) + "\n" for r in merged))
    solved = sum(1 for r in records if r["ok"])
    print(f"\n{solved}/{len(records)} solved -> {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

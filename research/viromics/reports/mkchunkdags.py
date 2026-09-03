#!/usr/bin/env python3
"""Draw the solved viromics plan as four readable views instead of one tall one.

The whole plan is 44 steps in a column 4500 px deep, which is honest and almost
unreadable. These are views of that same solve -- same step numbers, same node
vocabulary -- each keeping one part of the workflow and dropping the rest:

    spine       short reads -> contigs -> bins, and nothing else
    calling     contigs -> three callers -> the frozen set -> vOTUs
    function    contigs -> ORFs -> the four GPR lanes -> one gene table
    viral       everything that says something about a viral contig
    taxonomy    who is here, asked of contigs, viral contigs and bins

Two things about the underlying graph shape these views. Its data nodes are
TYPES, not instances, so `sequences::orfs` is one node with two producers
(prodigal on the contigs, prodigal-gv on the frozen set) and the two chunker
steps that follow it are indistinguishable from inside the graph -- each view
keeps the one that belongs to its story. And a step kept without its upstream
neighbours simply loses those edges, so a view's roots are wherever it was cut.

    PYTHONPATH=src python research/viromics/reports/mkchunkdags.py

Keep the basename free of dots: `render()` reads a suffix as the output format.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
sys.path.insert(0, str(ROOT / "src" / "metasmith_libraries"))
sys.path.insert(0, str(HERE.parent))

import viromics_survey_from_paired_reads as V  # noqa: E402

# A chunk keeps the transforms it names and the data types it names, and
# nothing else. `stem#n` picks the n-th run of a transform in step order, which
# is how the two `chunkOrfsForAnnotation` steps are told apart.
CHUNKS = {
    "spine": dict(
        title="Spine",
        transforms=[
            "bbduk", "megahit", "assembly_stats",
            "comebin", "semibin2", "metabat2", "checkm", "aggregator",
            "skani_dedup",
        ],
        types=[
            "sequences::short_reads", "sequences::clean_short_reads",
            "sequences::megahit_assembly", "alignment::bam",
            "sequences::assembly_per_contig_coverage",
            "sequences::comebin_bin_fasta", "sequences::semibin2_bin_fasta",
            "sequences::metabat2_bin_fasta",
            "binning::comebin_contig_to_bin_table",
            "binning::semibin2_contig_to_bin_table",
            "binning::metabat2_contig_to_bin_table",
            "taxonomy::checkm_stats", "binning_local::quality_bin_fasta",
            "binning_local::cluster_table",
        ],
    ),
    "calling": dict(
        title="Viral classification",
        transforms=[
            "splitContigsForAmr", "downloadVirsorter2DB", "ref_genomad",
            "vibrant", "virsorter2", "genomad", "merge_candidate_calls",
            "mmseqs_votu", "mmseqs_precluster", "contig_length_table",
        ],
        types=[
            "sequences::megahit_assembly", "sequences::contig_batch",
            "annotation::virsorter2_db", "ref::genomad",
            "viromics::vibrant_candidate_virus",
            "viromics::genomad_candidate_virus",
            "viromics::virsorter2_candidate_virus",
            "viromics::dereplicated_candidate_virus",
            "viromics::candidate_call_provenance",
            "viromics::votu_cluster_table", "viromics::precluster_table",
            "viromics::contig_length_table",
        ],
    ),
    "function": dict(
        title="Functional annotation",
        transforms=[
            "prodigal", "chunkOrfsForAnnotation#0",
            "clean", "proteinbert", "merge_proteinbert",
            "downloadUniRef50DB", "diamond_uniref50", "merge_diamond_uniref50",
            "downloadKofamDB", "kofamscan#0", "merge_kofamscan#0",
            "downloadDramDB", "dram_annotate_genes", "merge_dram_annotate_genes",
            "gpr_4lane",
        ],
        types=[
            "sequences::megahit_assembly", "sequences::orfs", "sequences::gff",
            "sequences::orf_chunk",
            "annotation::clean_predictions",
            "annotation::proteinbert_embeddings_chunk",
            "annotation::proteinbert_embeddings",
            "ref::uniref50_diamond_db",
            "annotation::diamond_uniref50_results_chunk",
            "annotation::diamond_uniref50_descriptions_chunk",
            "annotation::diamond_uniref50_results",
            "annotation::diamond_uniref50_descriptions",
            "ref::kofamscan_ko_list", "ref::kofamscan_profiles",
            "annotation::kofamscan_results_chunk",
            "annotation::kofamscan_descriptions_chunk",
            "annotation::kofamscan_results", "annotation::kofamscan_descriptions",
            "annotation::dram_db", "annotation::dram_annotations_chunk",
            "annotation::dram_annotations",
            "ref::mnxr_lookup", "ref::label_transfer_landmarks",
            "annotation::gpr_table",
        ],
    ),
    "viral": dict(
        title="Viral annotation",
        transforms=[
            "vibrant", "virsorter2", "genomad", "downloadVirsorter2DB",
            "ref_genomad", "downloadDramDB", "dramv", "checkv",
            "prodigal_gv", "chunkOrfsForAnnotation#1",
            "downloadKofamDB", "kofamscan#1", "merge_kofamscan#1",
            "aggregator", "skani_dedup", "downloadGtdbDB", "gtdbtk",
            "cctyper", "blast_spacers_to_contigs",
            "iphop_add_to_db", "iphop_predict",
        ],
        types=[
            "sequences::contig_batch", "viromics::dereplicated_candidate_virus",
            "annotation::virsorter2_db", "annotation::virsorter2_affi_contigs",
            "annotation::virsorter2_viral_sequences",
            "annotation::virsorter2_boundary", "annotation::virsorter2_scores",
            "ref::genomad", "annotation::dram_db",
            "annotation::dramv_annotations", "annotation::dramv_distill",
            "viromics::vibrant_amgs", "viromics::vibrant_genome_quality",
            "viromics::vibrant_lifestyle_table",
            "taxonomy::genomad_virus_summary", "taxonomy::genomad_virus_genes",
            "taxonomy::genomad_plasmid_summary",
            "viromics::checkv_complete_genomes", "viromics::checkv_completeness",
            "viromics::checkv_contamination", "viromics::checkv_quality_summary",
            "sequences::orfs", "sequences::orf_chunk",
            "ref::kofamscan_ko_list", "ref::kofamscan_profiles",
            "annotation::kofamscan_results_chunk",
            "annotation::kofamscan_descriptions_chunk",
            "annotation::kofamscan_results", "annotation::kofamscan_descriptions",
            "sequences::megahit_assembly", "sequences::comebin_bin_fasta",
            "sequences::semibin2_bin_fasta", "sequences::metabat2_bin_fasta",
            "binning_local::quality_bin_fasta", "binning_local::cluster_table",
            "ref::gtdb", "taxonomy::gtdbtk",
            "viromics::crispr_arrays", "viromics::crispr_spacers",
            "viromics::cas_operons", "viromics::spacer_host_links",
            "viromics::iphop_augmented_db", "viromics::host_prediction_genome",
            "viromics::host_prediction_genus", "viromics::host_prediction_detail",
        ],
    ),
    "taxonomy": dict(
        title="Taxonomy",
        transforms=[
            "downloadMetabuliDB", "metabuli", "ref_genomad", "genomad",
            "vcontact3", "downloadGtdbDB", "gtdbtk",
        ],
        types=[
            "sequences::megahit_assembly", "ref::metabuli_ref",
            "taxonomy::metabuli", "taxonomy::metabuli_krona",
            "taxonomy::metabuli_report",
            "sequences::contig_batch", "ref::genomad", "taxonomy::genomad_taxonomy",
            "viromics::dereplicated_candidate_virus",
            "viromics::vcontact3_ani", "viromics::vcontact3_assignments",
            "viromics::vcontact3_network",
            "sequences::semibin2_bin_fasta", "ref::gtdb", "taxonomy::gtdbtk",
        ],
    ),
}


def keep_set(all_names: list[str], chunk: dict) -> set[str]:
    """Resolve a chunk's spec against the plan's actual node names."""
    by_stem: dict[str, list[str]] = {}
    for n in all_names:
        m = re.match(r"^(\d+) (.+)$", n)
        if m:
            by_stem.setdefault(m.group(2), []).append(n)
    for v in by_stem.values():
        v.sort(key=lambda s: int(s.split(" ", 1)[0]))

    keep, missing = set(), []
    for want in chunk["transforms"]:
        stem, _, nth = want.partition("#")
        hits = by_stem.get(stem, [])
        if not hits:
            missing.append(want)
            continue
        keep |= {hits[int(nth)]} if nth else set(hits)
    for want in chunk["types"]:
        if want in all_names:
            keep.add(want)
        else:
            missing.append(want)
    assert not missing, f"{chunk['title']}: not in the plan: {missing}"
    return keep


def main() -> int:
    task = V.build_spec().Solve(max_iter=1024, max_refine=256, seed=42)
    plan = task.plan
    if not task.ok or plan.dropped_targets:
        print(f"refusing to draw an incomplete plan: ok={task.ok} "
              f"dropped={sorted(plan.dropped_targets)}")
        return 1
    print(f"plan: {len(plan.steps)} steps")

    all_names = [n.name for n in
                 plan.BuildDAG(colour="module", show_step_order=True).layout().nodes]
    for name, chunk in CHUNKS.items():
        keep = keep_set(all_names, chunk)
        for theme in ("light", "dark"):
            r = plan.BuildDAG(colour="module", theme=theme, background=False,
                              show_step_order=True)
            for n in all_names:
                if n not in keep:
                    r.remove_node(n)
            out = r.render(str(HERE / f"chunk-{name}-{theme}"), "svg")
            dims = re.search(r'width="(\d+)" height="(\d+)"',
                             Path(out).read_text()[:400])
            if theme == "light":
                print(f"  {chunk['title']:<22} {len(keep):>3} nodes  "
                      f"{dims.group(1)}x{dims.group(2)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

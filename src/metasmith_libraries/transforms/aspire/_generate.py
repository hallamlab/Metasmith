#!/usr/bin/env python3
"""The ASPIRE topology, in one table, and the generator that emits it.

`research/aspire/upstream/ASPIRE/asv_pipeline.nf` is 45 hand-wired Nextflow processes. This file
is the port of that wiring into a typed graph: `TABLE` has one row per ported
process, and running this module writes both `data_types/aspire.yml` and every
stub transform under `transforms/aspire/`.

Why a generator rather than fifty hand-written files: this pass exists to *look*
at the topology and decide what to merge, so collapsing two nodes has to be a
table edit, not a sweep. Once the stub bodies start becoming real protocols this
module stops being authoritative -- regenerating would clobber them. Delete it
then, or keep it as the record of where each transform came from.

Three things the port does to the source pipeline, each recorded per row:

* **`.done` sentinels are not ported.** A sentinel stood for an artifact the
  next process re-opened by absolute path out of a shared staging directory. The
  artifact is what gets declared -- a named file where one was read, a directory
  where one was scanned.
* **Optional stages that have downstream consumers are gated on a policy
  token.** Nextflow expressed them by *rebinding* the channel its consumers
  read; there is no rebinding here, so instead two transforms produce the same
  consumer-facing type and each requires a different sibling token. The driver
  registers exactly one, so the losing arm has no candidates and is never
  instantiated. Purely terminal optional stages need no token: you get them by
  asking for their output.
* **Plot-only processes are folded into the transform that computed their
  tables** (see `folds=` on the rows).

Run it:

    python transforms/aspire/_generate.py            # write types + stubs
    python transforms/aspire/_generate.py --lint      # check extends: subsumption
"""

from __future__ import annotations

import argparse
import sys
import textwrap
from pathlib import Path

HERE = Path(__file__).resolve().parent
MLIB = HERE.parent.parent
TYPES_YML = MLIB / "data_types" / "aspire.yml"

FILE, DIR, VALUE = "file", "dir", "value"


def t(kind, desc, **props):
    return kind, desc, props


TYPE_SECTIONS: list[tuple[str, dict]] = [
    ("the run keystone: what every stage descends from", {
        "run": t(VALUE, "one ASPIRE study; the grouping parent every stage descends from", ext="txt"),
        "sample_id": t(VALUE, "label for one sequenced sample within an ASPIRE run", ext="txt"),
    }),
    ("study-level inputs the .nf read out of its config", {
        "sample_metadata": t(FILE, "study metadata table, one row per sample", ext="tsv"),
        "sina_arb_reference": t(FILE, "SILVA ARB reference SINA aligns against", ext="arb"),
        "silva_ref_taxonomy": t(FILE, "SILVA reference taxonomy map for taxonomy assignment", ext="tsv"),
        "mito_reference_source": t(FILE, "mitochondrial reference sequences for the mito BLAST database", ext="fasta"),
        "contaminant_reference_source": t(FILE, "contaminant/biofilm reference sequences for the contaminant BLAST database", ext="fasta"),
    }),
    ("per-sample read processing", {
        "qc_reads_fwd": t(FILE, "fastp-trimmed forward reads for one sample", ext="fastq.gz"),
        "qc_reads_rev": t(FILE, "fastp-trimmed reverse reads for one sample", ext="fastq.gz"),
        "fastp_report_json": t(FILE, "fastp QC report for one sample", ext="json"),
        "fastp_report_html": t(FILE, "fastp QC report rendering for one sample", ext="html"),
        "merged_reads": t(FILE, "vsearch-merged read pair for one sample", ext="fastq.gz"),
        "filtered_fasta": t(FILE, "quality-filtered merged reads for one sample", ext="fasta.gz"),
    }),
    ("the run-level ASV spine", {
        "concat_fasta": t(FILE, "all samples' filtered reads concatenated, for dereplication", ext="fasta.gz"),
        "concat_counts_fasta": t(FILE, "all samples' filtered reads concatenated with sample-relabelled headers, for counting", ext="fasta.gz"),
        "derep_fasta": t(FILE, "dereplicated full-length sequences", ext="fasta.gz"),
        "centroids_fasta": t(FILE, "UNOISE denoised centroids", ext="fasta.gz"),
        "nochimera_fasta": t(FILE, "chimera-filtered centroids", ext="fasta.gz"),
        "asv_filtered_counts": t(FILE, "ASV count matrix after prevalence and depth filtering", ext="tsv"),
        "asv_filtered_seqs": t(FILE, "ASV representative sequences surviving table filtering", ext="fasta.gz"),
    }),
    ("SINA alignment and taxonomy", {
        "sina_trimmed_seqs": t(FILE, "SINA-aligned ASV sequences trimmed to the target variable regions", ext="fasta.gz"),
        "sina_aligned_seqs": t(FILE, "raw SINA alignment of the ASV sequences", ext="fasta.gz"),
        "sina_log": t(FILE, "SINA alignment log", ext="log"),
        "sina_v_regions": t(FILE, "per-sequence variable region assignments parsed from the SINA log", ext="tsv"),
        "taxonomy_uppercase_seqs": t(FILE, "ASV sequences uppercased for taxonomy assignment", ext="fasta.gz"),
        "taxonomy_stats": t(FILE, "taxonomy assignment summary statistics", ext="tsv"),
    }),
    ("mitochondrial screen and decontamination", {
        "mito_blast_db": t(DIR, "BLAST database built from the mitochondrial reference"),
        "contaminant_blast_db": t(DIR, "BLAST database built from the contaminant reference"),
        "mitomaster_table": t(FILE, "MitoMaster classification of each ASV", ext="tsv"),
        "mito_blast6": t(FILE, "ASV hits against the mitochondrial database", ext="tsv"),
        "contaminant_blast6": t(FILE, "ASV hits against the contaminant database", ext="tsv"),
        "nontarget_table": t(FILE, "master table of ASVs judged non-target (mitochondrial or contaminant)", ext="tsv"),
        "mito_summary_tables": t(DIR, "per-category summaries of the non-target call"),
        "mito_plots": t(DIR, "renderings of the non-target call"),
    }),
    ("count partitioning", {
        "counts_filtered": t(FILE, "ASV counts after non-target removal", ext="tsv"),
        "counts_micro": t(FILE, "ASV counts restricted to microbial ASVs", ext="tsv"),
        "counts_mito": t(FILE, "ASV counts restricted to mitochondrial ASVs", ext="tsv"),
        "counts_decon": t(FILE, "ASV counts with contaminant ASVs removed", ext="tsv"),
    }),
    ("read-accounting statistics", {
        "fastq_stats": t(FILE, "per-sample raw read counts", ext="tsv"),
        "fastp_stats": t(FILE, "per-sample post-fastp read counts", ext="tsv"),
        "filtered_stats": t(FILE, "per-sample post-filter read counts", ext="tsv"),
        "concat_stats": t(FILE, "per-sample read counts entering the concatenated pool", ext="tsv"),
        "sankey_outputs": t(DIR, "read-fate sankey renderings"),
    }),
    ("metadata joins", {
        "metadata_micro": t(FILE, "study metadata joined to microbial read accounting", ext="tsv"),
        "asv_meta_micro": t(FILE, "long-form ASV table joined to metadata and taxonomy, microbial", ext="tsv"),
        "asv_final_micro": t(FILE, "analysis-ready microbial ASV count matrix", ext="tsv"),
        "metadata_mito": t(FILE, "study metadata joined to mitochondrial read accounting", ext="tsv"),
        "asv_meta_mito": t(FILE, "long-form ASV table joined to metadata and taxonomy, mitochondrial", ext="tsv"),
        "asv_final_mito": t(FILE, "analysis-ready mitochondrial ASV count matrix", ext="tsv"),
    }),
    ("grouping diagnostics and soft labels", {
        "grouping_diagnostics_outputs": t(DIR, "group separability diagnostics and their renderings"),
        "soft_assignments": t(FILE, "soft group-label assignments for samples missing a hard label", ext="tsv"),
        "soft_validation": t(FILE, "per-sample validation of the soft label assignment", ext="tsv"),
        "soft_validation_summary": t(FILE, "summary of soft label assignment quality", ext="tsv"),
        "augmentation_audit": t(FILE, "record of which metadata rows the soft labels changed", ext="tsv"),
    }),
    ("the three consumer-facing analysis channels", {
        "analysis_metadata": t(FILE, "the metadata table every downstream analysis reads, augmented or not", ext="tsv"),
        "stage_asv_meta": t(FILE, "the ASV metadata table after label augmentation and before batch correction", ext="tsv"),
        "analysis_asv_meta": t(FILE, "the ASV metadata table every downstream analysis reads, corrected or not", ext="tsv"),
        "analysis_counts": t(FILE, "the ASV count matrix every downstream analysis reads, corrected or not", ext="tsv"),
    }),
    ("batch correction diagnostics", {
        "asv_clr_selected": t(FILE, "CLR-transformed counts for the selected feature set", ext="tsv"),
        "asv_clr_after": t(FILE, "CLR-transformed counts after ConQuR correction", ext="tsv"),
        "asv_corrected_counts": t(FILE, "ConQuR-corrected relative abundances", ext="tsv"),
        "asv_corrected_counts_int": t(FILE, "ConQuR-corrected counts with a pseudocount, integer-valued", ext="tsv"),
        "asv_selected_counts": t(FILE, "relative abundances for the selected feature set", ext="tsv"),
        "correction_decision": t(FILE, "per-feature record of whether correction was applied", ext="tsv"),
        "correction_countspace_plot": t(FILE, "count-space preservation check for the correction", ext="png"),
        "correction_countspace_metrics": t(FILE, "count-space preservation metrics for the correction", ext="tsv"),
        "correction_umap_plot": t(FILE, "UMAP before and after correction", ext="png"),
        "correction_stats": t(FILE, "batch correction summary statistics", ext="tsv"),
        "umap_hdbscan_results": t(FILE, "UMAP embedding with HDBSCAN cluster assignments", ext="tsv"),
    }),
    ("terminal analyses", {
        "upset_plots": t(DIR, "UpSet renderings of metadata group overlap"),
        "bubble_plots": t(DIR, "taxonomic bubble plot renderings"),
        "umap_plots": t(DIR, "UMAP clustering renderings"),
        "outlier_outputs": t(DIR, "sample outlier diagnostics"),
        "collectors_outputs": t(DIR, "collector's curve renderings"),
        "diversity_outputs": t(DIR, "alpha and beta diversity results and renderings"),
        "voc_correlation_outputs": t(DIR, "ASV to volatile-compound correlation results"),
        "measurement_association_outputs": t(DIR, "ASV to continuous-measurement association results"),
        "power_analysis_outputs": t(DIR, "group contrast statistical power analysis"),
        "taxonomy_group_association_outputs": t(DIR, "subject-aware taxonomy to group association results"),
        "paired_group_contrast_outputs": t(DIR, "paired within-subject group contrast results"),
        "clustermap_outputs": t(DIR, "abundance clustermap renderings"),
    }),
    ("indicator species analysis", {
        "indicspecies_group1_summary": t(FILE, "indicator species summary for the primary grouping", ext="tsv"),
        "indicspecies_group2_summary": t(FILE, "indicator species summary for the secondary grouping", ext="tsv"),
        "indicspecies_group1_results": t(FILE, "full indicator species results for the primary grouping", ext="tsv"),
        "indicspecies_group2_results": t(FILE, "full indicator species results for the secondary grouping", ext="tsv"),
        "indicspecies_tables": t(DIR, "every per-contrast indicator species table"),
        "indicspecies_plots": t(DIR, "indicator species renderings"),
        "indicspecies_aligned_plots": t(DIR, "indicator species renderings aligned across contrasts"),
    }),
    ("co-occurrence network", {
        "external_graph_all": t(FILE, "pre-computed unthresholded co-occurrence graph, supplied instead of running SpiecEasi", ext="graphml"),
        "external_graph_thr": t(FILE, "pre-computed thresholded co-occurrence graph, supplied instead of running SpiecEasi", ext="graphml"),
        "external_node_features": t(FILE, "pre-computed network node features, supplied instead of running SpiecEasi", ext="csv"),
        "network_graph_all": t(FILE, "unthresholded positive co-occurrence graph", ext="graphml"),
        "network_graph_thr": t(FILE, "thresholded positive co-occurrence graph", ext="graphml"),
        "network_node_features": t(FILE, "per-node features of the co-occurrence graph", ext="csv"),
        "network_modules_sub": t(FILE, "module assignment over the thresholded graph", ext="tsv"),
        "network_modules_all": t(FILE, "module assignment over the unthresholded graph", ext="tsv"),
        "network_modules_summary": t(FILE, "per-module summary statistics", ext="tsv"),
        "network_modules_runs": t(FILE, "per-run record of the module detection sweep", ext="tsv"),
        "network_outputs": t(DIR, "co-occurrence network renderings and overlays"),
    }),
    ("ASV to MAG linking", {
        "asv_mag_pairing": t(FILE, "ASV to MAG pairing table", ext="tsv"),
        "asv_mag_outputs": t(DIR, "ASV to MAG linking results"),
        "asv_mag_network_outputs": t(DIR, "MAG-annotated co-occurrence network renderings"),
        "module_asv_anchor_table": t(FILE, "per-ASV module anchor assignments", ext="tsv"),
        "module_mag_anchor_summary": t(FILE, "per-module MAG anchor summary", ext="tsv"),
        "sample_module_scores": t(FILE, "per-sample module activity scores", ext="tsv"),
        "sample_top_modules": t(FILE, "the highest scoring module per sample", ext="tsv"),
        "sample_module_matrix": t(FILE, "sample by module score matrix", ext="tsv"),
        "sample_module_heatmaps": t(DIR, "sample by module score heatmap renderings"),
    }),
    ("master summary", {
        "optional_outputs": t(DIR, "the master summary's optional-contributor slot; the .nf always passes it empty"),
        "master_long": t(FILE, "long-form master ASV table", ext="tsv"),
        "master_count_wide": t(FILE, "wide-form master count table", ext="tsv"),
        "master_source_manifest": t(FILE, "which stage contributed each master table column", ext="tsv"),
        "master_column_mapping": t(FILE, "original to canonical column name mapping", ext="tsv"),
        "master_column_collisions": t(FILE, "columns that collided during the master merge", ext="tsv"),
    }),
]


POLICIES: list[tuple[str, str]] = [
    ("augmentation", "apply soft group labels to the metadata before analysis"),
    ("batch_correction", "run ConQuR batch correction over the ASV counts"),
    ("indicspecies", "run indicator species analysis"),
    ("spieceasi", "infer the co-occurrence network with SpiecEasi rather than supplying one"),
    ("network_modules", "detect modules in the co-occurrence network"),
    ("asv_mag_link", "link ASVs to MAGs"),
    ("graph_network", "render and overlay the co-occurrence network"),
    ("sankey", "render the read-fate sankey"),
]


class T:
    def __init__(self, name, source, line, requires, products, group_by,
                 folds=(), note=None, cpus=1, memory_gb=4, hours=1):
        self.name = name
        self.source = source
        self.line = line
        self.requires = requires
        self.products = products
        self.group_by = group_by
        self.folds = folds
        self.note = note
        self.cpus, self.memory_gb, self.hours = cpus, memory_gb, hours


RUN = ("run", "aspire::run", ())


def _r(var, dtype, *parents):
    return (var, dtype, tuple(parents))


TABLE: list[T] = [
    T("fastp_qc", "FASTP_QC", 3121,
      [RUN,
       _r("sid", "aspire::sample_id", "run"),
       _r("pair", "sequences::read_pair", "sid"),
       _r("r1", "sequences::zipped_forward_short_reads", "pair"),
       _r("r2", "sequences::zipped_reverse_short_reads", "pair")],
      [("fwd", "aspire::qc_reads_fwd"), ("rev", "aspire::qc_reads_rev"),
       ("rjson", "aspire::fastp_report_json"), ("rhtml", "aspire::fastp_report_html")],
      "sid", cpus=4, hours=2),

    T("merge_reads", "MERGE_READS", 3179,
      [RUN, _r("sid", "aspire::sample_id", "run"),
       _r("fwd", "aspire::qc_reads_fwd", "sid"), _r("rev", "aspire::qc_reads_rev", "sid")],
      [("merged", "aspire::merged_reads")], "sid", cpus=4, hours=2),

    T("filter_reads", "FILTER_READS", 3221,
      [RUN, _r("sid", "aspire::sample_id", "run"),
       _r("merged", "aspire::merged_reads", "sid")],
      [("filtered", "aspire::filtered_fasta")], "sid", cpus=2),

    T("concat_fastas", "CONCAT_FASTAS", 3274,
      [RUN, _r("sid", "aspire::sample_id", "run"),
       _r("filtered", "aspire::filtered_fasta", "sid")],
      [("concat", "aspire::concat_fasta"), ("counts_fa", "aspire::concat_counts_fasta")],
      "run", folds=("RELABEL_FILTERED@3248",),
      note="the study fan-in: group_by=run collapses every sample's filtered "
           "reads into one task. RELABEL_FILTERED is folded in -- it is a "
           "header rewrite over a file this transform already reads, and "
           "keeping it separate and optional would add a third rebinding "
           "switch for a sed. The real protocol names each sequence from the "
           "sample_id its fasta descends from (context.SourceOf), never from "
           "the position of the file in the group.",
      cpus=2, hours=2),

    T("dereplicate", "DEREPLICATE", 3298,
      [RUN, _r("concat", "aspire::concat_fasta", "run")],
      [("derep", "aspire::derep_fasta")], "run", cpus=8, memory_gb=16, hours=4),

    T("denoise", "DENOISE", 3367,
      [RUN, _r("derep", "aspire::derep_fasta", "run")],
      [("centroids", "aspire::centroids_fasta")], "run", cpus=8, memory_gb=16, hours=4),

    T("chimera_check", "CHIMERA_CHECK", 3393,
      [RUN, _r("centroids", "aspire::centroids_fasta", "run")],
      [("nochi", "aspire::nochimera_fasta")], "run", cpus=8, memory_gb=16, hours=4),

    T("create_count_matrix", "CREATE_COUNT_MATRIX", 3417,
      [RUN, _r("counts_fa", "aspire::concat_counts_fasta", "run"),
       _r("nochi", "aspire::nochimera_fasta", "run")],
      [("counts", "amplicon::asv_table"), ("seqs", "amplicon::asv_seqs")],
      "run",
      note="the two generic ASV types this pipeline shares with the rest of "
           "the library: a sample-by-ASV count matrix and the representative "
           "sequences behind it.",
      cpus=8, memory_gb=16, hours=4),

    T("filter_table", "FILTER_TABLE", 3444,
      [RUN, _r("counts", "amplicon::asv_table", "run"),
       _r("seqs", "amplicon::asv_seqs", "run")],
      [("fcounts", "aspire::asv_filtered_counts"), ("fseqs", "aspire::asv_filtered_seqs")],
      "run"),

    T("sina_trim", "SINA_TRIM", 3322,
      [RUN, _r("fseqs", "aspire::asv_filtered_seqs", "run"),
       _r("ref", "aspire::sina_arb_reference")],
      [("trimmed", "aspire::sina_trimmed_seqs"), ("aligned", "aspire::sina_aligned_seqs"),
       ("log", "aspire::sina_log"), ("vreg", "aspire::sina_v_regions")],
      "run",
      note="the .nf calls its input `derep_fasta`, but the workflow body wires "
           "FILTER_TABLE's filtered ASV sequences in (asv_pipeline.nf:3631). "
           "The parameter name is legacy; the wiring is what is ported.",
      cpus=16, memory_gb=32, hours=8),

    T("taxonomy", "TAXONOMY", 3470,
      [RUN, _r("trimmed", "aspire::sina_trimmed_seqs", "run"),
       _r("refseqs", "amplicon::silva_db"), _r("reftax", "aspire::silva_ref_taxonomy")],
      [("tax", "amplicon::asv_taxonomy"), ("upper", "aspire::taxonomy_uppercase_seqs"),
       ("stats", "aspire::taxonomy_stats")],
      "run",
      note="the SILVA reference used to be fetched during Groovy config "
           "parsing, before any process ran. Here it is an ordinary input, so "
           "the planner either takes the one the driver supplies or reaches "
           "for transforms/logistics/downloadSilvaDB.",
      cpus=8, memory_gb=16, hours=6),

    T("prepare_blast_databases", "PREPARE_BLAST_DATABASES", 3511,
      [RUN, _r("mito_src", "aspire::mito_reference_source"),
       _r("cont_src", "aspire::contaminant_reference_source")],
      [("mito_db", "aspire::mito_blast_db"), ("cont_db", "aspire::contaminant_blast_db")],
      "run", hours=2),

    T("mitomaster", "MITOMASTER", 3544,
      [RUN, _r("fcounts", "aspire::asv_filtered_counts", "run"),
       _r("fseqs", "aspire::asv_filtered_seqs", "run"),
       _r("mito_db", "aspire::mito_blast_db", "run"),
       _r("cont_db", "aspire::contaminant_blast_db", "run")],
      [("master", "aspire::mitomaster_table"), ("mhits", "aspire::mito_blast6"),
       ("chits", "aspire::contaminant_blast6")],
      "run", cpus=8, memory_gb=16, hours=6),

    T("mito_decontam", "MITO_DECONTAM", 3595,
      [RUN, _r("master", "aspire::mitomaster_table", "run"),
       _r("mhits", "aspire::mito_blast6", "run"),
       _r("chits", "aspire::contaminant_blast6", "run"),
       _r("tax", "amplicon::asv_taxonomy", "run")],
      [("nontarget", "aspire::nontarget_table"),
       ("summaries", "aspire::mito_summary_tables"), ("plots", "aspire::mito_plots")],
      "run",
      note="the .nf emits the summaries and plots as optional globs; declared "
           "unconditionally here, since a Metasmith product is not optional."),

    T("filter_counts", "FILTER_COUNTS", 3637,
      [RUN, _r("fcounts", "aspire::asv_filtered_counts", "run"),
       _r("fseqs", "aspire::asv_filtered_seqs", "run"),
       _r("tax", "amplicon::asv_taxonomy", "run"),
       _r("nontarget", "aspire::nontarget_table", "run")],
      [("filtered", "aspire::counts_filtered"), ("micro", "aspire::counts_micro"),
       ("mito", "aspire::counts_mito"), ("decon", "aspire::counts_decon")],
      "run"),

    T("general_stats", "GENERAL_STATS", 3755,
      [RUN, _r("counts_fa", "aspire::concat_counts_fasta", "run")],
      [("fastq", "aspire::fastq_stats"), ("fastp", "aspire::fastp_stats"),
       ("filtered", "aspire::filtered_stats"), ("concat", "aspire::concat_stats")],
      "run",
      note="the .nf reaches into the publish directories for the per-stage "
           "read counts and takes the concatenated fasta only as an ordering "
           "barrier. Ported as declared: the barrier is the one real edge."),

    T("sankey", "SANKEY", 3687,
      [RUN, _r("policy", "aspire::sankey_on", "run"),
       _r("fastq", "aspire::fastq_stats", "run"),
       _r("filtered", "aspire::filtered_stats", "run"),
       _r("raw", "amplicon::asv_table", "run"),
       _r("decon", "aspire::counts_decon", "run"),
       _r("micro", "aspire::counts_micro", "run")],
      [("out", "aspire::sankey_outputs")], "run",
      note="`sankey.done` is dropped; the renderings are the output, and "
           "MASTER_SUMMARY reads the directory rather than the sentinel."),

    T("sankey_absent", None, None,
      [RUN, _r("policy", "aspire::sankey_off", "run")],
      [("out", "aspire::sankey_outputs")], "run",
      note="the `sankey.enabled = false` arm. In the .nf, MASTER_SUMMARY's "
           "sankey slot is filled with a zero-row placeholder file at "
           "asv_pipeline.nf:2790; here the placeholder gets a producer, so the "
           "consumer's requirement stays unconditional either way."),

    T("plot_metadata", "PLOT_METADATA", 3793,
      [RUN, _r("fastq", "aspire::fastq_stats", "run"),
       _r("micro", "aspire::counts_micro", "run"),
       _r("mito", "aspire::counts_mito", "run"),
       _r("tax", "amplicon::asv_taxonomy", "run"),
       _r("meta", "aspire::sample_metadata")],
      [("md_micro", "aspire::metadata_micro"), ("am_micro", "aspire::asv_meta_micro"),
       ("af_micro", "aspire::asv_final_micro"), ("md_mito", "aspire::metadata_mito"),
       ("am_mito", "aspire::asv_meta_mito"), ("af_mito", "aspire::asv_final_mito")],
      "run", cpus=4, memory_gb=16, hours=3,
      note="the join that turns counts into an analysis table. Everything "
           "downstream of here reads one of the three consumer-facing channels, "
           "not these directly."),

    T("grouping_diagnostics", "GROUPING_DIAGNOSTICS", 4929,
      [RUN, _r("md", "aspire::metadata_micro", "run"),
       _r("counts", "aspire::asv_final_micro", "run")],
      [("out", "aspire::grouping_diagnostics_outputs"),
       ("assign", "aspire::soft_assignments"), ("valid", "aspire::soft_validation"),
       ("vsum", "aspire::soft_validation_summary")],
      "run", cpus=4, memory_gb=16, hours=4,
      note="reads the pre-augmentation metadata deliberately -- it is what "
           "produces the soft labels augmentation applies. The .nf rebinds its "
           "counts input to the corrected matrix at asv_pipeline.nf:2915, after "
           "the call site, so that rebinding is dead and is not ported."),

    T("group_label_augmentation", "GROUP_LABEL_AUGMENTATION", 4986,
      [RUN, _r("policy", "aspire::augmentation_on", "run"),
       _r("md", "aspire::metadata_micro", "run"),
       _r("am", "aspire::asv_meta_micro", "run"),
       _r("assign", "aspire::soft_assignments", "run"),
       _r("vsum", "aspire::soft_validation_summary", "run")],
      [("out_md", "aspire::analysis_metadata"), ("out_am", "aspire::stage_asv_meta"),
       ("audit", "aspire::augmentation_audit")],
      "run",
      note="the `on` arm of the augmentation switch. In the .nf this stage "
           "reassigns the eleven `metaMicroFor*` and `asvMetaFor*` variables "
           "(asv_pipeline.nf:2872-2892); here it produces the shared "
           "consumer-facing types instead, and the token decides which "
           "producer exists."),

    T("augmentation_passthrough", None, None,
      [RUN, _r("policy", "aspire::augmentation_off", "run"),
       _r("md", "aspire::metadata_micro", "run"),
       _r("am", "aspire::asv_meta_micro", "run")],
      [("out_md", "aspire::analysis_metadata"), ("out_am", "aspire::stage_asv_meta")],
      "run",
      note="the `off` arm: the metadata tables reach the analyses unchanged."),

    T("asv_batch_correction", "ASV_BATCH_CORRECTION", 4101,
      [RUN, _r("policy", "aspire::batch_correction_on", "run"),
       _r("md", "aspire::analysis_metadata", "run"),
       _r("am", "aspire::stage_asv_meta", "run"),
       _r("counts", "aspire::asv_final_micro", "run")],
      [("out_counts", "aspire::analysis_counts"), ("out_am", "aspire::analysis_asv_meta"),
       ("clr_sel", "aspire::asv_clr_selected"), ("clr_after", "aspire::asv_clr_after"),
       ("corr", "aspire::asv_corrected_counts"), ("corr_int", "aspire::asv_corrected_counts_int"),
       ("sel", "aspire::asv_selected_counts"), ("decision", "aspire::correction_decision"),
       ("cs_plot", "aspire::correction_countspace_plot"),
       ("cs_metrics", "aspire::correction_countspace_metrics"),
       ("umap_plot", "aspire::correction_umap_plot"),
       ("stats", "aspire::correction_stats"), ("umap_res", "aspire::umap_hdbscan_results")],
      "run", folds=("ASV_META_FROM_CORRECTED@4246",),
      cpus=8, memory_gb=32, hours=8,
      note="the `on` arm of the batch correction switch. ASV_META_FROM_CORRECTED "
           "is folded in: it is a second table derived from the same corrected "
           "counts, gated on the same flag, and separating it would need a "
           "third arm for the case where correction ran but nothing downstream "
           "wanted the corrected ASV metadata. It requires the pre-correction "
           "counts type, never the shared consumer-facing one -- requiring the "
           "shared type would let the solver chain this transform into itself."),

    T("batch_correction_passthrough", None, None,
      [RUN, _r("policy", "aspire::batch_correction_off", "run"),
       _r("am", "aspire::stage_asv_meta", "run"),
       _r("counts", "aspire::asv_final_micro", "run")],
      [("out_counts", "aspire::analysis_counts"), ("out_am", "aspire::analysis_asv_meta")],
      "run",
      note="the `off` arm: the uncorrected counts and ASV metadata reach the "
           "analyses unchanged."),

    T("plot_upset", "PLOT_UPSET", 3927,
      [RUN, _r("md", "aspire::analysis_metadata", "run")],
      [("out", "aspire::upset_plots")], "run"),

    T("bubbleplotter", "BUBBLEPLOTTER", 4018,
      [RUN, _r("am", "aspire::analysis_asv_meta", "run")],
      [("out", "aspire::bubble_plots")], "run", cpus=2, memory_gb=8),

    T("umap_clustering", "UMAP_CLUSTERING", 4055,
      [RUN, _r("am", "aspire::analysis_asv_meta", "run")],
      [("out", "aspire::umap_plots")], "run", cpus=2, memory_gb=8),

    T("outlier_checker", "OUTLIER_CHECKER", 4316,
      [RUN, _r("clr", "aspire::asv_clr_selected", "run"),
       _r("md", "aspire::analysis_metadata", "run")],
      [("out", "aspire::outlier_outputs")], "run",
      note="reads the CLR matrix, which only the correction arm produces -- so "
           "this stage needs batch correction on, exactly as in the .nf, "
           "without a token of its own."),

    T("collectors_curve", "COLLECTORS_CURVE", 4364,
      [RUN, _r("counts", "aspire::analysis_counts", "run"),
       _r("md", "aspire::analysis_metadata", "run")],
      [("out", "aspire::collectors_outputs")], "run", cpus=2, memory_gb=8, hours=2),

    T("diversity_analysis", "DIVERSITY_ANALYSIS", 4399,
      [RUN, _r("md", "aspire::analysis_metadata", "run"),
       _r("counts", "aspire::analysis_counts", "run")],
      [("out", "aspire::diversity_outputs")], "run", cpus=4, memory_gb=16, hours=3),

    T("indicspecies", "INDICSPECIES", 4518,
      [RUN, _r("policy", "aspire::indicspecies_on", "run"),
       _r("md", "aspire::analysis_metadata", "run"),
       _r("counts", "aspire::analysis_counts", "run")],
      [("g1sum", "aspire::indicspecies_group1_summary"),
       ("g2sum", "aspire::indicspecies_group2_summary"),
       ("g1res", "aspire::indicspecies_group1_results"),
       ("g2res", "aspire::indicspecies_group2_results"),
       ("tables", "aspire::indicspecies_tables"),
       ("plots", "aspire::indicspecies_plots"),
       ("aligned", "aspire::indicspecies_aligned_plots")],
      "run", folds=("INDICSPECIES_PLOTS@4608", "INDICSPECIES_ALIGNED_PLOTS@4792"),
      cpus=8, memory_gb=32, hours=12),

    T("indicspecies_absent", None, None,
      [RUN, _r("policy", "aspire::indicspecies_off", "run")],
      [("tables", "aspire::indicspecies_tables"),
       ("g1sum", "aspire::indicspecies_group1_summary")],
      "run",
      note="the `off` arm. The .nf substitutes a zero-row placeholder at five "
           "call sites (asv_pipeline.nf:2660, 2661, 2983, 2984, 3036); one "
           "producer covers all of them."),

    T("voc_correlation", "VOC_CORRELATION", 4827,
      [RUN, _r("am", "aspire::analysis_asv_meta", "run"),
       _r("counts", "aspire::analysis_counts", "run"),
       _r("tables", "aspire::indicspecies_tables", "run")],
      [("out", "aspire::voc_correlation_outputs")], "run", cpus=2, memory_gb=8),

    T("measurement_association", "MEASUREMENT_ASSOCIATION", 4875,
      [RUN, _r("am", "aspire::analysis_asv_meta", "run"),
       _r("md", "aspire::analysis_metadata", "run"),
       _r("counts", "aspire::analysis_counts", "run")],
      [("out", "aspire::measurement_association_outputs")], "run", cpus=2, memory_gb=8),

    T("group_power_analysis", "GROUP_POWER_ANALYSIS", 5030,
      [RUN, _r("am", "aspire::analysis_asv_meta", "run"),
       _r("counts", "aspire::analysis_counts", "run"),
       _r("tables", "aspire::indicspecies_tables", "run")],
      [("out", "aspire::power_analysis_outputs")], "run",
      cpus=8, memory_gb=32, hours=12,
      note="one Nextflow task hiding a bash driver, three Python drivers, six "
           "analysis scripts and an R script. One stub here is honest for a "
           "topology pass and is the clearest candidate for expansion later."),

    T("taxonomy_group_association", "TAXONOMY_GROUP_ASSOCIATION", 5113,
      [RUN, _r("am", "aspire::analysis_asv_meta", "run"),
       _r("counts", "aspire::analysis_counts", "run")],
      [("out", "aspire::taxonomy_group_association_outputs")], "run",
      cpus=4, memory_gb=16, hours=4),

    T("paired_group_contrast", "PAIRED_GROUP_CONTRAST", 5234,
      [RUN, _r("am", "aspire::analysis_asv_meta", "run"),
       _r("counts", "aspire::analysis_counts", "run")],
      [("out", "aspire::paired_group_contrast_outputs")], "run",
      cpus=4, memory_gb=16, hours=4),

    T("clustermaps", "CLUSTERMAPS", 5324,
      [RUN, _r("am", "aspire::analysis_asv_meta", "run"),
       _r("md", "aspire::analysis_metadata", "run"),
       _r("tables", "aspire::indicspecies_tables", "run")],
      [("out", "aspire::clustermap_outputs")], "run", cpus=4, memory_gb=16, hours=3,
      note="the .nf passes a boolean saying whether indicator species ran and "
           "then globs the tables out of a shared directory. The directory is "
           "the real edge, so it is what is declared."),

    T("spieceasi", "SPIECEASI", 5452,
      [RUN, _r("policy", "aspire::spieceasi_on", "run"),
       _r("counts", "aspire::analysis_counts", "run"),
       _r("keep", "aspire::indicspecies_group1_summary", "run")],
      [("all", "aspire::network_graph_all"), ("thr", "aspire::network_graph_thr"),
       ("nf", "aspire::network_node_features")],
      "run", cpus=16, memory_gb=64, hours=24),

    T("spieceasi_external", None, None,
      [RUN, _r("policy", "aspire::spieceasi_off", "run"),
       _r("x_all", "aspire::external_graph_all"),
       _r("x_thr", "aspire::external_graph_thr"),
       _r("x_nf", "aspire::external_node_features")],
      [("all", "aspire::network_graph_all"), ("thr", "aspire::network_graph_thr"),
       ("nf", "aspire::network_node_features")],
      "run",
      note="the `off` arm. Unlike the other off-arms this one is not a null "
           "producer: asv_pipeline.nf:2708-2715 substitutes pre-computed "
           "graph files from disk, so the driver has to supply them."),

    T("network_modules", "NETWORK_MODULES", 5522,
      [RUN, _r("policy", "aspire::network_modules_on", "run"),
       _r("all", "aspire::network_graph_all", "run"),
       _r("thr", "aspire::network_graph_thr", "run")],
      [("sub", "aspire::network_modules_sub"), ("mall", "aspire::network_modules_all"),
       ("summary", "aspire::network_modules_summary"),
       ("runs", "aspire::network_modules_runs")],
      "run", cpus=8, memory_gb=32, hours=6),

    T("network_modules_absent", None, None,
      [RUN, _r("policy", "aspire::network_modules_off", "run")],
      [("sub", "aspire::network_modules_sub"), ("mall", "aspire::network_modules_all")],
      "run",
      note="the `off` arm; asv_pipeline.nf:2726-2729 falls back to files on "
           "disk if they exist and a zero-row placeholder if not."),

    T("asv_mag_link", "ASV_MAG_LINK", 5821,
      [RUN, _r("policy", "aspire::asv_mag_link_on", "run"),
       _r("fseqs", "aspire::asv_filtered_seqs", "run")],
      [("pairing", "aspire::asv_mag_pairing"), ("out", "aspire::asv_mag_outputs")],
      "run", cpus=8, memory_gb=32, hours=8,
      note="`asv_mag_link.done` stood for two different things at its four "
           "consumers: the pairing table the network stages read by path, and "
           "the results directory the master summary scans. Both are declared."),

    T("asv_mag_link_absent", None, None,
      [RUN, _r("policy", "aspire::asv_mag_link_off", "run")],
      [("pairing", "aspire::asv_mag_pairing"), ("out", "aspire::asv_mag_outputs")],
      "run"),

    T("graph_network", "GRAPH_NETWORK", 5579,
      [RUN, _r("policy", "aspire::graph_network_on", "run"),
       _r("all", "aspire::network_graph_all", "run"),
       _r("thr", "aspire::network_graph_thr", "run"),
       _r("nf", "aspire::network_node_features", "run"),
       _r("counts", "aspire::analysis_counts", "run"),
       _r("md", "aspire::analysis_metadata", "run"),
       _r("pairing", "aspire::asv_mag_pairing", "run"),
       _r("tax", "amplicon::asv_taxonomy", "run"),
       _r("tables", "aspire::indicspecies_tables", "run"),
       _r("sub", "aspire::network_modules_sub", "run"),
       _r("mall", "aspire::network_modules_all", "run")],
      [("out", "aspire::network_outputs")], "run", cpus=8, memory_gb=32, hours=6),

    T("graph_network_absent", None, None,
      [RUN, _r("policy", "aspire::graph_network_off", "run")],
      [("out", "aspire::network_outputs")], "run"),

    T("asv_mag_network", "ASV_MAG_NETWORK", 5654,
      [RUN, _r("graph", "aspire::network_graph_all", "run"),
       _r("nf", "aspire::network_node_features", "run"),
       _r("tax", "amplicon::asv_taxonomy", "run"),
       _r("counts", "aspire::analysis_counts", "run"),
       _r("pairing", "aspire::asv_mag_pairing", "run")],
      [("out", "aspire::asv_mag_network_outputs")], "run", cpus=4, memory_gb=16, hours=4,
      note="the .nf picks the unthresholded or thresholded graph by config "
           "(`asv_mag_network.graph_variant`). Ported against the "
           "unthresholded one; a variant switch would be a seventh token for "
           "something no consumer can tell apart."),

    T("module_mag_anchors", "MODULE_MAG_ANCHORS", 5705,
      [RUN, _r("mall", "aspire::network_modules_all", "run"),
       _r("nf", "aspire::network_node_features", "run"),
       _r("tax", "amplicon::asv_taxonomy", "run"),
       _r("counts", "aspire::analysis_counts", "run"),
       _r("md", "aspire::analysis_metadata", "run"),
       _r("pairing", "aspire::asv_mag_pairing", "run"),
       _r("net", "aspire::network_outputs", "run")],
      [("anchors", "aspire::module_asv_anchor_table"),
       ("summary", "aspire::module_mag_anchor_summary"),
       ("scores", "aspire::sample_module_scores"),
       ("top", "aspire::sample_top_modules"),
       ("matrix", "aspire::sample_module_matrix"),
       ("heatmaps", "aspire::sample_module_heatmaps")],
      "run", cpus=4, memory_gb=16, hours=4),

    T("master_summary", "MASTER_SUMMARY", 5763,
      [RUN, _r("am", "aspire::analysis_asv_meta", "run"),
       _r("counts", "aspire::analysis_counts", "run"),
       _r("net", "aspire::network_outputs", "run"),
       _r("sankey", "aspire::sankey_outputs", "run"),
       _r("magl", "aspire::asv_mag_outputs", "run"),
       _r("opt", "aspire::optional_outputs", "run")],
      [("long", "aspire::master_long"), ("wide", "aspire::master_count_wide"),
       ("manifest", "aspire::master_source_manifest"),
       ("colmap", "aspire::master_column_mapping"),
       ("collisions", "aspire::master_column_collisions")],
      "run", cpus=4, memory_gb=32, hours=4,
      note="four `.done` barriers plus a --data-dir scan, replaced by four "
           "directory-typed requirements each with an off-arm producer. Which "
           "of the merged tables actually appear is decided at runtime by a "
           "config whitelist and stays a runtime parameter: modelling it in "
           "the type system would need one transform per subset."),

    T("master_summary_optional_slot", None, None,
      [RUN],
      [("opt", "aspire::optional_outputs")], "run",
      note="asv_pipeline.nf:2792 fills the master summary's optional-contributor "
           "slot with an empty placeholder unconditionally -- there is no arm "
           "that fills it with anything. Ported as a permanent stub so the "
           "dead slot is visible in the DAG rather than hidden in a channel "
           "assignment."),
]


BANNER = "# generated by transforms/aspire/_generate.py -- do not hand-edit\n"

TYPE_KIND: dict[str, str] = {
    name: kind
    for _, section in TYPE_SECTIONS
    for name, (kind, _d, _p) in section.items()
}


def write_types() -> int:
    out: list[str] = [
        BANNER,
        "# The ASPIRE amplicon pipeline's type graph.\n",
        "#\n",
        "# `_:` is a description AND a matched property, which is what keeps any two\n",
        "# of these from accidentally standing in for one another.\n",
        "types:\n",
    ]
    n = 0
    for heading, section in TYPE_SECTIONS:
        out.append(f"# -- {heading} --\n")
        for name, (kind, desc, props) in section.items():
            out.append(f"  {name}:\n    properties:\n")
            out.append(f"      _: {desc}\n")
            if kind == DIR:
                out.append("      Data: directory\n")
            for k, v in props.items():
                out.append(f"      {k}: {v}\n")
            n += 1
    out.append("# -- policy tokens --\n")
    out.append("# List-form properties, deliberately: mapping-form `extends:` is a key-wise\n")
    out.append("# merge the child wins, so a child restating any of the parent's keys stops\n")
    out.append("# satisfying it. A list is a set union, so subsumption always holds.\n")
    for base, desc in POLICIES:
        out.append(f"  {base}_policy:\n")
        out.append(f"    properties:\n      - aspire-policy-{base.replace('_', '-')}\n")
        n += 1
        for arm in ("off", "on"):
            out.append(f"  # {'do not ' if arm == 'off' else ''}{desc}\n")
            out.append(f"  {base}_{arm}:\n")
            out.append(f"    extends:\n      - {base}_policy\n")
            out.append(f"    properties:\n      - aspire-policy-{base.replace('_', '-')}-{arm}\n")
            n += 1
    TYPES_YML.write_text("".join(out))
    return n


def _stub(row: T) -> str:
    src = f"{row.source} (asv_pipeline.nf:{row.line})" if row.source else "no .nf process"
    head = [
        BANNER,
        f'"""{row.name} -- {src}\n',
    ]
    if row.folds:
        folded = ", ".join(f.replace("@", " (asv_pipeline.nf:") + ")" for f in row.folds)
        head.append("\n" + textwrap.fill(f"Folded in: {folded}", 78) + "\n")
    if row.note:
        head.append("\n" + textwrap.fill(" ".join(row.note.split()), 78) + "\n")
    head.append(
        "\nStub: the model is the port, the body only touches its outputs.\n"
        'Regenerate with `python transforms/aspire/_generate.py`.\n"""\n\n'
    )
    body = ["from metasmith.python_api import *\n\n"]
    body.append('lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)\n')
    body.append("model = Transform()\n")
    pad = max([len(v) for v, _, _ in row.requires] + [len(v) for v, _ in row.products])
    for var, dtype, parents in row.requires:
        p = ", parents={%s}" % ", ".join(parents) if parents else ""
        body.append(f'{var:<{pad}} = model.AddRequirement(lib.GetType("{dtype}"){p})\n')
    for var, dtype in row.products:
        body.append(f'{var:<{pad}} = model.AddProduct(lib.GetType("{dtype}"))\n')

    body.append("\ndef protocol(context: ExecutionContext):\n")
    body.append("    made = {\n")
    for var, dtype in row.products:
        body.append(f"        {var}: context.Output({var}),\n")
    body.append("    }\n")
    body.append("    for key, path in made.items():\n")
    body.append("        make = 'mkdir -p' if key in _DIRECTORY_PRODUCTS else 'touch'\n")
    body.append("        context.external_shell.Exec(f'{make} {path.external}')\n")
    body.append("    return ExecutionResult(\n")
    body.append("        manifest=[{k: v.local for k, v in made.items()}],\n")
    body.append("        success=all(v.local.exists() for v in made.values()),\n")
    body.append("    )\n\n")

    dirs = [var for var, dtype in row.products
            if dtype.startswith("aspire::") and TYPE_KIND.get(dtype.split("::")[1]) == DIR]
    body.append("_DIRECTORY_PRODUCTS = %s\n\n" % ("{%s}" % ", ".join(dirs) if dirs else "set()"))

    body.append("TransformInstance(\n")
    body.append("    protocol=protocol,\n")
    body.append("    model=model,\n")
    body.append(f"    group_by={row.group_by},\n")
    body.append("    resources=Resources(\n")
    body.append(f"        cpus={row.cpus},\n")
    body.append(f"        memory=Size.GB({row.memory_gb}),\n")
    body.append(f"        duration=Duration(hours={row.hours}),\n")
    body.append("    ),\n")
    body.append(")\n")
    return "".join(head + body)


def write_stubs() -> int:
    existing = {p.name for p in HERE.glob("*.py") if not p.name.startswith("_")}
    wanted = {f"{row.name}.py" for row in TABLE}
    for stale in sorted(existing - wanted):
        (HERE / stale).unlink()
        print(f"  removed stale stub: {stale}")
    for row in TABLE:
        (HERE / f"{row.name}.py").write_text(_stub(row))
    return len(TABLE)


def check_table() -> None:
    declared = set(TYPE_KIND) | {
        f"{base}_{suffix}" for base, _ in POLICIES
        for suffix in ("policy", "off", "on")
    }
    used: set[str] = set()
    names: set[str] = set()
    for row in TABLE:
        assert row.name not in names, f"duplicate transform name [{row.name}]"
        names.add(row.name)
        slots = {v for v, _, _ in row.requires}
        assert row.group_by in slots, (
            f"[{row.name}] groups by [{row.group_by}], which is not one of its requirements"
        )
        for var, dtype, parents in row.requires:
            for p in parents:
                assert p in slots, f"[{row.name}] slot [{var}] names unknown parent [{p}]"
            if dtype.startswith("aspire::"):
                used.add(dtype.split("::")[1])
        for var, dtype in row.products:
            if dtype.startswith("aspire::"):
                used.add(dtype.split("::")[1])
    missing = sorted(used - declared)
    assert not missing, f"types used by the table but not declared: {missing}"
    unused = sorted(declared - used - {f"{b}_policy" for b, _ in POLICIES})
    assert not unused, f"types declared but never used: {unused}"


def lint_extends() -> None:
    sys.path.insert(0, str(MLIB))
    from metasmith.python_api import DataTypeLibrary  # noqa: E402
    import yaml  # noqa: E402

    raw = yaml.safe_load(TYPES_YML.read_text())
    lib = DataTypeLibrary.Load(TYPES_YML)
    edges = 0
    for name, spec in raw["types"].items():
        parents = spec.get("extends", [])
        if isinstance(parents, str): parents = [parents]
        for parent in parents:
            edges += 1
            assert lib[name].IsA(lib[parent]), (
                f"[{name}] declares `extends: {parent}` but does not satisfy it -- "
                f"it restates a property the parent set. Missing: "
                f"{sorted(lib[parent].properties - lib[name].properties)}"
            )
    print(f"  extends: {edges} edge(s) all subsume")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--lint", action="store_true",
                    help="only check the existing aspire.yml, write nothing")
    args = ap.parse_args()

    if args.lint:
        lint_extends()
        raise SystemExit(0)

    check_table()
    n_types = write_types()
    n_stubs = write_stubs()
    ported = sum(1 for r in TABLE if r.source)
    folded = sum(len(r.folds) for r in TABLE)
    print(f"  data_types/aspire.yml: {n_types} types")
    print(f"  transforms/aspire/:    {n_stubs} stubs "
          f"({ported} ported .nf processes + {folded} folded into them, "
          f"{n_stubs - ported} port artifacts)")
    lint_extends()

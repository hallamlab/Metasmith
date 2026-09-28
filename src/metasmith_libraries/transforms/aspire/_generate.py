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
        "run": t(VALUE, "one ASPIRE study; the grouping parent every stage descends from", ext="txt",
                 # r4: an ASPIRE run is a survey. Carrying `amplicon::survey`'s only property
                 # makes this a property superset of it, so the six lifted ecology transforms
                 # can require the generic grouping node and still be satisfied by a run.
                 # A cross-file `extends:` would say this more plainly and is forbidden --
                 # test_type_hierarchy.py::test_no_cross_file_extends.
                 logistics="to group per-sample abundance profiles into one count table"),
        # Module 1's settings, one YAML keyed by step. research/aspire/presets/ holds the
        # two presets: tool defaults, and the values ASPIRE's own study configs use.
        "params": t(FILE, "ASPIRE read-to-ASV settings, keyed by step", ext="yml"),
    }),
    ("study-level inputs the .nf read out of its config", {
        "sample_metadata": t(FILE, "study metadata table, one row per sample", ext="tsv"),
        "mito_reference_source": t(FILE, "mitochondrial reference sequences for the mito BLAST database", ext="fasta"),
        "contaminant_reference_source": t(FILE, "contaminant/biofilm reference sequences for the contaminant BLAST database", ext="fasta"),
    }),
    ("per-sample read processing", {
        # Interleaved for a paired sample, single-end otherwise; the parity is in the
        # sample's read_metadata, as it is for every short-read consumer in the library.
        "qc_reads": t(FILE, "fastp-trimmed amplicon reads for one sample", ext="fastq.gz"),
        "fastp_report_json": t(FILE, "fastp QC report for one sample", ext="json"),
        "fastp_report_html": t(FILE, "fastp QC report rendering for one sample", ext="html"),
        "filtered_fasta": t(FILE, "quality-filtered amplicon reads for one sample, merged when paired", ext="fasta.gz"),
        "read_counts": t(FILE, "per-sample read counts after merging and after quality filtering", ext="tsv"),
    }),
    ("the run-level ASV spine", {
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
        "mitomaster_table": t(FILE, "MitoMaster classification of each ASV", ext="tsv"),
        "mito_blast6": t(FILE, "ASV hits against the mitochondrial database", ext="tsv"),
        "contaminant_blast6": t(FILE, "ASV hits against the contaminant database", ext="tsv"),
        "mito_summary_tables": t(DIR, "per-category summaries of the non-target call"),
        "mito_plots": t(DIR, "renderings of the non-target call"),
    }),
    ("curation", {
        "counts_clean": t(FILE, "ASV counts with every non-target ASV removed", ext="tsv"),
        "counts_removed": t(FILE, "the counts curation removed, one reason per ASV", ext="tsv"),
    }),
    ("read accounting", {
        "read_fate": t(FILE, "per-sample read counts at every stage from raw reads to clean counts", ext="tsv"),
        "sankey_outputs": t(DIR, "read-fate sankey renderings"),
    }),
    ("metadata joins", {
        "metadata_mito": t(FILE, "study metadata joined to mitochondrial read accounting", ext="tsv"),
        "asv_meta_mito": t(FILE, "long-form ASV table joined to metadata and taxonomy, mitochondrial", ext="tsv"),
        "asv_final_mito": t(FILE, "analysis-ready mitochondrial ASV count matrix", ext="tsv"),
    }),
    ("grouping diagnostics and soft labels", {
        "grouping_diagnostics_outputs": t(DIR, "group separability diagnostics and their renderings"),
        "soft_assignments": t(FILE, "soft group-label assignments for samples missing a hard label", ext="tsv"),
        "soft_validation": t(FILE, "per-sample validation of the soft label assignment", ext="tsv"),
        "soft_validation_summary": t(FILE, "summary of soft label assignment quality", ext="tsv"),
    }),
    ("the three consumer-facing analysis channels", {
        "analysis_metadata": t(FILE, "study metadata joined to the microbial read accounting, as every analysis reads it", ext="tsv"),
        "analysis_asv_meta": t(FILE, "long-form microbial ASV table joined to metadata and taxonomy, as every analysis reads it", ext="tsv"),
        # r4: deliberately NOT a property superset of `amplicon::asv_table`, and so
        # deliberately not what the lifted ecology transforms read. Making it one would
        # let `filter_table` -- which requires `amplicon::asv_table` -- consume its own
        # descendant. See research/kbase/curation/r4/aspire_topology.md.
        "analysis_counts": t(FILE, "the microbial ASV count matrix every analysis reads", ext="tsv"),
    }),
    ("terminal analyses", {
        "upset_plots": t(DIR, "UpSet renderings of metadata group overlap"),
        "bubble_plots": t(DIR, "taxonomic bubble plot renderings"),
        "umap_plots": t(DIR, "UMAP clustering renderings"),
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
        "master_long": t(FILE, "long-form master ASV table", ext="tsv"),
        "master_count_wide": t(FILE, "wide-form master count table", ext="tsv"),
        "master_source_manifest": t(FILE, "which stage contributed each master table column", ext="tsv"),
        "master_column_mapping": t(FILE, "original to canonical column name mapping", ext="tsv"),
        "master_column_collisions": t(FILE, "columns that collided during the master merge", ext="tsv"),
    }),
]


POLICIES: list[tuple[str, str]] = [
    ("spieceasi", "infer the co-occurrence network with SpiecEasi rather than supplying one"),
    ("network_modules", "detect modules in the co-occurrence network"),
    ("asv_mag_link", "link ASVs to MAGs"),
    ("graph_network", "render and overlay the co-occurrence network"),
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

# r4, round 3's first refactor: the six community-ecology rows were gated on `run` and
# read `aspire::analysis_counts`, so nothing outside this pipeline could reach any of
# them -- five KBase verbs and 59 narrative copies locked in one namespace. They now hang
# off the GENERIC grouping node instead. The gate is not deleted, it is generalised: a
# fan-in needs a grouping parent its inputs descend from, and `aspire::run` carries
# `amplicon::survey`'s property, so an ASPIRE run still satisfies it unchanged.
SURVEY = ("survey", "amplicon::survey", ())

LIFT_NOTE_2 = (
    "r4 ecology lift (second pass): lifted off `aspire::run` onto the generic `amplicon::survey` grouping node so `spieceasi` and `graph_network` -- lifted in the first pass -- are actually REACHABLE from a bare `amplicon::asv_table`. Lifting a transform whose own inputs are still gated moves the gate, it does not remove it. See research/kbase/curation/r4/analyses.md."
)


def _r(var, dtype, *parents):
    return (var, dtype, tuple(parents))


# A sample is a `sequences::sample_name` under the run, with its read_metadata and its reads
# both directly beneath it. Parity lives in the read_metadata JSON and is read at run time,
# as megahit.py does. A paired sample's zipped halves reach `sequences::short_reads` through
# logistics/interleave_zipped_short_reads.py; a single-end sample is given as
# `sequences::short_reads_se`, which is one. That is the join, and everything from
# `fastp_qc` on is parity-blind at plan time.
#
# The reads are NOT under the read_metadata, as they are in fabfos. A given with four
# ancestors (run, name, metadata, pair) loses the link to its read_pair on the solver's
# given lineage, and the interleave step then has no candidates. Three solve.
NAME = _r("name", "sequences::sample_name", "run")
META = _r("meta", "sequences::read_metadata", "name")
PARAMS = _r("params", "aspire::params", "run")


TABLE: list[T] = [
    T("fastp_qc", "FASTP_QC", 3121,
      [RUN, NAME, META, _r("reads", "sequences::short_reads", "name"), PARAMS],
      [("qc", "aspire::qc_reads"),
       ("rjson", "aspire::fastp_report_json"), ("rhtml", "aspire::fastp_report_html")],
      "name", cpus=4, hours=2,
      note="Reads `parity` from the read_metadata JSON: `--interleaved_in` for a paired "
           "sample, plain `-i` for a single-end one. Settings under `fastp:` in the params "
           "file. The product is an aspire type on purpose, so the shipped "
           "kbase/clean_reads/fastp.py and bbduk cannot answer for this slot with their "
           "own trimming."),

    T("merge_and_filter_reads", "FILTER_READS", 3221,
      [RUN, NAME, META, _r("qc", "aspire::qc_reads", "name"), PARAMS],
      [("filtered", "aspire::filtered_fasta"), ("counts", "aspire::read_counts")],
      "name", folds=("MERGE_READS@3179",),
      note="Branches on `parity`. Paired: `vsearch --fastq_mergepairs` (settings under "
           "`merge:`) then `--fastq_filter` (under `filter:`). Single-end: the filter "
           "alone. The merged pair is not a product; what a reader wants from the merge is "
           "its rate, which goes in the per-sample count file beside the filtered reads "
           "for read_accounting.",
      cpus=4, hours=2),

    T("denoise", "DENOISE", 3367,
      [RUN, NAME, _r("filtered", "aspire::filtered_fasta", "name"), PARAMS],
      [("counts", "amplicon::asv_table"), ("seqs", "amplicon::asv_seqs")],
      "run",
      folds=("RELABEL_FILTERED@3248", "CONCAT_FASTAS@3274", "DEREPLICATE@3298",
             "CHIMERA_CHECK@3393", "CREATE_COUNT_MATRIX@3417"),
      note="The study fan-in, and the one place reads become ASVs. Relabel each sample's "
           "headers from the sample_name its fasta descends from (context.SourceOf, never "
           "the position in the group), concatenate, `--derep_fulllength`, "
           "`--cluster_unoise` (under `unoise:`), `--uchime3_denovo`, then "
           "`--usearch_global` of the pooled reads against the non-chimeric centroids "
           "(under `count:`). Every intermediate had exactly one consumer. The two "
           "products are the library's generic ASV types, so this is the seam a long-read "
           "denoiser joins at, the way both read lengths meet at `sequences::assembly`.",
      cpus=8, memory_gb=16, hours=6),

    T("filter_table", "FILTER_TABLE", 3444,
      [RUN, _r("counts", "amplicon::asv_table", "run"),
       _r("seqs", "amplicon::asv_seqs", "run"), PARAMS],
      [("fcounts", "aspire::asv_filtered_counts"), ("fseqs", "aspire::asv_filtered_seqs")],
      "run",
      note="Sample depth and ASV prevalence cuts, under `table_filter:`."),

    T("sina_trim", "SINA_TRIM", 3322,
      [RUN, _r("fseqs", "aspire::asv_filtered_seqs", "run"),
       _r("silva", "amplicon::silva_db")],
      [("trimmed", "aspire::sina_trimmed_seqs"), ("aligned", "aspire::sina_aligned_seqs"),
       ("log", "aspire::sina_log"), ("vreg", "aspire::sina_v_regions")],
      "run",
      note="the .nf calls its input `derep_fasta`, but the workflow body wires "
           "FILTER_TABLE's filtered ASV sequences in (asv_pipeline.nf:2630-2631). "
           "The parameter name is legacy; the wiring is what is ported.",
      cpus=16, memory_gb=32, hours=8),

    T("taxonomy", "TAXONOMY", 3470,
      [RUN, _r("trimmed", "aspire::sina_trimmed_seqs", "run"),
       _r("silva", "amplicon::silva_db")],
      [("tax", "amplicon::asv_taxonomy"), ("upper", "aspire::taxonomy_uppercase_seqs"),
       ("stats", "aspire::taxonomy_stats")],
      "run",
      note="Absorbs the retired placeholder lane's amplicon/qiime2_taxonomy.py. Its "
           "method, for the protocol pass: `qiime feature-classifier classify-sklearn` "
           "with the NB classifier, then `classify-consensus-vsearch` against the SILVA "
           "sequences and taxonomy (perc-identity 0.70, min-consensus 0.51, maxaccepts "
           "10, maxrejects 10), and a merge that keeps the NB call unless it is "
           "Unassigned or Unclassified, falls back to the vsearch call, and records which "
           "one won. Columns: Feature ID, Taxon, Confidence, Source. That file fetched "
           "SILVA 138-99 with wget inside the protocol; here the classifier, sequences and "
           "taxonomy come from the one `amplicon::silva_db` bundle, as SINA's ARB file does.",
      cpus=8, memory_gb=16, hours=6),

    T("mitomaster", "MITOMASTER", 3544,
      [RUN, _r("fcounts", "aspire::asv_filtered_counts", "run"),
       _r("fseqs", "aspire::asv_filtered_seqs", "run"),
       _r("mito_src", "aspire::mito_reference_source"),
       _r("cont_src", "aspire::contaminant_reference_source")],
      [("master", "aspire::mitomaster_table"), ("mhits", "aspire::mito_blast6"),
       ("chits", "aspire::contaminant_blast6")],
      "run", folds=("PREPARE_BLAST_DATABASES@3511",),
      note="r4 fold. PREPARE_BLAST_DATABASES was two makeblastdb calls whose products "
           "each had one consumer, which was this transform. The standard library "
           "already treats that as inside-the-transform work -- "
           "amplicon/blast_map_asvs.py runs makeblastdb and then blastn in one protocol "
           "-- so the two reference FASTAs move up here.",
      cpus=8, memory_gb=16, hours=6),

    T("curate", "MITO_DECONTAM", 3595,
      [RUN, _r("fcounts", "aspire::asv_filtered_counts", "run"),
       _r("fseqs", "aspire::asv_filtered_seqs", "run"),
       _r("tax", "amplicon::asv_taxonomy", "run"),
       _r("master", "aspire::mitomaster_table", "run"),
       _r("mhits", "aspire::mito_blast6", "run"),
       _r("chits", "aspire::contaminant_blast6", "run"),
       PARAMS],
      [("clean", "aspire::counts_clean"), ("removed", "aspire::counts_removed"),
       ("summaries", "aspire::mito_summary_tables"), ("plots", "aspire::mito_plots")],
      "run", folds=("FILTER_COUNTS@3637",),
      note="Decides every non-target call and applies it in one step. The .nf split "
           "this in two and partitioned the result into four count tables; here there "
           "are two, the counts kept and the counts removed, and each removed ASV carries "
           "its reason (mitochondrial, contaminant, below abundance, taxonomy). "
           "Thresholds under `curate:`. Negative controls are not modelled yet; they "
           "belong here as a second evidence input beside the contaminant hits."),

    T("read_accounting", "GENERAL_STATS", 3755,
      [RUN, NAME,
       _r("rjson", "aspire::fastp_report_json", "name"),
       _r("rcounts", "aspire::read_counts", "name"),
       _r("raw", "amplicon::asv_table", "run"),
       _r("clean", "aspire::counts_clean", "run"),
       _r("removed", "aspire::counts_removed", "run")],
      [("fate", "aspire::read_fate")], "run",
      note="One row per sample, one column per stage: raw and post-fastp from the fastp "
           "report, merged and filtered from the per-sample count file, mapped from the "
           "ASV table, then kept and removed by reason. The .nf read these out of other "
           "processes' publish directories behind an ordering barrier; every one is now a "
           "declared input."),

    T("sankey", "SANKEY", 3687,
      [RUN, _r("fate", "aspire::read_fate", "run"),
       _r("removed", "aspire::counts_removed", "run"),
       _r("meta", "aspire::sample_metadata")],
      [("out", "aspire::sankey_outputs")], "run",
      note="`sankey.done` is dropped; the renderings are the output, and "
           "MASTER_SUMMARY reads the directory rather than the sentinel."),

    T("plot_metadata", "PLOT_METADATA", 3793,
      [RUN, _r("fate", "aspire::read_fate", "run"),
       _r("clean", "aspire::counts_clean", "run"),
       _r("removed", "aspire::counts_removed", "run"),
       _r("tax", "amplicon::asv_taxonomy", "run"),
       _r("meta", "aspire::sample_metadata")],
      [("md", "aspire::analysis_metadata"), ("am", "aspire::analysis_asv_meta"),
       ("counts", "aspire::analysis_counts"), ("md_mito", "aspire::metadata_mito"),
       ("am_mito", "aspire::asv_meta_mito"), ("af_mito", "aspire::asv_final_mito")],
      "run", cpus=4, memory_gb=16, hours=3,
      note="The join that turns counts into analysis tables, and now the only producer "
           "of the three consumer-facing channels. The .nf rebound those channels "
           "through label augmentation and batch correction; both are off by default, "
           "outside the reads-to-ASV pipeline, and not ported. The mitochondrial "
           "tables come from the removed counts whose reason is mitochondrial."),

    T("grouping_diagnostics", "GROUPING_DIAGNOSTICS", 4929,
      [RUN, _r("md", "aspire::analysis_metadata", "run"),
       _r("counts", "aspire::analysis_counts", "run")],
      [("out", "aspire::grouping_diagnostics_outputs"),
       ("assign", "aspire::soft_assignments"), ("valid", "aspire::soft_validation"),
       ("vsum", "aspire::soft_validation_summary")],
      "run", cpus=4, memory_gb=16, hours=4,
      note="The soft labels are still computed and reported. Nothing applies them: "
           "GROUP_LABEL_AUGMENTATION, their only consumer in the .nf, is not ported."),

    T("plot_upset", "PLOT_UPSET", 3927,
      [RUN, _r("md", "aspire::analysis_metadata", "run")],
      [("out", "aspire::upset_plots")], "run"),

    T("bubbleplotter", "BUBBLEPLOTTER", 4018,
      [RUN, _r("am", "aspire::analysis_asv_meta", "run")],
      [("out", "aspire::bubble_plots")], "run", cpus=2, memory_gb=8),

    T("umap_clustering", "UMAP_CLUSTERING", 4055,
      [SURVEY, _r("counts", "amplicon::asv_table", "survey"),
       _r("am", "aspire::analysis_asv_meta", "survey")],
      [("out", "aspire::umap_plots")], "survey", cpus=2, memory_gb=8,
      note="r4 ecology lift: requires the generic `amplicon::survey` grouping node and a bare `amplicon::asv_table` instead of `aspire::run` and `aspire::analysis_counts`, so it is reachable from any count table -- `kbase/profile_abundance/kraken_abundance.py` produces one from kraken2 reports. An ASPIRE run satisfies the survey requirement unchanged. See research/kbase/curation/r4/aspire_topology.md."),

    T("collectors_curve", "COLLECTORS_CURVE", 4364,
      [RUN, _r("counts", "aspire::analysis_counts", "run"),
       _r("md", "aspire::analysis_metadata", "run")],
      [("out", "aspire::collectors_outputs")], "run", cpus=2, memory_gb=8, hours=2),

    T("diversity_analysis", "DIVERSITY_ANALYSIS", 4399,
      [SURVEY, _r("counts", "amplicon::asv_table", "survey"),
       _r("md", "aspire::analysis_metadata", "survey")],
      [("out", "aspire::diversity_outputs")], "survey", cpus=4, memory_gb=16, hours=3,
      note="r4 ecology lift: requires the generic `amplicon::survey` grouping node and a bare `amplicon::asv_table` instead of `aspire::run` and `aspire::analysis_counts`, so it is reachable from any count table -- `kbase/profile_abundance/kraken_abundance.py` produces one from kraken2 reports. An ASPIRE run satisfies the survey requirement unchanged. See research/kbase/curation/r4/aspire_topology.md."),

    T("indicspecies", "INDICSPECIES", 4518,
      [SURVEY, _r("md", "aspire::analysis_metadata", "survey"),
       _r("counts", "amplicon::asv_table", "survey")],
      [("g1sum", "aspire::indicspecies_group1_summary"),
       ("g2sum", "aspire::indicspecies_group2_summary"),
       ("g1res", "aspire::indicspecies_group1_results"),
       ("g2res", "aspire::indicspecies_group2_results"),
       ("tables", "aspire::indicspecies_tables"),
       ("plots", "aspire::indicspecies_plots"),
       ("aligned", "aspire::indicspecies_aligned_plots")],
      "survey", folds=("INDICSPECIES_PLOTS@4608", "INDICSPECIES_ALIGNED_PLOTS@4792"),
      cpus=8, memory_gb=32, hours=12, note=LIFT_NOTE_2),

    T("voc_correlation", "VOC_CORRELATION", 4827,
      [RUN, _r("am", "aspire::analysis_asv_meta", "run"),
       _r("counts", "aspire::analysis_counts", "run"),
       _r("tables", "aspire::indicspecies_tables", "run")],
      [("out", "aspire::voc_correlation_outputs")], "run", cpus=2, memory_gb=8),

    T("measurement_association", "MEASUREMENT_ASSOCIATION", 4875,
      [SURVEY, _r("counts", "amplicon::asv_table", "survey"),
       _r("am", "aspire::analysis_asv_meta", "survey"),
       _r("md", "aspire::analysis_metadata", "survey")],
      [("out", "aspire::measurement_association_outputs")], "survey", cpus=2, memory_gb=8,
      note="r4 ecology lift: requires the generic `amplicon::survey` grouping node and a bare `amplicon::asv_table` instead of `aspire::run` and `aspire::analysis_counts`, so it is reachable from any count table -- `kbase/profile_abundance/kraken_abundance.py` produces one from kraken2 reports. An ASPIRE run satisfies the survey requirement unchanged. See research/kbase/curation/r4/aspire_topology.md."),

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
      [SURVEY, _r("counts", "amplicon::asv_table", "survey"),
       _r("am", "aspire::analysis_asv_meta", "survey")],
      [("out", "aspire::paired_group_contrast_outputs")], "survey",
      cpus=4, memory_gb=16, hours=4,
      note="r4 ecology lift: requires the generic `amplicon::survey` grouping node and a bare `amplicon::asv_table` instead of `aspire::run` and `aspire::analysis_counts`, so it is reachable from any count table -- `kbase/profile_abundance/kraken_abundance.py` produces one from kraken2 reports. An ASPIRE run satisfies the survey requirement unchanged. See research/kbase/curation/r4/aspire_topology.md."),

    T("clustermaps", "CLUSTERMAPS", 5324,
      [RUN, _r("am", "aspire::analysis_asv_meta", "run"),
       _r("md", "aspire::analysis_metadata", "run"),
       _r("tables", "aspire::indicspecies_tables", "run"),
       _r("removed", "aspire::counts_removed", "run")],
      [("out", "aspire::clustermap_outputs")], "run", cpus=4, memory_gb=16, hours=3,
      note="the .nf passes a boolean saying whether indicator species ran and "
           "then globs the tables out of a shared directory. The directory is "
           "the real edge, so it is what is declared."),

    T("spieceasi", "SPIECEASI", 5452,
      [SURVEY, _r("policy", "aspire::spieceasi_on", "survey"),
       _r("counts", "amplicon::asv_table", "survey"),
       _r("keep", "aspire::indicspecies_group1_summary", "survey")],
      [("all", "aspire::network_graph_all"), ("thr", "aspire::network_graph_thr"),
       ("nf", "aspire::network_node_features")],
      "survey", cpus=16, memory_gb=64, hours=24,
      note="r4 ecology lift: requires the generic `amplicon::survey` grouping node and a bare `amplicon::asv_table` instead of `aspire::run` and `aspire::analysis_counts`, so it is reachable from any count table -- `kbase/profile_abundance/kraken_abundance.py` produces one from kraken2 reports. An ASPIRE run satisfies the survey requirement unchanged. See research/kbase/curation/r4/aspire_topology.md."),

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
      [SURVEY, _r("policy", "aspire::network_modules_on", "survey"),
       _r("all", "aspire::network_graph_all", "survey"),
       _r("thr", "aspire::network_graph_thr", "survey")],
      [("sub", "aspire::network_modules_sub"), ("mall", "aspire::network_modules_all"),
       ("summary", "aspire::network_modules_summary"),
       ("runs", "aspire::network_modules_runs")],
      "survey", cpus=8, memory_gb=32, hours=6, note=LIFT_NOTE_2),

    T("network_modules_absent", None, None,
      [SURVEY, _r("policy", "aspire::network_modules_off", "survey")],
      [("sub", "aspire::network_modules_sub"), ("mall", "aspire::network_modules_all")],
      "survey",
      note=LIFT_NOTE_2 + " the `off` arm; asv_pipeline.nf:2726-2729 falls back to files on "
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
      [SURVEY, _r("policy", "aspire::asv_mag_link_off", "survey")],
      [("pairing", "aspire::asv_mag_pairing"), ("out", "aspire::asv_mag_outputs")],
      "survey",
      note=LIFT_NOTE_2 + " Only the OFF arm lifts. `asv_mag_link` itself reads "
           "`aspire::asv_filtered_seqs`, which no transform outside the ASPIRE lane "
           "produces, so linking ASVs to MAGs stays a pipeline capability while "
           "declining to link them does not."),

    T("graph_network", "GRAPH_NETWORK", 5579,
      [SURVEY, _r("policy", "aspire::graph_network_on", "survey"),
       _r("all", "aspire::network_graph_all", "survey"),
       _r("thr", "aspire::network_graph_thr", "survey"),
       _r("nf", "aspire::network_node_features", "survey"),
       _r("counts", "amplicon::asv_table", "survey"),
       _r("md", "aspire::analysis_metadata", "survey"),
       _r("pairing", "aspire::asv_mag_pairing", "survey"),
       _r("tax", "amplicon::asv_taxonomy", "survey"),
       _r("tables", "aspire::indicspecies_tables", "survey"),
       _r("sub", "aspire::network_modules_sub", "survey"),
       _r("mall", "aspire::network_modules_all", "survey")],
      [("out", "aspire::network_outputs")], "survey", cpus=8, memory_gb=32, hours=6,
      note="r4 ecology lift: requires the generic `amplicon::survey` grouping node and a bare `amplicon::asv_table` instead of `aspire::run` and `aspire::analysis_counts`, so it is reachable from any count table -- `kbase/profile_abundance/kraken_abundance.py` produces one from kraken2 reports. An ASPIRE run satisfies the survey requirement unchanged. See research/kbase/curation/r4/aspire_topology.md." + " Reachable in principle and expensive in practice: it renders a "
           "co-occurrence network, so running it outside ASPIRE means supplying the "
           "network. That is a property of what it does, not of the gate removed."),

    T("graph_network_absent", None, None,
      [RUN, _r("policy", "aspire::graph_network_off", "run")],
      [("out", "aspire::network_outputs")], "run"),

    T("asv_mag_network", "ASV_MAG_NETWORK", 5654,
      [RUN, _r("graph", "aspire::network_graph_all", "run"),
       _r("nf", "aspire::network_node_features", "run"),
       _r("tax", "amplicon::asv_taxonomy", "run"),
       _r("counts", "aspire::analysis_counts", "run"),
       _r("pairing", "aspire::asv_mag_pairing", "run"),
       _r("magl", "aspire::asv_mag_outputs", "run")],
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
       _r("magl", "aspire::asv_mag_outputs", "run")],
      [("long", "aspire::master_long"), ("wide", "aspire::master_count_wide"),
       ("manifest", "aspire::master_source_manifest"),
       ("colmap", "aspire::master_column_mapping"),
       ("collisions", "aspire::master_column_collisions")],
      "run", cpus=4, memory_gb=32, hours=4,
      note="r4: the fourth requirement is gone with MASTER_SUMMARY_OPTIONAL_SLOT. "
           "Three `.done` barriers plus a --data-dir scan, replaced by three "
           "directory-typed requirements each with an off-arm producer. Which "
           "of the merged tables actually appear is decided at runtime by a "
           "config whitelist and stays a runtime parameter: modelling it in "
           "the type system would need one transform per subset."),
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

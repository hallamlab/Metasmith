#!/usr/bin/env python3
"""The ASPIRE topology, in one table, and the generator that emits it.

`research/aspire/upstream/ASPIRE/asv_pipeline.nf` is 45 hand-wired Nextflow processes. This file
is the port of that wiring into a typed graph: `TABLE` has one row per ported
process, and running this module writes both `data_types/aspire.yml` and every
stub transform under `transforms/aspire/`.

The table stays the one source of topology and of `aspire.yml`, including for
transforms with real bodies. A file that begins with `BANNER` is a stub and is
rewritten on every run; a file without it is hand-written and is never touched.
A row edit therefore has to be mirrored by hand in a real body, and `--lint`
fails until the body's requirements, products and grouping match its row.

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
    python transforms/aspire/_generate.py --lint      # check subsumption and real bodies

`--lint` loads the compiled library, so run `dev/libraries.sh -bm` after a regenerate.
"""

from __future__ import annotations

import argparse
import sys
import textwrap
from pathlib import Path

HERE = Path(__file__).resolve().parent
MLIB = HERE.parent.parent
TYPES_YML = MLIB / "data_types" / "aspire.yml"

FILE, DIR = "file", "dir"


def t(kind, desc, **props):
    return kind, desc, props


TYPE_SECTIONS: list[tuple[str, dict]] = [
    ("the study keystone: what every stage descends from", {
        # The sample sheet is the study. Its first column is the sample id, and every other
        # column is a categorical label: a group is any label, and an analysis that compares
        # groups runs once per label. Numeric measurements go in `sample_measurements`.
        "study_metadata": t(FILE, "the study sample sheet, one row per sample, with the sample id first and a categorical label in every other column", ext="tsv",
                 # r4: an ASPIRE study is a survey. Carrying `amplicon::survey`'s only property
                 # makes this a property superset of it, so the lifted ecology transforms
                 # can require the generic grouping node and still be satisfied by a study.
                 # A cross-file `extends:` would say this more plainly and is forbidden --
                 # test_type_hierarchy.py::test_no_cross_file_extends.
                 logistics="to group per-sample abundance profiles into one count table"),
        # Module 1's settings, one YAML keyed by step. research/aspire/presets/ holds the
        # two presets: tool defaults, and the values ASPIRE's shipped config uses.
        "params": t(FILE, "ASPIRE read-to-ASV settings, keyed by step", ext="yml"),
    }),
    ("study-level inputs the .nf read out of its config", {
        "sample_measurements": t(FILE, "numeric per-sample measurements, one row per sample keyed by the sample sheet's id", ext="tsv"),
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
    ("the study-level ASV spine", {
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
        # A property superset of `amplicon::abundance_table`, the curated table the lifted
        # ecology rows read, and deliberately NOT of `amplicon::asv_table`: that would let
        # `filter_table`, which requires the raw table, consume its own descendant.
        "analysis_counts": t(FILE, "the microbial ASV count matrix every analysis reads", ext="tsv",
                             Data="analysis-ready sample by feature abundance matrix"),
    }),
    ("terminal analyses", {
        "upset_plots": t(DIR, "UpSet renderings of metadata group overlap"),
        "bubble_plots": t(DIR, "taxonomic bubble plot renderings"),
        "umap_plots": t(DIR, "UMAP clustering renderings"),
        "collectors_outputs": t(DIR, "collector's curve renderings"),
        "diversity_outputs": t(DIR, "alpha and beta diversity results and renderings"),
        "diversity_mito_outputs": t(DIR, "alpha and beta diversity of the mitochondrial reads beside the microbial ones"),
        "measurement_association_outputs": t(DIR, "ASV to continuous-measurement association results"),
        "clustermap_outputs": t(DIR, "abundance clustermap renderings"),
    }),
    ("indicator species analysis", {
        "indicspecies_results": t(DIR, "indicator species results and summary, one pair of tables for each label of the sample sheet"),
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
        "mag_collection": t(DIR, "a MAG set laid out for the ASV linker -- genomes_subset/ with one FASTA per genome, barrnap/ with one GFF per genome named by the same stem, and Master_genome_QC.tsv keyed by genome id"),
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


STUDY = ("study", "aspire::study_metadata", ())

# r4, round 3's first refactor: the six community-ecology rows were gated on the study root and
# read `aspire::analysis_counts`, so nothing outside this pipeline could reach any of
# them -- five KBase verbs and 59 narrative copies locked in one namespace. They now hang
# off the GENERIC grouping node instead. The gate is not deleted, it is generalised: a
# fan-in needs a grouping parent its inputs descend from, and `aspire::study_metadata`
# carries `amplicon::survey`'s property, so an ASPIRE study still satisfies it unchanged.
SURVEY = ("survey", "amplicon::survey", ())

LIFT_NOTE = (
    "Hangs off the generic `amplicon::survey` grouping node and reads the curated "
    "`amplicon::abundance_table`, so any analysis-ready count table reaches it -- "
    "`kbase/profile_abundance/kraken_abundance.py` makes one from kraken2 reports. In an "
    "ASPIRE study the table is `plot_metadata`'s curated counts, never `denoise`'s raw one: "
    "the raw table does not satisfy the requirement. See research/kbase/curation/r4/aspire_topology.md."
)

LIFT_NOTE_2 = (
    "r4 ecology lift (second pass): lifted off the study root onto the generic `amplicon::survey` grouping node so `spieceasi` and `graph_network` -- lifted in the first pass -- are actually REACHABLE from a bare `amplicon::asv_table`. Lifting a transform whose own inputs are still gated moves the gate, it does not remove it. See research/kbase/curation/r4/analyses.md."
)


def _r(var, dtype, *parents):
    return (var, dtype, tuple(parents))


# A sample is a `sequences::read_metadata` under the study, with its reads beneath it. The JSON
# carries the sample id under `sample` beside `parity` and `length_class`: without the id, two
# samples' metadata would be the same value. Parity is read at run time, as megahit.py does. A
# paired sample's zipped halves reach `sequences::short_reads` through
# logistics/interleave_zipped_short_reads.py; a single-end sample is given as
# `sequences::short_reads_se`, which is one. That is the join, and everything from `fastp_qc`
# on is parity-blind at plan time.
#
# A paired half is three ancestors deep (study, metadata, read_pair). A fourth level loses the
# given's link to its read_pair in the solver, and the interleave step then has no candidates.
META = _r("meta", "sequences::read_metadata", "study")
PARAMS = _r("params", "aspire::params", "study")

# Every row that compares groups iterates over the label columns of the sample sheet.
LABELS_NOTE = ("Groups are the label columns of the sample sheet; the analysis runs once per "
               "label and skips a label with fewer than two levels.")


TABLE: list[T] = [
    T("fastp_qc", "FASTP_QC", 3121,
      [STUDY, META, _r("reads", "sequences::short_reads", "meta"), PARAMS],
      [("qc", "aspire::qc_reads"),
       ("rjson", "aspire::fastp_report_json"), ("rhtml", "aspire::fastp_report_html")],
      "meta", cpus=4, hours=2,
      note="Reads `parity` from the read_metadata JSON: `--interleaved_in` for a paired "
           "sample, plain `-i` for a single-end one. Settings under `fastp:` in the params "
           "file. The product is an aspire type on purpose, so the shipped "
           "kbase/clean_reads/fastp.py and bbduk cannot answer for this slot with their "
           "own trimming."),

    T("merge_and_filter_reads", "FILTER_READS", 3221,
      [STUDY, META, _r("qc", "aspire::qc_reads", "meta"), PARAMS],
      [("filtered", "aspire::filtered_fasta"), ("counts", "aspire::read_counts")],
      "meta", folds=("MERGE_READS@3179",),
      note="Branches on `parity`. Paired: `vsearch --fastq_mergepairs` (settings under "
           "`merge:`) then `--fastq_filter` (under `filter:`). Single-end: the filter "
           "alone. The filter relabels every read `<sample>.<n>;sample=<sample>` from the "
           "read_metadata's `sample` key, which is RELABEL_FILTERED's job upstream: the "
           "sample is known here without any lineage lookup. The merged pair is not a "
           "product; its rate goes in the per-sample count file for read_accounting.",
      cpus=4, hours=2),

    T("denoise", "DENOISE", 3367,
      [STUDY, META, _r("filtered", "aspire::filtered_fasta", "meta"), PARAMS],
      [("counts", "amplicon::asv_table"), ("seqs", "amplicon::asv_seqs")],
      "study",
      folds=("CONCAT_FASTAS@3274", "DEREPLICATE@3298",
             "CHIMERA_CHECK@3393", "CREATE_COUNT_MATRIX@3417"),
      note="The study fan-in, and the one place reads become ASVs. Concatenate the "
           "relabelled reads, `--derep_fulllength`, "
           "`--cluster_unoise` (under `unoise:`), `--uchime3_denovo`, then "
           "`--usearch_global` of the pooled reads against the non-chimeric centroids "
           "(under `count:`). Every intermediate had exactly one consumer. The two "
           "products are the library's generic ASV types, so this is the seam a long-read "
           "denoiser joins at, the way both read lengths meet at `sequences::assembly`.",
      cpus=8, memory_gb=16, hours=6),

    T("filter_table", "FILTER_TABLE", 3444,
      [STUDY, _r("counts", "amplicon::asv_table", "study"),
       _r("seqs", "amplicon::asv_seqs", "study"), PARAMS],
      [("fcounts", "aspire::asv_filtered_counts"), ("fseqs", "aspire::asv_filtered_seqs")],
      "study",
      note="Sample depth and ASV prevalence cuts, under `table_filter:`."),

    T("sina_trim", "SINA_TRIM", 3322,
      [STUDY, _r("fseqs", "aspire::asv_filtered_seqs", "study"),
       _r("silva", "amplicon::silva_db"), PARAMS],
      [("trimmed", "aspire::sina_trimmed_seqs"), ("aligned", "aspire::sina_aligned_seqs"),
       ("log", "aspire::sina_log"), ("vreg", "aspire::sina_v_regions")],
      "study",
      note="the .nf calls its input `derep_fasta`, but the workflow body wires "
           "FILTER_TABLE's filtered ASV sequences in (asv_pipeline.nf:2630-2631). "
           "The parameter name is legacy; the wiring is what is ported.",
      cpus=16, memory_gb=32, hours=8),

    T("taxonomy", "TAXONOMY", 3470,
      [STUDY, _r("trimmed", "aspire::sina_trimmed_seqs", "study"),
       _r("silva", "amplicon::silva_db")],
      [("tax", "amplicon::asv_taxonomy"), ("upper", "aspire::taxonomy_uppercase_seqs"),
       ("stats", "aspire::taxonomy_stats")],
      "study",
      note="Upstream's method: QIIME2 `classify-consensus-vsearch` of the uppercased "
           "trimmed sequences against the SILVA sequences and taxonomy in the "
           "`amplicon::silva_db` bundle, through qiime_vs_classifier.py. Columns: "
           "Feature ID, Taxon, Consensus. The bundle's naive Bayes classifier is unused.",
      cpus=8, memory_gb=16, hours=6),

    T("mitomaster", "MITOMASTER", 3544,
      [STUDY, _r("fcounts", "aspire::asv_filtered_counts", "study"),
       _r("fseqs", "aspire::asv_filtered_seqs", "study"),
       _r("mito_src", "aspire::mito_reference_source"),
       _r("cont_src", "aspire::contaminant_reference_source")],
      [("master", "aspire::mitomaster_table"), ("mhits", "aspire::mito_blast6"),
       ("chits", "aspire::contaminant_blast6")],
      "study", folds=("PREPARE_BLAST_DATABASES@3511",),
      note="r4 fold. PREPARE_BLAST_DATABASES was two makeblastdb calls whose products "
           "each had one consumer, which was this transform. The standard library "
           "already treats that as inside-the-transform work -- "
           "amplicon/blast_map_asvs.py runs makeblastdb and then blastn in one protocol "
           "-- so the two reference FASTAs move up here. MitoMaster itself is not run: "
           "it posts every ASV to mitomap.org, and compute nodes have no internet. The "
           "table is written header-only, which is upstream's own `run_mitomaster: false` "
           "output, so the mitochondrial call rests on the BLAST hits and the taxonomy.",
      cpus=8, memory_gb=16, hours=6),

    T("curate", "MITO_DECONTAM", 3595,
      [STUDY, _r("fcounts", "aspire::asv_filtered_counts", "study"),
       _r("fseqs", "aspire::asv_filtered_seqs", "study"),
       _r("tax", "amplicon::asv_taxonomy", "study"),
       _r("master", "aspire::mitomaster_table", "study"),
       _r("mhits", "aspire::mito_blast6", "study"),
       _r("chits", "aspire::contaminant_blast6", "study"),
       PARAMS],
      [("clean", "aspire::counts_clean"), ("removed", "aspire::counts_removed"),
       ("summaries", "aspire::mito_summary_tables"), ("plots", "aspire::mito_plots")],
      "study", folds=("FILTER_COUNTS@3637",),
      note="Decides every non-target call and applies it in one step. The .nf split "
           "this in two and partitioned the result into four count tables; here there "
           "are two, the counts kept and the counts removed, and each removed ASV carries "
           "its reason (mitochondrial, contaminant, below abundance, taxonomy). "
           "Thresholds under `curate:`. Negative controls are not modelled yet; they "
           "belong here as a second evidence input beside the contaminant hits."),

    T("read_accounting", "GENERAL_STATS", 3755,
      [STUDY, META,
       _r("rjson", "aspire::fastp_report_json", "meta"),
       _r("rcounts", "aspire::read_counts", "meta"),
       _r("raw", "amplicon::asv_table", "study"),
       _r("clean", "aspire::counts_clean", "study"),
       _r("removed", "aspire::counts_removed", "study")],
      [("fate", "aspire::read_fate")], "study",
      note="One row per sample, one column per stage: raw and post-fastp from the fastp "
           "report, merged and filtered from the per-sample count file, mapped from the "
           "ASV table, then kept and removed by reason. The .nf read these out of other "
           "processes' publish directories behind an ordering barrier; every one is now a "
           "declared input."),

    T("sankey", "SANKEY", 3687,
      [STUDY, _r("fate", "aspire::read_fate", "study"),
       _r("removed", "aspire::counts_removed", "study")],
      [("out", "aspire::sankey_outputs")], "study",
      note="`sankey.done` is dropped; the renderings are the output, and "
           "MASTER_SUMMARY reads the directory rather than the sentinel. The stage counts "
           "come from the read fate, laid out as upstream's stats files, and the flows "
           "split by the sheet's first label."),

    T("plot_metadata", "PLOT_METADATA", 3793,
      [STUDY, _r("fate", "aspire::read_fate", "study"),
       _r("clean", "aspire::counts_clean", "study"),
       _r("removed", "aspire::counts_removed", "study"),
       _r("tax", "amplicon::asv_taxonomy", "study"), PARAMS],
      [("md", "aspire::analysis_metadata"), ("am", "aspire::analysis_asv_meta"),
       ("counts", "aspire::analysis_counts"), ("md_mito", "aspire::metadata_mito"),
       ("am_mito", "aspire::asv_meta_mito"), ("af_mito", "aspire::asv_final_mito")],
      "study", cpus=4, memory_gb=16, hours=3,
      note="The join that turns counts into analysis tables, and now the only producer "
           "of the three consumer-facing channels. The .nf rebound those channels "
           "through label augmentation and batch correction; both are off by default, "
           "outside the reads-to-ASV pipeline, and not ported. The mitochondrial "
           "tables come from the removed counts whose reason is mitochondrial. A label value "
           "held by fewer than `analysis.min_level_size` samples is blanked here, so every "
           "analysis skips that level and keeps the sample."),

    T("grouping_diagnostics", "GROUPING_DIAGNOSTICS", 4929,
      [STUDY, _r("md", "aspire::analysis_metadata", "study"),
       _r("counts", "aspire::analysis_counts", "study")],
      [("out", "aspire::grouping_diagnostics_outputs"),
       ("assign", "aspire::soft_assignments"), ("valid", "aspire::soft_validation"),
       ("vsum", "aspire::soft_validation_summary")],
      "study", cpus=4, memory_gb=16, hours=4,
      note=LABELS_NOTE + " The soft labels are still computed and reported. Nothing applies them: "
           "GROUP_LABEL_AUGMENTATION, their only consumer in the .nf, is not ported."),

    T("plot_upset", "PLOT_UPSET", 3927,
      [STUDY, _r("md", "aspire::analysis_metadata", "study"),
       _r("clean", "aspire::counts_clean", "study"),
       _r("counts", "aspire::analysis_counts", "study"),
       _r("tax", "amplicon::asv_taxonomy", "study")],
      [("out", "aspire::upset_plots")], "study",
      note=LABELS_NOTE + " The .nf passes only the sheet and re-opens the target and final "
           "count tables and the taxonomy from its output directory; all four are declared."),

    T("bubbleplotter", "BUBBLEPLOTTER", 4018,
      [STUDY, _r("am", "aspire::analysis_asv_meta", "study")],
      [("out", "aspire::bubble_plots")], "study", cpus=2, memory_gb=8,
      note=LABELS_NOTE),

    T("umap_clustering", "UMAP_CLUSTERING", 4055,
      [SURVEY, _r("am", "aspire::analysis_asv_meta", "survey")],
      [("out", "aspire::umap_plots")], "survey", cpus=2, memory_gb=8,
      note=LABELS_NOTE + " Reads the long-form table only, as UMAP_CLUSTERING does."),

    T("collectors_curve", "COLLECTORS_CURVE", 4364,
      [STUDY, _r("counts", "aspire::analysis_counts", "study"),
       _r("md", "aspire::analysis_metadata", "study")],
      [("out", "aspire::collectors_outputs")], "study", cpus=2, memory_gb=8, hours=2,
      note=LABELS_NOTE),

    T("diversity_analysis", "DIVERSITY_ANALYSIS", 4399,
      [SURVEY, _r("counts", "amplicon::abundance_table", "survey"),
       _r("md", "aspire::analysis_metadata", "survey")],
      [("out", "aspire::diversity_outputs")], "survey", cpus=4, memory_gb=16, hours=3,
      note=LABELS_NOTE + " " + LIFT_NOTE),

    T("diversity_mito", "DIVERSITY_ANALYSIS", 4399,
      [STUDY, _r("removed", "aspire::counts_removed", "study"),
       _r("md", "aspire::analysis_metadata", "study")],
      [("out", "aspire::diversity_mito_outputs")], "study", cpus=4, memory_gb=16, hours=3,
      note=LABELS_NOTE + " The .nf's `diversity.run_mito` branch, split out so the generic "
           "row stays reachable from any count table. The mitochondrial counts are the removed "
           "counts whose reason is mitochondrial, laid out as upstream's ASV_target.mito.tsv."),

    T("indicspecies", "INDICSPECIES", 4518,
      [SURVEY, _r("md", "aspire::analysis_metadata", "survey"),
       _r("counts", "amplicon::abundance_table", "survey")],
      [("results", "aspire::indicspecies_results"),
       ("tables", "aspire::indicspecies_tables"),
       ("plots", "aspire::indicspecies_plots"),
       ("aligned", "aspire::indicspecies_aligned_plots")],
      "survey", folds=("INDICSPECIES_PLOTS@4608", "INDICSPECIES_ALIGNED_PLOTS@4792"),
      cpus=8, memory_gb=32, hours=12,
      note=LABELS_NOTE + " The .nf ran exactly two groupings as fixed channels and refused "
           "to start with fewer; here there is one results directory for any number of "
           "labels. " + LIFT_NOTE_2),

    T("measurement_association", "MEASUREMENT_ASSOCIATION", 4875,
      [SURVEY, _r("counts", "amplicon::abundance_table", "survey"),
       _r("am", "aspire::analysis_asv_meta", "survey"),
       _r("md", "aspire::analysis_metadata", "survey"),
       _r("measures", "aspire::sample_measurements", "survey")],
      [("out", "aspire::measurement_association_outputs")], "survey", cpus=2, memory_gb=8,
      note=LIFT_NOTE),

    T("clustermaps", "CLUSTERMAPS", 5324,
      [STUDY, _r("am", "aspire::analysis_asv_meta", "study"),
       _r("md", "aspire::analysis_metadata", "study"),
       _r("tables", "aspire::indicspecies_tables", "study"),
       _r("removed", "aspire::counts_removed", "study")],
      [("out", "aspire::clustermap_outputs")], "study", cpus=4, memory_gb=16, hours=3,
      note=LABELS_NOTE + " the .nf passes a boolean saying whether indicator species ran and "
           "then globs the tables out of a shared directory. The directory is "
           "the real edge, so it is what is declared."),

    T("spieceasi", "SPIECEASI", 5452,
      [SURVEY, _r("policy", "aspire::spieceasi_on", "survey"),
       _r("counts", "amplicon::abundance_table", "survey"),
       _r("keep", "aspire::indicspecies_results", "survey")],
      [("all", "aspire::network_graph_all"), ("thr", "aspire::network_graph_thr"),
       ("nf", "aspire::network_node_features")],
      "survey", cpus=16, memory_gb=64, hours=24,
      note="Force-keeps every ASV the indicator species results call significant for any label. " + LIFT_NOTE),

    T("spieceasi_external", None, None,
      [SURVEY, _r("policy", "aspire::spieceasi_off", "survey"),
       _r("x_all", "aspire::external_graph_all"),
       _r("x_thr", "aspire::external_graph_thr"),
       _r("x_nf", "aspire::external_node_features")],
      [("all", "aspire::network_graph_all"), ("thr", "aspire::network_graph_thr"),
       ("nf", "aspire::network_node_features")],
      "survey",
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

    T("collect_mags", None, None,
      [("asm", "sequences::assembly", ()),
       _r("table", "binning_local::cluster_table", "asm"),
       _r("bins", "binning_local::quality_bin_fasta", "asm"),
       _r("gffs", "binning_local::quality_bin_rrna_gff", "bins")],
      [("out", "aspire::mag_collection")], "table",
      note="no .nf process: upstream read a separate genome QC pipeline's directory. This "
           "builds that layout from the binning lane's 95% centroid bins and their barrnap GFFs."),

    T("asv_mag_link", "ASV_MAG_LINK", 5821,
      [STUDY, _r("policy", "aspire::asv_mag_link_on", "study"),
       _r("fseqs", "aspire::asv_filtered_seqs", "study"),
       _r("mags", "aspire::mag_collection")],
      [("pairing", "aspire::asv_mag_pairing"), ("out", "aspire::asv_mag_outputs")],
      "study", cpus=8, memory_gb=32, hours=8,
      note="`asv_mag_link.done` stood for two different things at its four "
           "consumers: the pairing table the network stages read by path, and "
           "the results directory the master summary scans. Both are declared. The .nf "
           "read the genomes from `--genome-qc-dir`; that layout is `aspire::mag_collection`."),

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
       _r("counts", "amplicon::abundance_table", "survey"),
       _r("md", "aspire::analysis_metadata", "survey"),
       _r("pairing", "aspire::asv_mag_pairing", "survey"),
       _r("tax", "amplicon::asv_taxonomy", "survey"),
       _r("tables", "aspire::indicspecies_tables", "survey"),
       _r("sub", "aspire::network_modules_sub", "survey"),
       _r("mall", "aspire::network_modules_all", "survey")],
      [("out", "aspire::network_outputs")], "survey", cpus=8, memory_gb=32, hours=6,
      note=LIFT_NOTE + " Reachable in principle and expensive in practice: it renders a "
           "co-occurrence network, so running it outside ASPIRE means supplying the "
           "network. That is a property of what it does, not of the gate removed."),

    T("graph_network_absent", None, None,
      [SURVEY, _r("policy", "aspire::graph_network_off", "survey")],
      [("out", "aspire::network_outputs")], "survey",
      note="the `off` arm: an empty directory, which the master summary scans and finds nothing in."),

    T("asv_mag_network", "ASV_MAG_NETWORK", 5654,
      [STUDY, _r("graph", "aspire::network_graph_all", "study"),
       _r("nf", "aspire::network_node_features", "study"),
       _r("tax", "amplicon::asv_taxonomy", "study"),
       _r("counts", "aspire::analysis_counts", "study"),
       _r("pairing", "aspire::asv_mag_pairing", "study"),
       _r("magl", "aspire::asv_mag_outputs", "study")],
      [("out", "aspire::asv_mag_network_outputs")], "study", cpus=4, memory_gb=16, hours=4,
      note="the .nf picks the unthresholded or thresholded graph by config "
           "(`asv_mag_network.graph_variant`). Ported against the "
           "unthresholded one; a variant switch would be a seventh token for "
           "something no consumer can tell apart."),

    T("module_mag_anchors", "MODULE_MAG_ANCHORS", 5705,
      [STUDY, _r("policy", "aspire::asv_mag_link_on", "study"),
       _r("mall", "aspire::network_modules_all", "study"),
       _r("nf", "aspire::network_node_features", "study"),
       _r("tax", "amplicon::asv_taxonomy", "study"),
       _r("counts", "aspire::analysis_counts", "study"),
       _r("md", "aspire::analysis_metadata", "study"),
       _r("pairing", "aspire::asv_mag_pairing", "study"),
       _r("net", "aspire::network_outputs", "study")],
      [("anchors", "aspire::module_asv_anchor_table"),
       ("summary", "aspire::module_mag_anchor_summary"),
       ("scores", "aspire::sample_module_scores"),
       ("top", "aspire::sample_top_modules"),
       ("matrix", "aspire::sample_module_matrix"),
       ("heatmaps", "aspire::sample_module_heatmaps")],
      "study", cpus=4, memory_gb=16, hours=4,
      note="Gated on the link, as the .nf's `networkEnabled && asvMagLinkEnabled` is. "
           "`net` stands for the .nf's graph-network barrier."),

    T("module_mag_anchors_absent", None, None,
      [STUDY, _r("policy", "aspire::asv_mag_link_off", "study")],
      [("anchors", "aspire::module_asv_anchor_table")], "study",
      note="the `off` arm: a header-only anchor table, so the master summary solves without a link."),

    T("master_summary", "MASTER_SUMMARY", 5763,
      [STUDY, _r("am", "aspire::analysis_asv_meta", "study"),
       _r("counts", "aspire::analysis_counts", "study"),
       _r("cmaps", "aspire::clustermap_outputs", "study"),
       _r("indic", "aspire::indicspecies_results", "study"),
       _r("net", "aspire::network_outputs", "study"),
       _r("anchors", "aspire::module_asv_anchor_table", "study"),
       _r("magl", "aspire::asv_mag_outputs", "study")],
      [("long", "aspire::master_long"), ("wide", "aspire::master_count_wide"),
       ("manifest", "aspire::master_source_manifest"),
       ("colmap", "aspire::master_column_mapping"),
       ("collisions", "aspire::master_column_collisions")],
      "study", cpus=4, memory_gb=32, hours=4,
      note="The script scans four directories for the tables in its whitelist: clustermaps, "
           "indicator species, the network's (node features and modules, which "
           "`graph_network` copies into its outputs) and the link's. Each is declared, plus "
           "the anchor table the .nf wrote into the network directory. The sankey barrier is "
           "dropped: the script reads nothing of the sankey's. Which tables merge stays a "
           "runtime whitelist, defaulted to this port's file names."),
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


def is_stub(path: Path) -> bool:
    with path.open() as f:
        return f.readline() == BANNER


def hand_written() -> list[T]:
    return [row for row in TABLE
            if (HERE / f"{row.name}.py").exists() and not is_stub(HERE / f"{row.name}.py")]


def write_stubs() -> int:
    wanted = {f"{row.name}.py" for row in TABLE}
    for path in sorted(HERE.glob("*.py")):
        if path.name.startswith("_") or path.name in wanted:
            continue
        if is_stub(path):
            path.unlink()
            print(f"  removed stale stub: {path.name}")
        else:
            print(f"  WARNING: hand-written {path.name} has no row", file=sys.stderr)
    n = 0
    for row in TABLE:
        path = HERE / f"{row.name}.py"
        if path.exists() and not is_stub(path):
            continue
        path.write_text(_stub(row))
        n += 1
    return n


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
        assert len(slots) == len(row.requires), f"[{row.name}] names a requirement slot twice"
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


# Stand-ins for a cross-file `extends:`, which test_type_hierarchy.py forbids: each aspire type
# carries the generic type's properties, and the lint checks that it still does.
CROSS_FILE_SUPERSETS = [("study_metadata", "survey"), ("analysis_counts", "abundance_table")]


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

    amplicon = DataTypeLibrary.Load(MLIB / "data_types" / "amplicon.yml")
    for name, generic in CROSS_FILE_SUPERSETS:
        missing = sorted(amplicon[generic].properties - lib[name].properties)
        assert not missing, f"[{name}] no longer satisfies [amplicon::{generic}]: missing {missing}"
    print(f"  cross-file: {len(CROSS_FILE_SUPERSETS)} property superset(s) hold")


def _signature(endpoint) -> tuple:
    return (frozenset(endpoint.properties),
            frozenset(_signature(p) for p in endpoint.parents))


def _row_signature(row: T, lib) -> tuple:
    slots = {var: (dtype, parents) for var, dtype, parents in row.requires}

    def sig(var):
        dtype, parents = slots[var]
        return (frozenset(lib.GetType(dtype).properties), frozenset(sig(p) for p in parents))

    requires = sorted((sig(var) for var in slots), key=repr)
    products = sorted((frozenset(lib.GetType(d).properties) for _v, d in row.products), key=repr)
    return requires, products, sig(row.group_by)


def check_hand_written() -> None:
    rows = hand_written()
    if not rows:
        return
    from metasmith.python_api import TransformInstanceLibrary  # noqa: E402

    lib = TransformInstanceLibrary.Load(HERE)
    # A real body adds its environments and scripts, which the table does not model.
    tooling = {frozenset(e.properties) for ns in ("env", "lib") if ns in lib.types
               for e in lib.types[ns].types.values()}
    for row in rows:
        ti = lib.GetTransform(f"{row.name}.py")
        requires = sorted((_signature(r) for r in ti.model.requires
                           if frozenset(r.properties) not in tooling), key=repr)
        products = sorted((frozenset(p.properties) for group in ti.model.produces for p in group),
                          key=repr)
        got = (requires, products, _signature(ti.group_by))
        want = _row_signature(row, lib)
        for label, g, w in zip(("requirements", "products", "group_by"), got, want):
            assert g == w, f"[{row.name}] hand-written {label} differ from its row"
    print(f"  hand-written: {len(rows)} bod(ies) match their rows")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--lint", action="store_true",
                    help="only check the existing aspire.yml, write nothing")
    args = ap.parse_args()

    if args.lint:
        check_table()
        lint_extends()
        check_hand_written()
        raise SystemExit(0)

    check_table()
    n_types = write_types()
    n_stubs = write_stubs()
    ported = sum(1 for r in TABLE if r.source)
    folded = sum(len(r.folds) for r in TABLE)
    print(f"  data_types/aspire.yml: {n_types} types")
    print(f"  transforms/aspire/:    {n_stubs} stubs written, {len(hand_written())} hand-written kept "
          f"({ported} ported .nf processes + {folded} folded into them)")
    lint_extends()

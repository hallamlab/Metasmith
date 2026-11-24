#!/usr/bin/env python3
import sys

import _authoring as A
from metasmith.python_api import DEFERRED, Spec, Template

NAME = "engcyc_metagenomics"
DESCRIPTION = """
The Engcyc metagenomics workflow from reads, in three cases planned together: short
reads, hybrid (short plus long reads) and long reads. Funnel each sample into the
case for its reads; give an SRA run accession in place of a read set if you like (a
short-read run, a long-read run, or one of each for hybrid). Delete a case you have
no samples for. Every lane is its own target; deselect a target to drop its lane.
A run keeps only its targets and deletes the rest once read. The built reference
databases are the references case's targets, so a later run finds them in the cache;
keep that case. Steps the cases share, such as every reference download, run once. Sizes are each
lane's reference download; a ~ marks an estimate. engcyc_metagenomics_lite is the
same workflow with the lite lanes, about a fifth of the reference downloads.

Assembly: short, BBDuk -> MEGAHIT; hybrid, BBDuk -> MEGAHIT draft -> OPERA-MS;
long, Filtlong -> Flye.
Binning: MetaBAT2, SemiBin2, COMEBin -> DAS Tool -> CheckM2 (~3 GB) -> quality MAGs
at >= 50% complete and <= 10% contamination.
Taxonomy: GTDB-Tk r232 on the quality MAGs (61 GB, 98 GB unpacked); rRNA scan against
GTDB r232 SSU (~2.4 GB); Metabuli on the contigs, against a GTDB r232 database the
plan builds from complete genomes, human T2T and RefSeq viral (192 GB download, 77 GB
built, 203 GB peak disk).
Read taxonomy: MetaPhlAn and StrainPhlAn (37 GB), sylph (GTDB r220 sketches, 14 GB),
Kraken2 with Bracken (standard 16 GB) and ganon2 (RefSeq archaea and bacteria, one
genome per species, ~50 GB), each in its long-read mode on long reads. Short and
hybrid also run Centrifuger (GTDB r226, 176 GB), phyloFlash (SILVA 138.2, 0.2 GB)
and inStrain on the quality MAGs. Hybrid's StrainPhlAn reads its long reads.
Gene function: DIAMOND UniRef50 (~24 GB), KOfam (~5 GB), InterProScan (~100 GB),
eggNOG (12 GB, 55 GB unpacked).
GPR table: the 4-lane table (KOfam, CLEAN, UniRef50, ProteinBERT); the DeepEC and
BiGG lane tables beside it.
Genome features: Bakta non-coding (~70 GB full DB), ARAGORN tRNA, antiSMASH
(~10 GB), mobileOG (~1 GB).
PathoFact 2.0 reports (PathoFact DB 1.1 GB, DeepARG 1.9 GB, CARD, AMRFinderPlus,
geNomad ~1.5 GB).
DRAM distill per quality MAG (DRAM forms only; dbCAN ~1 GB, MEROPS ~1 GB, VOGDB
~2 GB, RefSeq viral ~1 GB).
Resistome: RGI/CARD (~0.3 GB), AMRFinderPlus (~0.3 GB), MEGARes, BacMet, VFDB
(each < 0.1 GB), IntegronFinder, MobileElementFinder.
GEM per quality MAG: CarveMe 1.6.6 from the BiGG lane, then MEMOTE.
Viral: geNomad, VirSorter2 (~10 GB), VIBRANT (11 GB) -> CheckV (~7 GB) ->
vOTUs; DRAM-v; CCTyper; vConTACT3 (5.1 GB); iPHoP (~350 GB) with the quality MAGs
grafted into its host database; virus-host links to the quality MAGs by CCTyper spacer
BLASTn and by prophage position, unfiltered, with each spacer hit's mismatches.

Licensed lanes are not targets here. To turn one on, give its package and add its
target: PathoLogic per MAG (annotation::bin_pathway_table, with the MP4 pathway
table; licensed::pathway_tools_package, which also builds MetaCyc 27.0, plus
UniProt 2025_04 Swiss-Prot and UniRef90, ~270 GB of archives) and SignalP 6
(annotation::signalp6_results; licensed::signalp6_package). The every-lane
annotation::annotation_table includes the MetaCyc lane, so it needs the Pathway
Tools package too. CarveMe needs a growth medium: give modelling::media and
modelling::medium_name.
"""

ASSEMBLY = "sequences::assembly"

# Each entry hangs off the assembly (0) or the ORFs (ORFS).
ORFS = 3
_A, _O = [0], [ORFS]
TARGETS = [
    ASSEMBLY,                                                          # 0
    "sequences::read_qc_stats",                                        # 1
    {"type": "sequences::assembly_stats", "parents": _A},              # 2
    {"type": "sequences::assembly_orfs", "parents": _A},               # 3
    {"type": "sequences::gff", "parents": _A},                         # 4
    # binning
    {"type": "binning::das_tool_summary", "parents": _A},
    {"type": "taxonomy::checkm_stats", "parents": _A},
    {"type": "binning_local::quality_bin_fasta", "parents": _A},
    # taxonomy
    {"type": "taxonomy::gtdbtk_quality", "parents": _A},
    {"type": "taxonomy::rrna_gtdb", "parents": _A},
    # gene function, as lane tables
    *({"type": f"annotation::{lane}_lane_table", "parents": _O}
      for lane in ("uniref50", "kofam", "interproscan", "eggnog")),
    {"type": "annotation::interproscan_hits", "parents": _O},
    # lane tables the GPR tables, DRAM, PathoFact and CarveMe read
    *({"type": f"annotation::{lane}_lane_table", "parents": _O}
      for lane in ("merops", "refseq_viral", "vogdb", "dbcan", "deepec", "deeparg",
                   "pathofact_ml", "pathofact_hmm", "pbert", "clean", "bigg")),
    # the 4-lane GPR table
    {"type": "annotation::gpr_table", "parents": _O},
    # genome features
    {"type": "annotation::bakta_nc_lane_table", "parents": _A},
    {"type": "annotation::trna_gff", "parents": _A},
    {"type": "annotation::antismash_lane_table", "parents": _A},
    {"type": "annotation::mobileog_lane_table", "parents": _O},
    # PathoFact 2.0
    {"type": "annotation::pathofact_report", "parents": _A},
    {"type": "annotation::pathofact_vf", "parents": _A},
    {"type": "annotation::pathofact_tox", "parents": _A},
    {"type": "annotation::pathofact_amr", "parents": _A},
    # DRAM, per quality MAG
    {"type": "annotation::dram_metabolism_summary", "parents": _A},
    {"type": "annotation::dram_product_tsv", "parents": _A},
    # resistome
    *({"type": f"annotation::{lane}_lane_table", "parents": _O}
      for lane in ("rgi", "amrfinderplus", "megares", "bacmet", "vfdb")),
    {"type": "annotation::integronfinder_integrons", "parents": _A},
    {"type": "annotation::mobileelementfinder_results", "parents": _A},
    {"type": "annotation::instrain_genome_info", "parents": _A},
    "taxonomy::metaphlan_profile",
    "annotation::strainphlan_consensus_markers",
    "taxonomy::sylph_profile",
    "taxonomy::kraken2_report",
    "taxonomy::ganon2_report",
    "taxonomy::centrifuger_kreport",
    "taxonomy::phyloflash_summary",
    # GEM, per quality MAG
    {"type": "modelling::carveme_model", "parents": _A},
    {"type": "modelling::memote_score", "parents": _A},
    # viral
    *({"type": t, "parents": _A}
      for t in ("viromics::checkv_quality_summary", "viromics::votu_cluster_table",
                "annotation::dramv_amg_summary", "viromics::cas_operons",
                "viromics::vcontact3_assignments", "viromics::host_prediction_genus")),
    "viromics::virus_host_links",
]
assert TARGETS[ORFS]["type"] == "sequences::assembly_orfs"
METABULI = [{"type": t, "parents": _A} for t in ("taxonomy::metabuli", "taxonomy::metabuli_report")]
# inStrain, Centrifuger and phyloFlash document short reads only.
SHORT_ONLY = {"annotation::instrain_genome_info", "taxonomy::centrifuger_kreport",
              "taxonomy::phyloflash_summary"}
SHORT_TARGETS = TARGETS + METABULI


def _type(t) -> str:
    return t if isinstance(t, str) else t["type"]


# A long-read sample's case: Flye in place of BBDuk -> MEGAHIT, and each read-taxonomy
# lane's long-read transform answers the same target as its short-read one.
LONG_TARGETS = [t for t in SHORT_TARGETS if _type(t) not in SHORT_ONLY]

# A hybrid sample's case (short + long reads): the hybrid lane, MEGAHIT draft -> OPERA-MS,
# anchored to the assembly, so its assembly is the OPERA-MS assembly and every lane hanging
# off the assembly reads it.
HYBRID_TARGETS = [ASSEMBLY, {"type": "sequences::opera_ms_assembly", "parents": [0]}] + [
    t if isinstance(t, str) else t | {"parents": [p + (p >= 1) for p in t.get("parents", [])]}
    for t in SHORT_TARGETS[1:]
]
# A run keeps only its targets, so the built reference databases are targets too: a later
# run finds them in the cache. They take no sample, so they plan as a case of their own.
REFERENCES = [f"annotation::{t}" for t in (
    "amrfinderplus_db", "bacmet_diamond_db", "bakta_db", "card_db", "eggnog_data",
    "megares_diamond_db", "mobileog_diamond_db", "vfdb_diamond_db", "virsorter2_db")] + [
    f"ref::{t}" for t in (
        "antismash", "checkv_db", "dbcan_hmm", "deeparg_db", "dram_sheets", "ganon2_db", "genomad",
        "gtdb", "gtdb_rrna", "interproscan_data", "iphop_db", "kofamscan_ko_list",
        "kofamscan_profiles", "kraken2_db", "merops_diamond_db", "metabuli_ref", "metaphlan_db",
        "pathofact_db", "refseq_viral_diamond_db", "sylph_db", "uniref50_diamond_db", "vcontact3_db",
        "vibrant_db", "vogdb_hmm", "centrifuger_db", "phyloflash_db")]
CASE_TARGETS = {"short": SHORT_TARGETS, "long": LONG_TARGETS, "hybrid": HYBRID_TARGETS}

# engcyc lite: the lanes of the lite pick in docs/engcyc/DECISIONS.md (engcyc-lite).
# Each set lists what full has and lite leaves out.
LITE_DROPS = {
    "annotation::interproscan_lane_table", "annotation::interproscan_hits",
    "annotation::eggnog_lane_table", "annotation::merops_lane_table",
    "annotation::vogdb_lane_table", "annotation::dbcan_lane_table",
    "annotation::deepec_lane_table", "annotation::deeparg_lane_table",
    "annotation::pathofact_ml_lane_table", "annotation::pathofact_hmm_lane_table",
    "annotation::antismash_lane_table",
    "annotation::pathofact_report", "annotation::pathofact_vf", "annotation::pathofact_tox",
    "annotation::pathofact_amr",
    "annotation::dram_metabolism_summary", "annotation::dram_product_tsv",
    "annotation::amrfinderplus_lane_table", "annotation::megares_lane_table",
    "annotation::integronfinder_integrons", "annotation::mobileelementfinder_results",
    "annotation::strainphlan_consensus_markers", "taxonomy::ganon2_report",
    "taxonomy::centrifuger_kreport", "taxonomy::phyloflash_summary",
    "annotation::dramv_amg_summary", "viromics::cas_operons", "viromics::vcontact3_assignments",
    "viromics::host_prediction_genus", "viromics::virus_host_links",
    "taxonomy::metabuli", "taxonomy::metabuli_report",
}
LITE_REFERENCE_DROPS = {
    "annotation::amrfinderplus_db", "annotation::eggnog_data", "annotation::megares_diamond_db",
    "ref::antismash", "ref::dbcan_hmm", "ref::deeparg_db", "ref::dram_sheets", "ref::ganon2_db",
    "ref::interproscan_data", "ref::iphop_db", "ref::merops_diamond_db", "ref::metabuli_ref",
    "ref::pathofact_db", "ref::vcontact3_db", "ref::vogdb_hmm", "ref::centrifuger_db",
    "ref::phyloflash_db",
}
LITE_REFERENCES = [t for t in REFERENCES if t not in LITE_REFERENCE_DROPS]


# A hybrid sample has two BAMs, so without a target on MetaDecoder's table the planner runs
# MetaBAT2, SemiBin2 and DAS Tool once per BAM. Naming the table plans one of each.
def lite(targets: list) -> list:
    orfs = next(i for i, t in enumerate(targets) if _type(t) == "sequences::assembly_orfs")
    assert not LITE_DROPS & {_type(t) for t in targets[:orfs + 1]}
    return [t for t in targets if _type(t) not in LITE_DROPS] + [
        {"type": "binning::metadecoder_contig_to_bin_table", "parents": [0]}]


LITE_CASE_TARGETS = {case: lite(ts) for case, ts in CASE_TARGETS.items()}

# Consumed and produced by nothing, so they are given, and shared because they
# belong to no one sample.
SHARED_REFS = ("ref::mnxr_lookup", "ref::label_transfer_landmarks", "ref::lane_calibration",
               "modelling::media", "modelling::medium_name")
TYPE_LIBS = ("sequences.yml", "alignment.yml", "ref.yml", "annotation.yml", "taxonomy.yml",
             "binning.yml", "binning_local.yml", "viromics.yml", "modelling.yml", "licensed.yml",
             "ncbi.yml")
SRA = {"short": "ncbi::sra_short_read_accession", "long": "ncbi::sra_long_read_accession"}


# Each template gives the marker of its binner set; it picks the DAS Tool variant.
FULL_BINNERS, LITE_BINNERS = "metabat2_semibin2_comebin", "metabat2_semibin2_metadecoder"


def meta_path(name: str) -> str:
    return f"{name}_reads_metadata.json"


# A sample for `case`, as reads, or as SRA runs when `accessions` maps read class to run.
def add_sample(lib, study, case, name, accessions=None):
    # Kraken2 names its report from a sample name above the reads.
    parents = {lib.AddValue(f"{name}_name.txt", name, "sequences::sample_name", parents={study})}
    length = "long" if case == "long" else "short"
    parity = "single" if case == "long" else "paired"
    meta = lib.AddValue(meta_path(name), {"parity": parity, "length_class": length},
                        "sequences::read_metadata", parents=parents)
    if accessions:
        for kind, run in accessions.items():
            lib.AddValue(f"{name}_{kind}.acc", run, SRA[kind], parents={meta})
        return
    if case != "long":
        pair = lib.AddValue(f"{name}_read_pair.txt", name, "sequences::read_pair", parents={meta})
        lib.AddItem(DEFERRED, "sequences::zipped_forward_short_reads", parents={pair})
        lib.AddItem(DEFERRED, "sequences::zipped_reverse_short_reads", parents={pair})
    if case != "short":
        lib.AddItem(DEFERRED, "sequences::long_reads", parents={meta})


# The template's own inputs: one sample per case, each funnelled into its case.
DEFAULT_SAMPLES = [("short", "short_1"), ("hybrid", "hybrid_1"), ("long", "long_1")]


def _lineage(library) -> dict[str, set[str]]:
    # A re-used template library comes back as its packed dict.
    if isinstance(library, dict):
        return {p: {k.split("@", 1)[1] for k in (entry.get("parents") or {})}
                for p, entry in library["manifest"].items()}
    return {str(p): {str(m.path) for m in library.parents.get(p, [])} for p in library.manifest}


def _dtypes(library) -> dict[str, str]:
    if isinstance(library, dict):
        return {p: entry["type"] for p, entry in library["manifest"].items()}
    return {str(p): dtype for p, dtype in library.manifest.items()}


def case_rows(library, metas: list[str], shared: list[str]) -> list[str]:
    # A case lists every row of its samples rather than naming a sample type: the form
    # the GUI writes, so a GUI plan and a direct one read the same request. A sample's
    # rows are its metadata, what hangs off it, and what it hangs off short of the shared.
    parents = _lineage(library)
    children: dict[str, set[str]] = {}
    for p, ps in parents.items():
        for q in ps:
            children.setdefault(q, set()).add(p)

    def walk(start, step):
        seen, queue = set(), [start]
        while queue:
            for q in step.get(queue.pop(), ()):
                if q not in seen and q not in shared:
                    seen.add(q)
                    queue.append(q)
        return seen

    return sorted(set(metas).union(*(walk(m, children) | walk(m, parents) for m in metas)))


def template_for(name, samples=None, rebuild=False, targets=None, description=DESCRIPTION,
                 case_targets=CASE_TARGETS, references=REFERENCES,
                 binners=FULL_BINNERS) -> Template:
    # `samples` lists (case, sample name[, accessions]). With none, one case plans
    # `targets` from the whole library.
    samples = [tuple(s) + (None,) * (3 - len(s)) for s in samples or ()]

    def inputs(lib):
        for tl in TYPE_LIBS:
            lib.AddTypeLibrary(A.TYPES / tl)
        study = lib.AddValue("contig_study.json", {"logistics": "contig study"},
                             "viromics::contig_study")
        for case, sample, accessions in samples:
            add_sample(lib, study, case, sample, accessions)
        for t in SHARED_REFS:
            lib.AddItem(DEFERRED, t)
        # Opts the run into eggNOG's fetch; its content is not read.
        lib.AddValue("eggnog_source.json", {"release": "emapperdb-5.0.2"}, "annotation::eggnog_source")
        lib.AddValue(f"{binners}.json", {"binners": binners.replace("_", " ")}, f"binning::{binners}")

    input_library = A.deferred_inputs(name, inputs, rebuild=rebuild)
    shared = ["contig_study.json", "eggnog_source.json", f"{binners}.json"] + sorted(
        path for path, dtype in _dtypes(input_library).items() if dtype in SHARED_REFS)
    if not samples:
        cases = [Spec(input_library=input_library, target_types=targets)]
    else:
        order = list(dict.fromkeys(case for case, _, _ in samples))
        cases = [Spec(
            input_library=input_library, name=case,
            input_paths=case_rows(input_library, [meta_path(s) for c, s, _ in samples if c == case],
                                  shared),
            shared_input_paths=shared,
            target_types=case_targets[case] if targets is None else targets,
        ) for case in order]
        cases.append(Spec(input_library=input_library, name="references", input_paths=shared,
                          shared_input_paths=shared, target_types=references))
    return Template(
        name=name, description=description.strip(),
        cases=cases,
        transform_libraries=A.transforms(
            "logistics", "assembly", "metagenomics", "functionalAnnotation", "viromics",
            "metabolicModelling"),
        resource_libraries=[A.envs(), A.MLIB / "resources" / "lib"],
    )


def build_template(rebuild: bool = False) -> Template:
    return template_for(NAME, DEFAULT_SAMPLES, rebuild=rebuild)


if __name__ == "__main__":
    A.cli(sys.modules[__name__])

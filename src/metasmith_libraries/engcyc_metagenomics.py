#!/usr/bin/env python3
import sys

import _authoring as A
from metasmith.python_api import DEFERRED, Spec

NAME = "engcyc_metagenomics"
DESCRIPTION = """
The Engcyc metagenomics workflow from reads, one sample per read set. Every lane is
its own target; deselect a target to drop its lane. Sizes are each lane's reference
download; a ~ marks an estimate.

Assembly: BBDuk -> MEGAHIT for short reads; Flye for long reads; a hybrid sample
(short + long reads) gets the hybrid lane, MEGAHIT draft -> OPERA-MS, and its
downstream lanes read the OPERA-MS assembly.
Binning: MetaBAT2, SemiBin2, COMEBin -> DAS Tool -> CheckM2 (~3 GB) -> quality MAGs
at >= 50% complete and <= 10% contamination.
Taxonomy: GTDB-Tk r232 on the quality MAGs (61 GB, 98 GB unpacked); rRNA scan against GTDB r232
SSU (~2.4 GB).
Gene function: DIAMOND UniRef50 (~24 GB), KOfam (~5 GB), InterProScan (~100 GB),
eggNOG (12 GB, 55 GB unpacked).
GPR tables: the 4-lane table (KOfam, CLEAN, UniRef50, ProteinBERT) and the
all-reaction table (+ DeepEC, eggNOG).
Genome features: Bakta non-coding (~70 GB full DB), ARAGORN tRNA, antiSMASH
(~10 GB), mobileOG (~1 GB).
PathoFact 2.0 reports (PathoFact DB 1.1 GB, DeepARG 1.9 GB, CARD, AMRFinderPlus,
geNomad ~1.5 GB).
DRAM distill per quality MAG (DRAM forms only; dbCAN ~1 GB, MEROPS ~1 GB, VOGDB
~2 GB, RefSeq viral ~1 GB).
Resistome: RGI/CARD (~0.3 GB), AMRFinderPlus (~0.3 GB), MEGARes, BacMet, VFDB
(each < 0.1 GB), IntegronFinder, MobileElementFinder, inStrain,
MetaPhlAn/StrainPhlAn (4.8 GB).
GEM per quality MAG: CarveMe 1.6.6 from the BiGG lane, then MEMOTE.
Viral: geNomad, VirSorter2 (~10 GB), VIBRANT (11 GB) -> CheckV (~7 GB) ->
vOTUs; DRAM-v; CCTyper; vConTACT3 (5.1 GB); iPHoP (~350 GB) with the quality MAGs
grafted into its host database.

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
# The hybrid lane goes to the samples that carry long reads beside their short reads.
# Anchored to the assembly, so a hybrid sample's assembly is its OPERA-MS assembly
# and every lane hanging off the assembly reads it.
HYBRID = {"type": "sequences::opera_ms_assembly", "parents": [0],
          "when": ["sequences::long_reads", "sequences::zipped_forward_short_reads"]}

# Each entry hangs off the assembly (0), the ORFs (ORFS) or the DAS Tool bins (BINS).
ORFS, BINS = 4, 6
_A, _O, _B = [0], [ORFS], [BINS]
TARGETS = [
    ASSEMBLY,                                                          # 0
    HYBRID,                                                            # 1
    "sequences::read_qc_stats",                                        # 2
    {"type": "sequences::assembly_stats", "parents": _A},              # 3
    {"type": "sequences::assembly_orfs", "parents": _A},               # 4
    {"type": "sequences::gff", "parents": _A},                         # 5
    # binning
    {"type": "sequences::das_tool_bin_fasta", "parents": _A},          # 6
    {"type": "binning::das_tool_summary", "parents": _A},
    {"type": "taxonomy::checkm_stats", "parents": _B},
    {"type": "binning_local::quality_bin_fasta", "parents": _A},
    # taxonomy
    {"type": "taxonomy::gtdbtk_quality", "parents": _A},
    {"type": "taxonomy::rrna_gtdb", "parents": _A},
    # gene function, as lane tables
    *({"type": f"annotation::{lane}_lane_table", "parents": _O}
      for lane in ("uniref50", "kofam", "interproscan", "eggnog")),
    {"type": "annotation::interproscan_hits", "parents": _O},
    # GPR tables
    {"type": "annotation::gpr_table", "parents": _O},
    {"type": "annotation::gpr_table_all", "parents": _O},
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
    # GEM, per quality MAG
    {"type": "modelling::carveme_model", "parents": _A},
    {"type": "modelling::memote_score", "parents": _A},
    # viral
    *({"type": t, "parents": _A}
      for t in ("viromics::checkv_quality_summary", "viromics::votu_cluster_table",
                "annotation::dramv_amg_summary", "viromics::cas_operons",
                "viromics::vcontact3_assignments", "viromics::host_prediction_genus")),
]
assert TARGETS[ORFS]["type"] == "sequences::assembly_orfs"
assert TARGETS[BINS]["type"] == "sequences::das_tool_bin_fasta"

# Consumed and produced by nothing, so they are given, and shared because they
# belong to no one sample.
SHARED_REFS = ("ref::mnxr_lookup", "ref::label_transfer_landmarks", "ref::lane_calibration",
               "modelling::media", "modelling::medium_name")
TYPE_LIBS = ("sequences.yml", "alignment.yml", "ref.yml", "annotation.yml", "taxonomy.yml",
             "binning.yml", "binning_local.yml", "viromics.yml", "modelling.yml", "licensed.yml")


def add_sample(lib, study, name, long_reads=False):
    meta = lib.AddValue(f"{name}_reads_metadata.json",
                        {"parity": "paired", "length_class": "short"},
                        "sequences::read_metadata", parents={study})
    pair = lib.AddValue(f"{name}_read_pair.txt", name, "sequences::read_pair", parents={meta})
    lib.AddItem(DEFERRED, "sequences::zipped_forward_short_reads", parents={pair})
    lib.AddItem(DEFERRED, "sequences::zipped_reverse_short_reads", parents={pair})
    if long_reads:
        lib.AddItem(DEFERRED, "sequences::long_reads", parents={meta})


def short_sample(lib, study):
    add_sample(lib, study, "sample_1")


def _dtypes(library) -> dict[str, str]:
    # A re-used template library comes back as its packed dict.
    if isinstance(library, dict):
        return {p: entry["type"] for p, entry in library["manifest"].items()}
    return {str(p): dtype for p, dtype in library.manifest.items()}


def spec_for(name, samples=None, rebuild=False, targets=TARGETS) -> Spec:
    def inputs(lib):
        for tl in TYPE_LIBS:
            lib.AddTypeLibrary(A.TYPES / tl)
        study = lib.AddValue("contig_study.json", {"logistics": "contig study"},
                             "viromics::contig_study")
        if samples is not None:
            samples(lib, study)
        for t in SHARED_REFS:
            lib.AddItem(DEFERRED, t)
        # Opts the run into eggNOG's fetch; its content is not read.
        lib.AddValue("eggnog_source.json", {"release": "emapperdb-5.0.2"}, "annotation::eggnog_source")

    input_library = A.deferred_inputs(name, inputs, rebuild=rebuild)
    shared = ["contig_study.json", "eggnog_source.json"] + sorted(
        path for path, dtype in _dtypes(input_library).items() if dtype in SHARED_REFS)
    return Spec(
        input_library=input_library,
        sample_type=None if samples is None else "sequences::read_metadata",
        shared_input_paths=[] if samples is None else shared,
        target_types=targets,
        transform_libraries=A.transforms(
            "logistics", "assembly", "metagenomics", "functionalAnnotation", "viromics",
            "metabolicModelling"),
        resource_libraries=[A.envs(), A.MLIB / "resources" / "lib"],
    )


def build_spec(rebuild: bool = False) -> Spec:
    return spec_for(NAME, short_sample, rebuild=rebuild)


if __name__ == "__main__":
    A.cli(sys.modules[__name__])

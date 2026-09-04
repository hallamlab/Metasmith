#!/usr/bin/env python3
import sys

import _authoring as A
from metasmith.python_api import DEFERRED, Spec

NAME = "binning_and_gpr_from_assembly"
DESCRIPTION = """
Bin an existing assembly with three binners, filter the bins on CheckM2 quality
and dereplicate them with skani, and in the same pass build the gene-attributed
GPR evidence table from KOFAMSCAN, CLEAN, DIAMOND UniRef50 and ProteinBERT.
Takes the assembly and the long reads it was built from; the reads are what the
coverage the binners need is computed from.
"""

# Only checkm_stats is pinned, and only because all three binners' bin_fasta
# types share sequences::putative_genome: an unpinned target is satisfied by
# running CheckM2 on one binner's output, which starves the aggregator that
# needs all three. The assembly needs no pinning -- unlike
# metagenomics_from_paired_reads there is one concrete assembly here and no
# second producer to be ambiguous with.
_MB, _SB, _CB = 3, 4, 5
TARGETS = [
    "sequences::assembly_stats",
    "sequences::assembly_per_contig_coverage",
    "sequences::assembly_per_bp_coverage",
    "sequences::metabat2_bin_fasta",
    "sequences::semibin2_bin_fasta",
    "sequences::comebin_bin_fasta",
    "binning_local::quality_bin_fasta",
    "binning_local::cluster_table",
    "binning::metabat2_contig_to_bin_table",
    "binning::semibin2_contig_to_bin_table",
    "binning::comebin_contig_to_bin_table",
    "sequences::read_qc_stats",
    "annotation::gpr_table",
    # Each lane's merged output, named because gpr_4lane consuming them makes
    # them intermediates otherwise: the collect phase drops them, and none is
    # recoverable without repeating the DIAMOND pass, the HMM sweep or the
    # embedding run. The merged types, not the per-shard *_chunk variants.
    "annotation::kofamscan_results",
    "annotation::kofamscan_descriptions",
    "annotation::clean_predictions",
    "annotation::diamond_uniref50_results",
    "annotation::diamond_uniref50_descriptions",
    "annotation::proteinbert_embeddings",
    # The lane outputs are keyed on prodigal's ORF ids, so without the ORFs and
    # their coordinates they join back to neither the assembly nor each other.
    "sequences::orfs",
    "sequences::gff",
] + [
    {"type": "taxonomy::checkm_stats", "parents": [b]}
    for b in (_MB, _SB, _CB)
]


def build_spec(rebuild: bool = False) -> Spec:
    def inputs(lib):
        for tl in ("sequences.yml", "alignment.yml", "taxonomy.yml", "binning.yml",
                   "binning_local.yml", "ref.yml", "annotation.yml"):
            lib.AddTypeLibrary(A.TYPES / tl)
        meta = lib.AddValue("reads_metadata.json",
                            {"parity": "single", "length_class": "long"},
                            "sequences::read_metadata")
        lib.AddItem(DEFERRED, "sequences::assembly", parents={meta})
        lib.AddItem(DEFERRED, "sequences::long_reads", parents={meta})
        # These two have consumers and no producer anywhere in the library, so
        # they have to be given. Parenting them to the sample row is what keeps
        # them visible: a sample_type masks the library to that row's lineage,
        # and a reference with no relation to it disappears from the plan. The
        # other three GPR references (kofam profiles, kofam ko_list, uniref50)
        # do have download transforms and are deliberately left out.
        lib.AddItem(DEFERRED, "ref::mnxr_lookup", parents={meta})
        lib.AddItem(DEFERRED, "ref::label_transfer_landmarks", parents={meta})

    return Spec(
        input_library=A.deferred_inputs(NAME, inputs, rebuild=rebuild),
        sample_type="sequences::read_metadata",
        target_types=TARGETS,
        # Order decides which library answers a namespace, and metagenomics is
        # the only one carrying binning and binning_local.
        transform_libraries=A.transforms(
            "metagenomics", "assembly", "logistics", "functionalAnnotation", "fabfos"),
        resource_libraries=[A.envs(), A.MLIB / "resources" / "lib"],
    )


if __name__ == "__main__":
    A.cli(sys.modules[__name__])

#!/usr/bin/env python3
"""Author the `metagenomics_from_paired_reads` template.

The full metagenomics workflow from paired short reads:

  reads --> seqkit_reads --> read_qc_stats
  reads + read_qc_stats --> bbduk --> clean_short_reads
  clean_short_reads --> megahit --> assembly
  assembly --> prodigal --> orfs --> diamond_uniref50 + kofamscan
  assembly --> metabuli                                      (per-contig taxonomy)
  reads + assembly --> assembly_stats --> bam + per-contig/per-bp coverage
       --> metabat2 / semibin2 / comebin --> bin_fasta + contig_to_bin_table
            --> checkm2 --> aggregator --> quality_bin_fasta
                                          --> skani_dedup --> cluster_table
       (one binner's bin_fasta) --> gtdbtk                   (per-bin taxonomy)
  reads --> phyloFlash                                       (SSU rRNA taxonomy)

Every intermediate is targeted so it all appears in the graph. The external DBs
(UniRef50, KOFAM, metabuli, GTDB, phyloFlash) resolve through transforms/logistics.

    python main/metagenomics_from_paired_reads.py [--rebuild] [--dag]
"""
import sys

import _authoring as A
from metasmith.python_api import DEFERRED, Spec

NAME = "metagenomics_from_paired_reads"
DESCRIPTION = """
Full metagenomics workflow from paired short reads: QC, assembly, ORF calling,
functional annotation, three binners with quality filtering and dereplication,
and contig-, bin- and SSU-level taxonomy.
"""

# The three binners fan out per-binner checkm and gtdbtk instances. Each entry
# below is either a type name or `{type, parents:[i]}` naming an earlier entry;
# without the parent constraint the planner is free to satisfy checkm and gtdbtk
# from whichever single binner it likes.
_MB, _SB, _CB = 10, 11, 12
TARGETS = [
    "sequences::read_qc_stats",                     # 0
    "sequences::orfs",                              # 1
    "sequences::assembly_stats",                    # 2
    "sequences::assembly_per_contig_coverage",      # 3
    "sequences::assembly_per_bp_coverage",          # 4
    "annotation::diamond_uniref50_results",         # 5
    "annotation::kofamscan_results",                # 6
    "taxonomy::metabuli",                           # 7
    "taxonomy::phyloflash_summary",                 # 8
    "binning_local::cluster_table",                 # 9
    "sequences::metabat2_bin_fasta",                # 10
    "sequences::semibin2_bin_fasta",                # 11
    "sequences::comebin_bin_fasta",                 # 12
] + [
    {"type": t, "parents": [b]}
    for b in (_MB, _SB, _CB)
    for t in ("taxonomy::checkm_stats", "taxonomy::gtdbtk")
] + [
    "binning::metabat2_contig_to_bin_table",
    "binning::semibin2_contig_to_bin_table",
    "binning::comebin_contig_to_bin_table",
]


def build_spec(rebuild: bool = False) -> Spec:
    def inputs(lib):
        for tl in ("sequences.yml", "alignment.yml", "ref.yml", "annotation.yml",
                   "taxonomy.yml", "binning.yml", "binning_local.yml"):
            lib.AddTypeLibrary(A.TYPES / tl)
        # The metadata and the pair label are values, not files: they are what
        # the workflow is told about the reads, and they are known now.
        meta = lib.AddValue("reads_metadata.json",
                            {"parity": "paired", "length_class": "short"},
                            "sequences::read_metadata")
        pair = lib.AddValue("read_pair.txt", "sample_1", "sequences::read_pair",
                            parents={meta})
        lib.AddItem(DEFERRED, "sequences::zipped_forward_short_reads", parents={pair})
        lib.AddItem(DEFERRED, "sequences::zipped_reverse_short_reads", parents={pair})

    return Spec(
        input_library=A.deferred_inputs(NAME, inputs, rebuild=rebuild),
        sample_type="sequences::read_metadata",
        target_types=TARGETS,
        transform_libraries=A.transforms(
            "logistics", "assembly", "metagenomics", "functionalAnnotation"),
        resource_libraries=[A.envs()],
    )


if __name__ == "__main__":
    A.cli(sys.modules[__name__])

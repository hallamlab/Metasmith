#!/usr/bin/env python3
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
#
# The same constraint runs one level higher, and for a sharper reason: every
# assembly-derived target is pinned to the megahit assembly (index 0). Since
# `spades_assembly` joined `megahit_assembly` under `sequences::assembly`, an
# unpinned target is satisfiable from either, so the planner is free to answer
# each one from a different assembler -- it did, running BOTH and splitting the
# downstream work across them (35 steps rather than 29). Ambiguity that wide is
# also what the search pays for: unpinned, this template took ~190s and 6 GB to
# solve and blocked every other job behind the GUI's plan lock; pinned, ~3s.
_MB, _SB, _CB = 13, 14, 15
TARGETS = [
    "sequences::megahit_assembly",
    "sequences::read_qc_stats",
    {"type": "sequences::orfs", "parents": [0]},
    {"type": "sequences::assembly_stats", "parents": [0]},
    {"type": "sequences::assembly_per_contig_coverage", "parents": [0]},
    {"type": "sequences::assembly_per_bp_coverage", "parents": [0]},
    {"type": "annotation::diamond_uniref50_results", "parents": [0]},
    {"type": "annotation::diamond_uniref50_descriptions", "parents": [0]},
    {"type": "annotation::kofamscan_results", "parents": [0]},
    {"type": "annotation::kofamscan_descriptions", "parents": [0]},
    {"type": "taxonomy::metabuli", "parents": [0]},
    "taxonomy::phyloflash_summary",
    {"type": "binning_local::cluster_table", "parents": [0]},
    {"type": "sequences::metabat2_bin_fasta", "parents": [0]},
    {"type": "sequences::semibin2_bin_fasta", "parents": [0]},
    {"type": "sequences::comebin_bin_fasta", "parents": [0]},
] + [
    {"type": t, "parents": [b]}
    for b in (_MB, _SB, _CB)
    for t in ("taxonomy::checkm_stats", "taxonomy::gtdbtk")
] + [
    {"type": "binning::metabat2_contig_to_bin_table", "parents": [0]},
    {"type": "binning::semibin2_contig_to_bin_table", "parents": [0]},
    {"type": "binning::comebin_contig_to_bin_table", "parents": [0]},
]


def build_spec(rebuild: bool = False) -> Spec:
    def inputs(lib):
        for tl in ("sequences.yml", "alignment.yml", "ref.yml", "annotation.yml",
                   "taxonomy.yml", "binning.yml", "binning_local.yml"):
            lib.AddTypeLibrary(A.TYPES / tl)
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

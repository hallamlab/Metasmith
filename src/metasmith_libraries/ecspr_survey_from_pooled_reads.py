#!/usr/bin/env python3
# Every target is pinned to `putative_inserts` (index 0), not left free. Both
# `sequences::megahit_assembly` and `sequences::spades_assembly` also extend
# `sequences::assembly` and descend from this same experiment, so an unpinned
# annotation or GPR target is satisfiable from either raw assembly instead of
# the recovered inserts -- a different, and wrong, ORF set.
#
# No `sample_type`: `resolve_inserts` groups by the experiment and dedups across
# pools, and `ecspr_measure` groups by the same experiment, so splitting the
# library into per-item samples would hide the shared references from whichever
# piece needed them.
import sys

import _authoring as A
from metasmith.python_api import DEFERRED, Spec

NAME = "ecspr_survey_from_pooled_reads"
DESCRIPTION = """
Recover fosmid inserts from pooled reads, build their canonical chosen-4 GPR
evidence table, and measure ECSPr conductance over it -- saving the recovered
inserts, the per-lane annotations and the GPR table as outputs alongside the
final ecspr::results.
"""

TARGETS = [
    "fabfos::putative_inserts",
    "fabfos::insert_metadata",
    {"type": "sequences::assembly_stats", "parents": [0]},
    {"type": "annotation::kofamscan_results", "parents": [0]},
    {"type": "annotation::kofamscan_descriptions", "parents": [0]},
    {"type": "annotation::clean_predictions", "parents": [0]},
    {"type": "annotation::diamond_uniref50_results", "parents": [0]},
    {"type": "annotation::diamond_uniref50_descriptions", "parents": [0]},
    {"type": "annotation::proteinbert_embeddings", "parents": [0]},
    {"type": "annotation::gpr_table", "parents": [0]},
    {"type": "ecspr::results", "parents": [0]},
]


def build_spec(rebuild: bool = False) -> Spec:
    def inputs(lib):
        for tl in ("sequences.yml", "fabfos.yml", "algorithm.yml", "ref.yml",
                   "annotation.yml", "ecspr.yml"):
            lib.AddTypeLibrary(A.TYPES / tl)

        exp = lib.AddValue("experiment.txt", "fosmid_pool_study",
                           "fabfos::experiment")
        meta = lib.AddValue("read_metadata.json",
                            {"parity": "paired", "length_class": "short"},
                            "sequences::read_metadata", parents={exp})
        lib.AddItem(DEFERRED, "sequences::short_reads_pe", parents={meta})
        lib.AddItem(DEFERRED, "fabfos::vector_backbone", parents={exp})
        lib.AddItem(DEFERRED, "sequences::background_genome", parents={exp})
        lib.AddItem(DEFERRED, "algorithm::fabfos_recovery.py")

        lib.AddItem(DEFERRED, "ref::mnxr_lookup")
        lib.AddItem(DEFERRED, "ref::label_transfer_landmarks")

        lib.AddItem(DEFERRED, "ecspr::conditions", parents={exp})
        lib.AddItem(DEFERRED, "ecspr::atom_pairs")
        lib.AddItem(DEFERRED, "ecspr::direction_ratios")

    return Spec(
        input_library=A.deferred_inputs(NAME, inputs, rebuild=rebuild),
        sample_type=None,
        target_types=TARGETS,
        transform_libraries=A.transforms(
            "assembly", "metagenomics", "logistics", "functionalAnnotation", "fabfos"),
        resource_libraries=[A.envs(), A.MLIB / "resources" / "lib"],
    )


if __name__ == "__main__":
    A.cli(sys.modules[__name__])

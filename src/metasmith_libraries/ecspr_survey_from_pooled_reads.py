#!/usr/bin/env python3
"""Author the `ecspr_survey_from_pooled_reads` template.

The full fosmid-to-conductance chain in one spec, reusing the two narrower
templates' own wiring rather than restating it:

  [fosmid_inserts_from_pooled_reads]
  reads --> bbduk --> megahit + spades --> resolve_inserts --> putative_inserts
                                                            --> insert_metadata

  [gpr_table_from_assembly, run on putative_inserts]
  putative_inserts --> prodigal --> orfs --> kofamscan / clean / diamond_uniref50
                                          --> proteinbert
                    --> gpr_4lane --> annotation::gpr_table

  [ecspr_results_from_gpr_table]
  gpr_table + conditions + atom_pairs + direction_ratios --> ecspr_measure
                                                          --> ecspr::results

`putative_inserts` stands in for the bare `sequences::assembly` the narrower
GPR template takes -- it is a declared superset (`origin: assembler` in
`fabfos.yml`) so prodigal calls ORFs on it unchanged. `fabfos::reference_inserts`
(the length-filtered set fabfos.yml's own comment names as what ECSPr should
run against) has no producing transform in this library -- nothing here
length-filters `putative_inserts` down to it yet -- so this template runs the
mapper over the full recovered set, same as `putative_inserts` is what
`fosmid_inserts_from_pooled_reads` already ships as its own target.

TARGETS ARE ALL PINNED TO `putative_inserts` (index 0), not left free. Both
`sequences::megahit_assembly` and `sequences::spades_assembly` also extend
`sequences::assembly` and both descend from this same experiment, so an
unpinned annotation or GPR target is satisfiable from either raw assembly
instead of the recovered inserts -- a different, and wrong, ORF set. This is
the same reasoning `fosmid_inserts_from_pooled_reads` already applies to
`sequences::assembly_stats`; every lane target and the GPR table inherit it
here for the same reason `gpr_4lane` itself pins every lane to one shared
`orfs` ancestor.

`ecspr::results` is pinned to `putative_inserts` too, though indirectly: with
only one experiment in this library the `exp` pin `ecspr_measure` already
carries would be unambiguous on its own, but the explicit pin makes the same
guarantee visible in the target list rather than resting on "there happens to
be only one".

No `sample_type` -- `resolve_inserts` groups by the experiment and dedups
ACROSS pools (see `fosmid_inserts_from_pooled_reads`), and `ecspr_measure`
groups by the same experiment for the reasons `ecspr_results_from_gpr_table`
gives; splitting the library into per-item samples would hide the shared
references (backbone, host genome, GPR bridge, reference pool, atom pairs,
direction ratios) from whichever piece needed them.

    python src/metasmith_libraries/ecspr_survey_from_pooled_reads.py [--rebuild] [--dag]
"""
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

# `assembly_stats`, the four annotation lanes and the GPR table are all pinned
# to the recovered inserts (index 0) rather than left free -- see the module
# docstring. `ecspr::results` is pinned the same way for visibility, though the
# `exp` pin inside `ecspr_measure` already makes it unambiguous in this library.
TARGETS = [
    "fabfos::putative_inserts",                                    # 0
    "fabfos::insert_metadata",
    {"type": "sequences::assembly_stats", "parents": [0]},
    {"type": "annotation::kofamscan_results", "parents": [0]},
    {"type": "annotation::clean_predictions", "parents": [0]},
    {"type": "annotation::diamond_uniref50_results", "parents": [0]},
    {"type": "annotation::proteinbert_embeddings", "parents": [0]},
    {"type": "annotation::proteinbert_index", "parents": [0]},
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

        # GPR bridge + reference pool -- network-agnostic, unpinned; see
        # gpr_table_from_assembly.
        lib.AddItem(DEFERRED, "ref::mnxr_lookup")
        lib.AddItem(DEFERRED, "ref::reference_label_pool")

        # ECSPr's own inputs -- conditions are this experiment's claim about
        # what it is testing, so pinned to it; the atom-pair / direction-ratio
        # basis is frozen and unpinned, same footing as the GPR refs above.
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

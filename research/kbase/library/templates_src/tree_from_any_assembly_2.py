#!/usr/bin/env python3
"""tree_from_any_assembly_2 -- a workflow 8 public KBase narratives ran.

Reconstructed from narrative 248353/1 (untitled), whose app cells wire:

    1. RAST_SDK/annotate_genome_assembly
    2. SpeciesTreeBuilder/insert_set_of_genomes_into_species_tree

The mask `w_s0023` holds exactly those apps. Solving against the whole
generated library instead answers every target with an uploader, because 46 KBase
apps are pure sources and so are the cheapest producer of anything.
"""
import sys

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent.parent))
import _authoring as A
from metasmith.python_api import DEFERRED, Spec

NAME = "tree_from_any_assembly_2"
DESCRIPTION = """
annotate_genome_assembly -> insert_set_of_genomes_into_species_tree. Seen in 8 public narratives.
"""


def build_spec(rebuild: bool = False) -> Spec:
    def inputs(lib):
        lib.AddTypeLibrary(A.TYPES / "kbase.yml")
        study = lib.AddItem(DEFERRED, "kbase::study")
        sample = lib.AddItem(DEFERRED, "kbase::sample", parents={study})
        lib.AddItem(DEFERRED, "kbase::accepts_33", parents={sample})

    return Spec(
        input_library=A.deferred_inputs(NAME, inputs, rebuild=rebuild),
        sample_type="kbase::sample",
        target_types=["kbase::KBaseTrees_Tree", "kbase::KBaseSearch_GenomeSet"],
        transform_libraries=A.transforms("w_s0023"),
        resource_libraries=[A.envs()],
    )


if __name__ == "__main__":
    A.cli(sys.modules[__name__])

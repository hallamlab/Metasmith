#!/usr/bin/env python3
"""tree_from_any_assembly -- a workflow 11 public KBase narratives ran.

Reconstructed from narrative 253801/1 (untitled), whose app cells wire:

    1. ProkkaAnnotation/annotate_contigs
    2. SpeciesTreeBuilder/insert_set_of_genomes_into_species_tree

The mask `w_s0014` holds exactly those apps. Solving against the whole
generated library instead answers every target with an uploader, because 46 KBase
apps are pure sources and so are the cheapest producer of anything.
"""
import sys

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent.parent))
import _authoring as A
from metasmith.python_api import DEFERRED, Spec

NAME = "tree_from_any_assembly"
DESCRIPTION = """
annotate_contigs -> insert_set_of_genomes_into_species_tree. Seen in 11 public narratives.
"""


def build_spec(rebuild: bool = False) -> Spec:
    def inputs(lib):
        lib.AddTypeLibrary(A.TYPES / "kbase.yml")
        study = lib.AddItem(DEFERRED, "kbase::study")
        sample = lib.AddItem(DEFERRED, "kbase::sample", parents={study})
        lib.AddItem(DEFERRED, "kbase::accepts_40", parents={sample})

    return Spec(
        input_library=A.deferred_inputs(NAME, inputs, rebuild=rebuild),
        sample_type="kbase::sample",
        target_types=["kbase::KBaseTrees_Tree", "kbase::KBaseSearch_GenomeSet"],
        transform_libraries=A.transforms("w_s0014"),
        resource_libraries=[A.envs()],
    )


if __name__ == "__main__":
    A.cli(sys.modules[__name__])

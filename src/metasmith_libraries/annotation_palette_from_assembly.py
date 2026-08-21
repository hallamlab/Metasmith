#!/usr/bin/env python3
import sys

import _authoring as A
from metasmith.python_api import DEFERRED, Spec

NAME = "annotation_palette_from_assembly"
DESCRIPTION = """
Run the broadest feasible set of annotators against an existing assembly:
KOFAMSCAN, DIAMOND UniRef50, eggNOG-mapper and ProteinBERT on its ORFs, plus
Metabuli contig-level taxonomy.
"""


def build_spec(rebuild: bool = False) -> Spec:
    def inputs(lib):
        lib.AddTypeLibrary(A.TYPES / "sequences.yml")
        lib.AddTypeLibrary(A.TYPES / "ref.yml")
        lib.AddTypeLibrary(A.TYPES / "annotation.yml")
        lib.AddTypeLibrary(A.TYPES / "taxonomy.yml")
        lib.AddItem(DEFERRED, "sequences::assembly")
        lib.AddValue("eggnog_source.marker", "trigger", "annotation::eggnog_source")

    return Spec(
        input_library=A.deferred_inputs(NAME, inputs, rebuild=rebuild),
        sample_type="sequences::assembly",
        shared_input_paths=["eggnog_source.marker"],
        target_types=[
            "annotation::kofamscan_results",
            "annotation::kofamscan_descriptions",
            "annotation::diamond_uniref50_results",
            "annotation::diamond_uniref50_descriptions",
            "annotation::eggnog_results",
            "annotation::proteinbert_embeddings",
            "taxonomy::metabuli",
        ],
        transform_libraries=A.transforms(
            "logistics", "metagenomics", "functionalAnnotation"),
        resource_libraries=[A.envs()],
    )


if __name__ == "__main__":
    A.cli(sys.modules[__name__])

#!/usr/bin/env python3
"""Author the `annotation_palette_from_assembly` template.

  assembly --> prodigal --> orfs --> kofamscan
                                  --> diamond_uniref50
                                  --> eggnog_mapper
                                  --> proteinbert
  assembly --> metabuli                            (contig-level taxonomy)

`virsorter2` is deliberately not included: every virsorter2 transform requires
`annotation::virsorter2_db`, which has no producing transform anywhere in the
library (unlike uniref50/kofam/eggnog/metabuli, which all resolve through a
`logistics/download*.py` transform) -- including it would make this template
fail to solve.

    python main/annotation_palette_from_assembly.py [--rebuild] [--dag]
"""
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
        # A marker, never read -- downloadEggnogDB.py takes it only to trigger
        # the download once, and it has no ancestry connecting it to any one
        # sample's assembly, so it needs shared_input_paths to be visible at
        # solve time (same as pangenome::pangenome elsewhere).
        lib.AddValue("eggnog_source.marker", "trigger", "annotation::eggnog_source")

    return Spec(
        input_library=A.deferred_inputs(NAME, inputs, rebuild=rebuild),
        sample_type="sequences::assembly",
        shared_input_paths=["eggnog_source.marker"],
        target_types=[
            "annotation::kofamscan_results",
            "annotation::diamond_uniref50_results",
            "annotation::eggnog_results",
            "annotation::proteinbert_embeddings",
            "annotation::proteinbert_index",
            "taxonomy::metabuli",
        ],
        transform_libraries=A.transforms(
            "logistics", "metagenomics", "functionalAnnotation"),
        resource_libraries=[A.envs()],
    )


if __name__ == "__main__":
    A.cli(sys.modules[__name__])

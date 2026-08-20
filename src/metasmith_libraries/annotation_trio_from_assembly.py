#!/usr/bin/env python3
"""Author the `annotation_trio_from_assembly` template.

  assembly --> prodigal --> orfs --> kofamscan
                                  --> diamond_uniref50
                                  --> interproscan

The trio is the per-ORF annotation set shared by the fosmid and metagenome
palettes; the transform fan-out runs over ~5000-ORF chunks (chunkOrfsForAnnotation)
so the three tools scale past a single sample. `metabuli` is deliberately not
included here: its GTDB reference is far too large to fetch for a single
assembly run.

    python annotation_trio_from_assembly.py [--rebuild] [--dag]
"""
import sys

import _authoring as A
from metasmith.python_api import DEFERRED, Spec

NAME = "annotation_trio_from_assembly"
DESCRIPTION = """
Run the KOFAMSCAN, DIAMOND UniRef50 and InterProScan annotators against an
existing assembly.
"""


def build_spec(rebuild: bool = False) -> Spec:
    def inputs(lib):
        lib.AddTypeLibrary(A.TYPES / "sequences.yml")
        lib.AddTypeLibrary(A.TYPES / "ref.yml")
        lib.AddTypeLibrary(A.TYPES / "annotation.yml")
        lib.AddItem(DEFERRED, "sequences::assembly")

    return Spec(
        input_library=A.deferred_inputs(NAME, inputs, rebuild=rebuild),
        sample_type="sequences::assembly",
        target_types=[
            "annotation::kofamscan_results",
            "annotation::diamond_uniref50_results",
            "annotation::interproscan_results",
        ],
        transform_libraries=A.transforms(
            "logistics", "metagenomics", "functionalAnnotation"),
        resource_libraries=[A.envs()],
    )


if __name__ == "__main__":
    A.cli(sys.modules[__name__])

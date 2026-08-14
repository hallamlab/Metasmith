#!/usr/bin/env python3
"""Author the `isolate_assembly_from_long_reads` template.

  long_reads --> hifiasm_meta --> hifiasm_meta_assembly
  hifiasm_meta_assembly --> prodigal --> orfs --> diamond_uniref50 + kofamscan

`hifiasm_meta` consumes raw long reads directly -- no pre-chain like
filtlong/miniasm needed, unlike plain `hifiasm`.

    python main/isolate_assembly_from_long_reads.py [--rebuild] [--dag]
"""
import sys

import _authoring as A
from metasmith.python_api import DEFERRED, Spec

NAME = "isolate_assembly_from_long_reads"
DESCRIPTION = """
Assemble an isolate genome from long reads with hifiasm-meta, and annotate it
against UniRef50 and KOFAM.
"""


def build_spec(rebuild: bool = False) -> Spec:
    def inputs(lib):
        lib.AddTypeLibrary(A.TYPES / "sequences.yml")
        lib.AddTypeLibrary(A.TYPES / "ref.yml")
        lib.AddTypeLibrary(A.TYPES / "annotation.yml")
        lib.AddItem(DEFERRED, "sequences::long_reads")

    return Spec(
        input_library=A.deferred_inputs(NAME, inputs, rebuild=rebuild),
        sample_type="sequences::long_reads",
        target_types=[
            "sequences::hifiasm_meta_assembly",
            "annotation::diamond_uniref50_results",
            "annotation::kofamscan_results",
        ],
        transform_libraries=A.transforms(
            "logistics", "assembly", "metagenomics", "functionalAnnotation"),
        resource_libraries=[A.envs()],
    )


if __name__ == "__main__":
    A.cli(sys.modules[__name__])

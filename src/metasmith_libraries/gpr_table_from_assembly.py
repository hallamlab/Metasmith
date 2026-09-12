#!/usr/bin/env python3
import sys

import _authoring as A
from metasmith.python_api import DEFERRED, Spec

NAME = "gpr_table_from_assembly"
DESCRIPTION = """
Build the canonical chosen-4 GPR evidence table for an assembly: KOFAMSCAN,
CLEAN, DIAMOND UniRef50 and ProteinBERT folded into one gene-attributed
annotation::gpr_table.
"""


def build_spec(rebuild: bool = False) -> Spec:
    def inputs(lib):
        lib.AddTypeLibrary(A.TYPES / "sequences.yml")
        lib.AddTypeLibrary(A.TYPES / "ref.yml")
        lib.AddTypeLibrary(A.TYPES / "annotation.yml")
        lib.AddItem(DEFERRED, "sequences::assembly")
        lib.AddItem(DEFERRED, "ref::mnxr_lookup")
        lib.AddItem(DEFERRED, "ref::label_transfer_landmarks")

    return Spec(
        input_library=A.deferred_inputs(NAME, inputs, rebuild=rebuild),
        sample_type=None,
        target_types=["annotation::gpr_table"],
        transform_libraries=A.transforms(
            "metagenomics", "logistics", "functionalAnnotation", "fabfos"),
        resource_libraries=[A.envs(), A.MLIB / "resources" / "lib"],
    )


if __name__ == "__main__":
    A.cli(sys.modules[__name__])

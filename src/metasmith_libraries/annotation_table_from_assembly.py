#!/usr/bin/env python3
import sys

import _authoring as A
from metasmith.python_api import DEFERRED, Spec

NAME = "annotation_table_from_assembly"
DESCRIPTION = """
Run every annotation lane against an existing assembly and collect their calls into one
long annotation::annotation_table, one row per lane, ORF or contig feature, and term. Also
cuts the GPR evidence table over every reaction-mapping lane from it
(annotation::gpr_table_all).

Every lane includes MetaCyc, which is licensed: give the Pathway Tools installer
(licensed::pathway_tools_package), from which the MetaCyc 27.0 references are built.
"""


def build_spec(rebuild: bool = False) -> Spec:
    def inputs(lib):
        lib.AddTypeLibrary(A.TYPES / "sequences.yml")
        lib.AddTypeLibrary(A.TYPES / "ref.yml")
        lib.AddTypeLibrary(A.TYPES / "annotation.yml")
        lib.AddTypeLibrary(A.TYPES / "licensed.yml")
        lib.AddItem(DEFERRED, "sequences::assembly")
        # Consumed and produced by nothing, so they have to be given.
        lib.AddItem(DEFERRED, "ref::mnxr_lookup")
        lib.AddItem(DEFERRED, "ref::label_transfer_landmarks")
        lib.AddItem(DEFERRED, "ref::lane_calibration")
        lib.AddValue("eggnog_source.marker", "trigger", "annotation::eggnog_source")
        lib.AddItem(DEFERRED, "licensed::pathway_tools_package")

    return Spec(
        input_library=A.deferred_inputs(NAME, inputs, rebuild=rebuild),
        sample_type=None,
        target_types=["annotation::annotation_table", "annotation::gpr_table_all"],
        transform_libraries=A.transforms(
            "metagenomics", "logistics", "functionalAnnotation", "fabfos"),
        resource_libraries=[A.envs(), A.MLIB / "resources" / "lib"],
    )


if __name__ == "__main__":
    A.cli(sys.modules[__name__])

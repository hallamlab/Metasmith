#!/usr/bin/env python3
import sys

import _authoring as A
from metasmith.python_api import DEFERRED, Spec

NAME = "pangenome_heatmap_from_assembly"
DESCRIPTION = """
Build a pangenome from NCBI assembly accessions and render it as a heatmap.
"""


def build_spec(rebuild: bool = False) -> Spec:
    def inputs(lib):
        lib.AddTypeLibrary(A.TYPES / "ncbi.yml")
        lib.AddTypeLibrary(A.TYPES / "pangenome.yml")
        lib.AddTypeLibrary(A.TYPES / "sequences.yml")
        pan = lib.AddValue("pangenome.json", {"logistics": "pangenome"},
                            "pangenome::pangenome")
        nm = lib.AddItem(DEFERRED, "ncbi::genome_name", parents={pan})
        lib.AddItem(DEFERRED, "ncbi::assembly_accession", parents={nm})

    return Spec(
        input_library=A.deferred_inputs(NAME, inputs, rebuild=rebuild),
        sample_type="ncbi::assembly_accession",
        shared_input_paths=["pangenome.json"],
        target_types=["pangenome::heatmap"],
        transform_libraries=A.transforms("logistics", "pangenome"),
        resource_libraries=[A.envs(), A.MLIB / "resources" / "lib"],
    )


if __name__ == "__main__":
    A.cli(sys.modules[__name__])

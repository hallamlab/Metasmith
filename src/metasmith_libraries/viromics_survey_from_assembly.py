#!/usr/bin/env python3
import sys

import _authoring as A
from metasmith.python_api import DEFERRED, Spec

NAME = "viromics_survey_from_assembly"
DESCRIPTION = """
Call viral contigs from an assembly with VirSorter2 and geNomad, distill their
auxiliary metabolic genes with DRAM-v, and quantify crAssphage coverage in the
clean reads as a faecal-source marker.
"""

TARGETS = [
    "annotation::virsorter2_viral_sequences",
    "taxonomy::genomad_virus_summary",
    "taxonomy::genomad_plasmid_summary",
    "annotation::dramv_distill",
    "annotation::crassphage_coverage",
]


def build_spec(rebuild: bool = False) -> Spec:
    def inputs(lib):
        for tl in ("sequences.yml", "annotation.yml", "taxonomy.yml", "ref.yml"):
            lib.AddTypeLibrary(A.TYPES / tl)
        asm = lib.AddItem(DEFERRED, "sequences::assembly")
        lib.AddItem(DEFERRED, "sequences::clean_short_reads", parents={asm})

    return Spec(
        input_library=A.deferred_inputs(NAME, inputs, rebuild=rebuild),
        sample_type="sequences::assembly",
        target_types=TARGETS,
        transform_libraries=A.transforms(
            "logistics", "metagenomics", "functionalAnnotation"),
        resource_libraries=[A.envs()],
    )


if __name__ == "__main__":
    A.cli(sys.modules[__name__])

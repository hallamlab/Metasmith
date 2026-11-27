#!/usr/bin/env python3
import sys

import _authoring as A
from metasmith.python_api import DEFERRED, Spec, Template

NAME = "isolate_assembly_from_long_reads"
DESCRIPTION = """
Assemble an isolate genome from long reads with hifiasm-meta, and annotate it
against UniRef50 and KOFAM. Give the reads, or an SRA long-read run accession in
their place.
"""


def template_for(name, accession=None, rebuild=False) -> Template:
    def inputs(lib):
        for tl in ("sequences.yml", "ref.yml", "annotation.yml", "ncbi.yml"):
            lib.AddTypeLibrary(A.TYPES / tl)
        meta = lib.AddValue("isolate_reads_metadata.json",
                            {"parity": "single", "length_class": "long"}, "sequences::read_metadata")
        if accession:
            lib.AddValue("isolate.acc", accession, "ncbi::sra_long_read_accession", parents={meta})
        else:
            lib.AddItem(DEFERRED, "sequences::long_reads", parents={meta})

    return Template(
        name=name, description=DESCRIPTION.strip(),
        cases=[Spec(
            input_library=A.deferred_inputs(name, inputs, rebuild=rebuild),
            sample_type="sequences::read_metadata",
            target_types=[
                "sequences::hifiasm_meta_assembly",
                *({"type": t, "parents": [0]} for t in (
                    "annotation::diamond_uniref50_results",
                    "annotation::diamond_uniref50_descriptions",
                    "annotation::kofamscan_results",
                    "annotation::kofamscan_descriptions",
                )),
            ],
        )],
        transform_libraries=A.transforms(
            "logistics", "assembly", "isolate", "metagenomics", "functionalAnnotation"),
        resource_libraries=[A.envs()],
    )


def build_template(rebuild: bool = False) -> Template:
    return template_for(NAME, rebuild=rebuild)


if __name__ == "__main__":
    A.cli(sys.modules[__name__])

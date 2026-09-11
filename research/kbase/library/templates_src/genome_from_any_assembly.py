#!/usr/bin/env python3
"""genome_from_any_assembly -- a workflow 30 public KBase narratives ran.

Reconstructed from narrative 242005/1 (untitled), whose app cells wire:

    1. ProkkaAnnotation/annotate_contigs
    2. RAST_SDK/reannotate_microbial_genome

The mask `w_s0003` holds exactly those apps. Solving against the whole
generated library instead answers every target with an uploader, because 46 KBase
apps are pure sources and so are the cheapest producer of anything.
"""
import sys

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent.parent))
import _authoring as A
from metasmith.python_api import DEFERRED, Spec

NAME = "genome_from_any_assembly"
DESCRIPTION = """
annotate_contigs -> reannotate_microbial_genome. Seen in 30 public narratives.
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
        target_types=["kbase::KBaseGenomes_Genome"],
        transform_libraries=A.transforms("w_s0003"),
        resource_libraries=[A.envs()],
    )


if __name__ == "__main__":
    A.cli(sys.modules[__name__])

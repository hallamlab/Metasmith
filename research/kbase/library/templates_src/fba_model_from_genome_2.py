#!/usr/bin/env python3
"""fba_model_from_genome_2 -- a workflow 10 public KBase narratives ran.

Reconstructed from narrative 224682/1 (untitled), whose app cells wire:

    1. RAST_SDK/annotate_genome_assembly
    2. fba_tools/build_metabolic_model

The mask `w_s0017` holds exactly those apps. Solving against the whole
generated library instead answers every target with an uploader, because 46 KBase
apps are pure sources and so are the cheapest producer of anything.
"""
import sys

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent.parent))
import _authoring as A
from metasmith.python_api import DEFERRED, Spec

NAME = "fba_model_from_genome_2"
DESCRIPTION = """
annotate_genome_assembly -> build_metabolic_model. Seen in 10 public narratives.
"""


def build_spec(rebuild: bool = False) -> Spec:
    def inputs(lib):
        lib.AddTypeLibrary(A.TYPES / "kbase.yml")
        study = lib.AddItem(DEFERRED, "kbase::study")
        sample = lib.AddItem(DEFERRED, "kbase::sample", parents={study})
        lib.AddItem(DEFERRED, "kbase::KBaseGenomes_Genome", parents={sample})
        lib.AddItem(DEFERRED, "kbase::KBaseBiochem_Media", parents={sample})
        lib.AddItem(DEFERRED, "kbase::accepts_33", parents={sample})

    return Spec(
        input_library=A.deferred_inputs(NAME, inputs, rebuild=rebuild),
        sample_type="kbase::sample",
        target_types=["kbase::KBaseFBA_FBAModel"],
        transform_libraries=A.transforms("w_s0017"),
        resource_libraries=[A.envs()],
    )


if __name__ == "__main__":
    A.cli(sys.modules[__name__])

#!/usr/bin/env python3
"""fba_model_from_genome -- a workflow 45 public KBase narratives ran.

Reconstructed from narrative 256602/13 (untitled), whose app cells wire:

    1. fba_tools/build_metabolic_model
    2. fba_tools/run_flux_balance_analysis

The mask `w_s0002` holds exactly those apps. Solving against the whole
generated library instead answers every target with an uploader, because 46 KBase
apps are pure sources and so are the cheapest producer of anything.
"""
import sys

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent.parent))
import _authoring as A
from metasmith.python_api import DEFERRED, Spec

NAME = "fba_model_from_genome"
DESCRIPTION = """
build_metabolic_model -> run_flux_balance_analysis. Seen in 45 public narratives.
"""


def build_spec(rebuild: bool = False) -> Spec:
    def inputs(lib):
        lib.AddTypeLibrary(A.TYPES / "kbase.yml")
        study = lib.AddItem(DEFERRED, "kbase::study")
        sample = lib.AddItem(DEFERRED, "kbase::sample", parents={study})
        lib.AddItem(DEFERRED, "kbase::KBaseGenomes_Genome", parents={sample})
        lib.AddItem(DEFERRED, "kbase::KBaseBiochem_Media", parents={sample})

    return Spec(
        input_library=A.deferred_inputs(NAME, inputs, rebuild=rebuild),
        sample_type="kbase::sample",
        target_types=["kbase::KBaseFBA_FBAModel", "kbase::KBaseFBA_FBA"],
        transform_libraries=A.transforms("w_s0002"),
        resource_libraries=[A.envs()],
    )


if __name__ == "__main__":
    A.cli(sys.modules[__name__])

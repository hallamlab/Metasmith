#!/usr/bin/env python3
"""fba_from_fba_model -- a workflow 8 public KBase narratives ran.

Reconstructed from narrative 186558/1 (untitled), whose app cells wire:

    1. fba_tools/merge_metabolic_models_into_community_model
    2. fba_tools/run_flux_balance_analysis

The mask `w_s0022` holds exactly those apps. Solving against the whole
generated library instead answers every target with an uploader, because 46 KBase
apps are pure sources and so are the cheapest producer of anything.
"""
import sys

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent.parent))
import _authoring as A
from metasmith.python_api import DEFERRED, Spec

NAME = "fba_from_fba_model"
DESCRIPTION = """
merge_metabolic_models_into_community_model -> run_flux_balance_analysis. Seen in 8 public narratives.
"""


def build_spec(rebuild: bool = False) -> Spec:
    def inputs(lib):
        lib.AddTypeLibrary(A.TYPES / "kbase.yml")
        study = lib.AddItem(DEFERRED, "kbase::study")
        sample = lib.AddItem(DEFERRED, "kbase::sample", parents={study})
        lib.AddItem(DEFERRED, "kbase::KBaseFBA_FBAModel", parents={sample})

    return Spec(
        input_library=A.deferred_inputs(NAME, inputs, rebuild=rebuild),
        sample_type="kbase::sample",
        target_types=["kbase::KBaseFBA_FBA"],
        transform_libraries=A.transforms("w_s0022"),
        resource_libraries=[A.envs()],
    )


if __name__ == "__main__":
    A.cli(sys.modules[__name__])

#!/usr/bin/env python3
import sys

import _authoring as A
from metasmith.python_api import DEFERRED, Spec

NAME = "ecspr_results_from_gpr_table"
DESCRIPTION = """
Run an ECSPr ground-probe measurement over an existing GPR table: per-experiment
conditions plus the frozen atom-pair / direction-ratio reference basis, folded
into ecspr::results.
"""


def build_spec(rebuild: bool = False) -> Spec:
    def inputs(lib):
        for tl in ("fabfos.yml", "annotation.yml", "ecspr.yml"):
            lib.AddTypeLibrary(A.TYPES / tl)
        exp = lib.AddValue("experiment.txt", "ecspr_measurement", "fabfos::experiment")
        lib.AddItem(DEFERRED, "annotation::gpr_table", parents={exp})
        lib.AddItem(DEFERRED, "ecspr::conditions", parents={exp})
        lib.AddItem(DEFERRED, "ecspr::atom_pairs")
        lib.AddItem(DEFERRED, "ecspr::direction_ratios")

    return Spec(
        input_library=A.deferred_inputs(NAME, inputs, rebuild=rebuild),
        sample_type=None,
        target_types=["ecspr::results"],
        transform_libraries=A.transforms("fabfos"),
        resource_libraries=[A.envs()],
    )


if __name__ == "__main__":
    A.cli(sys.modules[__name__])

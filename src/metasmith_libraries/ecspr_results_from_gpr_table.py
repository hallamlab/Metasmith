#!/usr/bin/env python3
"""Author the `ecspr_results_from_gpr_table` template.

One `ecspr ground` measurement over an already-built GPR table:

  experiment --> gpr_table + conditions   (both per-experiment, both pinned to it)
             --> atom_pairs + direction_ratios   (frozen, network-agnostic, unpinned)
             --> ecspr_measure --> ecspr::results

`fabfos::experiment` has no producing transform -- it is the grouping root
`ecspr_measure` (`group_by=exp`) needs to know which GPR table and which
condition set belong to the SAME run, exactly as it roots
`fosmid_inserts_from_pooled_reads`. `gpr_table` and `conditions` are pinned to
it for the reason `ecspr_measure`'s own docstring gives: drop the pin and the
measurement silently measures a different experiment's table, or no experiment
at all.

`atom_pairs` and `direction_ratios` are NOT pinned to the experiment --
`ecspr::atom_pairs`'s own type doc calls them out as the frozen MetaNetX-derived
basis, static in the MNXR/MNXM id space, shared across every run rather than
rebuilt per experiment. Deferred leaf inputs, same footing as the GPR bridge
and reference pool in `gpr_table_from_assembly`.

No `sample_type`: one experiment, one measurement, and `gpr`/`conditions`
already carry the only lineage split that matters (the `exp` pin), so masking
would add nothing and would force the two unpinned refs into
`shared_input_paths` for free.

    python src/metasmith_libraries/ecspr_results_from_gpr_table.py [--rebuild] [--dag]
"""
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
        # Frozen reference basis -- see the module docstring.
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

#!/usr/bin/env python3
import sys

import _authoring as A
from metasmith.python_api import DEFERRED, Spec

NAME = "fosmid_inserts_from_pooled_reads"
DESCRIPTION = """
Recover cloned fosmid inserts from pooled short reads: host filtering, megahit
and spades per pool, then backbone-anchored insert resolution and cross-pool
dedup into a representative insert set.
"""

# `assembly_stats` is pinned to the recovered inserts rather than left free --
# unpinned, the planner is entitled to satisfy it from the raw assembly, which
# is a different number about a different thing.
TARGETS = [
    "fabfos::putative_inserts",
    "fabfos::insert_metadata",
    {"type": "sequences::assembly_stats", "parents": [0]},
]


def build_spec(rebuild: bool = False) -> Spec:
    def inputs(lib):
        for tl in ("sequences.yml", "fabfos.yml", "algorithm.yml", "ref.yml"):
            lib.AddTypeLibrary(A.TYPES / tl)
        exp = lib.AddValue("experiment.txt", "fosmid_pool_study",
                           "fabfos::experiment")
        meta = lib.AddValue("read_metadata.json",
                            {"parity": "paired", "length_class": "short"},
                            "sequences::read_metadata", parents={exp})
        lib.AddItem(DEFERRED, "sequences::short_reads_pe", parents={meta})
        lib.AddItem(DEFERRED, "fabfos::vector_backbone", parents={exp})
        lib.AddItem(DEFERRED, "sequences::background_genome", parents={exp})
        lib.AddItem(DEFERRED, "algorithm::fabfos_recovery.py")

    return Spec(
        input_library=A.deferred_inputs(NAME, inputs, rebuild=rebuild),
        sample_type=None,
        target_types=TARGETS,
        transform_libraries=A.transforms("assembly", "fabfos"),
        resource_libraries=[A.envs(), A.MLIB / "resources" / "lib"],
    )


if __name__ == "__main__":
    A.cli(sys.modules[__name__])

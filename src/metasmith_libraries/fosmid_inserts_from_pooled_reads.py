#!/usr/bin/env python3
"""Author the `fosmid_inserts_from_pooled_reads` template.

FabFos recovers cloned inserts from pooled fosmid libraries. One pool per read
set -- add rows in the GUI to grow the pool set:

  reads --> bbduk --> background_filter --> host_filtered_short_reads
  host_filtered --> megahit + spades      (both, per pool, not either/or)
  assemblies + graphs + backbone --> resolve_inserts --> putative_inserts
                                                     --> insert_metadata

`fabfos::experiment` is the root every pool hangs off. It has no producing
transform and exists only so `resolve_inserts` -- which groups by it -- can ask
for "every pool of this run", and so the backbone and host references a plan
reaches for are that run's rather than some other's.

Three inputs are deferred references rather than downloads because no transform
in the library produces them: the pCC1fos vector backbone, the host genome the
reads are filtered against, and `algorithm::fabfos_recovery.py`. The last is the
method itself, supplied as an input for the reason its type declaration gives --
it resolves against the installed `fabfos` package, so a cut or a threshold can
be inspected with `python -m fabfos.algorithm.fabfos_recovery` and no planner.

Both assemblers are named explicitly. A generic `sequences::assembly`
requirement would let the planner satisfy the graph with one of them; the
pipeline runs megahit AND spades per pool and carries both into the dedup.

    python src/metasmith_libraries/fosmid_inserts_from_pooled_reads.py [--rebuild] [--dag]
"""
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
    "fabfos::putative_inserts",                              # 0
    "fabfos::insert_metadata",
    {"type": "sequences::assembly_stats", "parents": [0]},
]


def build_spec(rebuild: bool = False) -> Spec:
    def inputs(lib):
        for tl in ("sequences.yml", "fabfos.yml", "algorithm.yml", "ref.yml"):
            lib.AddTypeLibrary(A.TYPES / tl)
        exp = lib.AddValue("experiment.txt", "fosmid_pool_study",
                           "fabfos::experiment")
        # Parity and length class are what the workflow is TOLD about the reads,
        # and they are known now -- so a value, not a deferred file.
        meta = lib.AddValue("read_metadata.json",
                            {"parity": "paired", "length_class": "short"},
                            "sequences::read_metadata", parents={exp})
        lib.AddItem(DEFERRED, "sequences::short_reads_pe", parents={meta})
        # Both hang off the experiment, not off a pool: one backbone and one
        # host per run, shared by every pool in it.
        lib.AddItem(DEFERRED, "fabfos::vector_backbone", parents={exp})
        lib.AddItem(DEFERRED, "sequences::background_genome", parents={exp})
        lib.AddItem(DEFERRED, "algorithm::fabfos_recovery.py")

    return Spec(
        input_library=A.deferred_inputs(NAME, inputs, rebuild=rebuild),
        # No sample_type: `resolve_inserts` groups by the experiment and dedups
        # ACROSS pools, so splitting the library per pool would hand it one pool
        # at a time and hide the shared references from all of them.
        sample_type=None,
        target_types=TARGETS,
        transform_libraries=A.transforms("assembly", "fabfos"),
        resource_libraries=[A.envs(), A.MLIB / "resources" / "lib"],
    )


if __name__ == "__main__":
    A.cli(sys.modules[__name__])

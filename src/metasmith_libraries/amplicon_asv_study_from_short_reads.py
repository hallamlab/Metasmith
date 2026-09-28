#!/usr/bin/env python3
# The four ASPIRE switches with downstream consumers are inputs, not
# configuration: there is no way to rebind the channel their consumers read.
# Each is a pair of mutually exclusive tokens and registering one arm selects it
# -- the losing arm's transform has zero candidates for its token slot, so the
# solver never instantiates it. They hang off the study so a driver that split the
# library by sample could not mask them out from under the stages that need them.
#
# The study is its sample sheet: the sample id first, then one categorical label per
# column. Each sample is a read_metadata under it, naming the sample, with its reads
# beneath that. The sample is paired. A single-end study registers
# `sequences::short_reads_se` under the read_metadata in place of the read pair, and the same targets solve
# without the interleave step. A study has one parity: research/aspire/
# aspire_asv_pipeline.py says why a mixed one cannot plan.
#
# `amplicon::silva_db` is deliberately NOT an input: withheld, the plan grows a
# download step for the whole SILVA bundle, which a user should not have to assemble.
import importlib.util
import sys

import _authoring as A
from metasmith.python_api import DEFERRED, Spec

NAME = "amplicon_asv_study_from_short_reads"
DESCRIPTION = """
ASPIRE amplicon study from paired or single-end short reads: fastp QC, merge and
quality filter, a study-wide UNOISE denoise to an ASV table, SILVA taxonomy,
non-target curation, read accounting with a read-fate sankey, and the core
analyses. Settings come from one aspire::params YAML; research/aspire/presets/
holds a tool-defaults and an ASPIRE preset.
"""

_spec = importlib.util.spec_from_file_location(
    "_aspire_topology", A.MLIB / "transforms" / "aspire" / "_generate.py")
_TOPOLOGY = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_TOPOLOGY)

SWITCHES = {
    "spieceasi": True,
    "network_modules": True,
    "asv_mag_link": True,
    "graph_network": True,
}

REFERENCES = [
    "aspire::mito_reference_source",
    "aspire::contaminant_reference_source",
]

TARGETS = [
    "amplicon::asv_table",
    "amplicon::asv_taxonomy",
    "aspire::counts_clean",
    "aspire::read_fate",
    "aspire::sankey_outputs",
    "aspire::analysis_metadata",
    "aspire::grouping_diagnostics_outputs",
    "aspire::collectors_outputs",
]


def build_spec(rebuild: bool = False) -> Spec:
    declared = {base for base, _ in _TOPOLOGY.POLICIES}
    unknown = set(SWITCHES) - declared
    missing = declared - set(SWITCHES)
    assert not unknown and not missing, (
        f"switch set disagrees with transforms/aspire/_generate.py: "
        f"unknown={sorted(unknown)} unset={sorted(missing)}"
    )

    def inputs(lib):
        for tl in ("aspire.yml", "amplicon.yml", "sequences.yml"):
            lib.AddTypeLibrary(A.TYPES / tl)
        study = lib.AddItem(DEFERRED, "aspire::study_metadata")
        lib.AddItem(DEFERRED, "aspire::params", parents={study})
        meta = lib.AddValue("read_metadata_1.json",
                            {"sample": "sample_1", "parity": "paired", "length_class": "short"},
                            "sequences::read_metadata", parents={study})
        pair = lib.AddValue("read_pair_1.txt", "sample_1", "sequences::read_pair",
                            parents={meta})
        lib.AddItem(DEFERRED, "sequences::zipped_forward_short_reads", parents={pair})
        lib.AddItem(DEFERRED, "sequences::zipped_reverse_short_reads", parents={pair})
        for dtype in REFERENCES:
            lib.AddItem(DEFERRED, dtype)
        for base, on in SWITCHES.items():
            arm = "on" if on else "off"
            lib.AddValue(f"policy_{base}.txt", arm, f"aspire::{base}_{arm}",
                         parents={study})

    return Spec(
        input_library=A.deferred_inputs(NAME, inputs, rebuild=rebuild),
        sample_type=None,
        target_types=TARGETS,
        transform_libraries=A.transforms("aspire", "logistics"),
        resource_libraries=[A.envs(), A.MLIB / "resources" / "lib"],
    )


if __name__ == "__main__":
    A.cli(sys.modules[__name__])

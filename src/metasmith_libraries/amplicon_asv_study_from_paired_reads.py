#!/usr/bin/env python3
# The eight ASPIRE switches with downstream consumers are inputs, not
# configuration: there is no way to rebind the channel their consumers read.
# Each is a pair of mutually exclusive tokens and registering one arm selects it
# -- the losing arm's transform has zero candidates for its token slot, so the
# solver never instantiates it. They hang off `run` so a driver that split the
# library by sample could not mask them out from under the stages that need them.
#
# `amplicon::silva_db` is deliberately NOT an input: withheld, the plan grows a
# download step for it, which is the one reference a user should not have to find.
import importlib.util
import sys

import _authoring as A
from metasmith.python_api import DEFERRED, Spec

NAME = "amplicon_asv_study_from_paired_reads"
DESCRIPTION = """
ASPIRE amplicon spine from paired short reads: QC, denoise, dereplicate,
chimera check, study-wide fan-in to an ASV count table, SILVA taxonomy,
abundance filtering and a read-fate sankey.
"""

_spec = importlib.util.spec_from_file_location(
    "_aspire_topology", A.MLIB / "transforms" / "aspire" / "_generate.py")
_TOPOLOGY = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_TOPOLOGY)

SWITCHES = {
    "augmentation": False,
    "batch_correction": False,
    "indicspecies": True,
    "spieceasi": True,
    "network_modules": True,
    "asv_mag_link": True,
    "graph_network": True,
    "sankey": True,
}

REFERENCES = [
    "aspire::sample_metadata",
    "aspire::sina_arb_reference",
    "aspire::silva_ref_taxonomy",
    "aspire::mito_reference_source",
    "aspire::contaminant_reference_source",
]

TARGETS = [
    "amplicon::asv_taxonomy",
    "aspire::counts_filtered",
    "aspire::sankey_outputs",
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
        run = lib.AddValue("run.txt", "aspire_study", "aspire::run")
        sid = lib.AddValue("sample_1.txt", "sample_1", "aspire::sample_id",
                           parents={run})
        pair = lib.AddValue("read_pair_1.txt", "sample_1", "sequences::read_pair",
                            parents={sid})
        lib.AddItem(DEFERRED, "sequences::zipped_forward_short_reads", parents={pair})
        lib.AddItem(DEFERRED, "sequences::zipped_reverse_short_reads", parents={pair})
        for dtype in REFERENCES:
            lib.AddItem(DEFERRED, dtype)
        for base, on in SWITCHES.items():
            arm = "on" if on else "off"
            lib.AddValue(f"policy_{base}.txt", arm, f"aspire::{base}_{arm}",
                         parents={run})

    return Spec(
        input_library=A.deferred_inputs(NAME, inputs, rebuild=rebuild),
        sample_type=None,
        target_types=TARGETS,
        transform_libraries=A.transforms("aspire", "logistics"),
        resource_libraries=[A.envs()],
    )


if __name__ == "__main__":
    A.cli(sys.modules[__name__])

#!/usr/bin/env python3
"""Author the `amplicon_asv_study_from_paired_reads` template.

The ASPIRE amplicon spine: reads to a taxonomy-annotated, filtered ASV table,
plus the read-fate sankey that says what each filtering stage removed.

  reads --> fastp --> denoise --> dereplicate --> chimera_check
        --> concat_fastas (fan-in over the whole study) --> counts
        --> taxonomy (SILVA) --> asv_taxonomy
        --> filter_counts --> counts_filtered
        --> sankey_outputs

ASPIRE is ONE STUDY over N samples with a hard fan-in, and `aspire::run` is what
makes that expressible: per-sample stages group by `sample_id`, the collector
groups by `run`, and it recovers each sequence's label from the `sample_id` its
fasta descends from. Add samples in the GUI to grow the study.

## The switches are inputs, not configuration

ASPIRE has ~35 toggles; the eight with downstream consumers cannot be config
here, because there is no way to rebind the channel eleven consumers read. Each
is instead a pair of mutually exclusive tokens, and registering one arm is what
selects it: the losing arm's transform has zero candidates for its token slot,
so the solver never instantiates it. That is a structural choice, not a search
preference. They hang off `run` so a driver that split the library by sample
could not mask them out from under the stages that need them.

This template registers the spine's own setting -- the two table-rewriting arms
off, the rest on -- matching `DEFAULT_ON` in the pipeline driver
(`research/aspire/aspire_asv_pipeline.py`), which is where the wider target
sets and the switch sweep live.

`amplicon::silva_db` is deliberately NOT an input: withheld, the plan grows a
download step for it, which is the one reference a user should not have to find.

    python src/metasmith_libraries/amplicon_asv_study_from_paired_reads.py [--rebuild] [--dag]
"""
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

# Read out of the topology table rather than restated, so adding a policy there
# cannot leave this template registering a token set the transforms disagree with.
_spec = importlib.util.spec_from_file_location(
    "_aspire_topology", A.MLIB / "transforms" / "aspire" / "_generate.py")
_TOPOLOGY = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_TOPOLOGY)

# Every stage on except the two that rewrite the analysis tables underneath
# their consumers.
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

# References the pipeline reads out of its config. Here they are ordinary
# inputs, which is what lets the SILVA database be withheld and downloaded.
REFERENCES = [
    "aspire::sample_metadata",
    "aspire::sina_arb_reference",
    "aspire::silva_ref_taxonomy",
    "aspire::mito_reference_source",
    "aspire::contaminant_reference_source",
]

# Three leaves that pull the spine in behind them. Kept small on purpose: the
# planner degrades once a solve carries more than a handful of DIVERGENT chains,
# and naming every leaf of a 45-process pipeline is exactly that ask.
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
        # No sample_type: the study is the unit. Splitting by sample would hand
        # the collector one sample at a time and hide the references from all
        # of them.
        sample_type=None,
        target_types=TARGETS,
        transform_libraries=A.transforms("aspire", "logistics"),
        # No aspire transform requires an env yet -- ASPIRE is 31 conda
        # environments and zero containers, and that port is separate -- but
        # transforms/logistics does, so without this the SILVA download
        # dead-ends on `env::python_for_data_science.env`.
        resource_libraries=[A.envs()],
    )


if __name__ == "__main__":
    A.cli(sys.modules[__name__])

#!/usr/bin/env python3
import sys

import _authoring as A
import _dl_embeddings as DL
from metasmith.python_api import DEFERRED, Spec

NAME = "dl_embeddings_from_orfs"
DESCRIPTION = """
Protein embeddings from an ORF FASTA: ESM-C, Ankh and ProtT5 sequence
embeddings, ESMFold structures, 3Di tokens and SaProt structure-aware
embeddings. Model weights are supplied as inputs.
"""

TARGET_KEYS = list(DL.TARGETS)


def build_spec(rebuild: bool = False) -> Spec:
    def inputs(lib):
        DL.add_inputs(lib, DEFERRED, {k: DEFERRED for k in DL.needed_weights(TARGET_KEYS)})

    return DL.spec(A.deferred_inputs(NAME, inputs, rebuild=rebuild), TARGET_KEYS)


if __name__ == "__main__":
    A.cli(sys.modules[__name__])

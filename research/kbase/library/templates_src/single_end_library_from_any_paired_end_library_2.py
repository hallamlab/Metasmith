#!/usr/bin/env python3
"""single_end_library_from_any_paired_end_library_2 -- a workflow 8 public KBase narratives ran.

Reconstructed from narrative 266084/15 (untitled), whose app cells wire:

    1. kb_uploadmethods/load_single_end_reads_from_URL
    2. kb_filtlong/run_kb_filtlong
    3. ProkkaAnnotation/annotate_contigs
    4. kb_SetUtilities/KButil_Build_GenomeSet

The mask `w_s0025` holds exactly those apps. Solving against the whole
generated library instead answers every target with an uploader, because 46 KBase
apps are pure sources and so are the cheapest producer of anything.
"""
import sys

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent.parent))
import _authoring as A
from metasmith.python_api import DEFERRED, Spec

NAME = "single_end_library_from_any_paired_end_library_2"
DESCRIPTION = """
load_single_end_reads_from_URL -> run_kb_filtlong -> annotate_contigs -> KButil_Build_GenomeSet. Seen in 8 public narratives.
"""


def build_spec(rebuild: bool = False) -> Spec:
    def inputs(lib):
        lib.AddTypeLibrary(A.TYPES / "kbase.yml")
        study = lib.AddItem(DEFERRED, "kbase::study")
        sample = lib.AddItem(DEFERRED, "kbase::sample", parents={study})
        lib.AddItem(DEFERRED, "kbase::accepts_05", parents={sample})
        lib.AddItem(DEFERRED, "kbase::accepts_12", parents={sample})
        lib.AddItem(DEFERRED, "kbase::accepts_40", parents={sample})

    return Spec(
        input_library=A.deferred_inputs(NAME, inputs, rebuild=rebuild),
        sample_type="kbase::sample",
        target_types=["kbase::KBaseAssembly_SingleEndLibrary", "kbase::KBaseSearch_GenomeSet"],
        transform_libraries=A.transforms("w_s0025"),
        resource_libraries=[A.envs()],
    )


if __name__ == "__main__":
    A.cli(sys.modules[__name__])

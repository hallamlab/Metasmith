#!/usr/bin/env python3
"""reads_set_from_any_paired_end_library -- a workflow 8 public KBase narratives ran.

Reconstructed from narrative 265326/1 (untitled), whose app cells wire:

    1. kb_uploadmethods/import_sra_as_reads_from_web
    2. kb_fastqc/runFastQC
    3. kb_trimmomatic/run_trimmomatic

The mask `w_s0024` holds exactly those apps. Solving against the whole
generated library instead answers every target with an uploader, because 46 KBase
apps are pure sources and so are the cheapest producer of anything.
"""
import sys

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent.parent))
import _authoring as A
from metasmith.python_api import DEFERRED, Spec

NAME = "reads_set_from_any_paired_end_library"
DESCRIPTION = """
import_sra_as_reads_from_web -> runFastQC -> run_trimmomatic. Seen in 8 public narratives.
"""


def build_spec(rebuild: bool = False) -> Spec:
    def inputs(lib):
        lib.AddTypeLibrary(A.TYPES / "kbase.yml")
        study = lib.AddItem(DEFERRED, "kbase::study")
        sample = lib.AddItem(DEFERRED, "kbase::sample", parents={study})
        lib.AddItem(DEFERRED, "kbase::accepts_09", parents={sample})
        lib.AddItem(DEFERRED, "kbase::accepts_10", parents={sample})

    return Spec(
        input_library=A.deferred_inputs(NAME, inputs, rebuild=rebuild),
        sample_type="kbase::sample",
        target_types=["kbase::KBaseSets_ReadsSet"],
        transform_libraries=A.transforms("w_s0024"),
        resource_libraries=[A.envs()],
    )


if __name__ == "__main__":
    A.cli(sys.modules[__name__])

#!/usr/bin/env python3
"""report_from_any_paired_end_library -- a workflow 24 public KBase narratives ran.

Reconstructed from narrative 242784/1 (untitled), whose app cells wire:

    1. SetAPI/create_sample_set
    2. kb_fastqc/runFastQC
    3. kb_hisat2/align_reads_using_hisat2
    4. kb_stringtie/run_stringtie
    5. kb_deseq/run_DESeq2
    6. FeatureSetUtils/upload_featureset_from_diff_expr

The mask `w_s0007` holds exactly those apps. Solving against the whole
generated library instead answers every target with an uploader, because 46 KBase
apps are pure sources and so are the cheapest producer of anything.
"""
import sys

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent.parent))
import _authoring as A
from metasmith.python_api import DEFERRED, Spec

NAME = "report_from_any_paired_end_library"
DESCRIPTION = """
create_sample_set -> runFastQC -> align_reads_using_hisat2 -> run_stringtie -> run_DESeq2 -> upload_featureset_from_diff_expr. Seen in 24 public narratives.
"""


def build_spec(rebuild: bool = False) -> Spec:
    def inputs(lib):
        lib.AddTypeLibrary(A.TYPES / "kbase.yml")
        study = lib.AddItem(DEFERRED, "kbase::study")
        sample = lib.AddItem(DEFERRED, "kbase::sample", parents={study})
        lib.AddItem(DEFERRED, "kbase::accepts_05", parents={sample})

    return Spec(
        input_library=A.deferred_inputs(NAME, inputs, rebuild=rebuild),
        sample_type="kbase::sample",
        target_types=["kbase::report"],
        transform_libraries=A.transforms("w_s0007"),
        resource_libraries=[A.envs()],
    )


if __name__ == "__main__":
    A.cli(sys.modules[__name__])

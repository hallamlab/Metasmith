"""Solve, without running, paired reads to DRAM-v distill over the study's vOTU representatives.

The viromics survey's inputs and libraries, plus the eggNOG trigger the annotation table's
eggnog lane needs. Prints the step count, dropped targets and the
steps the DRAM-v port added or depends on. Opens nothing and submits nothing.

    PYTHONPATH=src python research/dramv_port/probe_dramv_solve.py
"""
from __future__ import annotations

import sys
import time
from collections import Counter
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / "src" / "metasmith_libraries"))
sys.path.insert(0, str(HERE.parent / "viromics"))

import tempfile  # noqa: E402

import _authoring as A  # noqa: E402
import viromics_survey_from_paired_reads as V  # noqa: E402
from metasmith.python_api import DEFERRED, DataInstanceLibrary, Spec, record_library  # noqa: E402

DRAMV = ["annotation::dramv_votu_annotations", "annotation::dramv_amg_summary",
         "annotation::dramv_product_html", "annotation::dramv_vmag_stats"]
SHOWN = ("dramv", "RefseqViral", "refseq_viral", "DramSheets", "annotation_table", "virsorter2",
         "merge_candidate_calls", "mmseqs_votu", "prodigal", "splitContigs")


def spec():
    lib = DataInstanceLibrary(Path(tempfile.mkdtemp(prefix="msm-dramv-probe-")))
    for tl in ("sequences.yml", "alignment.yml", "ref.yml", "annotation.yml", "taxonomy.yml",
               "binning.yml", "binning_local.yml", "viromics.yml"):
        lib.AddTypeLibrary(A.TYPES / tl)
    study = lib.AddValue("contig_study.json", {"logistics": "contig study"}, "viromics::contig_study")
    # Two samples: the representatives are pooled, so one sample would hide a pairing fault.
    for i in (1, 2):
        meta = lib.AddValue(f"reads_metadata_{i}.json", {"parity": "paired", "length_class": "short"},
                            "sequences::read_metadata", parents={study})
        pair = lib.AddValue(f"read_pair_{i}.txt", f"sample_{i}", "sequences::read_pair", parents={meta})
        lib.AddItem(DEFERRED, "sequences::zipped_forward_short_reads", parents={pair})
        lib.AddItem(DEFERRED, "sequences::zipped_reverse_short_reads", parents={pair})
        # Consumed by annotation_table's pbert lane and produced by nothing.
        lib.AddItem(DEFERRED, "ref::label_transfer_landmarks", parents={meta})
        lib.AddValue(f"eggnog_source_{i}.marker", "trigger", "annotation::eggnog_source", parents={meta})
        lib.AddValue(f"metabat2_semibin2_comebin_{i}.json", {}, "binning::metabat2_semibin2_comebin", parents={meta})
    return Spec(
        input_library=record_library(lib),
        sample_type="sequences::read_metadata",
        shared_input_paths=["contig_study.json"],
        target_types=["sequences::megahit_assembly", "viromics::dereplicated_candidate_virus",
                      *({"type": t, "parents": [1]} for t in DRAMV)],
        transform_libraries=[V._assembly_without_spades()] + A.transforms(
            "logistics", "metagenomics", "functionalAnnotation", "viromics"),
        resource_libraries=[A.envs(), A.MLIB / "resources" / "lib"],
    )


def main():
    t0 = time.time()
    task = spec().Solve()
    plan = task.plan
    print(f"ok={task.ok} steps={len(plan.steps)} dropped={sorted(map(str, plan.dropped_targets))} "
          f"{time.time() - t0:.1f}s")
    picked = Counter(Path(step.transform._path).stem for step in plan.steps)
    for name, n in sorted(picked.items()):
        if any(k in name for k in SHOWN):
            print(f"  {n:3d} x {name}")
    return task.ok and not plan.dropped_targets


if __name__ == "__main__":
    sys.exit(0 if main() else 1)

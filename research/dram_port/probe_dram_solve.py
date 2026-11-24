"""Solve, without running, an assembly and its long reads to DRAM distill per quality MAG.

Prints each solve's step count, dropped targets and the steps the DRAM port added. Opens
nothing and submits nothing.

    PYTHONPATH=src python research/dram_port/probe_dram_solve.py
"""
from __future__ import annotations

import sys
import tempfile
import time
from collections import Counter
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
MLIB = REPO / "src" / "metasmith_libraries"
TYPES = MLIB / "data_types"

from metasmith.python_api import (  # noqa: E402
    DEFERRED, DataInstanceLibrary, Spec, TransformInstanceLibrary, record_library,
)

DRAM = ["annotation::dram_mag_annotations", "annotation::dram_product_html",
        "annotation::dram_product_tsv", "annotation::dram_metabolism_summary",
        "annotation::dram_genome_stats"]
LANES = ["annotation::dbcan_hmmsearch_results", "annotation::merops_diamond_results",
         "annotation::vogdb_hmmsearch_results"]
PORTED = ("dbcan", "merops", "vogdb", "Dbcan", "Merops", "Vog", "dram_distill", "DramSheets",
          "annotation_table")


def inputs():
    lib = DataInstanceLibrary(Path(tempfile.mkdtemp(prefix="msm-dram-probe-")))
    for tl in ("sequences.yml", "alignment.yml", "taxonomy.yml", "binning.yml",
               "binning_local.yml", "ref.yml", "annotation.yml"):
        lib.AddTypeLibrary(TYPES / tl)
    meta = lib.AddValue("reads_metadata.json", {"parity": "single", "length_class": "long"},
                        "sequences::read_metadata")
    lib.AddItem(DEFERRED, "sequences::assembly", parents={meta})
    lib.AddItem(DEFERRED, "sequences::long_reads", parents={meta})
    # Consumed by annotation_table's pbert lane and produced by nothing.
    lib.AddItem(DEFERRED, "ref::label_transfer_landmarks", parents={meta})
    lib.AddValue("eggnog_source.marker", "trigger", "annotation::eggnog_source", parents={meta})
    lib.AddValue("metabat2_semibin2_comebin.json", {}, "binning::metabat2_semibin2_comebin", parents={meta})
    return record_library(lib)


def spec(targets):
    return Spec(
        input_library=inputs(),
        sample_type="sequences::read_metadata",
        target_types=targets,
        transform_libraries=[MLIB / "transforms" / g for g in (
            "metagenomics", "assembly", "logistics", "functionalAnnotation", "fabfos")],
        resource_libraries=[MLIB / "resources" / "env", MLIB / "resources" / "lib"],
    )


def index(s):
    out = {}
    for p in s.transform_libraries:
        for key, tr in TransformInstanceLibrary.Load(Path(str(p))).IterateTransforms():
            out[str(tr.model.key)] = f"{Path(str(p)).name}/{key}"
    return out


def solve(label, targets):
    t0 = time.time()
    s = spec(targets)
    task = s.Solve()
    names = index(s)
    picked = Counter(names.get(str(st.transform.model.key), "?") for st in task.plan.steps)
    print(f"== {label}: ok={task.ok} steps={len(task.plan.steps)} "
          f"dropped={sorted(map(str, task.plan.dropped_targets))} {time.time() - t0:.1f}s")
    for name, n in sorted(picked.items()):
        if any(k in name for k in PORTED):
            print(f"   {n:3d} x {name}")
    return task.ok


if __name__ == "__main__":
    ok = solve("annotation table with the new lanes", ["annotation::annotation_table", *LANES])
    ok &= solve("DRAM distill per quality MAG", DRAM)
    sys.exit(0 if ok else 1)

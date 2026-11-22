"""Solve, without running, one assembly to PathoFact 2.0's reports.

Prints each solve's step count, dropped targets and the steps the PathoFact port added. Opens
nothing and submits nothing.

    PYTHONPATH=src python research/pathofact_port/probe_pathofact_solve.py
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

PATHOFACT = ["annotation::pathofact_vf", "annotation::pathofact_vf_ambiguous",
             "annotation::pathofact_tox", "annotation::pathofact_tox_ambiguous",
             "annotation::pathofact_amr", "annotation::pathofact_report"]
LANES = ["annotation::pathofact_tox_hmm", "annotation::pathofact_tox_ml",
         "annotation::deeparg_results"]
PORTED = ("pathofact", "Pathofact", "deeparg", "Deeparg", "genomad", "splitContigs",
          "annotation_table", "rgi", "amrfinder", "antismash")


def inputs():
    lib = DataInstanceLibrary(Path(tempfile.mkdtemp(prefix="msm-pathofact-probe-")))
    for tl in ("sequences.yml", "alignment.yml", "taxonomy.yml", "binning.yml",
               "binning_local.yml", "ref.yml", "annotation.yml"):
        lib.AddTypeLibrary(TYPES / tl)
    meta = lib.AddValue("reads_metadata.json", {"parity": "single", "length_class": "long"},
                        "sequences::read_metadata")
    lib.AddItem(DEFERRED, "sequences::assembly", parents={meta})
    # Consumed by annotation_table's pbert lane and produced by nothing.
    lib.AddItem(DEFERRED, "ref::label_transfer_landmarks", parents={meta})
    # Given, so the probe plans neither the UniProt 2025_04 archives nor the licensed MetaCyc build.
    for t in ("ref::uniref90_diamond_db", "ref::swissprot_diamond_db", "ref::uniprot_ec_maps",
              "ref::metacyc_protein_diamond_db", "ref::metacyc_monomer_reactions"):
        lib.AddItem(DEFERRED, t, parents={meta})
    lib.AddValue("eggnog_source.marker", "trigger", "annotation::eggnog_source", parents={meta})
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
    ok &= solve("PathoFact 2.0 reports", PATHOFACT)
    sys.exit(0 if ok else 1)

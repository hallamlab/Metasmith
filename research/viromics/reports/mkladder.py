#!/usr/bin/env python3
"""Solve the eight teaching rungs and render each one's DAG.

The rungs are not slices of one plan. Each is its own spec -- its own input
library, its own target list -- so the growth from one step to fifty-three is a
real sequence of solves, and every step in a picture was chosen by the planner
rather than placed by hand.

    1  spades only              1 step    a target is a type
    2  + read and assembly QC   4         remove an input, QC appears
    3  megahit, interleaved     5         the target names the assembler
    4  flye, long reads         3         a different input type, a different lane
    5  + binning and taxonomy  17         one target, a whole subgraph
    6  + functional panel      29         one target, four annotator lanes
    7  + viral identification  36         three callers, one frozen object
    8  + viral post-processing 53         the shipped template

    PYTHONPATH=src python research/viromics/reports/mkladder.py

Keep the basename free of dots: `render()` reads a suffix as the output format.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
MLIB = ROOT / "src" / "metasmith_libraries"
sys.path.insert(0, str(MLIB))

import _authoring as A                                                # noqa: E402
from metasmith.python_api import (DEFERRED, Spec,                     # noqa: E402
                                  TransformInstanceLibrary)
from dataclasses import replace                                       # noqa: E402
from metasmith.models.dag_renderer import NodeKind                    # noqa: E402

# The steps are the subject of these pictures, so they carry the ink and the data
# recedes -- the engine's default is the other way round, because elsewhere the
# reader is chasing a type. Applied to the renderer rather than to
# `dag_renderer.STYLES`, which is shared with the GUI and every other report.
_EMPHASIS = {
    "light": {NodeKind.TRANSFORM: "#0E141A",
              NodeKind.DATA:      "#7C8B92",
              NodeKind.TARGET:    "#48575E"},
    "dark":  {NodeKind.TRANSFORM: "#E4EAEC",
              NodeKind.DATA:      "#6E828B",
              NodeKind.TARGET:    "#9FB1B9"},
}


def emphasise_steps(r, theme: str):
    """Put the ink on the transforms, and drop the line above every label.

    The namespace line is emitted only for a `Label` that has one, and a node
    with no explicit label gets `default_label`, which derives a namespace from
    the node id -- so every node needs an explicit blank-namespace label, not
    just the ones already carrying one. `full` is preserved, so the SVG `<title>`
    tooltip still gives the qualified name.
    """
    for name, lab in r.labels.items():
        r._labels[name] = replace(lab, namespace="")
    r._theme = replace(r._theme, styles={
        kind: replace(st, text=_EMPHASIS[theme][kind])
        for kind, st in r._theme.styles.items()
    })
    return r

TYPE_LIBS = ("sequences.yml", "alignment.yml", "ref.yml", "annotation.yml",
             "taxonomy.yml", "binning.yml", "binning_local.yml", "viromics.yml")

GPR_REFS = ("ref::mnxr_lookup", "ref::label_transfer_landmarks")


def _no_spades() -> TransformInstanceLibrary:
    """The assembly library with metaSPAdes masked out.

    From rung 5 on there is a generic `sequences::assembly` slot that no target
    pins, and both assemblers satisfy it -- so the planner is free to assemble
    every sample twice. Lineage matching is ancestral and cannot separate the
    two. Masking is the only fix that does not build both lanes.
    """
    lib = TransformInstanceLibrary.Load((A.MLIB / "transforms" / "assembly").resolve())
    return lib.AsView({Path("spades.py")}, invert=True)


# ---------------------------------------------------------------- input shapes

def _clean_short(lib):
    meta = lib.AddValue("reads_metadata.json",
                        {"parity": "paired", "length_class": "short"},
                        "sequences::read_metadata")
    lib.AddItem(DEFERRED, "sequences::clean_short_reads", parents={meta})


def _raw_short(lib):
    meta = lib.AddValue("reads_metadata.json",
                        {"parity": "paired", "length_class": "short"},
                        "sequences::read_metadata")
    lib.AddItem(DEFERRED, "sequences::short_reads", parents={meta})


def _zipped_pair(lib, *, study: bool = False, gpr: bool = False):
    root = None
    if study:
        # The contentless root the per-sample viral calls are grouped under.
        root = lib.AddValue("contig_study.json", {"logistics": "contig study"},
                            "viromics::contig_study")
    meta = lib.AddValue("reads_metadata.json",
                        {"parity": "paired", "length_class": "short"},
                        "sequences::read_metadata",
                        **({"parents": {root}} if root else {}))
    pair = lib.AddValue("read_pair.txt", "sample_1", "sequences::read_pair",
                        parents={meta})
    lib.AddItem(DEFERRED, "sequences::zipped_forward_short_reads", parents={pair})
    lib.AddItem(DEFERRED, "sequences::zipped_reverse_short_reads", parents={pair})
    if gpr:
        for t in GPR_REFS:
            lib.AddItem(DEFERRED, t)


def _long(lib):
    lib.AddItem(DEFERRED, "sequences::long_reads")


# ---------------------------------------------------------------- target sets

_ASM, _FROZEN = 0, 1

_MAG = [
    "sequences::megahit_assembly",                                      # 0
    {"type": "sequences::assembly_stats", "parents": [0]},              # 1
    {"type": "binning_local::cluster_table", "parents": [0]},           # 2
    {"type": "taxonomy::metabuli", "parents": [0]},                     # 3
    # GTDB-Tk is not named. `cluster_table` reaches the aggregator, and the
    # aggregator requires one gtdbtk per bin set, so all three arrive as its
    # dependencies -- which is the rung's whole point.
]
_GPR = [{"type": "annotation::gpr_table", "parents": [0]}]

# Rung 8 is the shipped driver, and its target list is deliberately NOT rung 7's
# plus more. `taxonomy::gtdbtk` is dropped as a named target because iPHoP's
# add_to_db already collects the aggregator's quality pool by then; naming it as
# well buys a second GTDB-Tk run and nothing else.
_VIRAL_POST = [
    {"type": "viromics::contig_length_table", "parents": [_FROZEN]},
    {"type": "viromics::precluster_table", "parents": [_FROZEN]},
    {"type": "viromics::votu_cluster_table", "parents": [_FROZEN]},
    {"type": "viromics::checkv_contamination", "parents": [_FROZEN]},
    {"type": "viromics::vcontact3_network", "parents": [_FROZEN]},
    {"type": "annotation::kofamscan_descriptions", "parents": [_FROZEN]},
    {"type": "viromics::host_prediction_genome", "parents": [_FROZEN]},
    {"type": "viromics::spacer_host_links", "parents": [_FROZEN]},
    {"type": "annotation::dramv_distill", "parents": [_ASM]},
    {"type": "taxonomy::metabuli", "parents": [_ASM]},
    {"type": "annotation::dram_annotations", "parents": [_ASM]},
    {"type": "annotation::gpr_table", "parents": [_ASM]},
]

_STD = ("logistics", "assembly", "metagenomics", "functionalAnnotation")
_MASKED = ("logistics", "metagenomics", "functionalAnnotation")


RUNGS = [
    dict(key="1-spades", n=1, title="One assembler",
         caption="clean reads in, an assembly out",
         inputs=_clean_short, sample="sequences::read_metadata",
         targets=["sequences::spades_assembly"],
         libs=lambda: list(A.transforms(*_STD))),
    dict(key="2-qc", n=2, title="Quality control, unasked for",
         caption="the same target on raw reads",
         inputs=_raw_short, sample="sequences::read_metadata",
         targets=["sequences::spades_assembly",
                  {"type": "sequences::assembly_stats", "parents": [0]}],
         libs=lambda: list(A.transforms(*_STD))),
    dict(key="3-megahit", n=3, title="A different assembler",
         caption="zipped pairs in, megahit named instead of spades",
         inputs=_zipped_pair, sample="sequences::read_metadata",
         targets=["sequences::megahit_assembly",
                  {"type": "sequences::assembly_stats", "parents": [0]}],
         libs=lambda: list(A.transforms(*_STD))),
    dict(key="4-flye", n=4, title="A different read type",
         caption="long reads in, and only seqkit survives",
         inputs=_long, sample="sequences::long_reads",
         targets=["sequences::flye_assembly"],
         libs=lambda: list(A.transforms(*_STD))),
    dict(key="5-binning", n=5, title="Genome recovery and taxonomy",
         caption="one target pulls three binners, CheckM2, GTDB-Tk and skANI",
         inputs=_zipped_pair, sample="sequences::read_metadata",
         targets=_MAG,
         libs=lambda: [_no_spades()] + list(A.transforms(*_MASKED))),
    dict(key="6-function", n=6, title="The functional panel",
         caption="one target pulls four annotator lanes and their merges",
         inputs=lambda lib: _zipped_pair(lib, gpr=True),
         sample="sequences::read_metadata",
         targets=_MAG + _GPR, shared=GPR_REFS,
         libs=lambda: [_no_spades()] + list(A.transforms(*_MASKED, "fabfos")),
         res=lambda: [A.envs(), A.MLIB / "resources" / "lib"]),
    dict(key="7-viral", n=7, title="Viral identification",
         caption="three callers reconciled into one frozen object",
         inputs=lambda lib: _zipped_pair(lib, study=True, gpr=True),
         sample="sequences::read_metadata",
         targets=_MAG + _GPR + ["viromics::dereplicated_candidate_virus"],
         shared=GPR_REFS,
         libs=lambda: [_no_spades()] + list(A.transforms(*_MASKED, "fabfos", "viromics")),
         res=lambda: [A.envs(), A.MLIB / "resources" / "lib"]),
    dict(key="8-post", n=8, title="Everything after the freeze",
         caption="nothing below the frozen set writes sequence",
         inputs=lambda lib: _zipped_pair(lib, study=True, gpr=True),
         sample="sequences::read_metadata",
         targets=["sequences::megahit_assembly",
                  "viromics::dereplicated_candidate_virus"] + _VIRAL_POST,
         shared=GPR_REFS,
         libs=lambda: [_no_spades()] + list(A.transforms(*_MASKED, "fabfos", "viromics")),
         res=lambda: [A.envs(), A.MLIB / "resources" / "lib"]),
]


def solve(rung: dict):
    def build(lib):
        for t in TYPE_LIBS:
            lib.AddTypeLibrary(A.TYPES / t)
        rung["inputs"](lib)

    il = A.deferred_inputs(f"_ladder_{rung['key']}", build, rebuild=True)
    # A deferred item with no parents belongs to no sample, and `sample_type`
    # masks the library per sample -- so the solver never sees it and drops
    # EVERY target, not just the unreachable one. Its path is minted, so it can
    # only be named shared by reading it back off the manifest.
    want = set(rung.get("shared", ()))
    shared = sorted(str(p) for p, dt in getattr(il, "manifest", {}).items()
                    if dt in want)
    assert len(shared) == len(want), f"{rung['key']}: shared {shared}"

    spec = Spec(input_library=il, sample_type=rung["sample"],
                shared_input_paths=shared, target_types=rung["targets"],
                transform_libraries=rung["libs"](),
                resource_libraries=(rung.get("res") or (lambda: [A.envs()]))())
    task = spec.Solve(max_iter=1024, max_refine=256, seed=42)
    plan = task.plan
    assert task.ok and not plan.dropped_targets, (
        f"rung {rung['n']} does not solve: dropped {sorted(plan.dropped_targets)}")
    return plan


def main() -> int:
    index = []
    for rung in RUNGS:
        plan = solve(rung)
        sizes = {}
        for theme in ("light", "dark"):
            r = plan.BuildDAG(colour="module", theme=theme, background=False,
                              show_step_order=True)
            emphasise_steps(r, theme)
            out = Path(r.render(str(HERE / f"rung-{rung['key']}-{theme}"), "svg"))
            m = re.search(r'width="(\d+)" height="(\d+)"', out.read_text()[:400])
            sizes[theme] = (int(m.group(1)), int(m.group(2)))
        w, h = sizes["light"]
        index.append(dict(key=rung["key"], n=rung["n"], title=rung["title"],
                          caption=rung["caption"], steps=len(plan.steps),
                          targets=len(rung["targets"]), width=w, height=h,
                          tools=[Path(s.transform._path).stem for s in plan.steps]))
        print(f"  rung {rung['n']} {rung['title']:<32} {len(plan.steps):>2} steps  {w}x{h}")

    (HERE / "ladder-index.json").write_text(json.dumps(index, indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main())

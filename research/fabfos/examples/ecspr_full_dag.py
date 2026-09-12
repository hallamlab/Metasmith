#!/usr/bin/env python3
"""Render the FULL ECSPr workflow DAG: reference inserts -> significance.

This is the method's shape, drawn by the planner rather than by hand. It is
also the worked template for calling metasmith directly when the `fabfos`
CLI's single canonical path is not what you want -- it stages typed inputs,
picks the lanes, builds a target set, and asks the planner to resolve it.

Run it:

    PYTHONPATH=src python examples/ecspr_full_dag.py --dag reports/dag/ecspr_full

Planning does NOT require the staged files to exist; the planner resolves on
types and lineage. So this renders on a machine that has none of the 46 GB.
A real run obviously does need them.

The lane is selected here, and the choice is deliberate rather than
structural: `ecsprAtomB` builds the atom-transfer graph from the experiment's
own annotation evidence. THREE domains produce `ecspr::atom_graph` --
`ecsprAtomB`, `ecsprAtomA` (a curated GEM) and `ecsprAtomGPR` (a GEM plus an
active gene set) -- so loading two would let the planner pick by tiebreak.
Which lane runs is a claim about the method, so it is made here, explicitly,
and recorded in the method id.

The STAR lane was retired on 2026-07-20. On that topology a metabolite was a
single node joined to reaction-node HUBS, and eliminating a reaction node --
which is what the Woodbury update does -- left a CLIQUE over its participants,
so two participants sharing no atom still got a conductance. Here a node IS an
atom and an edge IS an atom transfer.

Where each staged input comes from is reported at the end, split into items
resolved through the data library and items still resolved from an absolute
path in the incumbent tree. That second list is the honest remaining gap
between this method and one that runs on another machine.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "src"))

from fabfos import canon  # noqa: E402
from fabfos.pipelines.common import resolve_library_root  # noqa: E402

from metasmith.python_api import (  # noqa: E402
    Agent, Runtime, DataInstanceLibrary, DataTypeLibrary, Source,
    TargetBuilder, TransformInstanceLibrary,
)
from metasmith.python_api import record_library

LIB = resolve_library_root()

DOMAINS = ["fabfos", "functionalAnnotation", "ecspr", "ecsprAtomB", "ecsprGround"]

FROM_LIBRARY: dict[str, str] = {
    "ecspr::metanetx_reac_prop": "REAC_PROP",
    "ecspr::atom_pairs": "REFERENCE_ATOM_PAIRS",
    "ecspr::direction_ratios": "REFERENCE_DIRECTION",
}

INCUMBENT = Path("/home/tony/agentic_workspace/data/scadc")
MM = Path("/home/tony/agentic_workspace/projects/scadc/metabolic-modelling/main/metabolic-modelling")
RN_CACHE = MM / "04_reaction_network" / "cache"

FROM_INCUMBENT: dict[str, Path] = {
    "ecspr::metanetx_chem_prop": INCUMBENT / "references/metanetx/chem_prop.tsv",
    "ecspr::biomass_axes": Path(canon.AXES_JSON),
    "functional_annotation::ko_to_mnxr": MM / "_reference_try1/betweenness/cache/ko_to_mnxr.tsv",
    "functional_annotation::metanetx_reac_xref": INCUMBENT / "references/metanetx/reac_xref.tsv",
    "functional_annotation::rhea2uniprot": INCUMBENT / "references/rhea/rhea2uniprot.tsv",
    "functional_annotation::rhea2uniprot_trembl": INCUMBENT / "references/rhea/rhea2uniprot_trembl.tsv.gz",
    "functional_annotation::kofam_profiles": INCUMBENT / "references/kofam/profiles",
    "functional_annotation::kofam_ko_list": INCUMBENT / "references/kofam/ko_list",
    "functional_annotation::uniref50_dmnd": INCUMBENT / "references/uniref50/uniref50.dmnd",
    "functional_annotation::evidence_source": INCUMBENT / "fabfos_2026/evidence_source.txt",
    "fabfos::reference_inserts": INCUMBENT / "fabfos_2026/putative_inserts_132_ge29kb.fna",
}


def curated_dir(staging: Path, name: str, files: list[Path]) -> Path:
    d = staging / "refs" / name
    d.mkdir(parents=True, exist_ok=True)
    for f in files:
        link = d / f.name
        if link.is_symlink() or link.exists():
            link.unlink()
        link.symlink_to(f)
    return d


def build_inputs(staging: Path) -> tuple[DataInstanceLibrary, list[str], list[str]]:
    import shutil

    xgdb = staging / "inputs.xgdb"
    if xgdb.exists():
        shutil.rmtree(xgdb)
    inputs = DataInstanceLibrary(xgdb)
    inputs.Purge()
    for ns in ("sequences", "fabfos", "functional_annotation", "ecspr"):
        inputs.AddTypeLibrary(namespace=ns, lib=DataTypeLibrary.Load(LIB / f"data_types/{ns}.yml"))

    exp = inputs.AddValue("experiment.txt", "fabfos_ecspr_canonical",
                          "fabfos::experiment")

    via_library, via_incumbent = [], []
    seen: dict[Path, str] = {}
    for type_name, symbol in FROM_LIBRARY.items():
        p = Path(getattr(canon, symbol))
        if p in seen:
            alias = staging / "refs" / "aliases" / type_name.replace("::", "__")
            alias.parent.mkdir(parents=True, exist_ok=True)
            if alias.is_symlink() or alias.exists():
                alias.unlink()
            alias.symlink_to(p)
            p = alias
        else:
            seen[p] = type_name
        inputs.AddItem(p, type_name)
        via_library.append(type_name)

    PER_EXPERIMENT = {
        "fabfos::reference_inserts",
        "functional_annotation::evidence_source",
    }
    for type_name, path in FROM_INCUMBENT.items():
        if type_name in PER_EXPERIMENT:
            inputs.AddItem(path, type_name, parents={exp})
        else:
            inputs.AddItem(path, type_name)
        via_incumbent.append(type_name)

    nulls = curated_dir(staging, "ground_null", canon.ground_null_paths())
    inputs.AddItem(nulls, "ecspr::ground_null")
    via_library.append("ecspr::ground_null")


    # Synthetic placeholders, authored here and used nowhere else, so
    # writing them down is the whole of their record.
    inputs = record_library(inputs)
    return inputs, via_library, via_incumbent


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dag", default="reports/dag/ecspr_full",
                    help="path base for the rendered SVG")
    ap.add_argument("--staging", default=None)
    a = ap.parse_args()

    staging = Path(a.staging).resolve() if a.staging else REPO / ".awm" / "data" / "runs" / "ecspr_dag"
    staging.mkdir(parents=True, exist_ok=True)

    print("=== staging typed inputs ===")
    inputs, via_library, via_incumbent = build_inputs(staging)

    print("=== resources & transforms ===")
    resources = [DataInstanceLibrary.Load(LIB / f"resources/{n}") for n in ("containers", "envs", "lib")]
    transforms = [TransformInstanceLibrary.Load(LIB / f"transforms/{d}") for d in DOMAINS]

    agent = Agent(home=Source.FromLocal(staging / "agent_home"), runtime=Runtime.APPTAINER)

    print("=== planning ===")
    targets = TargetBuilder()
    targets.Add("ecspr::ground_probe_report")
    targets.Add("ecspr::ground_significance")
    task = agent.GenerateWorkflow(
        samples=list(inputs.AsSamples("fabfos::experiment")),
        resources=resources + [inputs],
        transforms=transforms,
        targets=targets,
    )

    if not task.ok:
        print("\nPLAN DID NOT RESOLVE. Planner hints:", file=sys.stderr)
        hints = task.plan.RenderHints() if hasattr(task.plan, "RenderHints") else task
        print(hints, file=sys.stderr)
        return 1

    print(f"\nresolved workflow: {len(task.plan.steps)} steps")
    for i, step in enumerate(task.plan.steps):
        name = getattr(getattr(step, "transform", None), "name", None) or f"step{i}"
        print(f"  [{i}] {name}")

    base = (REPO / a.dag).resolve() if not Path(a.dag).is_absolute() else Path(a.dag)
    base.parent.mkdir(parents=True, exist_ok=True)
    task.plan.RenderDAG(base)
    out = base.with_suffix(".svg")
    print(f"\nDAG -> {out}")

    print(f"\n--- {len(via_library)} inputs resolved through the data library ---")
    for t in sorted(via_library):
        print(f"  lib   {t}")
    print(f"\n--- {len(via_incumbent)} inputs still on an absolute incumbent path ---")
    print("    (each of these is a reason this method is not yet portable)")
    for t in sorted(via_incumbent):
        print(f"  ABS   {t}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

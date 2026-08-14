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

LIB = resolve_library_root()

# The lanes. Both members of each pair produce the same types; loading both
# would make the planner choose by tiebreak rather than by intent.
DOMAINS = ["fabfos", "functionalAnnotation", "ecspr", "ecsprAtomB", "ecsprGround"]

# Staged inputs that the data library carries, addressed through canon so that
# no absolute path appears here.
FROM_LIBRARY: dict[str, str] = {
    "ecspr::metanetx_reac_prop": "REAC_PROP",
    # The atom-transfer universe. `ecspr::reaction_roles` used to sit beside this
    # -- the star needed it, parsed out of reac_prop.tsv, to know which side of a
    # reaction a metabolite sat on. The atom-pair table already names substrate and
    # product per transfer, so the question disappeared with the topology.
    "ecspr::atom_pairs": "REFERENCE_ATOM_PAIRS",
    "ecspr::direction_ratios": "REFERENCE_DIRECTION",
}

# Staged inputs the data library does NOT yet declare, so they are still
# addressed by absolute path in the incumbent tree. Every entry here is a
# reason this method is not yet portable -- see the report this script prints.
INCUMBENT = Path("/home/tony/agentic_workspace/data/scadc")
MM = Path("/home/tony/agentic_workspace/projects/scadc/metabolic-modelling/main/metabolic-modelling")
RN_CACHE = MM / "04_reaction_network" / "cache"

FROM_INCUMBENT: dict[str, Path] = {
    "ecspr::metanetx_chem_prop": INCUMBENT / "references/metanetx/chem_prop.tsv",
    # canon.AXES_JSON, NOT a hand-written filename. This used to name a retired
    # axis set directly -- one that canon.RETIRED_AXIS_SETS lists and
    # canon.assert_canonical_axes() rejects. The cache dir holds the canonical
    # set and the retired ones side by side, so the wrong one is one typo away
    # and nothing downstream would have complained.
    "ecspr::biomass_axes": Path(canon.AXES_JSON),
    # NOT `ecspr::direction_ratios` -- the benchmark-v3 side added it here as an
    # incumbent absolute path, but it is already staged through the library above
    # as REFERENCE_DIRECTION, which is the copy canon pins by sha256.
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
    """A DIRECTORY-typed input, built from an EXPLICIT file list.

    Never point these at a raw cache. The significance scorer discovers its
    draw sizes by listing this directory and regex-matching filenames, so a
    cache carrying retired sizes would silently widen the null basis and
    change every answer without any error.
    """
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

    # The sample root. Nothing produces fabfos::experiment -- 24
    # transforms require it as their grouping key, so it is staged, and it is
    # what AsSamples() roots on.
    exp = inputs.AddValue("experiment.txt", "fabfos_ecspr_canonical",
                          "fabfos::experiment")

    via_library, via_incumbent = [], []
    seen: dict[Path, str] = {}
    for type_name, symbol in FROM_LIBRARY.items():
        p = Path(getattr(canon, symbol))
        # ecspr::reaction_roles and ecspr::metanetx_reac_prop are the SAME
        # bytes under two types, and a DataInstanceLibrary keys its manifest by
        # path -- `AddItem` asserts on a repeat. So the second type gets a
        # curated alias rather than the original path. The alias is a symlink,
        # so it is still the same bytes and still the same sha256; only the
        # manifest key differs.
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

    # Per-experiment inputs must be PARENTED to the experiment root. A
    # transform that asks for one with parents={exp} will not match an
    # unparented instance, and the symptom is an unsatisfiable plan rather than
    # a type error -- compile_evidence fails this way on evidence_source.
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

    # The ground null, curated by canon's EXPLICIT file list -- never a glob. The
    # scorer derives its draw sizes by listing this directory, so the list IS the
    # basis. Unlike the retired ecspr::frozen_null, this type HAS a producer
    # (ecsprGround/null.py) -- that missing producer was a named Known gap.
    #
    # NOT existence-checked here, unlike the frozen null this replaces: the ground
    # null has a producer but has not been produced, so a check would hard-fail the
    # example for everyone. The experiment spec's preflight is where a RUN refuses,
    # naming the missing draw size rather than interpolating across it.
    nulls = curated_dir(staging, "ground_null", canon.ground_null_paths())
    inputs.AddItem(nulls, "ecspr::ground_null")
    via_library.append("ecspr::ground_null")

    # ecspr::frozen_null was staged here until the star-lane retirement. Its
    # lesson carries over and is why ground_null_paths() returns JOINED paths:
    # canon.FROZEN_NULL_FILES were bare filenames, and passing them to
    # symlink_to() unjoined produced ten links pointing at nothing. The planner
    # resolves on TYPES rather than existence, so the plan rendered perfectly and
    # the breakage only surfaced inside a scoring container.

    inputs.Save()
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
    # Both artifacts, because they are different questions and only one of them
    # needs the built graph. `ground_significance` runs off atom_pairs +
    # evidence_weights directly -- it must REBUILD per unit, so it needs the
    # ingredients, not the built object -- and would therefore never select the
    # atom_graph builder on its own. `ground_probe_report` is what describes the
    # base network, and it is the step that exercises `ecsprAtomB`.
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

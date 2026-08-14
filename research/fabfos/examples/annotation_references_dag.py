"""Resolve and render the DAG that builds the ANNOTATION references.

    PATH="/home/tony/lib/miniforge3/envs/msm/bin:$PATH" \\
        python examples/annotation_references_dag.py

The four things a fabfos run needs to turn ORFs into reactions, and nothing else:

    kofam       profiles/ + ko_list        the HMM lane
    uniref50    uniref50.dmnd              the homology lane
    CLEAN       -- no artifact --          weights baked into external_clean
    proteinBERT reference_label_pool       Swiss-Prot embeddings labelled with MNXR

plus `mnxr_lookup`, which is not a fourth lane but every lane's terminus: kofam emits
KOs, CLEAN emits ECs, DIAMOND and the pool emit UniProt accessions, and that one table
turns all three id spaces into MetaNetX reactions.

CLEAN HAS NO TARGET HERE BECAUSE IT NEEDS NO ARTIFACT. ESM-1b and the max-separation
bundle are baked into `docker://quay.io/hallamlab/external_clean:2026.06.14` at /app,
so under a container runtime its weights cost nothing to "acquire" and there is
nothing for a transform to produce. The same is true of ProteinBERT's weights -- what
R7 below builds is the labelled POOL, not the model. The build's whole obligation to
the CLEAN lane is the `ec` route of mnxr_lookup.

WHY THIS RUNS UNDER A CONTAINER RUNTIME AND THE METABOLISM BAKE DOES NOT. `Agent.runtime`
is one global setting, so a graph whose envs cannot all satisfy it does not run. Every env
this chain touches -- python_for_data_science, diamond, proteinbert -- publishes a
container image; `proteinbert.env` and `clean.env` publish ONLY an image and have no
`conda:` key at all, so there is no MAMBA path for them. The metabolism bake is the mirror
image: `build-refs-{rdkit,equilibrator,cobra}` are conda specs with no published image. The
two halves are therefore separate runs by construction, not by preference.

THE GRAPH HAS NO GIVEN, which is the claim this asserts. Every leaf is a download with
a transform behind it -- metanetx, kegg, rhea, kofam, uniref, swissprot -- so the
annotation half of the reference build is reproducible from the library alone on a
machine holding none of it. (The metabolism half has exactly one given, the licensed
MetaCyc drop-in; that is `examples/metabolism_references_dag.py`'s business, not
this one's.)

Planning is type-driven -- nothing is staged, containerised or executed -- so this
renders on a machine holding none of the bytes.
"""
from __future__ import annotations

import sys
import tempfile
from pathlib import Path

from metasmith.python_api import (
    Agent,
    Source,
    DataInstanceLibrary,
    TransformInstanceLibrary,
    TargetBuilder,
    Runtime,
)

REPO = Path(__file__).resolve().parents[3]
MLIB = REPO / "src" / "metasmith_libraries"
BREF = REPO / "src" / "fabfos" / "build_references"
ARTIFACTS = REPO / "tests" / "fabfos" / "artifacts"

# By artifact id in build_references/REFERENCES.md.
TARGETS = [
    ("R3", "ref::kofamscan_profiles"),
    ("R3", "ref::kofamscan_ko_list"),
    ("R4", "ref::uniref50_diamond_db"),
    ("R5", "ref::mnxr_lookup"),
    ("R7", "ref::reference_label_pool"),
    # R8 belongs to the three decided-against lanes, not the canonical four -- the
    # ESM-C embedding lane and the EZpred EC heads share these weights. It is here
    # because it now HAS a producer that is not logistics/'s run-time downloader.
    ("R8", "ref::esm_c_600m_weights"),
    # R10 is R7's twin: the same bridge cut and the same Swiss-Prot release, embedded
    # with ESM-C instead of ProteinBERT, so the ESM-C kNN lane has a pool of its own to
    # vote against. Two references rather than one directory with two stacks, because
    # the embedders live in two images and only this one needs a GPU.
    ("R10", "ref::reference_label_pool_esmc"),
]

# Every transform that must appear, so a plan that quietly drops a branch fails rather
# than rendering a smaller graph.
# Two producers for one reference makes provenance a planner tiebreak, so
# `transforms/logistics/` is never loaded -- and that is asserted BY NAME rather
# than left to "we did not load it", because a future edit could.
DUPLICATE_PRODUCERS = ("downloadKofamscanDB", "downloadUniref50", "downloadEsmC")

EXPECTED = {
    # acquire -- one per source folder
    "metanetx", "kegg", "rhea", "kofam", "uniref", "swissprot", "esm_c",
    # compile
    "kofam_ref", "uniref50_dmnd", "mnxr_lookup", "reference_label_pool",
    "esm_c_weights", "reference_label_pool_esmc",
}


def plan(work: Path):
    inputs = DataInstanceLibrary(work / "inputs.xgdb")
    for tl in ("ncbi.yml", "sequences.yml", "annotation.yml", "ref.yml", "lib.yml"):
        inputs.AddTypeLibrary(MLIB / "data_types" / tl)
    for tl in ("fabfos_data.yml", "raw.yml", "interm.yml", "bench.yml", "buildlib.yml"):
        inputs.AddTypeLibrary(BREF / "data_types" / tl)
    inputs.Save()

    resources = [
        DataInstanceLibrary.Load(MLIB / "resources" / "env"),
        DataInstanceLibrary.Load(MLIB / "resources" / "lib"),
        DataInstanceLibrary.Load(BREF / "resources" / "buildlib"),
        inputs,
    ]
    # acquire + compile only. The run-side annotation lanes are what CONSUME these
    # references; loading them here would pull in host proteomes and turn this into the
    # benchmark DAG. logistics/ is likewise left out: it carries downloaders that produce
    # the same ref:: types compile/ does, and two producers for one reference is a
    # tiebreak deciding provenance.
    transforms = [
        TransformInstanceLibrary.Load(BREF / "transforms" / "acquire"),
        TransformInstanceLibrary.Load(BREF / "transforms" / "compile"),
    ]

    targets = TargetBuilder()
    for _id, dtype in TARGETS:
        targets.Add(dtype)

    agent = Agent(home=Source.FromLocal(work / "agent_home"),
                  runtime=Runtime.APPTAINER)
    # The solver roots its search in samples and refuses an empty set ("nothing given"),
    # so the EMPTY inputs library is the one sample: it registers the type libraries and
    # contributes zero data endpoints. That is the shape of the claim -- one case, and
    # nothing handed to it but the env/lib/buildlib resources, which are code rather than
    # data. A reference build has no samples in the sense the fosmid pipeline does.
    return agent.GenerateWorkflow(
        samples=[inputs],
        resources=resources,
        transforms=transforms,
        targets=targets,
    )


def main() -> int:
    with tempfile.TemporaryDirectory() as td:
        task = plan(Path(td))
        if not task.ok:
            print(f"FAILED to plan:\n{task.plan}")
            return 1

        used = {Path(s.transform._path).stem for s in task.plan.steps}
        print(f"Plan OK -- {len(task.plan.steps)} steps, {len(used)} distinct transforms\n")
        for step in sorted(task.plan.steps, key=lambda s: s.order):
            prods = [i.dtype_name for g in step.produces for i in g]
            print(f"  {step.order:>3}  {Path(step.transform._path).stem:<24} -> {prods}")

        dups = set(DUPLICATE_PRODUCERS) & used
        if dups:
            print(f"\nDUPLICATE REFERENCE PRODUCER(S) in the plan: {sorted(dups)}. "
                  f"logistics/ produces the same ref:: types compile/ does, so which "
                  f"one built a reference would be a planner tiebreak.")
        missing = (EXPECTED - used) | dups
        extra = used - EXPECTED
        print()
        if missing:
            print(f"MISSING expected transforms: {sorted(missing)}")
        if extra:
            print(f"note -- transforms used but not in EXPECTED: {sorted(extra)}")
        if not missing:
            print("every expected transform is in the plan")

        ARTIFACTS.mkdir(parents=True, exist_ok=True)
        svg = ARTIFACTS / "annotation_references_dag.svg"
        task.plan.RenderDAG(svg, blacklist_namespaces={"lib", "env", "buildlib"})
        print(f"\nDAG -> {svg}")
        return 1 if missing else 0


if __name__ == "__main__":
    sys.exit(main())

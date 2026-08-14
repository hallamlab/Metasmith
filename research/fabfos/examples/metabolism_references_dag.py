"""Resolve and render the DAG that builds the METABOLISM references (R6).

    PATH="/home/tony/lib/miniforge3/envs/msm/bin:$PATH" \\
        python examples/metabolism_references_dag.py

The other half of stage 2. Where the annotation references turn ORFs into reaction
IDS, this turns reactions into a NETWORK: which atoms carry through a reaction, and
which way it runs.

    rxnmapper / localmapper / indigo    the three atom-mapping members
    aam_ensemble        vocab + atom_pairs   who maps to whom, and the bake's identity
    equilibrator / dgbyg          the two thermodynamic members
    direction_ensemble  direction_ratios     which way, coded against that vocabulary

ONE STEP PER TOOL, which is a change from the two-transform generation this gate was
written against. Each member that runs a MODEL is its own node with its own image, its
own resources and its own product, so a member that did not run is a hole the planner
refuses to schedule around rather than a column that came out empty. What is left in the
two `*_ensemble` steps is the curated member -- each reads its own .dat from the licensed
drop-in -- the arithmetic over the model members, and the encoding.

THE TRIO IS BUILT 2 + 1, and the edge that makes that safe is `aam_ensemble ->
direction_ensemble`. All three files must carry a byte-identical bake-identity block --
reading atom_pairs against another bake's vocab decodes every node to the wrong
metabolite SILENTLY -- which is why a third step used to exist to write all three at
once. Instead the AAM assembly MINTS the block and the direction assembly INHERITS it,
requiring `ref::metabolism_vocab` and passing its identity through verbatim. Agreement is
structural rather than two computations coinciding, and there is no step whose only job
is to re-encode what the assemblies already produced.

THIS GRAPH HAS EXACTLY ONE GIVEN, and that is the claim this asserts. MetaCyc is
licensed and not redistributable, so nothing fetches it and nothing ever will; it is
staged. Every other leaf -- MetaNetX, the eQuilibrator compound cache -- is a download
with a transform behind it. A SECOND given appearing here means something fetchable is
being handed in instead of produced.

The given is also not a formality: MetaCyc is the INDEPENDENT member of both ensembles.
RXNMapper and LocalMapper are two transformers over the same reaction SMILES, and
eQuilibrator and dGbyG are both TECRDB-fitted, so each ensemble's other two members are
correlated by construction. Losing the drop-in does not shrink either ensemble evenly --
it removes the only member that can break a tie.

WHY THIS IS A SEPARATE RUN FROM THE ANNOTATION HALF. `Agent.runtime` is one global
setting, so a graph whose envs cannot all satisfy it does not run. This half is now
satisfiable either way -- `rdkit.env`, `equilibrator.env` and `dgbyg.env` carry both a
`conda:` and a `container:` key -- but the annotation half carries an image and no
`conda:` key at all, so the two cannot be fused under MAMBA, and they remain separate
runs.

Planning is type-driven -- nothing is staged, containerised or executed -- so this
renders on a machine holding none of the bytes, including the licensed drop-in: an
empty stand-in resolves the type exactly as the real 23,557 files do. A RUN is what
needs the real one, and it refuses where the .dat is read.
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

# THE ONE GIVEN. `fabfos_data::metacyc` is a source FOLDER holding one release
# directory, so this is the folder above `26/`, not `26/` itself.
GIVEN_TYPE = "fabfos_data::metacyc"
GIVEN_AT = REPO / "data" / "fabfos" / "originals" / "metacyc"

# The R6 trio, by artifact id in build_references/REFERENCES.md.
TARGETS = [
    ("R6", "ref::atom_pairs"),
    ("R6", "ref::metabolism_vocab"),
    ("R6", "ref::direction_ratios"),
]

# Every transform that must appear, so a plan that quietly drops a branch fails rather
# than rendering a smaller graph.
# Two producers for one reference makes provenance a planner tiebreak, so
# `transforms/logistics/` is never loaded -- and that is asserted BY NAME rather
# than left to "we did not load it", because a future edit could.
DUPLICATE_PRODUCERS = ("downloadKofamscanDB", "downloadUniref50", "downloadEsmC")

EXPECTED = {
    # acquire -- one per source folder. The metacyc ACQUISITION is absent by design: the
    # drop-in is the given. (`equilibrator` is both an acquisition and a bake lane; this
    # set is keyed by transform stem, so one entry covers the pair.)
    "metanetx", "equilibrator", "chebi", "modelseed",
    # compile. mnx_lookups is one transform with five products: the atom node identity
    # `(mnxm, canonical rank)` has to be produced by exactly ONE piece of code, and a
    # transform boundary inside that is an invitation for a second one to appear.
    "mnx_lookups",
    # bake -- the adjudication, one lane per tool per PASS, the rescue between them, then
    # the two assemblies. Six AAM member lanes: the same three mappers over the
    # adjudicated universe and again over the reactions the rescue completed.
    "aam_worklist",
    "rxnmapper", "localmapper", "indigo",
    "aam_rescue",
    "rxnmapper_rescue", "localmapper_rescue", "indigo_rescue",
    "aam_ensemble",
    "dgbyg", "direction_ensemble",
}

# WHICH RUNTIME. This gate used to be MAMBA-only, because `rdkit.env` and
# `equilibrator.env` carried a `conda:` key and no `container:` -- so no container runtime
# could satisfy the graph, and the metabolism half ran nowhere but a workstation with the
# conda envs already built. Both now name an image out of docker/ecspr_bake, so the gate
# plans under EITHER runtime and this is a parameter rather than a fact.
#
# They name THREE images, forced rather than chosen, and the two splits have different
# causes: equilibrator-cache 0.7.1 requires numpy>=2 while torch 2.2.1 is compiled
# against the numpy 1.x C API (that separates :aam), and dGbyG's source needs python 3.12
# to parse while the eQuilibrator stack is pinned at 3.11 (that separates :dgbyg).
RUNTIME = Runtime.APPTAINER
# Under MAMBA, `Agent.container` is read as a conda environment NAME rather than an image
# URI; leaving the default makes staging try `conda run -n docker://quay.io/...`.
AGENT_ENV = "msm-fabfos"
AGENT_IMAGE = "docker://quay.io/hallamlab/metasmith:0.15.1"


def plan(work: Path):
    inputs = DataInstanceLibrary(work / "inputs.xgdb")
    for tl in ("ncbi.yml", "sequences.yml", "annotation.yml", "ref.yml", "lib.yml"):
        inputs.AddTypeLibrary(MLIB / "data_types" / tl)
    for tl in ("fabfos_data.yml", "raw.yml", "interm.yml", "bench.yml", "buildlib.yml",
               "lookup.yml", "evidence.yml"):
        inputs.AddTypeLibrary(BREF / "data_types" / tl)

    given = GIVEN_AT
    if not given.exists():
        # Planning never opens an input, so an empty directory resolves the type exactly
        # as the licensed distribution does. This is what lets the gate run on a machine
        # that has not licensed MetaCyc -- and it is only ever a PLAN.
        given = work / "metacyc_standin"
        given.mkdir(parents=True, exist_ok=True)
        print(f"NOTE: no MetaCyc drop-in at {GIVEN_AT}; standing in an empty directory "
              f"so the plan can resolve. A run refuses where the .dat is read.\n")
    inputs.AddItem(given, GIVEN_TYPE)
    inputs.Save()

    resources = [
        DataInstanceLibrary.Load(MLIB / "resources" / "env"),
        DataInstanceLibrary.Load(MLIB / "resources" / "lib"),
        DataInstanceLibrary.Load(BREF / "resources" / "buildlib"),
        inputs,
    ]
    # acquire + compile only. benchmark/ is left out because it is a different question
    # (what a host's GEM asserts, not what the chemistry is), and logistics/ because it
    # carries downloaders producing the same ref:: types compile/ does -- two producers
    # for one reference is a tiebreak deciding provenance.
    transforms = [
        TransformInstanceLibrary.Load(BREF / "transforms" / "acquire"),
        TransformInstanceLibrary.Load(BREF / "transforms" / "compile"),
        TransformInstanceLibrary.Load(BREF / "transforms" / "bake"),
    ]

    targets = TargetBuilder()
    for _id, dtype in TARGETS:
        targets.Add(dtype)

    # `Agent.container` means two different things under the two runtimes -- an image URI
    # under a container runtime, a conda env NAME under MAMBA -- so it is selected with
    # the runtime rather than beside it.
    agent = Agent(home=Source.FromLocal(work / "agent_home"),
                  runtime=RUNTIME,
                  container=AGENT_ENV if RUNTIME == Runtime.MAMBA else AGENT_IMAGE)
    return inputs, agent.GenerateWorkflow(
        samples=list(inputs.AsSamples(GIVEN_TYPE)),
        resources=resources,
        transforms=transforms,
        targets=targets,
    )


def main() -> int:
    with tempfile.TemporaryDirectory() as td:
        inputs, task = plan(Path(td))
        if not task.ok:
            print(f"FAILED to plan:\n{task.plan}")
            return 1

        used = {Path(s.transform._path).stem for s in task.plan.steps}
        print(f"Plan OK -- {len(task.plan.steps)} steps, {len(used)} distinct transforms\n")
        for step in sorted(task.plan.steps, key=lambda s: s.order):
            prods = [i.dtype_name for g in step.produces for i in g]
            print(f"  {step.order:>3}  {Path(step.transform._path).stem:<20} -> {prods}")

        # The one-given assertion. Counted off the input library rather than off the plan
        # because a given is precisely what is NOT a step: it is an endpoint handed in.
        staged = [(path, name) for path, name, _ in inputs.Iterate()]
        print(f"\ngiven: {len(staged)} staged input(s) -- "
              f"{sorted({n for _, n in staged})}")

        problems = []
        if len(staged) != 1:
            problems.append(
                f"expected exactly one given, found {len(staged)}. Everything but the "
                f"licensed MetaCyc drop-in is fetchable, so a second given means "
                f"something with a transform behind it is being handed in instead.")
        dups = set(DUPLICATE_PRODUCERS) & used
        if dups:
            print(f"\nDUPLICATE REFERENCE PRODUCER(S) in the plan: {sorted(dups)}. "
                  f"logistics/ produces the same ref:: types compile/ does, so which "
                  f"one built a reference would be a planner tiebreak.")
        missing = (EXPECTED - used) | dups
        if missing:
            problems.append(f"MISSING expected transforms: {sorted(missing)}")
        extra = used - EXPECTED
        if extra:
            print(f"note -- transforms used but not in EXPECTED: {sorted(extra)}")

        print()
        for p in problems:
            print(f"FAIL: {p}")
        if not problems:
            print("every expected transform is in the plan, and MetaCyc is the only given")

        ARTIFACTS.mkdir(parents=True, exist_ok=True)
        svg = ARTIFACTS / "metabolism_references_dag.svg"
        task.plan.RenderDAG(svg, blacklist_namespaces={"lib", "env", "buildlib"})
        print(f"\nDAG -> {svg}")
        return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())

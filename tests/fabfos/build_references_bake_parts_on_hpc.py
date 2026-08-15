#!/usr/bin/env python3
"""R6, the metabolism bake, in PARTS -- planned here, executed on Sockeye under SLURM.

    PYTHONPATH=src python tests/fabfos/build_references_bake_parts_on_hpc.py direction
    ... build_references_bake_parts_on_hpc.py members --run --user txyliu

WHY PARTS AND NOT ONE RUN. `build_references_bake_on_hpc.py` plans all twelve lanes and
submits them as one graph. That is the right shape when one host owns the whole thing and
the wrong shape the moment two branches are in flight on different machines: the AAM
branch is nine lanes and most of a day, the direction branch is a handful of table
operations, and a single plan makes the second wait on the first even where no data flows
between them. This driver runs ONE part at a time and treats the boundary between parts as
a declared import.

WHY THE PARTS ARE WHERE THEY ARE. Three of the four boundaries are the IMAGE boundary,
which every bake transform already declares as its `group_by` -- so a part pulls one image
and owns one coherent piece. The fourth is new and is the reason this file exists:
`direction_ensemble` used to require `ref::metabolism_vocab` and so sat strictly
downstream of the AAM branch, for an ENCODING it does in its last four lines. That encode
is `direction_bake` now, and the science runs the moment the two thermodynamic members
are in. See build_references/transforms/bake/direction_bake.py.

    aam            -> vocab, atom_pairs                 nine lanes, most of a day
    members        -> direction_member_eq, _dgbyg       concurrent with it
    direction      -> direction_annotation              concurrent with it, after members
    direction_bake -> direction_ratios                  seconds, once aam lands

EVERY SEAM IS A DECLARED IMPORT, which is the mechanism this graph already uses everywhere
else rather than one invented for the split -- the five `lookup::` tables are PRODUCED by
`mnx_lookups` and are nevertheless handed to every run as givens. What makes it safe is
the gate: an import that fails to satisfy its type does not error, it quietly leaves the
PRODUCER in the plan, so a direction run whose seam path was wrong by one character
silently re-runs the entire AAM branch on a host with neither the image nor the inputs for
it. `check_plan` asserts the plan is EXACTLY this part's lanes, and
`assert_seams_are_outputs` (at import) asserts every import is some other part's declared
output at the identical path.

WHAT THIS ADDS OVER THE OFF-CLUSTER SIBLING. `tests/build_references_bake_split.py` runs
the same parts on VMs, under a hand-written local Nextflow pool. Sockeye is a scheduler,
and four of its properties are load-bearing rather than incidental:

  * The two-step Lmod load, whose ORDER matters and which must be two commands.
  * `/scratch` is ALLOCATION-scoped -- there is no `/scratch/<user>` -- and its metadata
    layer is degraded by more than two orders of magnitude on small files, which is why
    the image store lives on `/arc` and why a part that writes tens of thousands of small
    files does not belong here. These parts write a handful of parquets.
  * The pinned engine reaches a run as TWO artifacts: `<agent_home>/dev/metasmith/`, bound
    by the login node, and `<agent_home>/dev/metasmith.tar`, staged by every SLURM task.
    Ship one and not the other and the login node runs one engine while every compute node
    runs another.
  * A `labels=["local"]` on any step here is FATAL rather than cosmetic. Under the local
    executor the label is inert; under the slurm preset it maps to an 8-core / 8 GB login
    pool with `errorStrategy='ignore'`, so a step asking for more is dropped silently and
    the workflow finishes green with its output absent.

THE AGENT BASE IMAGE IS THE SITE'S, NOT THE ENGINE HASH'S. `Agent.container` defaults to
`metasmith:{VERSION}-{BUILD_HASH}`, which names an image nobody necessarily built; a tag
that resolves to nothing fails as `manifest unknown` against a private registry, then a
FATAL about a missing .sif, then an assertion about a missing relay binary, none of which
says "nobody built this image". The store holds `0.19.0-6639608`, and the dev overlay is
what makes the engine inside it the pinned one.
"""
from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]

# The PINNED engine, ahead of whatever is installed. Planning works on either; execution
# does not -- only the pin resolves a `container:`/`conda:` env declaration by runtime
# instead of handing the whole declaration to apptainer as a URI.
_ENGINE = REPO / "src"
if (_ENGINE / "metasmith").is_dir():
    sys.path.insert(0, str(_ENGINE))

from metasmith.python_api import (                                      # noqa: E402
    Agent,
    DataInstanceLibrary,
    Runtime,
    Source,
    SshSource,
    TargetBuilder,
    TransformInstanceLibrary,
)

# The site bundle and the cluster checks live with the other executing drivers; there is
# one copy of "what sockeye does differently" and this is not a second one.
sys.path.insert(0, str(REPO / "research" / "fabfos" / "examples"))
from _driver import (                                                   # noqa: E402
    SOCKEYE_ACCOUNT, SOCKEYE_CONTAINER, SOCKEYE_HOST, SOCKEYE_IMAGE_STORE,
    SOCKEYE_SETUP_COMMANDS,
    check_schedulable, check_staged_executor, check_tasks, check_walltimes,
    envs_from_plan, preflight, provision_dev_overlay_remote, ssh_once,
)

MLIB = REPO / "src" / "metasmith_libraries"
BREF = REPO / "src" / "fabfos" / "build_references"
DATA = REPO / "data" / "fabfos"
ARTIFACTS = REPO / "tests" / "fabfos" / "artifacts"
WORK_ROOT = DATA / "scratch" / "r6_bake_parts"

TYPE_LIBRARIES = (
    [MLIB / "data_types" / f for f in
     ("ncbi.yml", "sequences.yml", "annotation.yml", "ref.yml", "lib.yml")]
    + [BREF / "data_types" / f for f in
       ("fabfos_data.yml", "raw.yml", "interm.yml", "bench.yml", "buildlib.yml",
        "lookup.yml", "evidence.yml")]
)

# THE ONE GIVEN. Licensed, not redistributable, and the independent member of BOTH
# ensembles. It is also the sample every part is planned against, including the two that
# never open it -- `GenerateWorkflow` needs a sample and this graph has exactly one.
GIVEN_TYPE = "fabfos_data::metacyc"
GIVEN_AT = DATA / "originals" / "metacyc"

# Everything any part might stage, keyed by type and valued by its path under data/. ONE
# map, so the re-rooting onto the cluster and the local existence check cannot drift.
ALL_INPUTS = {
    "fabfos_data::metanetx":     "originals/metanetx",
    "fabfos_data::chebi":        "originals/chebi",
    "fabfos_data::modelseed":    "originals/modelseed",
    "fabfos_data::equilibrator": "originals/equilibrator",
    "lookup::reactions":         "processed/lookups/reactions.parquet",
    "lookup::metabolites":       "processed/lookups/metabolites.parquet",
    "lookup::atom_ranks":        "processed/lookups/atom_ranks.parquet",
    "lookup::xrefs":             "processed/lookups/xrefs.parquet",
    "lookup::synonyms":          "processed/lookups/synonyms.parquet",
}

# The MetaCyc files any transform in this graph opens. The whole distribution is NOT
# pushed, and that is a licence decision rather than a transfer-size one: the drop-in is
# 1.6 GB of which 60 MB is read, it is not redistributable, and a shared cluster
# filesystem is not the place to put more of it than the method needs.
METACYC_FILES = ("atom-mappings-smiles.dat", "reactions.dat", "compounds.dat")

# `data/.gitignore` is `/*/*` with a `.dvc` negation, so this is already ignored and must
# NOT get a .gitignore of its own -- that file is the only one under data/ and the tier
# rule depends on it staying that way.
TEMP = DATA / "temp"

BAKE_SIF_DIR = REPO / "docker" / "ecspr_bake"
IMG = "docker://quay.io/hallamlab/ecspr_bake:{}"

# ---------------------------------------------------------------------------
# the parts
# ---------------------------------------------------------------------------
#
# `lanes` is an EQUALITY assertion. A lane missing means the part was planned short; a
# lane EXTRA means an import did not satisfy its type and the planner resurrected another
# part's producer, which is the failure this whole design has to make loud.
#
# `outputs` and `imports` are the same namespace: every import is some other part's
# output, at the identical path, checked on startup by assert_seams_are_outputs.
BRANCHES = {
    "aam": dict(
        images=[IMG.format("aam")],
        lanes={
            "aam_worklist",
            "indigo", "rxnmapper", "localmapper",
            "aam_rescue",
            "indigo_rescue", "rxnmapper_rescue", "localmapper_rescue",
            # Pass 3: element-reduced submissions for what no full map reached, and the
            # same three mappers over them. Sequenced after both passes because its
            # target set is what ENDED with nothing -- a fact about a run.
            "aam_partial",
            "indigo_partial", "rxnmapper_partial", "localmapper_partial",
            "aam_ensemble",
        },
        targets=["ref::atom_pairs", "ref::metabolism_vocab"],
        inputs=[
            "fabfos_data::metanetx", "fabfos_data::chebi", "fabfos_data::modelseed",
            "lookup::reactions", "lookup::metabolites", "lookup::atom_ranks",
            "lookup::xrefs", "lookup::synonyms",
        ],
        imports={},
        needs_metacyc=True,
        evidence={
            "worklist":           "aam_worklist",
            "rxnmapper":          "rxnmapper",
            "localmapper":        "localmapper",
            "indigo":             "indigo",
            "rescue":             "aam_rescue",
            "rxnmapper_rescue":   "rxnmapper_rescue",
            "localmapper_rescue": "localmapper_rescue",
            "indigo_rescue":      "indigo_rescue",
            "partial":            "aam_partial",
            "rxnmapper_partial":  "rxnmapper_partial",
            "localmapper_partial": "localmapper_partial",
            "indigo_partial":     "indigo_partial",
            "metacyc":            "aam_ensemble",
            "ensemble":           "aam_ensemble",
        },
        outputs={
            "ref::metabolism_vocab": "temp/metabolism/vocab.parquet",
            "ref::atom_pairs":       "temp/metabolism/atom_pairs.parquet",
            # Not a seam and not part of the trio -- the UNCODED stack, which is what
            # build_references_tier4_agreement.py compares against the deployed table.
            # `ref::atom_pairs` is encoded against the vocabulary, so the gate cannot
            # read it.
            "interm::aam_pairs":     "temp/_seams/aam_pairs.parquet",
        },
    ),
    "members": dict(
        images=[IMG.format("direction"), IMG.format("dgbyg")],
        lanes={"equilibrator", "dgbyg"},
        # Targeted by their INTERMEDIATE types rather than by the trio, which is the
        # point of this part: neither member needs the vocabulary, so both run while the
        # AAM part is still going.
        targets=["interm::direction_member_eq", "interm::direction_member_dgbyg"],
        inputs=["fabfos_data::metanetx", "fabfos_data::equilibrator"],
        imports={},
        # Neither member reads the drop-in -- the curated member belongs to the assembly.
        needs_metacyc=False,
        evidence={"equilibrator": "equilibrator", "dgbyg": "dgbyg"},
        outputs={
            "interm::direction_member_eq":    "temp/_seams/direction_member_eq.parquet",
            "interm::direction_member_dgbyg": "temp/_seams/direction_member_dgbyg.parquet",
        },
    ),
    "direction": dict(
        images=[IMG.format("direction")],
        lanes={"direction_ensemble"},
        # The UNCODED annotation, not the compiled ratios. That is the whole reason this
        # part can run beside the AAM branch instead of behind it.
        targets=["interm::direction_annotation"],
        inputs=["fabfos_data::metanetx"],
        imports={
            "interm::direction_member_eq":    "temp/_seams/direction_member_eq.parquet",
            "interm::direction_member_dgbyg": "temp/_seams/direction_member_dgbyg.parquet",
        },
        # The curated member IS the assembly's independent vote, read straight from
        # reactions.dat. Without it this ensemble keeps only its two correlated members.
        needs_metacyc=True,
        evidence={
            "metacyc_direction":     "direction_ensemble",
            "direction_calibration": "direction_ensemble",
        },
        outputs={
            "interm::direction_annotation": "temp/_seams/direction_annotation.parquet",
        },
    ),
    "direction_bake": dict(
        images=[IMG.format("direction")],
        lanes={"direction_bake"},
        targets=["ref::direction_ratios"],
        inputs=[],
        imports={
            # From the AAM host, and an input to the ENCODING only: the ratios are coded
            # against this vocabulary's `rxn` space and inherit its identity block.
            "ref::metabolism_vocab":        "temp/metabolism/vocab.parquet",
            "interm::direction_annotation": "temp/_seams/direction_annotation.parquet",
        },
        needs_metacyc=False,
        # No tool runs here -- one table is re-expressed in another table's codes. The
        # evidence for the direction call belongs to the assembly; the evidence for this
        # step is `selftest_direction`, which is an exit code rather than a file.
        evidence={},
        outputs={"ref::direction_ratios": "temp/metabolism/direction.parquet"},
    ),
}


def assert_seams_are_outputs() -> None:
    """Every import must be some other part's output, at the same path.

    Cheap, and it closes the one gap the plan gate cannot see. The gate proves an import
    RESOLVED; it cannot prove the file it resolved against is the one the producing part
    actually writes. A path typo would sail through planning -- the item is declared, the
    type is satisfied -- and surface as a direction table encoded against nothing.
    """
    produced = {d: p for b in BRANCHES.values() for d, p in b["outputs"].items()}
    for name, spec in BRANCHES.items():
        for dtype, rel in spec["imports"].items():
            if produced.get(dtype) != rel:
                raise SystemExit(
                    f"[{name}] imports {dtype} from data/{rel}, but no part declares that "
                    f"as an output there (declared: {produced.get(dtype)!r}). An import "
                    f"and the output it comes from are one fact written in two places, "
                    f"and they have drifted.")


assert_seams_are_outputs()

ACQUIRE_LIB = BREF / "transforms" / "acquire"


def cached_image_name(image: str) -> str:
    """The filename metasmith looks for in the image store.

    Mirrors `Environment._cached_name` in the pinned engine. A copy rather than an import
    because the driver must PLACE the file before any engine code runs on the remote --
    but it is a mirror, so if that method changes, this must too.
    """
    return image.replace("://", "..").replace(":", "..").replace("/", "_") + ".sif"


# ---------------------------------------------------------------------------
# planning
# ---------------------------------------------------------------------------

def build_inputs(work: Path, branch: str, remote_root: str | None):
    """Stage the given, this part's sources, and its imports from the parts before it.

    RE-ROOTING IS NOT AN OPTIMISATION. metasmith binds an item's OWN path into the task
    container -- the same string on both sides -- so an input declared at a workstation
    path is bind-mounted at that path on the cluster node, where it does not exist, and
    apptainer refuses with "mount source does not exist", naming neither the item nor the
    path.

    Existence is always checked against the LOCAL copy even when the declared path is
    remote: the planner resolves on types and lineage and never on existence, so a missing
    tree plans perfectly and fails hours later inside a container. The local copy is what
    rsync just pushed, so checking it is checking the far side.
    """
    spec = BRANCHES[branch]
    inputs = DataInstanceLibrary(work / "inputs.xgdb")
    for tl in TYPE_LIBRARIES:
        inputs.AddTypeLibrary(tl)

    def declared(rel: str) -> Path:
        return Path(remote_root) / rel if remote_root else DATA / rel

    missing, imported_missing = [], []
    for dtype in sorted(spec["inputs"]):
        rel = ALL_INPUTS[dtype]
        if not (DATA / rel).exists():
            missing.append(f"{dtype:32s} data/{rel}")
            continue
        inputs.AddItem(declared(rel), dtype)

    # The seam. Declared whether or not it is here yet, so PLANNING can be checked before
    # the producing part has run. What it must never do is silently fall back to
    # scheduling the producer, and check_plan is what makes that impossible.
    for dtype, rel in sorted(spec["imports"].items()):
        if not (DATA / rel).exists():
            imported_missing.append(f"{dtype:32s} data/{rel}")
        inputs.AddItem(declared(rel), dtype)

    given = GIVEN_AT
    if not given.exists():
        # Planning is type-driven and never opens an input, so an empty directory
        # resolves the type exactly as the licensed distribution does. A RUN refuses --
        # but only for the parts that actually read it.
        given = work / "metacyc_standin"
        given.mkdir(parents=True, exist_ok=True)
        print(f"NOTE: no MetaCyc drop-in at {GIVEN_AT}; standing in an empty directory "
              f"so the plan resolves.")
    inputs.AddItem(declared("originals/metacyc") if remote_root else given, GIVEN_TYPE)

    inputs.Save()
    return inputs, missing, imported_missing


def plan(work: Path, branch: str, remote_root: str | None, agent):
    inputs, missing, imported_missing = build_inputs(work, branch, remote_root)
    if missing:
        print("\nNOT ON DISK, so its producer will be scheduled rather than skipped:")
        for m in missing:
            print(f"    {m}")
        print()
    if imported_missing:
        print("\nIMPORTED FROM AN EARLIER PART, and not here yet:")
        for m in imported_missing:
            print(f"    {m}")
        print("    (the plan is still checkable -- planning never opens an input -- but "
              "--run refuses)\n")

    resources = [
        DataInstanceLibrary.Load(MLIB / "resources" / "env"),
        DataInstanceLibrary.Load(MLIB / "resources" / "lib"),
        DataInstanceLibrary.Load(BREF / "resources" / "buildlib"),
        inputs,
    ]
    # acquire/ and compile/ are LOADED, not hidden. Loading them is what turns "the inputs
    # were staged" into a checkable assertion instead of an assumption: hiding a transform
    # makes an unmet input an unresolvable plan whose error names a TYPE, while loading it
    # and asserting it did not run names the exact step that was skipped.
    transforms = [
        TransformInstanceLibrary.Load(ACQUIRE_LIB),
        TransformInstanceLibrary.Load(BREF / "transforms" / "compile"),
        TransformInstanceLibrary.Load(BREF / "transforms" / "bake"),
    ]

    targets = TargetBuilder()
    for dtype in BRANCHES[branch]["targets"]:
        targets.Add(dtype)

    task = agent.GenerateWorkflow(
        samples=list(inputs.AsSamples(GIVEN_TYPE)),
        resources=resources,
        transforms=transforms,
        targets=targets,
    )
    return inputs, task, bool(imported_missing)


def check_plan(task, branch: str) -> tuple[set[str], list[str]]:
    """The split's correctness proof: exactly this part's lanes, and nothing else.

    EQUALITY, not containment, and the extra-lane case is the one that matters. A declared
    import that does not satisfy its type is not an error anywhere in metasmith -- the
    planner finds the type unmet and schedules its PRODUCER. For the direction part that
    means quietly re-running the entire AAM branch: nine lanes and most of a day, charged
    to an allocation, on nodes that have neither the image nor the inputs for them.
    """
    spec = BRANCHES[branch]
    used, pinned_local = set(), set()
    for step in task.plan.steps:
        p = Path(step.transform._path)
        used.add(p.stem)
        if "local" in (getattr(step.transform, "labels", None) or []):
            pinned_local.add(p.stem)

    print(f"\nPlan OK -- {len(task.plan.steps)} steps, {len(used)} distinct transforms\n")
    for step in sorted(task.plan.steps, key=lambda s: s.order):
        prods = [i.dtype_name for g in step.produces for i in g]
        print(f"  {step.order:>3}  {Path(step.transform._path).stem:<20} -> {prods}")

    problems = []
    absent = spec["lanes"] - used
    if absent:
        problems.append(
            f"MISSING lanes this part owns: {sorted(absent)}. Every one of those is a "
            f"member or an assembly; a plan without it produces a table that looks "
            f"complete.")
    extra = used - spec["lanes"]
    if extra:
        problems.append(
            f"LANES THIS PART DOES NOT OWN ARE SCHEDULED: {sorted(extra)}.\n"
            f"    An unmet type is not an error -- the planner schedules its producer -- "
            f"so this is what a failed import looks like from the inside. Check that the "
            f"declared path for each of {sorted(spec['imports'])} exists on the remote at "
            f"the path the plan declares, and that it is the same TYPE (an item added "
            f"under the wrong dtype resolves nothing and reports nothing).\n"
            f"    If any of these came from acquire/, the assumption that broke is that "
            f"everything under data/originals/ is already on disk.")
    if pinned_local:
        problems.append(
            f"PINNED TO THE LOGIN NODE: {sorted(pinned_local)}. A `local` label renders "
            f"as `label 'xlocalx'`, which the slurm preset maps to executor='local' "
            f"against a pool it declares as 8 cores / 8 GB, with errorStrategy='ignore' "
            f"and no retry -- so a step asking for more is DROPPED and the workflow "
            f"finishes green with its output absent. Correct for acquire/, which needs "
            f"the outbound route only a login node has; wrong for every step here, which "
            f"are the compute. Inert under a local executor, which is why it survives a "
            f"workstation run and only ever fails on a cluster.")
    return used, problems


def plan_resources(task) -> dict:
    """The declared Resources of each planned step, keyed by transform stem.

    Read off the RESOLVED plan rather than kept by hand. `check_walltimes` and
    `check_schedulable` both need the longest ask, and a hand-kept table drifts in the
    direction that matters -- omitting the one step whose duration is unschedulable.
    """
    out = {}
    for step in task.plan.steps:
        r = getattr(step.transform, "resources", None)
        if r is not None:
            out[Path(step.transform._path).stem] = r
    return out


# ---------------------------------------------------------------------------
# execution
# ---------------------------------------------------------------------------

def push_data(host: str, branch: str, remote_root: str) -> None:
    """rsync this part's inputs to the host, at the paths the declarations use.

    Converges: the sources are release-pinned directories and the lookups are rebuilt only
    when MetaNetX moves, so a second run transfers nothing.

    --size-only, and NEITHER the default (size+mtime) NOR --checksum. mtime is out because
    hardlink placement and DVC checkout give a re-staged file a fresh one with identical
    bytes. --checksum is out because of WHERE the reading happens: it makes the remote side
    read and digest every byte it already has, and the remote side here is a LOGIN NODE.
    Three consecutive runs died as `connection unexpectedly closed`, always on eQuilibrator
    or MetaNetX, never on the small directories -- the scheduler killing a process that
    spent minutes at full CPU on a shared host. It reads as a flaky link and is not one.

    NO --delete: the remote root also holds a previous run's work directory and the caches
    that make a lane resumable.
    """
    spec = BRANCHES[branch]
    rels = [ALL_INPUTS[t] for t in spec["inputs"]] + list(spec["imports"].values())
    if spec["needs_metacyc"]:
        # The drop-in, file by file rather than as a directory -- see METACYC_FILES.
        for release in sorted(p for p in (DATA / "originals/metacyc").glob("*")
                              if p.is_dir()):
            for name in METACYC_FILES:
                for cand in (release / "data" / name, release / name):
                    if cand.exists():
                        rels.append(str(cand.relative_to(DATA)))
                        break
    ssh_once(host, f"mkdir -p {remote_root}")
    for rel in sorted(set(rels)):
        src = DATA / rel
        if not src.exists():
            raise SystemExit(f"cannot push data/{rel} -- it is not here")
        print(f"  {rel}", flush=True)
        ssh_once(host, f"mkdir -p {remote_root}/{Path(rel).parent}")
        # A trailing slash on a directory source, none on a file. rsync treats the two
        # differently and getting it wrong nests the tree one level deeper every run.
        subprocess.run(["rsync", "-a", "--size-only", "--partial", "--info=stats1",
                        f"{src}/" if src.is_dir() else str(src),
                        f"{host}:{remote_root}/{rel}"], check=True)


def place_images(host: str, branch: str, cache_dir: str, container: str) -> None:
    """Put the agent image and this part's task images in the persistent store.

    Runs on the LOGIN node, because a compute node has no outbound route: an image absent
    when a task starts cannot be pulled, and the slurm preset's `errorStrategy='ignore'`
    turns that into a SILENT green run with empty outputs.

    ALL PUSHED, none pulled. `hallamlab/ecspr_bake` is private -- an anonymous-token
    manifest request returns 401 for all three tags -- so a pull is not a fallback for a
    missing local build, it is a different way to fail. Everything already in the store is
    left alone, which is the normal case here: /arc outlives every run.
    """
    ssh_once(host, f"mkdir -p {cache_dir}")
    wanted = [(container, None)]
    wanted += [(i, BAKE_SIF_DIR / cached_image_name(i)) for i in BRANCHES[branch]["images"]]

    for image, local in wanted:
        dest = f"{cache_dir}/{cached_image_name(image)}"
        if "CACHED" in ssh_once(host, f"[ -s {dest} ] && echo CACHED || true"):
            print(f"  cached   {image}", flush=True)
            continue
        if local is None or not local.exists():
            raise SystemExit(
                f"{image} is not in the store on {host}"
                + (f" and there is no local .sif at\n    {local}" if local else "")
                + f"\nThe registry copy is NOT a fallback -- the repo is private, and the "
                f"compute nodes have no route out regardless. Build and place it:\n"
                f"    docker/ecspr_bake/dev.sh --build --sif --sync   (task images)\n"
                f"    ./dev/metasmith.sh --update_container           (agent image)\n"
                f"Do NOT point at an older agent image: the staging semantics come from "
                f"the agent, and one that disagrees with the engine on the PYTHONPATH is "
                f"the exact mismatch the dev overlay exists to prevent.")
        print(f"  upload   {image}  ({local.stat().st_size / 1e9:.1f} GB)", flush=True)
        subprocess.run(["rsync", "-a", "--partial", "--info=progress2",
                        str(local), f"{host}:{dest}"], check=True)


GRACE_S = 300
GRACE_STEP_S = 20
# A FLOOR ON --poll-s, and it is a correctness bound rather than a courtesy to the login
# node. `WaitForWorkflow` only returns `errored` once a SINGLE call has been going longer
# than 5 s, so the poll interval is what decides whether the second question is asked at
# t=30 s or t=120 s -- and `PID.lock` lands ~50 s after the trigger returns. A 30 s poll
# therefore asks the one question that can be answered wrongly, inside the one window
# where it is wrong. The default of 120 s is not a tuning knob; it is above that window.
MIN_POLL_S = 90.0


def wait_for_run(agent, task, timeout_s: int, poll_s: float) -> dict:
    """Wait, and do not believe an `errored` that has not survived a real grace period.

    `WaitForWorkflow` calls a run errored when `PID.lock` is absent AND the sentinel
    `run completed at` is not yet in agent.log. Neither of those is a statement about the
    run; they are two files written by different things at different times, and the gap
    is open at BOTH ends of a run:

      * At the END -- nextflow's process exits, and only then does the agent compile
        results, resolve manifests and write the sentinel. On `direction` that gap was
        48 s, so a successful three-minute run reported errored and its results sat on
        the cluster unretrieved.
      * At the START, which is the worse one, because it does not need a short run to
        bite. `PID.lock` is written by the launcher ~50 s after the trigger returns, and
        anything asking before that sees exactly the same absent-lock/absent-sentinel
        pair. Observed 2026-07-27: the trigger returned at 22:30:39, the lock landed at
        22:31:28, and a driver that asked in between declared a run errored that then ran
        to completion with nobody watching it. See MIN_POLL_S for why the poll interval
        is what decided whether that question got asked at all.

    THE RE-CHECK MUST SLEEP, and the previous one did not. `WaitForWorkflow` RETURNS on an
    errored verdict rather than continuing to poll, so a second call with `timeout_s=180`
    asked once, got the same answer microseconds later, and reported a 3-minute grace
    period it had never waited out. The loop below is the grace period: it is the sleeps
    that distinguish a dead run from an unborn one, not the number of questions.

    The probe's `timeout_s=8` is likewise not arbitrary. `errored` is only reachable after
    5 s inside one call, so a shorter probe can only ever come back `timeout` -- which
    here means the lock EXISTS and the run is alive, since that is the one state the call
    cannot name.
    """
    if poll_s < MIN_POLL_S:
        print(f"=== poll {poll_s:.0f}s raised to {MIN_POLL_S:.0f}s -- see MIN_POLL_S ===",
              flush=True)
        poll_s = MIN_POLL_S
    result = agent.WaitForWorkflow(task, timeout_s=timeout_s, poll_s=poll_s)
    if result["status"] != "errored":
        return result
    print(f"=== status came back `errored`; PID.lock and the sentinel both lag the run, "
          f"so re-asking every {GRACE_STEP_S}s for {GRACE_S}s before believing it ===",
          flush=True)
    waited = 0.0
    while waited < GRACE_S:
        time.sleep(GRACE_STEP_S)
        waited += GRACE_STEP_S
        again = agent.WaitForWorkflow(task, timeout_s=8, poll_s=2.0)
        if again["status"] == "errored":
            continue
        if again["status"] == "completed":
            print(f"=== it had not finished writing; the run completed "
                  f"({waited:.0f}s into the grace period) ===", flush=True)
            again["elapsed_s"] += result["elapsed_s"] + waited
            return again
        # Anything else -- `running` above all -- means the lock is there now and the run
        # never was errored. Hand it back to the real wait with the remaining budget.
        print(f"=== the lock exists now (`{again['status']}`): the run had not started "
              f"when it was first asked. Resuming the wait ===", flush=True)
        second = agent.WaitForWorkflow(task, timeout_s=timeout_s, poll_s=poll_s)
        second["elapsed_s"] += result["elapsed_s"] + waited
        return second
    return result


def retrieve(src_path: str, branch: str, host: str, staging: Path) -> int:
    """Bring this part home, per lane, into data/temp -- where the other parts also land.

    ROUTED BY THE `<tool>/` DIRECTORY INSIDE EACH ARTIFACT, not by filename and not by
    type. An output is named `{batch}-{i}-{branch}.{hash}-{type key}` and every evidence
    artifact has the SAME type, so neither names the lane that wrote it. The tool
    directory is the only attribution left, which is why the lanes copy their evidence
    ROOT rather than the directory under it.

    The parts write disjoint tool directories and disjoint output paths, so running this
    once per part composes into one complete data/temp rather than one overwriting the
    next -- and the outputs land exactly where the next part's imports declare them, which
    is what makes the seams work.

    Nothing is published here. data/reference/ is written deliberately, because it
    rewrites DVC directory hashes.
    """
    import shutil

    spec = BRANCHES[branch]
    staging.mkdir(parents=True, exist_ok=True)
    print(f"\n=== retrieving {branch} into {TEMP.relative_to(REPO)} ===", flush=True)
    # -L, AND IT IS THE WHOLE RETRIEVAL. Every entry under results/ is a symlink into
    # nxf_work -- the engine publishes by linking, not by copying -- so a plain `-a`
    # faithfully reproduces the links and lands a staging directory of dangling pointers.
    # It does not fail: rsync reports success, the manifest parses, and every product
    # reads as "MISSING on disk" while the run that produced it was perfect.
    #
    # --delete so staging MIRRORS this part's run rather than accumulating across runs.
    # Safe because each part has its OWN staging directory.
    subprocess.run(["rsync", "-aL", "--delete", "--partial", "--info=stats1",
                    f"{host}:{src_path}/", f"{staging}/"], check=True)

    lib = DataInstanceLibrary.Load(staging)
    found: dict[str, Path] = {}
    products: dict[str, Path] = {}
    for rel, dtype_name, _ in lib.Iterate():
        # The manifest covers the run's INPUTS too, and their declared paths are remote
        # (`/scratch/...`, `/msm_home/...`) precisely so the task container binds them at
        # the same string on both sides -- so none of them is under `staging` and every
        # one would report MISSING. That noise is not harmless: "MISSING on disk" is the
        # exact symptom of a retrieval that failed to dereference the engine's rellinks,
        # and eleven false ones per run teach a reader to skip the line that matters.
        if dtype_name != "evidence::tool_output" and dtype_name not in spec["outputs"]:
            continue
        path = staging / rel
        if not path.exists():
            print(f"  MISSING on disk: {rel} ({dtype_name})", file=sys.stderr)
            continue
        if dtype_name == "evidence::tool_output":
            for tool_dir in sorted(p for p in path.iterdir() if p.is_dir()):
                found[tool_dir.name] = tool_dir
        elif dtype_name in spec["outputs"]:
            products[dtype_name] = path

    for tool, tool_dir in sorted(found.items()):
        dest = TEMP / tool
        if dest.exists():
            shutil.rmtree(dest)
        shutil.copytree(tool_dir, dest)
        n = sum(1 for _ in dest.rglob("*") if _.is_file())
        print(f"  {tool:<24} -> {dest.relative_to(REPO)}  ({n} files)")

    for dtype_name, rel in spec["outputs"].items():
        p = products.get(dtype_name)
        if p is None:
            continue
        dest = DATA / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(p, dest)
        print(f"  {dtype_name:<32} -> {dest.relative_to(REPO)}  "
              f"({dest.stat().st_size / 1e6:.2f} MB)")

    rc = 0
    missing_tools = sorted(set(spec["evidence"]) - set(found))
    if missing_tools:
        rc = 4
        print("\nNO RAW OUTPUT RETRIEVED for:", file=sys.stderr)
        for t in missing_tools:
            print(f"    {t:<24} (written by {spec['evidence'][t]})", file=sys.stderr)
        print("Every tool's direct output is part of the result. A run missing one is not "
              "a run to publish.", file=sys.stderr)
    missing_products = sorted(set(spec["outputs"]) - set(products))
    if missing_products:
        rc = 4
        print(f"\nINCOMPLETE -- this part did not produce {missing_products}. Anything "
              f"importing them will resurrect this part's lanes rather than fail.",
              file=sys.stderr)

    extra = sorted(set(found) - set(spec["evidence"]))
    if extra:
        print(f"\nnote -- evidence for tools not expected from this part: {extra}")

    print(f"\nraw run kept at {staging.relative_to(REPO)} (logs and step manifests too)")
    return rc


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("branch", choices=sorted(BRANCHES),
                    help="which part of the graph this run owns. aam and members are "
                         "independent and go first; direction waits on members only; "
                         "direction_bake waits on aam and direction")
    ap.add_argument("--run", action="store_true",
                    help="execute on the host. Without it this plans, checks and renders "
                         "the DAG, and touches no remote machine")
    ap.add_argument("--host", default=SOCKEYE_HOST)
    ap.add_argument("--user", default=None,
                    help="remote username, REQUIRED with --run and never auto-resolved. "
                         "Shelling `ssh <host> echo $USER` cannot work behind this "
                         "workstation's ControlMaster guard, and a loop around that call "
                         "is a Duo push per iteration -- a prior run in this project was "
                         "halted by an account lockout caused exactly that way")
    ap.add_argument("--scratch-root", default="/scratch/st-shallam-1",
                    help="sockeye scratch is ALLOCATION-scoped: there is no "
                         "/scratch/<user>, so this is the allocation and the user is "
                         "appended")
    ap.add_argument("--remote-data", default=None,
                    help="where the inputs live on the host. Defaults to "
                         "{scratch}/{user}/fabfos_r6/data, which is where the earlier "
                         "runs already staged MetaNetX, the eQuilibrator cache and the "
                         "drop-in")
    ap.add_argument("--no-push", action="store_true",
                    help="trust --remote-data is already populated and skip the rsync")
    ap.add_argument("--apptainer-cache", default=SOCKEYE_IMAGE_STORE,
                    help="persistent image store on the host. Must OUTLIVE a single run: "
                         "apptainer resolves ${APPTAINER_CACHEDIR:-<agent home>} and "
                         "agent home carries a per-run timestamp. On /arc rather than "
                         "/scratch because sockeye's scratch metadata layer is degraded "
                         "by orders of magnitude on small files")
    ap.add_argument("--slurm-account", default=SOCKEYE_ACCOUNT,
                    help="charged on every sbatch. slurm.nf's default is the literal "
                         "placeholder '<slurm_account>', which sbatch rejects")
    ap.add_argument("--container", default=SOCKEYE_CONTAINER,
                    help="the agent BASE image, which is the site's and NOT derived from "
                         "the engine hash -- the pinned engine reaches the run through "
                         "the dev overlay instead")
    ap.add_argument("--work", type=Path, default=None,
                    help="local scratch for this part's plan. Defaults to a per-part "
                         "directory so two parts cannot share an input library")
    ap.add_argument("--timeout-hours", type=float, default=36.0)
    ap.add_argument("--poll-s", type=float, default=120.0)
    ap.add_argument("--retrieve-only", metavar="RESULTS_PATH", default=None,
                    help="skip planning and execution; pull an ALREADY FINISHED run's "
                         "results directory from --host and route it. The path is the one "
                         "the run printed as `results: <host>:<path>`. Exists because "
                         "retrieval is separable from the run and a run is far too "
                         "expensive to repeat for a bug on this side of it")
    a = ap.parse_args()

    spec = BRANCHES[a.branch]
    work = a.work or (WORK_ROOT / a.branch)
    work.mkdir(parents=True, exist_ok=True)
    staging = TEMP / f"_run_{a.branch}"
    ts = int(time.time())

    if a.retrieve_only:
        return retrieve(a.retrieve_only, a.branch, a.host, staging)

    if a.run and not a.user:
        raise SystemExit("--run needs --user; see its help for why it is never guessed.")
    if a.run and spec["needs_metacyc"] and not GIVEN_AT.exists():
        raise SystemExit(
            f"the MetaCyc drop-in is not at {GIVEN_AT}.\n"
            f"  It is LICENSED and not redistributable, so nothing fetches it, and it is "
            f"the independent member of this part's ensemble -- without it the direction "
            f"ensemble keeps only its two CORRELATED members and has nothing that can "
            f"break a tie. Planning does not need it; drop --run to see the plan.")

    remote_root = None
    agent_home = None
    if a.run:
        remote_root = a.remote_data or f"{a.scratch_root}/{a.user}/fabfos_r6/data"
        agent_home = f"{a.scratch_root}/{a.user}/fabfos_r6/agent_{a.branch}_{ts}"

    # ---- the agent -------------------------------------------------------------
    if not a.run:
        # A local agent for planning only. The runtime still has to be APPTAINER: it is
        # what decides whether an env declaration resolves its `container:` or its
        # `conda:` key, and the plan is only a claim about the real run if it resolved the
        # same side of that fork.
        agent = Agent(home=Source.FromLocal(work / "agent_home"),
                      runtime=Runtime.APPTAINER)
    else:
        print(f"=== {a.branch} on {a.host}:{agent_home} ===", flush=True)
        print(f"=== agent base image: {a.container} ===", flush=True)
        print(f"=== image store: {a.apptainer_cache} ===", flush=True)

        if not a.no_push:
            print(f"=== pushing inputs -> {a.host}:{remote_root} ===", flush=True)
            push_data(a.host, a.branch, remote_root)
        print("=== placing images on the login node ===", flush=True)
        place_images(a.host, a.branch, a.apptainer_cache, a.container)

        agent = Agent(home=SshSource(host=a.host, path=agent_home).AsSource(),
                      runtime=Runtime.APPTAINER,
                      container=a.container,
                      # Exported for BOTH sides: the login node writes the store here and
                      # the compute node reads it here. One side missing the variable
                      # resolves a different directory, finds nothing, and attempts a pull
                      # on a node with no route out.
                      setup_commands=[c for c in SOCKEYE_SETUP_COMMANDS
                                      if not c.startswith("export APPTAINER_CACHEDIR=")]
                                     + [f"export APPTAINER_CACHEDIR={a.apptainer_cache}"])

    # ---- plan ------------------------------------------------------------------
    print("=== planning ===", flush=True)
    inputs, task, seam_missing = plan(work, a.branch, remote_root, agent)
    if not task.ok:
        print(f"FAILED to plan:\n{getattr(task.plan, 'hints', task.plan)}",
              file=sys.stderr)
        return 1

    used, problems = check_plan(task, a.branch)

    staged = [(p, n) for p, n, _ in inputs.Iterate()]
    print(f"\nstaged: {len(staged)} item(s) -- {sorted({n for _, n in staged})}")
    if spec["imports"]:
        print(f"imported from earlier parts: {sorted(spec['imports'])}")

    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    svg = ARTIFACTS / f"build_references_bake_{a.branch}.svg"
    try:
        task.plan.RenderDAG(svg, blacklist_namespaces={"lib", "env", "buildlib"})
        print(f"\nDAG -> {svg}")
    except Exception as e:
        # A picture, not a check. The gate above is the check, and losing the drawing
        # because graphviz's `dot` is not on this env's PATH must not stop a cluster run.
        print(f"\nDAG not rendered ({type(e).__name__}: {e})")

    for p in problems:
        print(f"\nFAIL: {p}")
    if problems:
        return 1
    print(f"\nexactly the {a.branch} part's lanes are in the plan, and nothing else")

    if not a.run:
        print("\n(plan only -- pass --run --user <name> to execute on the host)")
        return 0
    if seam_missing:
        raise SystemExit(
            f"\nthis part imports {sorted(spec['imports'])} and at least one local copy "
            f"is not here, so there was nothing to push and the remote path is empty. "
            f"Run the producing part first and retrieve it.")

    # ---- execute ---------------------------------------------------------------
    print("=== Deploy() ===", flush=True)
    try:
        agent.Deploy()
    except subprocess.CalledProcessError as e:
        # ONE session, one failure, one message. Never a retry loop: each attempt is a Duo
        # push, and a prior run in this project was halted by an account lockout caused
        # exactly that way.
        print(f"\ndeploy failed ({e}). The connection is multiplexed: open ONE session "
              f"by hand (`ssh {a.host}`), leave it open, and re-run. Do NOT delete the "
              f"ControlMaster socket and do NOT retry in a loop.", file=sys.stderr)
        return 4

    # The pinned engine, as BOTH artifacts -- see provision_dev_overlay_remote. Deploy()
    # binds `dev/metasmith` but does not create it, and every SLURM task stages the
    # tarball rather than the directory.
    provision_dev_overlay_remote(a.host, agent_home)

    print("=== preflight ===", flush=True)
    if preflight(a.host, agent_home, a.container, envs_from_plan(task),
                 mlib=MLIB, image_store=a.apptainer_cache):
        return 4

    print(f"=== task key: {task.GetKey()} ===", flush=True)
    # on_exist="clear" is safe HERE and only here: agent_home carries a timestamp, so it
    # is a fresh directory every run and there is no prior intermediate to destroy. Never
    # carry this flag onto a resubmission.
    agent.StageWorkflow(task, on_exist="clear")

    if check_staged_executor(a.host, agent_home, task.GetKey()):
        return 4
    declared = plan_resources(task)
    if check_walltimes(a.host, declared):
        return 4
    # The run's own directory, because sockeye's job_submit plugin refuses a submission
    # with no working directory -- and ssh lands in $HOME, which is not where work runs.
    if check_schedulable(a.host, a.slurm_account, declared,
                         workdir=f"{agent_home}/runs/{task.GetKey()}"):
        return 4

    # RunWorkflow's config_file DEFAULTS to the `local` preset, which would run every step
    # on whatever node the agent sits on -- the login node. Selected explicitly so a
    # future reader sees the choice rather than a default.
    params = {
        "slurmAccount": a.slurm_account,
        # Job arrays are the condition under which overlay filesystems throw bus errors,
        # and a part is at most nine steps -- there is nothing to batch that is worth the
        # exposure.
        "process_array": 0,
    }
    print(f"=== executor: slurm, account {a.slurm_account}, arrays disabled ===",
          flush=True)
    agent.RunWorkflow(task, config_file=agent.GetNxfConfigPresets()["slurm"],
                      params=params)

    print(f"=== waiting (timeout {a.timeout_hours:.0f}h, poll {a.poll_s:.0f}s) ===",
          flush=True)
    result = wait_for_run(agent, task, int(a.timeout_hours * 3600), a.poll_s)
    print(f"=== status: {result['status']} after {result['elapsed_s'] / 3600:.1f}h ===",
          flush=True)
    for line in result["tail"]:
        print(f"    {line}")
    if result["status"] != "completed":
        # The run is on the cluster whatever this driver decided. Say where, and say how
        # to pull it, so a verdict this side gets wrong costs a command rather than a run.
        print(f"\nIf the run did finish, its results are at\n"
              f"    {a.host}:{agent.GetResultSource(task).GetPath()}\n"
              f"and retrieval is separable:\n"
              f"    python tests/{Path(__file__).name} {a.branch} --retrieve-only "
              f"<that path>", file=sys.stderr)
        return 2

    # "COMPLETED" IS NOT "SUCCEEDED". slurm.nf sets errorStrategy='ignore' once a process
    # exhausts its retries, so a step that died every attempt leaves the workflow green,
    # everything downstream running on nothing, and a results directory that exists and is
    # empty. A previous sockeye run failed exactly this way -- a missing container read as
    # a 3.6-minute success. The task table is where the truth is.
    if check_tasks(a.host, agent_home, task.GetKey()):
        print("\nA STEP FAILED AND NEXTFLOW IGNORED IT -- this is not a result. The real "
              "error is in that step's .command.err under <run>/nxf_work/<hash>/.",
              file=sys.stderr)
        return 3

    src = agent.GetResultSource(task)
    print(f"\nresults: {a.host}:{src.GetPath()}")
    return retrieve(src.GetPath(), a.branch, a.host, staging)


if __name__ == "__main__":
    sys.exit(main())

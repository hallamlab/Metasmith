#!/usr/bin/env python3
"""R6, the metabolism bake, in PARTS -- planned here, executed on Sockeye under SLURM.

    PYTHONPATH=src python tests/fabfos/build_references_bake_parts_on_hpc.py direction
    ... build_references_bake_parts_on_hpc.py members --run --user txyliu

WHY PARTS AND NOT ONE RUN. `build_references_bake_on_hpc.py` plans every lane and submits
them as one graph. That is the right shape when one host owns the whole thing and the
wrong shape the moment two branches are in flight on different machines: the AAM branch is
fifteen lanes and most of a day, the direction branch is a handful of table operations, and
a single plan makes the second wait on the first even where no data flows between them.
This driver runs ONE part at a time and treats the boundary between parts as a declared
import.

WHY THE PARTS ARE WHERE THEY ARE. Some of the boundaries are the IMAGE boundary, which
every bake transform already declares as its `group_by` -- so a part pulls one image and
owns one coherent piece. Two are not. One is the reason this file exists:
`direction_ensemble` used to require `ref::metabolism_vocab` and so sat strictly
downstream of the AAM branch, for an ENCODING it does in its last four lines. That encode
is `direction_bake` now, and the science runs the moment the two thermodynamic members
are in. See build_references/transforms/bake/direction_bake.py.

    lookups        -> the five lookup:: tables          first, alone -- everything reads them
    prepare        -> interm::aam_universe              nine lanes, no mapper, ~an hour
    map            -> the three member tables           three lanes, most of a day
    assemble       -> vocab, atom_pairs                 fuse, correct, mint
    members        -> direction_member_eq, _dgbyg       concurrent with all three
    direction      -> direction_annotation              concurrent with them, after members
    direction_bake -> direction_ratios                  seconds, once assemble lands

THE AAM PARTS ARE THE THREE STAGES OF THE GRAPH, which is what makes their seams natural
rather than negotiated: `prepare` ends where the last thing that can be settled without a
mapper is settled, `map` ends at the three member tables, `assemble` ends at the reference
trio. Splitting there means a failed assembly re-maps nothing, and a method change in the
preparation costs an hour rather than a day. `redox` and `reference` are recovery cuts of
the assembly and are not part of the normal route -- see BRANCHES.

THE REUSE AUDIT IS WHY --run ON `map` IS NOT JUST A SUBMISSION. Before any job is placed
the driver decomposes each member's work against the STAGED cache -- universe by
submission class, what the cache holds as finished, what the sidecars hold as attempted,
and the todo those leave -- and REFUSES when a cache that holds rows leaves a todo within
a few percent of the whole universe. That is a cache that did not take, and the only thing
distinguishing it from an honest first run is that somebody said there was one. It reads
the cache off the REMOTE, because a path that resolves on this workstation and not on the
cluster is exactly the failure it exists to catch. `--audit` runs it and stops. The
gap-filling member is asked a different question -- see GAP_FILLERS, whose universe column
is an upper bound it never reaches, so only its resume rate carries any signal.

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
import hashlib
import shutil
import subprocess
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]

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

GIVEN_TYPE = "fabfos_data::metacyc"
GIVEN_AT = DATA / "originals" / "metacyc"

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
    "fabfos_data::prior_bake_logs": "processed/metabolism_bake/logs",
    "fabfos_data::aam_cache":       "temp/aam_cache",
}

MEMBER_ADMITS = {
    "rxnmapper":   ("mappable",),
    "localmapper": ("mappable",),
    "indigo":      ("mappable", "oversize"),
}

# THE MEMBER THAT IS A GAP-FILLER RATHER THAN A THIRD VOTE, and the reason the audit has
# to know. `localmapper.py` takes the indigo and rxnmapper products as REQUIREMENTS and
# hands them to the member as `--covered`, so at run time it maps only what those two left
# behind -- a couple of thousand reactions, which is what its six shards and twelve hours
# are sized for. Its `universe` column below is therefore an upper bound it never reaches,
# and the whole-universe todo ratio says nothing about whether its cache took: on the r7
# gapfill the cache resumed 2,258 rows against 2,256 previously attempted -- a COMPLETE
# resume -- and the ratio test read it as 97.2% unresumed and refused the run.
GAP_FILLERS = {"localmapper"}

DEPLOYED_BAKE = DATA / "processed" / "metabolism_bake"

METACYC_FILES = ("atom-mappings-smiles.dat", "reactions.dat", "compounds.dat")

# `data/.gitignore` is `/*/*` with a `.dvc` negation, so this is already ignored and must
# NOT get a .gitignore of its own -- that file is the only one under data/ and the tier
# rule depends on it staying that way.
TEMP = DATA / "temp"

BAKE_SIF_DIR = REPO / "docker" / "ecspr_bake"
IMG = "docker://quay.io/hallamlab/ecspr_bake:{}"

BRANCHES = {
    "lookups": dict(
        images=[IMG.format("aam")],
        lanes={"mnx_lookups"},
        targets=["lookup::reactions", "lookup::metabolites", "lookup::atom_ranks",
                 "lookup::xrefs", "lookup::synonyms"],
        inputs=["fabfos_data::metanetx", "fabfos_data::chebi",
                "fabfos_data::modelseed"],
        imports={},
        needs_metacyc=True,
        evidence={},
        outputs={
            "lookup::reactions":   "processed/lookups/reactions.parquet",
            "lookup::metabolites": "processed/lookups/metabolites.parquet",
            "lookup::atom_ranks":  "processed/lookups/atom_ranks.parquet",
            "lookup::xrefs":       "processed/lookups/xrefs.parquet",
            "lookup::synonyms":    "processed/lookups/synonyms.parquet",
        },
    ),
    "prepare": dict(
        images=[IMG.format("aam")],
        lanes={
            "aam_recount", "aam_worklist", "aam_blockers", "aam_nametwin",
            "aam_rescue",
            "aam_algebra", "aam_forecast", "aam_partial", "aam_universe",
        },
        targets=["interm::aam_universe"],
        inputs=[
            "fabfos_data::chebi", "fabfos_data::modelseed",
            "fabfos_data::prior_bake_logs",
            "lookup::reactions", "lookup::metabolites", "lookup::atom_ranks",
            "lookup::xrefs", "lookup::synonyms",
        ],
        imports={},
        needs_metacyc=False,
        evidence={
            "recount":  "aam_recount",
            "worklist": "aam_worklist",
            "blockers": "aam_blockers",
            "nametwin": "aam_nametwin",
            "rescue":   "aam_rescue",
            "algebra":  "aam_algebra",
            "forecast": "aam_forecast",
            "partial":  "aam_partial",
            "universe": "aam_universe",
        },
        outputs={
            "interm::aam_universe":    "temp/_seams/aam_universe.parquet",
            "interm::aam_worklist":    "temp/_seams/aam_worklist.parquet",
            "interm::aam_forecast":    "temp/_seams/aam_forecast.parquet",
            "interm::aam_rescue":      "temp/_seams/aam_rescue",
            "interm::aam_partial":     "temp/_seams/aam_partial",
            "interm::aam_algebra":     "temp/_seams/aam_algebra",
            "lookup::element_counts":  "temp/_seams/element_counts.parquet",
        },
    ),
    "map": dict(
        images=[IMG.format("aam")],
        lanes={"rxnmapper", "indigo", "localmapper"},
        targets=["interm::aam_member_rxnmapper", "interm::aam_member_indigo",
                 "interm::aam_member_localmapper"],
        inputs=["fabfos_data::metanetx", "fabfos_data::aam_cache"],
        imports={
            "interm::aam_universe": "temp/_seams/aam_universe.parquet",
            "interm::aam_rescue":   "temp/_seams/aam_rescue",
        },
        needs_metacyc=False,
        evidence={"rxnmapper": "rxnmapper", "indigo": "indigo",
                  "localmapper": "localmapper"},
        outputs={
            "interm::aam_member_rxnmapper":   "temp/_seams/aam_member_rxnmapper.parquet",
            "interm::aam_member_indigo":      "temp/_seams/aam_member_indigo.parquet",
            "interm::aam_member_localmapper": "temp/_seams/aam_member_localmapper.parquet",
        },
    ),
    "assemble": dict(
        images=[IMG.format("aam")],
        lanes={"aam_stack", "aam_redox", "aam_reference"},
        targets=["ref::atom_pairs", "ref::metabolism_vocab"],
        inputs=["fabfos_data::metanetx",
                "lookup::reactions", "lookup::metabolites", "lookup::atom_ranks"],
        imports={
            "interm::aam_member_rxnmapper":   "temp/_seams/aam_member_rxnmapper.parquet",
            "interm::aam_member_indigo":      "temp/_seams/aam_member_indigo.parquet",
            "interm::aam_member_localmapper": "temp/_seams/aam_member_localmapper.parquet",
            "interm::aam_worklist":           "temp/_seams/aam_worklist.parquet",
            "interm::aam_forecast":           "temp/_seams/aam_forecast.parquet",
            "interm::aam_rescue":             "temp/_seams/aam_rescue",
            "interm::aam_partial":            "temp/_seams/aam_partial",
            "interm::aam_algebra":            "temp/_seams/aam_algebra",
        },
        needs_metacyc=True,
        evidence={
            "metacyc":   "aam_stack",
            "stack":     "aam_stack",
            "redox":     "aam_redox",
            "reference": "aam_reference",
        },
        outputs={
            "ref::metabolism_vocab": "temp/metabolism/vocab.parquet",
            "ref::atom_pairs":       "temp/metabolism/atom_pairs.parquet",
            "interm::aam_stack":     "temp/_seams/aam_stack.parquet",
            "interm::aam_pairs":     "temp/_seams/aam_pairs",
            "interm::aam_ledger":    "temp/_seams/aam_ledger.parquet",
        },
    ),
    "redox": dict(
        images=[IMG.format("aam")],
        lanes={"aam_redox"},
        targets=["interm::aam_pairs"],
        inputs=["lookup::reactions", "lookup::metabolites", "lookup::atom_ranks"],
        imports={"interm::aam_stack": "temp/_seams/aam_stack.parquet"},
        needs_metacyc=False,
        evidence={"redox": "aam_redox"},
        outputs={"interm::aam_pairs": "temp/_seams/aam_pairs"},
    ),
    "reference": dict(
        images=[IMG.format("aam")],
        lanes={"aam_reference"},
        targets=["ref::atom_pairs", "ref::metabolism_vocab"],
        inputs=["lookup::reactions"],
        imports={
            "interm::aam_pairs":    "temp/_seams/aam_pairs",
            "interm::aam_worklist": "temp/_seams/aam_worklist.parquet",
            "interm::aam_forecast": "temp/_seams/aam_forecast.parquet",
            "interm::aam_rescue":   "temp/_seams/aam_rescue",
        },
        needs_metacyc=False,
        evidence={"reference": "aam_reference"},
        outputs={
            "ref::metabolism_vocab": "temp/metabolism/vocab.parquet",
            "ref::atom_pairs":       "temp/metabolism/atom_pairs.parquet",
            "interm::aam_ledger":    "temp/_seams/aam_ledger.parquet",
        },
    ),
    "members": dict(
        images=[IMG.format("direction"), IMG.format("dgbyg")],
        lanes={"equilibrator", "dgbyg"},
        targets=["interm::direction_member_eq", "interm::direction_member_dgbyg"],
        inputs=["fabfos_data::metanetx", "fabfos_data::equilibrator"],
        imports={},
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
        targets=["interm::direction_annotation"],
        inputs=["fabfos_data::metanetx"],
        imports={
            "interm::direction_member_eq":    "temp/_seams/direction_member_eq.parquet",
            "interm::direction_member_dgbyg": "temp/_seams/direction_member_dgbyg.parquet",
        },
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
            "ref::metabolism_vocab":        "temp/metabolism/vocab.parquet",
            "interm::direction_annotation": "temp/_seams/direction_annotation.parquet",
        },
        needs_metacyc=False,
        evidence={},
        outputs={"ref::direction_ratios": "temp/metabolism/direction.parquet"},
    ),
}


def assert_seams_are_outputs() -> None:
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
    return image.replace("://", "..").replace(":", "..").replace("/", "_") + ".sif"


def build_inputs(work: Path, branch: str, remote_root: str | None):
    spec = BRANCHES[branch]
    inputs = DataInstanceLibrary(work / "inputs.xgdb")
    for tl in TYPE_LIBRARIES:
        inputs.AddTypeLibrary(tl)

    def declared(rel: str) -> Path:
        return Path(remote_root) / rel if remote_root else DATA / rel

    missing, imported_missing = [], []
    for dtype in sorted(spec["inputs"]):
        rel = ALL_INPUTS[dtype]
        if dtype == "fabfos_data::aam_cache":
            (DATA / rel).mkdir(parents=True, exist_ok=True)
        if not (DATA / rel).exists():
            missing.append(f"{dtype:32s} data/{rel}")
            continue
        inputs.AddItem(declared(rel), dtype)

    for dtype, rel in sorted(spec["imports"].items()):
        if not (DATA / rel).exists():
            imported_missing.append(f"{dtype:32s} data/{rel}")
        inputs.AddItem(declared(rel), dtype)

    given = GIVEN_AT
    if not given.exists():
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
    out = {}
    for step in task.plan.steps:
        r = getattr(step.transform, "resources", None)
        if r is not None:
            out[Path(step.transform._path).stem] = r
    return out


def push_data(host: str, branch: str, remote_root: str) -> None:
    spec = BRANCHES[branch]
    rels = [ALL_INPUTS[t] for t in spec["inputs"]] + list(spec["imports"].values())
    if spec["needs_metacyc"]:
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
        subprocess.run(["rsync", "-a", "--size-only", "--partial", "--info=stats1",
                        f"{src}/" if src.is_dir() else str(src),
                        f"{host}:{remote_root}/{rel}"], check=True)


def place_images(host: str, branch: str, cache_dir: str, container: str) -> None:
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
MIN_POLL_S = 90.0


def wait_for_run(agent, task, timeout_s: int, poll_s: float) -> dict:
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
        print(f"=== the lock exists now (`{again['status']}`): the run had not started "
              f"when it was first asked. Resuming the wait ===", flush=True)
        second = agent.WaitForWorkflow(task, timeout_s=timeout_s, poll_s=poll_s)
        second["elapsed_s"] += result["elapsed_s"] + waited
        return second
    return result


def recover_evidence(run_dir: str, host: str, missing: list[str], staging: Path) -> dict:
    if not (missing and host and run_dir):
        return {}
    names = " -o ".join(f"-name {t}" for t in missing if t.replace("_", "").isalnum())
    if not names:
        return {}
    listing = subprocess.run(
        ["ssh", host, f"find {run_dir}/nxf_work -mindepth 4 -maxdepth 4 -type d "
                      f"\\( {names} \\) 2>/dev/null"],
        capture_output=True, text=True)
    hits: dict[str, str] = {}
    for line in listing.stdout.splitlines():
        line = line.strip()
        if line:
            hits.setdefault(Path(line).name, line)
    out: dict[str, Path] = {}
    for tool, remote in sorted(hits.items()):
        dest = staging / "_recovered" / tool
        dest.mkdir(parents=True, exist_ok=True)
        subprocess.run(["rsync", "-aL", "--partial", f"{host}:{remote}/", f"{dest}/"],
                       check=True)
        print(f"  RECOVERED {tool:<14} from nxf_work -- publishing dropped it on an "
              f"artifact-id collision")
        out[tool] = dest
    return out


def retrieve(src_path: str, branch: str, host: str, staging: Path) -> int:
    spec = BRANCHES[branch]
    staging.mkdir(parents=True, exist_ok=True)
    print(f"\n=== retrieving {branch} into {TEMP.relative_to(REPO)} ===", flush=True)
    subprocess.run(["rsync", "-aL", "--delete", "--partial", "--info=stats1",
                    f"{host}:{src_path}/", f"{staging}/"], check=True)

    lib = DataInstanceLibrary.Load(staging)
    found: dict[str, Path] = {}
    products: dict[str, Path] = {}
    for rel, dtype_name, _ in lib.Iterate():
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

    for d in sorted(p for p in staging.iterdir() if p.is_dir()):
        stem = d.name.split("_", 1)[-1] if d.name[0].isdigit() else d.name
        ns, _, tname = stem.partition("-")
        dtype = f"{ns}::{tname}"
        artifacts = sorted(p for p in d.iterdir() if p.is_dir())
        if dtype == "evidence::tool_output":
            for artifact in artifacts:
                for tool_dir in sorted(p for p in artifact.iterdir() if p.is_dir()):
                    found.setdefault(tool_dir.name, tool_dir)
        elif dtype in spec["outputs"] and dtype not in products:
            if len(artifacts) == 1:
                products[dtype] = artifacts[0]
            elif artifacts:
                print(f"  AMBIGUOUS: {dtype} has {len(artifacts)} published artifacts "
                      f"under {d.name}; refusing to choose", file=sys.stderr)

    for tool, tool_dir in recover_evidence(
            str(Path(src_path).parent), host,
            sorted(set(spec["evidence"]) - set(found)), staging).items():
        found.setdefault(tool, tool_dir)

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
        if p.is_dir():
            if dest.exists():
                shutil.rmtree(dest)
            shutil.copytree(p, dest)
            size = sum(f.stat().st_size for f in dest.rglob("*") if f.is_file())
        else:
            shutil.copy2(p, dest)
            size = dest.stat().st_size
        print(f"  {dtype_name:<32} -> {dest.relative_to(REPO)}  ({size / 1e6:.2f} MB)")

    if branch == "map":
        promote_cache(found)
    if branch in ("direction_bake", "reference"):
        promote_logs()

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


CACHE_LOCAL = DATA / ALL_INPUTS["fabfos_data::aam_cache"]

CACHE_DIGEST_PY = r"""
import hashlib, os, sys
root = sys.argv[1]
if not os.path.isdir(root):
    print("MISSING\t" + root)
    raise SystemExit(0)
print("PRESENT\t" + root)
for member in sorted(os.listdir(root)):
    d = os.path.join(root, member)
    if not os.path.isdir(d):
        continue
    for dirpath, _dirs, files in os.walk(d):
        for fn in sorted(files):
            p = os.path.join(dirpath, fn)
            if fn.endswith(".attempted"):
                with open(p) as fh:
                    for line in fh:
                        s = line.strip()
                        if s and not s.startswith("#"):
                            print(member + "\tattempted\t" + s)
            elif fn.endswith(".tsv"):
                with open(p) as fh:
                    head = fh.readline().rstrip("\n").split("\t")
                    if "mnxr" not in head or "rxn_smiles" not in head:
                        continue
                    i, j = head.index("mnxr"), head.index("rxn_smiles")
                    for line in fh:
                        f = line.rstrip("\n").split("\t")
                        if len(f) <= max(i, j):
                            continue
                        h = hashlib.sha1(f[j].encode()).hexdigest()[:16]
                        print(member + "\tcache\t" + f[i] + "\t" + h)
"""

FULL_REMAP_FRACTION = 0.95


def read_staged_cache(host: str, remote_cache: str) -> tuple[bool, dict, dict]:
    return parse_cache_digest(
        ssh_once(host, f"python3 - {remote_cache} <<'PYEOF'\n{CACHE_DIGEST_PY}\nPYEOF\n"))


def parse_cache_digest(out: str) -> tuple[bool, dict, dict]:
    finished: dict[str, dict[str, str]] = {}
    attempted: dict[str, set[str]] = {}
    present = False
    for line in out.splitlines():
        f = line.rstrip("\n").split("\t")
        if f[0] == "PRESENT":
            present = True
        elif f[0] == "MISSING":
            present = False
        elif len(f) == 4 and f[1] == "cache":
            finished.setdefault(f[0], {})[f[2]] = f[3]
        elif len(f) == 3 and f[1] == "attempted":
            attempted.setdefault(f[0], set()).add(f[2])
    return present, finished, attempted


def deployed_reactions() -> set[str]:
    import pandas as pd

    pairs, vocab = DEPLOYED_BAKE / "atom_pairs.parquet", DEPLOYED_BAKE / "vocab.parquet"
    if not (pairs.exists() and vocab.exists()):
        return set()
    codes = set(pd.read_parquet(pairs, columns=["rxn"])["rxn"].unique().tolist())
    v = pd.read_parquet(vocab)
    v = v[v["kind"] == "rxn"]
    return {s for c, s in zip(v["code"], v["symbol"]) if c in codes}


def decompose_member(keys: set, want: dict, have: dict, tried_all: set,
                     covered: set, base_of: dict):
    reusable = {k for k in keys if k in have and want.get(k) == have[k]}
    stale = sum(1 for k, h in have.items() if want.get(k) != h)
    tried = tried_all & keys
    todo = keys - reusable - tried
    todo_new = ({base_of.get(k, k) for k in todo} - covered) if covered else None
    return reusable, stale, tried, todo, todo_new


def audit(host: str, remote_root: str) -> list[str]:
    import pandas as pd

    problems: list[str] = []
    uni_rel = BRANCHES["map"]["imports"]["interm::aam_universe"]
    uni_local = DATA / uni_rel
    if not uni_local.exists():
        return [f"cannot audit: the universe is not at data/{uni_rel}. Run `prepare` and "
                f"retrieve it first -- the audit reads the same table the members will."]

    u = pd.read_parquet(uni_local, columns=["mnxr", "verdict", "rxn_smiles",
                                            "base_mnxr", "submission_class"])
    u = u[u["rxn_smiles"].notna()]
    want = {r.mnxr: hashlib.sha1(str(r.rxn_smiles).encode()).hexdigest()[:16]
            for r in u.itertuples(index=False)}
    base_of = dict(zip(u["mnxr"], u["base_mnxr"]))

    remote_cache = f"{remote_root}/{ALL_INPUTS['fabfos_data::aam_cache']}"
    print(f"\n=== reuse audit -- staged cache at {host}:{remote_cache} ===", flush=True)
    present, finished, attempted = read_staged_cache(host, remote_cache)
    if not present:
        print("  the staged cache directory is NOT on the remote. That is the ordinary "
              "first-run state and it is what --run creates; every member below will map "
              "its whole universe.")

    covered = deployed_reactions()
    print(f"  deployed bake covers {len(covered):,} reactions"
          if covered else
          "  the deployed bake is not on disk, so the last column is omitted rather "
          "than reported as zero")

    print(f"\n  {'member':<12} {'universe':>9} {'whole':>8} {'compl':>7} {'reduc':>7} "
          f"{'cached':>8} {'tried':>8} {'todo':>9} {'todo new':>9}")
    for member, admits in sorted(MEMBER_ADMITS.items()):
        mine = u[u["verdict"].isin(admits)]
        keys = set(mine["mnxr"])
        by_class = mine["submission_class"].value_counts().to_dict()

        have = finished.get(member, {})
        reusable, stale, tried, todo, todo_new = decompose_member(
            keys, want, have, attempted.get(member, set()), covered, base_of)

        print(f"  {member:<12} {len(keys):>9,} {by_class.get('whole', 0):>8,} "
              f"{by_class.get('completed', 0):>7,} {by_class.get('reduced', 0):>7,} "
              f"{len(reusable):>8,} {len(tried):>8,} {len(todo):>9,} "
              f"{(len(todo_new) if todo_new is not None else 0):>9,}")
        if stale:
            print(f"    {stale:,} cached rows are for a submission string this run would "
                  f"not send, and are not counted as finished")

        if member in GAP_FILLERS:
            print(f"    gap-filler: bounded at run time to what indigo and rxnmapper "
                  f"leave, so the todo above is an upper bound it does not reach")
            if have and tried and len(reusable) < FULL_REMAP_FRACTION * len(tried):
                problems.append(
                    f"{member}: the staged cache holds {len(have):,} rows but resumes "
                    f"only {len(reusable):,} of the {len(tried):,} submissions this "
                    f"member previously reached "
                    f"({len(reusable) / len(tried):.1%}). For a gap-filler that is the "
                    f"whole signal -- its todo is most of its universe by construction --"
                    f" and it says the cache did not take: either its member directory "
                    f"is not `{remote_cache}/{member}`, or the submission strings moved "
                    f"({stale:,} rows were dropped as stale).")
        elif have and keys and len(todo) >= FULL_REMAP_FRACTION * len(keys):
            problems.append(
                f"{member}: the staged cache holds {len(have):,} rows and the todo is "
                f"still {len(todo):,} of {len(keys):,} ({len(todo) / len(keys):.1%}). A "
                f"cache that resumes nothing is not a cache -- either its member "
                f"directory is not `{remote_cache}/{member}`, or every submission string "
                f"moved ({stale:,} rows were dropped as stale) and this is a re-map "
                f"wearing a resume's clothes. Submitting would cost the whole lane again.")

    return problems


def audit_plan(used: set[str], branch: str) -> list[str]:
    problems = []
    if "mnx_lookups" in used and "mnx_lookups" not in BRANCHES[branch]["lanes"]:
        problems.append(
            "mnx_lookups is in the plan. The five lookup:: tables are staged, all five or "
            "none -- four satisfy four types and leave this lane to produce the fifth, "
            "which re-derives all five and hands the run two generations of one table. "
            "Run the `lookups` part and check data/fabfos/processed/lookups/.")
    ghosts = sorted(l for l in used if l.endswith(("_rescue", "_partial"))
                    and l not in ("aam_rescue", "aam_partial"))
    if ghosts:
        problems.append(
            f"deleted mapper lanes are in the plan: {ghosts}. The three members run ONCE "
            f"over `interm::aam_universe`, which carries all three submission classes; "
            f"these six transforms and their types were removed in T5. A plan holding one "
            f"is reading a transform library from before that.")
    return problems


def promote_cache(found: dict) -> None:
    for member in sorted(MEMBER_ADMITS):
        src = found.get(member)
        if src is None:
            continue
        dest = CACHE_LOCAL / member
        dest.mkdir(parents=True, exist_ok=True)
        merged = sorted(p for p in src.rglob(f"{member}.tsv"))
        sidecars = sorted(src.rglob("*.attempted"))
        for p in merged:
            cur = dest / "cache.tsv"
            if cur.exists():
                shutil.copy2(cur, dest / "cache.prev.tsv")
            shutil.copy2(p, cur)
        for p in sidecars:
            shutil.copy2(p, dest / p.name)
        n = sum(1 for _ in (dest / "cache.tsv").open()) - 1 if (dest / "cache.tsv").exists() else 0
        print(f"  cache    {member:<12} {n:,} rows, {len(sidecars)} sidecar(s) "
              f"-> {dest.relative_to(REPO)}")
    print("  the next run stages this directory; nothing here is pinned or committed.")


def promote_logs() -> None:
    from ecspr.bake.aam import runlogs                                  # noqa: PLC0415

    curated = sorted(TEMP.glob("metacyc/*/status.tsv"))
    runs = sorted(p for p in TEMP.glob("_run_*") if p.is_dir())
    argv = ["build", "--cache", str(CACHE_LOCAL), "--evidence", str(TEMP),
            "--out", str(TEMP / "logs")]
    if curated:
        argv += ["--curated-status", str(curated[-1])]
    if runs:
        argv += ["--runs"] + [str(p) for p in runs]
    a = runlogs.parse_args(argv)
    a.fn(a)
    print(f"  logs     -> {(TEMP / 'logs').relative_to(REPO)}; they belong in the bake "
          f"chunk beside the trio, which is where the next run stages them from.")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("branch", choices=sorted(BRANCHES),
                    help="which part of the graph this run owns. lookups goes first and "
                         "alone; then prepare -> map -> assemble in order, with members "
                         "beside them and direction after members; direction_bake waits "
                         "on assemble and direction. redox and reference are recovery cuts "
                         "of assemble, not steps of the normal route")
    ap.add_argument("--run", action="store_true",
                    help="execute on the host. Without it this plans, checks and renders "
                         "the DAG, and touches no remote machine")
    ap.add_argument("--audit", action="store_true",
                    help="run the reuse audit and stop. Needs --user, because the whole "
                         "point is to read the cache STAGED on the remote rather than the "
                         "copy here. It runs anyway before `map` is submitted")
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
    if "fabfos_data::aam_cache" in spec["inputs"]:
        CACHE_LOCAL.mkdir(parents=True, exist_ok=True)
    work = a.work or (WORK_ROOT / a.branch)
    work.mkdir(parents=True, exist_ok=True)
    staging = TEMP / f"_run_{a.branch}"
    ts = int(time.time())

    if a.retrieve_only:
        return retrieve(a.retrieve_only, a.branch, a.host, staging)

    if (a.run or a.audit) and not a.user:
        raise SystemExit("--run and --audit both need --user; see its help for why it is "
                         "never guessed.")
    if a.run and spec["needs_metacyc"] and not GIVEN_AT.exists():
        raise SystemExit(
            f"the MetaCyc drop-in is not at {GIVEN_AT}.\n"
            f"  It is LICENSED and not redistributable, so nothing fetches it, and it is "
            f"the independent member of this part's ensemble -- without it the direction "
            f"ensemble keeps only its two CORRELATED members and has nothing that can "
            f"break a tie. Planning does not need it; drop --run to see the plan.")

    remote_root = None
    agent_home = None
    if a.run or a.audit:
        remote_root = a.remote_data or f"{a.scratch_root}/{a.user}/fabfos_r6/data"
        agent_home = f"{a.scratch_root}/{a.user}/fabfos_r6/agent_{a.branch}_{ts}"

    if not a.run:
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

    print("=== planning ===", flush=True)
    inputs, task, seam_missing = plan(work, a.branch, remote_root, agent)
    if not task.ok:
        print(f"FAILED to plan:\n{getattr(task.plan, 'hints', task.plan)}",
              file=sys.stderr)
        return 1

    used, problems = check_plan(task, a.branch)
    problems += audit_plan(used, a.branch)

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
        print(f"\nDAG not rendered ({type(e).__name__}: {e})")

    for p in problems:
        print(f"\nFAIL: {p}")
    if problems:
        return 1
    print(f"\nexactly the {a.branch} part's lanes are in the plan, and nothing else")

    if a.branch == "map" and (a.run or a.audit):
        audit_problems = audit(a.host, remote_root)
        for p in audit_problems:
            print(f"\nFAIL: {p}")
        if audit_problems:
            return 1
    if a.audit:
        print("\n(audit only -- nothing was submitted)")
        return 0

    if not a.run:
        print("\n(plan only -- pass --run --user <name> to execute on the host)")
        return 0
    if seam_missing:
        raise SystemExit(
            f"\nthis part imports {sorted(spec['imports'])} and at least one local copy "
            f"is not here, so there was nothing to push and the remote path is empty. "
            f"Run the producing part first and retrieve it.")

    print("=== Deploy() ===", flush=True)
    try:
        agent.Deploy()
    except subprocess.CalledProcessError as e:
        print(f"\ndeploy failed ({e}). The connection is multiplexed: open ONE session "
              f"by hand (`ssh {a.host}`), leave it open, and re-run. Do NOT delete the "
              f"ControlMaster socket and do NOT retry in a loop.", file=sys.stderr)
        return 4

    provision_dev_overlay_remote(a.host, agent_home)

    print("=== preflight ===", flush=True)
    if preflight(a.host, agent_home, a.container, envs_from_plan(task),
                 mlib=MLIB, image_store=a.apptainer_cache):
        return 4

    print(f"=== task key: {task.GetKey()} ===", flush=True)
    agent.StageWorkflow(task, on_exist="clear")

    if check_staged_executor(a.host, agent_home, task.GetKey()):
        return 4
    declared = plan_resources(task)
    if check_walltimes(a.host, declared):
        return 4
    if check_schedulable(a.host, a.slurm_account, declared,
                         workdir=f"{agent_home}/runs/{task.GetKey()}"):
        return 4

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

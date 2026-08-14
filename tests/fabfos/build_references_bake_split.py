#!/usr/bin/env python3
"""R6, the metabolism bake: planned here, executed on hardware we control, in three parts.

    PYTHONPATH=src python tests/fabfos/build_references_bake_split.py aam
    ... build_references_bake_split.py members --run --host chamois --user tliu

WHY A SIBLING AND NOT A FLAG ON THE SOCKEYE DRIVER. `build_references_bake_on_hpc.py`
plans the whole twelve-lane graph and runs it under SLURM, and its Sockeye assumptions are
load-bearing rather than incidental: the two-step Lmod load whose order matters, the
allocation-scoped scratch path that has no per-user directory, the pre-pull onto a login
node because compute nodes have no route out, the assertion that all twelve lanes are
present. Threading a conditional through each of those would leave two half-explained
paths instead of two explained ones. This file is the off-cluster driver; that one stays
the record of how the cluster works.

WHY OFF-CLUSTER AT ALL. Three runs died in staging on Sockeye and the cause was not
metasmith: the allocation's Lustre scratch is degraded to the point where a three-byte
write took eighteen seconds on the same host where a write to /arc/home took six
milliseconds, and it reproduced over Globus's separate transfer network, which rules out
the link. The other doors are shut too -- Sockeye forbids compute-node writes to project
space and forbids submitting from it, fir's compute nodes are held for maintenance.

THE CUT IS THE IMAGE BOUNDARY, and that is not a coincidence to be explained but the
reason the split is cheap. Every bake transform declares `group_by=image`, so the three
environments already partition the twelve lanes into groups that share nothing but tables:

    aam        9 lanes   worklist, three mappers x two passes, the rescue, the assembly
    direction  2 lanes   equilibrator, direction_ensemble
    dgbyg      1 lane    dgbyg

So the host boundary goes there, and the only things crossing a host are things that
already cross an image boundary inside the graph. `direction` and `dgbyg` ride the same
host because the second is fifty minutes of work with no reason to hold a machine of its
own -- see bake/dgbyg.py for the measurement that replaced its twenty-four-hour guess.

THREE PARTS, NOT TWO, AND THE THIRD IS ABOUT WALL CLOCK. `direction_ensemble` needs the
vocabulary the AAM assembly mints, so it cannot start until the long branch finishes -- but
the two thermodynamic MEMBERS need nothing from it. Folding them into one direction run
would idle a whole machine for a day and then ask it for several more hours. So `members`
runs first and concurrently with `aam`, and `direction` is the few minutes of table
arithmetic that waits for both.

    aam        -> vocab, atom_pairs                      the long branch, its own host
    members    -> direction_member_eq, _dgbyg            concurrent with it
    direction  -> direction_ratios                       waits on both

EVERY SEAM IS A DECLARED IMPORT, which is the mechanism this graph already uses everywhere
else rather than one invented for the split. The five `lookup::` tables are PRODUCED by
`mnx_lookups` and are nevertheless handed to every run of this driver as givens, by adding
a path and a type to the input library; a member table or a vocabulary crossing between
runs is the same move with a different type. What makes it safe is the gate below: an
import that fails to satisfy its type does not error, it quietly leaves the PRODUCER in
the plan -- so the direction run would silently redo the entire AAM branch. Asserting the
plan is exactly this part's lanes is what turns that into a refusal. Every import here is
some other part's declared OUTPUT, at the same path, which is checked on startup.
"""
from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]

# The PINNED engine, ahead of whatever is installed -- same reasoning as the HPC driver:
# the agent container tag is derived from the engine's own build hash, so importing the
# wrong one asks quay for a manifest that does not exist.
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

MLIB = REPO / "src" / "metasmith_libraries"
BREF = REPO / "src" / "fabfos" / "build_references"
DATA = REPO / "data" / "fabfos"
ARTIFACTS = REPO / "tests" / "fabfos" / "artifacts"
WORK_ROOT = DATA / "scratch" / "r6_bake_split"

TYPE_LIBRARIES = (
    [MLIB / "data_types" / f for f in
     ("ncbi.yml", "sequences.yml", "annotation.yml", "ref.yml", "lib.yml")]
    + [BREF / "data_types" / f for f in
       ("fabfos_data.yml", "raw.yml", "interm.yml", "bench.yml", "buildlib.yml",
        "lookup.yml", "evidence.yml")]
)

GIVEN_TYPE = "fabfos_data::metacyc"
GIVEN_AT = DATA / "originals" / "metacyc"

# Everything any part might stage, keyed by type, valued by its path under data/. The
# per-branch sets below select from this, so the re-rooting onto a remote host and the
# local existence check cannot drift apart.
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
# 1.6 GB of which 60 MB is read, and it is not redistributable. Two of the three parts run
# an assembly that reads it -- the AAM one takes atom-mappings-smiles.dat, the direction
# one takes reactions.dat -- so those hosts get the same three files.
METACYC_FILES = ("atom-mappings-smiles.dat", "reactions.dat", "compounds.dat")

# Where a finished run lands. `data/.gitignore` is `/*/*` with a `.dvc` negation, so this
# is already ignored and must NOT get a .gitignore of its own -- that file is the only one
# under data/ and the tier rule depends on it staying that way.
TEMP = DATA / "temp"
# Each part's `outputs` lands under data/. The trio goes to temp/metabolism/ under the
# names every reader expects rather than under a content-addressed product filename;
# intermediates that exist only because the graph was cut across hosts go to temp/_seams/,
# kept apart so that what is a RESULT and what is a SEAM stay distinguishable.

BAKE_SIF_DIR = REPO / "docker" / "ecspr_bake"
IMG = "docker://quay.io/hallamlab/ecspr_bake:{}"

# ---------------------------------------------------------------------------
# the three parts
# ---------------------------------------------------------------------------
#
# `lanes` is an EQUALITY assertion, not a subset one. A lane missing means the part was
# planned short; a lane EXTRA means an import did not satisfy its type and the planner
# resurrected another part's producer, which is the failure this whole design has to make
# loud. See check_plan.
#
# `outputs` and `imports` are the same namespace: every import below is some other part's
# output, at the identical path, and assert_seams_are_outputs checks that on startup so a
# renamed file is a refusal here rather than a resurrected branch three lines later.
BRANCHES = {
    "aam": dict(
        images=[IMG.format("aam")],
        lanes={
            "aam_worklist",
            "indigo", "rxnmapper", "localmapper",
            "aam_rescue",
            "indigo_rescue", "rxnmapper_rescue", "localmapper_rescue",
            "aam_ensemble",
        },
        targets=["ref::atom_pairs", "ref::metabolism_vocab"],
        inputs=[
            "fabfos_data::metanetx", "fabfos_data::chebi", "fabfos_data::modelseed",
            "lookup::reactions", "lookup::metabolites", "lookup::atom_ranks",
            "lookup::xrefs", "lookup::synonyms",
        ],
        imports={},
        evidence={
            "worklist":           "aam_worklist",
            "rxnmapper":          "rxnmapper",
            "localmapper":        "localmapper",
            "indigo":             "indigo",
            "rescue":             "aam_rescue",
            "rxnmapper_rescue":   "rxnmapper_rescue",
            "localmapper_rescue": "localmapper_rescue",
            "indigo_rescue":      "indigo_rescue",
            "metacyc":            "aam_ensemble",
            "ensemble":           "aam_ensemble",
        },
        outputs={
            "ref::metabolism_vocab": "temp/metabolism/vocab.parquet",
            "ref::atom_pairs":       "temp/metabolism/atom_pairs.parquet",
            # Not a seam and not part of the trio -- the UNCODED stack, which is what
            # build_references_tier4_agreement.py compares against the deployed table.
            # `ref::atom_pairs` is encoded against the vocabulary, so the gate cannot read
            # it; without this the one number the whole build is judged on would have to be
            # dug out of the raw run directory by hand.
            "interm::aam_pairs":     "temp/_seams/aam_pairs.parquet",
        },
        # A 32 vCPU / 120 GB Arbutus flavour, less a couple of cores for the agent and the
        # nextflow driver itself. The three concurrent pass-1 lanes declare 64 + 32 + 24 GB
        # between them, so one of them WILL queue behind the others -- that is the pool
        # doing its job, not a stall.
        # THE WHOLE BOX, because the pool admits by what a lane DECLARES and the mapper
        # lanes now declare close to what they use. Thirty left indigo's twenty-eight and
        # rxnmapper's four unable to coexist by two cores, which on a thirty-two-core
        # host is the difference between the two long lanes overlapping and queueing.
        executor=dict(cpus=32, memory="110 GB", queue=6),
    ),
    "members": dict(
        images=[IMG.format("direction"), IMG.format("dgbyg")],
        lanes={"equilibrator", "dgbyg"},
        # Targeted by their INTERMEDIATE types rather than by the trio, which is the whole
        # point of this part: neither member needs the vocabulary, so both can run while
        # the AAM part is still going.
        targets=["interm::direction_member_eq", "interm::direction_member_dgbyg"],
        inputs=["fabfos_data::metanetx", "fabfos_data::equilibrator"],
        imports={},
        evidence={"equilibrator": "equilibrator", "dgbyg": "dgbyg"},
        outputs={
            "interm::direction_member_eq":    "temp/_seams/direction_member_eq.parquet",
            "interm::direction_member_dgbyg": "temp/_seams/direction_member_dgbyg.parquet",
        },
        # chamois is 16c/176 GB and SHARED with the lab, so the pool is deliberately less
        # than the box. dgbyg asks for 8 and equilibrator for 4, which fits concurrently --
        # which is the whole reason dgbyg shards to eight rather than sixteen.
        executor=dict(cpus=14, memory="120 GB", queue=4),
    ),
    "direction": dict(
        images=[IMG.format("direction")],
        lanes={"direction_ensemble"},
        targets=["ref::direction_ratios"],
        inputs=["fabfos_data::metanetx"],
        imports={
            # From the AAM host. Not an input to the science -- an input to the ENCODING:
            # the ratios are coded against this vocabulary's reaction space and inherit its
            # bake-identity block.
            "ref::metabolism_vocab":          "temp/metabolism/vocab.parquet",
            # From the members run on this same host, hours earlier.
            "interm::direction_member_eq":    "temp/_seams/direction_member_eq.parquet",
            "interm::direction_member_dgbyg": "temp/_seams/direction_member_dgbyg.parquet",
        },
        evidence={
            "metacyc_direction":     "direction_ensemble",
            "direction_calibration": "direction_ensemble",
        },
        outputs={"ref::direction_ratios": "temp/metabolism/direction.parquet"},
        # Minutes of table arithmetic over tables that already exist.
        executor=dict(cpus=8, memory="64 GB", queue=2),
    ),
}


def assert_seams_are_outputs() -> None:
    """Every import must be some other part's output, at the same path.

    Cheap, and it closes the one gap the plan gate cannot see. The gate proves an import
    RESOLVED; it cannot prove the file it resolved against is the one the producing part
    actually writes. A path typo here would sail through planning -- the item is declared,
    the type is satisfied -- and surface as a direction table encoded against nothing.
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
    because the driver must PLACE the file before any engine code runs on the remote -- but
    it is a mirror, so if that method changes, this must too.
    """
    return image.replace("://", "..").replace(":", "..").replace("/", "_") + ".sif"


# ---------------------------------------------------------------------------
# the executor config
# ---------------------------------------------------------------------------

# The shipped `local` preset cannot be used as-is, and not because of taste:
#
#   * Its pool is EIGHT CPUs and EIGHT GIGABYTES, hardcoded inside a `params {}` block.
#     A config `params` block beats `-params-file`, so passing executor.memory as a
#     parameter is a silent no-op -- and every lane in this graph asks for more than 8 GB,
#     so they would simply never be scheduled.
#   * Its errorStrategy degrades to `ignore` the moment a task exhausts `params.process
#     .tries`, which defaults to 1 -- so the FIRST failure of any task is ignored, the
#     workflow reports `completed`, and the outputs are empty. That is how a missing
#     container once read as a 3.6-minute success. `finish` instead: stop scheduling new
#     work, let what is running finish, and report failure.
#
# `failOnIgnore` is set true as well. Nothing here sets `ignore` any more, but it is the
# cheap standing guard against something reintroducing it.
NXF_LOCAL = """\
// GENERATED by tests/build_references_bake_split.py for the {branch} part -- edits here
// are overwritten on the next run. See NXF_LOCAL in that file for why the shipped
// `local` preset is not used directly.

params {{
    executor {{
        cpus = {cpus}
        memory = '{memory}'
        queueSize = {queue}
    }}
}}

filePorter.maxThreads = 2
report.overwrite = true
timeline.overwrite = true

env {{
    NUMBA_CACHE_DIR = './temp/numba_cache'
    MPLCONFIGDIR = './temp/matplotlib'
    XDG_CACHE_HOME = './temp/xdg_home'
}}

executor {{
    cpus = params.executor.cpus
    memory = params.executor.memory
    queueSize = params.executor.queueSize
}}

workflow {{
    failOnIgnore = true
    output {{
        enabled = true
        ignoreErrors = false
        mode = 'rellink'
    }}
}}

process {{
    cache = 'lenient'
    executor = 'local'
    errorStrategy = 'finish'
    maxRetries = 0
    maxErrors = '-1'
}}
"""


def write_config(work: Path, branch: str) -> Path:
    ex = BRANCHES[branch]["executor"]
    p = work / f"nxf_local_{branch}.nf"
    p.write_text(NXF_LOCAL.format(branch=branch, cpus=ex["cpus"],
                                  memory=ex["memory"], queue=ex["queue"]))
    return p


# ---------------------------------------------------------------------------
# planning
# ---------------------------------------------------------------------------

def build_inputs(work: Path, branch: str, remote_root: str | None):
    """Stage the given, this part's sources, and its imports from the parts before it.

    RE-ROOTING IS NOT AN OPTIMISATION. metasmith binds an item's OWN path into the task
    container -- the same string on both sides -- so an input declared at a workstation
    path is bind-mounted at that path on the remote, where it does not exist, and apptainer
    refuses with "mount source does not exist" naming neither the item nor the path.

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
    # the producing part has run -- what it must never do is silently fall back to scheduling
    # the producer, and check_plan is what makes that impossible.
    for dtype, rel in sorted(spec["imports"].items()):
        if not (DATA / rel).exists():
            imported_missing.append(f"{dtype:32s} data/{rel}")
        inputs.AddItem(declared(rel), dtype)

    given = GIVEN_AT
    if not given.exists():
        # Planning is type-driven and never opens an input, so an empty directory resolves
        # the type exactly as the licensed distribution does. A RUN refuses.
        given = work / "metacyc_standin"
        given.mkdir(parents=True, exist_ok=True)
        print(f"NOTE: no MetaCyc drop-in at {GIVEN_AT}; standing in an empty directory "
              f"so the plan resolves. --run refuses without the real one.")
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
    planner simply finds the type unmet and schedules its PRODUCER. On the direction host
    that means quietly re-running the entire AAM branch: nine lanes and most of a day, on a
    machine that has neither the image nor the inputs for them, failing hours later in a
    way that names a container rather than a seam.
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
            f"LANES THIS HALF DOES NOT OWN ARE SCHEDULED: {sorted(extra)}.\n"
            f"    An unmet type is not an error -- the planner schedules its producer -- "
            f"so this is what a failed import looks like from the inside. Check that the "
            f"declared path for each of {sorted(spec['imports'])} exists on the remote at "
            f"the path the plan declares, and that it is the same TYPE (an item added "
            f"under the wrong dtype resolves nothing and reports nothing).\n"
            f"    If any of these came from acquire/, the assumption that broke is that "
            f"everything under data/originals/ is already on disk.")
    if pinned_local:
        problems.append(
            f"PINNED TO A LOGIN NODE: {sorted(pinned_local)}. Inert under the local "
            f"executor used here, but it means a transform in this graph carries a label "
            f"that belongs to acquire/.")
    return used, problems


# ---------------------------------------------------------------------------
# execution
# ---------------------------------------------------------------------------

def push_data(host: str, branch: str, remote_root: str) -> None:
    """rsync this part's inputs to its host, at the paths the declarations use.

    Converges: the sources are release-pinned directories and the lookups are rebuilt only
    when MetaNetX moves, so a second run transfers nothing.

    --size-only, and NEITHER the default (size+mtime) NOR --checksum. mtime is out because
    hardlink placement and DVC checkout give a re-staged file a fresh one with identical
    bytes. --checksum is out because it makes the REMOTE side read and digest every byte it
    already has. Size alone is the right test for these inputs rather than a concession:
    every source is a release-pinned upstream directory held immutable by DVC, and nothing
    here is edited in place, so a changed file that keeps its exact byte count is not a
    case that arises.

    NO --delete: the remote root also holds the previous run's work directory and the
    caches that make a lane resumable.
    """
    spec = BRANCHES[branch]
    rels = [ALL_INPUTS[t] for t in spec["inputs"]] + list(spec["imports"].values())
    # The drop-in, file by file rather than as a directory -- see METACYC_FILES.
    for release in sorted(p for p in (DATA / "originals/metacyc").glob("*") if p.is_dir()):
        for name in METACYC_FILES:
            for cand in (release / "data" / name, release / name):
                if cand.exists():
                    rels.append(str(cand.relative_to(DATA)))
                    break
    subprocess.run(["ssh", "-o", "BatchMode=yes", host, f"mkdir -p {remote_root}"],
                   check=True)
    for rel in sorted(set(rels)):
        src = DATA / rel
        if not src.exists():
            raise SystemExit(f"cannot push data/{rel} -- it is not here")
        print(f"  {rel}", flush=True)
        subprocess.run(["ssh", "-o", "BatchMode=yes", host,
                        f"mkdir -p {remote_root}/{Path(rel).parent}"], check=True)
        # A trailing slash on a directory source, none on a file. rsync treats the two
        # differently and getting it wrong nests the tree one level deeper every run.
        subprocess.run(["rsync", "-a", "--size-only", "--partial", "--info=stats1",
                        f"{src}/" if src.is_dir() else str(src),
                        f"{host}:{remote_root}/{rel}"], check=True)


def place_images(host: str, branch: str, cache_dir: str, container: str) -> None:
    """Put the agent image and this part's task images in the persistent store.

    ALL PUSHED, none pulled. `hallamlab/ecspr_bake` is private -- an anonymous-token
    manifest request returns 401 for all three tags -- so a pull is not a fallback for a
    missing local build, it is a different way to fail.

    The AGENT image goes first and is the one nothing else checks: its tag is derived from
    the engine source's build hash, so it changes every time the pinned submodule moves,
    whether or not anyone built a matching .sif. Without this the failure lands mid-Deploy
    as a registry error, then a FATAL about a missing .sif, then an assertion about a
    missing relay binary -- three errors, none of which says "nobody built this image".
    """
    subprocess.run(["ssh", "-o", "BatchMode=yes", host, f"mkdir -p {cache_dir}"],
                   check=True)

    wanted = [(container, _ENGINE.parent / "metasmith.sif")]
    wanted += [(i, BAKE_SIF_DIR / cached_image_name(i)) for i in BRANCHES[branch]["images"]]

    for image, local in wanted:
        dest = f"{cache_dir}/{cached_image_name(image)}"
        probe = subprocess.run(["ssh", "-o", "BatchMode=yes", host,
                                f"[ -s {dest} ] && echo CACHED"],
                               capture_output=True, text=True)
        if "CACHED" in probe.stdout:
            print(f"  cached   {image}", flush=True)
            continue
        if not local.exists():
            raise SystemExit(
                f"{image} is not in the store on {host} and there is no local .sif at\n"
                f"    {local}\n"
                f"The registry copy is NOT a fallback -- the repo is private. Build it:\n"
                f"    docker/ecspr_bake/dev.sh --build --sif --sync   (task images)\n"
                f"    ./dev/metasmith.sh --update_container           (agent image)\n"
                f"Do not point at an older agent image: the staging semantics come from "
                f"the agent, and one that disagrees with the engine on the PYTHONPATH is "
                f"exactly the mismatch assert_pinned_engine exists to prevent.")
        print(f"  upload   {image}  ({local.stat().st_size / 1e9:.1f} GB)", flush=True)
        subprocess.run(["rsync", "-a", "--partial", "--info=progress2",
                        str(local), f"{host}:{dest}"], check=True)


def assert_pinned_engine() -> str:
    import metasmith
    got = (Path(metasmith.__file__).parent / "version.txt").read_text().strip()
    want = (_ENGINE / "metasmith" / "version.txt").read_text().strip()
    if got != want:
        raise SystemExit(
            f"engine version [{got}] is not the pin [{want}].\n"
            f"  imported from: {Path(metasmith.__file__).parent}\n"
            f"Re-run with the pin first on the path:\n"
            f"  PYTHONPATH=src python tests/fabfos/{Path(__file__).name} ...")
    return got


def agent_container() -> str:
    """The published agent image for the engine we actually imported.

    `Agent.container` defaults to `metasmith:{CONTAINER_TAG}` where CONTAINER_TAG is
    `{VERSION}-{BUILD_HASH}` and BUILD_HASH is written AT BUILD TIME. A source checkout --
    which is what the submodule pin is -- has no build_hash.txt, so the tag silently
    degrades to the bare version, and bare versions were never pushed.
    """
    from metasmith._build_hash import compute_build_hash
    from metasmith.constants import VERSION
    return f"docker://quay.io/hallamlab/metasmith:{VERSION}-{compute_build_hash()}"


def retrieve(src_path: str, branch: str, host: str, staging: Path) -> int:
    """Bring this part home, per lane, into data/temp -- where the other parts also land.

    ROUTED BY THE `<tool>/` DIRECTORY INSIDE EACH ARTIFACT, not by filename and not by
    type. An output is named `{batch}-{i}-{branch}.{hash}-{type key}` and every evidence
    artifact now has the SAME type, so neither names the lane that wrote it. The tool
    directory is the only attribution left, which is why the lanes copy their evidence ROOT
    rather than the directory under it.

    The three parts write disjoint tool directories and disjoint output filenames, so
    running this three times composes into one complete data/temp rather than one
    overwriting the next -- and the outputs land exactly where the next part's imports
    declare them, which is what makes the seams work.

    Nothing is published here. data/reference/ is written deliberately, because it rewrites
    DVC directory hashes.
    """
    import shutil

    spec = BRANCHES[branch]
    staging.mkdir(parents=True, exist_ok=True)
    print(f"\n=== retrieving {branch} into {TEMP.relative_to(REPO)} ===", flush=True)
    # -L, AND IT IS THE WHOLE RETRIEVAL. Every entry under results/ is a symlink into
    # nxf_work -- the engine publishes by linking, not by copying -- so a plain `-a`
    # faithfully reproduces the links and lands a staging directory of dangling pointers.
    # It does not fail: rsync reports success, the manifest parses, and every product
    # reads as "MISSING on disk" while the run that produced it was perfect. Follow the
    # links and copy what they point at.
    #
    # --delete so staging MIRRORS this part's run rather than accumulating across runs.
    # Safe because each part has its OWN staging directory.
    subprocess.run(["rsync", "-aL", "--delete", "--partial", "--info=stats1",
                    f"{host}:{src_path}/", f"{staging}/"], check=True)

    lib = DataInstanceLibrary.Load(staging)
    found: dict[str, Path] = {}
    products: dict[str, Path] = {}
    for rel, dtype_name, _ in lib.Iterate():
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
                    help="which part of the graph this run owns. aam and members are\n"
                         "independent and go first, on separate hosts; direction waits "
                         "on both")
    ap.add_argument("--run", action="store_true",
                    help="execute on the host. Without it this plans, checks and renders "
                         "the DAG, and touches no remote machine")
    ap.add_argument("--host", default=None,
                    help="ssh alias or user@address. REQUIRED with --run")
    ap.add_argument("--user", default=None,
                    help="remote username, used only to build the default --root. Never "
                         "auto-resolved: shelling `ssh <host> echo $USER` cannot work "
                         "behind this workstation's ControlMaster guard, and a loop around "
                         "that call is a Duo push per iteration -- a prior run in this "
                         "project was halted by an account lockout caused exactly that way")
    ap.add_argument("--root", default=None,
                    help="absolute path on the host holding inputs, images and the agent. "
                         "Defaults to /home/<user>/fabfos_r6")
    ap.add_argument("--no-push", action="store_true",
                    help="trust --root is already populated and skip the rsync")
    ap.add_argument("--work", type=Path, default=None,
                    help="local scratch for this part's plan. Defaults to a per-branch "
                         "directory so the two halves cannot share an input library")
    ap.add_argument("--timeout-hours", type=float, default=36.0)
    ap.add_argument("--poll-s", type=float, default=120.0)
    ap.add_argument("--retrieve-only", metavar="RESULTS_PATH", default=None,
                    help="skip planning and execution; pull an ALREADY FINISHED run's\n"
                         "results directory from --host and route it. The path is the one\n"
                         "the run printed as `results: <host>:<path>`, or\n"
                         "<agent_home>/runs/<key>/results. Exists because retrieval is\n"
                         "separable from the run and a run is far too expensive to repeat\n"
                         "for a bug on this side of it")
    a = ap.parse_args()

    spec = BRANCHES[a.branch]
    work = a.work or (WORK_ROOT / a.branch)
    work.mkdir(parents=True, exist_ok=True)
    staging = TEMP / f"_run_{a.branch}"
    ts = int(time.time())

    if a.retrieve_only:
        if not a.host:
            raise SystemExit("--retrieve-only needs --host.")
        return retrieve(a.retrieve_only, a.branch, a.host, staging)

    if a.run and not (a.host and a.user):
        raise SystemExit("--run needs --host and --user; see --user's help for why the "
                         "second is never guessed.")
    if a.run and not GIVEN_AT.exists():
        raise SystemExit(
            f"the MetaCyc drop-in is not at {GIVEN_AT}.\n"
            f"  It is LICENSED and not redistributable, so nothing fetches it, and it is "
            f"the independent member of BOTH ensembles. Planning does not need it; drop "
            f"--run to see the plan.")

    remote_root = None
    if a.run:
        remote_root = a.root or f"/home/{a.user}/fabfos_r6"

    # ---- the agent -------------------------------------------------------------
    if not a.run:
        # A local agent for planning only. The runtime still has to be APPTAINER: it is
        # what decides whether an env declaration resolves its `container:` or its `conda:`
        # key, and the plan is only a claim about the real run if it resolved the same side
        # of that fork. It is also why BOTH halves must use the same runtime -- two halves
        # that resolved different sides did not run the same method.
        agent = Agent(home=Source.FromLocal(work / "agent_home"),
                      runtime=Runtime.APPTAINER)
    else:
        print(f"=== engine pin: {assert_pinned_engine()} ===", flush=True)
        cache_dir = f"{remote_root}/container_images"
        agent_path = f"{remote_root}/agent_{a.branch}_{ts}"
        container = agent_container()
        print(f"=== {a.branch} on {a.host}:{remote_root} ===", flush=True)
        print(f"=== agent image: {container} ===", flush=True)

        if not a.no_push:
            print(f"=== pushing inputs -> {a.host}:{remote_root} ===", flush=True)
            push_data(a.host, a.branch, remote_root)
        print("=== placing images ===", flush=True)
        place_images(a.host, a.branch, cache_dir, container)

        agent = Agent(home=SshSource(host=a.host, path=agent_path).AsSource(),
                      runtime=Runtime.APPTAINER,
                      container=container,
                      # EMPTY, and that is the point of these hosts: neither has a module
                      # system, so there is nothing to load and nothing whose load order
                      # can go wrong. APPTAINER_CACHEDIR still has to be exported on both
                      # sides -- one side missing it resolves a different directory, finds
                      # nothing, and attempts a pull against a private registry.
                      setup_commands=[f"export APPTAINER_CACHEDIR={cache_dir}"])

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
    task.plan.RenderDAG(svg, blacklist_namespaces={"lib", "env", "buildlib"})
    print(f"\nDAG -> {svg}")

    for p in problems:
        print(f"\nFAIL: {p}")
    if problems:
        return 1
    print(f"\nexactly the {a.branch} part's lanes are in the plan, and nothing else")

    if not a.run:
        print(f"\n(plan only -- pass --run --host <h> --user <u> to execute)")
        return 0
    if seam_missing:
        raise SystemExit(
            f"\nthis part imports {sorted(spec['imports'])} and at least one local copy is "
            f"not here, so there is nothing to push and the remote path would be empty. "
            f"Run the producing part first and retrieve it.")

    # ---- execute ---------------------------------------------------------------
    print("=== Deploy() ===", flush=True)
    try:
        agent.Deploy()
    except subprocess.CalledProcessError as e:
        # ONE session, one failure, one message. Never a retry loop: on a Duo-guarded host
        # each attempt is a push, and a prior run in this project was halted by an account
        # lockout caused exactly that way.
        print(f"\ndeploy failed ({e}). Open ONE session by hand (`ssh {a.host}`), leave "
              f"it open, and re-run. Do NOT retry in a loop.", file=sys.stderr)
        return 4

    print(f"=== task key: {task.GetKey()} ===", flush=True)
    # on_exist="clear" is safe HERE and only here: agent_path carries a timestamp, so it is
    # a fresh directory every run and there is no prior intermediate to destroy. Never
    # carry this flag onto a resubmission.
    agent.StageWorkflow(task, on_exist="clear")

    cfg = write_config(work, a.branch)
    ex = spec["executor"]
    print(f"=== executor: local, pool {ex['cpus']} cpu / {ex['memory']}, "
          f"queue {ex['queue']} ===", flush=True)
    agent.RunWorkflow(task, config_file=cfg)

    print(f"=== waiting (timeout {a.timeout_hours:.0f}h, poll {a.poll_s:.0f}s) ===",
          flush=True)
    result = agent.WaitForWorkflow(task, timeout_s=int(a.timeout_hours * 3600),
                                   poll_s=a.poll_s)
    print(f"=== status: {result['status']} after {result['elapsed_s'] / 3600:.1f}h ===",
          flush=True)
    for line in result["tail"]:
        print(f"    {line}")
    if result["status"] != "completed":
        return 2

    # "COMPLETED" IS NOT "SUCCEEDED", even with errorStrategy='finish' and failOnIgnore
    # set: the check that matters is on the PRODUCTS, which is what retrieve() does. This
    # scan stays because it names the failing step, and retrieve() can only name the
    # missing output.
    swallowed = [ln for ln in result["tail"]
                 if "Error is ignored" in ln or "terminated with an error" in ln]
    if swallowed:
        print("\nA STEP FAILED -- this is not a result:", file=sys.stderr)
        for ln in swallowed:
            print(f"    {ln}", file=sys.stderr)
        print("\nThe real error is in that step's .command.err under "
              "<run>/nxf_work/<hash>/. Do not bake or publish this output.",
              file=sys.stderr)
        return 3

    src = agent.GetResultSource(task)
    print(f"\nresults: {a.host}:{src.GetPath()}")
    return retrieve(src.GetPath(), a.branch, a.host, staging)


if __name__ == "__main__":
    sys.exit(main())

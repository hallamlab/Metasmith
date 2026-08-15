#!/usr/bin/env python3
"""R6, the metabolism bake: planned here, executed on Sockeye.

    PYTHONPATH=src python tests/fabfos/build_references_bake_on_hpc.py
    ... tests/fabfos/build_references_bake_on_hpc.py --run --user txyliu

WHAT THIS REPLACES, AND WHY THAT MATTERS MORE THAN WHAT IT ADDS. R6 used to be run by a
pair of shell drivers whose stages called the same `buildlib::` modules the transforms
call, with the same arguments. They were honest about being scaffold:
"the thing that gets the LAYERS run and measured while the contract is still being
shaped". The cost of scaffold is that the METHOD lives in a bash file. A stage list is
not a graph: nothing checks that a member ran, nothing records what a step consumed, a
backgrounded shard that dies is collected by a bare `wait` that returns zero, and the
provenance of the trio is whatever the shell happened to do that night. On 2026-07-26 an
Indigo shard was OOM-killed and the stage carried on to merge a short member.

So the lanes are transforms now (build_references/transforms/bake/), one per TOOL and,
on the AAM side, one per PASS:

    aam_worklist         -> aam_worklist            the adjudicated universe
    rxnmapper            -> aam_member_rxnmapper           pass 1
    indigo               -> aam_member_indigo              pass 1
    localmapper          -> aam_member_localmapper         pass 1, over their gap
    aam_rescue           -> aam_rescue               structures for the blocked
    rxnmapper_rescue     -> aam_member_rxnmapper_rescue    pass 2
    indigo_rescue        -> aam_member_indigo_rescue       pass 2
    localmapper_rescue   -> aam_member_localmapper_rescue  pass 2, over their gap
    equilibrator         -> direction_member_eq
    dgbyg                -> direction_member_dgbyg

ADJUDICATE, MAP TWICE. The worklist classifies all 83,796 reactions before anything runs,
so a reaction that produced nothing and a reaction nobody attempted are different rows.
The rescue then supplies structures for the blocked ones and completes their reactions,
and pass 2 shows those to all three mappers -- which is what the deployed chain could not
do, having built its crosswalk after its mappers had already run.

The arithmetic over them is two assemblies (`aam_ensemble`, `direction_ensemble`).
Each assembly reads the licensed MetaCyc drop-in directly for its own curated member --
the AAM one takes atom-mappings-smiles.dat, the direction one takes reactions.dat -- so
the drop-in feeds the graph at two leaves rather than through a step of its own. A member
that did not run is now a missing NODE the planner refuses to schedule around, not a
column that quietly came out empty.

THE TRIO IS BUILT 2 + 1, by the two assemblies, with no trailing encode step. The AAM
assembly mints the vocabulary and encodes the pairs; the direction assembly requires that
vocabulary, inherits its bake-identity block verbatim and encodes the ratios against it.
That edge is why the direction side -- minutes of table arithmetic -- now waits on
LocalMapper. It is not new wall clock, since the old bake waited on both, but it does
mean a direction-only re-run needs the AAM product present.

WHAT IS DELIBERATELY NOT RUN, AND HOW YOU KNOW. Everything already on disk is STAGED as
an input, and the driver then asserts that its producer is absent from the plan. That is
the same backwards-looking check stage 2 uses against acquire/: hiding a transform makes
an unmet input an unresolvable plan whose error names a TYPE, while loading it and
asserting it did not run names the exact step that was skipped. Two groups are skipped
this way here:

  * THE ACQUISITIONS. `data/originals/` holds MetaNetX 4.5, ChEBI, ModelSEED and the
    eQuilibrator cache; nothing in this run touches the network.
  * THE LOOKUPS. `data/processed/lookups/` holds the five tables `mnx_lookups` builds.
    They are minutes of work and they are already correct, and re-deriving the atom node
    identity `(mnxm, canonical rank)` in the same run that consumes it buys nothing.

WHY THIS RUNS ON SOCKEYE AND NOT HERE. Not preference: this workstation exhausted its
memory on the ensemble, which is what moved the build to the cluster in the first place.
The planner does not care where it runs, so the plan is built and checked locally -- that
part needs none of the 3 GB -- and only execution is remote.

THREE IMAGES, ONE OF THEM NEW. `:aam` (numpy<2, torch 2.2.1), `:direction` (numpy>=2,
python 3.11) and `:dgbyg` (python 3.12, torch 2.8). The third one is what makes dGbyG a
member rather than a deferral; see build_references/transforms/bake/dgbyg.py for why the
long-standing "it needs a third image" was right about the conclusion and wrong about the
cause. `docker/ecspr_bake/dev.sh --build --sif --sync` builds and places all three.
"""
from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]

# The PINNED engine, ahead of whatever is installed. The `msm` env resolves metasmith
# through a global symlink into a different worktree; the agent container tag is derived
# from the engine's own version, so importing the wrong one asks quay for a manifest that
# does not exist and the run dies ~40s in with "manifest unknown".
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
SCRATCH = DATA / "scratch"
WORK = SCRATCH / "r6_bake"

TYPE_LIBRARIES = (
    [MLIB / "data_types" / f for f in
     ("ncbi.yml", "sequences.yml", "annotation.yml", "ref.yml", "lib.yml")]
    + [BREF / "data_types" / f for f in
       ("fabfos_data.yml", "raw.yml", "interm.yml", "bench.yml", "buildlib.yml",
        "lookup.yml", "evidence.yml")]
)

# THE ONE GIVEN. Licensed, not redistributable, and the independent member of BOTH
# ensembles -- the gate over this graph asserts the count rather than the name, because
# everything else in it is fetchable.
GIVEN_TYPE = "fabfos_data::metacyc"
GIVEN_AT = DATA / "originals" / "metacyc"

# Acquired sources, staged so no acquisition is scheduled. The value is the path under
# data/ -- ONE map, so the re-rooting onto the cluster and the local existence check
# cannot drift apart.
SOURCES = {
    "fabfos_data::metanetx":     "originals/metanetx",
    "fabfos_data::chebi":        "originals/chebi",
    "fabfos_data::modelseed":    "originals/modelseed",
    "fabfos_data::equilibrator": "originals/equilibrator",
}

# The five lookups, staged so `mnx_lookups` is not scheduled. All five or none: the
# planner reasons about TYPES, so staging four of them satisfies four types and leaves
# `mnx_lookups` in the plan to produce the fifth -- which would re-derive all five and
# hand the run two generations of the same table.
LOOKUPS = {
    "lookup::reactions":   "processed/lookups/reactions.parquet",
    "lookup::metabolites": "processed/lookups/metabolites.parquet",
    "lookup::atom_ranks":  "processed/lookups/atom_ranks.parquet",
    "lookup::xrefs":       "processed/lookups/xrefs.parquet",
    "lookup::synonyms":    "processed/lookups/synonyms.parquet",
}

# The MetaCyc files any transform in this graph actually opens. THE WHOLE DISTRIBUTION IS
# NOT PUSHED, and that is a licence decision rather than a transfer-size one: the drop-in
# is 1.6 GB of which 60 MB is read, it is not redistributable, and a shared cluster
# filesystem is not a place to put more of it than the method needs. The layout is
# preserved (`<release>/data/`) because that is one of the two shapes the transforms
# accept.
METACYC_FILES = ("atom-mappings-smiles.dat", "reactions.dat", "compounds.dat")

TARGETS = ["ref::atom_pairs", "ref::metabolism_vocab", "ref::direction_ratios"]

# Every lane and assembly that must appear. A plan that quietly drops one renders a
# smaller graph and produces a trio that looks complete.
EXPECTED = {
    "aam_worklist",
    "rxnmapper", "localmapper", "indigo",
    "aam_rescue",
    "rxnmapper_rescue", "localmapper_rescue", "indigo_rescue",
    # Pass 3, the partial lane: element-reduced submissions for the reactions no full
    # map reached, and the same three mappers over them. It sits AFTER both mapper
    # passes because its target set is "what ended with nothing", which is a fact about
    # a run rather than about a reaction.
    "aam_partial",
    "rxnmapper_partial", "localmapper_partial", "indigo_partial",
    "aam_ensemble",
    "equilibrator", "dgbyg", "direction_ensemble",
}

# Producers of things that are already on disk. Any of these in the plan means a staging
# assumption did not hold -- named individually, because "which one" is the whole
# diagnostic value of the check.
#
# The eQuilibrator ACQUISITION is absent from this set and not by oversight:
# `acquire/equilibrator.py` and `bake/equilibrator.py` share a stem, so a name-keyed
# check cannot tell the fetch from the member. Acquisitions are detected by their
# LIBRARY instead, which is the property that actually matters.
SKIPPED = {"metanetx", "chebi", "modelseed", "mnx_lookups"}
ACQUIRE_LIB = BREF / "transforms" / "acquire"

# Where a finished run lands. `data/.gitignore` is `/*/*` with a `.dvc` negation, so this
# is already ignored and must NOT get a .gitignore of its own -- that file is the only one
# under data/ and the tier rule depends on it staying that way.
TEMP = DATA / "temp"
# The raw run, exactly as the cluster wrote it, kept beside the routed copy. Routing reads
# it rather than consuming it: it also holds the nextflow logs and the step manifests,
# which are the only record of what a step was handed.
STAGING = TEMP / "_run"

# Every `<tool>/` directory a complete run must produce, and which step writes it. This is
# the check that survives collapsing the nine evidence types into one: with a single type
# the artifact filenames and the run manifest no longer name a lane, so attribution lives
# in these directory names and nowhere else. A lane whose raw output never arrived has to
# be a loud absence, not six entries where seven were expected.
EVIDENCE_TOOLS = {
    "worklist":              "aam_worklist",
    "rxnmapper":             "rxnmapper",
    "localmapper":           "localmapper",
    "indigo":                "indigo",
    "rescue":                "aam_rescue",
    "rxnmapper_rescue":      "rxnmapper_rescue",
    "localmapper_rescue":    "localmapper_rescue",
    "indigo_rescue":         "indigo_rescue",
    "equilibrator":          "equilibrator",
    "dgbyg":                 "dgbyg",
    "metacyc":               "aam_ensemble",
    "ensemble":              "aam_ensemble",
    "metacyc_direction":     "direction_ensemble",
    "direction_calibration": "direction_ensemble",
}

# The trio, under the names every reader expects rather than under a content-addressed
# product filename.
TRIO = {
    "ref::metabolism_vocab": "vocab.parquet",
    "ref::atom_pairs":       "atom_pairs.parquet",
    "ref::direction_ratios": "direction.parquet",
}

# Sockeye's Lmod hides apptainer behind a gcc dependency, and the two loads must be
# SEPARATE commands: `module load gcc/9.4.0 apptainer/1.3.1` in one call resolves the
# second name against the module tree as it stood BEFORE gcc loaded, silently finds
# nothing, and the shell later reports `apptainer: command not found` with no hint that a
# module was skipped.
SETUP_COMMANDS = ["module load gcc/9.4.0", "module load apptainer/1.3.1"]

# The task images, and where a locally-built .sif for each one lives. All three are
# SYNCED rather than pulled: quay defaults new repos to private, the compute nodes have no
# outbound route anyway, and `Environment._cached_name()` derives the store filename from
# the URI deterministically -- so a file placed under that name IS the image as far as
# metasmith is concerned.
#
# THREE, not four. `python_for_data_science` was here for the trailing bake step, which no
# longer exists: the two assemblies encode their own output in the images they already
# run. Nothing in this graph is pulled from the network now.
BAKE_SIF_DIR = REPO / "docker" / "ecspr_bake"
REQUIRED_IMAGES = {
    "docker://quay.io/hallamlab/ecspr_bake:aam":       BAKE_SIF_DIR,
    "docker://quay.io/hallamlab/ecspr_bake:direction": BAKE_SIF_DIR,
    "docker://quay.io/hallamlab/ecspr_bake:dgbyg":     BAKE_SIF_DIR,
}


def cached_image_name(image: str) -> str:
    """The filename metasmith looks for in the image store.

    Mirrors `Environment._cached_name` in the pinned engine. Kept as a copy rather than
    an import because the driver must be able to PLACE the file before any engine code
    runs on the remote -- but it is a mirror, so if that method changes, this must too.
    """
    return image.replace("://", "..").replace(":", "..").replace("/", "_") + ".sif"


# ---------------------------------------------------------------------------
# planning
# ---------------------------------------------------------------------------

def build_inputs(work: Path, remote_root: str | None):
    """Stage the given, the sources and the lookups; re-root them if this is remote.

    RE-ROOTING IS NOT AN OPTIMISATION. metasmith binds an item's OWN path into the task
    container -- the same string on both sides -- so an input declared at a workstation
    path is bind-mounted at that path on the cluster node, where it does not exist.
    Apptainer then refuses with "mount source does not exist" and the run dies as a
    missing launcher, naming neither the item nor the path.

    Existence is always checked against the LOCAL copy, even when the declared path is
    remote: the planner resolves on types and lineage and never on existence, so a
    missing tree plans perfectly and fails hours later inside a container. The local copy
    is what rsync just pushed, so checking it is checking the far side.
    """
    inputs = DataInstanceLibrary(work / "inputs.xgdb")
    for tl in TYPE_LIBRARIES:
        inputs.AddTypeLibrary(tl)

    wanted = dict(SOURCES)
    wanted.update(LOOKUPS)

    def declared(rel: str) -> Path:
        p = DATA / rel
        if remote_root is None:
            return p
        return Path(remote_root) / rel

    missing = []
    for dtype, rel in sorted(wanted.items()):
        if not (DATA / rel).exists():
            missing.append(f"{dtype:32s} data/{rel}")
            continue
        inputs.AddItem(declared(rel), dtype)

    given = GIVEN_AT
    if not given.exists():
        # Planning is type-driven and never opens an input, so an empty directory
        # resolves the type exactly as the licensed distribution does. That is what lets
        # the plan be checked on a machine that has not licensed MetaCyc. A RUN refuses.
        given = work / "metacyc_standin"
        given.mkdir(parents=True, exist_ok=True)
        print(f"NOTE: no MetaCyc drop-in at {GIVEN_AT}; standing in an empty directory "
              f"so the plan resolves. --run refuses without the real one.")
    inputs.AddItem(declared("originals/metacyc") if remote_root else given, GIVEN_TYPE)

    inputs.Save()
    return inputs, missing


def plan(work: Path, remote_root: str | None, agent):
    inputs, missing = build_inputs(work, remote_root)
    if missing:
        print("\nNOT ON DISK, so its producer will be scheduled rather than skipped:")
        for m in missing:
            print(f"    {m}")
        print()

    resources = [
        DataInstanceLibrary.Load(MLIB / "resources" / "env"),
        DataInstanceLibrary.Load(MLIB / "resources" / "lib"),
        DataInstanceLibrary.Load(BREF / "resources" / "buildlib"),
        inputs,
    ]
    # acquire/ and compile/ are LOADED, not hidden. Loading them is what turns "the
    # lookups were staged" into a checkable assertion instead of an assumption -- see
    # the module docstring.
    transforms = [
        TransformInstanceLibrary.Load(ACQUIRE_LIB),
        TransformInstanceLibrary.Load(BREF / "transforms" / "compile"),
        TransformInstanceLibrary.Load(BREF / "transforms" / "bake"),
    ]

    targets = TargetBuilder()
    for dtype in TARGETS:
        targets.Add(dtype)

    task = agent.GenerateWorkflow(
        samples=list(inputs.AsSamples(GIVEN_TYPE)),
        resources=resources,
        transforms=transforms,
        targets=targets,
    )
    return inputs, task


def check_plan(task) -> tuple[set[str], list[str]]:
    used, from_acquire, pinned_local = set(), set(), set()
    for step in task.plan.steps:
        p = Path(step.transform._path)
        used.add(p.stem)
        if ACQUIRE_LIB.name in p.parts:
            from_acquire.add(p.stem)
        if "local" in (getattr(step.transform, "labels", None) or []):
            pinned_local.add(p.stem)

    print(f"\nPlan OK -- {len(task.plan.steps)} steps, {len(used)} distinct transforms\n")
    for step in sorted(task.plan.steps, key=lambda s: s.order):
        prods = [i.dtype_name for g in step.produces for i in g]
        print(f"  {step.order:>3}  {Path(step.transform._path).stem:<20} -> {prods}")

    problems = []
    absent = EXPECTED - used
    if absent:
        problems.append(
            f"MISSING expected lanes: {sorted(absent)}. Every one of those is a member "
            f"or an assembly; a plan without it produces a trio that looks complete.")
    if from_acquire:
        problems.append(
            f"AN ACQUISITION IS SCHEDULED: {sorted(from_acquire)}. Everything this run "
            f"reads is supposed to be on disk under data/originals/ already, and a "
            f"fetch inside the compute half is the thing the tier split exists to "
            f"prevent.")
    if "mnx_lookups" in used:
        problems.append(
            "mnx_lookups IS SCHEDULED, so the five staged lookups did not satisfy it. "
            "Staging is all-or-nothing here: check data/processed/lookups/ has all "
            "five parquet files, because four of five leaves the producer in the plan "
            "and the run then carries two generations of the same table.")
    if pinned_local:
        problems.append(
            f"PINNED TO THE LOGIN NODE: {sorted(pinned_local)}. A `local` label renders "
            f"as `label 'xlocalx'`, which slurm.nf maps to executor='local' -- so the "
            f"step never reaches a compute node, and the four-CPU minimum any lane here "
            f"asks for is measured against the one CPU the agent container is given. "
            f"That is correct for acquire/, whose steps need the outbound route only the "
            f"login node has, and wrong for every step in this graph: they are the "
            f"compute. Under the local runtime the label is inert, which is why it "
            f"survives a workstation run and only ever fails here.")
    return used, problems


# ---------------------------------------------------------------------------
# execution
# ---------------------------------------------------------------------------

def push_data(host: str, remote_root: str) -> None:
    """rsync the inputs to the execution host, at the paths the declarations use.

    Converges: the sources are release-pinned directories and the lookups are rebuilt
    only when MetaNetX moves, so a second run transfers nothing. ~3.4 GB the first time,
    of which the eQuilibrator compound cache is 1.3 GB.

    --size-only, and NEITHER the default (size+mtime) NOR --checksum. mtime is out
    because hardlink placement and DVC checkout give a re-staged file a fresh one with
    identical bytes, and re-sending an 810 MB TSV for that is waste. --checksum is out
    because of where the reading happens: it makes the REMOTE side read and digest every
    byte it already has, and the remote side here is a LOGIN NODE. Three consecutive runs
    died the same way -- `connection unexpectedly closed (0 bytes received so far)`, always
    on eQuilibrator or MetaNetX, never on the small directories -- which is the scheduler
    killing a process that spent minutes at full CPU on a shared login host. It reads as a
    flaky link and is not one; retrying cannot fix it.

    Size alone is the right test for these inputs rather than a concession. Every source
    is a release-pinned upstream directory held immutable by DVC, and the lookups are
    regenerated wholesale when MetaNetX moves. Nothing here is edited in place, so a
    changed file that keeps its exact byte count is not a case that arises.

    NO --delete. The remote root also holds a previous generation's work directory and
    the caches that make a lane resumable; deleting to match a local tree that never had
    them would throw away exactly what a resume needs.
    """
    rels = list(SOURCES.values()) + list(LOOKUPS.values())
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
        dest = f"{host}:{remote_root}/{rel}"
        print(f"  {rel}", flush=True)
        subprocess.run(["ssh", "-o", "BatchMode=yes", host,
                        f"mkdir -p {remote_root}/{Path(rel).parent}"], check=True)
        # A trailing slash on a directory source, none on a file. rsync treats the two
        # differently and getting it wrong nests the tree one level deeper every run.
        subprocess.run(["rsync", "-a", "--size-only", "--partial", "--info=stats1",
                        f"{src}/" if src.is_dir() else str(src), dest], check=True)


def assert_agent_image(host: str, cache_dir: str, container: str) -> None:
    """The AGENT image must be in the store before anything is deployed.

    A fourth image, and the one nothing else checks. `REQUIRED_IMAGES` covers the images
    the TASKS run in; this is the one metasmith itself runs in, and its tag is derived
    from the engine source's build hash -- so it changes every time the pinned submodule
    moves, whether or not anyone built and synced a matching .sif.

    Without this the failure lands mid-`Deploy()`: apptainer reports `manifest unknown`
    against quay (new repos default to private, and the compute nodes have no route out
    regardless), then a second FATAL about the missing .sif, and finally an assertion
    about a missing relay binary. Three errors, none of which says "nobody built this
    image". `assert_pinned_engine` does not catch it either -- it checks that the engine
    on the path IS the pin, not that the pin was ever released.
    """
    dest = f"{cache_dir}/{cached_image_name(container)}"
    probe = subprocess.run(["ssh", "-o", "BatchMode=yes", host,
                            f"[ -s {dest} ] && echo CACHED"],
                           capture_output=True, text=True)
    if "CACHED" in probe.stdout:
        print(f"  cached   {container}  (agent)", flush=True)
        return
    local = _ENGINE.parent / f"{Path(container).name.split(':')[0]}.sif"
    raise SystemExit(
        f"the AGENT image is not in the store:\n"
        f"    {container}\n"
        f"    expected at {host}:{dest}\n"
        f"\n"
        f"Its tag is {{version}}-{{build hash of the engine source}}, so moving the "
        f"src/metasmith pin invalidates it. Build and sync a matching one:\n"
        f"    ./dev/metasmith.sh --update_container\n"
        f"    rsync -a {local} {host}:{dest}\n"
        f"\n"
        f"Do NOT work around this by pointing at an older agent image: the staging "
        f"semantics come from the agent, and one that disagrees with the engine on the "
        f"PYTHONPATH is exactly the mismatch `assert_pinned_engine` exists to prevent.")


def place_images(host: str, cache_dir: str) -> None:
    """Put every task image in the persistent store, on the LOGIN node.

    The compute nodes have no outbound route, so an image pulled as part of a task dies
    with "no route to host" -- and slurm.nf sets errorStrategy='ignore' once a process
    exhausts its retries, which turns that into a SILENT green run with empty outputs. A
    green run with an empty result is worse than a red one, which is why this happens up
    front and why the caller checks the products rather than the status.

    APPTAINER_CACHEDIR governs the .sif destination only. APPTAINER_TMPDIR and the blob
    cache default to $HOME/.apptainer and silently accreted ~23 GB on a quota'd home
    during a run whose .sif was correctly written to scratch -- so both are pinned here.
    """
    subprocess.run(["ssh", "-o", "BatchMode=yes", host,
                    f"mkdir -p {cache_dir} {cache_dir}/tmp"], check=True)
    setup = "; ".join(SETUP_COMMANDS + [
        f"export APPTAINER_CACHEDIR={cache_dir}",
        f"export APPTAINER_TMPDIR={cache_dir}/tmp",
    ])
    for image, sif_dir in REQUIRED_IMAGES.items():
        dest = f"{cache_dir}/{cached_image_name(image)}"
        probe = subprocess.run(["ssh", "-o", "BatchMode=yes", host,
                                f"[ -s {dest} ] && echo CACHED"],
                               capture_output=True, text=True)
        if "CACHED" in probe.stdout:
            print(f"  cached   {image}", flush=True)
            continue
        if sif_dir is not None:
            local = sif_dir / cached_image_name(image)
            if not local.exists():
                raise SystemExit(
                    f"{image} is not in the store and there is no local .sif at "
                    f"{local}.\n"
                    f"  Build and place all three with:\n"
                    f"      docker/ecspr_bake/dev.sh --build --sif --sync\n"
                    f"  (or TAGS='{image.rsplit(':', 1)[1]}' for just this one). The "
                    f"registry copy is not a fallback: quay defaults new repos to "
                    f"private and the compute nodes have no route out regardless.")
            print(f"  upload   {image}  ({local.stat().st_size / 1e9:.1f} GB)",
                  flush=True)
            subprocess.run(["rsync", "-a", "--partial", "--info=progress2",
                            str(local), f"{host}:{dest}"], check=True)
            continue
        print(f"  pull     {image}", flush=True)
        r = subprocess.run(["ssh", "-o", "BatchMode=yes", host,
                            f"{setup}; apptainer pull {dest} {image}"],
                           capture_output=True, text=True)
        if r.returncode != 0:
            raise SystemExit(f"pre-pull failed for {image}:\n{r.stderr.strip()[-2000:]}")


def assert_pinned_engine() -> str:
    """Refuse an engine version whose agent container was never published.

    `Deploy()` derives the agent image tag from the engine's OWN version, so an
    unreleased checkout asks quay for a manifest that does not exist. The failure
    surfaces as a registry error followed by an assertion about a missing relay binary --
    neither of which names the cause. Checked on the version actually imported, before a
    remote directory is created.
    """
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
    `{VERSION}-{BUILD_HASH}`, and BUILD_HASH is written AT BUILD TIME. A source checkout
    -- which is what the submodule pin is -- has no build_hash.txt, so the tag silently
    degrades to the bare version, and bare versions were never pushed. Computing the hash
    here is the difference between "this tag happens to work today" and "this image is
    provably built from the source on our PYTHONPATH".
    """
    from metasmith._build_hash import compute_build_hash
    from metasmith.constants import VERSION
    return f"docker://quay.io/hallamlab/metasmith:{VERSION}-{compute_build_hash()}"


def retrieve(agent, task, host: str) -> int:
    """Bring a finished run home, per lane, into data/temp.

    ROUTED BY THE `<tool>/` DIRECTORY INSIDE EACH ARTIFACT, not by filename and not by
    type. An output is named `{batch}-{i}-{branch}.{hash}-{type key}` and the run manifest
    records only the type -- and every evidence artifact now has the SAME type -- so
    neither names the lane that wrote it. The tool directory is the only attribution left,
    which is why the lanes copy their evidence ROOT rather than the directory under it.

    Nothing is published here. `data/reference/` is written deliberately, because it
    rewrites DVC directory hashes.
    """
    import shutil

    src = agent.GetResultSource(task)
    STAGING.mkdir(parents=True, exist_ok=True)
    print(f"\n=== retrieving into {TEMP.relative_to(REPO)} ===", flush=True)
    # --delete, so staging MIRRORS the run rather than accumulating across runs. Without
    # it a lane that produced evidence once and failed the next time still has a directory
    # here, and the completeness check below passes on last week's output.
    subprocess.run(["rsync", "-a", "--delete", "--partial", "--info=stats1",
                    f"{host}:{src.GetPath()}/", f"{STAGING}/"], check=True)

    lib = DataInstanceLibrary.Load(STAGING)
    found: dict[str, Path] = {}
    trio: dict[str, Path] = {}
    for rel, dtype_name, _ in lib.Iterate():
        path = STAGING / rel
        if not path.exists():
            print(f"  MISSING on disk: {rel} ({dtype_name})", file=sys.stderr)
            continue
        if dtype_name == "evidence::tool_output":
            # One artifact, one or more `<tool>/` directories inside it.
            for tool_dir in sorted(p for p in path.iterdir() if p.is_dir()):
                found[tool_dir.name] = tool_dir
        elif dtype_name in TRIO:
            trio[dtype_name] = path

    for tool, tool_dir in sorted(found.items()):
        dest = TEMP / tool
        if dest.exists():
            shutil.rmtree(dest)
        shutil.copytree(tool_dir, dest)
        n = sum(1 for _ in dest.rglob("*") if _.is_file())
        print(f"  {tool:<24} -> {dest.relative_to(REPO)}  ({n} files)")

    metabolism = TEMP / "metabolism"
    metabolism.mkdir(parents=True, exist_ok=True)
    for dtype_name, name in TRIO.items():
        p = trio.get(dtype_name)
        if p is None:
            continue
        shutil.copy2(p, metabolism / name)
        print(f"  {dtype_name:<24} -> "
              f"{(metabolism / name).relative_to(REPO)}  "
              f"({(metabolism / name).stat().st_size / 1e6:.2f} MB)")

    # A workflow can report `completed` with nothing in it, because slurm.nf sets
    # errorStrategy='ignore' for a task that exhausted its retries. An empty retrieval is
    # therefore a real outcome to refuse, not an impossible one.
    rc = 0
    missing_tools = sorted(set(EVIDENCE_TOOLS) - set(found))
    if missing_tools:
        rc = 4
        print("\nNO RAW OUTPUT RETRIEVED for:", file=sys.stderr)
        for t in missing_tools:
            print(f"    {t:<24} (written by {EVIDENCE_TOOLS[t]})", file=sys.stderr)
        print("Every tool's direct output is part of the result. A run missing one is "
              "not a run to publish.", file=sys.stderr)
    missing_trio = sorted(set(TRIO) - set(trio))
    if missing_trio:
        rc = 4
        print(f"\nTRIO INCOMPLETE -- missing {missing_trio}", file=sys.stderr)

    extra = sorted(set(found) - set(EVIDENCE_TOOLS))
    if extra:
        print(f"\nnote -- evidence for tools not in EVIDENCE_TOOLS: {extra}")

    print(f"\nraw run kept at {STAGING.relative_to(REPO)} (logs and step manifests too)")
    if rc == 0:
        print("Publish to data/reference/ deliberately -- it rewrites DVC directory "
              "hashes.")
    return rc


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--run", action="store_true",
                    help="execute on the host. Without it this plans, checks and renders "
                         "the DAG, and touches no remote machine")
    ap.add_argument("--host", default="sockeye")
    ap.add_argument("--user", default=None,
                    help="remote username, REQUIRED with --run and never auto-resolved. "
                         "Shelling `ssh <host> echo $USER` to find it cannot work behind "
                         "this workstation's ControlMaster guard, and a loop around that "
                         "call is a Duo push per iteration -- a prior run here was "
                         "halted by an account lockout caused exactly that way")
    ap.add_argument("--scratch-root", default="/scratch/st-shallam-1",
                    help="Sockeye scratch is ALLOCATION-scoped: there is no "
                         "/scratch/<user>, so this is the allocation and the user is "
                         "appended")
    ap.add_argument("--remote-data", default=None,
                    help="where the inputs live on the host. Defaults to the tree the "
                         "shell driver already staged: {scratch}/{user}/fabfos_r6/data")
    ap.add_argument("--no-push", action="store_true",
                    help="trust --remote-data is already populated and skip the rsync. "
                         "Saves the checksum pass over 3.4 GB; use it only when nothing "
                         "under data/ has moved")
    ap.add_argument("--apptainer-cache", default=None,
                    help="persistent image store on the host. Must OUTLIVE a single run: "
                         "apptainer resolves ${APPTAINER_CACHEDIR:-<agent home>} and "
                         "agent home carries a per-run timestamp")
    ap.add_argument("--slurm-account", default="st-shallam-1",
                    help="charged on every sbatch. slurm.nf's default is the literal "
                         "placeholder '<slurm_account>', which sbatch rejects")
    ap.add_argument("--work", type=Path, default=WORK)
    ap.add_argument("--timeout-hours", type=float, default=72.0)
    ap.add_argument("--poll-s", type=float, default=120.0)
    a = ap.parse_args()

    a.work.mkdir(parents=True, exist_ok=True)
    ts = int(time.time())

    if a.run and not a.user:
        raise SystemExit("--run needs --user; see its help for why it is never guessed.")
    if a.run and not GIVEN_AT.exists():
        raise SystemExit(
            f"the MetaCyc drop-in is not at {GIVEN_AT}.\n"
            f"  It is LICENSED and not redistributable, so nothing fetches it. It is "
            f"also the independent member of BOTH ensembles -- without it the AAM stack "
            f"loses layer 1 and the direction ensemble keeps only its two correlated "
            f"members. Planning does not need it; drop --run to see the plan.")

    remote_root = None
    if a.run:
        remote_root = a.remote_data or f"{a.scratch_root}/{a.user}/fabfos_r6/data"

    # ---- the agent -------------------------------------------------------------
    if not a.run:
        # A local agent for planning only. The runtime still has to be APPTAINER: it is
        # what decides whether an env declaration resolves its `container:` or its
        # `conda:` key, and the plan is only a claim about the real run if it resolved
        # the same side of that fork.
        agent = Agent(home=Source.FromLocal(a.work / "agent_home"),
                      runtime=Runtime.APPTAINER)
    else:
        print(f"=== engine pin: {assert_pinned_engine()} ===", flush=True)
        cache_dir = a.apptainer_cache or \
            "/arc/project/st-shallam-1/metasmith/container_images"
        agent_path = f"{a.scratch_root}/{a.user}/fabfos_r6/agent_{ts}"
        container = agent_container()
        print(f"=== remote: {a.host}:{agent_path} ===", flush=True)
        print(f"=== agent image: {container} ===", flush=True)
        print(f"=== image store: {cache_dir} ===", flush=True)

        if not a.no_push:
            print(f"=== pushing inputs -> {a.host}:{remote_root} ===", flush=True)
            push_data(a.host, remote_root)
        print("=== placing images on the login node ===", flush=True)
        # The agent image FIRST: it is the one thing that must exist before Deploy() can
        # do anything, and it is the one whose tag moves with the engine pin.
        assert_agent_image(a.host, cache_dir, container)
        place_images(a.host, cache_dir)

        agent = Agent(home=SshSource(host=a.host, path=agent_path).AsSource(),
                      runtime=Runtime.APPTAINER,
                      container=container,
                      # Exported for BOTH sides: the login node writes the store here and
                      # the compute node reads it here. One side missing the variable
                      # resolves to a different directory, finds nothing, and attempts a
                      # pull on a node with no route out.
                      setup_commands=SETUP_COMMANDS +
                                     [f"export APPTAINER_CACHEDIR={cache_dir}"])

    # ---- plan ------------------------------------------------------------------
    print("=== planning ===", flush=True)
    inputs, task = plan(a.work, remote_root, agent)
    if not task.ok:
        print(f"FAILED to plan:\n{getattr(task.plan, 'hints', task.plan)}",
              file=sys.stderr)
        return 1

    used, problems = check_plan(task)

    staged = [(p, n) for p, n, _ in inputs.Iterate()]
    print(f"\nstaged: {len(staged)} item(s) -- {sorted({n for _, n in staged})}")
    # Named rather than counted. "5 transforms were skipped" is not checkable by a
    # reader; "mnx_lookups was skipped" is, and it is the sentence that would be missing
    # if a staging assumption quietly stopped holding.
    print(f"not scheduled, because it is already on disk: "
          f"{sorted(set(SKIPPED) - used)}")

    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    svg = ARTIFACTS / "build_references_bake.svg"
    task.plan.RenderDAG(svg, blacklist_namespaces={"lib", "env", "buildlib"})
    print(f"\nDAG -> {svg}")

    for p in problems:
        print(f"\nFAIL: {p}")
    if problems:
        return 1
    print("\nevery lane is in the plan; nothing already on disk is being rebuilt")

    if not a.run:
        print("\n(plan only -- pass --run --user <name> to execute on the host)")
        return 0

    # ---- execute ---------------------------------------------------------------
    print("=== Deploy() ===", flush=True)
    try:
        agent.Deploy()
    except subprocess.CalledProcessError as e:
        # ONE session, one failure, one message. Never a retry loop: each attempt is a
        # Duo push, and a prior run in this project was halted by an account lockout
        # caused exactly that way.
        print(f"\ndeploy failed ({e}). The connection is multiplexed: open ONE session "
              f"by hand (`ssh {a.host}`), leave it open, and re-run. Do NOT delete the "
              f"ControlMaster socket and do NOT retry in a loop.", file=sys.stderr)
        return 4

    print(f"=== task key: {task.GetKey()} ===", flush=True)
    # on_exist="clear" is safe HERE and only here: agent_path carries a timestamp, so it
    # is a fresh directory every run and there is no prior intermediate to destroy. Never
    # carry this flag onto a resubmission.
    agent.StageWorkflow(task, on_exist="clear")

    # RunWorkflow's config_file DEFAULTS to the `local` preset, which would run every
    # step on whatever node the agent sits on -- the login node. Selected explicitly so a
    # future reader sees the choice rather than a default.
    nxf_config = agent.GetNxfConfigPresets()["slurm"]
    params = {
        "slurmAccount": a.slurm_account,
        # Job arrays are the condition under which overlay filesystems throw bus errors,
        # and this graph is a dozen steps -- there is nothing to batch that is worth the
        # exposure.
        "process_array": 0,
    }
    print(f"=== executor: slurm, account {a.slurm_account}, arrays disabled ===",
          flush=True)
    agent.RunWorkflow(task, config_file=nxf_config, params=params)

    print(f"=== waiting (timeout {a.timeout_hours:.0f}h, poll {a.poll_s:.0f}s) ===",
          flush=True)
    result = agent.WaitForWorkflow(task, timeout_s=int(a.timeout_hours * 3600),
                                   poll_s=a.poll_s)
    print(f"=== status: {result['status']} after "
          f"{result['elapsed_s'] / 3600:.1f}h ===", flush=True)
    for line in result["tail"]:
        print(f"    {line}")
    if result["status"] != "completed":
        return 2

    # "COMPLETED" IS NOT "SUCCEEDED". slurm.nf sets errorStrategy='ignore' once a process
    # exhausts its retries, so a step that died every attempt leaves the workflow green,
    # everything downstream running on nothing, and a results directory that exists and
    # is empty. A previous Sockeye run failed exactly this way -- a missing container
    # read as a 3.6-minute success.
    swallowed = [ln for ln in result["tail"]
                 if "Error is ignored" in ln or "terminated with an error" in ln]
    if swallowed:
        print("\nA STEP FAILED AND NEXTFLOW IGNORED IT -- this is not a result:",
              file=sys.stderr)
        for ln in swallowed:
            print(f"    {ln}", file=sys.stderr)
        print("\nThe real error is in that step's .command.err under "
              "<run>/nxf_work/<hash>/. Do not bake or publish this output.",
              file=sys.stderr)
        return 3

    src = agent.GetResultSource(task)
    print(f"\nresults: {a.host}:{src.GetPath()}")
    return retrieve(agent, task, a.host)


if __name__ == "__main__":
    sys.exit(main())

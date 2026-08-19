#!/usr/bin/env python3
"""R6, the metabolism bake: planned here, executed on hardware we control, in parts.

    PYTHONPATH=src python tests/fabfos/build_references_bake_split.py prepare
    ... build_references_bake_split.py members --run --host chamois --user tliu

WHY A SIBLING AND NOT A FLAG ON THE SOCKEYE DRIVER. `build_references_bake_on_hpc.py`
plans the whole graph and runs it under SLURM, and its Sockeye assumptions are
load-bearing rather than incidental: the two-step Lmod load whose order matters, the
allocation-scoped scratch path that has no per-user directory, the pre-pull onto a login
node because compute nodes have no route out, the assertion that every lane is present.
Threading a conditional through each of those would leave two half-explained paths instead
of two explained ones. This file is the off-cluster driver; that one stays the record of
how the cluster works.

WHY OFF-CLUSTER AT ALL. Three runs died in staging on Sockeye and the cause was not
metasmith: the allocation's Lustre scratch is degraded to the point where a three-byte
write took eighteen seconds on the same host where a write to /arc/home took six
milliseconds, and it reproduced over Globus's separate transfer network, which rules out
the link. The other doors are shut too -- Sockeye forbids compute-node writes to project
space and forbids submitting from it, fir's compute nodes are held for maintenance.

THE FIRST CUT IS THE IMAGE BOUNDARY, and that is not a coincidence to be explained but the
reason the split is cheap. Every bake transform declares `group_by=image`, so the three
environments already partition the lanes into groups that share nothing but tables:

    aam        15 lanes  the preparation, the three mappers, the assembly
    direction   3 lanes  equilibrator, direction_ensemble, direction_bake
    dgbyg       1 lane   dgbyg

So the host boundary goes there, and the only things crossing a host are things that
already cross an image boundary inside the graph. `direction` and `dgbyg` ride the same
host because the second is fifty minutes of work with no reason to hold a machine of its
own -- see bake/dgbyg.py for the measurement that replaced its twenty-four-hour guess.

THE SECOND CUT IS THE AAM BRANCH'S THREE STAGES, which the graph already has: prepare
everything a mapper needs, map once, then assemble and correct. The parts follow them
because their failure characters differ -- the preparation is nine table operations and an
hour, the mapping is most of a day of inference, the assembly is minutes over tables that
already exist -- so a method change in one must not re-pay the others.

THE REST IS ABOUT WALL CLOCK. `direction_ensemble` needs nothing the AAM branch produces
and `direction_bake` needs only its vocabulary, so the thermodynamic members and their
assembly run start to finish beside the long branch and only the final encode waits.

    prepare    -> interm::aam_universe                   no mapper runs
    map        -> the three member tables                the long one, its own host
    assemble   -> vocab, atom_pairs                      fuse, correct, mint
    members    -> direction_member_eq, _dgbyg            concurrent with all three
    direction  -> direction_annotation                   waits on members only
    direction_bake -> direction_ratios                   seconds, once assemble lands

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

METACYC_FILES = ("atom-mappings-smiles.dat", "reactions.dat", "compounds.dat")

TEMP = DATA / "temp"

BAKE_SIF_DIR = REPO / "docker" / "ecspr_bake"
IMG = "docker://quay.io/hallamlab/ecspr_bake:{}"

BRANCHES = {
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
            "interm::aam_universe":   "temp/_seams/aam_universe.parquet",
            "interm::aam_worklist":   "temp/_seams/aam_worklist.parquet",
            "interm::aam_forecast":   "temp/_seams/aam_forecast.parquet",
            "interm::aam_rescue":     "temp/_seams/aam_rescue",
            "interm::aam_partial":    "temp/_seams/aam_partial",
            "interm::aam_algebra":    "temp/_seams/aam_algebra",
            "lookup::element_counts": "temp/_seams/element_counts.parquet",
        },
        executor=dict(cpus=8, memory="96 GB", queue=4),
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
        evidence={"rxnmapper": "rxnmapper", "indigo": "indigo",
                  "localmapper": "localmapper"},
        outputs={
            "interm::aam_member_rxnmapper":   "temp/_seams/aam_member_rxnmapper.parquet",
            "interm::aam_member_indigo":      "temp/_seams/aam_member_indigo.parquet",
            "interm::aam_member_localmapper": "temp/_seams/aam_member_localmapper.parquet",
        },
        executor=dict(cpus=32, memory="110 GB", queue=6),
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
        executor=dict(cpus=8, memory="96 GB", queue=2),
    ),
    "redox": dict(
        images=[IMG.format("aam")],
        lanes={"aam_redox"},
        targets=["interm::aam_pairs"],
        inputs=["lookup::reactions", "lookup::metabolites", "lookup::atom_ranks"],
        imports={"interm::aam_stack": "temp/_seams/aam_stack.parquet"},
        evidence={"redox": "aam_redox"},
        outputs={"interm::aam_pairs": "temp/_seams/aam_pairs"},
        executor=dict(cpus=8, memory="64 GB", queue=1),
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
        evidence={"reference": "aam_reference"},
        outputs={
            "ref::metabolism_vocab": "temp/metabolism/vocab.parquet",
            "ref::atom_pairs":       "temp/metabolism/atom_pairs.parquet",
            "interm::aam_ledger":    "temp/_seams/aam_ledger.parquet",
        },
        executor=dict(cpus=8, memory="64 GB", queue=1),
    ),
    "members": dict(
        images=[IMG.format("direction"), IMG.format("dgbyg")],
        lanes={"equilibrator", "dgbyg"},
        targets=["interm::direction_member_eq", "interm::direction_member_dgbyg"],
        inputs=["fabfos_data::metanetx", "fabfos_data::equilibrator"],
        imports={},
        evidence={"equilibrator": "equilibrator", "dgbyg": "dgbyg"},
        outputs={
            "interm::direction_member_eq":    "temp/_seams/direction_member_eq.parquet",
            "interm::direction_member_dgbyg": "temp/_seams/direction_member_dgbyg.parquet",
        },
        executor=dict(cpus=14, memory="120 GB", queue=4),
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
        evidence={
            "metacyc_direction":     "direction_ensemble",
            "direction_calibration": "direction_ensemble",
        },
        outputs={
            "interm::direction_annotation": "temp/_seams/direction_annotation.parquet",
        },
        executor=dict(cpus=8, memory="64 GB", queue=2),
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
        evidence={},
        outputs={"ref::direction_ratios": "temp/metabolism/direction.parquet"},
        executor=dict(cpus=8, memory="64 GB", queue=1),
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


def push_data(host: str, branch: str, remote_root: str) -> None:
    spec = BRANCHES[branch]
    rels = [ALL_INPUTS[t] for t in spec["inputs"]] + list(spec["imports"].values())
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
        subprocess.run(["rsync", "-a", "--size-only", "--partial", "--info=stats1",
                        f"{src}/" if src.is_dir() else str(src),
                        f"{host}:{remote_root}/{rel}"], check=True)


def place_images(host: str, branch: str, cache_dir: str, container: str) -> None:
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
    from metasmith._build_hash import compute_build_hash
    from metasmith.constants import VERSION
    return f"docker://quay.io/hallamlab/metasmith:{VERSION}-{compute_build_hash()}"


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
    import shutil

    spec = BRANCHES[branch]
    staging.mkdir(parents=True, exist_ok=True)
    print(f"\n=== retrieving {branch} into {TEMP.relative_to(REPO)} ===", flush=True)
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
                    help="which part of the graph this run owns. prepare -> map ->\n"
                         "assemble in order on the AAM host; members and direction run\n"
                         "beside them on another; direction_bake waits on both branches.\n"
                         "redox and reference are recovery cuts of assemble")
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
    if "fabfos_data::aam_cache" in spec["inputs"]:
        (DATA / ALL_INPUTS["fabfos_data::aam_cache"]).mkdir(parents=True, exist_ok=True)
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

    if not a.run:
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
                      setup_commands=[f"export APPTAINER_CACHEDIR={cache_dir}"])

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

    print("=== Deploy() ===", flush=True)
    try:
        agent.Deploy()
    except subprocess.CalledProcessError as e:
        print(f"\ndeploy failed ({e}). Open ONE session by hand (`ssh {a.host}`), leave "
              f"it open, and re-run. Do NOT retry in a loop.", file=sys.stderr)
        return 4

    print(f"=== task key: {task.GetKey()} ===", flush=True)
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

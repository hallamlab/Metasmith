#!/usr/bin/env python3
"""Driver 3/3: GPR table(s) + conditions + tests -> the measurement, on either arm.

    gpr_table + conditions + ecspr_tests + atom_pairs + direction_ratios
        -> ecspr_measure -> ecspr::results
    gpr_table + conditions + gem_tests + frame + media + metanetx_release
        -> fba_measure   -> ecspr::fba_results

TWO ARMS, ONE CONDITIONS TABLE. ``--fba`` attaches the flux balance arm to a unit
that already has one, and both transforms read that unit's own conditions table --
so a construction measured as atom conductance and as flux balance is the SAME
construction, and the comparison is a property of the plan rather than of two
invocations agreeing by hand. A unit with no ``--fba`` plans exactly as before.

One transform per arm, deliberately -- see ``transforms/fabfos/ecspr_measure.py``. Each
``--unit`` is its own ``fabfos::experiment`` (a GPR table paired with its own
condition set); ``ecspr_measure`` is ``group_by=exp``, so N units resolve to N
jobs in one plan with no change to the transform (see the "runtime fan-out"
pattern the fosmid pipeline already relies on). Both reference parquets
(atom-atom mapping, direction ratios) are NETWORK-AGNOSTIC and shared across
every unit, never per-unit -- pinning them to a run would wrongly imply the
basis changes between runs.

REFERENCE DEFAULTS. ``atom_pairs`` and ``direction_ratios`` default to this
repo's baked copies at ``data/fabfos/processed/metabolism_bake/{atom_pairs,
direction}.parquet``. Those are stored CODED -- integer reaction and metabolite
ids against the bake's own vocab -- and the graph builder reads the string
schema, so the caller decodes first (`benchmarks/eydallin/bake_pairs.py`).
Handing the coded table straight through does not raise: `element == "C"` is
compared against integers, matches nothing, and an empty graph is measured.

A COMPOSED PAIR TABLE IS NOT NETWORK-AGNOSTIC, and that is the one place this
driver's shape is decided by something other than the type contract. The
reference parquets are shared because they are static functions of the
MNXR/MNXM id space. A community network built by ``ecspr.model.compose`` is not: it
carries organism-prefixed metabolite ids and bridge rows that mean nothing to
any other network. Rather than pin the reference type per-experiment -- which
would change the measurement transform and weaken a contract that is right for
every other caller -- the caller invokes this driver ONCE PER COMPOSED GRAPH and
passes that graph's tables through ``--atom-pairs`` / ``--direction-ratios``.
See ``research/fabfos/examples/nostoc_ecspr.py``.

`ecspr::metabolite_names` is NOT staged. Deciding which MNXM a name means is
what someone does while WRITING a conditions table -- the table names its hubs
as ids -- so it is the experiment designer's input, not this plan's. The type
still exists for whoever stages it elsewhere.

The measurement itself is `ecspr ground`, which the `ecspr` conda package
provides. `--runtime mamba` runs it from the `ecspr` env with no container at
all, which is also what makes a benchmark run and this plan the same code.

Usage:

    python -m fabfos.pipelines.ecspr \\
        --unit pool_a:gpr_a.parquet:conditions_a.parquet:tests_a.parquet \\
        --unit pool_b:gpr_b.parquet:conditions_b.parquet:tests_b.parquet

A UNIT NAMES SEVERAL GPR TABLES. Repeat the GPR field with a comma -- a
conditions table selects across the host table and the clone table by name, and
staging only one of them resolves the condition to half of itself.

Planning is what it does; ``--run`` opts into executing. The DAG lands under
``research/fabfos/reports/dag/`` unless ``--dag`` says otherwise.
"""
from __future__ import annotations

import argparse
import shutil
import sys
from dataclasses import dataclass
from pathlib import Path

from metasmith.python_api import (
    Agent,
    Runtime,
    DataInstanceLibrary,
    Source,
    TargetBuilder,
    TransformInstanceLibrary,
)

from .. import refs
from . import common

DOMAINS = ["fabfos"]

DEFAULT_ATOM_PAIRS = common.DATA_PROCESSED / refs.relpaths_for("ecspr::atom_pairs")[0]
DEFAULT_DIRECTION_RATIOS = common.DATA_PROCESSED / refs.relpaths_for("ecspr::direction_ratios")[0]
DEFAULT_METANETX = common.REPO_ROOT / "data/fabfos/originals/metanetx/4.5"


@dataclass
class Unit:
    name: str
    gpr_tables: list
    conditions: Path
    tests: Path
    # The flux balance arm, attached by `--fba` rather than folded into `--unit`. It
    # reads the SAME conditions table -- that is the whole point of the split -- and adds
    # only what an LP needs that a conductance measurement does not.
    gem_tests: Path | None = None
    frames: list | None = None
    media: Path | None = None

    @property
    def measures_flux(self) -> bool:
        return bool(self.gem_tests and self.frames and self.media)


def parse_unit(spec: str) -> Unit:
    parts = spec.split(":")
    if len(parts) != 4:
        raise argparse.ArgumentTypeError(
            f"--unit expects NAME:GPR_TABLES:CONDITIONS:TESTS, where GPR_TABLES is one "
            f"or more paths separated by commas; got [{spec}]"
        )
    name, gpr, cond, tests = parts
    tables = [Path(g) for g in gpr.split(",") if g]
    if not tables:
        raise argparse.ArgumentTypeError(f"--unit {name} names no GPR table")
    return Unit(name=name, gpr_tables=tables, conditions=Path(cond),
                tests=Path(tests))


def parse_fba(spec: str) -> tuple:
    parts = spec.split(":")
    if len(parts) != 4:
        raise argparse.ArgumentTypeError(
            f"--fba expects NAME:GEM_TESTS:FRAMES:MEDIA, where FRAMES is one or more "
            f"model paths separated by commas; got [{spec}]")
    name, tests, frames, media = parts
    models = [Path(f) for f in frames.split(",") if f]
    if not models:
        raise argparse.ArgumentTypeError(f"--fba {name} names no frame")
    return name, Path(tests), models, Path(media)


def attach_fba(units: list, specs) -> list:
    by_name = {u.name: u for u in units}
    for name, tests, frames, media in specs or ():
        if name not in by_name:
            raise SystemExit(f"--fba names unit {name!r}, which no --unit declares "
                             f"(have {sorted(by_name)})")
        u = by_name[name]
        u.gem_tests, u.frames, u.media = tests, frames, media
    return units


def build_inputs(work: Path, *, units: list[Unit], atom_pairs: Path | None,
                  direction_ratios: Path | None, metanetx: Path | None = None,
                  stage: str = "reference", use_pinned_refs: bool = True,
                  ) -> tuple[DataInstanceLibrary, dict[str, Path], "DataInstanceLibrary | None"]:
    lib = common.resolve_library_root()

    inputs = DataInstanceLibrary(work / "inputs.xgdb")
    inputs.Purge()
    for ns in ("fabfos", "annotation", "ecspr", "ref"):
        inputs.AddTypeLibrary(lib / "data_types" / f"{ns}.yml")

    def _add(path: Path, dtype: str, *, name: str, parents=None):
        p = Path(path).expanduser().resolve()
        if stage == "copy":
            dest = inputs.location / name
            if p.is_dir():
                shutil.copytree(p, dest, dirs_exist_ok=True)
            else:
                shutil.copy(p, dest)
            inputs.AddItem(name, dtype, parents=parents or set())
        else:
            inputs.AddItem(p, dtype, parents=parents or set())

    for unit in units:
        exp = inputs.AddValue(f"experiment_{unit.name}.txt", unit.name, "fabfos::experiment")
        # THE STAGED NAME IS THE NAME THE CONDITIONS TABLE USES. A condition's `table`
        # column is a basename, so renaming a staged table to `<unit>.gpr.parquet` would
        # make every condition that names it resolve to nothing -- and an empty
        # selection raises rather than reporting a null, which is the point.
        for g in unit.gpr_tables:
            _add(g, "annotation::gpr_table", name=Path(g).name, parents={exp})
        _add(unit.conditions, "ecspr::conditions",
             name=f"{unit.name}.conditions.parquet", parents={exp})
        _add(unit.tests, "ecspr::ecspr_tests",
             name=f"{unit.name}.ecspr_tests.parquet", parents={exp})
        if not unit.measures_flux:
            continue
        _add(unit.gem_tests, "ecspr::gem_tests",
             name=f"{unit.name}.gem_tests.parquet", parents={exp})
        _add(unit.media, "ecspr::media", name=f"{unit.name}.media.parquet",
             parents={exp})
        # A FRAME KEEPS ITS OWN NAME, like a GPR table and for the same reason: a
        # `gem_tests` row names its frame by stem.
        for f in unit.frames:
            _add(f, "ecspr::frame", name=Path(f).name, parents={exp})

    if any(u.measures_flux for u in units):
        mnx = Path(metanetx) if metanetx else DEFAULT_METANETX
        if not mnx.exists():
            raise SystemExit(
                f"the flux balance arm needs a MetaNetX release and {mnx} is not there. "
                f"Without it a lane-asserted reaction the frame never named is lost, "
                f"which on a de-novo condition is most of them; pass --metanetx")
        _add(mnx, "ref::metanetx_release", name=mnx.name)

    stubs: dict[str, Path] = {}
    pinned = refs.load_pinned_refs(common.DATA_PROCESSED) if use_pinned_refs else None
    covered = set(pinned.manifest.values()) if pinned is not None else set()
    overridden = set()
    for dtype, given, default, stem in (
        ("ecspr::atom_pairs", atom_pairs, DEFAULT_ATOM_PAIRS, "atom_pairs"),
        ("ecspr::direction_ratios", direction_ratios, DEFAULT_DIRECTION_RATIOS, "direction"),
    ):
        if dtype in covered and given is None:
            continue
        if dtype in covered:
            overridden.add(dtype)
        src = given if given is not None else default
        if stage == "copy" and src is not None and Path(src).expanduser().exists():
            _add(Path(src), dtype, name=f"{stem}.parquet")
            continue
        path, real = common.stage_ref(inputs, work, dtype, given=given, default=default)
        if not real:
            stubs[dtype] = path

    inputs.Save()
    if pinned is not None:
        pinned = refs.refs_view(pinned, set(refs.ECSPR_REFS) & covered - overridden)
    return inputs, stubs, pinned


def generate_workflow(work: Path, *, units: list[Unit], atom_pairs: Path | None,
                       direction_ratios: Path | None, runtime: Runtime,
                       metanetx: Path | None = None,
                       agent_env: str | None = None, stage: str = "reference",
                       agent=None, on_inputs=None):
    lib = common.resolve_library_root()
    inputs, stubs, pinned_refs = build_inputs(
        work, units=units, atom_pairs=atom_pairs,
        direction_ratios=direction_ratios, metanetx=metanetx, stage=stage,
    )
    if on_inputs is not None:
        on_inputs(inputs)

    resources = [
        DataInstanceLibrary.Load(lib / "resources" / "env"),
        DataInstanceLibrary.Load(lib / "resources" / "lib"),
        *([pinned_refs] if pinned_refs is not None else []),
        inputs,
    ]
    transforms = [TransformInstanceLibrary.Load(lib / f"transforms/{d}") for d in DOMAINS]

    targets = TargetBuilder()
    targets.Add("ecspr::results")
    # BOTH ARMS IN ONE PLAN, so the comparison happens inside one run against one set of
    # staged tables rather than across two invocations that could have drifted apart.
    if any(u.measures_flux for u in units):
        targets.Add("ecspr::fba_results")

    if agent is None:
        agent = common.make_agent(work, runtime, container=agent_env)
    task = agent.GenerateWorkflow(
        samples=list(inputs.AsSamples("fabfos::experiment")),
        resources=resources,
        transforms=transforms,
        targets=targets,
    )
    return agent, task, stubs


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--unit", action="append", required=True, type=parse_unit,
                    metavar="NAME:GPR_TABLES:CONDITIONS:TESTS",
                    help="one measurement unit (repeatable): the GPR tables the "
                         "conditions select across (comma-separated), the conditions "
                         "table, and the ecspr_tests table")
    p.add_argument("--fba", action="append", default=None, type=parse_fba,
                    metavar="NAME:GEM_TESTS:FRAMES:MEDIA",
                    help="add the flux balance arm to the named unit (repeatable). It "
                         "reads that unit's own conditions table, so the two arms measure "
                         "the same construction rather than two versions of it")
    p.add_argument("--metanetx", default=None, metavar="DIR",
                    help=f"MetaNetX release directory (default: {DEFAULT_METANETX})")
    p.add_argument("--atom-pairs", default=None, metavar="PARQUET")
    p.add_argument("--direction-ratios", default=None, metavar="PARQUET")
    p.add_argument("--staging", default=None, help="working dir (default: <output>/_fabfos)")
    p.add_argument("--output", default="./fabfos_ecspr_out", help="output directory")
    p.add_argument("--dag", default="research/fabfos/reports/dag/ecspr", help="path base for the rendered SVG")
    p.add_argument("--runtime", choices=[r.value for r in Runtime],
                    default=Runtime.APPTAINER.value)
    p.add_argument("--threads", type=int, default=8)
    p.add_argument("--run", action="store_true", default=False, help="also execute the plan")
    common.add_execution_args(p)
    return p


def main(argv=None) -> int:
    a = _build_parser().parse_args(argv)

    output = Path(a.output).resolve()
    staging = Path(a.staging).resolve() if a.staging else output / "_fabfos"
    staging.mkdir(parents=True, exist_ok=True)

    runtime = Runtime(a.runtime)

    common.require_method(a.require_method)

    print("=== ecspr: staging inputs + planning ===")
    agent, task, stubs = generate_workflow(
        staging, units=attach_fba(a.unit, a.fba),
        atom_pairs=Path(a.atom_pairs) if a.atom_pairs else None,
        direction_ratios=Path(a.direction_ratios) if a.direction_ratios else None,
        metanetx=Path(a.metanetx) if a.metanetx else None,
        runtime=runtime, agent_env=a.agent_env,
    )

    if not task.ok:
        print("\nPLAN DID NOT RESOLVE:", file=sys.stderr)
        print(task.plan, file=sys.stderr)
        return 1

    common.print_plan(task)
    common.report_stubs("ecspr", stubs)

    if a.dag:
        base = (common.REPO_ROOT / a.dag).resolve() if not Path(a.dag).is_absolute() else Path(a.dag)
        svg = common.render_dag(task, base)
        print(f"\nDAG -> {svg}")

    if a.run:
        print("\n=== ecspr: running ===")
        results = common.run_workflow(
            agent, task, staging, threads=a.threads,
            config_file=common.resolve_nxf_config(a.config, runtime),
        )
        print(f"results -> {results}")

    return 0


if __name__ == "__main__":
    sys.exit(main())

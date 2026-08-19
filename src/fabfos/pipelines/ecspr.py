#!/usr/bin/env python3
"""Driver 3/3: GPR table(s) + conditions -> the ECSPr measurement.

    gpr_table + conditions + atom_pairs + direction_ratios
        -> ecspr_measure -> ecspr::results

One transform, deliberately -- see ``transforms/fabfos/ecspr_measure.py``. Each
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
        --unit pool_a:gpr_a.parquet:conditions_a.parquet \\
        --unit pool_b:gpr_b.parquet:conditions_b.parquet

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

from . import common

DOMAINS = ["fabfos"]

DEFAULT_ATOM_PAIRS = common.DATA_PROCESSED / "metabolism_bake" / "atom_pairs.parquet"
DEFAULT_DIRECTION_RATIOS = common.DATA_PROCESSED / "metabolism_bake" / "direction.parquet"


@dataclass
class Unit:
    name: str
    gpr_table: Path
    conditions: Path


def parse_unit(spec: str) -> Unit:
    parts = spec.split(":")
    if len(parts) != 3:
        raise argparse.ArgumentTypeError(
            f"--unit expects NAME:GPR_TABLE:CONDITIONS, got [{spec}]"
        )
    name, gpr, cond = parts
    return Unit(name=name, gpr_table=Path(gpr), conditions=Path(cond))


def build_inputs(work: Path, *, units: list[Unit], atom_pairs: Path | None,
                  direction_ratios: Path | None, stage: str = "reference"
                  ) -> tuple[DataInstanceLibrary, dict[str, Path]]:
    lib = common.resolve_library_root()

    inputs = DataInstanceLibrary(work / "inputs.xgdb")
    inputs.Purge()
    for ns in ("fabfos", "annotation", "ecspr"):
        inputs.AddTypeLibrary(lib / "data_types" / f"{ns}.yml")

    def _add(path: Path, dtype: str, *, name: str, parents=None):
        p = Path(path).expanduser().resolve()
        if stage == "copy":
            shutil.copy(p, inputs.location / name)
            inputs.AddItem(name, dtype, parents=parents or set())
        else:
            inputs.AddItem(p, dtype, parents=parents or set())

    for unit in units:
        exp = inputs.AddValue(f"experiment_{unit.name}.txt", unit.name, "fabfos::experiment")
        _add(unit.gpr_table, "annotation::gpr_table",
             name=f"{unit.name}.gpr.parquet", parents={exp})
        _add(unit.conditions, "ecspr::conditions",
             name=f"{unit.name}.conditions.parquet", parents={exp})

    stubs: dict[str, Path] = {}
    for dtype, given, default, stem in (
        ("ecspr::atom_pairs", atom_pairs, DEFAULT_ATOM_PAIRS, "atom_pairs"),
        ("ecspr::direction_ratios", direction_ratios, DEFAULT_DIRECTION_RATIOS, "direction"),
    ):
        src = given if given is not None else default
        if stage == "copy" and src is not None and Path(src).expanduser().exists():
            _add(Path(src), dtype, name=f"{stem}.parquet")
            continue
        path, real = common.stage_ref(inputs, work, dtype, given=given, default=default)
        if not real:
            stubs[dtype] = path

    inputs.Save()
    return inputs, stubs


def generate_workflow(work: Path, *, units: list[Unit], atom_pairs: Path | None,
                       direction_ratios: Path | None, runtime: Runtime,
                       agent_env: str | None = None, stage: str = "reference",
                       agent=None, on_inputs=None):
    lib = common.resolve_library_root()
    inputs, stubs = build_inputs(
        work, units=units, atom_pairs=atom_pairs,
        direction_ratios=direction_ratios, stage=stage,
    )
    if on_inputs is not None:
        on_inputs(inputs)

    resources = [
        DataInstanceLibrary.Load(lib / "resources" / "env"),
        DataInstanceLibrary.Load(lib / "resources" / "lib"),
        inputs,
    ]
    transforms = [TransformInstanceLibrary.Load(lib / f"transforms/{d}") for d in DOMAINS]

    targets = TargetBuilder()
    targets.Add("ecspr::results")

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
                    metavar="NAME:GPR_TABLE:CONDITIONS",
                    help="one measurement unit (repeatable): a GPR table + its condition set")
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
        staging, units=a.unit,
        atom_pairs=Path(a.atom_pairs) if a.atom_pairs else None,
        direction_ratios=Path(a.direction_ratios) if a.direction_ratios else None,
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

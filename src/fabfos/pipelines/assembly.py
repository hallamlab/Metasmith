#!/usr/bin/env python3
"""Driver 1/3: reads, host, pCC1 -> resolved fosmid inserts.

    reads (per pool) -> QC/clean -> host filter -> {megahit, spades}
        -> resolve_inserts (backbone junction map + cut + cross-assembler dedup)
        -> fabfos::putative_inserts + fabfos::insert_metadata

One metasmith job per pool up through the assemblers, then ONE job over every
pool's pieces for the dedup (``resolve_inserts`` is ``group_by=exp``; see its
docstring in ``transforms/fabfos/resolve_inserts.py``). Host filtering is not
optional here: ``resolve_inserts`` pins its assembly requirement to a
``host_filtered_short_reads`` ancestor, which is what forces
``background_filter`` into the plan, so ``--host`` is required rather than a
flag.

KNOWN GAP -- ``algorithm::fabfos_recovery.py``. ``resolve_inserts`` requires
this as a staged CLI script (the actual map/rectify/dedup implementation), and
per its type declaration (``data_types/algorithm.yml``) it is meant to resolve
against an installed ``src/fabfos/algorithm/`` package that does not exist in
this checkout yet -- the port from the deployed method is still pending. Pass
``--recovery-lib`` once it exists; until then this stages an empty stub, which
plans and renders fine but cannot execute.

Usage:

    python -m fabfos.pipelines.assembly \\
        --reads pool_0.fq.gz --reads pool_1.fq.gz \\
        --host host_background.fna --pcc1 pcc1fos_backbone.fna \\
        --dag reports/dag/assembly --plan-only

    python -m fabfos.pipelines.assembly --reads pool_0.fq.gz \\
        --host host.fna --pcc1 pcc1.fna --output ./out --run
"""
from __future__ import annotations

import argparse
import json
import sys
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

DOMAINS = ["assembly", "fabfos"]


def build_inputs(work: Path, *, experiment: str, reads: list[Path], parity: str,
                  host: Path, pcc1: Path, recovery_lib: Path | None) -> tuple[DataInstanceLibrary, dict[str, Path]]:
    lib = common.resolve_library_root()

    inputs = DataInstanceLibrary(work / "inputs.xgdb")
    inputs.Purge()
    for ns in ("sequences", "fabfos", "algorithm"):
        inputs.AddTypeLibrary(lib / "data_types" / f"{ns}.yml")

    exp = inputs.AddValue("experiment.txt", experiment, "fabfos::experiment")

    read_type = "sequences::short_reads_pe" if parity == "paired" else "sequences::short_reads_se"
    for i, r in enumerate(reads):
        meta = inputs.AddValue(
            f"read_metadata_{i}.json",
            {"parity": parity, "length_class": "short"},
            "sequences::read_metadata",
            parents={exp},
        )
        inputs.AddItem(Path(r).expanduser().resolve(), read_type, parents={meta})

    inputs.AddItem(Path(host).expanduser().resolve(), "sequences::background_genome", parents={exp})
    inputs.AddItem(Path(pcc1).expanduser().resolve(), "fabfos::vector_backbone", parents={exp})

    stubs: dict[str, Path] = {}
    _, real = common.stage_ref(inputs, work, "algorithm::fabfos_recovery.py", given=recovery_lib)
    if not real:
        stubs["algorithm::fabfos_recovery.py"] = work / "stubs" / "algorithm__fabfos_recovery.py"

    inputs.Save()
    return inputs, stubs


def generate_workflow(work: Path, *, experiment: str, reads: list[Path], parity: str,
                       host: Path, pcc1: Path, recovery_lib: Path | None,
                       runtime: Runtime, agent_env: str | None = None):
    lib = common.resolve_library_root()
    inputs, stubs = build_inputs(
        work, experiment=experiment, reads=reads, parity=parity,
        host=host, pcc1=pcc1, recovery_lib=recovery_lib,
    )

    resources = [
        DataInstanceLibrary.Load(lib / "resources" / "env"),
        DataInstanceLibrary.Load(lib / "resources" / "lib"),
        inputs,
    ]
    transforms = [TransformInstanceLibrary.Load(lib / f"transforms/{d}") for d in DOMAINS]

    targets = TargetBuilder()
    ins = targets.Add("fabfos::putative_inserts")
    targets.Add("fabfos::insert_metadata")
    # Coverage of the RECOVERED inserts (pinned to `ins`), not the raw assembly --
    # standalone target, since nothing downstream consumes it.
    targets.Add("sequences::assembly_stats", parents={ins})

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
    p.add_argument("--experiment", default="fabfos_assembly", help="run/grouping name")
    p.add_argument("--reads", action="append", required=True, metavar="FASTQ",
                    help="one pooled-clone read file (repeatable, one per pool)")
    p.add_argument("--parity", choices=["paired", "single"], default="paired",
                    help="paired == one interleaved fastq per pool (default); single == single-end")
    p.add_argument("--host", required=True, metavar="FASTA", help="host background genome to filter out")
    p.add_argument("--pcc1", required=True, metavar="FASTA", help="pCC1fos vector backbone reference")
    p.add_argument("--recovery-lib", default=None, metavar="PY",
                    help="algorithm::fabfos_recovery.py script; omit to stage a stub (plan-only)")
    p.add_argument("--staging", default=None, help="working dir (default: <output>/_fabfos)")
    p.add_argument("--output", default="./fabfos_assembly_out", help="output directory")
    p.add_argument("--dag", default="reports/dag/assembly", help="path base for the rendered SVG")
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

    reads = [Path(r) for r in a.reads]
    runtime = Runtime(a.runtime)

    common.require_method(a.require_method)

    print("=== assembly: staging inputs + planning ===")
    agent, task, stubs = generate_workflow(
        staging, experiment=a.experiment, reads=reads, parity=a.parity,
        host=Path(a.host), pcc1=Path(a.pcc1), recovery_lib=Path(a.recovery_lib) if a.recovery_lib else None,
        runtime=runtime, agent_env=a.agent_env,
    )

    if not task.ok:
        print("\nPLAN DID NOT RESOLVE:", file=sys.stderr)
        print(task.plan, file=sys.stderr)
        return 1

    common.print_plan(task)
    common.report_stubs("assembly", stubs)

    if a.dag:
        base = (common.REPO_ROOT / a.dag).resolve() if not Path(a.dag).is_absolute() else Path(a.dag)
        svg = common.render_dag(task, base)
        print(f"\nDAG -> {svg}")

    if a.run:
        print("\n=== assembly: running ===")
        results = common.run_workflow(
            agent, task, staging, threads=a.threads,
            config_file=common.resolve_nxf_config(a.config, runtime),
        )
        print(f"results -> {results}")

    return 0


if __name__ == "__main__":
    sys.exit(main())

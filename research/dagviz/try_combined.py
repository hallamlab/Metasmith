#!/usr/bin/env python3
"""One solve over both read shapes at once. Prototype for the combined panels."""

import os
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
os.environ.setdefault("E2_CACHE_DIR", str(HERE / "e2cache"))

import render_e2 as R  # noqa: E402
import _common as c  # noqa: E402
import e2_cami as e2  # noqa: E402
from metasmith.python_api import (  # noqa: E402
    DataInstanceLibrary, TransformInstanceLibrary, TargetBuilder,
)

LIMIT = int(os.environ.get("LIMIT", "2"))


def combined_targets():
    """Both arms' targets, asked for in the one language both cases speak.

    `e2::assembly` is the supertype of `megahit_assembly` and `flye_assembly`,
    so one target slot lets each case bind the assembler its reads admit.
    Naming either subtype instead asks every case for both and drops the lot.
    """
    t = TargetBuilder()
    asm = t.Add("e2::assembly")
    which = os.environ.get("TARGETS", "all")
    if which == "asm":
        return t
    if which == "bins":
        for binner in ("metabat2", "semibin2", "comebin", "das_tool"):
            t.Add(f"e2::{binner}_bin", parents=[asm])
        return t
    if which == "checkm2":
        bins = t.Add("e2::metabat2_bin", parents=[asm])
        t.Add("e2::checkm2_quality", parents=[bins])
        return t
    if os.environ.get("SHORT_REPORTS") == "1":
        for report in ("fastqc_raw_reports", "fastqc_trimmed_reports", "fastp_json", "fastp_html"):
            t.Add(f"e2::{report}")
    for binner in ("metabat2", "semibin2", "comebin", "das_tool"):
        bins = t.Add(f"e2::{binner}_bin", parents=[asm])
        t.Add(f"e2::{binner}_contig_to_bin", parents=[asm])
        t.Add("e2::checkm2_quality", parents=[bins])
    t.Add("e2::das_tool_summary", parents=[asm])
    t.Add("e2::amber_results")
    t.Add("e2::amber_bin_metrics")
    return t


def main():
    arms = e2.enumerate_arms()
    cache = Path(os.environ["E2_CACHE_DIR"])
    samples, smith = [], None
    for arm in os.environ.get("ARMS", "short,long").split(","):
        s = c.agent_for("cami", False, cache / f"dryrun_home_{arm}")
        smith = smith or s
        inputs, globals_lib = e2.declare_givens(s, arm, arms[arm][:LIMIT], ensure=False)
        samples += list(inputs.AsSamples("e2::read_metadata"))
        last_globals = globals_lib
    print(f"{len(samples)} samples across both arms")
    task = smith.GenerateWorkflow(
        samples=samples,
        resources=[
            DataInstanceLibrary.Load(c.LIBRARY / "resources" / "e2"),
            DataInstanceLibrary.Load(c.MLIB / "resources" / "lib"),
            last_globals,
        ],
        transforms=[TransformInstanceLibrary.Load(c.LIBRARY / "transforms" / "e2")],
        targets=combined_targets(),
        max_iter=int(os.environ.get("MAX_ITER", "256")),
        seed=int(os.environ.get("SEED", "42")),
    )
    print("ok:", task.ok)
    print("dropped:", sorted(task.plan.dropped_targets))
    c.print_plan(task, 26)


if __name__ == "__main__":
    sys.exit(main())

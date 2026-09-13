#!/usr/bin/env python3
"""E4: metaGEM's published MAGs through Prodigal, CarveMe (CPLEX) and MEMOTE.

`run` solves against a local dry-run home and renders the DAG. `import`, `--stage-only`
and `--launch` act on the fir agent home, from a Slurm job there, after
e4_extract_mags.sh has unpacked the MAGs.

Subcommands: list, import, run [--study ...] [--limit N] [--dag] [--stage-only | --launch].
"""

import argparse
import csv
import os
import sys
from collections import Counter, defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
CACHE_DIR = Path(os.environ.get("E4_CACHE_DIR", HERE / ".cache" / "e4"))

import _common as c  # noqa: E402
from metasmith.python_api import (  # noqa: E402
    DataInstanceLibrary, TransformInstanceLibrary, TargetBuilder, Resources, Size, Duration,
)

MAG_LIST = HERE / "e4_published_mags.tsv"
PUBLISHED = Path(os.environ.get("METAGEM_PUBLISHED", "/scratch/phyberos/metagem/published"))
STUDY_ORDER = ["li2019", "korem2015", "karlsson2013", "bissett_base", "sunagawa2015"]
MEDIUM_TSV = c.REPO / "research" / "metasmith_libraries" / "carveme_m8_medium.tsv"
MEDIUM_NAME = "M8"
CPLEX_ROOT = Path(os.environ.get("CPLEX_ROOT", "/home/phyberos/projects/rpp-shallam/phyberos/cplex/cplex_runtime"))

RESOURCE_OVERRIDES = {
    # B11: 5 of 100+ live gapfills hit the transform's 2 h wall and were ignored.
    "carveme_from_orfs_cplex": Resources(memory=Size.GB(16), cpus=4, duration=Duration(hours=12)),
}


def enumerate_mags():
    rows = list(csv.DictReader(MAG_LIST.open(), delimiter="\t"))
    return [(r["study"], r["mag"], PUBLISHED / r["study"] / "mags" / f"{r['mag']}.fa") for r in rows]


def select(mags, args):
    if args.study:
        unknown = set(args.study) - set(STUDY_ORDER)
        if unknown:
            sys.exit(f"unknown study: {sorted(unknown)}")
        mags = [m for m in mags if m[0] in args.study]
    if args.limit:
        per_study = defaultdict(list)
        for m in mags:
            per_study[m[0]].append(m)
        mags = [m for s in STUDY_ORDER for m in per_study[s][: args.limit]]
    return mags


def declare_globals(smith, solver, location, ensure):
    """The medium and solver, cited as their own resource library."""
    givens = smith.PoolGivens()
    c.add_value(givens, "ref/modelling::media", MEDIUM_TSV.read_text(), "modelling::media", tags=["reference"])
    c.add_value(givens, "ref/modelling::medium_name", MEDIUM_NAME, "modelling::medium_name", tags=["reference"])
    if solver == "cplex":
        c.declare_refs(givens, {"modelling::cplex_installation": CPLEX_ROOT})
    return c.cite(givens, location, [c.MLIB / "data_types" / "modelling.yml"], ensure)


def declare_givens(smith, mags, ensure):
    givens = smith.PoolGivens()
    for study, mag, path in mags:
        c.add_file(givens, f"e4/{study}/{mag}", path, "sequences::bin_fasta", tags=["e4", study])
    return c.cite(givens, CACHE_DIR / "e4_inputs.xgdb", [c.MLIB / "data_types" / "sequences.yml"], ensure)


def build_targets(solver):
    t = TargetBuilder()
    orfs = t.Add("sequences::bin_orfs")
    model_type = "modelling::carveme_model_cplex" if solver == "cplex" else "modelling::carveme_model"
    model = t.Add(model_type, parents=[orfs])
    t.Add("modelling::memote_score", parents=[model])
    return t


def cmd_list(args):
    mags = select(enumerate_mags(), args)
    for study, n in sorted(Counter(m[0] for m in mags).items(), key=lambda kv: STUDY_ORDER.index(kv[0])):
        print(f"{study:14s} {n:5d} MAGs")
    print(f"{len(mags)} MAGs")
    return 0


def cmd_run(args):
    mags = select(enumerate_mags(), args)
    if not mags:
        sys.exit("no MAGs selected")
    print(f"{len(mags)} MAGs: " + ", ".join(f"{s}={n}" for s, n in Counter(m[0] for m in mags).items()))

    importing = args.cmd == "import"
    remote = importing or args.stage_only or args.launch
    smith = c.agent_for("metagem", remote, CACHE_DIR / "dryrun_home")
    ensure = importing or not remote
    inputs = declare_givens(smith, mags, ensure)
    globals_lib = declare_globals(smith, args.solver, CACHE_DIR / "e4_globals.xgdb", ensure)
    if importing:
        print(f"the pool at {smith.home.GetPath()} holds the givens of {len(mags)} MAGs")
        return 0

    task = smith.GenerateWorkflow(
        samples=list(inputs.AsSamples("sequences::bin_fasta")),
        resources=[DataInstanceLibrary.Load(c.MLIB / "resources" / "env"),
                   DataInstanceLibrary.Load(c.MLIB / "resources" / "lib"), globals_lib],
        transforms=[TransformInstanceLibrary.Load(c.MLIB / "transforms" / "logistics"),
                    TransformInstanceLibrary.Load(c.MLIB / "transforms" / "metabolicModelling")],
        targets=build_targets(args.solver),
    )
    expected = {"sequences::bin_fasta": len(mags), "modelling::media": 1, "modelling::medium_name": 1}
    if args.solver == "cplex":
        expected["modelling::cplex_installation"] = 1
    c.check_plan(task, expected)
    c.print_plan(task, 28)

    if args.dag:
        c.write_dag(task, "e4_metagem", CACHE_DIR)
    if remote:
        c.stage_and_run(smith, task, CACHE_DIR, args.tag or f"e4_{len(mags)}mags", stage_only=args.stage_only,
                        params=dict(executor=dict(queueSize=500), process=dict(tries=2, array=100)),
                        resource_overrides=RESOURCE_OVERRIDES)
    else:
        print("(dry run; nothing staged or submitted)")
    return 0


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name, fn in (("list", cmd_list), ("import", cmd_run), ("run", cmd_run)):
        p = sub.add_parser(name)
        p.add_argument("--study", nargs="*", help=", ".join(STUDY_ORDER))
        p.add_argument("--limit", type=int, help="first N MAGs per study")
        p.set_defaults(fn=fn, dag=False, stage_only=False, launch=False, tag=None)
        if name != "list":
            p.add_argument("--solver", default="cplex", choices=["cplex", "open"])
        if name == "run":
            p.add_argument("--dag", action="store_true", help="render the plan to page/dags/e4_metagem.dag.svg")
            mode = p.add_mutually_exclusive_group()
            mode.add_argument("--stage-only", action="store_true")
            mode.add_argument("--launch", action="store_true")
            p.add_argument("--tag")
    args = ap.parse_args()
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())

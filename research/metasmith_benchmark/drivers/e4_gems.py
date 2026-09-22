#!/usr/bin/env python3
"""E4: metaGEM's published protein bins to GEMs, with metaGEM's own carve and memote commands.

`run` solves against a local dry-run home and renders the DAG. `import`, `--materialise`,
`--stage-only` and `--launch` act on the fir agent home, from a Slurm job there, after
e4_extract_proteins.sh has unpacked the bins.

Subcommands: list, import, run [--study ...] [--limit N] [--chunk N --chunk-size S] [--dag]
[--materialise | --stage-only | --launch].
"""

import argparse
import csv
import os
import sys
from collections import Counter, defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
CACHE_DIR = Path(os.environ.get("E4_CACHE_DIR", HERE / ".cache" / "e4_gems"))

import _common as c  # noqa: E402
from metasmith.python_api import (  # noqa: E402
    DataInstanceLibrary, TransformInstanceLibrary, TargetBuilder,
)

PROTEIN_LIST = HERE / "e4_published_proteins.tsv"
PUBLISHED = Path(os.environ.get("METAGEM_PUBLISHED", "/scratch/phyberos/metagem/published"))
STUDY_ORDER = ["li2019", "korem2015", "karlsson2013", "bissett_base", "sunagawa2015"]
# metaGEM's workflow/scripts/media_db.tsv and its config's carveMedia.
MEDIA_DB = HERE / "refs" / "metagem_media_db.tsv"
MEDIUM_NAME = "M8"
CPLEX_ROOT = Path(os.environ.get("CPLEX_ROOT", "/home/phyberos/projects/rpp-shallam/phyberos/cplex/cplex_runtime"))
EXTRA_CONFIG = HERE / "e4_gems.config.nf"
STEPS = 3


def enumerate_bins():
    rows = csv.DictReader(PROTEIN_LIST.open(), delimiter="\t")
    return [(r["study"], r["bin"], PUBLISHED / r["study"] / "proteins" / f"{r['bin']}.faa") for r in rows]


def select(bins, args):
    if args.study:
        unknown = set(args.study) - set(STUDY_ORDER)
        if unknown:
            sys.exit(f"unknown study: {sorted(unknown)}")
        bins = [b for b in bins if b[0] in args.study]
    if args.limit:
        per_study = defaultdict(list)
        for b in bins:
            per_study[b[0]].append(b)
        bins = [b for s in STUDY_ORDER for b in per_study[s][: args.limit]]
    if args.bins:
        wanted = set(args.bins)
        bins = [b for b in bins if b[1] in wanted]
        missing = wanted - {b[1] for b in bins}
        if missing:
            sys.exit(f"unknown bins: {sorted(missing)}")
    if args.chunk:
        bins = sorted(bins)[(args.chunk - 1) * args.chunk_size: args.chunk * args.chunk_size]
    return bins


def declare_globals(smith, ensure):
    givens = smith.PoolGivens()
    c.add_value(givens, "ref/bench::metagem_media_db", MEDIA_DB.read_text(), "bench::metagem_media_db",
                tags=["reference"])
    c.add_value(givens, "ref/modelling::medium_name", MEDIUM_NAME, "modelling::medium_name", tags=["reference"])
    c.declare_refs(givens, {"modelling::cplex_installation": CPLEX_ROOT})
    types = [c.MLIB / "data_types" / t for t in ("modelling.yml", "ref.yml")] + [c.LIBRARY / "data_types" / "bench.yml"]
    return c.cite(givens, CACHE_DIR / "e4_gems_globals.xgdb", types, ensure)


def declare_givens(smith, bins, ensure, name):
    givens = smith.PoolGivens()
    for study, bin_id, path in bins:
        c.add_file(givens, f"e4g/{study}/{bin_id}", path, "sequences::bin_orfs", tags=["e4", study])
    return c.cite(givens, CACHE_DIR / f"e4_gems_inputs_{name}.xgdb", [c.MLIB / "data_types" / "sequences.yml"], ensure)


def build_targets():
    t = TargetBuilder()
    hits = t.Add("bench::bigg_diamond_hits")
    gem = t.Add("modelling::carveme_model_cplex", parents=[hits])
    t.Add("modelling::memote_report", parents=[gem])
    t.Add("modelling::memote_results", parents=[gem])
    return t


def cmd_list(args):
    bins = select(enumerate_bins(), args)
    for study, n in sorted(Counter(b[0] for b in bins).items(), key=lambda kv: STUDY_ORDER.index(kv[0])):
        print(f"{study:14s} {n:5d} bins")
    print(f"{len(bins)} bins")
    return 0


def cmd_run(args):
    bins = select(enumerate_bins(), args)
    if not bins:
        sys.exit("no bins selected")
    print(f"{len(bins)} bins: " + ", ".join(f"{s}={n}" for s, n in Counter(b[0] for b in bins).items()))

    importing = args.cmd == "import"
    remote = importing or args.stage_only or args.launch or args.materialise
    smith = c.agent_for("metagem", remote, CACHE_DIR / "dryrun_home")
    ensure = importing or args.import_givens or not remote
    name = "_".join([f"chunk{args.chunk}of{args.chunk_size}" if args.chunk else "all", *(args.study or []),
                     *([f"limit{args.limit}"] if args.limit else []), *(["picked"] if args.bins else [])])
    inputs = declare_givens(smith, bins, ensure, name)
    globals_lib = declare_globals(smith, ensure)
    if importing:
        print(f"the pool at {smith.home.GetPath()} holds the givens of {len(bins)} bins")
        return 0

    task = smith.GenerateWorkflow(
        samples=list(inputs.AsSamples("sequences::bin_orfs")),
        resources=[DataInstanceLibrary.Load(c.LIBRARY / "resources" / "e4"), globals_lib],
        transforms=[TransformInstanceLibrary.Load(c.MLIB / "transforms" / "logistics"),
                    TransformInstanceLibrary.Load(c.LIBRARY / "transforms" / "e4")],
        targets=build_targets(),
    )
    c.check_plan(task, {"sequences::bin_orfs": len(bins), "bench::metagem_media_db": 1,
                        "modelling::medium_name": 1, "modelling::cplex_installation": 1})
    c.print_plan(task, 24)
    if len(task.plan.steps) != STEPS:
        sys.exit(f"expected {STEPS} steps, the plan has {len(task.plan.steps)}")

    if args.dag:
        c.write_dag(task, "e4_gems", CACHE_DIR)
    if remote:
        c.stage_and_run(smith, task, CACHE_DIR, args.tag or f"e4g_{name}", stage_only=args.stage_only,
                        params=dict(process=dict(tries=2)), materialise=args.materialise,
                        extra_config=EXTRA_CONFIG)
    else:
        print("(dry run; nothing staged or submitted)")
    return 0


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name, fn in (("list", cmd_list), ("import", cmd_run), ("run", cmd_run)):
        p = sub.add_parser(name)
        p.add_argument("--study", nargs="*", help=", ".join(STUDY_ORDER))
        p.add_argument("--limit", type=int, help="first N bins per study")
        p.add_argument("--bins", nargs="*", help="exact metaGEM bin ids, e.g. SRR7664617_bin.15.p")
        p.add_argument("--chunk", type=int, help="1-based chunk of the sorted selection")
        p.add_argument("--chunk-size", type=int, default=2000)
        p.set_defaults(fn=fn, dag=False, stage_only=False, launch=False, materialise=False, tag=None,
                       import_givens=False)
        if name == "run":
            p.add_argument("--dag", action="store_true", help="render the plan to page/dags/e4_gems.dag.svg")
            mode = p.add_mutually_exclusive_group()
            mode.add_argument("--stage-only", action="store_true")
            mode.add_argument("--launch", action="store_true")
            mode.add_argument("--materialise", action="store_true", help="stage, fetch every image the plan needs, stop")
            p.add_argument("--tag")
            p.add_argument("--import", dest="import_givens", action="store_true",
                           help="import what the pool lacks before planning, as `import` does")
    args = ap.parse_args()
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())

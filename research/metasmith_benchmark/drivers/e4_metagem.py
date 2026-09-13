#!/usr/bin/env python3
"""E4: metaGEM's published MAGs through Prodigal, CarveMe (CPLEX) and MEMOTE.

Solves locally by default and renders the DAG. --stage-only and --launch reach fir and
need Tony's sign-off, and both need e4_extract_mags.sh to have unpacked the MAGs first.

Subcommands: list, run [--study ...] [--limit N] [--stage-only | --launch].
"""

import argparse
import csv
import hashlib
import json
import os
import sys
from collections import Counter, defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]
CACHE_DIR = Path(os.environ.get("E4_CACHE_DIR", HERE / ".cache" / "e4"))
os.environ.setdefault("MSM_CACHE_DIR", str(CACHE_DIR))
sys.path.insert(0, str(REPO / "research" / "metagem"))

import run_metagem as rm  # noqa: E402
from metasmith.python_api import (  # noqa: E402
    Agent, Source, Runtime, DataInstanceLibrary, TransformInstanceLibrary,
    TargetBuilder, Resources, Size, Duration,
)

MAG_LIST = HERE / "e4_published_mags.tsv"
PUBLISHED = Path(os.environ.get("METAGEM_PUBLISHED", "/scratch/phyberos/metagem/published"))
STUDY_ORDER = ["li2019", "korem2015", "karlsson2013", "bissett_base", "sunagawa2015"]
DAG_DIR = HERE.parent / "page" / "dags"

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


def build_inputs(mags):
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    inputs = DataInstanceLibrary(CACHE_DIR / "e4_inputs.xgdb")
    inputs.Purge()
    inputs.AddTypeLibrary(rm.MLIB / "data_types" / "sequences.yml")
    for study, mag, path in mags:
        inputs.RegisterItem(
            path, "sequences::bin_fasta",
            instance_id=rm._stable_id("metagem_published", "bin_fasta", study, mag),
        )
    inputs.Save()
    return inputs


def build_globals(solver):
    # Keyed on content, not on the medium table's checkout path, so plan keys match across checkouts.
    globals_lib = DataInstanceLibrary(CACHE_DIR / "e4_globals.xgdb")
    globals_lib.Purge()
    globals_lib.AddTypeLibrary(rm.MLIB / "data_types" / "modelling.yml")
    medium_table = rm.MEDIUM_TSV.read_text()
    (globals_lib.location / "media.tsv").write_text(medium_table)
    globals_lib.RegisterItem(
        "media.tsv", "modelling::media",
        instance_id=rm._stable_id("metagem_published", "media",
                                  hashlib.sha256(medium_table.encode()).hexdigest()),
    )
    (globals_lib.location / "medium_name.txt").write_text(rm.MEDIUM_NAME)
    globals_lib.RegisterItem(
        "medium_name.txt", "modelling::medium_name",
        instance_id=rm._stable_id("metagem_published", "medium_name", rm.MEDIUM_NAME),
    )
    if solver == "cplex":
        globals_lib.RegisterItem(
            rm.CPLEX_ROOT, "modelling::cplex_installation",
            instance_id=rm._stable_id("metagem_published", "cplex_installation", str(rm.CPLEX_ROOT)),
        )
    globals_lib.Save()
    return globals_lib


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

    inputs = build_inputs(mags)
    globals_lib = build_globals(args.solver)
    containers = DataInstanceLibrary.Load(rm.MLIB / "resources" / "env")
    helpers = DataInstanceLibrary.Load(rm.MLIB / "resources" / "lib")
    transforms = [
        TransformInstanceLibrary.Load(rm.MLIB / "transforms" / "logistics"),
        TransformInstanceLibrary.Load(rm.MLIB / "transforms" / "metabolicModelling"),
    ]

    remote = args.stage_only or args.launch
    smith = rm.get_agent() if remote else Agent(
        home=Source.FromLocal(CACHE_DIR / "dryrun_home"), runtime=Runtime.APPTAINER)
    samples = list(inputs.AsSamples("sequences::bin_fasta"))
    task = smith.GenerateWorkflow(
        samples=samples,
        resources=[containers, helpers, globals_lib],
        transforms=transforms,
        targets=build_targets(args.solver),
    )
    if not task.ok:
        print(f"dropped: {sorted(task.plan.dropped_targets)}", file=sys.stderr)
        rm._report_plan_failure(task)

    expected = Counter({"sequences::bin_fasta": len(mags), "modelling::media": 1, "modelling::medium_name": 1})
    if args.solver == "cplex":
        expected["modelling::cplex_installation"] = 1
    rm._check_given_leaf_counts(task, expected, "E4")

    print(f"Plan OK -- {len(task.plan.steps)} steps, key={task.GetKey()}")
    for s in task.plan.steps:
        prods = sorted({i.dtype_name for g in s.produces for i in g})
        print(f"  {s.order:>2}. {Path(s.transform._path).stem:28s} -> {prods}")

    if args.dag:
        DAG_DIR.mkdir(parents=True, exist_ok=True)
        rendered = Path(str(task.plan.RenderDAG(str(CACHE_DIR / "e4_metagem"), format="svg")))
        dag = DAG_DIR / "e4_metagem.dag.svg"
        dag.write_bytes(rendered.read_bytes())
        print(f"dag: {dag}")

    if not remote:
        print("(dry run; nothing staged or submitted)")
        return 0

    keys_file = CACHE_DIR / "task_keys.json"
    keys = json.loads(keys_file.read_text()) if keys_file.exists() else {}
    keys[args.tag or f"e4_{len(mags)}mags"] = task.GetKey()
    keys_file.write_text(json.dumps(keys, indent=2))

    smith.StageWorkflow(task, on_exist="update", verify_external_paths=False)
    if args.stage_only:
        print(f"staged as {task.GetKey()}")
        return 0
    smith.RunWorkflow(
        task=task, config_file=rm.make_slurm_config(),
        params=dict(slurmAccount=rm.SLURM_ACCOUNT, executor=dict(queueSize=500),
                    process=dict(tries=2, array=100)),
        resource_overrides=RESOURCE_OVERRIDES,
    )
    print(f"submitted {task.GetKey()}")
    return 0


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name, fn in (("list", cmd_list), ("run", cmd_run)):
        p = sub.add_parser(name)
        p.add_argument("--study", nargs="*", help=", ".join(STUDY_ORDER))
        p.add_argument("--limit", type=int, help="first N MAGs per study")
        p.set_defaults(fn=fn)
        if name == "run":
            p.add_argument("--solver", default="cplex", choices=["cplex", "open"])
            p.add_argument("--dag", action="store_true", help="render the plan to page/dags/e4_metagem.dag.svg")
            mode = p.add_mutually_exclusive_group()
            mode.add_argument("--stage-only", action="store_true")
            mode.add_argument("--launch", action="store_true")
            p.add_argument("--tag")
    args = ap.parse_args()
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())

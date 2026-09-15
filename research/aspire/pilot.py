#!/usr/bin/env python3
"""Plan ASPIRE over six samples of its own mock dataset, and draw the DAG.

The mock sits on fir, at `MOCK`. Every given is declared by its fir path into a
local dry-run agent's pool, so the plan keys on pool identities the way a real
campaign's would. Nothing is staged or launched: the transforms are still stubs,
and this driver exists to show the topology over real inputs.

    python research/aspire/pilot.py list
    python research/aspire/pilot.py run --case all --dag

The mock ships no SINA ARB reference, SILVA taxonomy or SILVA database. Those
three stay DEFERRED placeholders in their own resource library, since a pool
entry has to name a path that exists.
"""

import argparse
import hashlib
import json
import os
import shutil
import sys
from collections import Counter
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from aspire_asv_pipeline import (  # noqa: E402
    CASES, DEFAULT_ON, EXTERNAL_GRAPH, MLIB, REPO, SWITCHES, TRANSFORMS, targets_for,
    transform_names,
)
from metasmith.python_api import (  # noqa: E402
    DEFERRED, Agent, DataInstanceLibrary, Runtime, Source, TransformInstanceLibrary,
    record_library,
)

MOCK = Path(os.environ.get("ASPIRE_MOCK", "/scratch/phyberos/aspire_mock/mock_dataset"))
CACHE_DIR = Path(os.environ.get("ASPIRE_PILOT_CACHE", REPO / "cache" / "aspire" / "pilot"))
DAG_DIR = HERE / "reports" / "dag"
STUDY = "aspire_mock_pilot"
TYPE_LIBS = [MLIB / "data_types" / t for t in ("aspire.yml", "amplicon.yml", "sequences.yml")]

# Three controls and three cases, so `Case` has both levels. Pair counts were
# checked against the mock's fastq_validation.tsv.
SAMPLES = ["CTRL_001_BAL", "CTRL_002_BRUSH", "CTRL_003_BAL",
           "CASE_001_BRUSH", "CASE_002_BAL", "CASE_003_BAL_CONTRA"]

MOCK_REFERENCES = {
    "aspire::sample_metadata": MOCK / "sample_metadata.tsv",
    "aspire::mito_reference_source": MOCK / "references" / "mitochondria.fasta",
    "aspire::contaminant_reference_source": MOCK / "references" / "contaminants.fasta",
}
ABSENT_REFERENCES = ["aspire::sina_arb_reference", "aspire::silva_ref_taxonomy", "amplicon::silva_db"]


def given_name(base, declared):
    """A pool name that moves when what it declares moves, so a moved file never cites a stale entry."""
    return f"{base}@{hashlib.sha256(str(declared).encode()).hexdigest()[:12]}"


def add_file(givens, base, path, dtype, **kw):
    return givens.Add(path, dtype, name=given_name(base, path), **kw)


def add_value(givens, base, content, dtype, **kw):
    text = content if isinstance(content, str) else json.dumps(content, sort_keys=True)
    return givens.Value(given_name(base, text), text, dtype, **kw)


def cite(givens, location):
    Path(location).parent.mkdir(parents=True, exist_ok=True)
    return givens.Build(location, type_library_paths=TYPE_LIBS, ensure=True)


def declare_study(smith, on):
    """The study view: run, its samples and reads, and the policy tokens, all hanging off run."""
    ns = f"aspire/{STUDY}"
    givens = smith.PoolGivens()
    run = add_value(givens, f"{ns}/run", STUDY, "aspire::run", tags=["aspire", STUDY])
    for sid in SAMPLES:
        tags = ["aspire", STUDY, sid]
        sample = add_value(givens, f"{ns}/{sid}/sample_id", sid, "aspire::sample_id", parents=[run], tags=tags)
        pair = add_value(givens, f"{ns}/{sid}/read_pair", sid, "sequences::read_pair", parents=[sample], tags=tags)
        for mate, dtype in (("R1", "zipped_forward_short_reads"), ("R2", "zipped_reverse_short_reads")):
            add_file(givens, f"{ns}/{sid}/{dtype}", MOCK / "fastq" / f"{sid}_{mate}.fastq.gz",
                     f"sequences::{dtype}", parents=[pair], tags=tags)
    for base, enabled in on.items():
        arm = "on" if enabled else "off"
        add_value(givens, f"{ns}/policy/{base}", arm, f"aspire::{base}_{arm}", parents=[run], tags=["aspire", STUDY])
    return cite(givens, CACHE_DIR / "study.xgdb")


def declare_references(smith):
    givens = smith.PoolGivens()
    for dtype, path in MOCK_REFERENCES.items():
        add_file(givens, f"aspire/{STUDY}/ref/{dtype}", path, dtype, tags=["aspire", "reference"])
    return cite(givens, CACHE_DIR / "references.xgdb")


def declare_placeholders(on):
    """What the mock does not ship, as DEFERRED slots a plan may name but a stage refuses."""
    location = CACHE_DIR / "placeholders.xgdb"
    if location.exists(): shutil.rmtree(location)
    lib = DataInstanceLibrary(location)
    lib.Purge()
    for t in TYPE_LIBS:
        lib.AddTypeLibrary(t)
    dtypes = list(ABSENT_REFERENCES)
    if not on["spieceasi"]:
        dtypes += EXTERNAL_GRAPH
    for dtype in dtypes:
        lib.AddItem(DEFERRED, dtype)
    return record_library(lib)


def expected_givens():
    """The study givens every case consumes. `plan.given` holds only what a step reads, so a
    policy token outside the case's slice is absent by design and is not counted."""
    n = len(SAMPLES)
    return {"aspire::run": 1, "sequences::read_pair": n,
            "sequences::zipped_forward_short_reads": n, "sequences::zipped_reverse_short_reads": n}


def check_plan(task, expected):
    """Exit unless the plan solved and carries every study given at its declared count."""
    if not task.ok:
        print(f"dropped: {sorted(task.plan.dropped_targets)}", file=sys.stderr)
        for h in getattr(task.plan, "hints", []) or []:
            print(f"  [{h.kind}] target={getattr(h, 'target', '?')}: {getattr(h, 'message', '')}", file=sys.stderr)
            for line in (getattr(h, "chain", []) or []) + (getattr(h, "near_misses", []) or []):
                print(f"      {line}", file=sys.stderr)
        sys.exit("ERROR: the pilot did not solve")
    got = Counter(i.dtype_name for i in task.plan.given)
    problems = [f"  {dtype}: declared {want}, plan.given has {got.get(dtype, 0)}"
                for dtype, want in expected.items() if got.get(dtype, 0) != want]
    if problems:
        sys.exit("ERROR: given count mismatch:\n" + "\n".join(problems))


def cmd_list(_args):
    print(f"{STUDY}: {len(SAMPLES)} samples from {MOCK}")
    for sid in SAMPLES:
        print(f"  {sid}")
    for dtype, path in MOCK_REFERENCES.items():
        print(f"  {dtype:<40}{path}")
    for dtype in ABSENT_REFERENCES:
        print(f"  {dtype:<40}DEFERRED (not in the mock)")
    return 0


def cmd_run(args):
    on = dict(DEFAULT_ON)
    for name in args.on + args.off:
        if name not in on:
            sys.exit(f"unknown switch [{name}]; choices: {', '.join(SWITCHES)}")
    on.update({n: True for n in args.on})
    on.update({n: False for n in args.off})

    if args.case == "all":
        # The shared script's leaves skip a row once any of its products is consumed,
        # which leaves the analysis and network branches untargeted. Ask for every case too.
        named = [t for case, ts in CASES.items() if case != "all" for t in ts]
        targets = list(dict.fromkeys(named + targets_for("all", on)))
    else:
        targets = targets_for(args.case, on)
    print(f"[{args.case}] {len(targets)} target(s), {len(SAMPLES)} samples")
    smith = Agent(home=Source.FromLocal(CACHE_DIR / "dryrun_home"), runtime=Runtime.APPTAINER)
    study = declare_study(smith, on)
    task = smith.GenerateWorkflow(
        samples=list(study.AsSamples("aspire::run")),
        resources=[DataInstanceLibrary.Load(MLIB / "resources" / "env"),
                   declare_references(smith), declare_placeholders(on)],
        transforms=[TransformInstanceLibrary.Load(t) for t in TRANSFORMS],
        targets=targets,
        max_iter=args.max_iter, max_refine=args.max_refine, seed=args.seed,
    )
    check_plan(task, expected_givens())

    names = transform_names()
    print(f"Plan OK: {len(task.plan.steps)} steps, key={task.GetKey()}")
    for i, step in enumerate(task.plan.steps, 1):
        print(f"  {i:2d}. {names.get(step.transform.model.key, '?')}")

    if args.dag:
        DAG_DIR.mkdir(parents=True, exist_ok=True)
        # RenderDAG reads a dotted basename's suffix as the format, so the stem carries no dot.
        stem = "pilot" if args.case == "all" else f"pilot_{args.case}"
        print(f"dag: {task.plan.RenderDAG(str(DAG_DIR / stem), format='svg')}")
    return 0


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("list", help="print the pilot samples and references")
    run = sub.add_parser("run", help="solve the pilot against a local dry-run home")
    run.add_argument("--case", default="all", choices=sorted(CASES))
    run.add_argument("--on", action="append", default=[], metavar="SWITCH")
    run.add_argument("--off", action="append", default=[], metavar="SWITCH")
    run.add_argument("--dag", action="store_true", help=f"render the plan under {DAG_DIR}")
    run.add_argument("--seed", type=int, default=42)
    run.add_argument("--max-iter", type=int, default=1024)
    run.add_argument("--max-refine", type=int, default=256)
    args = ap.parse_args()
    return {"list": cmd_list, "run": cmd_run}[args.cmd](args)


if __name__ == "__main__":
    raise SystemExit(main())

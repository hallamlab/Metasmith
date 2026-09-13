#!/usr/bin/env python3
"""E2: nf-core/mag's E1 pipeline on the same 249 CAMI samples, driven by metasmith.

Binds only `library/transforms/e2`, whose transforms run nf-core/mag 5.5.0's own commands,
arguments and images. Two arms, solved separately because one solve cannot mix read shapes:
  short  208 samples: FastQC, fastp, MEGAHIT, bowtie2, MetaBAT2/SemiBin2/COMEBin, DAS Tool, CheckM2, AMBER
  long   41 Nanopore samples: Porechop ABI, Chopper, Flye, minimap2, the same binning tail

Solves locally by default and renders one DAG per arm. --stage-only and --launch reach fir
and need Tony's sign-off.

Subcommands: list, run [--arm short|long|both] [--limit N] [--stage-only | --launch].
"""

import argparse
import csv
import json
import os
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]
LIBRARY = HERE.parent / "library"
CACHE_DIR = Path(os.environ.get("E2_CACHE_DIR", HERE / ".cache" / "e2"))
os.environ.setdefault("MSM_CACHE_DIR", str(CACHE_DIR))
sys.path.insert(0, str(REPO / "research" / "cami"))

import run_cami_metag as rcm  # noqa: E402
from metasmith.python_api import (  # noqa: E402
    Agent, Source, Runtime, DataInstanceLibrary, TransformInstanceLibrary, TargetBuilder,
)

# E1's read choice: long reads where CAMI simulated Nanopore, short reads everywhere else.
LONG_DATASETS = {"plant_associated_long_nano": "plant_associated", "toy_humangut_long": "toy_humangut"}
# Must equal the lr_platform E1's sample sheet declares. plant_associated NanoSim aligns at ~15%
# error, so OXFORD_NANOPORE. toy_humangut waits on the research agent's Flye error rates.
PLATFORM = {"plant_associated_long_nano": "OXFORD_NANOPORE", "toy_humangut_long": "OXFORD_NANOPORE"}
EXPECTED = {"short": 208, "long": 41}
DAG_DIR = HERE.parent / "page" / "dags"
CHECKM2_DB = Path(os.environ.get(
    "CHECKM2_DB", "/scratch/phyberos/wave2_b3_nfcore/checkm2_db/CheckM2_database/uniref100.KO.1.dmnd"))


def enumerate_arms():
    rows = list(csv.DictReader(rcm.CAMI_SAMPLES_TSV.open(), delimiter="\t"))
    long_replaced = set(LONG_DATASETS.values())
    short = [(f"{r['dataset']}_{r['sample_id']}", Path(r["reads_path"]))
             for r in rows if r["read_type"] == "short" and r["dataset"] not in long_replaced]
    long_ = [s for s in rcm.enumerate_long_read_samples() if s[3] in LONG_DATASETS]
    return {"short": short, "long": long_}


def build_inputs(arm, samples):
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    inputs = DataInstanceLibrary(CACHE_DIR / f"e2_{arm}_inputs.xgdb")
    inputs.Purge()
    inputs.AddTypeLibrary(LIBRARY / "data_types" / "e2.yml")

    def register(sid, reads, truth, length_class, platform):
        value = json.dumps({"sample": sid, "length_class": length_class, "platform": platform})
        name = f"{sid}_read_metadata.json"
        (inputs.location / name).write_text(value)
        meta = inputs.RegisterItem(name, "e2::read_metadata",
                                   instance_id=rcm._stable_id("e2", "read_metadata", sid, value))
        inputs.RegisterItem(reads, f"e2::{length_class}_reads", parents={meta},
                            instance_id=rcm._stable_id("e2", f"{length_class}_reads", sid, str(reads)))
        if truth is not None:
            inputs.RegisterItem(truth, "e2::read_truth", parents={meta},
                                instance_id=rcm._stable_id("e2", "read_truth", sid, str(truth)))

    if arm == "short":
        for sid, reads in samples:
            register(sid, reads, reads.parent / "reads_mapping.tsv.gz", "short", "ILLUMINA")
    else:
        for sid, reads, truth, dataset in samples:
            register(sid, reads, truth, "long", PLATFORM[dataset])
    inputs.Save()
    return inputs


def build_globals():
    lib = DataInstanceLibrary(CACHE_DIR / "e2_globals.xgdb")
    lib.Purge()
    lib.AddTypeLibrary(LIBRARY / "data_types" / "e2.yml")
    lib.RegisterItem(CHECKM2_DB, "e2::checkm2_database",
                     instance_id=rcm._stable_id("e2", "checkm2_database", str(CHECKM2_DB)))
    lib.Save()
    return lib


def build_targets(arm):
    t = TargetBuilder()
    if arm == "short":
        asm = t.Add("e2::megahit_assembly")
        for report in ("fastqc_raw_reports", "fastqc_trimmed_reports", "fastp_json", "fastp_html"):
            t.Add(f"e2::{report}")
    else:
        asm = t.Add("e2::flye_assembly")
    # nf-core/mag's control config sends raw and refined bins downstream, so CheckM2 scores all four sets.
    for binner in ("metabat2", "semibin2", "comebin", "das_tool"):
        bins = t.Add(f"e2::{binner}_bin", parents=[asm])
        t.Add(f"e2::{binner}_contig_to_bin", parents=[asm])
        t.Add("e2::checkm2_quality", parents=[bins])
    t.Add("e2::das_tool_summary", parents=[asm])
    t.Add("e2::amber_results")
    t.Add("e2::amber_bin_metrics")
    return t


def solve(arm, samples, args):
    inputs = build_inputs(arm, samples)
    remote = args.stage_only or args.launch
    smith = rcm.get_agent("cami" if arm == "short" else "cami_long") if remote else Agent(
        home=Source.FromLocal(CACHE_DIR / f"dryrun_home_{arm}"), runtime=Runtime.APPTAINER)

    print(f"\n=== {arm} arm: {len(samples)} samples ===")
    task = smith.GenerateWorkflow(
        samples=list(inputs.AsSamples("e2::read_metadata")),
        resources=[DataInstanceLibrary.Load(LIBRARY / "resources" / "e2"),
                   DataInstanceLibrary.Load(rcm.MLIB / "resources" / "lib"), build_globals()],
        transforms=[TransformInstanceLibrary.Load(LIBRARY / "transforms" / "e2")],
        targets=build_targets(arm),
    )
    if not task.ok:
        print(f"dropped: {sorted(task.plan.dropped_targets)}", file=sys.stderr)
        rcm._report_plan_failure(task)
    n_truth = len(samples) if arm == "short" else sum(1 for s in samples if s[2] is not None)
    rcm._assert_given_counts(task, {f"e2::{arm}_reads": len(samples), "e2::read_metadata": len(samples),
                                    "e2::read_truth": n_truth})

    print(f"Plan OK -- {len(task.plan.steps)} steps, key={task.GetKey()}")
    for s in task.plan.steps:
        prods = sorted({i.dtype_name for g in s.produces for i in g})
        print(f"  {s.order:>2}. {Path(s.transform._path).stem:22s} -> {prods}")

    if args.dag:
        DAG_DIR.mkdir(parents=True, exist_ok=True)
        rendered = Path(str(task.plan.RenderDAG(str(CACHE_DIR / f"e2_cami_{arm}"), format="svg")))
        dag = DAG_DIR / f"e2_cami_{arm}.dag.svg"
        dag.write_bytes(rendered.read_bytes())
        print(f"dag: {dag}")

    if not remote:
        return
    keys_file = CACHE_DIR / "task_keys.json"
    keys = json.loads(keys_file.read_text()) if keys_file.exists() else {}
    keys[f"{args.tag or 'e2'}_{arm}"] = task.GetKey()
    keys_file.write_text(json.dumps(keys, indent=2))
    smith.StageWorkflow(task, on_exist="update", verify_external_paths=False)
    if args.stage_only:
        print(f"staged {arm} arm as {task.GetKey()}")
        return
    smith.RunWorkflow(
        task=task, config_file=rcm.make_slurm_config(),
        params=dict(slurmAccount=rcm.SLURM_ACCOUNT, executor=dict(queueSize=500), process=dict(tries=4, array=25)),
    )
    print(f"submitted {arm} arm: {task.GetKey()}")


def cmd_list(args):
    for arm, samples in enumerate_arms().items():
        print(f"{arm}: {len(samples)} samples (E1 uses {EXPECTED[arm]})")
    return 0


def cmd_run(args):
    arms = enumerate_arms()
    for arm in (["short", "long"] if args.arm == "both" else [args.arm]):
        samples = arms[arm]
        assert len(samples) == EXPECTED[arm], f"{arm} arm has {len(samples)} samples, E1 has {EXPECTED[arm]}"
        solve(arm, samples[: args.limit] if args.limit else samples, args)
    return 0


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("list").set_defaults(fn=cmd_list)
    p = sub.add_parser("run")
    p.add_argument("--arm", default="both", choices=["short", "long", "both"])
    p.add_argument("--limit", type=int, help="first N samples per arm")
    p.add_argument("--dag", action="store_true", help="render each arm to page/dags/e2_cami_<arm>.dag.svg")
    mode = p.add_mutually_exclusive_group()
    mode.add_argument("--stage-only", action="store_true")
    mode.add_argument("--launch", action="store_true")
    p.add_argument("--tag")
    p.set_defaults(fn=cmd_run)
    args = ap.parse_args()
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())

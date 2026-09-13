#!/usr/bin/env python3
"""E2: nf-core/mag's E1 pipeline on the same 249 CAMI samples, driven by metasmith.

Two arms, solved separately because one solve cannot mix read shapes:
  short  208 samples: fastp, FastQC, MEGAHIT, MetaBAT2/SemiBin2/COMEBin, DAS Tool, CheckM2, AMBER
  long   41 Nanopore samples: Flye, the same binners, DAS Tool, CheckM2, AMBER

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
CACHE_DIR = Path(os.environ.get("E2_CACHE_DIR", HERE / ".cache" / "e2"))
os.environ.setdefault("MSM_CACHE_DIR", str(CACHE_DIR))
sys.path.insert(0, str(REPO / "research" / "cami"))

import run_cami_metag as rcm  # noqa: E402
from metasmith.python_api import (  # noqa: E402
    Agent, Source, Runtime, DataInstanceLibrary, TransformInstanceLibrary, Resources, Size, Duration,
)

# E1's read choice: long reads where CAMI simulated Nanopore, short reads everywhere else.
LONG_DATASETS = {"plant_associated_long_nano": "plant_associated", "toy_humangut_long": "toy_humangut"}
EXPECTED = {"short": 208, "long": 41}
DAG_DIR = HERE.parent / "page" / "dags"

# Not in either plan yet, each needed to match E1 tool for tool.
PARITY_GAPS = [
    "bowtie2 BAM for binning coverage (new transform; bowtie2_align emits an RNA-seq type)",
    "Porechop ABI (new transform)",
    "Chopper (new transform)",
    "flye_raw preset from the declared platform, not read quality (B12)",
    "phiX removal (to decide: keep for parity, or drop)",
]

BINNER_PARAMS = dict(metabat2_min_contig=1500, metabat2_seed=1, semibin2_min_len=1500, semibin2_seed=1)
RESOURCE_OVERRIDES = {
    "short": {
        "fastp": Resources(memory=Size.GB(32), cpus=16),
        "megahit": Resources(memory=Size.GB(128), cpus=32, duration=Duration(hours=12)),
    },
    # B12: 32.68 GiB peak on plant nano sample 0, over flye_raw's declared 32 GB.
    "long": {"flye_raw": Resources(memory=Size.GB(128), cpus=32, duration=Duration(hours=24))},
}


def enumerate_arms():
    rows = list(csv.DictReader(rcm.CAMI_SAMPLES_TSV.open(), delimiter="\t"))
    long_replaced = set(LONG_DATASETS.values())
    short = [(f"{r['dataset']}_{r['sample_id']}", Path(r["reads_path"]))
             for r in rows if r["read_type"] == "short" and r["dataset"] not in long_replaced]
    long_ = [s for s in rcm.enumerate_long_read_samples() if s[3] in LONG_DATASETS]
    return {"short": short, "long": long_}


def short_arm(samples):
    inputs = rcm.build_inputs(samples)
    kbase = TransformInstanceLibrary.Load(rcm.MLIB / "transforms" / "kbase")
    transforms = [
        TransformInstanceLibrary.Load(rcm.MLIB / "transforms" / "logistics"),
        rcm._assembly_without("spades", "bbduk", "seqkit_reads"),
        TransformInstanceLibrary.Load(rcm.MLIB / "transforms" / "metagenomics"),
        kbase.AsView({Path("clean_reads/fastp.py"), Path("qc_reads/fastqc.py")}),
    ]
    targets = rcm.build_targets(with_dedup=False, variant="variant")
    targets.Add("sequences::fastqc_html_report")
    expected = {"sequences::short_reads_pe": len(samples), "sequences::read_metadata": len(samples),
                "binning::cami_read_truth": len(samples)}
    return inputs, rcm.build_globals(), transforms, targets, expected


def long_arm(samples):
    inputs = rcm.build_inputs_long_read(samples)
    n_truth = sum(1 for s in samples if s[2] is not None)
    expected = {"sequences::long_reads": len(samples), "sequences::read_metadata": len(samples),
                "binning::cami_read_truth": n_truth}
    return (inputs, rcm.build_globals_long_read(), rcm.build_transforms_for_long_read(),
            rcm.build_targets_long_read(), expected)


def solve(arm, samples, args):
    inputs, globals_lib, transforms, targets, expected = (short_arm if arm == "short" else long_arm)(samples)
    remote = args.stage_only or args.launch
    smith = rcm.get_agent("cami" if arm == "short" else "cami_long") if remote else Agent(
        home=Source.FromLocal(CACHE_DIR / f"dryrun_home_{arm}"), runtime=Runtime.APPTAINER)

    print(f"\n=== {arm} arm: {len(samples)} samples ===")
    task = smith.GenerateWorkflow(
        samples=list(inputs.AsSamples("sequences::read_metadata")),
        resources=[DataInstanceLibrary.Load(rcm.MLIB / "resources" / "env"),
                   DataInstanceLibrary.Load(rcm.MLIB / "resources" / "lib"), globals_lib],
        transforms=transforms,
        targets=targets,
    )
    if not task.ok:
        print(f"dropped: {sorted(task.plan.dropped_targets)}", file=sys.stderr)
        rcm._report_plan_failure(task)
    rcm._assert_given_counts(task, expected)

    print(f"Plan OK -- {len(task.plan.steps)} steps, key={task.GetKey()}")
    for s in task.plan.steps:
        prods = sorted({i.dtype_name for g in s.produces for i in g})
        print(f"  {s.order:>2}. {Path(s.transform._path).stem:28s} -> {prods}")

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
        params=dict(slurmAccount=rcm.SLURM_ACCOUNT, executor=dict(queueSize=500),
                    process=dict(tries=4, array=25), **BINNER_PARAMS),
        resource_overrides=RESOURCE_OVERRIDES[arm],
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
    print("\nStill needed for parity with E1:")
    for gap in PARITY_GAPS:
        print(f"  - {gap}")
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

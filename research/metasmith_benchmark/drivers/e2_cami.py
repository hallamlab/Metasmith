#!/usr/bin/env python3
"""E2: nf-core/mag's E1 pipeline on the same 249 CAMI samples, driven by metasmith.

Binds only `library/transforms/e2`, whose transforms run nf-core/mag 5.5.0's own commands,
arguments and images. Two arms, solved separately because one solve cannot mix read shapes:
  short  208 samples: FastQC, fastp, MEGAHIT, bowtie2, MetaBAT2/SemiBin2/COMEBin, DAS Tool, CheckM2, AMBER
  long   41 Nanopore samples: Porechop ABI, Chopper, Flye, minimap2, the same binning tail

`run` solves against a local dry-run home and renders one DAG per arm. `import`,
`--stage-only` and `--launch` act on the fir agent home, from a Slurm job there.

Subcommands: list, import [--arm], run [--arm short|long|both] [--limit N] [--dag] [--stage-only | --launch].
"""

import argparse
import os
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
CACHE_DIR = Path(os.environ.get("E2_CACHE_DIR", HERE / ".cache" / "e2"))

import _common as c  # noqa: E402
from metasmith.python_api import DataInstanceLibrary, TransformInstanceLibrary, TargetBuilder  # noqa: E402

# E1's read choice: long reads where CAMI simulated Nanopore, short reads everywhere else.
LONG_DATASETS = {"plant_associated_long_nano": "plant_associated", "toy_humangut_long": "toy_humangut"}
# Must equal the long_reads_platform E1's sample sheet declares. Both align at 12-15% final
# error, which rules out the HQ presets. toy_humangut's read-length shape leans PacBio CLR,
# so its OXFORD_NANOPORE is an inference, recorded in the deviations table.
PLATFORM = {"plant_associated_long_nano": "OXFORD_NANOPORE", "toy_humangut_long": "OXFORD_NANOPORE"}
EXPECTED = {"short": 208, "long": 41}
CHECKM2_DB = Path(os.environ.get(
    "CHECKM2_DB", "/scratch/phyberos/wave2_b3_nfcore/checkm2_db/CheckM2_database/uniref100.KO.1.dmnd"))
TYPE_LIBS = [c.LIBRARY / "data_types" / "e2.yml"]


def enumerate_arms():
    rows = c.cami_rows()
    long_replaced = set(LONG_DATASETS.values())
    short = [(f"{r['dataset']}_{r['sample_id']}", Path(r["reads_path"]), r["dataset"])
             for r in rows if r["read_type"] == "short" and r["dataset"] not in long_replaced]
    long_ = []
    for r in rows:
        if r["read_type"] == "long" and r["dataset"] in LONG_DATASETS:
            reads = Path(r["reads_path"])
            truth = reads.parent / "reads_mapping.tsv.gz" if int(r["has_truth"]) else None
            long_.append((f"{r['dataset']}_{r['sample_id']}", reads, truth, r["dataset"]))
    return {"short": short, "long": long_}


def declare_givens(smith, arm, samples, ensure):
    givens = smith.PoolGivens()
    for sample in samples:
        if arm == "short":
            sid, reads, dataset = sample
            truth, platform = reads.parent / "reads_mapping.tsv.gz", "ILLUMINA"
        else:
            sid, reads, truth, dataset = sample
            platform = PLATFORM[dataset]
        tags = ["e2", arm, sid]
        meta = c.add_value(givens, f"e2/{sid}/read_metadata",
                           {"sample": sid, "length_class": arm, "platform": platform},
                           "e2::read_metadata", tags=tags)
        if arm == "short":
            for mate, path in zip((1, 2), c.cami_split_pair(dataset, sid)):
                c.add_file(givens, f"e2/{sid}/reads_{mate}", path, f"e2::short_reads_{mate}", parents=[meta], tags=tags)
        else:
            c.add_file(givens, f"e2/{sid}/reads", reads, "e2::long_reads", parents=[meta], tags=tags)
        if truth is not None:
            c.add_file(givens, f"e2/{sid}/read_truth", truth, "e2::read_truth", parents=[meta], tags=tags)

    refs = smith.PoolGivens()
    c.declare_refs(refs, {"e2::checkm2_database": CHECKM2_DB}, ["e2"])
    return (c.cite(givens, CACHE_DIR / f"e2_{arm}_inputs.xgdb", TYPE_LIBS, ensure),
            c.cite(refs, CACHE_DIR / "e2_globals.xgdb", TYPE_LIBS, ensure))


def build_targets(arm):
    t = TargetBuilder()
    if arm == "short":
        asm = t.Add("e2::megahit_assembly")
        for report in ("trimmed_short_reads", "fastqc_raw_reports", "fastqc_trimmed_reports", "fastp_json", "fastp_html"):
            t.Add(f"e2::{report}")
    else:
        asm = t.Add("e2::flye_assembly")
        t.Add("e2::filtered_long_reads")
    t.Add("e2::binning_bam", parents=[asm])
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
    importing = args.cmd == "import"
    remote = importing or args.stage_only or args.launch or args.materialise
    smith = c.agent_for("cami", remote, CACHE_DIR / f"dryrun_home_{arm}")
    inputs, globals_lib = declare_givens(smith, arm, samples, ensure=importing or args.import_givens or not remote)
    if importing:
        print(f"{arm}: the pool at {smith.home.GetPath()} holds the givens of {len(samples)} samples")
        return

    print(f"\n=== {arm} arm: {len(samples)} samples ===")
    task = smith.GenerateWorkflow(
        samples=list(inputs.AsSamples("e2::read_metadata")),
        resources=[DataInstanceLibrary.Load(c.LIBRARY / "resources" / "e2"),
                   DataInstanceLibrary.Load(c.MLIB / "resources" / "lib"), globals_lib],
        transforms=[TransformInstanceLibrary.Load(c.LIBRARY / "transforms" / "e2")],
        targets=build_targets(arm),
    )
    n_truth = len(samples) if arm == "short" else sum(1 for s in samples if s[2] is not None)
    reads = {"e2::short_reads_1": len(samples), "e2::short_reads_2": len(samples)} if arm == "short" \
        else {"e2::long_reads": len(samples)}
    c.check_plan(task, {**reads, "e2::read_metadata": len(samples),
                        "e2::read_truth": n_truth})
    c.print_plan(task, 22)
    if args.dag:
        c.write_dag(task, f"e2_cami_{arm}", CACHE_DIR)
    if remote:
        c.stage_and_run(smith, task, CACHE_DIR, f"{args.tag or 'e2'}_{arm}", stage_only=args.stage_only,
                        params=dict(executor=dict(queueSize=500), process=dict(tries=4, array=25)),
                        materialise=args.materialise)


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
    for name in ("import", "run"):
        p = sub.add_parser(name)
        p.add_argument("--arm", default="both", choices=["short", "long", "both"])
        p.add_argument("--limit", type=int, help="first N samples per arm")
        p.set_defaults(fn=cmd_run, dag=False, stage_only=False, launch=False, materialise=False, tag=None)
        if name == "run":
            p.add_argument("--dag", action="store_true", help="render each arm to page/dags/e2_cami_<arm>.dag.svg")
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

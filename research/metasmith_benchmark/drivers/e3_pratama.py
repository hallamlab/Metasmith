#!/usr/bin/env python3
"""E3: Pratama 2026 groundwater virome from the pre-interleaved Illumina runs.

Solves locally by default and renders the DAG. --stage-only and --launch reach fir and
need Tony's sign-off. Both refuse until every run's interleave .ok stamp exists.

Subcommands: list, run [--dataset ...] [--limit N] [--stage-only | --launch].
"""

import argparse
import csv
import json
import os
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]
CACHE_DIR = Path(os.environ.get("E3_CACHE_DIR", HERE / ".cache" / "e3"))
os.environ.setdefault("MSM_CACHE_DIR", str(CACHE_DIR))
sys.path.insert(0, str(REPO / "research" / "cami"))

import run_cami_metag as rcm  # noqa: E402
from metasmith.python_api import (  # noqa: E402
    Agent, Source, Runtime, DataInstanceLibrary, TargetBuilder, Resources, Size, Duration,
)

INTERLEAVED = Path(os.environ.get("PRATAMA_INTERLEAVED", "/scratch/phyberos/pratama2026/interleaved"))
DAG_DIR = HERE.parent / "page" / "dags"

RESOURCE_OVERRIDES = {
    "bbduk": Resources(memory=Size.GB(64), cpus=16),
    "megahit": Resources(memory=Size.GB(128), cpus=32, duration=Duration(hours=12)),
    "downloadDramDB": Resources(cpus=4, memory=Size.GB(8), duration=Duration(hours=12)),
}


def enumerate_runs():
    rows = csv.DictReader(rcm.PRATAMA_RUNS_TSV.open(), delimiter="\t")
    return [(r["run_accession"], r["dataset"], INTERLEAVED / r["dataset"] / f"{r['run_accession']}.fastq.gz")
            for r in rows if r["library_layout"] == "PAIRED"]


def select(runs, args):
    if args.dataset:
        runs = [r for r in runs if r[1] in args.dataset]
    if args.limit:
        runs = runs[: args.limit]
    return runs


def missing_on_fir(runs):
    tokens = " ".join(f"{d}/{r}" for r, d, _ in runs)
    out, _ = rcm.ssh_cmd(f'for e in {tokens}; do [ -s "{INTERLEAVED}/$e.fastq.gz" ] && [ -e "{INTERLEAVED}/$e.ok" ] || echo "$e"; done')
    return out.split()


def build_inputs(runs):
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    inputs = DataInstanceLibrary(CACHE_DIR / "e3_inputs.xgdb")
    inputs.Purge()
    for tl in ["sequences.yml", "viromics.yml"]:
        inputs.AddTypeLibrary(rcm.MLIB / "data_types" / tl)

    # One study root: merge_candidate_calls pools every run's calls through read_pair's study parent.
    study_value = json.dumps({"logistics": "contig study"})
    (inputs.location / "contig_study.json").write_text(study_value)
    study = inputs.RegisterItem(
        "contig_study.json", "viromics::contig_study",
        instance_id=rcm._stable_id("pratama", "contig_study", study_value),
    )
    for run, dataset, reads in runs:
        meta_value = json.dumps({"parity": "paired", "length_class": "short"})
        (inputs.location / f"{run}_read_metadata.json").write_text(meta_value)
        meta = inputs.RegisterItem(
            f"{run}_read_metadata.json", "sequences::read_metadata", parents={study},
            instance_id=rcm._stable_id("pratama", "read_metadata", run, meta_value),
        )
        (inputs.location / f"{run}_read_pair.txt").write_text(run)
        pair = inputs.RegisterItem(
            f"{run}_read_pair.txt", "sequences::read_pair", parents={meta},
            instance_id=rcm._stable_id("pratama", "read_pair", run, run),
        )
        inputs.RegisterItem(
            reads, "sequences::short_reads_pe", parents={pair},
            instance_id=rcm._stable_id("pratama", "short_reads_pe", run, f"interleaved/{dataset}/{run}"),
        )
    inputs.Save()
    return inputs


def build_targets(with_dram_mags=False, with_host_prediction=False):
    t = TargetBuilder()
    asm = t.Add("sequences::megahit_assembly")
    frozen = t.Add("viromics::dereplicated_candidate_virus")

    viral = ["viromics::contig_length_table", "viromics::precluster_table",
             "viromics::votu_cluster_table", "viromics::checkv_contamination",
             "viromics::vcontact3_network", "pratama::votu_recovery_table"]
    if with_host_prediction:
        viral.append("viromics::host_prediction_genome")
    for dtype in viral:
        t.Add(dtype, parents=[frozen])

    per_assembly = ["annotation::dramv_distill", "sequences::orfs", "sequences::gff",
                    "sequences::assembly_stats", "sequences::assembly_per_contig_coverage", "alignment::bam"]
    if with_dram_mags:
        per_assembly.append("annotation::dram_annotations")
    for dtype in per_assembly:
        t.Add(dtype, parents=[asm])

    t.Add("sequences::read_qc_stats")
    mw_bin = t.Add("sequences::metawrap_bin_fasta", parents=[asm])
    t.Add("binning::metawrap_contig_to_bin_table", parents=[asm])
    t.Add("taxonomy::checkm_stats", parents=[mw_bin])
    return t


def cmd_list(args):
    runs = select(enumerate_runs(), args)
    for run, dataset, reads in runs:
        print(f"{run:14s} {dataset:11s} {reads}")
    print(f"{len(runs)} paired runs")
    if args.check:
        missing = missing_on_fir(runs)
        print(f"{len(runs) - len(missing)} interleaved and verified on fir, {len(missing)} not yet: {' '.join(missing)}")
    return 0


def cmd_run(args):
    runs = select(enumerate_runs(), args)
    if not runs:
        sys.exit("no runs selected")
    remote = args.stage_only or args.launch
    if remote:
        missing = missing_on_fir(runs)
        if missing:
            sys.exit(f"{len(missing)} runs lack a verified interleaved file: {' '.join(missing)}")

    inputs = build_inputs(runs)
    samples = list(inputs.AsSamples("sequences::read_metadata"))
    print(f"{len(runs)} runs -> {len(samples)} sample view(s), pooled under the study root")

    smith = rcm.get_agent("pratama") if remote else Agent(
        home=Source.FromLocal(CACHE_DIR / "dryrun_home"), runtime=Runtime.APPTAINER)
    task = smith.GenerateWorkflow(
        samples=samples,
        resources=[DataInstanceLibrary.Load(rcm.MLIB / "resources" / "env"),
                   DataInstanceLibrary.Load(rcm.MLIB / "resources" / "lib"),
                   rcm.build_globals_pratama(with_zenodo_comparison=True)],
        transforms=rcm.build_transforms_for_pratama(),
        targets=build_targets(args.with_dram_mags, args.with_host_prediction),
    )
    if not task.ok:
        print(f"dropped: {sorted(task.plan.dropped_targets)}", file=sys.stderr)
        rcm._report_plan_failure(task)
    rcm._assert_given_counts(task, {
        "viromics::contig_study": 1,
        "sequences::read_metadata": len(runs),
        "sequences::read_pair": len(runs),
        "sequences::short_reads_pe": len(runs),
    })

    steps = task.plan.steps
    print(f"Plan OK -- {len(steps)} steps, key={task.GetKey()}")
    for s in steps:
        prods = sorted({i.dtype_name for g in s.produces for i in g})
        print(f"  {s.order:>2}. {Path(s.transform._path).stem:30s} -> {prods}")
    interleave = [s for s in steps if Path(s.transform._path).stem == "interleave_zipped_short_reads"]
    assert not interleave, "the plan interleaves reads that are registered pre-interleaved"

    if args.dag:
        DAG_DIR.mkdir(parents=True, exist_ok=True)
        rendered = Path(str(task.plan.RenderDAG(str(CACHE_DIR / "e3_pratama"), format="svg")))
        dag = DAG_DIR / "e3_pratama.dag.svg"
        dag.write_bytes(rendered.read_bytes())
        print(f"dag: {dag}")

    if not remote:
        print("(dry run; nothing staged or submitted)")
        return 0

    keys_file = CACHE_DIR / "task_keys.json"
    keys = json.loads(keys_file.read_text()) if keys_file.exists() else {}
    keys[args.tag or f"e3_{len(runs)}runs"] = task.GetKey()
    keys_file.write_text(json.dumps(keys, indent=2))

    smith.StageWorkflow(task, on_exist="update", verify_external_paths=False)
    if args.stage_only:
        print(f"staged as {task.GetKey()}")
        return 0
    smith.RunWorkflow(
        task=task, config_file=rcm.make_slurm_config(),
        params=dict(slurmAccount=rcm.SLURM_ACCOUNT, executor=dict(queueSize=500),
                    process=dict(tries=4, array=25)),
        resource_overrides=RESOURCE_OVERRIDES,
    )
    print(f"submitted {task.GetKey()}")
    return 0


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name, fn in (("list", cmd_list), ("run", cmd_run)):
        p = sub.add_parser(name)
        p.add_argument("--dataset", nargs="*", help="reads_2019, reads_2022")
        p.add_argument("--limit", type=int)
        p.set_defaults(fn=fn)
        if name == "list":
            p.add_argument("--check", action="store_true", help="count verified interleaved files on fir")
        else:
            p.add_argument("--with-dram-mags", action="store_true", help="DRAM on MAGs; needs B3 fixed")
            p.add_argument("--with-host-prediction", action="store_true", help="iPHoP; pulls GTDB-Tk de novo")
            p.add_argument("--dag", action="store_true", help="render the plan to page/dags/e3_pratama.dag.svg")
            mode = p.add_mutually_exclusive_group()
            mode.add_argument("--stage-only", action="store_true")
            mode.add_argument("--launch", action="store_true")
            p.add_argument("--tag")
    args = ap.parse_args()
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())

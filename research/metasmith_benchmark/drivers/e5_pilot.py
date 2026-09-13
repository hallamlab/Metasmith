#!/usr/bin/env python3
"""E5 pilot: the provisional E5 tool list on 9 samples, 3 per corpus, one plan per corpus.

Every corpus registers the same shape, study -> read_metadata -> read_pair -> reads, so the
viral merge pools per study and the three plans differ only in their read leaves:
  cami     toy_mousegut, interleaved short reads
  pratama  reads_2019, the research agent's pre-interleaved runs
  metagem  li2019, paired mate files (the plan interleaves them)

Solves what the library supports today and prints the transforms E5 still needs.
Solves locally by default and renders one DAG per corpus. --stage-only and --launch reach
fir and need Tony's sign-off.

Subcommands: list, run [--corpus cami|pratama|metagem|all] [--stage-only | --launch].
"""

import argparse
import csv
import json
import os
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]
CACHE_DIR = Path(os.environ.get("E5_CACHE_DIR", HERE / ".cache" / "e5"))
os.environ.setdefault("MSM_CACHE_DIR", str(CACHE_DIR))
os.environ.setdefault("E4_CACHE_DIR", str(CACHE_DIR / "modelling"))
for corpus_dir in ("cami", "metagem"):
    sys.path.insert(0, str(REPO / "research" / corpus_dir))
sys.path.insert(0, str(HERE))

import run_cami_metag as rcm  # noqa: E402
import run_metagem as rm  # noqa: E402
import e3_pratama  # noqa: E402
import e4_metagem  # noqa: E402
from metasmith.python_api import (  # noqa: E402
    Agent, Source, Runtime, DataInstanceLibrary, TransformInstanceLibrary,
    TargetBuilder, Resources, Size, Duration,
)

PILOT = {"cami": "toy_mousegut", "pratama": "reads_2019", "metagem": "li2019"}
PER_CORPUS = 3
DAG_DIR = HERE.parent / "page" / "dags"

# In the E5 tool list, not reachable in any plan yet.
MISSING = [
    "MAGScoT (new transform): the second refiner",
    "dRep (new transform): the second dereplicator",
    "skani_dedup over DAS Tool and MAGScoT bins per study: today it takes the aggregator's pool, per assembly",
    "a study grouping type: viromics::contig_study stands in, and only the viral merge groups by it",
    "Prodigal on MAG ORFs for the 4-lane panel: the panel takes whole-assembly ORFs only",
    "DeepVirFinder (new transform)",
    "MetaPop (new transform): every sample mapped to its study's vOTU catalogue",
    "minced (new transform): the only spacer source, so spacer_host_links stays out",
    "SMETANA (new transform)",
    "Chopper (new transform): only for long-read corpora, none in this pilot",
    "CoverM (new transform, if chosen)",
]

RESOURCE_OVERRIDES = {
    "megahit": Resources(memory=Size.GB(128), cpus=32, duration=Duration(hours=12)),
    "carveme_from_orfs": Resources(memory=Size.GB(16), cpus=4, duration=Duration(hours=12)),
    "downloadDramDB": Resources(cpus=4, memory=Size.GB(8), duration=Duration(hours=12)),
}
BINNER_PARAMS = dict(metabat2_min_contig=1500, metabat2_seed=1, semibin2_min_len=1500, semibin2_seed=1)


def pilot_samples():
    """{corpus: [(sample id, reads)]}, where reads is one interleaved path or a (fwd, rev) pair."""
    cami_rows = csv.DictReader(rcm.CAMI_SAMPLES_TSV.open(), delimiter="\t")
    cami = [(f"{r['dataset']}_{r['sample_id']}", Path(r["reads_path"])) for r in cami_rows
            if r["dataset"] == PILOT["cami"] and r["read_type"] == "short"]
    pratama = [(run, reads) for run, dataset, reads in e3_pratama.enumerate_runs() if dataset == PILOT["pratama"]]
    metagem = [(run, paths) for dataset, run, layout, paths in rm.enumerate_runs()
               if dataset == PILOT["metagem"] and layout == "paired"]
    return {"cami": cami[:PER_CORPUS], "pratama": pratama[:PER_CORPUS], "metagem": metagem[:PER_CORPUS]}


def _write(lib, name, value):
    (lib.location / name).write_text(value)
    return name


def build_inputs(corpus, samples):
    inputs = DataInstanceLibrary(CACHE_DIR / f"e5_{corpus}_inputs.xgdb")
    inputs.Purge()
    for tl in ["sequences.yml", "viromics.yml"]:
        inputs.AddTypeLibrary(rcm.MLIB / "data_types" / tl)

    ns = f"e5_pilot_{corpus}"
    study_value = json.dumps({"logistics": "contig study", "study": PILOT[corpus]})
    study = inputs.RegisterItem(_write(inputs, "contig_study.json", study_value), "viromics::contig_study",
                                instance_id=rcm._stable_id(ns, "contig_study", study_value))
    for sid, reads in samples:
        meta_value = json.dumps({"parity": "paired", "length_class": "short"})
        meta = inputs.RegisterItem(_write(inputs, f"{sid}_read_metadata.json", meta_value),
                                   "sequences::read_metadata", parents={study},
                                   instance_id=rcm._stable_id(ns, "read_metadata", sid, meta_value))
        pair = inputs.RegisterItem(_write(inputs, f"{sid}_read_pair.txt", sid), "sequences::read_pair",
                                   parents={meta}, instance_id=rcm._stable_id(ns, "read_pair", sid))
        if isinstance(reads, tuple):
            for dtype, path in zip(("zipped_forward_short_reads", "zipped_reverse_short_reads"), reads):
                inputs.RegisterItem(path, f"sequences::{dtype}", parents={pair},
                                    instance_id=rcm._stable_id(ns, dtype, sid, str(path)))
        else:
            inputs.RegisterItem(reads, "sequences::short_reads_pe", parents={pair},
                                instance_id=rcm._stable_id(ns, "short_reads_pe", sid, str(reads)))
    inputs.Save()
    return inputs


def expected_counts(samples):
    n = len(samples)
    expected = {"viromics::contig_study": 1, "sequences::read_metadata": n, "sequences::read_pair": n}
    if isinstance(samples[0][1], tuple):
        expected.update({"sequences::zipped_forward_short_reads": n, "sequences::zipped_reverse_short_reads": n})
    else:
        expected["sequences::short_reads_pe"] = n
    return expected


def build_transforms():
    viromics = TransformInstanceLibrary.Load(rcm.MLIB / "transforms" / "viromics")
    modelling = TransformInstanceLibrary.Load(rcm.MLIB / "transforms" / "metabolicModelling")
    metagenomics = TransformInstanceLibrary.Load(rcm.MLIB / "transforms" / "metagenomics")
    return [
        TransformInstanceLibrary.Load(rcm.MLIB / "transforms" / "logistics"),
        rcm._assembly_without("spades"),
        # E5 bins with no MetaWRAP. Visible, it answers GTDB-Tk de novo's bare putative_genome slot.
        metagenomics.AsView({Path("binning/metawrap.py")}, invert=True),
        TransformInstanceLibrary.Load(rcm.MLIB / "transforms" / "functionalAnnotation"),
        TransformInstanceLibrary.Load(rcm.MLIB / "transforms" / "fabfos"),
        # CCTyper is dropped from every experiment.
        viromics.AsView({Path("cctyper.py")}, invert=True),
        modelling.AsView({Path("carveme_from_orfs_cplex.py")}, invert=True),
    ]


def build_targets():
    t = TargetBuilder()
    asm = t.Add("sequences::megahit_assembly")
    t.Add("sequences::read_qc_stats")
    for dtype in ("sequences::orfs", "sequences::gff", "sequences::assembly_stats",
                  "sequences::assembly_per_contig_coverage", "alignment::bam"):
        t.Add(dtype, parents=[asm])

    for b in ("metabat2", "semibin2", "comebin"):
        t.Add(f"sequences::{b}_bin_fasta", parents=[asm])
        t.Add(f"binning::{b}_contig_to_bin_table", parents=[asm])
    mags = t.Add("sequences::das_tool_bin_fasta", parents=[asm])
    t.Add("binning::das_tool_contig_to_bin_table", parents=[asm])
    t.Add("taxonomy::checkm_stats", parents=[mags])
    t.Add("taxonomy::gtdbtk", parents=[mags])
    t.Add("binning_local::cluster_table", parents=[asm])

    bin_orfs = t.Add("sequences::bin_orfs", parents=[mags])
    model = t.Add("modelling::carveme_model", parents=[bin_orfs])
    t.Add("modelling::memote_score", parents=[model])

    for dtype in ("annotation::kofamscan_results", "annotation::diamond_uniref50_results",
                  "annotation::clean_predictions", "annotation::proteinbert_embeddings"):
        t.Add(dtype, parents=[asm])

    frozen = t.Add("viromics::dereplicated_candidate_virus")
    for dtype in ("viromics::contig_length_table", "viromics::precluster_table",
                  "viromics::votu_cluster_table", "viromics::checkv_contamination",
                  "viromics::vcontact3_network", "viromics::host_prediction_genome"):
        t.Add(dtype, parents=[frozen])
    t.Add("annotation::dramv_distill", parents=[asm])
    return t


def solve(corpus, samples, args):
    remote = args.stage_only or args.launch
    if remote and corpus == "pratama":
        missing = e3_pratama.missing_on_fir([(sid, PILOT["pratama"], reads) for sid, reads in samples])
        if missing:
            sys.exit(f"{len(missing)} pilot runs lack a verified interleaved file: {' '.join(missing)}")

    print(f"\n=== {corpus}: {PILOT[corpus]}, {len(samples)} samples ===")
    inputs = build_inputs(corpus, samples)
    agent_home = {"cami": lambda: rcm.get_agent("cami"), "pratama": lambda: rcm.get_agent("pratama"),
                  "metagem": rm.get_agent}
    smith = agent_home[corpus]() if remote else Agent(
        home=Source.FromLocal(CACHE_DIR / f"dryrun_home_{corpus}"), runtime=Runtime.APPTAINER)
    task = smith.GenerateWorkflow(
        samples=list(inputs.AsSamples("sequences::read_metadata")),
        resources=[DataInstanceLibrary.Load(rcm.MLIB / "resources" / "env"),
                   DataInstanceLibrary.Load(rcm.MLIB / "resources" / "lib"),
                   rcm.build_globals_pratama(),
                   e4_metagem.build_globals("open")],
        transforms=build_transforms(),
        targets=build_targets(),
    )
    if not task.ok:
        print(f"dropped: {sorted(task.plan.dropped_targets)}", file=sys.stderr)
        rcm._report_plan_failure(task)
    rcm._assert_given_counts(task, expected_counts(samples))

    print(f"Plan OK -- {len(task.plan.steps)} steps, key={task.GetKey()}")
    for s in task.plan.steps:
        prods = sorted({i.dtype_name for g in s.produces for i in g})
        print(f"  {s.order:>2}. {Path(s.transform._path).stem:30s} -> {prods}")

    if args.dag:
        DAG_DIR.mkdir(parents=True, exist_ok=True)
        rendered = Path(str(task.plan.RenderDAG(str(CACHE_DIR / f"e5_pilot_{corpus}"), format="svg")))
        dag = DAG_DIR / f"e5_pilot_{corpus}.dag.svg"
        dag.write_bytes(rendered.read_bytes())
        print(f"dag: {dag}")

    if not remote:
        return
    keys_file = CACHE_DIR / "task_keys.json"
    keys = json.loads(keys_file.read_text()) if keys_file.exists() else {}
    keys[f"{args.tag or 'e5_pilot'}_{corpus}"] = task.GetKey()
    keys_file.write_text(json.dumps(keys, indent=2))
    smith.StageWorkflow(task, on_exist="update", verify_external_paths=False)
    if args.stage_only:
        print(f"staged {corpus} as {task.GetKey()}")
        return
    make_config = rm.make_slurm_config if corpus == "metagem" else rcm.make_slurm_config
    smith.RunWorkflow(
        task=task, config_file=make_config(),
        params=dict(slurmAccount=rcm.SLURM_ACCOUNT, executor=dict(queueSize=500),
                    process=dict(tries=4, array=25), **BINNER_PARAMS),
        resource_overrides=RESOURCE_OVERRIDES,
    )
    print(f"submitted {corpus}: {task.GetKey()}")


def cmd_list(args):
    for corpus, samples in pilot_samples().items():
        print(f"{corpus} ({PILOT[corpus]}): {', '.join(sid for sid, _ in samples)}")
    return 0


def cmd_run(args):
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    pilots = pilot_samples()
    for corpus in (list(PILOT) if args.corpus == "all" else [args.corpus]):
        samples = pilots[corpus]
        assert len(samples) == PER_CORPUS, f"{corpus} has {len(samples)} pilot samples, expected {PER_CORPUS}"
        solve(corpus, samples, args)
    print("\nIn the E5 tool list, not in the plans yet:")
    for gap in MISSING:
        print(f"  - {gap}")
    return 0


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("list").set_defaults(fn=cmd_list)
    p = sub.add_parser("run")
    p.add_argument("--corpus", default="all", choices=[*PILOT, "all"])
    p.add_argument("--dag", action="store_true", help="render each plan to page/dags/e5_pilot_<corpus>.dag.svg")
    mode = p.add_mutually_exclusive_group()
    mode.add_argument("--stage-only", action="store_true")
    mode.add_argument("--launch", action="store_true")
    p.add_argument("--tag")
    p.set_defaults(fn=cmd_run)
    args = ap.parse_args()
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())

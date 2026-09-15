#!/usr/bin/env python3
"""E3: Pratama 2026 groundwater virome from the pre-interleaved Illumina runs.

One sample is one metagenome: well x year x filter fraction x replicate, which is one ENA
run, assembled alone. 31 from 2019 and 34 from 2022 make Pratama's 65.

`run` solves against a local dry-run home and renders the DAG. `import`, `--stage-only`
and `--launch` act on the fir agent home, from a Slurm job there, and refuse until every
run's interleave .ok stamp exists.

Subcommands: list [--check], import, run [--dataset ...] [--limit N] [--dag] [--stage-only | --launch].
"""

import argparse
import os
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
CACHE_DIR = Path(os.environ.get("E3_CACHE_DIR", HERE / ".cache" / "e3"))

import _common as c  # noqa: E402
from metasmith.python_api import (  # noqa: E402
    DataInstanceLibrary, TransformInstanceLibrary, TargetBuilder,
)

INTERLEAVED = Path(os.environ.get("PRATAMA_INTERLEAVED", "/scratch/phyberos/pratama2026/interleaved"))
# ENA's 2019 BioSamples group both filter fractions of a well, so a BioSample is not a sample.
# ERR3858126 (PNK108_H32_0_2) is a second run of the H32 0.2 um R1 metagenome, which the paper
# counts once as H32_0_2_1 (ERR3858122).
EXCLUDED_RUNS = {"ERR3858126"}
EXPECTED_RUNS = 65
TYPE_LIBS = [c.MLIB / "data_types" / t for t in ("sequences.yml", "viromics.yml")]

# First-attempt (cpus, GB, hours); retries scale with the attempt.
SCALED = {"megahit": (32, 128, 12)}

# The standard transforms each E3 library transform replaces, by library.
REPLACED = {
    "assembly": {"bbduk.py", "spades.py", "assembly_stats.py"},
    "metagenomics": {"binning/metawrap.py", "taxonomy/genomad.py", "taxonomy/gtdbtk.py"},
    "functionalAnnotation": {"virsorter2.py", "dramv.py"},
    # CCTyper is dropped from every experiment. Pratama's spacer caller is minced, a gapfill.
    "viromics": {"vibrant.py", "merge_candidate_calls.py", "mmseqs_votu.py", "mmseqs_precluster.py", "cctyper.py",
                 "prodigal_gv.py"},
}


def enumerate_runs():
    runs = [(r["run_accession"], r["dataset"], INTERLEAVED / r["dataset"] / f"{r['run_accession']}.fastq.gz")
            for r in c.pratama_rows()
            if r["library_layout"] == "PAIRED" and r["run_accession"] not in EXCLUDED_RUNS]
    assert len(runs) == EXPECTED_RUNS, f"{len(runs)} short-read runs, Pratama has {EXPECTED_RUNS}"
    return runs


def select(runs, args):
    if args.dataset:
        runs = [r for r in runs if r[1] in args.dataset]
    if args.limit:
        runs = runs[: args.limit]
    return runs


def missing_on_fir(runs):
    tokens = " ".join(f"{d}/{r}" for r, d, _ in runs)
    out = c.host_sh(f'for e in {tokens}; do [ -s "{INTERLEAVED}/$e.fastq.gz" ] && [ -e "{INTERLEAVED}/$e.ok" ] || echo "$e"; done')
    return out.split()


def declare_givens(smith, runs, ensure):
    # One study root: merge_candidate_calls pools every run's calls through read_pair's study parent.
    givens = smith.PoolGivens()
    study = c.add_value(givens, "e3/contig_study", {"logistics": "contig study"},
                        "viromics::contig_study", tags=["e3"])
    for run, dataset, reads in runs:
        tags = ["e3", dataset, run]
        meta = c.add_value(givens, f"e3/{run}/read_metadata", {"parity": "paired", "length_class": "short"},
                           "sequences::read_metadata", parents=[study], tags=tags)
        pair = c.add_value(givens, f"e3/{run}/read_pair", run, "sequences::read_pair", parents=[meta], tags=tags)
        c.add_file(givens, f"e3/{run}/reads", reads, "sequences::short_reads_pe", parents=[pair], tags=tags)
    return c.cite(givens, CACHE_DIR / "e3_inputs.xgdb", TYPE_LIBS, ensure)


def build_transforms():
    std = [lib if name not in REPLACED else lib.AsView({Path(p) for p in REPLACED[name]}, invert=True)
           for name, lib in ((n, TransformInstanceLibrary.Load(c.MLIB / "transforms" / n))
                             for n in ("logistics", "assembly", "metagenomics", "functionalAnnotation", "viromics"))]
    return [TransformInstanceLibrary.Load(c.LIBRARY / "transforms" / "e3"),
            TransformInstanceLibrary.Load(c.LIBRARY / "transforms" / "bench"), *std]


def build_targets(with_host_prediction=False, with_gtdbtk=False):
    """The tool table's E3 column, less its gapfills: BinSanity, abawaca, dRep, DeepVirFinder, MetaPop,
    minced with its BLASTn spacers, hybrid metaSPAdes, and iPHoP on the augmented database."""
    t = TargetBuilder()
    t.Add("sequences::read_qc_stats")
    t.Add("e3::fastp_report_json")
    t.Add("e3::fastp_report_html")

    # MAG lane: metaSPAdes, MetaWRAP (CheckM inside it), DRAM on the MAGs.
    spades = t.Add("sequences::spades_assembly")
    for dtype in ("sequences::orfs", "sequences::gff", "sequences::assembly_stats",
                  "sequences::assembly_per_contig_coverage", "alignment::bam",
                  "binning::metawrap_contig_to_bin_table", "binning::metawrap_bin_stats",
                  "e3::mag_dram_annotations", "e3::mag_dram_distill"):
        t.Add(dtype, parents=[spades])
    mags = t.Add("sequences::metawrap_bin_fasta", parents=[spades])
    if with_gtdbtk:
        t.Add("taxonomy::gtdbtk", parents=[mags])

    # Viral lane: both assemblies' calls pooled into one frozen set.
    t.Add("sequences::megahit_assembly")
    frozen = t.Add("viromics::dereplicated_candidate_virus")
    viral = ["viromics::contig_length_table", "viromics::votu_cluster_table", "viromics::checkv_contamination",
             "viromics::checkv_quality_summary", "viromics::vcontact3_network", "e3::viral_orfs", "e3::viral_gff",
             "annotation::dramv_annotations", "annotation::dramv_distill", "pratama::votu_recovery_table"]
    if with_host_prediction:
        viral.append("e3::host_prediction_genome_default")
    for dtype in viral:
        t.Add(dtype, parents=[frozen])
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
    importing = args.cmd == "import"
    remote = importing or args.stage_only or args.launch or args.materialise
    if remote:
        missing = missing_on_fir(runs)
        if missing:
            sys.exit(f"{len(missing)} runs lack a verified interleaved file: {' '.join(missing)}")

    smith = c.agent_for("pratama", remote, CACHE_DIR / "dryrun_home")
    ensure = importing or args.import_givens or not remote
    inputs = declare_givens(smith, runs, ensure)
    globals_lib = c.pratama_globals(smith, CACHE_DIR, ensure, with_zenodo_comparison=True)
    if importing:
        print(f"the pool at {smith.home.GetPath()} holds the givens of {len(runs)} runs")
        return 0

    task = smith.GenerateWorkflow(
        samples=list(inputs.AsSamples("sequences::read_metadata")),
        resources=[DataInstanceLibrary.Load(c.MLIB / "resources" / "env"),
                   DataInstanceLibrary.Load(c.MLIB / "resources" / "lib"), globals_lib],
        transforms=build_transforms(),
        targets=build_targets(args.with_host_prediction, args.with_gtdbtk),
    )
    c.check_plan(task, {"viromics::contig_study": 1, "sequences::read_metadata": len(runs),
                        "sequences::read_pair": len(runs), "sequences::short_reads_pe": len(runs)})
    c.print_plan(task)
    interleave = [s for s in task.plan.steps if Path(s.transform._path).stem == "interleave_zipped_short_reads"]
    assert not interleave, "the plan interleaves reads that are registered pre-interleaved"

    if args.dag:
        c.write_dag(task, "e3_pratama", CACHE_DIR)
    if remote:
        c.stage_and_run(smith, task, CACHE_DIR, args.tag or f"e3_{len(runs)}runs", stage_only=args.stage_only,
                        params=dict(executor=dict(queueSize=500), process=dict(tries=4, array=25)),
                        scaled=SCALED, materialise=args.materialise)
    else:
        print("(dry run; nothing staged or submitted)")
    return 0


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name, fn in (("list", cmd_list), ("import", cmd_run), ("run", cmd_run)):
        p = sub.add_parser(name)
        p.add_argument("--dataset", nargs="*", help="reads_2019, reads_2022")
        p.add_argument("--limit", type=int)
        p.set_defaults(fn=fn, dag=False, stage_only=False, launch=False, materialise=False, tag=None,
                       with_host_prediction=False, with_gtdbtk=False)
        if name == "list":
            p.add_argument("--check", action="store_true", help="count verified interleaved files on fir")
        elif name == "run":
            p.add_argument("--with-host-prediction", action="store_true",
                           help="iPHoP on the shipped database; needs ref::iphop_db staged")
            p.add_argument("--with-gtdbtk", action="store_true",
                           help="GTDB-Tk on the MAGs; needs ref::gtdb's representative genomes")
            p.add_argument("--dag", action="store_true", help="render the plan to page/dags/e3_pratama.dag.svg")
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

#!/usr/bin/env python3
"""E3: Pratama 2026 groundwater virome from the pre-interleaved Illumina runs.

One sample is one metagenome: well x year x filter fraction x replicate, which is one ENA
run, assembled alone. 31 from 2019 and 34 from 2022 make Pratama's 65.

`run` solves against a local dry-run home and renders the DAG. `import`, `--stage-only`
and `--launch` act on the fir agent home, from a Slurm job there, and refuse until every
run's interleave .ok stamp exists.

Subcommands: list [--check], import, run [--runs ...] [--dataset ...] [--limit N] [--dag] [--stage-only | --launch].
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
# ENA's MinION runs as fetched, one per well; not interleaved or otherwise touched.
RAW_2022 = Path(os.environ.get("PRATAMA_RAW_2022", "/scratch/phyberos/pratama2026/reads_2022"))
# One hard link per hybrid pairing, staged by stage_hybrid_pairs.sh -- see hybrid_partners().
HYBRID_PAIRS = Path(os.environ.get("PRATAMA_HYBRID_PAIRS", "/scratch/phyberos/pratama2026/hybrid_pairs"))
EXPECTED_HYBRIDS = 17
TYPE_LIBS = [c.MLIB / "data_types" / t for t in ("sequences.yml", "viromics.yml")] + [c.LIBRARY / "data_types" / "e3.yml"]

# First-attempt (cpus, GB, hours); retries double memory and time. Sized to the MaxRSS and wall that
# sacct recorded for waves 1-8 (September 2026), so the first grant covers p90 and the doubled one the
# tail. CAUTION bbduk's -Xmx and MEGAHIT's --memory follow the grant, so their RSS measures the grant,
# not the need. Their sizes come from spanish-lakes, whose bbduk ran at 2 cpus / 32 GB and MEGAHIT at
# 8 / 32 on every sample. Short-read metaSPAdes keeps its declaration: 40 of 65 assemblies failed at
# 192 GB and finished at 384 GB, at MaxRSS 324-332 GB.
SCALED = {
    "seqkit_reads": (2, 4, 1),              # MaxRSS 1.5 GB
    "bbduk_pratama": (4, 16, 2),
    "megahit": (16, 64, 12),
    "deepvirfinder_pratama": (8, 32, 6),    # 16 GB stalls at the cap on 240 Mbp batches, no OOM
    "vibrant_pratama": (8, 16, 4),          # MaxRSS 7.3 GB, 0.8 h
    "virsorter2_pratama": (8, 16, 8),       # MaxRSS 5.0 GB, 4.7 h
    "genomad_pratama": (8, 16, 6),          # MaxRSS 11.3 GB, 2.2 h at 16 cpus
    "genomad_island_annotate_pratama": (8, 32, 6),  # OOM at 16 GB: mmseqs prefilter loads the whole DB
    "spades_hybrid_pratama": (48, 384, 36),  # its declaration, capped at LARGE_MEMORY_STEPS' 768 GB
    "merge_candidate_calls_pratama": (4, 32, 4),  # 65 runs sat at a 16 GB cap for 1.6 h
    "pratama_votu_recovery": (8, 128, 4),   # 65 runs: OOM at 32 GB, MaxRSS 60.7 GB at 64
    "mmseqs_votu_pratama": (16, 128, 6),    # 65 runs: OOM at 64 GB, MaxRSS 121.6 GB at 128
}

# The standard transforms each E3 library transform replaces, by library.
REPLACED = {
    "assembly": {"bbduk.py", "spades.py", "assembly_stats.py"},
    "metagenomics": {"binning/metawrap.py", "taxonomy/genomad.py", "taxonomy/gtdbtk.py"},
    "functionalAnnotation": {"virsorter2.py", "dramv.py"},
    # CCTyper is dropped from every experiment. Pratama's spacer caller is minced, a gapfill.
    # checkv.py joins the list in wave 7: E3 scores the frozen set in slices (checkv_batch_pratama plus
    # checkv_merge_pratama), and the pinned merge produces the same four viromics::checkv_* types. Without
    # the mask the standard whole-set transform would be a second producer of all four.
    # vcontact3.py joins for the same reason and a harder one: the standard transform requires the frozen
    # set, whose 4,597,542 contigs make vConTACT3's quadratic network and agglomerative stages unservable
    # at any grant fir has (three OOMs at 128/256/512 GiB; ~38 TiB needed for the dense component matrix).
    # vcontact3_pratama.py runs the same tool on the 70,785 >=10 kb vOTU representatives and produces the
    # same two viromics::vcontact3_* types, so without the mask both would answer the network target.
    "viromics": {"vibrant.py", "merge_candidate_calls.py", "mmseqs_votu.py", "mmseqs_precluster.py", "cctyper.py",
                 "prodigal_gv.py", "checkv.py", "vcontact3.py"},
}


def enumerate_runs():
    runs = [(r["run_accession"], r["dataset"], INTERLEAVED / r["dataset"] / f"{r['run_accession']}.fastq.gz")
            for r in c.pratama_rows()
            if r["library_layout"] == "PAIRED" and r["run_accession"] not in EXCLUDED_RUNS]
    assert len(runs) == EXPECTED_RUNS, f"{len(runs)} short-read runs, Pratama has {EXPECTED_RUNS}"
    return runs


def select(runs, args):
    if args.runs:
        unknown = set(args.runs) - {r[0] for r in runs}
        assert not unknown, f"not among the {len(runs)} short-read runs: {sorted(unknown)}"
        runs = [r for r in runs if r[0] in args.runs]
    if args.dataset:
        runs = [r for r in runs if r[1] in args.dataset]
    if args.limit:
        runs = runs[: args.limit]
    return runs


def missing_on_fir(runs):
    tokens = " ".join(f"{d}/{r}" for r, d, _ in runs)
    out = c.host_sh(f'for e in {tokens}; do [ -s "{INTERLEAVED}/$e.fastq.gz" ] && [ -e "{INTERLEAVED}/$e.ok" ] || echo "$e"; done')
    return out.split()


def hybrid_pairings():
    """(illumina_run, minion_run) for every 0.2 um 2022 replicate paired with its well's MinION run.

    The pure well-matching this and hybrid_partners() share, factored out so a test -- and
    stage_hybrid_pairs.sh's own test -- can check the 17 pairs without touching a path.
    """
    rows = c.pratama_rows()
    minion = {r["sample_alias"].split("_")[0]: r["run_accession"]
              for r in rows if r["instrument_model"] == "MinION"}
    pairs = [(r["run_accession"], minion[r["sample_alias"][:3]])
             for r in rows if r["dataset"] == "reads_2022" and r["library_layout"] == "PAIRED"
             and r["sample_alias"].endswith("02um_2022") and r["sample_alias"][:3] in minion]
    assert len(pairs) == EXPECTED_HYBRIDS, f"{len(pairs)} hybrid pairs, Pratama has {EXPECTED_HYBRIDS}"
    return pairs


def hybrid_partners():
    """Illumina run -> its own hard-linked copy of its well's MinION file (Pratama's 17 hybrids).

    A distinct path per pairing, not the shared RAW_2022 source and not a symlink to it: the given
    library's manifest is keyed by path (pool.py's GivenLibrary, transfer.py's Unpack), and importing
    resolves symlinks to their target (ops.data's `Path(path).resolve()`) before that key is taken --
    so three Illumina replicates sharing one MinION run would collapse to a single given however they
    are named. A hard link is a second directory entry for the same bytes that `resolve()` does not
    walk through, so it alone keeps the 17 pairings 17 distinct paths. stage_hybrid_pairs.sh creates them.
    """
    return {run: HYBRID_PAIRS / f"{run}__{minion_run}.fastq.gz" for run, minion_run in hybrid_pairings()}


def declare_givens(smith, runs, ensure):
    # One study root: merge_candidate_calls pools every run's calls through read_pair's study parent.
    givens = smith.PoolGivens()
    study = c.add_value(givens, "e3/contig_study", {"logistics": "contig study"},
                        "viromics::contig_study", tags=["e3"])
    partners = hybrid_partners()
    for run, dataset, reads in runs:
        tags = ["e3", dataset, run]
        meta = c.add_value(givens, f"e3/{run}/read_metadata", {"parity": "paired", "length_class": "short"},
                           "sequences::read_metadata", parents=[study], tags=tags)
        pair = c.add_value(givens, f"e3/{run}/read_pair", run, "sequences::read_pair", parents=[meta], tags=tags)
        c.add_file(givens, f"e3/{run}/reads", reads, "sequences::short_reads_pe", parents=[pair], tags=tags)
        if run in partners:
            c.add_file(givens, f"e3/{run}/nanopore", partners[run], "e3::nanopore_reads", parents=[pair], tags=tags)
    return c.cite(givens, CACHE_DIR / "e3_inputs.xgdb", TYPE_LIBS, ensure)


def build_transforms():
    std = [lib if name not in REPLACED else lib.AsView({Path(p) for p in REPLACED[name]}, invert=True)
           for name, lib in ((n, TransformInstanceLibrary.Load(c.MLIB / "transforms" / n))
                             for n in ("logistics", "assembly", "metagenomics", "functionalAnnotation", "viromics"))]
    return [TransformInstanceLibrary.Load(c.LIBRARY / "transforms" / "e3"),
            TransformInstanceLibrary.Load(c.LIBRARY / "transforms" / "bench"), *std]


def build_targets():
    t = TargetBuilder()
    t.Add("sequences::read_qc_stats")
    t.Add("e3::fastp_report_json")
    t.Add("e3::fastp_report_html")

    # Hybrid design A: only the 17 runs that carry a MinION partner can produce it.
    t.Add("e3::hybrid_spades_assembly")
    t.Add("sequences::spades_assembly")
    t.Add("sequences::megahit_assembly")

    # Every assembly lane's calls pooled into one frozen set and scored at the pool level, then
    # curated, host-trimmed and clustered to vOTUs; the >=5 kb representatives go through the
    # island filter to the final published-catalogue shape, scored against Pratama's vOTUs again.
    frozen = t.Add("viromics::dereplicated_candidate_virus")
    for dtype in ("viromics::contig_length_table", "viromics::checkv_contamination",
                  "viromics::checkv_quality_summary", "pratama::votu_recovery_table"):
        t.Add(dtype, parents=[frozen])

    curated = t.Add("e3::curated_candidate_virus", parents=[frozen])
    t.Add("viromics::votu_cluster_table", parents=[curated])
    final = t.Add("e3::final_votu_representatives", parents=[curated])
    t.Add("e3::final_votu_recovery_table", parents=[final])
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
                   DataInstanceLibrary.Load(c.MLIB / "resources" / "lib"),
                   DataInstanceLibrary.Load(c.LIBRARY / "resources" / "bench"), globals_lib],
        transforms=build_transforms(),
        targets=build_targets(),
    )
    c.check_plan(task, {"viromics::contig_study": 1, "sequences::read_metadata": len(runs),
                        "sequences::read_pair": len(runs), "sequences::short_reads_pe": len(runs),
                        "e3::nanopore_reads": sum(1 for r, _, _ in runs if r in hybrid_partners())})
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
        p.add_argument("--runs", nargs="*", help="run accessions, e.g. a hybrid pilot")
        p.add_argument("--dataset", nargs="*", help="reads_2019, reads_2022")
        p.add_argument("--limit", type=int)
        p.set_defaults(fn=fn, dag=False, stage_only=False, launch=False, materialise=False, tag=None)
        if name == "list":
            p.add_argument("--check", action="store_true", help="count verified interleaved files on fir")
        elif name == "run":
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

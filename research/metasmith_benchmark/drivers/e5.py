#!/usr/bin/env python3
"""E5: the three reproductions combined into one pipeline, one plan per corpus.

  E3  QC, the assemblies and the viral lane: bbduk and seqkit on Pratama's settings, MEGAHIT,
      metaSPAdes and hybrid metaSPAdes, four callers pooled into one frozen set, CheckV curation
      and host trimming, MMseqs2 vOTUs, and the island filter on the >=5 kb representatives
  E2  the MAG lane on the MEGAHIT assembly: MetaBAT2, SemiBin2, COMEBin, DAS Tool, CheckM2
  E4  the modern GEM lane on DAS Tool's MAGs: Prodigal, CarveMe 1.6.6 on SCIP, MEMOTE 0.17.0

Every tool here ran in a reproduction. Scoring (AMBER, vOTU recovery, GEM parity) runs after
the plan, outside it. Each corpus declares study -> read_metadata -> read_pair -> reads, and the
Pratama pilot takes 2022 runs with a MinION partner, so its plan carries the hybrid lane.

Subcommands: list, run [--corpus cami|pratama|metagem|all] [--dag].
"""

import argparse
import os
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
CACHE_DIR = Path(os.environ.get("E5_CACHE_DIR", HERE / ".cache" / "e5"))

import _common as c  # noqa: E402
import e3_pratama  # noqa: E402
import e4_metagem  # noqa: E402
from metasmith.python_api import (  # noqa: E402
    DataInstanceLibrary, TransformInstanceLibrary, TargetBuilder,
)

STUDY = {"cami": "toy_mousegut", "pratama": "reads_2022", "metagem": "li2019"}
PER_CORPUS = 3
TYPE_LIBS = e3_pratama.TYPE_LIBS
# The modern lane's CarveMe and MEMOTE come from the bench modelling library.
GEM_MASK = {Path("carveme_from_orfs.py"), Path("memote_score.py"), Path("carveme_from_orfs_cplex.py")}


def samples_by_corpus():
    """{corpus: [(sample id, reads, nanopore or None)]}, where reads is one interleaved path or a (fwd, rev) pair."""
    cami = [(f"{r['dataset']}_{r['sample_id']}", Path(r["reads_path"]), None) for r in c.cami_rows()
            if r["dataset"] == STUDY["cami"] and r["read_type"] == "short"]
    partners = e3_pratama.hybrid_partners()
    pratama = [(run, reads, partners[run]) for run, dataset, reads in e3_pratama.enumerate_runs()
               if dataset == STUDY["pratama"] and run in partners]
    metagem = [(run, paths, None) for dataset, run, layout, paths in c.metagem_runs()
               if dataset == STUDY["metagem"] and layout == "paired"]
    return {"cami": cami[:PER_CORPUS], "pratama": pratama[:PER_CORPUS], "metagem": metagem[:PER_CORPUS]}


def declare_givens(smith, corpus, samples, ensure):
    ns = f"e5/{corpus}"
    givens = smith.PoolGivens()
    study = c.add_value(givens, f"{ns}/contig_study", {"logistics": "contig study", "study": STUDY[corpus]},
                        "viromics::contig_study", tags=["e5", corpus])
    for sid, reads, nanopore in samples:
        tags = ["e5", corpus, sid]
        meta = c.add_value(givens, f"{ns}/{sid}/read_metadata", {"parity": "paired", "length_class": "short"},
                           "sequences::read_metadata", parents=[study], tags=tags)
        pair = c.add_value(givens, f"{ns}/{sid}/read_pair", sid, "sequences::read_pair", parents=[meta], tags=tags)
        if isinstance(reads, tuple):
            for dtype, path in zip(("zipped_forward_short_reads", "zipped_reverse_short_reads"), reads):
                c.add_file(givens, f"{ns}/{sid}/{dtype}", path, f"sequences::{dtype}", parents=[pair], tags=tags)
        else:
            c.add_file(givens, f"{ns}/{sid}/reads", reads, "sequences::short_reads_pe", parents=[pair], tags=tags)
        if nanopore is not None:
            c.add_file(givens, f"{ns}/{sid}/nanopore", nanopore, "e3::nanopore_reads", parents=[pair], tags=tags)
    return c.cite(givens, CACHE_DIR / corpus / "inputs.xgdb", TYPE_LIBS, ensure)


def expected_counts(samples):
    n = len(samples)
    expected = {"viromics::contig_study": 1, "sequences::read_metadata": n, "sequences::read_pair": n,
                "e3::nanopore_reads": sum(1 for s in samples if s[2] is not None)}
    if isinstance(samples[0][1], tuple):
        expected.update({"sequences::zipped_forward_short_reads": n, "sequences::zipped_reverse_short_reads": n})
    else:
        expected["sequences::short_reads_pe"] = n
    return expected


def build_transforms():
    # E3's masks, except that the standard assembly_stats answers instead of E3's copy: they differ
    # only in E3's dropping the BAM, which the MAG lane's binners read.
    replaced = {**e3_pratama.REPLACED, "assembly": e3_pratama.REPLACED["assembly"] - {"assembly_stats.py"}}
    std = [TransformInstanceLibrary.Load(c.MLIB / "transforms" / name).AsView(
               {Path(p) for p in replaced.get(name, ())}, invert=True)
           for name in ("logistics", "assembly", "metagenomics", "functionalAnnotation", "viromics")]
    e3 = TransformInstanceLibrary.Load(c.LIBRARY / "transforms" / "e3").AsView(
        {Path("assembly_stats_pratama.py")}, invert=True)
    modelling = TransformInstanceLibrary.Load(c.MLIB / "transforms" / "metabolicModelling")
    return [e3, TransformInstanceLibrary.Load(c.LIBRARY / "transforms" / "bench"), *std,
            modelling.AsView(GEM_MASK, invert=True),
            TransformInstanceLibrary.Load(c.LIBRARY / "transforms" / "modelling")]


def build_targets(hybrid):
    t = TargetBuilder()
    t.Add("sequences::read_qc_stats")

    asm = t.Add("sequences::megahit_assembly")
    t.Add("sequences::spades_assembly")
    if hybrid:
        t.Add("e3::hybrid_spades_assembly")

    for b in ("metabat2", "semibin2", "comebin"):
        bins = t.Add(f"sequences::{b}_bin_fasta", parents=[asm])
        t.Add(f"binning::{b}_contig_to_bin_table", parents=[asm])
        t.Add("bench::checkm2_quality", parents=[bins])
    mags = t.Add("sequences::das_tool_bin_fasta", parents=[asm])
    t.Add("binning::das_tool_contig_to_bin_table", parents=[asm])
    t.Add("bench::checkm2_quality", parents=[mags])

    gem = t.Add("modelling::carveme_model", parents=[t.Add("sequences::bin_orfs", parents=[mags])])
    t.Add("modelling::memote_score", parents=[gem])

    frozen = t.Add("viromics::dereplicated_candidate_virus")
    for dtype in ("viromics::contig_length_table", "viromics::checkv_contamination",
                  "viromics::checkv_quality_summary"):
        t.Add(dtype, parents=[frozen])
    curated = t.Add("e3::curated_candidate_virus", parents=[frozen])
    t.Add("viromics::votu_cluster_table", parents=[curated])
    t.Add("e3::final_votu_representatives", parents=[curated])
    return t


def solve(corpus, samples, args):
    print(f"\n=== {corpus}: {STUDY[corpus]}, {len(samples)} samples ===")
    smith = c.agent_for(corpus, False, CACHE_DIR / corpus / "dryrun_home")
    inputs = declare_givens(smith, corpus, samples, ensure=True)
    pratama_globals = c.pratama_globals(smith, CACHE_DIR / corpus, ensure=True)
    gem_globals = e4_metagem.declare_globals(smith, "open", CACHE_DIR / corpus / "gem_globals.xgdb", True,
                                             with_checkm2=True)
    task = smith.GenerateWorkflow(
        samples=list(inputs.AsSamples("sequences::read_metadata")),
        resources=[DataInstanceLibrary.Load(c.MLIB / "resources" / "env"),
                   DataInstanceLibrary.Load(c.MLIB / "resources" / "lib"),
                   DataInstanceLibrary.Load(c.LIBRARY / "resources" / "bench"),
                   pratama_globals, gem_globals],
        transforms=build_transforms(),
        targets=build_targets(hybrid=any(s[2] is not None for s in samples)),
    )
    c.check_plan(task, expected_counts(samples))
    c.print_plan(task)
    if args.dag:
        c.write_dag(task, f"e5_{corpus}", CACHE_DIR / corpus)


def cmd_list(args):
    for corpus, samples in samples_by_corpus().items():
        print(f"{corpus} ({STUDY[corpus]}): {', '.join(s[0] for s in samples)}")
    return 0


def cmd_run(args):
    by_corpus = samples_by_corpus()
    for corpus in (list(STUDY) if args.corpus == "all" else [args.corpus]):
        samples = by_corpus[corpus]
        assert len(samples) == PER_CORPUS, f"{corpus} has {len(samples)} samples, expected {PER_CORPUS}"
        solve(corpus, samples, args)
    return 0


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("list").set_defaults(fn=cmd_list)
    p = sub.add_parser("run")
    p.add_argument("--corpus", default="all", choices=[*STUDY, "all"])
    p.add_argument("--dag", action="store_true", help="render each plan to page/dags/e5_<corpus>.dag.svg")
    p.set_defaults(fn=cmd_run)
    args = ap.parse_args()
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())

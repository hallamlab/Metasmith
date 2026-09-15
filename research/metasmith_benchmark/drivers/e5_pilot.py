#!/usr/bin/env python3
"""E5 pilot: the provisional E5 tool list on 9 samples, 3 per corpus, one plan per corpus.

Every corpus declares the same shape, study -> read_metadata -> read_pair -> reads, so the
viral merge pools per study and the three plans differ only in their read leaves:
  cami     toy_mousegut, interleaved short reads
  pratama  reads_2019, the research agent's pre-interleaved runs
  metagem  li2019, paired mate files (the plan interleaves them)

Solves what the library supports today and prints the transforms E5 still needs.
`run` solves against local dry-run homes and renders one DAG per corpus. `import`,
`--stage-only` and `--launch` act on the fir agent homes, from a Slurm job there.

Subcommands: list, import [--corpus], run [--corpus cami|pratama|metagem|all] [--dag] [--stage-only | --launch].
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

PILOT = {"cami": "toy_mousegut", "pratama": "reads_2019", "metagem": "li2019"}
PER_CORPUS = 3
TYPE_LIBS = [c.MLIB / "data_types" / t for t in ("sequences.yml", "viromics.yml")]

# In the E5 tool list, not reachable in any plan yet.
MISSING = [
    "a study grouping type: viromics::contig_study stands in for the viral merge, dRep and skani per study",
    "dRep and skani over refined sets (DAS Tool, MAGScoT): they run over the three binners' raw bins, as siblings (D1)",
    "Prodigal on MAG ORFs for the 4-lane panel: the panel takes whole-assembly ORFs only",
    "DeepVirFinder calls in the frozen viral set: Pratama gives no score or p-value cut, so only its table is a target",
    "MetaPop (new transform): every sample mapped to its study's vOTU catalogue",
    "minced (new transform): the only spacer source, so spacer_host_links stays out",
    "SMETANA on CPLEX: E5 runs metaGEM's call on SCIP",
    "Chopper (new transform): only for long-read corpora, none in this pilot",
    "CoverM (new transform, if chosen)",
    "DRAM-v and minced: decided after E3 reports",
    "GTDB-Tk and iPHoP (--with-gtdbtk): wait on ref::gtdb's representative genomes",
]

# First-attempt (cpus, GB, hours); retries scale with the attempt.
SCALED = {"megahit": (32, 128, 12), "carveme_from_orfs": (4, 16, 12), "carveme_from_orfs_cplex": (4, 16, 12)}
BINNER_PARAMS = dict(metabat2_min_contig=1500, metabat2_seed=1, semibin2_min_len=1500, semibin2_seed=1)


def pilot_samples():
    """{corpus: [(sample id, reads)]}, where reads is one interleaved path or a (fwd, rev) pair."""
    cami = [(f"{r['dataset']}_{r['sample_id']}", Path(r["reads_path"])) for r in c.cami_rows()
            if r["dataset"] == PILOT["cami"] and r["read_type"] == "short"]
    pratama = [(run, reads) for run, dataset, reads in e3_pratama.enumerate_runs() if dataset == PILOT["pratama"]]
    metagem = [(run, paths) for dataset, run, layout, paths in c.metagem_runs()
               if dataset == PILOT["metagem"] and layout == "paired"]
    return {"cami": cami[:PER_CORPUS], "pratama": pratama[:PER_CORPUS], "metagem": metagem[:PER_CORPUS]}


def declare_givens(smith, corpus, samples, ensure):
    ns = f"e5/{corpus}"
    givens = smith.PoolGivens()
    study = c.add_value(givens, f"{ns}/contig_study", {"logistics": "contig study", "study": PILOT[corpus]},
                        "viromics::contig_study", tags=["e5", corpus])
    for sid, reads in samples:
        tags = ["e5", corpus, sid]
        meta = c.add_value(givens, f"{ns}/{sid}/read_metadata", {"parity": "paired", "length_class": "short"},
                           "sequences::read_metadata", parents=[study], tags=tags)
        pair = c.add_value(givens, f"{ns}/{sid}/read_pair", sid, "sequences::read_pair", parents=[meta], tags=tags)
        if isinstance(reads, tuple):
            for dtype, path in zip(("zipped_forward_short_reads", "zipped_reverse_short_reads"), reads):
                c.add_file(givens, f"{ns}/{sid}/{dtype}", path, f"sequences::{dtype}", parents=[pair], tags=tags)
        else:
            c.add_file(givens, f"{ns}/{sid}/reads", reads, "sequences::short_reads_pe", parents=[pair], tags=tags)
    return c.cite(givens, CACHE_DIR / corpus / "inputs.xgdb", TYPE_LIBS, ensure)


def expected_counts(samples):
    n = len(samples)
    expected = {"viromics::contig_study": 1, "sequences::read_metadata": n, "sequences::read_pair": n}
    if isinstance(samples[0][1], tuple):
        expected.update({"sequences::zipped_forward_short_reads": n, "sequences::zipped_reverse_short_reads": n})
    else:
        expected["sequences::short_reads_pe"] = n
    return expected


def build_transforms(solver="open"):
    # The open solver's CarveMe is the bench library's 1.6.6 pin, so the standard 1.6.1 one is always masked.
    masked = {Path("carveme_from_orfs.py"), Path("memote_score.py")}
    if solver == "open":
        masked.add(Path("carveme_from_orfs_cplex.py"))
    viromics = TransformInstanceLibrary.Load(c.MLIB / "transforms" / "viromics")
    modelling = TransformInstanceLibrary.Load(c.MLIB / "transforms" / "metabolicModelling")
    metagenomics = TransformInstanceLibrary.Load(c.MLIB / "transforms" / "metagenomics")
    return [
        TransformInstanceLibrary.Load(c.MLIB / "transforms" / "logistics"),
        c.assembly_without("spades"),
        # E5 bins with no MetaWRAP. Visible, it answers GTDB-Tk de novo's bare putative_genome slot.
        # GTDB-Tk comes from the bench library, which reads GTDB's genomes from one image.
        metagenomics.AsView({Path("binning/metawrap.py"), Path("taxonomy/gtdbtk.py")}, invert=True),
        TransformInstanceLibrary.Load(c.LIBRARY / "transforms" / "bench"),
        TransformInstanceLibrary.Load(c.MLIB / "transforms" / "functionalAnnotation"),
        TransformInstanceLibrary.Load(c.MLIB / "transforms" / "fabfos"),
        # CCTyper is dropped from every experiment. prodigal-gv runs from the bench library (B23).
        viromics.AsView({Path("cctyper.py"), Path("prodigal_gv.py")}, invert=True),
        modelling.AsView(masked, invert=True),
        TransformInstanceLibrary.Load(c.LIBRARY / "transforms" / "modelling"),
    ]


def build_targets(with_gtdbtk=False, solver="open"):
    t = TargetBuilder()
    asm = t.Add("sequences::megahit_assembly")
    t.Add("sequences::read_qc_stats")
    for dtype in ("sequences::orfs", "sequences::gff", "sequences::assembly_stats",
                  "sequences::assembly_per_contig_coverage", "alignment::bam"):
        t.Add(dtype, parents=[asm])

    for b in ("metabat2", "semibin2", "comebin"):
        bins = t.Add(f"sequences::{b}_bin_fasta", parents=[asm])
        t.Add(f"binning::{b}_contig_to_bin_table", parents=[asm])
        t.Add("bench::checkm2_quality", parents=[bins])
    mags = t.Add("sequences::das_tool_bin_fasta", parents=[asm])
    t.Add("binning::das_tool_contig_to_bin_table", parents=[asm])
    t.Add("bench::checkm2_quality", parents=[mags])
    if with_gtdbtk:
        t.Add("taxonomy::gtdbtk", parents=[mags])
    # Dereplication: DAS Tool above, MAGScoT per sample, and dRep and skani over the same three bin
    # sets, per sample and per study. The standard skani_dedup reads the CheckM 1 aggregator's pool.
    for dtype in ("bench::magscot_contig_to_bin", "bench::drep_sample_winners", "bench::skani_sample_clusters"):
        t.Add(dtype, parents=[asm])
    for dtype in ("bench::drep_study_winners", "bench::skani_study_clusters"):
        t.Add(dtype)

    bin_orfs = t.Add("sequences::bin_orfs", parents=[mags])
    model_type = "modelling::carveme_model_cplex" if solver == "cplex" else "modelling::carveme_model"
    model = t.Add(model_type, parents=[bin_orfs])
    t.Add("modelling::memote_score", parents=[model])
    if solver == "open":
        t.Add("bench::smetana_detailed", parents=[asm])

    for dtype in ("annotation::kofamscan_results", "annotation::diamond_uniref50_results",
                  "annotation::clean_predictions", "annotation::proteinbert_embeddings"):
        t.Add(dtype, parents=[asm])

    t.Add("bench::deepvirfinder_scores", parents=[asm])
    frozen = t.Add("viromics::dereplicated_candidate_virus")
    viral = ["viromics::contig_length_table", "viromics::precluster_table", "viromics::votu_cluster_table",
             "viromics::checkv_contamination", "viromics::vcontact3_network"]
    if with_gtdbtk:
        # iPHoP's augmented database needs GTDB-Tk de novo's decorated trees.
        viral.append("viromics::host_prediction_genome")
    for dtype in (*viral, "bench::viral_orfs", "bench::viral_gff"):
        t.Add(dtype, parents=[frozen])
    return t


def solve(corpus, samples, args):
    importing = args.cmd == "import"
    remote = importing or args.stage_only or args.launch or args.materialise
    if remote and corpus == "pratama":
        missing = e3_pratama.missing_on_fir([(sid, PILOT["pratama"], reads) for sid, reads in samples])
        if missing:
            sys.exit(f"{len(missing)} pilot runs lack a verified interleaved file: {' '.join(missing)}")

    print(f"\n=== {corpus}: {PILOT[corpus]}, {len(samples)} samples ===")
    smith = c.agent_for(corpus, remote, CACHE_DIR / corpus / "dryrun_home")
    ensure = importing or args.import_givens or not remote
    inputs = declare_givens(smith, corpus, samples, ensure)
    pratama_globals = c.pratama_globals(smith, CACHE_DIR / corpus, ensure)
    modelling_globals = e4_metagem.declare_globals(smith, args.solver, CACHE_DIR / corpus / "e4_globals.xgdb", ensure,
                                                   with_checkm2=True)
    if importing:
        print(f"the pool at {smith.home.GetPath()} holds the givens of {len(samples)} samples")
        return

    task = smith.GenerateWorkflow(
        samples=list(inputs.AsSamples("sequences::read_metadata")),
        resources=[DataInstanceLibrary.Load(c.MLIB / "resources" / "env"),
                   DataInstanceLibrary.Load(c.MLIB / "resources" / "lib"),
                   DataInstanceLibrary.Load(c.LIBRARY / "resources" / "bench"),
                   pratama_globals, modelling_globals],
        transforms=build_transforms(args.solver),
        targets=build_targets(args.with_gtdbtk, args.solver),
    )
    c.check_plan(task, expected_counts(samples))
    c.print_plan(task)
    if args.dag:
        c.write_dag(task, f"e5_pilot_{corpus}", CACHE_DIR / corpus)
    if remote:
        c.stage_and_run(smith, task, CACHE_DIR / corpus, f"{args.tag or 'e5_pilot'}_{corpus}",
                        stage_only=args.stage_only,
                        params=dict(executor=dict(queueSize=500), process=dict(tries=4, array=25), **BINNER_PARAMS),
                        scaled=SCALED, materialise=args.materialise, gpus=c.FIR_GPU)


def cmd_list(args):
    for corpus, samples in pilot_samples().items():
        print(f"{corpus} ({PILOT[corpus]}): {', '.join(sid for sid, _ in samples)}")
    return 0


def cmd_run(args):
    pilots = pilot_samples()
    for corpus in (list(PILOT) if args.corpus == "all" else [args.corpus]):
        samples = pilots[corpus]
        assert len(samples) == PER_CORPUS, f"{corpus} has {len(samples)} pilot samples, expected {PER_CORPUS}"
        solve(corpus, samples, args)
    if args.cmd == "run":
        print("\nIn the E5 tool list, not in the plans yet:")
        for gap in MISSING:
            print(f"  - {gap}")
    return 0


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("list").set_defaults(fn=cmd_list)
    for name in ("import", "run"):
        p = sub.add_parser(name)
        p.add_argument("--corpus", default="all", choices=[*PILOT, "all"])
        # E5 pratama's open-solver gapfill ran past 2 h on 353 of 381 bins; cplex is E4's route.
        p.add_argument("--solver", default="open", choices=["open", "cplex"])
        p.set_defaults(fn=cmd_run, dag=False, stage_only=False, launch=False, materialise=False, tag=None,
                       with_gtdbtk=False)
        if name == "run":
            p.add_argument("--dag", action="store_true", help="render each plan to page/dags/e5_pilot_<corpus>.dag.svg")
            p.add_argument("--with-gtdbtk", action="store_true",
                           help="GTDB-Tk and iPHoP; needs ref::gtdb's representative genomes")
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

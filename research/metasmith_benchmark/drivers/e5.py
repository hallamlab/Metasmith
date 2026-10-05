#!/usr/bin/env python3
"""E5: the combined E2 + E3 + E4 targets on every CAMI, Pratama 2026 and metaGEM run, one plan per read type.

  E3  QC, the viral lane: four callers pooled per study, CheckV curation and host trimming,
      MMseqs2 vOTUs, and the island filter on the >=5 kb representatives
  E2  the MAG lane: MetaBAT2, SemiBin2, COMEBin, DAS Tool, with CheckM2 on DAS Tool's bins only
  E4  the modern GEM lane on DAS Tool's MAGs: their assembly ORFs, CarveMe 1.6.6, MEMOTE 0.17.0

A plan takes one read type, so each type renders its own DAG:

  cami_pe            CAMI short reads, interleaved
  cami_hybrid_ont    CAMI short + simulated Nanopore
  cami_long_pacbio   CAMI simulated PacBio CLR alone
  pratama_pe         Pratama short reads, interleaved
  pratama_hybrid_ont Pratama short + real MinION
  metagem_pe_split   metaGEM paired reads as two gzipped files
  metagem_se         metaGEM single-end reads

A hybrid sample's assembly is MEGAHIT -> OPERA-MS, the lane the hybrid pilot chose. A PacBio
sample's is Flye on its long reads alone, after Filtlong. Every other sample's is MEGAHIT. QC is bbduk on JGI's settings for every corpus. Each study is one root, so the
viral lane (opt-in with --viral) pools a study, and a batch holds whole studies.

Subcommands: list [--batch N], import --batch N [--shape S],
run --batch N [--shape S] [--dag] [--stage-only|--launch|--materialise] [--import] [--clear].
"""

import argparse
import os
import sys
import time
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

TYPE_LIBS = e3_pratama.TYPE_LIBS + [c.LIBRARY / "data_types" / "e5.yml"]
PILOT_SIZE = 3
# The modern lane's CarveMe comes from e5_gem, split at its DIAMOND search, and MEMOTE from the bench
# modelling library.
GEM_MASK = {Path("carveme_from_orfs.py"), Path("memote_score.py"), Path("carveme_from_orfs_cplex.py")}
# The hybrid pilot's comparison lanes and scorers.
PILOT_ONLY = {Path("flye.py"), Path("polca.py"), Path("quast.py"), Path("metaquast.py")}

# shape -> (corpus, read layout, assembly: short, hybrid or long).
SHAPES = {
    "cami_pe": ("cami", "pe", "short"),
    "cami_hybrid_ont": ("cami", "pe", "hybrid"),
    "cami_long_pacbio": ("cami", "long", "long"),
    "pratama_pe": ("pratama", "pe", "short"),
    "pratama_hybrid_ont": ("pratama", "pe", "hybrid"),
    "metagem_pe_split": ("metagem", "split", "short"),
    "metagem_se": ("metagem", "se", "short"),
}
# Run only when named with --shape: their assembler waits on the Pratama assembler ablation.
HELD_SHAPES = {"cami_hybrid_ont", "pratama_hybrid_ont"}
# A long-read-only sample's platform, which picks Flye's mode and minimap2's preset.
PLATFORM = {"cami_long_pacbio": "PACBIO_CLR"}

# batch -> {shape: [study]}. Batch 0 is the pilot: the first PILOT_SIZE samples of each shape's first study.
BATCHES = {
    1: {"cami_pe": ["toy_mousegut", "toy_hmp_airskinurogenital", "toy_hmp_gastrooral"]},
    2: {"cami_hybrid_ont": ["toy_humangut", "plant_associated"], "cami_long_pacbio": ["marine", "strain"]},
    3: {"pratama_pe": ["pratama_short"], "pratama_hybrid_ont": ["pratama_hybrid"]},
    4: {"metagem_se": ["korem2015"], "metagem_pe_split": ["li2019", "bissett_base"]},
    5: {"metagem_pe_split": ["karlsson2013"]},
    6: {"metagem_pe_split": ["sunagawa2015"]},
}
BATCHES[0] = {shape: [studies[0]] for b in (2, 3, 4, 1) for shape, studies in BATCHES[b].items()}
# Batches whose assemblies came from the standard MEGAHIT and e5's own megahit_draft, before e5_assembly.
# They keep those transforms so a relaunch serves its assemblies, and everything after them, from the cache.
PRE_E5_ASSEMBLY = {1, 3, 4}

# CAMI's long-read set for each hybrid study.
CAMI_LONG = {"toy_humangut": "toy_humangut_long", "plant_associated": "plant_associated_long_nano",
             "marine": "marine_long", "strain": "strain_long"}

# First-attempt (cpus, GB, hours), retries doubling memory and time. The viral sizes are E3's, measured
# on its 65 runs (e3_pratama.SCALED). The MAG lane keeps E2's declarations, except COMEBin (COMEBIN below).
SCALED = {
    "seqkit_reads": (2, 4, 1),
    "bbduk": (4, 16, 2),
    "megahit": (16, 64, 12),
    "megahit_draft": (16, 64, 12),
    "deepvirfinder": (8, 64, 6),   # batch 0: a CAMI hybrid contig batch OOMed at 32 GB after 6 h
    "vibrant": (8, 16, 4),
    "virsorter2": (8, 16, 8),
    "genomad": (8, 16, 6),
    "votu_island_annotate": (8, 32, 6),
    "viral_merge_calls": (4, 32, 4),
    "votu_cluster": (16, 128, 6),
}
# COMEBin's first-attempt (cpus, GB, hours) per shape, on the CPU account; a retry doubles memory and time.
# The transform's CPU patch trains a ~97K-contig Pratama sample at 27.7 s/epoch on 24 cpus and 25.8 s on 48,
# so every shape takes 24. That sample runs all 200 epochs in ~2 h; its Leiden sweep peaks at 38 GB. A CAMI sample takes
# ~17 min at 12 cpus. Hours are about 2x the measured run, 3x for the shapes with ~180K-contig assemblies.
COMEBIN = {
    "cami_pe": (24, 24, 2), "cami_hybrid_ont": (24, 24, 2), "cami_long_pacbio": (24, 24, 3), "metagem_se": (24, 24, 2),
    "pratama_pe": (24, 48, 4), "pratama_hybrid_ont": (24, 48, 6), "metagem_pe_split": (24, 48, 6),
}
# The account may queue 1,000 jobs. A batch's plans share this, which leaves room for their drivers.
# Nextflow counts each element of a job array against queueSize, and refuses an array wider than it,
# so a plan's share must stay at or above slurm.nf's array width of 100.
QUEUE_BUDGET = 840


def _cami(study):
    rows = c.cami_rows()
    short = {r["sample_id"]: Path(r["reads_path"]) for r in rows if r["dataset"] == study and r["read_type"] == "short"}
    order = lambda s: int(s.split("_")[1])  # noqa: E731
    if study not in CAMI_LONG:
        return [(f"{study}_{s}", short[s], None) for s in sorted(short, key=order)]
    long_ = {r["sample_id"]: Path(r["reads_path"]) for r in rows
             if r["dataset"] == CAMI_LONG[study] and r["read_type"] == "long"}
    assert short.keys() == long_.keys(), f"{study}: short and long samples differ"
    return [(f"{CAMI_LONG[study]}_{s}", short[s], long_[s]) for s in sorted(short, key=order)]


def _pratama(hybrid):
    partners = e3_pratama.hybrid_partners()
    return [(run, reads, partners[run] if hybrid else None) for run, _, reads in e3_pratama.enumerate_runs()
            if (run in partners) == hybrid]


def _metagem(study):
    return [(run, paths if layout == "paired" else paths[0], None) for ds, run, layout, paths in c.metagem_runs()
            if ds == study]


def samples_of(study):
    if study == "pratama_short":
        return _pratama(False)
    if study == "pratama_hybrid":
        return _pratama(True)
    if study in {r["dataset"] for r in c.cami_rows()}:
        return _cami(study)
    return _metagem(study)


def draw(batch, only=None):
    """{shape: {study: [(sample id, reads, long reads or None)]}} for one batch."""
    plans = {}
    for shape, studies in BATCHES[batch].items():
        if only and shape != only:
            continue
        plans[shape] = {s: samples_of(s)[:PILOT_SIZE] if batch == 0 else samples_of(s) for s in studies}
        for study, samples in plans[shape].items():
            assert samples, f"{shape}/{study}: no samples"
    return plans


def declare_givens(smith, shape, by_study, cache_dir, ensure):
    layout = SHAPES[shape][1]
    givens = smith.PoolGivens()
    for study_name, samples in by_study.items():
        study = c.add_value(givens, f"e5/{study_name}", {"study": study_name}, "sequences::study",
                            tags=["e5", shape, study_name])
        for sid, reads, long_ in samples:
            tags = ["e5", shape, study_name, sid]
            ns = f"e5/{study_name}/{sid}"
            if layout == "long":
                meta = c.add_value(givens, f"{ns}/read_metadata",
                                   {"sample": sid, "parity": "single", "length_class": "long",
                                    "platform": PLATFORM[shape]},
                                   "sequences::read_metadata", parents=[study], tags=tags)
                # CAUTION not `long_reads`: a given's name hashes its path but not its type, so that name cites
                # the hybrid pilot's import of the same file as e3::nanopore_reads.
                c.add_file(givens, f"{ns}/long_only_reads", long_, "sequences::long_reads", parents=[meta], tags=tags)
                continue
            meta = c.add_value(givens, f"{ns}/read_metadata",
                               {"sample": sid, "parity": "single" if layout == "se" else "paired",
                                "length_class": "short"},
                               "sequences::read_metadata", parents=[study], tags=tags)
            if layout == "se":
                c.add_file(givens, f"{ns}/reads", reads, "sequences::short_reads_se", parents=[meta], tags=tags)
            elif layout == "split":
                for dtype, path in zip(("zipped_forward_short_reads", "zipped_reverse_short_reads"), reads):
                    c.add_file(givens, f"{ns}/{dtype}", path, f"sequences::{dtype}", parents=[meta], tags=tags)
            else:
                c.add_file(givens, f"{ns}/reads", reads, "sequences::short_reads_pe", parents=[meta], tags=tags)
            if long_ is not None:
                c.add_file(givens, f"{ns}/long_reads", long_, "e3::nanopore_reads", parents=[meta], tags=tags)
    return c.cite(givens, cache_dir / "inputs.xgdb", TYPE_LIBS, ensure)


def expected_counts(shape, by_study):
    layout = SHAPES[shape][1]
    n = sum(len(s) for s in by_study.values())
    want = {"sequences::study": len(by_study), "sequences::read_metadata": n, "sequences::read_pair": 0}
    if layout == "long":
        want["sequences::long_reads"] = n
    elif layout == "se":
        want["sequences::short_reads_se"] = n
    elif layout == "split":
        want.update({"sequences::zipped_forward_short_reads": n, "sequences::zipped_reverse_short_reads": n})
    else:
        want["sequences::short_reads_pe"] = n
    if SHAPES[shape][2] == "hybrid":
        want["e3::nanopore_reads"] = n
    return want


def build_transforms(mode, pre_e5_assembly=False):
    # E5 owns its viral lane, the MAG ORF mapping and the hybrid pair. e5_binning owns a COMEBin and DAS Tool
    # that survive an assembly too small for COMEBin. The standard libraries keep E3's masks, except
    # assembly_stats (its BAM feeds the binners) and bbduk, whose JGI settings QC every corpus.
    # e5_assembly owns both MEGAHITs, which delete their workspace: the assembly for a short-read sample, and
    # for a hybrid one a draft that only OPERA-MS reads. e5's own megahit_draft predates it and stays masked,
    # since rebuilding e5 would fork the cache of every transform in it. A long-read-only sample takes its
    # Flye, read mapping and binners from e5_long instead.
    replaced = {**e3_pratama.REPLACED,
                "assembly": e3_pratama.REPLACED["assembly"] - {"assembly_stats.py", "bbduk.py"} | {"megahit.py"},
                "metagenomics": e3_pratama.REPLACED["metagenomics"] | {"binning/comebin.py", "binning/das_tool.py"},
                "logistics": {"interleave_zipped_short_reads.py"}}
    hybrid = mode == "hybrid"
    legacy = pre_e5_assembly and mode != "long"
    if legacy and not hybrid:
        replaced["assembly"] = replaced["assembly"] - {"megahit.py"}
    own_masked = PILOT_ONLY | (set() if hybrid else {Path("opera_ms.py")}) | (
        set() if legacy and hybrid else {Path("megahit_draft.py")})
    own = [TransformInstanceLibrary.Load(c.LIBRARY / "transforms" / "e5").AsView(own_masked, invert=True)]
    if mode == "long":
        replaced["assembly"] = replaced["assembly"] | {"assembly_stats.py", "flye.py", "flye_raw.py"}
        replaced["metagenomics"] = replaced["metagenomics"] | {"binning/semibin2.py", "binning/metabat2.py"}
        own.append(TransformInstanceLibrary.Load(c.LIBRARY / "transforms" / "e5_long"))
    elif not legacy:
        own.append(TransformInstanceLibrary.Load(c.LIBRARY / "transforms" / "e5_assembly").AsView(
            {Path("megahit.py" if hybrid else "megahit_draft.py")}, invert=True))
    std = [TransformInstanceLibrary.Load(c.MLIB / "transforms" / name).AsView(
               {Path(p) for p in replaced.get(name, ())}, invert=True)
           for name in ("logistics", "assembly", "metagenomics", "functionalAnnotation", "viromics")]
    modelling = TransformInstanceLibrary.Load(c.MLIB / "transforms" / "metabolicModelling")
    bench = TransformInstanceLibrary.Load(c.LIBRARY / "transforms" / "bench").AsView({Path("deepvirfinder.py")}, invert=True)
    binning = TransformInstanceLibrary.Load(c.LIBRARY / "transforms" / "e5_binning")
    return [*own, binning, bench, *std,
            modelling.AsView(GEM_MASK | {Path("prodigal_from_bin.py")}, invert=True),
            TransformInstanceLibrary.Load(c.LIBRARY / "transforms" / "modelling").AsView(
                {Path("carveme_from_orfs.py")}, invert=True),
            TransformInstanceLibrary.Load(c.LIBRARY / "transforms" / "e5_gem")]


def build_targets(mode, mags=True, viral=False):
    # The sample's assembly is OPERA-MS for a hybrid sample, Flye for a long-read-only one and MEGAHIT
    # otherwise; every lane reads it.
    # DAS Tool's bins are the MAG set: CheckM2, ORFs and models run on them alone.
    # The viral lane is opt-in: a study-pooled catalogue is not comparable to Pratama's, which pools
    # short and hybrid samples together, so the pooled Pratama ablation owns vOTUs.
    t = TargetBuilder()
    t.Add("sequences::read_qc_stats")
    asm = t.Add({"hybrid": "e5::opera_ms_assembly", "long": "sequences::flye_assembly",
                 "short": "sequences::megahit_assembly"}[mode])

    if mags:
        bins = t.Add("sequences::das_tool_bin_fasta", parents=[asm])
        t.Add("binning::das_tool_contig_to_bin_table", parents=[asm])
        t.Add("bench::checkm2_quality", parents=[bins])
        gem = t.Add("modelling::carveme_model", parents=[t.Add("sequences::bin_orfs", parents=[bins])])
        t.Add("modelling::memote_score", parents=[gem])

    if not viral:
        return t
    frozen = t.Add("viromics::dereplicated_candidate_virus", parents=[asm])
    for dtype in ("viromics::contig_length_table", "viromics::checkv_contamination",
                  "viromics::checkv_quality_summary"):
        t.Add(dtype, parents=[frozen])
    curated = t.Add("e3::curated_candidate_virus", parents=[frozen])
    t.Add("viromics::votu_cluster_table", parents=[curated])
    t.Add("e3::final_votu_representatives", parents=[curated])
    return t


def solve(args, shape, by_study):
    n = sum(len(s) for s in by_study.values())
    print(f"\n=== batch {args.batch} {shape}: {n} samples in {', '.join(f'{s} ({len(v)})' for s, v in by_study.items())}",
          flush=True)
    cache_dir = CACHE_DIR / f"batch{args.batch}" / shape
    importing = args.cmd == "import"
    remote = importing or args.stage_only or args.launch or args.materialise
    smith = c.agent_for("e5", remote, cache_dir / "dryrun_home")
    ensure = importing or args.import_givens or not remote
    inputs = declare_givens(smith, shape, by_study, cache_dir, ensure)
    pratama_globals = c.pratama_globals(smith, cache_dir, ensure)
    gem_globals = e4_metagem.declare_globals(smith, "open", cache_dir / "gem_globals.xgdb", ensure, with_checkm2=True)
    if importing:
        print(f"the pool at {smith.home.GetPath()} holds the givens of {n} samples")
        return

    mode = SHAPES[shape][2]
    t0 = time.time()
    task = smith.GenerateWorkflow(
        samples=list(inputs.AsSamples("sequences::read_metadata")),
        resources=[DataInstanceLibrary.Load(c.MLIB / "resources" / "env"),
                   DataInstanceLibrary.Load(c.MLIB / "resources" / "lib"),
                   DataInstanceLibrary.Load(c.LIBRARY / "resources" / "bench"),
                   DataInstanceLibrary.Load(c.LIBRARY / "resources" / "e5"),
                   pratama_globals, gem_globals],
        transforms=build_transforms(mode, pre_e5_assembly=args.batch in PRE_E5_ASSEMBLY),
        targets=build_targets(mode, mags=not args.no_mags, viral=args.viral),
    )
    print(f"solved in {time.time() - t0:.1f}s", flush=True)
    c.check_plan(task, expected_counts(shape, by_study))
    c.print_plan(task, width=34)
    if args.dag:
        c.write_dag(task, f"e5_{shape}", cache_dir)
    if remote:
        queue = args.queue_size or QUEUE_BUDGET // len(draw(args.batch, args.shape))
        if queue < 100:
            sys.exit(f"a queue of {queue} is narrower than a 100-wide job array")
        c.stage_and_run(smith, task, cache_dir, args.tag or f"e5_b{args.batch}_{shape}", stage_only=args.stage_only,
                        params=dict(executor=dict(queueSize=queue),
                                    process=dict(tries=4)),
                        scaled=SCALED, comebin_cpus=COMEBIN[shape][0], comebin_memory_gb=COMEBIN[shape][1],
                        comebin_hours=COMEBIN[shape][2], materialise=args.materialise,
                        on_exist="clear" if args.clear else "update")
    else:
        print(f"key={task.GetKey()} (dry run; nothing staged or submitted)")


def cmd_list(args):
    for batch in ([args.batch] if args.batch is not None else sorted(BATCHES)):
        total = 0
        for shape, by_study in draw(batch).items():
            for study, samples in by_study.items():
                total += len(samples)
                print(f"{batch}  {shape:20s} {study:26s} {len(samples):3d}  {samples[0][0]} ..")
        print(f"batch {batch}: {total} samples\n")
    return 0


def cmd_run(args):
    failed = []
    for shape, by_study in draw(args.batch, args.shape).items():
        if shape in HELD_SHAPES and args.shape != shape:
            print(f"skipping {shape}: held until the Pratama assembler ablation reports; name it with --shape to run it")
            continue
        try:
            solve(args, shape, by_study)
        except SystemExit as e:
            failed.append((shape, e.code))
            print(f"FAILED {shape}: {e.code}", file=sys.stderr, flush=True)
    for shape, code in failed:
        print(f"FAILED {shape}: {code}", file=sys.stderr)
    return 1 if failed else 0


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("list")
    p.add_argument("--batch", type=int, choices=sorted(BATCHES))
    p.set_defaults(fn=cmd_list)
    for name in ("import", "run"):
        p = sub.add_parser(name)
        p.add_argument("--batch", type=int, required=True, choices=sorted(BATCHES))
        p.add_argument("--shape", choices=list(SHAPES))
        p.set_defaults(fn=cmd_run, dag=False, stage_only=False, launch=False, materialise=False, tag=None,
                       import_givens=False, clear=False, queue_size=None, no_mags=False, viral=False)
        if name == "run":
            p.add_argument("--dag", action="store_true", help="render each plan to page/dags/e5_<shape>.dag.svg")
            mode = p.add_mutually_exclusive_group()
            mode.add_argument("--stage-only", action="store_true")
            mode.add_argument("--launch", action="store_true")
            mode.add_argument("--materialise", action="store_true", help="stage, fetch every image the plan needs, stop")
            p.add_argument("--tag")
            p.add_argument("--queue-size", type=int,
                           help=f"tasks this run may queue; default {QUEUE_BUDGET} split across the batch's plans")
            p.add_argument("--clear", action="store_true",
                           help="restage from scratch, which a protocol-only transform edit needs; deletes the run's logs")
            p.add_argument("--import", dest="import_givens", action="store_true",
                           help="import what the pool lacks before planning, as `import` does")
            p.add_argument("--no-mags", action="store_true",
                           help="leave out the MAG lane (binners, DAS Tool, CheckM2, GEMs); a later full run adds it from the cache")
            p.add_argument("--viral", action="store_true",
                           help="add the viral lane, pooled per study; off by default")
    args = ap.parse_args()
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())

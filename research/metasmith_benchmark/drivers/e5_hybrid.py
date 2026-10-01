#!/usr/bin/env python3
"""E5 hybrid pilot: graph resolution against polishing, on every dataset with both read types.

  short first  MEGAHIT on the short reads, then OPERA-MS scaffolds and gap-fills its contigs with the long reads
  long first   metaFlye on the long reads, then POLCA corrects it with the short reads

Short reads take E5's QC (bbduk on Pratama's settings); long reads go in raw, as Pratama's do. QUAST
scores every sample's four assemblies (MEGAHIT, OPERA-MS, Flye, Flye + POLCA) without references, and
metaQUAST scores the CAMI samples' against their study's source genomes as well. The samples are a
seeded 10% of every long-read set that shares its samples with a short-read set, rounded to the
nearest sample and at least one, as E1's control subset rounds: six sets, 19 samples. The corpus
picks the agent home: CAMI's five sets in one, Pratama's hybrid pairings in the other.

Subcommands: list, import --corpus C, run --corpus C [--dag] [--stage-only | --launch | --materialise] [--import] [--tag] [--opera-only].
"""

import argparse
import math
import os
import random
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
CACHE_DIR = Path(os.environ.get("E5H_CACHE_DIR", HERE / ".cache" / "e5_hybrid"))

import _common as c  # noqa: E402
import e3_pratama  # noqa: E402
from metasmith.python_api import DataInstanceLibrary, TransformInstanceLibrary, TargetBuilder  # noqa: E402

# study (the short-read dataset) -> its source genomes and [(long-read dataset, platform)]
G = Path("/scratch/phyberos/cami")
STUDIES = {
    "marine": (G / "cami2_challenge/marine/marmgCAMI2_genomes.tar.gz", [("marine_long", "PACBIO_CLR")]),
    "plant_associated": (G / "cami2_challenge/plant_associated/rhimgCAMI2_genomes.tar.gz",
                         [("plant_associated_long_nano", "OXFORD_NANOPORE"),
                          ("plant_associated_long_pacbio", "PACBIO_CLR")]),
    "strain": (G / "cami2_challenge/strain/strmgCAMI2_genomes.tar.gz", [("strain_long", "PACBIO_CLR")]),
    "toy_humangut": (G / "cami3_toy_humangut/long/source_genomes.tar.gz", [("toy_humangut_long", "OXFORD_NANOPORE")]),
}
PRATAMA = "pratama2022"
PRATAMA_PLATFORM = "OXFORD_NANOPORE_HQ"     # R9.4.1, basecalled with Guppy sup
FRACTION = 0.1
SEED = 20261001
TYPE_LIBS = [c.MLIB / "data_types" / t for t in ("sequences.yml", "viromics.yml")] + \
    [c.LIBRARY / "data_types" / t for t in ("e3.yml", "e5.yml")]
# E3's first-attempt (cpus, GB, hours) for the two steps it shares.
SCALED = {
    "cami": {"bbduk_pratama": (4, 16, 2), "megahit": (16, 64, 12)},
    # Pratama's MinION runs hold 6.4 to 12.9 Gbp, against 1 to 3 for a CAMI long-read sample.
    "pratama": {"bbduk_pratama": (4, 16, 2), "megahit": (16, 64, 12), "flye": (16, 128, 24)},
}


def subset_size(n):
    return max(1, math.floor(FRACTION * n + 0.5))


def draw_cami(rng):
    """{study: [(sample id, short reads, long reads, platform)]}.

    A study's long-read sets draw distinct samples: two givens on one short-read path collapse into one.
    """
    rows = c.cami_rows()
    picked = {}
    for study, (_, long_sets) in sorted(STUDIES.items()):
        short = {r["sample_id"]: Path(r["reads_path"]) for r in rows
                 if r["dataset"] == study and r["read_type"] == "short"}
        taken = set()
        picked[study] = []
        for long_dataset, platform in long_sets:
            long_ = {r["sample_id"]: Path(r["reads_path"]) for r in rows
                     if r["dataset"] == long_dataset and r["read_type"] == "long"}
            both = short.keys() & long_.keys()
            free = sorted(both - taken, key=lambda s: int(s.split("_")[1]))
            chosen = sorted(rng.sample(free, subset_size(len(both))), key=lambda s: int(s.split("_")[1]))
            taken.update(chosen)
            picked[study] += [(f"{long_dataset}_{s}", short[s], long_[s], platform) for s in chosen]
    return picked


def draw_pratama(rng):
    """{PRATAMA: [...]}: one pairing from each of a seeded draw of wells, so no two share a MinION run."""
    pairings = e3_pratama.hybrid_pairings()
    partners = e3_pratama.hybrid_partners()
    short = {run: reads for run, _, reads in e3_pratama.enumerate_runs()}
    by_well = {}
    for run, minion in pairings:
        by_well.setdefault(minion, []).append(run)
    wells = sorted(rng.sample(sorted(by_well), subset_size(len(pairings))))
    runs = sorted(rng.choice(sorted(by_well[w])) for w in wells)
    return {PRATAMA: [(run, short[run], partners[run], PRATAMA_PLATFORM) for run in runs]}


def draw(corpus):
    rng = random.Random(SEED)
    cami = draw_cami(rng)
    return cami if corpus == "cami" else draw_pratama(rng)


def declare_givens(smith, by_study, cache_dir, ensure):
    givens = smith.PoolGivens()
    for name, samples in by_study.items():
        study = c.add_value(givens, f"e5h/{name}/contig_study", {"logistics": "contig study", "study": name},
                            "viromics::contig_study", tags=["e5h", name])
        if name in STUDIES:
            c.add_file(givens, f"e5h/{name}/source_genomes", STUDIES[name][0], "e5::source_genomes",
                       parents=[study], tags=["e5h", name])
        for sid, short, long_, platform in samples:
            tags = ["e5h", name, sid]
            meta = c.add_value(givens, f"e5h/{sid}/read_metadata", {"parity": "paired", "length_class": "short"},
                               "sequences::read_metadata", parents=[study], tags=tags)
            pair = c.add_value(givens, f"e5h/{sid}/read_pair", sid, "sequences::read_pair", parents=[meta], tags=tags)
            c.add_file(givens, f"e5h/{sid}/reads", short, "sequences::short_reads_pe", parents=[pair], tags=tags)
            c.add_file(givens, f"e5h/{sid}/nanopore", long_, "e3::nanopore_reads", parents=[pair], tags=tags)
            c.add_value(givens, f"e5h/{sid}/long_read_platform", {"platform": platform}, "e5::long_read_platform",
                        parents=[pair], tags=tags)
    return c.cite(givens, cache_dir / "inputs.xgdb", TYPE_LIBS, ensure)


def build_transforms():
    qc = TransformInstanceLibrary.Load(c.LIBRARY / "transforms" / "e3").AsView({Path("bbduk_pratama.py")})
    megahit = TransformInstanceLibrary.Load(c.MLIB / "transforms" / "assembly").AsView({Path("megahit.py")})
    return [qc, megahit, TransformInstanceLibrary.Load(c.LIBRARY / "transforms" / "e5")]


def build_targets(corpus, opera_only):
    t = TargetBuilder()
    if opera_only:
        t.Add("e5::opera_ms_assembly")
        return t
    t.Add("e5::quast_report")
    if corpus == "cami":
        t.Add("e5::metaquast_report")
    return t


def solve(args):
    corpus = args.corpus
    by_study = draw(corpus)
    cache_dir = CACHE_DIR / corpus
    importing = args.cmd == "import"
    remote = importing or args.stage_only or args.launch or args.materialise
    smith = c.agent_for(corpus, remote, cache_dir / "dryrun_home")
    inputs = declare_givens(smith, by_study, cache_dir, ensure=importing or args.import_givens or not remote)
    n = sum(len(s) for s in by_study.values())
    if importing:
        print(f"the pool at {smith.home.GetPath()} holds the givens of {n} samples")
        return

    task = smith.GenerateWorkflow(
        samples=list(inputs.AsSamples("sequences::read_metadata")),
        resources=[DataInstanceLibrary.Load(c.MLIB / "resources" / "env"),
                   DataInstanceLibrary.Load(c.LIBRARY / "resources" / "e5")],
        transforms=build_transforms(),
        targets=build_targets(corpus, args.opera_only),
    )
    genomes = 0 if args.opera_only else sum(1 for s in by_study if s in STUDIES)
    c.check_plan(task, {"viromics::contig_study": len(by_study), "e5::source_genomes": genomes,
                        "sequences::read_metadata": n, "sequences::read_pair": n, "sequences::short_reads_pe": n,
                        "e3::nanopore_reads": n, "e5::long_read_platform": 0 if args.opera_only else n})
    c.print_plan(task)
    if args.dag:
        c.write_dag(task, "e5_hybrid" if corpus == "cami" else "e5_hybrid_pratama", cache_dir)
    if remote:
        tag = args.tag or f"e5_hybrid_{corpus}" + ("_opera" if args.opera_only else "")
        c.stage_and_run(smith, task, cache_dir, tag, stage_only=args.stage_only,
                        params=dict(executor=dict(queueSize=100), process=dict(tries=3)),
                        scaled=SCALED[corpus], materialise=args.materialise)


def cmd_list(args):
    total = 0
    for corpus in ("cami", "pratama"):
        for study, samples in draw(corpus).items():
            total += len(samples)
            print(f"{corpus:8s} {study:17s} {len(samples):3d}  {', '.join(s[0] for s in samples)}")
    print(f"{total} samples")
    return 0


def cmd_run(args):
    solve(args)
    return 0


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("list").set_defaults(fn=cmd_list)
    for name in ("import", "run"):
        p = sub.add_parser(name)
        p.add_argument("--corpus", required=True, choices=("cami", "pratama"))
        p.set_defaults(fn=cmd_run, dag=False, stage_only=False, launch=False, materialise=False, tag=None,
                       import_givens=False, opera_only=False)
        if name == "run":
            p.add_argument("--dag", action="store_true", help="render the plan to page/dags/")
            mode = p.add_mutually_exclusive_group()
            mode.add_argument("--stage-only", action="store_true")
            mode.add_argument("--launch", action="store_true")
            mode.add_argument("--materialise", action="store_true", help="stage, fetch every image the plan needs, stop")
            p.add_argument("--tag")
            p.add_argument("--opera-only", action="store_true",
                           help="target the OPERA-MS assemblies alone, beside a full run that is still going")
            p.add_argument("--import", dest="import_givens", action="store_true",
                           help="import what the pool lacks before planning, as `import` does")
    args = ap.parse_args()
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())
